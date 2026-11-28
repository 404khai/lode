// Package ledger is Lode's metadata service, backed by PostgreSQL. It is the
// source of truth for buckets, objects, versions, multipart uploads, the
// manifests that map them to nuggets, and nugget reference counts.
package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/nugget"
)

var (
	ErrNoSuchBucket   = errors.New("ledger: no such bucket")
	ErrBucketExists   = errors.New("ledger: bucket already exists")
	ErrBucketNotEmpty = errors.New("ledger: bucket not empty")
	ErrNoSuchKey      = errors.New("ledger: no such key")
	ErrNoSuchVersion  = errors.New("ledger: no such version")
	// ErrPreconditionFailed means a conditional write's If-Match or
	// If-None-Match did not hold against the current version.
	ErrPreconditionFailed = errors.New("ledger: precondition failed")
)

// Condition is an S3 conditional-write precondition, checked atomically
// against the key's current version.
type Condition struct {
	IfMatch     string
	IfNoneMatch string
}

func etagIn(list, etag string) bool {
	for _, e := range strings.Split(list, ",") {
		if e = strings.TrimSpace(e); e == "*" || normETag(e) == normETag(etag) {
			return true
		}
	}
	return false
}

// checkCondition evaluates c against the object's current version. The
// object row must already be locked.
func checkCondition(ctx context.Context, tx pgx.Tx, objectID int64, c Condition) error {
	if c.IfMatch == "" && c.IfNoneMatch == "" {
		return nil
	}
	var etag string
	err := tx.QueryRow(ctx, `SELECT v.etag FROM objects o JOIN object_versions v ON v.id = o.current_version_id
		WHERE o.id = $1 AND NOT v.is_delete_marker`, objectID).Scan(&etag)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if c.IfMatch != "" {
		if !exists {
			return ErrNoSuchKey
		}
		if !etagIn(c.IfMatch, etag) {
			return ErrPreconditionFailed
		}
	}
	if c.IfNoneMatch != "" && exists && etagIn(c.IfNoneMatch, etag) {
		return ErrPreconditionFailed
	}
	return nil
}

const (
	errUniqueViolation = "23505"

	// NullVersion is the version ID of objects written while versioning is
	// off or suspended.
	NullVersion = "null"

	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"
)

type Bucket struct {
	Name        string
	CreatedAt   time.Time
	Versioning  string // "", Enabled or Suspended
	Replication int    // replicas per nugget; 0 = cluster default
}

// NuggetRef is one entry of a manifest.
type NuggetRef struct {
	ID       nugget.ID
	Length   int64
	Replicas []string // veins holding a copy, filled when reading manifests
}

// Version is the metadata of one object version.
type Version struct {
	VersionID         string
	IsDeleteMarker    bool
	Size              int64
	ETag              string
	ChecksumAlgorithm string
	ChecksumValue     string
	Headers           map[string]string
	ChunkSize         int
	PartsCount        int
	PartSizes         []int64 // sizes of the parts of a multipart object
	Nuggets           []NuggetRef
	LastModified      time.Time
}

// Object is a key with one of its versions.
type Object struct {
	Key        string
	Versioning string
	Version
}

// ObjectSummary is a listing entry.
type ObjectSummary struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// WriteResult describes a committed write.
type WriteResult struct {
	VersionID    string
	Versioning   string
	DeleteMarker bool
}

type Ledger struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// Pool exposes the connection pool for workers that need dedicated
// connections (leader election).
func (l *Ledger) Pool() *pgxpool.Pool { return l.pool }

// Ping checks connectivity.
func (l *Ledger) Ping(ctx context.Context) error { return l.pool.Ping(ctx) }

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (l *Ledger) CreateBucket(ctx context.Context, name string, replication int) error {
	_, err := l.pool.Exec(ctx, "INSERT INTO buckets (name, replication) VALUES ($1, $2)", name, replication)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == errUniqueViolation {
		return ErrBucketExists
	}
	return err
}

func (l *Ledger) HeadBucket(ctx context.Context, name string) (Bucket, error) {
	b := Bucket{Name: name}
	err := l.pool.QueryRow(ctx, "SELECT created_at, versioning, replication FROM buckets WHERE name = $1", name).
		Scan(&b.CreatedAt, &b.Versioning, &b.Replication)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNoSuchBucket
	}
	return b, err
}

// SetVersioning sets a bucket's versioning status (Enabled or Suspended).
func (l *Ledger) SetVersioning(ctx context.Context, name, status string) error {
	tag, err := l.pool.Exec(ctx, "UPDATE buckets SET versioning = $2 WHERE name = $1", name, status)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNoSuchBucket
	}
	return err
}

func (l *Ledger) DeleteBucket(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var id int64
		err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE name = $1 FOR UPDATE", name).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoSuchBucket
		}
		if err != nil {
			return err
		}
		var nonEmpty bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM objects WHERE bucket_id = $1)", id).Scan(&nonEmpty); err != nil {
			return err
		}
		if nonEmpty {
			return ErrBucketNotEmpty
		}
		// Like S3, incomplete multipart uploads do not keep a bucket alive.
		rows, err := tx.Query(ctx, "SELECT upload_id FROM multipart_uploads WHERE bucket_id = $1 FOR UPDATE", id)
		if err != nil {
			return err
		}
		uploads, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, u := range uploads {
			if err := abortLocked(ctx, tx, u); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, "DELETE FROM buckets WHERE id = $1", id)
		return err
	})
}

func (l *Ledger) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := l.pool.Query(ctx, "SELECT name, created_at, versioning FROM buckets ORDER BY name")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Bucket, error) {
		var b Bucket
		return b, r.Scan(&b.Name, &b.CreatedAt, &b.Versioning)
	})
}

// lockBucket share-locks the bucket row so it cannot be deleted mid-write.
func lockBucket(ctx context.Context, tx pgx.Tx, name string) (id int64, versioning string, err error) {
	err = tx.QueryRow(ctx, "SELECT id, versioning FROM buckets WHERE name = $1 FOR SHARE", name).Scan(&id, &versioning)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", ErrNoSuchBucket
	}
	return id, versioning, err
}

// TouchNugget records that an upload is about to write ref, so Tailings will
// not reclaim it during the grace window, and returns the veins that already
// hold it. Callers must touch a nugget before writing it to any vein.
func (l *Ledger) TouchNugget(ctx context.Context, ref NuggetRef) ([]string, error) {
	var replicas []string
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO nuggets (id, length, refs, last_touched) VALUES ($1, $2, 0, now())
			ON CONFLICT (id) DO UPDATE SET last_touched = now()`, ref.ID[:], ref.Length); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT vein_id FROM nugget_replicas WHERE nugget = $1 ORDER BY vein_id", ref.ID[:])
		if err != nil {
			return err
		}
		replicas, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return replicas, err
}

// insertManifest stores a manifest and adds one reference per entry.
func insertManifest(ctx context.Context, tx pgx.Tx, chunkSize int, refs []NuggetRef) (int64, error) {
	var id int64
	if err := tx.QueryRow(ctx, "INSERT INTO manifests (chunk_size) VALUES ($1) RETURNING id", chunkSize).Scan(&id); err != nil {
		return 0, err
	}
	if len(refs) == 0 {
		return id, nil
	}
	rows := make([][]any, len(refs))
	for i, n := range refs {
		rows[i] = []any{id, i, n.ID[:], n.Length}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"manifest_nuggets"},
		[]string{"manifest_id", "seq", "nugget", "length"}, pgx.CopyFromRows(rows)); err != nil {
		return 0, err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO nuggets (id, length, refs, last_touched)
		SELECT nugget, max(length), count(*), now() FROM manifest_nuggets WHERE manifest_id = $1 GROUP BY nugget
		ON CONFLICT (id) DO UPDATE SET refs = nuggets.refs + EXCLUDED.refs, last_touched = now()`, id)
	return id, err
}

// dropManifest removes a manifest and its references. last_touched is reset
// so the grace window starts when a nugget's last reference goes.
func dropManifest(ctx context.Context, tx pgx.Tx, id int64) error {
	if _, err := tx.Exec(ctx, `
		UPDATE nuggets n SET refs = n.refs - d.cnt, last_touched = now()
		FROM (SELECT nugget, count(*) AS cnt FROM manifest_nuggets WHERE manifest_id = $1 GROUP BY nugget) d
		WHERE n.id = d.nugget`, id); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "DELETE FROM manifests WHERE id = $1", id) // cascades manifest_nuggets
	return err
}

func loadManifest(ctx context.Context, tx pgx.Tx, id int64) ([]NuggetRef, error) {
	rows, err := tx.Query(ctx, `
		SELECT mn.nugget, mn.length, coalesce(r.veins, '{}')
		FROM manifest_nuggets mn
		LEFT JOIN LATERAL (SELECT array_agg(vein_id ORDER BY vein_id) AS veins FROM nugget_replicas WHERE nugget = mn.nugget) r ON true
		WHERE mn.manifest_id = $1 ORDER BY mn.seq`, id)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (NuggetRef, error) {
		var b []byte
		var n NuggetRef
		if err := r.Scan(&b, &n.Length, &n.Replicas); err != nil {
			return n, err
		}
		id, err := nugget.FromBytes(b)
		n.ID = id
		return n, err
	})
}

// lockObject returns the object row for key, creating it if create is set.
// The row is locked for the rest of tx.
func lockObject(ctx context.Context, tx pgx.Tx, bucketID int64, key string, create bool) (int64, error) {
	var id int64
	var err error
	if create {
		err = tx.QueryRow(ctx, `
			INSERT INTO objects (bucket_id, key) VALUES ($1, $2)
			ON CONFLICT (bucket_id, key) DO UPDATE SET key = EXCLUDED.key
			RETURNING id`, bucketID, key).Scan(&id)
	} else {
		err = tx.QueryRow(ctx, "SELECT id FROM objects WHERE bucket_id = $1 AND key = $2 FOR UPDATE", bucketID, key).Scan(&id)
	}
	return id, err
}

// dropVersion deletes one version row and its manifest.
func dropVersion(ctx context.Context, tx pgx.Tx, objectID int64, versionID string) (found, wasMarker bool, err error) {
	var manifestID *int64
	err = tx.QueryRow(ctx, "DELETE FROM object_versions WHERE object_id = $1 AND version_id = $2 RETURNING manifest_id, is_delete_marker",
		objectID, versionID).Scan(&manifestID, &wasMarker)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if manifestID != nil {
		err = dropManifest(ctx, tx, *manifestID)
	}
	return true, wasMarker, err
}

// refreshCurrent points the object at its newest version, deleting the object
// row once no versions remain.
func refreshCurrent(ctx context.Context, tx pgx.Tx, objectID int64) error {
	var current *int64
	if err := tx.QueryRow(ctx, "SELECT max(id) FROM object_versions WHERE object_id = $1", objectID).Scan(&current); err != nil {
		return err
	}
	if current == nil {
		_, err := tx.Exec(ctx, "DELETE FROM objects WHERE id = $1", objectID)
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE objects SET current_version_id = $1 WHERE id = $2", *current, objectID)
	return err
}

// addVersion makes a new version current, honouring the bucket's versioning
// mode: with versioning enabled every write gets a fresh version ID;
// otherwise it replaces the "null" version.
func addVersion(ctx context.Context, tx pgx.Tx, objectID int64, versioning string, manifestID *int64, v Version) (string, error) {
	versionID := NullVersion
	if versioning == VersioningEnabled {
		versionID = newID()
	} else if _, _, err := dropVersion(ctx, tx, objectID, NullVersion); err != nil {
		return "", err
	}
	headers := v.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	var parts *int
	if v.PartsCount > 0 {
		parts = &v.PartsCount
	}
	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO object_versions (object_id, manifest_id, version_id, is_delete_marker, size, etag,
			checksum_algorithm, checksum_value, headers, parts_count, part_sizes)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''), $9, $10, $11) RETURNING id`,
		objectID, manifestID, versionID, v.IsDeleteMarker, v.Size, v.ETag,
		v.ChecksumAlgorithm, v.ChecksumValue, headers, parts, v.PartSizes).Scan(&id)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, "UPDATE objects SET current_version_id = $1 WHERE id = $2", id, objectID)
	return versionID, err
}

// PutObject commits v as the newest version of key. It becomes visible
// atomically with the manifest.
func (l *Ledger) PutObject(ctx context.Context, bucket, key string, v Version, cond Condition) (res WriteResult, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		bid, versioning, err := lockBucket(ctx, tx, bucket)
		if err != nil {
			return err
		}
		res.Versioning = versioning
		manifestID, err := insertManifest(ctx, tx, v.ChunkSize, v.Nuggets)
		if err != nil {
			return err
		}
		objectID, err := lockObject(ctx, tx, bid, key, true)
		if err != nil {
			return err
		}
		if err := checkCondition(ctx, tx, objectID, cond); err != nil {
			return err
		}
		res.VersionID, err = addVersion(ctx, tx, objectID, versioning, &manifestID, v)
		return err
	})
	return res, err
}

// GetObject returns a version of key: the current one when versionID is
// empty. A current delete marker is returned with IsDeleteMarker set and
// ErrNoSuchKey.
func (l *Ledger) GetObject(ctx context.Context, bucket, key, versionID string) (*Object, error) {
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o := &Object{Key: key}
	var bucketID int64
	if err := tx.QueryRow(ctx, "SELECT id, versioning FROM buckets WHERE name = $1", bucket).Scan(&bucketID, &o.Versioning); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoSuchBucket
		}
		return nil, err
	}
	cond := "v.id = o.current_version_id"
	args := []any{bucketID, key}
	if versionID != "" {
		cond = "v.version_id = $3"
		args = append(args, versionID)
	}
	var manifestID *int64
	var algo, sum *string
	var chunk *int
	var parts *int
	err = tx.QueryRow(ctx, `
		SELECT v.manifest_id, v.version_id, v.is_delete_marker, v.size, v.etag, v.checksum_algorithm, v.checksum_value,
		       v.headers, v.created_at, v.parts_count, v.part_sizes, m.chunk_size
		FROM objects o
		JOIN object_versions v ON v.object_id = o.id
		LEFT JOIN manifests m ON m.id = v.manifest_id
		WHERE o.bucket_id = $1 AND o.key = $2 AND `+cond, args...).
		Scan(&manifestID, &o.VersionID, &o.IsDeleteMarker, &o.Size, &o.ETag, &algo, &sum,
			&o.Headers, &o.LastModified, &parts, &o.PartSizes, &chunk)
	if errors.Is(err, pgx.ErrNoRows) {
		if versionID != "" {
			return nil, ErrNoSuchVersion
		}
		return nil, ErrNoSuchKey
	}
	if err != nil {
		return nil, err
	}
	if algo != nil {
		o.ChecksumAlgorithm = *algo
	}
	if sum != nil {
		o.ChecksumValue = *sum
	}
	if chunk != nil {
		o.ChunkSize = *chunk
	}
	if parts != nil {
		o.PartsCount = *parts
	}
	if o.IsDeleteMarker {
		return o, ErrNoSuchKey
	}
	o.Nuggets, err = loadManifest(ctx, tx, *manifestID)
	return o, err
}

// DeleteObject deletes key following S3 semantics. Without a versionID it
// removes the null version (versioning never enabled), or adds a delete
// marker (enabled, or suspended replacing the null version). With a
// versionID it permanently removes that version.
func (l *Ledger) DeleteObject(ctx context.Context, bucket, key, versionID string) (res WriteResult, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		bid, versioning, err := lockBucket(ctx, tx, bucket)
		if err != nil {
			return err
		}
		res.Versioning = versioning

		if versionID == "" && versioning != "" {
			objectID, err := lockObject(ctx, tx, bid, key, true)
			if err != nil {
				return err
			}
			res.DeleteMarker = true
			res.VersionID, err = addVersion(ctx, tx, objectID, versioning, nil, Version{IsDeleteMarker: true})
			return err
		}

		objectID, err := lockObject(ctx, tx, bid, key, false)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // deleting a missing key or version succeeds, as in S3
		}
		if err != nil {
			return err
		}
		if versionID == "" {
			versionID = NullVersion
		}
		found, wasMarker, err := dropVersion(ctx, tx, objectID, versionID)
		if err != nil || !found {
			return err
		}
		res.VersionID, res.DeleteMarker = versionID, wasMarker
		return refreshCurrent(ctx, tx, objectID)
	})
	return res, err
}

// ListObjects returns up to limit live objects (current version is not a
// delete marker) whose key starts with prefix and sorts after after.
func (l *Ledger) ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]ObjectSummary, error) {
	if _, err := l.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := l.pool.Query(ctx, `
		SELECT o.key, v.size, v.etag, v.created_at
		FROM buckets b
		JOIN objects o ON o.bucket_id = b.id
		JOIN object_versions v ON v.id = o.current_version_id
		WHERE b.name = $1 AND o.key > $2 AND starts_with(o.key, $3) AND NOT v.is_delete_marker
		ORDER BY o.key
		LIMIT $4`, bucket, after, prefix, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ObjectSummary, error) {
		var s ObjectSummary
		return s, r.Scan(&s.Key, &s.Size, &s.ETag, &s.LastModified)
	})
}

// VersionSummary is a ListObjectVersions entry.
type VersionSummary struct {
	Key            string
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
	Size           int64
	ETag           string
	LastModified   time.Time
}

// ListVersions lists versions newest-first within each key, starting after
// (keyMarker, versionMarker). An empty versionMarker skips keyMarker entirely.
func (l *Ledger) ListVersions(ctx context.Context, bucket, prefix, keyMarker, versionMarker string, limit int) ([]VersionSummary, error) {
	if _, err := l.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := l.pool.Query(ctx, `
		WITH marker AS (
			SELECT coalesce((SELECT v.id FROM buckets b JOIN objects o ON o.bucket_id = b.id
				JOIN object_versions v ON v.object_id = o.id
				WHERE b.name = $1 AND o.key = $3 AND v.version_id = $4 AND $4 <> ''), 0) AS seq
		)
		SELECT o.key, v.version_id, v.id = o.current_version_id, v.is_delete_marker, v.size, v.etag, v.created_at
		FROM buckets b
		JOIN objects o ON o.bucket_id = b.id
		JOIN object_versions v ON v.object_id = o.id, marker
		WHERE b.name = $1 AND starts_with(o.key, $2)
		  AND (o.key > $3 OR (o.key = $3 AND v.id < marker.seq))
		ORDER BY o.key, v.id DESC
		LIMIT $5`, bucket, prefix, keyMarker, versionMarker, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (VersionSummary, error) {
		var s VersionSummary
		return s, r.Scan(&s.Key, &s.VersionID, &s.IsLatest, &s.IsDeleteMarker, &s.Size, &s.ETag, &s.LastModified)
	})
}
