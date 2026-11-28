// Package ledger is Lode's metadata service, backed by PostgreSQL. It is the
// source of truth for buckets, objects, versions, multipart uploads, the
// manifests that map them to nuggets, and nugget reference counts.
package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	Name       string
	CreatedAt  time.Time
	Versioning string // "", Enabled or Suspended
	BucketConfig
}

// BucketConfig is a bucket's storage class and placement.
type BucketConfig struct {
	Replication int      // replicas per nugget; 0 = cluster default
	ECData      int      // k data shards; 0 = replicated
	ECParity    int      // m parity shards
	Placement   string   // Survey strategy; "" = cluster default
	Pinned      []string // veins the "pinned" strategy may use
}

// NuggetRef is one entry of a manifest.
type NuggetRef struct {
	ID       nugget.ID
	Length   int64
	Replicas []string // veins holding a copy, filled when reading manifests
	// Erasure-coded layout (ECData > 0): where each shard lives.
	ECData, ECParity int
	Shards           []ShardLoc
}

// ShardLoc is one stored shard of an erasure-coded nugget.
type ShardLoc struct {
	Index int
	ID    nugget.ID
	Vein  string
}

// Location is one stored blob (a replica or a shard) on a vein.
type Location struct {
	Vein string
	ID   nugget.ID
}

// Version is the metadata of one object version.
type Version struct {
	VersionID         string
	IsDeleteMarker    bool
	Size              int64
	ETag              string
	ChecksumAlgorithm string
	ChecksumValue     string
	ChecksumType      string // COMPOSITE or FULL_OBJECT for multipart objects; "" otherwise
	Headers           map[string]string
	Tags              map[string]string
	AccessKey         string // who wrote this version (per-key quotas)
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
	// Multipart checksum computed at Complete, if the upload declared one.
	ChecksumAlgorithm, ChecksumValue, ChecksumType string
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

func (l *Ledger) CreateBucket(ctx context.Context, name string, cfg BucketConfig) error {
	if cfg.Pinned == nil {
		cfg.Pinned = []string{}
	}
	_, err := l.pool.Exec(ctx, `INSERT INTO buckets (name, replication, ec_data, ec_parity, placement, pinned)
		VALUES ($1, $2, $3, $4, $5, $6)`, name, cfg.Replication, cfg.ECData, cfg.ECParity, cfg.Placement, cfg.Pinned)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == errUniqueViolation {
		return ErrBucketExists
	}
	return err
}

func (l *Ledger) HeadBucket(ctx context.Context, name string) (Bucket, error) {
	b := Bucket{Name: name}
	err := l.pool.QueryRow(ctx, `SELECT created_at, versioning, replication, ec_data, ec_parity, placement, pinned
		FROM buckets WHERE name = $1`, name).
		Scan(&b.CreatedAt, &b.Versioning, &b.Replication, &b.ECData, &b.ECParity, &b.Placement, &b.Pinned)
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
		if _, err := tx.Exec(ctx, "DELETE FROM usage_counters WHERE scope = 'bucket' AND subject = $1", name); err != nil {
			return err
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
// not reclaim it during the grace window, and returns what is already stored
// for it. A new nugget takes layout k+m (0+0 = replicated); an existing
// nugget keeps the layout it was first written with. Callers must touch a
// nugget before writing it to any vein.
func (l *Ledger) TouchNugget(ctx context.Context, ref NuggetRef, replication, k, m int) (NuggetRef, error) {
	out := NuggetRef{ID: ref.ID, Length: ref.Length}
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO nuggets (id, length, refs, last_touched, replication, ec_data, ec_parity) VALUES ($1, $2, 0, now(), $3, $4, $5)
			ON CONFLICT (id) DO UPDATE SET last_touched = now(), replication = greatest(nuggets.replication, EXCLUDED.replication)
			RETURNING ec_data, ec_parity`,
			ref.ID[:], ref.Length, replication, k, m).Scan(&out.ECData, &out.ECParity); err != nil {
			return err
		}
		var err error
		if out.Replicas, err = replicasOf(ctx, tx, ref.ID); err != nil {
			return err
		}
		out.Shards, err = shardsOf(ctx, tx, ref.ID)
		return err
	})
	return out, err
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func replicasOf(ctx context.Context, q querier, id nugget.ID) ([]string, error) {
	rows, err := q.Query(ctx, "SELECT vein_id FROM nugget_replicas WHERE nugget = $1 ORDER BY vein_id", id[:])
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func shardsOf(ctx context.Context, q querier, id nugget.ID) ([]ShardLoc, error) {
	rows, err := q.Query(ctx, "SELECT idx, shard, vein_id FROM nugget_shards WHERE nugget = $1 ORDER BY idx, vein_id", id[:])
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanShard)
}

func scanShard(r pgx.CollectableRow) (ShardLoc, error) {
	var s ShardLoc
	var b []byte
	if err := r.Scan(&s.Index, &b, &s.Vein); err != nil {
		return s, err
	}
	id, err := nugget.FromBytes(b)
	s.ID = id
	return s, err
}

// AddShards records stored shards of id.
func (l *Ledger) AddShards(ctx context.Context, id nugget.ID, shards []ShardLoc) error {
	if len(shards) == 0 {
		return nil
	}
	idx := make([]int32, len(shards))
	ids := make([][]byte, len(shards))
	veins := make([]string, len(shards))
	for i, s := range shards {
		idx[i] = int32(s.Index) // #nosec G115 -- shard index < 32
		ids[i] = s.ID[:]
		veins[i] = s.Vein
	}
	_, err := l.pool.Exec(ctx, `INSERT INTO nugget_shards (nugget, idx, shard, vein_id)
		SELECT $1, i, s, v FROM unnest($2::int[], $3::bytea[], $4::text[]) AS t(i, s, v) ON CONFLICT DO NOTHING`,
		id[:], idx, ids, veins)
	return err
}

// RemoveShard forgets one stored shard.
func (l *Ledger) RemoveShard(ctx context.Context, id nugget.ID, idx int, vein string) error {
	_, err := l.pool.Exec(ctx, "DELETE FROM nugget_shards WHERE nugget = $1 AND idx = $2 AND vein_id = $3", id[:], idx, vein)
	return err
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
		SELECT mn.nugget, mn.length, coalesce(r.veins, '{}'), coalesce(n.ec_data, 0), coalesce(n.ec_parity, 0)
		FROM manifest_nuggets mn
		LEFT JOIN nuggets n ON n.id = mn.nugget
		LEFT JOIN LATERAL (SELECT array_agg(vein_id ORDER BY vein_id) AS veins FROM nugget_replicas WHERE nugget = mn.nugget) r ON true
		WHERE mn.manifest_id = $1 ORDER BY mn.seq`, id)
	if err != nil {
		return nil, err
	}
	refs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (NuggetRef, error) {
		var b []byte
		var n NuggetRef
		if err := r.Scan(&b, &n.Length, &n.Replicas, &n.ECData, &n.ECParity); err != nil {
			return n, err
		}
		id, err := nugget.FromBytes(b)
		n.ID = id
		return n, err
	})
	if err != nil {
		return nil, err
	}
	// Shard locations for erasure-coded nuggets, in one query.
	srows, err := tx.Query(ctx, `
		SELECT s.nugget, s.idx, s.shard, s.vein_id FROM nugget_shards s
		WHERE s.nugget IN (SELECT mn.nugget FROM manifest_nuggets mn JOIN nuggets n ON n.id = mn.nugget
			WHERE mn.manifest_id = $1 AND n.ec_data > 0)
		ORDER BY s.idx, s.vein_id`, id)
	if err != nil {
		return nil, err
	}
	byNugget := map[nugget.ID][]ShardLoc{}
	for srows.Next() {
		var nb, sb []byte
		var loc ShardLoc
		if err := srows.Scan(&nb, &loc.Index, &sb, &loc.Vein); err != nil {
			srows.Close()
			return nil, err
		}
		nid, _ := nugget.FromBytes(nb)
		loc.ID, _ = nugget.FromBytes(sb)
		byNugget[nid] = append(byNugget[nid], loc)
	}
	if err := srows.Err(); err != nil {
		return nil, err
	}
	for i := range refs {
		if refs[i].ECData > 0 {
			refs[i].Shards = byNugget[refs[i].ID]
		}
	}
	return refs, nil
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
	var size int64
	var accessKey string
	err = tx.QueryRow(ctx, `DELETE FROM object_versions WHERE object_id = $1 AND version_id = $2
		RETURNING manifest_id, is_delete_marker, size, access_key`,
		objectID, versionID).Scan(&manifestID, &wasMarker, &size, &accessKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !wasMarker {
		if err := adjustUsage(ctx, tx, objectID, accessKey, -size, -1, false); err != nil {
			return false, false, err
		}
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
	tags := v.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	if !v.IsDeleteMarker {
		if err := adjustUsage(ctx, tx, objectID, v.AccessKey, v.Size, 1, true); err != nil {
			return "", err
		}
	}
	var id int64
	err := tx.QueryRow(ctx, `
		INSERT INTO object_versions (object_id, manifest_id, version_id, is_delete_marker, size, etag,
			checksum_algorithm, checksum_value, headers, parts_count, part_sizes, tags, checksum_type, access_key)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), NULLIF($8, ''), $9, $10, $11, $12, NULLIF($13, ''), $14) RETURNING id`,
		objectID, manifestID, versionID, v.IsDeleteMarker, v.Size, v.ETag,
		v.ChecksumAlgorithm, v.ChecksumValue, headers, parts, v.PartSizes, tags, v.ChecksumType, v.AccessKey).Scan(&id)
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
		       v.headers, v.created_at, v.parts_count, v.part_sizes, m.chunk_size, v.tags, coalesce(v.checksum_type, '')
		FROM objects o
		JOIN object_versions v ON v.object_id = o.id
		LEFT JOIN manifests m ON m.id = v.manifest_id
		WHERE o.bucket_id = $1 AND o.key = $2 AND `+cond, args...).
		Scan(&manifestID, &o.VersionID, &o.IsDeleteMarker, &o.Size, &o.ETag, &algo, &sum,
			&o.Headers, &o.LastModified, &parts, &o.PartSizes, &chunk, &o.Tags, &o.ChecksumType)
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

// ErrQuotaExceeded means a write would take a bucket or access key past
// its quota.
var ErrQuotaExceeded = errors.New("ledger: quota exceeded")

// adjustUsage changes the usage counters of the object's bucket and of
// accessKey, and with enforce set fails with ErrQuotaExceeded if either is
// now over its quota. The counter rows stay locked until the transaction
// ends, so concurrent writes cannot jointly overshoot a quota.
func adjustUsage(ctx context.Context, tx pgx.Tx, objectID int64, accessKey string, dBytes, dObjects int64, enforce bool) error {
	var bucket string
	if err := tx.QueryRow(ctx, "SELECT b.name FROM objects o JOIN buckets b ON b.id = o.bucket_id WHERE o.id = $1", objectID).Scan(&bucket); err != nil {
		return err
	}
	subjects := [][2]string{{"bucket", bucket}}
	if accessKey != "" {
		subjects = append(subjects, [2]string{"key", accessKey})
	}
	for _, sub := range subjects {
		var bytes, objects int64
		var maxBytes, maxObjects *int64
		err := tx.QueryRow(ctx, `
			WITH u AS (
				INSERT INTO usage_counters (scope, subject, bytes, objects) VALUES ($1, $2, greatest($3, 0), greatest($4, 0))
				ON CONFLICT (scope, subject) DO UPDATE SET bytes = greatest(usage_counters.bytes + $3, 0),
					objects = greatest(usage_counters.objects + $4, 0)
				RETURNING bytes, objects)
			SELECT u.bytes, u.objects, q.max_bytes, q.max_objects
			FROM u LEFT JOIN quotas q ON q.scope = $1 AND q.subject = $2`, sub[0], sub[1], dBytes, dObjects).
			Scan(&bytes, &objects, &maxBytes, &maxObjects)
		if err != nil {
			return err
		}
		if enforce && ((maxBytes != nil && bytes > *maxBytes) || (maxObjects != nil && objects > *maxObjects)) {
			return fmt.Errorf("%w: %s %s", ErrQuotaExceeded, sub[0], sub[1])
		}
	}
	return nil
}

// CopySource names the object version a copy reads from.
type CopySource struct {
	Bucket, Key, VersionID string
}

// CopyObject writes a new version of dst that shares src's nuggets: no bytes
// move. headers and tags replace src's when non-nil. The source is read in
// the same transaction, so a concurrent overwrite of src cannot tear it.
func (l *Ledger) CopyObject(ctx context.Context, src CopySource, dstBucket, dstKey string,
	headers, tags map[string]string, cond Condition, accessKey string) (copied *Object, res WriteResult, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		srcBucketID, _, err := lockBucket(ctx, tx, src.Bucket)
		if err != nil {
			return err
		}
		o := &Object{Key: src.Key}
		cond2 := "v.id = o.current_version_id"
		args := []any{srcBucketID, src.Key}
		if src.VersionID != "" {
			cond2 = "v.version_id = $3"
			args = append(args, src.VersionID)
		}
		var manifestID *int64
		var algo, sum *string
		var parts *int
		err = tx.QueryRow(ctx, `
			SELECT v.manifest_id, v.version_id, v.is_delete_marker, v.size, v.etag, v.checksum_algorithm, v.checksum_value,
			       coalesce(v.checksum_type, ''), v.headers, v.tags, v.parts_count, v.part_sizes, v.created_at
			FROM objects o JOIN object_versions v ON v.object_id = o.id
			WHERE o.bucket_id = $1 AND o.key = $2 AND `+cond2+` FOR SHARE OF v`, args...).
			Scan(&manifestID, &o.VersionID, &o.IsDeleteMarker, &o.Size, &o.ETag, &algo, &sum, &o.ChecksumType,
				&o.Headers, &o.Tags, &parts, &o.PartSizes, &o.LastModified)
		if errors.Is(err, pgx.ErrNoRows) {
			if src.VersionID != "" {
				return ErrNoSuchVersion
			}
			return ErrNoSuchKey
		}
		if err != nil {
			return err
		}
		if o.IsDeleteMarker {
			if src.VersionID != "" {
				return ErrCopyFromDeleteMarker
			}
			return ErrNoSuchKey
		}
		if algo != nil {
			o.ChecksumAlgorithm = *algo
		}
		if sum != nil {
			o.ChecksumValue = *sum
		}
		if parts != nil {
			o.PartsCount = *parts
		}
		if o.Nuggets, err = loadManifest(ctx, tx, *manifestID); err != nil {
			return err
		}
		copied = o

		dstBucketID, versioning, err := lockBucket(ctx, tx, dstBucket)
		if err != nil {
			return err
		}
		res.Versioning = versioning
		v := o.Version
		v.VersionID, v.LastModified, v.AccessKey = "", time.Time{}, accessKey
		if headers != nil {
			v.Headers = headers
		}
		if tags != nil {
			v.Tags = tags
		}
		newManifest, err := insertManifest(ctx, tx, o.ChunkSize, v.Nuggets)
		if err != nil {
			return err
		}
		// The destination bucket may want more copies than the source did.
		if _, err := tx.Exec(ctx, `UPDATE nuggets n SET replication = greatest(n.replication, b.replication)
			FROM manifest_nuggets mn, buckets b WHERE mn.manifest_id = $1 AND n.id = mn.nugget AND b.id = $2`,
			newManifest, dstBucketID); err != nil {
			return err
		}
		objectID, err := lockObject(ctx, tx, dstBucketID, dstKey, true)
		if err != nil {
			return err
		}
		if err := checkCondition(ctx, tx, objectID, cond); err != nil {
			return err
		}
		res.VersionID, err = addVersion(ctx, tx, objectID, versioning, &newManifest, v)
		return err
	})
	return copied, res, err
}

// ErrCopyFromDeleteMarker means a copy named a delete marker as its source.
var ErrCopyFromDeleteMarker = errors.New("ledger: copy source is a delete marker")

// GetObjectTags returns a version's tags (the current version when versionID is empty).
func (l *Ledger) GetObjectTags(ctx context.Context, bucket, key, versionID string) (map[string]string, string, error) {
	o, err := l.GetObject(ctx, bucket, key, versionID)
	if err != nil {
		if o != nil && o.IsDeleteMarker && versionID != "" {
			return nil, "", ErrCopyFromDeleteMarker
		}
		return nil, "", err
	}
	return o.Tags, o.VersionID, nil
}

// SetObjectTags replaces a version's tags and returns the version ID.
func (l *Ledger) SetObjectTags(ctx context.Context, bucket, key, versionID string, tags map[string]string) (string, error) {
	if tags == nil {
		tags = map[string]string{}
	}
	cond := "v.id = o.current_version_id"
	args := []any{bucket, key, tags}
	if versionID != "" {
		cond = "v.version_id = $4"
		args = append(args, versionID)
	}
	var vid string
	err := l.pool.QueryRow(ctx, `UPDATE object_versions v SET tags = $3
		FROM objects o JOIN buckets b ON b.id = o.bucket_id
		WHERE v.object_id = o.id AND b.name = $1 AND o.key = $2 AND NOT v.is_delete_marker AND `+cond+`
		RETURNING v.version_id`, args...).Scan(&vid)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, berr := l.HeadBucket(ctx, bucket); berr != nil {
			return "", berr
		}
		if versionID != "" {
			return "", ErrNoSuchVersion
		}
		return "", ErrNoSuchKey
	}
	return vid, err
}

// BucketTags returns a bucket's tags; nil means none were ever set.
func (l *Ledger) BucketTags(ctx context.Context, bucket string) (map[string]string, error) {
	var tags map[string]string
	err := l.pool.QueryRow(ctx, "SELECT tags FROM buckets WHERE name = $1", bucket).Scan(&tags)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchBucket
	}
	return tags, err
}

// SetBucketTags replaces (nil deletes) a bucket's tags.
func (l *Ledger) SetBucketTags(ctx context.Context, bucket string, tags map[string]string) error {
	tag, err := l.pool.Exec(ctx, "UPDATE buckets SET tags = $2 WHERE name = $1", bucket, tags)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNoSuchBucket
	}
	return err
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
