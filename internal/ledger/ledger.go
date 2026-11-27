// Package ledger is Lode's metadata service, backed by PostgreSQL. It is the
// source of truth for buckets, objects and the manifests that map object
// versions to nuggets.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/nugget"
)

var (
	ErrNoSuchBucket    = errors.New("ledger: no such bucket")
	ErrBucketExists    = errors.New("ledger: bucket already exists")
	ErrBucketNotEmpty  = errors.New("ledger: bucket not empty")
	ErrNoSuchKey       = errors.New("ledger: no such key")
	errUniqueViolation = "23505"
)

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

// NuggetRef is one entry of a manifest.
type NuggetRef struct {
	ID     nugget.ID
	Length int64
}

// Version is the metadata of one object version.
type Version struct {
	Size              int64
	ETag              string
	ChecksumAlgorithm string
	ChecksumValue     string
	Headers           map[string]string
	ChunkSize         int
	Nuggets           []NuggetRef
	LastModified      time.Time
}

// Object is a key with its current version.
type Object struct {
	Key string
	Version
}

// ObjectSummary is a listing entry.
type ObjectSummary struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

type Ledger struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

// Ping checks connectivity.
func (l *Ledger) Ping(ctx context.Context) error { return l.pool.Ping(ctx) }

func (l *Ledger) CreateBucket(ctx context.Context, name string) error {
	_, err := l.pool.Exec(ctx, "INSERT INTO buckets (name) VALUES ($1)", name)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == errUniqueViolation {
		return ErrBucketExists
	}
	return err
}

func (l *Ledger) HeadBucket(ctx context.Context, name string) (Bucket, error) {
	b := Bucket{Name: name}
	err := l.pool.QueryRow(ctx, "SELECT created_at FROM buckets WHERE name = $1", name).Scan(&b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, ErrNoSuchBucket
	}
	return b, err
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
		_, err = tx.Exec(ctx, "DELETE FROM buckets WHERE id = $1", id)
		return err
	})
}

func (l *Ledger) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := l.pool.Query(ctx, "SELECT name, created_at FROM buckets ORDER BY name")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Bucket, error) {
		var b Bucket
		return b, r.Scan(&b.Name, &b.CreatedAt)
	})
}

// bucketID locks the bucket row for share so it cannot be deleted mid-write.
func bucketID(ctx context.Context, tx pgx.Tx, name string) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx, "SELECT id FROM buckets WHERE name = $1 FOR SHARE", name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNoSuchBucket
	}
	return id, err
}

// dropVersion deletes a version and its manifest, returning the manifest's
// nuggets so the caller can release them on the veins.
func dropVersion(ctx context.Context, tx pgx.Tx, versionID int64) ([]nugget.ID, error) {
	var manifestID int64
	if err := tx.QueryRow(ctx, "DELETE FROM object_versions WHERE id = $1 RETURNING manifest_id", versionID).Scan(&manifestID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, "DELETE FROM manifest_nuggets WHERE manifest_id = $1 RETURNING nugget", manifestID)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (nugget.ID, error) {
		var b []byte
		if err := r.Scan(&b); err != nil {
			return nugget.ID{}, err
		}
		return nugget.FromBytes(b)
	})
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, "DELETE FROM manifests WHERE id = $1", manifestID)
	return ids, err
}

// PutObject commits v as the current version of key. The version becomes
// visible atomically. It returns the nuggets of any version it replaced.
func (l *Ledger) PutObject(ctx context.Context, bucket, key string, v Version) (released []nugget.ID, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		bid, err := bucketID(ctx, tx, bucket)
		if err != nil {
			return err
		}
		var manifestID int64
		if err := tx.QueryRow(ctx, "INSERT INTO manifests (chunk_size) VALUES ($1) RETURNING id", v.ChunkSize).Scan(&manifestID); err != nil {
			return err
		}
		if len(v.Nuggets) > 0 {
			rows := make([][]any, len(v.Nuggets))
			for i, n := range v.Nuggets {
				rows[i] = []any{manifestID, i, n.ID[:], n.Length}
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"manifest_nuggets"},
				[]string{"manifest_id", "seq", "nugget", "length"}, pgx.CopyFromRows(rows)); err != nil {
				return err
			}
		}

		var objectID int64
		var oldVersion *int64
		err = tx.QueryRow(ctx, `
			INSERT INTO objects (bucket_id, key) VALUES ($1, $2)
			ON CONFLICT (bucket_id, key) DO UPDATE SET key = EXCLUDED.key
			RETURNING id, current_version_id`, bid, key).Scan(&objectID, &oldVersion)
		if err != nil {
			return err
		}
		headers := v.Headers
		if headers == nil {
			headers = map[string]string{}
		}
		var versionID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO object_versions (object_id, manifest_id, size, etag, checksum_algorithm, checksum_value, headers)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7) RETURNING id`,
			objectID, manifestID, v.Size, v.ETag, v.ChecksumAlgorithm, v.ChecksumValue, headers).Scan(&versionID)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE objects SET current_version_id = $1 WHERE id = $2", versionID, objectID); err != nil {
			return err
		}
		if oldVersion != nil {
			released, err = dropVersion(ctx, tx, *oldVersion)
		}
		return err
	})
	return released, err
}

// GetObject returns key's current version and its manifest.
func (l *Ledger) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	tx, err := l.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	o := &Object{Key: key}
	var manifestID int64
	var algo, sum *string
	err = tx.QueryRow(ctx, `
		SELECT v.manifest_id, v.size, v.etag, v.checksum_algorithm, v.checksum_value, v.headers, v.created_at, m.chunk_size
		FROM buckets b
		JOIN objects o ON o.bucket_id = b.id
		JOIN object_versions v ON v.id = o.current_version_id
		JOIN manifests m ON m.id = v.manifest_id
		WHERE b.name = $1 AND o.key = $2`, bucket, key).
		Scan(&manifestID, &o.Size, &o.ETag, &algo, &sum, &o.Headers, &o.LastModified, &o.ChunkSize)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, berr := l.HeadBucket(ctx, bucket); errors.Is(berr, ErrNoSuchBucket) {
			return nil, ErrNoSuchBucket
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
	rows, err := tx.Query(ctx, "SELECT nugget, length FROM manifest_nuggets WHERE manifest_id = $1 ORDER BY seq", manifestID)
	if err != nil {
		return nil, err
	}
	o.Nuggets, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (NuggetRef, error) {
		var b []byte
		var n NuggetRef
		if err := r.Scan(&b, &n.Length); err != nil {
			return n, err
		}
		id, err := nugget.FromBytes(b)
		n.ID = id
		return n, err
	})
	return o, err
}

// DeleteObject removes key. Deleting a missing key is not an error, matching
// S3. It returns the nuggets that are no longer referenced by the key.
func (l *Ledger) DeleteObject(ctx context.Context, bucket, key string) (released []nugget.ID, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		bid, err := bucketID(ctx, tx, bucket)
		if err != nil {
			return err
		}
		var objectID int64
		var current *int64
		err = tx.QueryRow(ctx, "SELECT id, current_version_id FROM objects WHERE bucket_id = $1 AND key = $2 FOR UPDATE",
			bid, key).Scan(&objectID, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "UPDATE objects SET current_version_id = NULL WHERE id = $1", objectID); err != nil {
			return err
		}
		if current != nil {
			if released, err = dropVersion(ctx, tx, *current); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, "DELETE FROM objects WHERE id = $1", objectID)
		return err
	})
	return released, err
}

// ListObjects returns up to limit objects in bucket whose key starts with
// prefix and sorts after after, in key order.
func (l *Ledger) ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]ObjectSummary, error) {
	if _, err := l.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := l.pool.Query(ctx, `
		SELECT o.key, v.size, v.etag, v.created_at
		FROM buckets b
		JOIN objects o ON o.bucket_id = b.id
		JOIN object_versions v ON v.id = o.current_version_id
		WHERE b.name = $1 AND o.key > $2 AND starts_with(o.key, $3)
		ORDER BY o.key
		LIMIT $4`, bucket, after, prefix, limit)
	if err != nil {
		return nil, fmt.Errorf("ledger: list: %w", err)
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ObjectSummary, error) {
		var s ObjectSummary
		return s, r.Scan(&s.Key, &s.Size, &s.ETag, &s.LastModified)
	})
}
