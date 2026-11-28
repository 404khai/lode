package ledger

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/404khai/lode/internal/nugget"
)

// LifecycleRule is one parsed S3 lifecycle rule (the subset Strata runs).
type LifecycleRule struct {
	ID      string            `json:"id"`
	Enabled bool              `json:"enabled"`
	Prefix  string            `json:"prefix,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`

	ExpirationDays            int          `json:"expiration_days,omitempty"`
	ExpirationDate            *time.Time   `json:"expiration_date,omitempty"`
	ExpiredObjectDeleteMarker bool         `json:"expired_object_delete_marker,omitempty"`
	NoncurrentDays            int          `json:"noncurrent_days,omitempty"`
	NewerNoncurrentVersions   int          `json:"newer_noncurrent_versions,omitempty"`
	AbortIncompleteDays       int          `json:"abort_incomplete_days,omitempty"`
	Transitions               []Transition `json:"transitions,omitempty"`
}

// Transition moves current versions to a storage class after Days.
type Transition struct {
	Days  int    `json:"days"`
	Class string `json:"class"` // STANDARD (replicated) or EC_K_M
	// Parsed form of Class.
	ECData   int `json:"ec_data,omitempty"`
	ECParity int `json:"ec_parity,omitempty"`
}

// ErrNoLifecycle means a bucket has no lifecycle configuration.
var ErrNoLifecycle = errors.New("ledger: no lifecycle configuration")

// SetLifecycle replaces (nil deletes) a bucket's lifecycle rules.
func (l *Ledger) SetLifecycle(ctx context.Context, bucket string, rules []LifecycleRule) error {
	var v any
	if rules != nil {
		v = rules
	}
	tag, err := l.pool.Exec(ctx, "UPDATE buckets SET lifecycle = $2 WHERE name = $1", bucket, v)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNoSuchBucket
	}
	return err
}

// Lifecycle returns a bucket's rules.
func (l *Ledger) Lifecycle(ctx context.Context, bucket string) ([]LifecycleRule, error) {
	var rules []LifecycleRule
	var exists bool
	err := l.pool.QueryRow(ctx, "SELECT lifecycle IS NOT NULL, coalesce(lifecycle, '[]') FROM buckets WHERE name = $1", bucket).
		Scan(&exists, &rules)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoSuchBucket
	}
	if err == nil && !exists {
		return nil, ErrNoLifecycle
	}
	return rules, err
}

// LifecycleBuckets lists buckets that have lifecycle rules.
func (l *Ledger) LifecycleBuckets(ctx context.Context) (map[string][]LifecycleRule, error) {
	rows, err := l.pool.Query(ctx, "SELECT name, lifecycle FROM buckets WHERE lifecycle IS NOT NULL ORDER BY name")
	if err != nil {
		return nil, err
	}
	out := map[string][]LifecycleRule{}
	for rows.Next() {
		var name string
		var rules []LifecycleRule
		if err := rows.Scan(&name, &rules); err != nil {
			rows.Close()
			return nil, err
		}
		out[name] = rules
	}
	return out, rows.Err()
}

// VersionInfo is a version considered by Strata.
type VersionInfo struct {
	Key, VersionID string
	Tags           map[string]string
	Size           int64
	IsCurrent      bool
	IsDeleteMarker bool
	Created        time.Time
	// NoncurrentSince is when a newer version replaced this one.
	NoncurrentSince *time.Time
	// NoncurrentRank counts newer noncurrent versions (0 = newest noncurrent).
	NoncurrentRank int
	OnlyVersion    bool
	ManifestID     *int64
}

// LifecycleVersions returns versions in bucket under prefix after the
// (key, versionID) cursor, with the facts lifecycle rules need.
func (l *Ledger) LifecycleVersions(ctx context.Context, bucket, prefix, afterKey string, afterSeq int64, limit int) ([]VersionInfo, int64, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT key, version_id, tags, size, is_current, is_delete_marker, created_at, noncurrent_since,
		       noncurrent_rank, only_version, manifest_id, id FROM (
			SELECT o.key, v.version_id, v.tags, v.size, v.id = o.current_version_id AS is_current, v.is_delete_marker,
			       v.created_at, v.manifest_id, v.id,
			       lead(v.created_at) OVER w AS noncurrent_since,
			       (row_number() OVER (PARTITION BY v.object_id ORDER BY v.id DESC)) - 2 AS noncurrent_rank,
			       count(*) OVER (PARTITION BY v.object_id) = 1 AS only_version
			FROM buckets b JOIN objects o ON o.bucket_id = b.id JOIN object_versions v ON v.object_id = o.id
			WHERE b.name = $1 AND starts_with(o.key, $2)
			WINDOW w AS (PARTITION BY v.object_id ORDER BY v.id)
		) x
		WHERE (key, id) > ($3, $4)
		ORDER BY key, id
		LIMIT $5`, bucket, prefix, afterKey, afterSeq, limit)
	if err != nil {
		return nil, 0, err
	}
	var last int64
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (VersionInfo, error) {
		var v VersionInfo
		err := r.Scan(&v.Key, &v.VersionID, &v.Tags, &v.Size, &v.IsCurrent, &v.IsDeleteMarker, &v.Created,
			&v.NoncurrentSince, &v.NoncurrentRank, &v.OnlyVersion, &v.ManifestID, &last)
		return v, err
	})
	return out, last, err
}

// VersionNuggets returns the nuggets of a manifest (for transitions).
func (l *Ledger) VersionNuggets(ctx context.Context, manifestID int64) ([]nugget.ID, error) {
	rows, err := l.pool.Query(ctx, "SELECT DISTINCT nugget FROM manifest_nuggets WHERE manifest_id = $1", manifestID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (nugget.ID, error) {
		var b []byte
		if err := r.Scan(&b); err != nil {
			return nugget.ID{}, err
		}
		return nugget.FromBytes(b)
	})
}

// SwitchLayout changes a nugget's storage layout to k+m (0+0 = replicated
// with `replication` copies) after the new copies or shards have been
// written and recorded. The old layout's locations are forgotten and their
// blobs scheduled for deletion after delay, so readers that loaded the old
// layout keep working meanwhile.
func (l *Ledger) SwitchLayout(ctx context.Context, id nugget.ID, k, m, replication int, delay time.Duration) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var oldK int
		if err := tx.QueryRow(ctx, "SELECT ec_data FROM nuggets WHERE id = $1 FOR UPDATE", id[:]).Scan(&oldK); err != nil {
			return err
		}
		var drop string
		if k > 0 {
			drop = `DELETE FROM nugget_replicas WHERE nugget = $1 RETURNING vein_id, nugget AS blob`
		} else {
			drop = `DELETE FROM nugget_shards WHERE nugget = $1 RETURNING vein_id, shard AS blob`
		}
		if _, err := tx.Exec(ctx, `WITH gone AS (`+drop+`)
			INSERT INTO deferred_deletes (vein_id, blob, nugget, not_before)
			SELECT vein_id, gone.blob, $1, now() + $2::interval FROM gone
			ON CONFLICT (vein_id, blob) DO NOTHING`, id[:], delay.String()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE nuggets SET ec_data = $2, ec_parity = $3,
			replication = CASE WHEN $2 = 0 THEN greatest($4, 1) ELSE replication END WHERE id = $1`, id[:], k, m, replication)
		return err
	})
}

// DueDeletes returns deferred deletes whose time has come.
func (l *Ledger) DueDeletes(ctx context.Context, limit int) ([]Location, error) {
	rows, err := l.pool.Query(ctx, "SELECT vein_id, blob FROM deferred_deletes WHERE not_before <= now() ORDER BY not_before LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Location, error) {
		var loc Location
		var b []byte
		if err := r.Scan(&loc.Vein, &b); err != nil {
			return loc, err
		}
		loc.ID, err = nugget.FromBytes(b)
		return loc, err
	})
}

// RunDeferredDelete performs one deferred delete via remove, unless the
// blob's nugget was touched after the delete was scheduled or the blob is
// recorded as live again (a concurrent write reused it). Either way the
// entry is consumed. It reports whether remove ran.
func (l *Ledger) RunDeferredDelete(ctx context.Context, loc Location, remove func() error) (bool, error) {
	ran := false
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var nug []byte
		var created time.Time
		err := tx.QueryRow(ctx, "SELECT nugget, created_at FROM deferred_deletes WHERE vein_id = $1 AND blob = $2 FOR UPDATE",
			loc.Vein, loc.ID[:]).Scan(&nug, &created)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		var touched time.Time
		var live bool
		err = tx.QueryRow(ctx, `SELECT n.last_touched,
			EXISTS (SELECT 1 FROM nugget_replicas WHERE nugget = $3 AND vein_id = $1 AND nugget = $2)
			OR EXISTS (SELECT 1 FROM nugget_shards WHERE nugget = $3 AND vein_id = $1 AND shard = $2)
			FROM nuggets n WHERE n.id = $3 FOR UPDATE`, loc.Vein, loc.ID[:], nug).Scan(&touched, &live)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// The nugget is gone entirely; Tailings already reclaimed it.
		case err != nil:
			return err
		case !live && !touched.After(created):
			if err := remove(); err != nil {
				return err
			}
			ran = true
		}
		_, err = tx.Exec(ctx, "DELETE FROM deferred_deletes WHERE vein_id = $1 AND blob = $2", loc.Vein, loc.ID[:])
		return err
	})
	return ran, err
}

// StaleUploads returns in-progress uploads in bucket under prefix initiated before cutoff.
func (l *Ledger) StaleUploads(ctx context.Context, bucket, prefix string, cutoff time.Time, limit int) ([]ExpiredUpload, error) {
	rows, err := l.pool.Query(ctx, `SELECT b.name, u.key, u.upload_id FROM multipart_uploads u JOIN buckets b ON b.id = u.bucket_id
		WHERE b.name = $1 AND starts_with(u.key, $2) AND u.created_at < $3 AND u.completed_at IS NULL
		ORDER BY u.created_at LIMIT $4`, bucket, prefix, cutoff, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ExpiredUpload, error) {
		var u ExpiredUpload
		return u, r.Scan(&u.Bucket, &u.Key, &u.UploadID)
	})
}
