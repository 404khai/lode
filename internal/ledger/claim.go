package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Quota scopes.
const (
	ScopeBucket = "bucket"
	ScopeKey    = "key"
)

// Quota is a limit on a bucket or access key. Nil means unlimited.
type Quota struct {
	Scope, Subject string
	MaxBytes       *int64
	MaxObjects     *int64
}

// Usage is what a bucket or access key currently stores (current and
// noncurrent versions; delete markers do not count).
type Usage struct {
	Scope, Subject string
	Bytes, Objects int64
}

func checkScope(scope string) error {
	if scope != ScopeBucket && scope != ScopeKey {
		return fmt.Errorf("ledger: quota scope must be %q or %q", ScopeBucket, ScopeKey)
	}
	return nil
}

// SetQuota creates or replaces a quota; both limits nil removes it.
func (l *Ledger) SetQuota(ctx context.Context, q Quota) error {
	if err := checkScope(q.Scope); err != nil {
		return err
	}
	if q.Scope == ScopeBucket {
		if _, err := l.HeadBucket(ctx, q.Subject); err != nil {
			return err
		}
	}
	if q.MaxBytes == nil && q.MaxObjects == nil {
		_, err := l.pool.Exec(ctx, "DELETE FROM quotas WHERE scope = $1 AND subject = $2", q.Scope, q.Subject)
		return err
	}
	_, err := l.pool.Exec(ctx, `INSERT INTO quotas (scope, subject, max_bytes, max_objects) VALUES ($1, $2, $3, $4)
		ON CONFLICT (scope, subject) DO UPDATE SET max_bytes = EXCLUDED.max_bytes, max_objects = EXCLUDED.max_objects`,
		q.Scope, q.Subject, q.MaxBytes, q.MaxObjects)
	return err
}

// Quotas lists every quota with its subject's usage.
func (l *Ledger) Quotas(ctx context.Context) ([]Quota, []Usage, error) {
	rows, err := l.pool.Query(ctx, `SELECT q.scope, q.subject, q.max_bytes, q.max_objects,
		coalesce(u.bytes, 0), coalesce(u.objects, 0)
		FROM quotas q LEFT JOIN usage_counters u ON u.scope = q.scope AND u.subject = q.subject
		ORDER BY q.scope, q.subject`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var qs []Quota
	var us []Usage
	for rows.Next() {
		var q Quota
		var u Usage
		if err := rows.Scan(&q.Scope, &q.Subject, &q.MaxBytes, &q.MaxObjects, &u.Bytes, &u.Objects); err != nil {
			return nil, nil, err
		}
		u.Scope, u.Subject = q.Scope, q.Subject
		qs, us = append(qs, q), append(us, u)
	}
	return qs, us, rows.Err()
}

// GetUsage returns a subject's usage.
func (l *Ledger) GetUsage(ctx context.Context, scope, subject string) (Usage, error) {
	u := Usage{Scope: scope, Subject: subject}
	err := l.pool.QueryRow(ctx, "SELECT bytes, objects FROM usage_counters WHERE scope = $1 AND subject = $2", scope, subject).
		Scan(&u.Bytes, &u.Objects)
	if errors.Is(err, pgx.ErrNoRows) {
		return u, nil
	}
	return u, err
}

// QuotaHeadroom reports whether adding bytes (and one object) fits both
// the bucket's and the key's quotas right now. Adit calls it before reading
// a body so an over-quota upload is refused without transferring it; the
// commit re-checks under lock.
//
// In a bucket without versioning an overwrite of key replaces its null
// version, so that version's size and count are credited back.
func (l *Ledger) QuotaHeadroom(ctx context.Context, bucket, accessKey, key string, bytes int64) error {
	var over bool
	err := l.pool.QueryRow(ctx, `
		WITH replaced AS (
			SELECT coalesce(sum(v.size), 0) AS size, count(v.id) AS n, coalesce(max(v.access_key), '') AS owner
			FROM buckets b JOIN objects o ON o.bucket_id = b.id
			JOIN object_versions v ON v.object_id = o.id AND v.version_id = 'null' AND NOT v.is_delete_marker
			WHERE b.name = $1 AND o.key = $4 AND b.versioning <> 'Enabled')
		SELECT EXISTS (
			SELECT 1 FROM quotas q LEFT JOIN usage_counters u ON u.scope = q.scope AND u.subject = q.subject, replaced r
			WHERE ((q.scope = 'bucket' AND q.subject = $1) OR (q.scope = 'key' AND q.subject = $2))
			  AND ((q.max_bytes IS NOT NULL AND coalesce(u.bytes, 0) + $3
			        - CASE WHEN q.scope = 'bucket' OR r.owner = $2 THEN r.size ELSE 0 END > q.max_bytes)
			    OR (q.max_objects IS NOT NULL AND coalesce(u.objects, 0) + 1
			        - CASE WHEN q.scope = 'bucket' OR r.owner = $2 THEN r.n ELSE 0 END > q.max_objects)))`,
		bucket, accessKey, bytes, key).Scan(&over)
	if err == nil && over {
		return ErrQuotaExceeded
	}
	return err
}

// ReconcileUsage recomputes every usage counter from object_versions,
// correcting any drift. It returns how many counters changed.
func (l *Ledger) ReconcileUsage(ctx context.Context) (int64, error) {
	var changed int64
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "LOCK TABLE usage_counters IN EXCLUSIVE MODE"); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			WITH truth AS (
				SELECT 'bucket' AS scope, b.name AS subject, coalesce(sum(v.size), 0) AS bytes, count(v.id) AS objects
				FROM buckets b LEFT JOIN objects o ON o.bucket_id = b.id
				LEFT JOIN object_versions v ON v.object_id = o.id AND NOT v.is_delete_marker
				GROUP BY b.name
				UNION ALL
				SELECT 'key', v.access_key, sum(v.size), count(*) FROM object_versions v
				WHERE NOT v.is_delete_marker AND v.access_key <> '' GROUP BY v.access_key
			)
			INSERT INTO usage_counters (scope, subject, bytes, objects) SELECT scope, subject, bytes, objects FROM truth
			ON CONFLICT (scope, subject) DO UPDATE SET bytes = EXCLUDED.bytes, objects = EXCLUDED.objects
			WHERE usage_counters.bytes <> EXCLUDED.bytes OR usage_counters.objects <> EXCLUDED.objects`)
		if err != nil {
			return err
		}
		changed = tag.RowsAffected()
		// Subjects with no versions left.
		tag, err = tx.Exec(ctx, `UPDATE usage_counters u SET bytes = 0, objects = 0
			WHERE (u.bytes <> 0 OR u.objects <> 0) AND NOT EXISTS (
				SELECT 1 FROM object_versions v JOIN objects o ON o.id = v.object_id JOIN buckets b ON b.id = o.bucket_id
				WHERE NOT v.is_delete_marker AND ((u.scope = 'bucket' AND b.name = u.subject) OR (u.scope = 'key' AND v.access_key = u.subject)))`)
		changed += tag.RowsAffected()
		return err
	})
	return changed, err
}
