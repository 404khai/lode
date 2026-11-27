package ledger

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/404khai/lode/internal/nugget"
)

// ReclaimCandidates returns unreferenced nuggets last touched before cutoff.
func (l *Ledger) ReclaimCandidates(ctx context.Context, cutoff time.Time, limit int) ([]nugget.ID, error) {
	rows, err := l.pool.Query(ctx, "SELECT id FROM nuggets WHERE refs = 0 AND last_touched < $1 ORDER BY last_touched LIMIT $2", cutoff, limit)
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

// ReclaimNugget deletes id if it is still unreferenced and untouched since
// cutoff. The nugget row stays locked while remove runs and is deleted only
// after remove succeeds, so a concurrent upload touching the same nugget
// waits, then re-creates the row and rewrites the bytes. It reports whether
// the nugget was reclaimed.
func (l *Ledger) ReclaimNugget(ctx context.Context, id nugget.ID, cutoff time.Time, remove func() error) (bool, error) {
	reclaimed := false
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var refs int64
		var touched time.Time
		err := tx.QueryRow(ctx, "SELECT refs, last_touched FROM nuggets WHERE id = $1 FOR UPDATE", id[:]).Scan(&refs, &touched)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if refs != 0 || !touched.Before(cutoff) {
			return nil
		}
		if err := remove(); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM nuggets WHERE id = $1", id[:]); err != nil {
			return err
		}
		reclaimed = true
		return nil
	})
	return reclaimed && err == nil, err
}

// NuggetRefs returns the Ledger's reference count for id (0 if unknown).
func (l *Ledger) NuggetRefs(ctx context.Context, id nugget.ID) (int64, error) {
	var refs int64
	err := l.pool.QueryRow(ctx, "SELECT refs FROM nuggets WHERE id = $1", id[:]).Scan(&refs)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return refs, err
}

// ExpiredUpload identifies a multipart upload to abort.
type ExpiredUpload struct {
	Bucket, Key, UploadID string
}

// ExpiredUploads returns uploads initiated before cutoff.
func (l *Ledger) ExpiredUploads(ctx context.Context, cutoff time.Time, limit int) ([]ExpiredUpload, error) {
	rows, err := l.pool.Query(ctx, `SELECT b.name, u.key, u.upload_id FROM multipart_uploads u JOIN buckets b ON b.id = u.bucket_id
		WHERE u.created_at < $1 AND u.completed_at IS NULL ORDER BY u.created_at LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ExpiredUpload, error) {
		var u ExpiredUpload
		return u, r.Scan(&u.Bucket, &u.Key, &u.UploadID)
	})
}

// PurgeCompletedUploads removes records of uploads completed before cutoff.
func (l *Ledger) PurgeCompletedUploads(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := l.pool.Exec(ctx, "DELETE FROM multipart_uploads WHERE completed_at < $1", cutoff)
	return tag.RowsAffected(), err
}

// TryLeader takes a session-level advisory lock on a dedicated connection.
// ok is false if another process holds it. release frees the lock.
func (l *Ledger) TryLeader(ctx context.Context, key int64) (release func(), ok bool, err error) {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&ok); err != nil || !ok {
		conn.Release()
		return nil, false, err
	}
	return func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		conn.Release()
	}, true, nil
}
