package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/404khai/lode/internal/nugget"
)

// Vein states.
const (
	VeinHealthy  = "healthy"
	VeinSuspect  = "suspect"
	VeinDown     = "down"
	VeinDraining = "draining"
	VeinRemoved  = "removed"
)

// Repair reasons.
const (
	ReasonVeinDown        = "vein_down"
	ReasonUnderReplicated = "under_replicated"
	ReasonCorrupt         = "corrupt"
	ReasonDrain           = "drain"
)

// RepairJob is one unit of Smelter work.
type RepairJob struct {
	ID       int64
	Nugget   nugget.ID
	Reason   string
	BadVeins []string
	Attempts int
}

// tolerance is how many more losses nugget n survives on healthy veins:
// shards beyond k for erasure-coded nuggets, copies beyond one otherwise.
const toleranceSQL = `(CASE WHEN n.ec_data > 0 THEN
	(SELECT count(DISTINCT s.idx) FROM nugget_shards s JOIN veins v ON v.id = s.vein_id
		WHERE s.nugget = n.id AND v.state = 'healthy') - n.ec_data
	ELSE (SELECT count(*) FROM nugget_replicas r JOIN veins v ON v.id = r.vein_id
		WHERE r.nugget = n.id AND v.state = 'healthy') - 1 END)`

// wantedToleranceSQL is the tolerance nugget n should have.
const wantedToleranceSQL = `(CASE WHEN n.ec_data > 0 THEN n.ec_parity ELSE greatest(n.replication, 1) - 1 END)`

// EnqueueRepair queues a repair for id unless one is already open (badVein,
// if set, is added to the open job's list of veins whose copy must be
// replaced) or the nugget was already declared lost, which needs an operator.
// id may be a shard ID; it is resolved to its nugget.
func (l *Ledger) EnqueueRepair(ctx context.Context, id nugget.ID, reason, badVein string) error {
	bad := []string{}
	if badVein != "" {
		bad = append(bad, badVein)
	}
	_, err := l.pool.Exec(ctx, `
		WITH t AS (SELECT coalesce((SELECT nugget FROM nugget_shards WHERE shard = $1 LIMIT 1), $1::bytea) AS id)
		INSERT INTO repair_jobs (nugget, reason, bad_veins, priority)
		SELECT t.id, $2, $3, coalesce((SELECT `+toleranceSQL+` FROM nuggets n WHERE n.id = t.id), 0) FROM t
		WHERE NOT EXISTS (SELECT 1 FROM repair_jobs WHERE nugget = t.id AND state = 'lost')
		ON CONFLICT (nugget) WHERE state IN ('queued', 'leased') DO UPDATE
			SET bad_veins = (SELECT array_agg(DISTINCT b) FROM unnest(repair_jobs.bad_veins || EXCLUDED.bad_veins) b),
			    updated_at = now()`, id[:], reason, bad)
	return err
}

// EnqueueVeinRepairs queues repairs for every referenced nugget with a copy
// or shard on vein. It returns how many jobs were added.
func (l *Ledger) EnqueueVeinRepairs(ctx context.Context, vein, reason string) (int64, error) {
	tag, err := l.pool.Exec(ctx, `
		INSERT INTO repair_jobs (nugget, reason, priority)
		SELECT n.id, $2, `+toleranceSQL+`
		FROM nuggets n
		WHERE n.refs > 0 AND n.id IN (SELECT nugget FROM nugget_replicas WHERE vein_id = $1
			UNION SELECT nugget FROM nugget_shards WHERE vein_id = $1)
		ON CONFLICT (nugget) WHERE state IN ('queued', 'leased') DO NOTHING`, vein, reason)
	return tag.RowsAffected(), err
}

// EnqueueUnderReplicated queues repairs for referenced nuggets with less
// fault tolerance on healthy veins than they want. It is the safety net that
// catches anything the event-driven paths missed.
func (l *Ledger) EnqueueUnderReplicated(ctx context.Context, limit int) (int64, error) {
	tag, err := l.pool.Exec(ctx, `
		INSERT INTO repair_jobs (nugget, reason, priority)
		SELECT id, 'under_replicated', tol FROM (
			SELECT n.id, `+toleranceSQL+` AS tol, `+wantedToleranceSQL+` AS want
			FROM nuggets n WHERE n.refs > 0
			  AND NOT EXISTS (SELECT 1 FROM repair_jobs j WHERE j.nugget = n.id AND j.state IN ('queued', 'leased', 'lost'))
		) x WHERE tol < want
		LIMIT $1
		ON CONFLICT (nugget) WHERE state IN ('queued', 'leased') DO NOTHING`, limit)
	return tag.RowsAffected(), err
}

// LeaseRepair claims the most urgent runnable job for owner, or returns nil.
func (l *Ledger) LeaseRepair(ctx context.Context, owner string, lease time.Duration) (*RepairJob, error) {
	var j RepairJob
	var b []byte
	err := l.pool.QueryRow(ctx, `
		UPDATE repair_jobs SET state = 'leased', lease_owner = $1, lease_until = now() + $2::interval,
			attempts = attempts + 1, updated_at = now()
		WHERE id = (
			SELECT id FROM repair_jobs
			WHERE (state = 'queued' OR (state = 'leased' AND lease_until < now())) AND not_before <= now()
			ORDER BY priority, created_at
			FOR UPDATE SKIP LOCKED LIMIT 1)
		RETURNING id, nugget, reason, bad_veins, attempts`, owner, lease.String()).
		Scan(&j.ID, &b, &j.Reason, &j.BadVeins, &j.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j.Nugget, err = nugget.FromBytes(b)
	return &j, err
}

// FinishRepair closes a job as done (or lost when no copy could be read).
func (l *Ledger) FinishRepair(ctx context.Context, id int64, owner, state, msg string) error {
	_, err := l.pool.Exec(ctx, `UPDATE repair_jobs SET state = $3, last_error = NULLIF($4, ''), lease_owner = NULL,
		lease_until = NULL, updated_at = now() WHERE id = $1 AND lease_owner = $2`, id, owner, state, msg)
	return err
}

// RetryRepair returns a job to the queue after backoff.
func (l *Ledger) RetryRepair(ctx context.Context, id int64, owner, msg string, backoff time.Duration) error {
	_, err := l.pool.Exec(ctx, `UPDATE repair_jobs SET state = 'queued', last_error = $3, lease_owner = NULL,
		lease_until = NULL, not_before = now() + $4::interval, updated_at = now() WHERE id = $1 AND lease_owner = $2`,
		id, owner, msg, backoff.String())
	return err
}

// RepairStats summarises the repair queue and redundancy.
type RepairStats struct {
	Queued, Leased, Lost int64
	Runnable             int64 // queued jobs not waiting out a backoff (e.g. for capacity)
	UnderReplicated      int64 // referenced nuggets with less fault tolerance on healthy veins than wanted
}

func (l *Ledger) RepairStats(ctx context.Context) (RepairStats, error) {
	var s RepairStats
	err := l.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state = 'queued'), count(*) FILTER (WHERE state = 'leased'), count(*) FILTER (WHERE state = 'lost'),
		count(*) FILTER (WHERE state = 'queued' AND not_before <= now())
		FROM repair_jobs`).Scan(&s.Queued, &s.Leased, &s.Lost, &s.Runnable)
	if err != nil {
		return s, err
	}
	err = l.pool.QueryRow(ctx, `SELECT count(*) FROM nuggets n
		WHERE n.refs > 0 AND `+toleranceSQL+` < `+wantedToleranceSQL).Scan(&s.UnderReplicated)
	return s, err
}

// NuggetState is what Smelter needs to repair a nugget.
type NuggetState struct {
	Length           int64
	Refs             int64
	Replication      int
	Replicas         []string
	ECData, ECParity int
	Shards           []ShardLoc
}

// Nugget returns a nugget's state; ok is false if the Ledger has no record.
func (l *Ledger) Nugget(ctx context.Context, id nugget.ID) (st NuggetState, ok bool, err error) {
	err = l.pool.QueryRow(ctx, `SELECT n.length, n.refs, n.replication, n.ec_data, n.ec_parity
		FROM nuggets n WHERE n.id = $1`, id[:]).Scan(&st.Length, &st.Refs, &st.Replication, &st.ECData, &st.ECParity)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, false, nil
	}
	if err != nil {
		return st, false, err
	}
	if st.Replicas, err = replicasOf(ctx, l.pool, id); err != nil {
		return st, false, err
	}
	st.Shards, err = shardsOf(ctx, l.pool, id)
	return st, err == nil, err
}

// RemoveReplica forgets vein's copy of id.
func (l *Ledger) RemoveReplica(ctx context.Context, id nugget.ID, vein string) error {
	_, err := l.pool.Exec(ctx, "DELETE FROM nugget_replicas WHERE nugget = $1 AND vein_id = $2", id[:], vein)
	return err
}

// SetVeinState changes a vein's state and reports the previous one.
func (l *Ledger) SetVeinState(ctx context.Context, id, state string) (prev string, err error) {
	err = l.pool.QueryRow(ctx, `UPDATE veins v SET state = $2, state_changed = now()
		FROM (SELECT id, state FROM veins WHERE id = $1 FOR UPDATE) old
		WHERE v.id = old.id RETURNING old.state`, id, state).Scan(&prev)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNoSuchVein
	}
	return prev, err
}

// ErrNoSuchVein means the vein is not registered.
var ErrNoSuchVein = errors.New("ledger: no such vein")

// VeinReplicaCount reports how many copies of referenced nuggets the Ledger
// records on vein.
func (l *Ledger) VeinReplicaCount(ctx context.Context, vein string) (int64, error) {
	var n int64
	err := l.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM nugget_replicas r JOIN nuggets n ON n.id = r.nugget WHERE r.vein_id = $1 AND n.refs > 0) +
		(SELECT count(*) FROM nugget_shards s JOIN nuggets n ON n.id = s.nugget WHERE s.vein_id = $1 AND n.refs > 0)`, vein).Scan(&n)
	return n, err
}

// ErrVeinHoldsLastCopy means removing a vein would leave a referenced nugget
// without any copy on another non-removed vein.
var ErrVeinHoldsLastCopy = errors.New("ledger: vein holds the last copy of referenced nuggets")

// RemoveVein retires a vein and forgets every copy recorded on it. Unless
// force is set it refuses while the vein holds the only copy of any
// referenced nugget.
func (l *Ledger) RemoveVein(ctx context.Context, id string, force bool) error {
	return pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM veins WHERE id = $1 FOR UPDATE)", id).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNoSuchVein
		}
		if !force {
			var orphans int64
			if err := tx.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM nugget_replicas r JOIN nuggets n ON n.id = r.nugget
					WHERE r.vein_id = $1 AND n.refs > 0 AND NOT EXISTS (
						SELECT 1 FROM nugget_replicas o JOIN veins v ON v.id = o.vein_id
						WHERE o.nugget = r.nugget AND o.vein_id <> $1 AND v.state <> 'removed')) +
				(SELECT count(*) FROM nuggets n WHERE n.refs > 0 AND n.ec_data > 0
					AND n.id IN (SELECT nugget FROM nugget_shards WHERE vein_id = $1)
					AND (SELECT count(DISTINCT s.idx) FROM nugget_shards s JOIN veins v ON v.id = s.vein_id
						WHERE s.nugget = n.id AND s.vein_id <> $1 AND v.state <> 'removed') < n.ec_data)`, id).Scan(&orphans); err != nil {
				return err
			}
			if orphans > 0 {
				return fmt.Errorf("%w (%d nuggets)", ErrVeinHoldsLastCopy, orphans)
			}
		}
		if _, err := tx.Exec(ctx, "DELETE FROM nugget_replicas WHERE vein_id = $1", id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM nugget_shards WHERE vein_id = $1", id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "UPDATE veins SET state = 'removed', state_changed = now() WHERE id = $1", id)
		return err
	})
}

// NuggetEntry is a referenced nugget with its storage state.
type NuggetEntry struct {
	ID nugget.ID
	NuggetState
}

// ListNuggets pages through referenced nuggets in ID order after `after`.
func (l *Ledger) ListNuggets(ctx context.Context, after nugget.ID, limit int) ([]NuggetEntry, error) {
	rows, err := l.pool.Query(ctx, `
		SELECT n.id, n.length, n.refs, n.replication, n.ec_data, n.ec_parity,
			coalesce((SELECT array_agg(vein_id ORDER BY vein_id) FROM nugget_replicas WHERE nugget = n.id), '{}')
		FROM nuggets n WHERE n.refs > 0 AND n.id > $1 ORDER BY n.id LIMIT $2`, after[:], limit)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (NuggetEntry, error) {
		var e NuggetEntry
		var b []byte
		if err := r.Scan(&b, &e.Length, &e.Refs, &e.Replication, &e.ECData, &e.ECParity, &e.Replicas); err != nil {
			return e, err
		}
		e.ID, err = nugget.FromBytes(b)
		return e, err
	})
	if err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].ECData > 0 {
			if out[i].Shards, err = shardsOf(ctx, l.pool, out[i].ID); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
