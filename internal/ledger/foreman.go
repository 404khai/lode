package ledger

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/404khai/lode/internal/nugget"
)

// ClusterTotals summarises what the cluster stores.
type ClusterTotals struct {
	Buckets       int64 `json:"buckets"`
	Objects       int64 `json:"objects"`        // keys whose current version is not a delete marker
	Versions      int64 `json:"versions"`       // stored versions (current and noncurrent)
	LogicalBytes  int64 `json:"logical_bytes"`  // sum of version sizes
	UniqueNuggets int64 `json:"unique_nuggets"` // referenced nuggets after dedup
	UniqueBytes   int64 `json:"unique_bytes"`
}

func (l *Ledger) ClusterTotals(ctx context.Context) (ClusterTotals, error) {
	var t ClusterTotals
	err := l.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM buckets),
		(SELECT count(*) FROM objects o JOIN object_versions v ON v.id = o.current_version_id WHERE NOT v.is_delete_marker),
		(SELECT count(*) FROM object_versions WHERE NOT is_delete_marker),
		(SELECT coalesce(sum(size), 0) FROM object_versions WHERE NOT is_delete_marker),
		(SELECT count(*) FROM nuggets WHERE refs > 0),
		(SELECT coalesce(sum(length), 0) FROM nuggets WHERE refs > 0)`).
		Scan(&t.Buckets, &t.Objects, &t.Versions, &t.LogicalBytes, &t.UniqueNuggets, &t.UniqueBytes)
	return t, err
}

// VeinHoldings counts what a vein stores for referenced nuggets.
type VeinHoldings struct {
	Replicas int64 `json:"replicas"`
	Shards   int64 `json:"shards"`
	Bytes    int64 `json:"bytes"` // logical bytes of the nuggets it holds a piece of
}

func (l *Ledger) VeinHoldings(ctx context.Context, vein string) (VeinHoldings, error) {
	var h VeinHoldings
	err := l.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM nugget_replicas r JOIN nuggets n ON n.id = r.nugget WHERE r.vein_id = $1 AND n.refs > 0),
		(SELECT count(*) FROM nugget_shards s JOIN nuggets n ON n.id = s.nugget WHERE s.vein_id = $1 AND n.refs > 0),
		(SELECT coalesce(sum(n.length), 0) FROM nuggets n WHERE n.refs > 0 AND n.id IN (
			SELECT nugget FROM nugget_replicas WHERE vein_id = $1 UNION SELECT nugget FROM nugget_shards WHERE vein_id = $1))`, vein).
		Scan(&h.Replicas, &h.Shards, &h.Bytes)
	return h, err
}

// AtRisk is an object version that would lose redundancy (or data) if a
// vein died now.
type AtRisk struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id"`
	IsCurrent bool   `json:"is_current"`
	// Tolerance is how many further vein losses the object survives after
	// the hypothetical failure: -1 means data would be unreadable, 0 means
	// one more loss would make it unreadable.
	Tolerance int `json:"tolerance"`
}

// ObjectsAtRisk lists object versions whose weakest nugget would have
// tolerance <= threshold if vein (and every vein not currently healthy)
// were gone. It also reports how many versions were found in total.
func (l *Ledger) ObjectsAtRisk(ctx context.Context, vein string, threshold, limit int) ([]AtRisk, int64, error) {
	const q = `
		WITH affected AS (
			SELECT n.id,
				CASE WHEN n.ec_data > 0 THEN
					(SELECT count(DISTINCT s.idx) FROM nugget_shards s JOIN veins v ON v.id = s.vein_id
						WHERE s.nugget = n.id AND v.state = 'healthy' AND s.vein_id <> $1) - n.ec_data
				ELSE (SELECT count(*) FROM nugget_replicas r JOIN veins v ON v.id = r.vein_id
						WHERE r.nugget = n.id AND v.state = 'healthy' AND r.vein_id <> $1) - 1 END AS tol
			FROM nuggets n
			WHERE n.refs > 0 AND n.id IN (SELECT nugget FROM nugget_replicas WHERE vein_id = $1
				UNION SELECT nugget FROM nugget_shards WHERE vein_id = $1)
		), versions AS (
			SELECT b.name, o.key, v.version_id, v.id = o.current_version_id AS is_current, min(a.tol) AS tol
			FROM affected a
			JOIN manifest_nuggets mn ON mn.nugget = a.id
			JOIN object_versions v ON v.manifest_id = mn.manifest_id
			JOIN objects o ON o.id = v.object_id
			JOIN buckets b ON b.id = o.bucket_id
			WHERE a.tol <= $2
			GROUP BY b.name, o.key, v.version_id, v.id, o.current_version_id
		)
		SELECT name, key, version_id, is_current, tol, count(*) OVER () FROM versions
		ORDER BY tol, name, key LIMIT $3`
	rows, err := l.pool.Query(ctx, q, vein, threshold, limit)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (AtRisk, error) {
		var a AtRisk
		return a, r.Scan(&a.Bucket, &a.Key, &a.VersionID, &a.IsCurrent, &a.Tolerance, &total)
	})
	return out, total, err
}

// RepairJobInfo is a repair job as shown to operators.
type RepairJobInfo struct {
	ID         int64      `json:"id"`
	Nugget     string     `json:"nugget"`
	Reason     string     `json:"reason"`
	State      string     `json:"state"`
	Priority   int        `json:"priority"`
	BadVeins   []string   `json:"bad_veins,omitempty"`
	Attempts   int        `json:"attempts"`
	LastError  string     `json:"last_error,omitempty"`
	LeaseOwner string     `json:"lease_owner,omitempty"`
	NotBefore  time.Time  `json:"not_before"`
	Created    time.Time  `json:"created"`
	Updated    time.Time  `json:"updated"`
	LeaseUntil *time.Time `json:"lease_until,omitempty"`
}

const repairCols = `id, nugget, reason, state, priority, bad_veins, attempts, coalesce(last_error, ''),
	coalesce(lease_owner, ''), not_before, created_at, updated_at, lease_until`

func scanRepair(r pgx.CollectableRow) (RepairJobInfo, error) {
	var j RepairJobInfo
	var b []byte
	if err := r.Scan(&j.ID, &b, &j.Reason, &j.State, &j.Priority, &j.BadVeins, &j.Attempts, &j.LastError,
		&j.LeaseOwner, &j.NotBefore, &j.Created, &j.Updated, &j.LeaseUntil); err != nil {
		return j, err
	}
	id, err := nugget.FromBytes(b)
	j.Nugget = id.String()
	return j, err
}

// RepairJobs lists jobs in the given states ("" = open), most urgent first.
func (l *Ledger) RepairJobs(ctx context.Context, state string, limit int) ([]RepairJobInfo, error) {
	where := "state IN ('queued', 'leased')"
	args := []any{limit}
	if state != "" {
		where = "state = $2"
		args = append(args, state)
	}
	rows, err := l.pool.Query(ctx, "SELECT "+repairCols+" FROM repair_jobs WHERE "+where+" ORDER BY priority, created_at LIMIT $1", args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanRepair)
}

// ErrNoSuchRepair means no job has that ID.
var ErrNoSuchRepair = errors.New("ledger: no such repair job")

// RepairJob returns one job, its position in the queue (0 = next), and how
// many jobs finished in the last five minutes (for an ETA).
func (l *Ledger) RepairJob(ctx context.Context, id int64) (RepairJobInfo, int64, int64, error) {
	rows, err := l.pool.Query(ctx, "SELECT "+repairCols+" FROM repair_jobs WHERE id = $1", id)
	if err != nil {
		return RepairJobInfo{}, 0, 0, err
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanRepair)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, 0, 0, ErrNoSuchRepair
	}
	if err != nil {
		return j, 0, 0, err
	}
	var ahead, recent int64
	err = l.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM repair_jobs WHERE state IN ('queued', 'leased') AND (priority, created_at) < ($1, $2)),
		(SELECT count(*) FROM repair_jobs WHERE state = 'done' AND updated_at > now() - interval '5 minutes')`,
		j.Priority, j.Created).Scan(&ahead, &recent)
	return j, ahead, recent, err
}

// RetryRepairNow returns a waiting or lost job to the front of the queue.
func (l *Ledger) RetryRepairNow(ctx context.Context, id int64) error {
	tag, err := l.pool.Exec(ctx, `UPDATE repair_jobs SET state = 'queued', not_before = now(), attempts = 0,
		lease_owner = NULL, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND state IN ('queued', 'lost')
		  AND NOT EXISTS (SELECT 1 FROM repair_jobs o WHERE o.nugget = repair_jobs.nugget AND o.id <> $1 AND o.state IN ('queued', 'leased'))`, id)
	if err == nil && tag.RowsAffected() == 0 {
		var exists bool
		_ = l.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM repair_jobs WHERE id = $1)", id).Scan(&exists)
		if !exists {
			return ErrNoSuchRepair
		}
		return errors.New("ledger: job is running, done, or another job for its nugget is open")
	}
	return err
}

// DrainNeeds reports what draining vein requires: the widest layout among
// referenced nuggets it holds a piece of (copies, or k+m shards, each on a
// distinct vein) and the bytes to move.
func (l *Ledger) DrainNeeds(ctx context.Context, vein string) (width int, bytes, nuggets int64, err error) {
	err = l.pool.QueryRow(ctx, `SELECT coalesce(max(CASE WHEN n.ec_data > 0 THEN n.ec_data + n.ec_parity ELSE greatest(n.replication, 1) END), 0),
		coalesce(sum(n.length), 0), count(*)
		FROM nuggets n WHERE n.refs > 0 AND n.id IN (
			SELECT nugget FROM nugget_replicas WHERE vein_id = $1 UNION SELECT nugget FROM nugget_shards WHERE vein_id = $1)`, vein).
		Scan(&width, &bytes, &nuggets)
	return width, bytes, nuggets, err
}
