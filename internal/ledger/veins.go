package ledger

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/404khai/lode/internal/nugget"
)

// VeinInfo is a registered storage node.
type VeinInfo struct {
	ID             string
	Addr           string
	Zone           string
	Host           string
	State          string
	CapacityBytes  int64
	UsedBytes      int64
	LastHeartbeat  time.Time
	IOErrors       int64
	CorruptNuggets int64
	StateChanged   time.Time
}

// RegisterVein records a vein, or updates its address and labels.
func (l *Ledger) RegisterVein(ctx context.Context, v VeinInfo) error {
	_, err := l.pool.Exec(ctx, `
		INSERT INTO veins (id, addr, zone, host, capacity_bytes, used_bytes, last_heartbeat)
		VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (id) DO UPDATE SET addr = EXCLUDED.addr, zone = EXCLUDED.zone, host = EXCLUDED.host,
			capacity_bytes = EXCLUDED.capacity_bytes, used_bytes = EXCLUDED.used_bytes, last_heartbeat = now()`,
		v.ID, v.Addr, v.Zone, v.Host, v.CapacityBytes, v.UsedBytes)
	return err
}

// Heartbeat refreshes a vein's liveness, usage and disk health. Canary
// decides state; a heartbeat never changes it.
func (l *Ledger) Heartbeat(ctx context.Context, h VeinInfo) error {
	_, err := l.pool.Exec(ctx, `UPDATE veins SET last_heartbeat = now(), capacity_bytes = $2, used_bytes = $3,
		io_errors = $4, corrupt_nuggets = $5 WHERE id = $1`, h.ID, h.CapacityBytes, h.UsedBytes, h.IOErrors, h.CorruptNuggets)
	return err
}

// ListVeins returns every vein that has not been removed.
func (l *Ledger) ListVeins(ctx context.Context) ([]VeinInfo, error) {
	rows, err := l.pool.Query(ctx, `SELECT id, addr, zone, host, state, capacity_bytes, used_bytes, last_heartbeat,
		io_errors, corrupt_nuggets, state_changed
		FROM veins WHERE state <> 'removed' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (VeinInfo, error) {
		var v VeinInfo
		return v, r.Scan(&v.ID, &v.Addr, &v.Zone, &v.Host, &v.State, &v.CapacityBytes, &v.UsedBytes, &v.LastHeartbeat,
			&v.IOErrors, &v.CorruptNuggets, &v.StateChanged)
	})
}

// AddReplicas records that veins hold a copy of id.
func (l *Ledger) AddReplicas(ctx context.Context, id nugget.ID, veins []string) error {
	if len(veins) == 0 {
		return nil
	}
	_, err := l.pool.Exec(ctx, `INSERT INTO nugget_replicas (nugget, vein_id)
		SELECT $1, v FROM unnest($2::text[]) AS v ON CONFLICT DO NOTHING`, id[:], veins)
	return err
}

// Replicas returns the veins recorded as holding id.
func (l *Ledger) Replicas(ctx context.Context, id nugget.ID) ([]string, error) {
	rows, err := l.pool.Query(ctx, "SELECT vein_id FROM nugget_replicas WHERE nugget = $1 ORDER BY vein_id", id[:])
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
