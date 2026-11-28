package ledger

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/events"
)

// EventChannel is the Postgres NOTIFY channel carrying cluster events, so
// Lantern sees events from every Adit and vein process.
const EventChannel = "lode_events"

// maxNotify keeps payloads under Postgres's 8000-byte NOTIFY limit.
const maxNotify = 7900

// ForwardEvents publishes every event on bus to EventChannel until ctx is
// done. Delivery is best effort: events are for observers, never for
// correctness.
func (l *Ledger) ForwardEvents(ctx context.Context, bus *events.Bus, log *slog.Logger) {
	ch, cancel := bus.Subscribe(1024)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(e)
			if err != nil || len(b) > maxNotify {
				continue
			}
			if _, err := l.pool.Exec(ctx, "SELECT pg_notify($1, $2)", EventChannel, string(b)); err != nil && ctx.Err() == nil {
				log.Debug("forward event", "err", err)
			}
		}
	}
}

// ListenEvents delivers events from EventChannel to fn until ctx is done,
// reconnecting on errors.
func ListenEvents(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger, fn func(events.Event)) {
	for ctx.Err() == nil {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN "+EventChannel); err != nil {
			conn.Release()
			continue
		}
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Warn("event listener", "err", err)
				}
				break
			}
			var e events.Event
			if json.Unmarshal([]byte(n.Payload), &e) == nil {
				fn(e)
			}
		}
		conn.Release()
	}
}

// BucketHealth counts a bucket's referenced nuggets by remaining fault
// tolerance on healthy veins (-1 = unreadable now, 0 = no redundancy, ...).
type BucketHealth struct {
	Bucket      string        `json:"bucket"`
	Class       string        `json:"class"`
	ByTolerance map[int]int64 `json:"by_tolerance"`
}

// HealthMap returns BucketHealth for every bucket.
func (l *Ledger) HealthMap(ctx context.Context) ([]BucketHealth, error) {
	rows, err := l.pool.Query(ctx, `
		WITH bn AS (
			SELECT DISTINCT b.name, b.ec_data, b.ec_parity, b.replication, mn.nugget
			FROM buckets b JOIN objects o ON o.bucket_id = b.id
			JOIN object_versions v ON v.object_id = o.id AND NOT v.is_delete_marker
			JOIN manifest_nuggets mn ON mn.manifest_id = v.manifest_id
		)
		SELECT bn.name, bn.ec_data, bn.ec_parity, bn.replication, greatest(least(`+toleranceSQL+`, 3), -1) AS tol, count(*)
		FROM bn JOIN nuggets n ON n.id = bn.nugget
		GROUP BY bn.name, bn.ec_data, bn.ec_parity, bn.replication, tol
		ORDER BY bn.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byName := map[string]*BucketHealth{}
	var order []string
	for rows.Next() {
		var name string
		var k, m, r, tol int
		var n int64
		if err := rows.Scan(&name, &k, &m, &r, &tol, &n); err != nil {
			return nil, err
		}
		h := byName[name]
		if h == nil {
			class := "STANDARD"
			if k > 0 {
				class = "EC " + itoa(k) + "+" + itoa(m)
			}
			h = &BucketHealth{Bucket: name, Class: class, ByTolerance: map[int]int64{}}
			byName[name] = h
			order = append(order, name)
		}
		h.ByTolerance[tol] += n
	}
	out := make([]BucketHealth, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out, rows.Err()
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
