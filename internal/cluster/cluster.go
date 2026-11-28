// Package cluster is Adit's view of the storage tier: it tracks registered
// veins and their health, writes nuggets to a quorum of replicas chosen by
// Survey, and reads from the fastest healthy replica.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	lodev1 "github.com/404khai/lode/gen/lode/v1"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/survey"
)

var (
	// ErrInsufficientReplicas means fewer than the write quorum of replicas
	// could be stored.
	ErrInsufficientReplicas = errors.New("cluster: not enough healthy veins for the write quorum")
	// ErrUnavailable means no replica of a nugget could be read.
	ErrUnavailable = errors.New("cluster: no readable replica")
	// ErrCorrupt means every reachable replica failed verification.
	ErrCorrupt = errors.New("cluster: every readable replica is corrupt")
)

const (
	streamChunk = 1 << 20
	// reconnectWait bounds how long a read waits for a reconnecting vein:
	// about two reconnect backoff intervals.
	reconnectWait = 2 * time.Second
)

// Options configure a Client.
type Options struct {
	Ledger     *ledger.Ledger
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	StaleAfter  time.Duration // a vein whose heartbeat is older is unavailable
	Refresh     time.Duration // how often to reload the vein list
	Backoff     time.Duration // how long to avoid a vein after a transport failure
	RPCTimeout  time.Duration
	DialOptions []grpc.DialOption
	Now         func() time.Time
}

type member struct {
	info      ledger.VeinInfo
	conn      *grpc.ClientConn
	client    lodev1.VeinServiceClient
	downUntil time.Time
	latency   time.Duration // EWMA of successful reads
}

// Client is safe for concurrent use.
type Client struct {
	opts Options
	log  *slog.Logger

	mu      sync.RWMutex
	members map[string]*member

	rpcs       *prometheus.CounterVec
	rpcLatency *prometheus.HistogramVec
	writes     *prometheus.CounterVec
	failovers   prometheus.Counter
	bytesStored prometheus.Counter
	available   prometheus.Gauge
}

func New(opts Options) *Client {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = 10 * time.Second
	}
	if opts.Refresh <= 0 {
		opts.Refresh = 2 * time.Second
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 5 * time.Second
	}
	if opts.RPCTimeout <= 0 {
		opts.RPCTimeout = 30 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	f := promauto.With(opts.Registerer)
	return &Client{
		opts:    opts,
		log:     opts.Logger,
		members: map[string]*member{},
		rpcs: f.NewCounterVec(prometheus.CounterOpts{Namespace: "lode", Subsystem: "cluster", Name: "rpcs_total",
			Help: "Vein RPCs by vein, method and outcome."}, []string{"vein", "method", "code"}),
		rpcLatency: f.NewHistogramVec(prometheus.HistogramOpts{Namespace: "lode", Subsystem: "cluster", Name: "rpc_duration_seconds",
			Help: "Vein RPC latency.", Buckets: prometheus.DefBuckets}, []string{"method"}),
		writes: f.NewCounterVec(prometheus.CounterOpts{Namespace: "lode", Subsystem: "cluster", Name: "nugget_writes_total",
			Help: "Nugget writes by outcome: full, degraded (quorum but not all replicas), failed."}, []string{"outcome"}),
		failovers: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "cluster", Name: "read_failovers_total",
			Help: "Reads that had to try another replica."}),
		bytesStored: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "cluster", Name: "bytes_stored_total",
			Help: "Nugget bytes sent to veins in successful puts (all replicas)."}),
		available: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "cluster", Name: "veins_available",
			Help: "Veins currently available for placement."}),
	}
}

// Run refreshes the vein list until ctx is done.
func (c *Client) Run(ctx context.Context) {
	t := time.NewTicker(c.opts.Refresh)
	defer t.Stop()
	for {
		if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("refresh veins", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Refresh reloads the vein list from the Ledger, dialing new veins.
func (c *Client) Refresh(ctx context.Context) error {
	veins, err := c.opts.Ledger.ListVeins(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[string]bool{}
	for _, v := range veins {
		seen[v.ID] = true
		m := c.members[v.ID]
		if m != nil && m.info.Addr == v.Addr {
			m.info = v
			continue
		}
		if m != nil {
			_ = m.conn.Close()
		}
		dial := append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// Reconnect quickly after a vein restarts.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
				MinConnectTimeout: 2 * time.Second,
			}),
		}, c.opts.DialOptions...)
		conn, err := grpc.NewClient(v.Addr, dial...)
		if err != nil {
			c.log.Warn("dial vein", "vein", v.ID, "addr", v.Addr, "err", err)
			continue
		}
		c.members[v.ID] = &member{info: v, conn: conn, client: lodev1.NewVeinServiceClient(conn)}
		c.log.Info("vein joined", "vein", v.ID, "addr", v.Addr, "zone", v.Zone, "host", v.Host)
	}
	for id, m := range c.members {
		if !seen[id] {
			_ = m.conn.Close()
			delete(c.members, id)
		}
	}
	c.available.Set(float64(len(c.availableLocked())))
	return nil
}

func (c *Client) usable(m *member, now time.Time) bool {
	return m.info.State == "healthy" && now.Sub(m.info.LastHeartbeat) < c.opts.StaleAfter && now.After(m.downUntil)
}

func (c *Client) availableLocked() []survey.Node {
	now := c.opts.Now()
	var nodes []survey.Node
	for _, m := range c.members {
		if c.usable(m, now) {
			nodes = append(nodes, survey.Node{ID: m.info.ID, Zone: m.info.Zone, Host: m.info.Host})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

// Available returns the veins that may receive writes.
func (c *Client) Available() []survey.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.availableLocked()
}

// Veins returns a snapshot of every known vein.
func (c *Client) Veins() []ledger.VeinInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]ledger.VeinInfo, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c *Client) member(id string) (*member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := c.members[id]
	if m == nil {
		return nil, fmt.Errorf("cluster: unknown vein %q", id)
	}
	return m, nil
}

// transportFailure reports errors that say the vein itself is unreachable
// or unhealthy, rather than something about one nugget.
func transportFailure(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown, codes.ResourceExhausted:
		return true
	}
	return false
}

// observe records an RPC outcome. A transport failure takes the vein out of
// rotation briefly, unless the caller's own context ended first: a client
// that hangs up says nothing about the vein.
func (c *Client) observe(ctx context.Context, veinID, method string, start time.Time, err error) {
	c.rpcs.WithLabelValues(veinID, method, status.Code(err).String()).Inc()
	c.rpcLatency.WithLabelValues(method).Observe(time.Since(start).Seconds())
	if err == nil || !transportFailure(err) || ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	m := c.members[veinID]
	wasUp := m != nil && c.opts.Now().After(m.downUntil)
	if m != nil {
		m.downUntil = c.opts.Now().Add(c.opts.Backoff)
	}
	c.mu.Unlock()
	if wasUp {
		c.log.Warn("vein unreachable", "vein", veinID, "method", method, "err", err)
		c.opts.Events.Publish(events.Event{Type: "vein.unreachable", Component: "adit", Attrs: map[string]any{"vein": veinID, "error": err.Error()}})
	}
}

// Put stores data on one vein.
func (c *Client) Put(ctx context.Context, veinID string, id nugget.ID, data []byte) (err error) {
	m, err := c.member(veinID)
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() {
		c.observe(ctx, veinID, "PutShard", start, err)
		if err == nil {
			c.bytesStored.Add(float64(len(data)))
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
	defer cancel()
	stream, err := m.client.PutShard(ctx)
	if err != nil {
		return err
	}
	off := 0
	for first := true; first || off < len(data); first = false {
		end := min(off+streamChunk, len(data))
		msg := &lodev1.PutShardRequest{Data: data[off:end]}
		if first {
			msg.Id, msg.Length = id[:], int64(len(data))
		}
		if err := stream.Send(msg); err != nil {
			if errors.Is(err, io.EOF) {
				_, err = stream.CloseAndRecv() // surface the server's error
			}
			return err
		}
		off = end
	}
	_, err = stream.CloseAndRecv()
	return err
}

// Get reads and verifies one replica.
func (c *Client) Get(ctx context.Context, veinID string, id nugget.ID, length int64) ([]byte, error) {
	return c.get(ctx, veinID, id, length, false)
}

// get reads one replica. With wait set, it waits (briefly) for a connection
// that is reconnecting instead of failing fast.
func (c *Client) get(ctx context.Context, veinID string, id nugget.ID, length int64, wait bool) (data []byte, err error) {
	m, err := c.member(veinID)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	defer func() {
		c.observe(ctx, veinID, "GetShard", start, err)
		if err == nil {
			c.mu.Lock()
			d := time.Since(start)
			if m.latency == 0 {
				m.latency = d
			} else {
				m.latency = (m.latency*7 + d) / 8
			}
			c.mu.Unlock()
		}
	}()
	timeout := c.opts.RPCTimeout
	var callOpts []grpc.CallOption
	if wait {
		timeout = min(timeout, reconnectWait)
		callOpts = append(callOpts, grpc.WaitForReady(true))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stream, err := m.client.GetShard(ctx, &lodev1.GetShardRequest{Id: id[:]}, callOpts...)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, max(length, 0))
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		buf = append(buf, msg.GetData()...)
	}
	// End-to-end check: never trust bytes that crossed the network unverified.
	if nugget.Sum(buf) != id {
		return nil, status.Errorf(codes.DataLoss, "replica on %s failed verification", veinID)
	}
	return buf, nil
}

// Delete removes id from one vein.
func (c *Client) Delete(ctx context.Context, veinID string, id nugget.ID) (err error) {
	m, err := c.member(veinID)
	if err != nil {
		return err
	}
	start := time.Now()
	defer func() { c.observe(ctx, veinID, "DeleteShard", start, err) }()
	ctx, cancel := context.WithTimeout(ctx, c.opts.RPCTimeout)
	defer cancel()
	_, err = m.client.DeleteShard(ctx, &lodev1.DeleteShardRequest{Id: id[:]})
	return err
}

// Store writes data to enough veins that id has `replicas` copies, given the
// veins that already hold it. It succeeds once at least `quorum` copies are
// durable and returns the veins newly written. Fewer than `replicas` copies
// is a degraded write that repair (Smelter) will top up.
func (c *Client) Store(ctx context.Context, id nugget.ID, data []byte, existing []string, replicas, quorum int) ([]string, error) {
	avail := c.Available()
	isAvail := map[string]bool{}
	for _, n := range avail {
		isAvail[n.ID] = true
	}
	has := map[string]bool{}
	held := 0
	for _, v := range existing {
		has[v] = true
		if isAvail[v] {
			held++
		}
	}
	if held >= replicas {
		c.writes.WithLabelValues("deduplicated").Inc()
		return nil, nil
	}
	var targets []string
	for _, n := range survey.Place(id, len(avail), avail) {
		if len(targets) == replicas-held {
			break
		}
		if !has[n.ID] {
			targets = append(targets, n.ID)
		}
	}

	type result struct {
		vein string
		err  error
	}
	results := make(chan result, len(targets))
	for _, v := range targets {
		go func(v string) { results <- result{v, c.Put(ctx, v, id, data)} }(v)
	}
	var written []string
	var errs []error
	for range targets {
		r := <-results
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.vein, r.err))
			continue
		}
		written = append(written, r.vein)
	}
	sort.Strings(written)
	switch total := held + len(written); {
	case total < quorum:
		c.writes.WithLabelValues("failed").Inc()
		return written, fmt.Errorf("%w: %d of %d replicas stored (need %d): %w", ErrInsufficientReplicas, total, replicas, quorum, errors.Join(errs...))
	case total < replicas:
		c.writes.WithLabelValues("degraded").Inc()
		c.opts.Events.Publish(events.Event{Type: "nugget.under_replicated", Component: "adit",
			Attrs: map[string]any{"nugget": id.String(), "replicas": total, "want": replicas}})
	default:
		c.writes.WithLabelValues("full").Inc()
	}
	return written, nil
}

// Read returns ref's bytes from the best replica: available veins first,
// lowest read latency first, falling back through every replica. If every
// replica was unreachable, it tries once more waiting briefly for
// reconnecting veins (one may just have restarted).
func (c *Client) Read(ctx context.Context, ref ledger.NuggetRef) ([]byte, error) {
	order := c.readOrder(ref.Replicas)
	var errs []error
	corrupt, attempts := 0, 0
	for _, wait := range []bool{false, true} {
		unreachable := 0
		for _, v := range order {
			if attempts > 0 {
				c.failovers.Inc()
			}
			attempts++
			data, err := c.get(ctx, v, ref.ID, ref.Length, wait)
			if err == nil {
				return data, nil
			}
			switch {
			case status.Code(err) == codes.DataLoss:
				corrupt++
				c.opts.Events.Publish(events.Event{Type: "nugget.corrupt", Component: "adit",
					Attrs: map[string]any{"nugget": ref.ID.String(), "vein": v}})
			case transportFailure(err):
				unreachable++
			}
			errs = append(errs, fmt.Errorf("%s: %w", v, err))
			if ctx.Err() != nil {
				break
			}
		}
		if unreachable == 0 || unreachable < len(order)-corrupt || ctx.Err() != nil {
			break
		}
	}
	if corrupt > 0 && corrupt == len(errs) {
		return nil, fmt.Errorf("%w: nugget %s: %w", ErrCorrupt, ref.ID, errors.Join(errs...))
	}
	return nil, fmt.Errorf("%w: nugget %s: %w", ErrUnavailable, ref.ID, errors.Join(errs...))
}

func (c *Client) readOrder(replicas []string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.opts.Now()
	type cand struct {
		id      string
		usable  bool
		latency time.Duration
	}
	cs := make([]cand, 0, len(replicas))
	for _, v := range replicas {
		m := c.members[v]
		if m == nil {
			continue
		}
		cs = append(cs, cand{v, c.usable(m, now), m.latency})
	}
	sort.SliceStable(cs, func(i, j int) bool {
		if cs[i].usable != cs[j].usable {
			return cs[i].usable
		}
		return cs[i].latency < cs[j].latency
	})
	out := make([]string, len(cs))
	for i, x := range cs {
		out[i] = x.id
	}
	return out
}

// Close closes every connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, m := range c.members {
		_ = m.conn.Close()
		delete(c.members, id)
	}
}
