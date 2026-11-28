// Package lantern serves the cluster explorer: a JSON API, a live
// Server-Sent Events stream (cluster events relayed from every process via
// the Ledger, plus periodic snapshots), a bridge to Foreman's MCP tools, and
// the static web UI.
package lantern

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/foreman"
	"github.com/404khai/lode/internal/ledger"
)

type Options struct {
	Pool     *pgxpool.Pool
	Ledger   *ledger.Ledger
	Cluster  *cluster.Client
	Foreman  *foreman.Foreman
	Logger   *slog.Logger
	UI       http.Handler  // static files; nil serves a placeholder
	Interval time.Duration // snapshot period on the event stream
}

// Server is Lantern's HTTP handler.
type Server struct {
	opts Options
	log  *slog.Logger
	mux  *http.ServeMux

	mu      sync.Mutex
	clients map[chan []byte]struct{}
	recent  [][]byte // last events, replayed to new clients

	mcpOnce sync.Once
	mcp     *mcp.ClientSession
	mcpErr  error
}

func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Interval <= 0 {
		opts.Interval = 2 * time.Second
	}
	s := &Server{opts: opts, log: opts.Logger, mux: http.NewServeMux(), clients: map[chan []byte]struct{}{}}
	s.mux.HandleFunc("GET /api/snapshot", s.handleSnapshot)
	s.mux.HandleFunc("GET /api/object", s.handleObject)
	s.mux.HandleFunc("GET /api/events", s.handleEvents)
	s.mux.HandleFunc("GET /api/foreman/tools", s.handleTools)
	s.mux.HandleFunc("POST /api/foreman/call", s.handleCall)
	ui := opts.UI
	if ui == nil {
		ui = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "Lantern UI not built: run `npm ci && npm run build` in web/ and start lode-lantern with -ui web/dist", http.StatusNotFound)
		})
	}
	s.mux.Handle("/", ui)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Run relays Ledger events and periodic snapshots to stream clients.
func (s *Server) Run(ctx context.Context) {
	if s.opts.Pool != nil {
		go ledger.ListenEvents(ctx, s.opts.Pool, s.log, func(e events.Event) { s.broadcast("event", e, true) })
	}
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !s.hasClients() {
				continue
			}
			snap, err := s.Snapshot(ctx)
			if err != nil {
				s.log.Warn("snapshot", "err", err)
				continue
			}
			s.broadcast("snapshot", snap, false)
		}
	}
}

func (s *Server) hasClients() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients) > 0
}

func (s *Server) broadcast(kind string, v any, keep bool) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", kind, b))
	s.mu.Lock()
	defer s.mu.Unlock()
	if keep {
		s.recent = append(s.recent, msg)
		if len(s.recent) > 200 {
			s.recent = s.recent[len(s.recent)-200:]
		}
	}
	for c := range s.clients {
		select {
		case c <- msg:
		default: // slow client: drop rather than block the cluster view
		}
	}
}

// VeinNode is a vein in the topology.
type VeinNode struct {
	foreman.VeinView
	Holdings ledger.VeinHoldings `json:"holdings"`
}

// Snapshot is the whole-cluster view the UI renders.
type Snapshot struct {
	Time    time.Time             `json:"time"`
	Veins   []VeinNode            `json:"veins"`
	Totals  ledger.ClusterTotals  `json:"totals"`
	Repairs ledger.RepairStats    `json:"repairs"`
	Health  []ledger.BucketHealth `json:"health"`
	// Status is "green" (fully redundant, no open repairs), "repairing"
	// or "degraded" (under-replicated with nothing running), "red" if data
	// is unreadable.
	Status string `json:"status"`
}

// Snapshot gathers the current cluster state.
func (s *Server) Snapshot(ctx context.Context) (Snapshot, error) {
	L := s.opts.Ledger
	snap := Snapshot{Time: time.Now().UTC()}
	vs, err := L.ListVeins(ctx)
	if err != nil {
		return snap, err
	}
	avail := map[string]bool{}
	if s.opts.Cluster != nil {
		for _, n := range s.opts.Cluster.Available() {
			avail[n.ID] = true
		}
	}
	for _, v := range vs {
		h, err := L.VeinHoldings(ctx, v.ID)
		if err != nil {
			return snap, err
		}
		snap.Veins = append(snap.Veins, VeinNode{VeinView: foreman.VeinView{
			ID: v.ID, State: v.State, Available: avail[v.ID], Zone: v.Zone, Host: v.Host, Addr: v.Addr,
			UsedBytes: v.UsedBytes, CapacityBytes: v.CapacityBytes, IOErrors: v.IOErrors, CorruptNuggets: v.CorruptNuggets,
			HeartbeatAgeSecs: time.Since(v.LastHeartbeat).Round(time.Millisecond).Seconds(),
		}, Holdings: h})
	}
	if snap.Totals, err = L.ClusterTotals(ctx); err != nil {
		return snap, err
	}
	if snap.Repairs, err = L.RepairStats(ctx); err != nil {
		return snap, err
	}
	if snap.Health, err = L.HealthMap(ctx); err != nil {
		return snap, err
	}
	unreadable := false
	for _, b := range snap.Health {
		if b.ByTolerance[-1] > 0 {
			unreadable = true
		}
	}
	switch {
	case unreadable:
		snap.Status = "red"
	case snap.Repairs.UnderReplicated == 0 && snap.Repairs.Queued+snap.Repairs.Leased == 0:
		snap.Status = "green"
	case snap.Repairs.Queued+snap.Repairs.Leased > 0:
		snap.Status = "repairing"
	default:
		snap.Status = "degraded"
	}
	return snap, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Snapshot(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, snap)
}

func (s *Server) handleObject(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	loc, err := s.opts.Foreman.Locate(r.Context(), foreman.LocateInput{Bucket: q.Get("bucket"), Key: q.Get("key"), VersionID: q.Get("version")})
	switch {
	case errors.Is(err, ledger.ErrNoSuchKey), errors.Is(err, ledger.ErrNoSuchBucket), errors.Is(err, ledger.ErrNoSuchVersion):
		http.Error(w, err.Error(), http.StatusNotFound)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, loc)
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	ch := make(chan []byte, 256)
	s.mu.Lock()
	backlog := append([][]byte(nil), s.recent...)
	s.clients[ch] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
	}()
	if snap, err := s.Snapshot(r.Context()); err == nil {
		b, _ := json.Marshal(snap)
		_, _ = fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b)
	}
	for _, m := range backlog {
		_, _ = w.Write(m)
	}
	fl.Flush()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m := <-ch:
			if _, err := w.Write(m); err != nil {
				return
			}
			fl.Flush()
		case <-keepalive.C:
			_, _ = io.WriteString(w, ": keepalive\n\n")
			fl.Flush()
		}
	}
}

// session connects Lantern to Foreman as an MCP client, so the "Ask
// Foreman" panel goes through the same protocol agents use.
func (s *Server) session(ctx context.Context) (*mcp.ClientSession, error) {
	s.mcpOnce.Do(func() {
		st, ct := mcp.NewInMemoryTransports()
		if _, err := s.opts.Foreman.Server().Connect(context.WithoutCancel(ctx), st, nil); err != nil {
			s.mcpErr = err
			return
		}
		s.mcp, s.mcpErr = mcp.NewClient(&mcp.Implementation{Name: "lantern", Version: "1"}, nil).Connect(context.WithoutCancel(ctx), ct, nil)
	})
	return s.mcp, s.mcpErr
}

func (s *Server) handleTools(w http.ResponseWriter, r *http.Request) {
	cs, err := s.session(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := cs.ListTools(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, res.Tools)
}

type callRequest struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type callResponse struct {
	Summary string `json:"summary"`
	IsError bool   `json:"is_error"`
	Result  any    `json:"result,omitempty"`
}

func (s *Server) handleCall(w http.ResponseWriter, r *http.Request) {
	var in callRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil || in.Name == "" {
		http.Error(w, "body must be {\"name\": ..., \"arguments\": {...}}", http.StatusBadRequest)
		return
	}
	cs, err := s.session(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := cs.CallTool(r.Context(), &mcp.CallToolParams{Name: in.Name, Arguments: in.Arguments})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	out := callResponse{IsError: res.IsError, Result: res.StructuredContent}
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			out.Summary = t.Text
			break
		}
	}
	writeJSON(w, out)
}
