// Package foreman is Lode's MCP server: the agent-native surface over the
// cluster. Read-only tools are always available; tools that change the
// cluster are registered only with AllowWrite and are annotated destructive.
// Every tool returns structured JSON plus a one-line human summary.
package foreman

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/survey"
	"github.com/404khai/lode/internal/version"
)

// Options configure Foreman.
type Options struct {
	Ledger     *ledger.Ledger
	Cluster    *cluster.Client
	Logger     *slog.Logger
	AllowWrite bool
	// DefaultReplication and DefaultPlacement mirror Adit's settings.
	DefaultReplication int
	DefaultPlacement   string
	Now                func() time.Time
}

// Foreman holds the MCP server.
type Foreman struct {
	opts   Options
	log    *slog.Logger
	server *mcp.Server
}

// New builds Foreman and registers its tools and resources.
func New(opts Options) *Foreman {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.DefaultReplication <= 0 {
		opts.DefaultReplication = 3
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	f := &Foreman{opts: opts, log: opts.Logger}
	instructions := "Foreman exposes a Lode object-storage cluster. Start with cluster_status. " +
		"objects_at_risk answers what a vein failure would endanger; locate_object shows where an object's shards live. "
	if opts.AllowWrite {
		instructions += "Write tools are enabled: run drain_vein with dry_run first."
	} else {
		instructions += "This server is read-only."
	}
	f.server = mcp.NewServer(&mcp.Implementation{Name: "lode-foreman", Version: version.Version},
		&mcp.ServerOptions{Instructions: instructions})
	f.registerTools()
	f.registerResources()
	return f
}

// Server returns the MCP server for a transport.
func (f *Foreman) Server() *mcp.Server { return f.server }

// result builds a tool result: a one-line summary plus the structured output.
func result[T any](summary string, out T) (*mcp.CallToolResult, T, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: summary}}}, out, nil
}

func readOnly(title string) *mcp.ToolAnnotations {
	f := false
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: &f}
}

func destructive(title string, idempotent bool) *mcp.ToolAnnotations {
	t, f := true, false
	return &mcp.ToolAnnotations{Title: title, DestructiveHint: &t, IdempotentHint: idempotent, OpenWorldHint: &f}
}

// ---- views ----

// VeinView is a vein as tools report it.
type VeinView struct {
	ID               string  `json:"id"`
	State            string  `json:"state"`
	Available        bool    `json:"available" jsonschema:"accepting writes now"`
	Zone             string  `json:"zone"`
	Host             string  `json:"host"`
	Addr             string  `json:"addr"`
	UsedBytes        int64   `json:"used_bytes"`
	CapacityBytes    int64   `json:"capacity_bytes"`
	HeartbeatAgeSecs float64 `json:"heartbeat_age_seconds"`
	IOErrors         int64   `json:"io_errors"`
	CorruptNuggets   int64   `json:"corrupt_nuggets"`
}

func (f *Foreman) veinViews(ctx context.Context) ([]VeinView, error) {
	vs, err := f.opts.Ledger.ListVeins(ctx)
	if err != nil {
		return nil, err
	}
	avail := map[string]bool{}
	if f.opts.Cluster != nil {
		for _, n := range f.opts.Cluster.Available() {
			avail[n.ID] = true
		}
	}
	now := f.opts.Now()
	out := make([]VeinView, len(vs))
	for i, v := range vs {
		out[i] = VeinView{ID: v.ID, State: v.State, Available: avail[v.ID], Zone: v.Zone, Host: v.Host, Addr: v.Addr,
			UsedBytes: v.UsedBytes, CapacityBytes: v.CapacityBytes, HeartbeatAgeSecs: now.Sub(v.LastHeartbeat).Round(time.Millisecond).Seconds(),
			IOErrors: v.IOErrors, CorruptNuggets: v.CorruptNuggets}
	}
	return out, nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// ---- inputs and outputs ----

type empty struct{}

type ClusterStatus struct {
	Veins           int                  `json:"veins"`
	VeinsByState    map[string]int       `json:"veins_by_state"`
	Available       int                  `json:"available_veins"`
	Totals          ledger.ClusterTotals `json:"totals"`
	RepairsOpen     int64                `json:"repairs_open"`
	RepairsLost     int64                `json:"repairs_lost"`
	UnderReplicated int64                `json:"under_replicated_nuggets"`
}

type VeinInput struct {
	Vein string `json:"vein" jsonschema:"vein ID, e.g. vein3"`
}

type VeinDetail struct {
	VeinView
	Holdings ledger.VeinHoldings `json:"holdings"`
	// AtRiskVersions counts object versions with no redundancy left (or
	// unreadable) if this vein died now.
	AtRiskVersions int64 `json:"at_risk_versions_if_lost"`
}

type DiskHealth struct {
	Veins   []VeinView `json:"veins"`
	Flagged []string   `json:"flagged" jsonschema:"veins with I/O errors, corrupt reads, or a stale heartbeat"`
}

type LocateInput struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty" jsonschema:"defaults to the current version"`
}

type Location struct {
	Vein  string `json:"vein"`
	State string `json:"state"`
	Shard *int   `json:"shard,omitempty" jsonschema:"shard index for erasure-coded nuggets"`
}

type NuggetLocation struct {
	Nugget    string     `json:"nugget"`
	Length    int64      `json:"length"`
	Layout    string     `json:"layout" jsonschema:"replicated or ec K+M"`
	Locations []Location `json:"locations"`
	Tolerance int        `json:"tolerance" jsonschema:"further vein losses survivable on healthy veins"`
}

type ObjectLocation struct {
	Bucket    string           `json:"bucket"`
	Key       string           `json:"key"`
	VersionID string           `json:"version_id"`
	Size      int64            `json:"size"`
	Nuggets   []NuggetLocation `json:"nuggets"`
	Tolerance int              `json:"tolerance" jsonschema:"minimum over nuggets"`
	Veins     []string         `json:"veins" jsonschema:"every vein holding part of the object"`
}

type AtRiskInput struct {
	Vein string `json:"vein" jsonschema:"the vein whose loss to simulate"`
	// Threshold: report versions whose remaining tolerance would be <= this.
	Threshold *int `json:"threshold,omitempty" jsonschema:"report versions left with at most this many further losses (default 0)"`
	Limit     int  `json:"limit,omitempty" jsonschema:"maximum versions to return (default 100)"`
}

type AtRiskResult struct {
	Vein             string          `json:"vein"`
	Threshold        int             `json:"threshold"`
	Total            int64           `json:"total"`
	WouldBeLost      int             `json:"would_be_unreadable" jsonschema:"versions in this page that would be unreadable"`
	NoRedundancyLeft int             `json:"no_redundancy_left" jsonschema:"versions in this page one further loss from unreadable"`
	Objects          []ledger.AtRisk `json:"objects"`
}

type RepairsInput struct {
	State string `json:"state,omitempty" jsonschema:"queued, leased, done or lost; default open jobs"`
	Limit int    `json:"limit,omitempty"`
}

type RepairsResult struct {
	Stats ledger.RepairStats     `json:"stats"`
	Jobs  []ledger.RepairJobInfo `json:"jobs"`
}

type RepairInput struct {
	ID int64 `json:"id" jsonschema:"repair job ID"`
}

type RepairExplanation struct {
	Job          ledger.RepairJobInfo `json:"job"`
	Why          string               `json:"why"`
	QueueAhead   int64                `json:"queue_ahead"`
	ETASeconds   *float64             `json:"eta_seconds,omitempty" jsonschema:"estimate from recent throughput; absent when unknown"`
	NuggetLayout string               `json:"nugget_layout,omitempty"`
	Copies       []string             `json:"copies_on,omitempty"`
}

type BucketInput struct {
	Bucket string `json:"bucket"`
}

type BucketUsage struct {
	Bucket     string              `json:"bucket"`
	Versioning string              `json:"versioning"`
	Class      string              `json:"storage_class"`
	Placement  string              `json:"placement"`
	Usage      ledger.Usage        `json:"usage"`
	Quota      *ledger.Quota       `json:"quota,omitempty"`
	Config     ledger.BucketConfig `json:"config"`
}

type LifecycleRules struct {
	Bucket string                 `json:"bucket"`
	Rules  []ledger.LifecycleRule `json:"rules"`
}

type SimulateInput struct {
	Bucket string `json:"bucket"`
	Sample string `json:"sample" jsonschema:"any content; its BLAKE3 hash stands in for a nugget ID"`
}

type Placement struct {
	Bucket   string   `json:"bucket"`
	Nugget   string   `json:"nugget"`
	Strategy string   `json:"strategy"`
	Layout   string   `json:"layout"`
	Targets  []string `json:"targets" jsonschema:"veins in placement order (shard i goes to targets[i])"`
	Short    bool     `json:"short" jsonschema:"fewer available veins than the layout needs"`
}

// ---- tools ----

func (f *Foreman) registerTools() {
	s := f.server
	mcp.AddTool(s, &mcp.Tool{Name: "cluster_status", Description: "Overall cluster health: veins by state, totals, repair queue and redundancy.",
		Annotations: readOnly("Cluster status")}, f.clusterStatus)
	mcp.AddTool(s, &mcp.Tool{Name: "list_veins", Description: "Every storage node with state, zone, usage and heartbeat age.",
		Annotations: readOnly("List veins")}, f.listVeins)
	mcp.AddTool(s, &mcp.Tool{Name: "vein_detail", Description: "One vein: state, usage, what it holds, and how many object versions would lose redundancy if it died.",
		Annotations: readOnly("Vein detail")}, f.veinDetail)
	mcp.AddTool(s, &mcp.Tool{Name: "disk_health", Description: "I/O errors, corrupt reads and heartbeat freshness per vein, with problem veins flagged.",
		Annotations: readOnly("Disk health")}, f.diskHealth)
	mcp.AddTool(s, &mcp.Tool{Name: "locate_object", Description: "Which veins hold each nugget/shard of an object version, and how many more vein losses it survives.",
		Annotations: readOnly("Locate object")}, f.locateObject)
	mcp.AddTool(s, &mcp.Tool{Name: "objects_at_risk", Description: "Object versions that would be unreadable, or left without redundancy, if the given vein died right now.",
		Annotations: readOnly("Objects at risk")}, f.objectsAtRisk)
	mcp.AddTool(s, &mcp.Tool{Name: "list_repairs", Description: "Repair jobs (open by default), most urgent first, with queue statistics.",
		Annotations: readOnly("List repairs")}, f.listRepairs)
	mcp.AddTool(s, &mcp.Tool{Name: "explain_repair", Description: "Why a repair job exists, what it will do, and an ETA from recent throughput.",
		Annotations: readOnly("Explain repair")}, f.explainRepair)
	mcp.AddTool(s, &mcp.Tool{Name: "bucket_usage", Description: "A bucket's storage class, placement, usage and quota.",
		Annotations: readOnly("Bucket usage")}, f.bucketUsage)
	mcp.AddTool(s, &mcp.Tool{Name: "list_lifecycle_rules", Description: "A bucket's lifecycle rules.",
		Annotations: readOnly("Lifecycle rules")}, f.listLifecycle)
	mcp.AddTool(s, &mcp.Tool{Name: "simulate_placement", Description: "Where a nugget with the given sample content would be placed in a bucket right now.",
		Annotations: readOnly("Simulate placement")}, f.simulatePlacement)
	if !f.opts.AllowWrite {
		return
	}
	mcp.AddTool(s, &mcp.Tool{Name: "drain_vein", Description: "Move every copy off a vein so it can be decommissioned. Refuses when the remaining healthy veins cannot hold the data's redundancy. Use dry_run first.",
		Annotations: destructive("Drain vein", true)}, f.drainVein)
	mcp.AddTool(s, &mcp.Tool{Name: "trigger_scrub", Description: "Start an integrity scrub (Assay) on a vein now.",
		Annotations: destructive("Trigger scrub", true)}, f.triggerScrub)
	mcp.AddTool(s, &mcp.Tool{Name: "set_quota", Description: "Set (or with no limits, remove) a bucket or access-key quota.",
		Annotations: destructive("Set quota", true)}, f.setQuota)
	mcp.AddTool(s, &mcp.Tool{Name: "put_lifecycle_rule", Description: "Add or replace (by ID) one lifecycle rule on a bucket.",
		Annotations: destructive("Put lifecycle rule", true)}, f.putLifecycleRule)
	mcp.AddTool(s, &mcp.Tool{Name: "retry_repair", Description: "Return a waiting or lost repair job to the front of the queue.",
		Annotations: destructive("Retry repair", true)}, f.retryRepair)
}

func (f *Foreman) clusterStatus(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, ClusterStatus, error) {
	var out ClusterStatus
	vs, err := f.veinViews(ctx)
	if err != nil {
		return nil, out, err
	}
	out.Veins, out.VeinsByState = len(vs), map[string]int{}
	for _, v := range vs {
		out.VeinsByState[v.State]++
		if v.Available {
			out.Available++
		}
	}
	if out.Totals, err = f.opts.Ledger.ClusterTotals(ctx); err != nil {
		return nil, out, err
	}
	st, err := f.opts.Ledger.RepairStats(ctx)
	if err != nil {
		return nil, out, err
	}
	out.RepairsOpen, out.RepairsLost, out.UnderReplicated = st.Queued+st.Leased, st.Lost, st.UnderReplicated
	var states []string
	for s, n := range out.VeinsByState {
		states = append(states, fmt.Sprintf("%d %s", n, s))
	}
	sort.Strings(states)
	health := "fully redundant"
	if out.UnderReplicated > 0 {
		health = fmt.Sprintf("%d nuggets under-replicated", out.UnderReplicated)
	}
	return result(fmt.Sprintf("%d veins (%s); %d objects, %s stored (%s unique); %d repairs open; %s.",
		out.Veins, strings.Join(states, ", "), out.Totals.Objects, humanBytes(out.Totals.LogicalBytes),
		humanBytes(out.Totals.UniqueBytes), out.RepairsOpen, health), out)
}

type VeinList struct {
	Veins []VeinView `json:"veins"`
}

func (f *Foreman) listVeins(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, VeinList, error) {
	vs, err := f.veinViews(ctx)
	if err != nil {
		return nil, VeinList{}, err
	}
	down := 0
	for _, v := range vs {
		if v.State != ledger.VeinHealthy {
			down++
		}
	}
	return result(fmt.Sprintf("%d veins, %d not healthy.", len(vs), down), VeinList{Veins: vs})
}

func (f *Foreman) findVein(ctx context.Context, id string) (VeinView, error) {
	vs, err := f.veinViews(ctx)
	if err != nil {
		return VeinView{}, err
	}
	for _, v := range vs {
		if v.ID == id {
			return v, nil
		}
	}
	return VeinView{}, fmt.Errorf("no vein %q (list_veins shows the IDs)", id)
}

func (f *Foreman) veinDetail(ctx context.Context, _ *mcp.CallToolRequest, in VeinInput) (*mcp.CallToolResult, VeinDetail, error) {
	var out VeinDetail
	v, err := f.findVein(ctx, in.Vein)
	if err != nil {
		return nil, out, err
	}
	out.VeinView = v
	if out.Holdings, err = f.opts.Ledger.VeinHoldings(ctx, in.Vein); err != nil {
		return nil, out, err
	}
	if _, out.AtRiskVersions, err = f.opts.Ledger.ObjectsAtRisk(ctx, in.Vein, 0, 1); err != nil {
		return nil, out, err
	}
	return result(fmt.Sprintf("%s is %s in zone %s: %d copies + %d shards (%s); %d object versions would be left without redundancy if it died.",
		v.ID, v.State, v.Zone, out.Holdings.Replicas, out.Holdings.Shards, humanBytes(out.Holdings.Bytes), out.AtRiskVersions), out)
}

func (f *Foreman) diskHealth(ctx context.Context, _ *mcp.CallToolRequest, _ empty) (*mcp.CallToolResult, DiskHealth, error) {
	vs, err := f.veinViews(ctx)
	if err != nil {
		return nil, DiskHealth{}, err
	}
	out := DiskHealth{Veins: vs, Flagged: []string{}}
	for _, v := range vs {
		if v.IOErrors > 0 || v.CorruptNuggets > 0 || v.HeartbeatAgeSecs > 10 {
			out.Flagged = append(out.Flagged, v.ID)
		}
	}
	if len(out.Flagged) == 0 {
		return result(fmt.Sprintf("All %d veins report no I/O errors or corrupt reads and are heartbeating.", len(vs)), out)
	}
	return result(fmt.Sprintf("%d of %d veins need attention: %s.", len(out.Flagged), len(vs), strings.Join(out.Flagged, ", ")), out)
}

func layoutName(k, m, replication int) string {
	if k > 0 {
		return fmt.Sprintf("ec %d+%d", k, m)
	}
	return fmt.Sprintf("replicated x%d", max(replication, 1))
}

// Locate reports where an object version's nuggets and shards live.
func (f *Foreman) Locate(ctx context.Context, in LocateInput) (ObjectLocation, error) {
	out := ObjectLocation{Bucket: in.Bucket, Key: in.Key}
	o, err := f.opts.Ledger.GetObject(ctx, in.Bucket, in.Key, in.VersionID)
	if err != nil {
		return out, err
	}
	states := map[string]string{}
	vs, err := f.opts.Ledger.ListVeins(ctx)
	if err != nil {
		return out, err
	}
	for _, v := range vs {
		states[v.ID] = v.State
	}
	out.VersionID, out.Size = o.VersionID, o.Size
	all := map[string]bool{}
	out.Tolerance = 1 << 30
	for _, n := range o.Nuggets {
		ns, _, err := f.opts.Ledger.Nugget(ctx, n.ID)
		if err != nil {
			return out, err
		}
		nl := NuggetLocation{Nugget: n.ID.String(), Length: n.Length, Layout: layoutName(n.ECData, n.ECParity, ns.Replication)}
		if n.ECData > 0 {
			idx := map[int]bool{}
			for _, s := range n.Shards {
				i := s.Index
				nl.Locations = append(nl.Locations, Location{Vein: s.Vein, State: states[s.Vein], Shard: &i})
				all[s.Vein] = true
				if states[s.Vein] == ledger.VeinHealthy {
					idx[s.Index] = true
				}
			}
			nl.Tolerance = len(idx) - n.ECData
		} else {
			healthy := 0
			for _, r := range n.Replicas {
				nl.Locations = append(nl.Locations, Location{Vein: r, State: states[r]})
				all[r] = true
				if states[r] == ledger.VeinHealthy {
					healthy++
				}
			}
			nl.Tolerance = healthy - 1
		}
		out.Tolerance = min(out.Tolerance, nl.Tolerance)
		out.Nuggets = append(out.Nuggets, nl)
	}
	if len(o.Nuggets) == 0 {
		out.Tolerance = 0
	}
	for v := range all {
		out.Veins = append(out.Veins, v)
	}
	sort.Strings(out.Veins)
	return out, nil
}

func (f *Foreman) locateObject(ctx context.Context, _ *mcp.CallToolRequest, in LocateInput) (*mcp.CallToolResult, ObjectLocation, error) {
	out, err := f.Locate(ctx, in)
	if err != nil {
		return nil, out, err
	}
	return result(fmt.Sprintf("%s/%s (%s, %d nuggets) is spread over %s and survives %d more vein loss(es).",
		in.Bucket, in.Key, humanBytes(out.Size), len(out.Nuggets), strings.Join(out.Veins, ", "), out.Tolerance), out)
}

func (f *Foreman) objectsAtRisk(ctx context.Context, _ *mcp.CallToolRequest, in AtRiskInput) (*mcp.CallToolResult, AtRiskResult, error) {
	out := AtRiskResult{Vein: in.Vein, Objects: []ledger.AtRisk{}}
	if _, err := f.findVein(ctx, in.Vein); err != nil {
		return nil, out, err
	}
	if in.Threshold != nil {
		out.Threshold = *in.Threshold
	}
	limit := in.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	objs, total, err := f.opts.Ledger.ObjectsAtRisk(ctx, in.Vein, out.Threshold, limit)
	if err != nil {
		return nil, out, err
	}
	out.Total = total
	if objs != nil {
		out.Objects = objs
	}
	for _, o := range out.Objects {
		switch {
		case o.Tolerance < 0:
			out.WouldBeLost++
		case o.Tolerance == 0:
			out.NoRedundancyLeft++
		}
	}
	if total == 0 {
		return result(fmt.Sprintf("Nothing is at risk: every object with data on %s keeps at least %d more loss of redundancy without it.",
			in.Vein, out.Threshold+1), out)
	}
	return result(fmt.Sprintf("If %s died now, %d object version(s) would be at risk: %d unreadable and %d left with no redundancy (first %d listed).",
		in.Vein, total, out.WouldBeLost, out.NoRedundancyLeft, len(out.Objects)), out)
}

func (f *Foreman) listRepairs(ctx context.Context, _ *mcp.CallToolRequest, in RepairsInput) (*mcp.CallToolResult, RepairsResult, error) {
	var out RepairsResult
	limit := in.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var err error
	if out.Stats, err = f.opts.Ledger.RepairStats(ctx); err != nil {
		return nil, out, err
	}
	if out.Jobs, err = f.opts.Ledger.RepairJobs(ctx, in.State, limit); err != nil {
		return nil, out, err
	}
	if out.Jobs == nil {
		out.Jobs = []ledger.RepairJobInfo{}
	}
	return result(fmt.Sprintf("%d queued, %d running, %d lost; %d nuggets under-replicated.",
		out.Stats.Queued, out.Stats.Leased, out.Stats.Lost, out.Stats.UnderReplicated), out)
}

var reasons = map[string]string{
	ledger.ReasonVeinDown:        "a vein holding a copy was marked down by Canary",
	ledger.ReasonUnderReplicated: "the nugget has fewer copies or shards on healthy veins than it should",
	ledger.ReasonCorrupt:         "a copy failed BLAKE3 verification (found by Assay or on read)",
	ledger.ReasonDrain:           "a vein holding a copy is being drained",
}

func (f *Foreman) explainRepair(ctx context.Context, _ *mcp.CallToolRequest, in RepairInput) (*mcp.CallToolResult, RepairExplanation, error) {
	var out RepairExplanation
	j, ahead, recent, err := f.opts.Ledger.RepairJob(ctx, in.ID)
	if err != nil {
		return nil, out, err
	}
	out.Job, out.QueueAhead = j, ahead
	out.Why = reasons[j.Reason]
	if out.Why == "" {
		out.Why = j.Reason
	}
	if len(j.BadVeins) > 0 {
		out.Why += "; copies on " + strings.Join(j.BadVeins, ", ") + " will be replaced"
	}
	if id, err := nugget.Parse(j.Nugget); err == nil {
		if ns, ok, err := f.opts.Ledger.Nugget(ctx, id); err == nil && ok {
			out.NuggetLayout = layoutName(ns.ECData, ns.ECParity, ns.Replication)
			out.Copies = append([]string{}, ns.Replicas...)
			for _, s := range ns.Shards {
				out.Copies = append(out.Copies, fmt.Sprintf("%s(shard %d)", s.Vein, s.Index))
			}
		}
	}
	summary := fmt.Sprintf("Job %d is %s because %s.", j.ID, j.State, out.Why)
	switch {
	case j.State == "done":
		summary += " It has finished."
	case j.State == "lost":
		summary += " It gave up: no readable copy was found (" + j.LastError + ")."
	case recent > 0:
		eta := float64(ahead+1) / (float64(recent) / 300)
		out.ETASeconds = &eta
		summary += fmt.Sprintf(" %d jobs ahead; at the last 5 minutes' rate it should run in about %s.", ahead, time.Duration(eta*float64(time.Second)).Round(time.Second))
	default:
		summary += fmt.Sprintf(" %d jobs ahead; no recent throughput to estimate an ETA.", ahead)
	}
	if j.LastError != "" && j.State != "lost" {
		summary += " Last attempt: " + j.LastError + "."
	}
	return result(summary, out)
}

func className(c ledger.BucketConfig, defReplication int) string {
	if c.ECData > 0 {
		return fmt.Sprintf("EC_%d_%d", c.ECData, c.ECParity)
	}
	r := c.Replication
	if r <= 0 {
		r = defReplication
	}
	return fmt.Sprintf("STANDARD (x%d)", r)
}

func (f *Foreman) bucketUsage(ctx context.Context, _ *mcp.CallToolRequest, in BucketInput) (*mcp.CallToolResult, BucketUsage, error) {
	out := BucketUsage{Bucket: in.Bucket}
	b, err := f.opts.Ledger.HeadBucket(ctx, in.Bucket)
	if err != nil {
		return nil, out, err
	}
	out.Versioning, out.Config = b.Versioning, b.BucketConfig
	out.Class = className(b.BucketConfig, f.opts.DefaultReplication)
	out.Placement = b.Placement
	if out.Placement == "" {
		out.Placement = f.opts.DefaultPlacement
	}
	if out.Usage, err = f.opts.Ledger.GetUsage(ctx, ledger.ScopeBucket, in.Bucket); err != nil {
		return nil, out, err
	}
	qs, _, err := f.opts.Ledger.Quotas(ctx)
	if err != nil {
		return nil, out, err
	}
	for i := range qs {
		if qs[i].Scope == ledger.ScopeBucket && qs[i].Subject == in.Bucket {
			out.Quota = &qs[i]
		}
	}
	summary := fmt.Sprintf("%s stores %s in %d object versions, class %s.", in.Bucket, humanBytes(out.Usage.Bytes), out.Usage.Objects, out.Class)
	if out.Quota != nil && out.Quota.MaxBytes != nil {
		summary += fmt.Sprintf(" Quota: %s (%.0f%% used).", humanBytes(*out.Quota.MaxBytes), 100*float64(out.Usage.Bytes)/float64(max(*out.Quota.MaxBytes, 1)))
	}
	return result(summary, out)
}

func (f *Foreman) listLifecycle(ctx context.Context, _ *mcp.CallToolRequest, in BucketInput) (*mcp.CallToolResult, LifecycleRules, error) {
	out := LifecycleRules{Bucket: in.Bucket, Rules: []ledger.LifecycleRule{}}
	rules, err := f.opts.Ledger.Lifecycle(ctx, in.Bucket)
	if errors.Is(err, ledger.ErrNoLifecycle) {
		return result(in.Bucket+" has no lifecycle rules.", out)
	}
	if err != nil {
		return nil, out, err
	}
	out.Rules = rules
	return result(fmt.Sprintf("%s has %d lifecycle rule(s).", in.Bucket, len(rules)), out)
}

func (f *Foreman) simulatePlacement(ctx context.Context, _ *mcp.CallToolRequest, in SimulateInput) (*mcp.CallToolResult, Placement, error) {
	out := Placement{Bucket: in.Bucket}
	b, err := f.opts.Ledger.HeadBucket(ctx, in.Bucket)
	if err != nil {
		return nil, out, err
	}
	name := b.Placement
	if name == "" {
		name = f.opts.DefaultPlacement
	}
	placer, err := survey.ByName(name, b.Pinned)
	if err != nil {
		return nil, out, err
	}
	id := nugget.Sum([]byte(in.Sample))
	out.Nugget, out.Strategy = id.String(), placer.Name()
	want := b.Replication
	if want <= 0 {
		want = f.opts.DefaultReplication
	}
	out.Layout = layoutName(b.ECData, b.ECParity, want)
	if b.ECData > 0 {
		want = b.ECData + b.ECParity
	}
	var nodes []survey.Node
	if f.opts.Cluster != nil {
		nodes = f.opts.Cluster.Available()
	}
	for _, n := range placer.Place(id, want, nodes) {
		out.Targets = append(out.Targets, n.ID)
	}
	out.Short = len(out.Targets) < want
	summary := fmt.Sprintf("In %s (%s, %s placement) this nugget would go to %s.", in.Bucket, out.Layout, out.Strategy, strings.Join(out.Targets, ", "))
	if out.Short {
		summary += fmt.Sprintf(" Only %d of %d needed veins are available, so the write would be degraded.", len(out.Targets), want)
	}
	return result(summary, out)
}

// ---- write tools ----

type DrainInput struct {
	Vein   string `json:"vein"`
	DryRun bool   `json:"dry_run,omitempty" jsonschema:"report the plan and safety checks without changing anything"`
}

type DrainPlan struct {
	Vein          string `json:"vein"`
	Started       bool   `json:"started"`
	Safe          bool   `json:"safe"`
	Reason        string `json:"reason,omitempty" jsonschema:"why the drain was refused"`
	NuggetsToMove int64  `json:"nuggets_to_move"`
	BytesToMove   int64  `json:"bytes_to_move"`
	NeededVeins   int    `json:"needed_veins" jsonschema:"distinct healthy veins the widest layout needs"`
	OtherHealthy  int    `json:"other_healthy_veins"`
	JobsQueued    int64  `json:"jobs_queued"`
}

func (f *Foreman) drainVein(ctx context.Context, _ *mcp.CallToolRequest, in DrainInput) (*mcp.CallToolResult, DrainPlan, error) {
	out := DrainPlan{Vein: in.Vein}
	v, err := f.findVein(ctx, in.Vein)
	if err != nil {
		return nil, out, err
	}
	vs, err := f.veinViews(ctx)
	if err != nil {
		return nil, out, err
	}
	for _, o := range vs {
		if o.ID != in.Vein && o.State == ledger.VeinHealthy {
			out.OtherHealthy++
		}
	}
	if out.NeededVeins, out.BytesToMove, out.NuggetsToMove, err = f.opts.Ledger.DrainNeeds(ctx, in.Vein); err != nil {
		return nil, out, err
	}
	switch {
	case v.State == ledger.VeinDraining:
		out.Safe, out.Reason = true, "already draining"
	case v.State != ledger.VeinHealthy && v.State != ledger.VeinSuspect:
		out.Reason = fmt.Sprintf("vein is %s; use `lode vein remove` for a dead vein", v.State)
	case out.OtherHealthy < out.NeededVeins:
		out.Reason = fmt.Sprintf("its data needs %d distinct healthy veins but only %d others are healthy; add capacity first", out.NeededVeins, out.OtherHealthy)
	default:
		out.Safe = true
	}
	if !out.Safe {
		return result(fmt.Sprintf("Refusing to drain %s: %s.", in.Vein, out.Reason), out)
	}
	if in.DryRun {
		return result(fmt.Sprintf("Draining %s is safe: %d nuggets (%s) would move to the other %d healthy veins. Run again without dry_run to start.",
			in.Vein, out.NuggetsToMove, humanBytes(out.BytesToMove), out.OtherHealthy), out)
	}
	if v.State != ledger.VeinDraining {
		if _, err := f.opts.Ledger.SetVeinState(ctx, in.Vein, ledger.VeinDraining); err != nil {
			return nil, out, err
		}
	}
	if out.JobsQueued, err = f.opts.Ledger.EnqueueVeinRepairs(ctx, in.Vein, ledger.ReasonDrain); err != nil {
		return nil, out, err
	}
	out.Started = true
	f.log.Warn("drain started via MCP", "vein", in.Vein, "jobs", out.JobsQueued)
	return result(fmt.Sprintf("Draining %s: %d repair jobs queued to move %s; it is retired automatically once empty. Track it with vein_detail or list_repairs.",
		in.Vein, out.JobsQueued, humanBytes(out.BytesToMove)), out)
}

type ScrubResult struct {
	Vein    string `json:"vein"`
	Started bool   `json:"started"`
}

func (f *Foreman) triggerScrub(ctx context.Context, _ *mcp.CallToolRequest, in VeinInput) (*mcp.CallToolResult, ScrubResult, error) {
	out := ScrubResult{Vein: in.Vein}
	if f.opts.Cluster == nil {
		return nil, out, errors.New("foreman has no cluster connection")
	}
	started, err := f.opts.Cluster.TriggerScrub(ctx, in.Vein)
	if err != nil {
		return nil, out, err
	}
	out.Started = started
	if !started {
		return result(fmt.Sprintf("%s is already scrubbing.", in.Vein), out)
	}
	return result(fmt.Sprintf("Scrub started on %s; corrupt copies it finds are queued for repair.", in.Vein), out)
}

type QuotaInput struct {
	Scope      string `json:"scope" jsonschema:"bucket or key"`
	Subject    string `json:"subject" jsonschema:"bucket name or access key"`
	MaxBytes   *int64 `json:"max_bytes,omitempty"`
	MaxObjects *int64 `json:"max_objects,omitempty"`
}

func (f *Foreman) setQuota(ctx context.Context, _ *mcp.CallToolRequest, in QuotaInput) (*mcp.CallToolResult, ledger.Quota, error) {
	q := ledger.Quota{Scope: in.Scope, Subject: in.Subject, MaxBytes: in.MaxBytes, MaxObjects: in.MaxObjects}
	if err := f.opts.Ledger.SetQuota(ctx, q); err != nil {
		return nil, q, err
	}
	if q.MaxBytes == nil && q.MaxObjects == nil {
		return result(fmt.Sprintf("Removed the quota on %s %s.", q.Scope, q.Subject), q)
	}
	b, o := "unlimited", "unlimited"
	if q.MaxBytes != nil {
		b = humanBytes(*q.MaxBytes)
	}
	if q.MaxObjects != nil {
		o = fmt.Sprint(*q.MaxObjects)
	}
	return result(fmt.Sprintf("%s %s quota: %s, %s objects.", q.Scope, q.Subject, b, o), q)
}

type RuleInput struct {
	Bucket string `json:"bucket"`
	ledger.LifecycleRule
	TransitionDays  int    `json:"transition_days,omitempty" jsonschema:"move current versions after this many days"`
	TransitionClass string `json:"transition_class,omitempty" jsonschema:"STANDARD or EC_K_M, e.g. EC_4_2"`
}

func (f *Foreman) putLifecycleRule(ctx context.Context, _ *mcp.CallToolRequest, in RuleInput) (*mcp.CallToolResult, LifecycleRules, error) {
	out := LifecycleRules{Bucket: in.Bucket}
	r := in.LifecycleRule
	if r.ID == "" {
		return nil, out, errors.New("rule id is required")
	}
	if in.TransitionClass != "" {
		t := ledger.Transition{Days: in.TransitionDays, Class: in.TransitionClass}
		if in.TransitionClass != "STANDARD" {
			if _, err := fmt.Sscanf(in.TransitionClass, "EC_%d_%d", &t.ECData, &t.ECParity); err != nil || ec.Validate(t.ECData, t.ECParity) != nil {
				return nil, out, fmt.Errorf("transition_class must be STANDARD or EC_K_M")
			}
		}
		if t.Days <= 0 {
			return nil, out, errors.New("transition_days must be positive")
		}
		r.Transitions = []ledger.Transition{t}
	}
	for _, d := range []int{r.ExpirationDays, r.NoncurrentDays, r.AbortIncompleteDays, r.NewerNoncurrentVersions} {
		if d < 0 {
			return nil, out, errors.New("day counts must not be negative")
		}
	}
	if r.ExpirationDays == 0 && r.ExpirationDate == nil && !r.ExpiredObjectDeleteMarker && r.NoncurrentDays == 0 &&
		r.AbortIncompleteDays == 0 && len(r.Transitions) == 0 {
		return nil, out, errors.New("the rule needs at least one action")
	}
	rules, err := f.opts.Ledger.Lifecycle(ctx, in.Bucket)
	if err != nil && !errors.Is(err, ledger.ErrNoLifecycle) {
		return nil, out, err
	}
	replaced := false
	for i := range rules {
		if rules[i].ID == r.ID {
			rules[i], replaced = r, true
		}
	}
	if !replaced {
		rules = append(rules, r)
	}
	if err := f.opts.Ledger.SetLifecycle(ctx, in.Bucket, rules); err != nil {
		return nil, out, err
	}
	out.Rules = rules
	verb := "Added"
	if replaced {
		verb = "Replaced"
	}
	return result(fmt.Sprintf("%s rule %q on %s; it now has %d rule(s).", verb, r.ID, in.Bucket, len(rules)), out)
}

type RetryResult struct {
	ID     int64 `json:"id"`
	Queued bool  `json:"queued"`
}

func (f *Foreman) retryRepair(ctx context.Context, _ *mcp.CallToolRequest, in RepairInput) (*mcp.CallToolResult, RetryResult, error) {
	out := RetryResult{ID: in.ID}
	if err := f.opts.Ledger.RetryRepairNow(ctx, in.ID); err != nil {
		return nil, out, err
	}
	out.Queued = true
	return result(fmt.Sprintf("Repair job %d is back at the front of the queue.", in.ID), out)
}

// ---- resources ----

func (f *Foreman) jsonResource(uri string, v any) (*mcp.ReadResourceResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(b)}}}, nil
}

func (f *Foreman) registerResources() {
	s := f.server
	s.AddResource(&mcp.Resource{URI: "lode://cluster", Name: "cluster", Description: "Cluster status", MIMEType: "application/json"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			_, out, err := f.clusterStatus(ctx, nil, empty{})
			if err != nil {
				return nil, err
			}
			return f.jsonResource(req.Params.URI, out)
		})
	s.AddResource(&mcp.Resource{URI: "lode://repairs", Name: "repairs", Description: "Open repair jobs", MIMEType: "application/json"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			_, out, err := f.listRepairs(ctx, nil, RepairsInput{})
			if err != nil {
				return nil, err
			}
			return f.jsonResource(req.Params.URI, out)
		})
	s.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "lode://veins/{id}", Name: "vein", Description: "One vein", MIMEType: "application/json"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			id := strings.TrimPrefix(req.Params.URI, "lode://veins/")
			_, out, err := f.veinDetail(ctx, nil, VeinInput{Vein: id})
			if err != nil {
				return nil, mcp.ResourceNotFoundError(req.Params.URI)
			}
			return f.jsonResource(req.Params.URI, out)
		})
	s.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "lode://buckets/{name}", Name: "bucket", Description: "Bucket usage and configuration", MIMEType: "application/json"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			name := strings.TrimPrefix(req.Params.URI, "lode://buckets/")
			_, out, err := f.bucketUsage(ctx, nil, BucketInput{Bucket: name})
			if err != nil {
				return nil, mcp.ResourceNotFoundError(req.Params.URI)
			}
			return f.jsonResource(req.Params.URI, out)
		})
	s.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "lode://objects/{bucket}/{+key}", Name: "object", Description: "Where an object's shards live", MIMEType: "application/json"},
		func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
			rest := strings.TrimPrefix(req.Params.URI, "lode://objects/")
			bucket, key, ok := strings.Cut(rest, "/")
			if !ok {
				return nil, mcp.ResourceNotFoundError(req.Params.URI)
			}
			out, err := f.Locate(ctx, LocateInput{Bucket: bucket, Key: key})
			if err != nil {
				return nil, mcp.ResourceNotFoundError(req.Params.URI)
			}
			return f.jsonResource(req.Params.URI, out)
		})
}
