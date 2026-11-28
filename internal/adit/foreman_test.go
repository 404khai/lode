package adit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/404khai/lode/internal/canary"
	"github.com/404khai/lode/internal/foreman"
	"github.com/404khai/lode/internal/ledger"
)

// mcpSession connects an MCP client to a Foreman over in-memory transports.
func (h *harness) mcpSession(allowWrite bool) *mcp.ClientSession {
	h.t.Helper()
	ctx := context.Background()
	f := foreman.New(foreman.Options{Ledger: h.ledger, Cluster: h.cluster, AllowWrite: allowWrite, DefaultPlacement: "zone-spread"})
	st, ct := mcp.NewInMemoryTransports()
	ss, err := f.Server().Connect(ctx, st, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-agent", Version: "1"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// call invokes a tool and decodes its structured output, returning the summary.
func call(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any, out any) string {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	var summary string
	if len(res.Content) > 0 {
		if tc, ok := res.Content[0].(*mcp.TextContent); ok {
			summary = tc.Text
		}
	}
	if res.IsError {
		t.Fatalf("%s returned an error: %s", tool, summary)
	}
	if out != nil {
		b, _ := json.Marshal(res.StructuredContent)
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s structured output: %v (%s)", tool, err, b)
		}
	}
	if summary == "" {
		t.Fatalf("%s returned no human summary", tool)
	}
	return summary
}

// Phase 7 exit criterion, part 1: an agent can answer "which objects are at
// risk if vein-3 dies right now?".
func TestForemanAnswersObjectsAtRisk(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/risk", nil, nil), 200, "")
	for i := range 40 {
		h.expect(h.do("PUT", fmt.Sprintf("/risk/o%02d", i), []byte(fmt.Sprintf("object %d", i)), nil), 200, "")
	}
	cs := h.mcpSession(false)

	var none foreman.AtRiskResult
	call(t, cs, "objects_at_risk", map[string]any{"vein": "v3"}, &none)
	if none.Total != 0 {
		t.Fatalf("healthy cluster reported %d at-risk versions", none.Total)
	}

	// v1 is already down: anything with copies on both v1 and v3 has only
	// one copy left if v3 dies too.
	h.veins[0].stop()
	if _, err := h.ledger.SetVeinState(ctx, "v1", ledger.VeinDown); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for i := range 40 {
		key := fmt.Sprintf("o%02d", i)
		rs := h.replicaSets("risk", key)[0]
		has := map[string]bool{}
		for _, v := range rs {
			has[v] = true
		}
		if has["v1"] && has["v3"] {
			want[key] = true
		}
	}
	if len(want) == 0 {
		t.Skip("no object shares v1 and v3")
	}
	var risk foreman.AtRiskResult
	summary := call(t, cs, "objects_at_risk", map[string]any{"vein": "v3", "limit": 1000}, &risk)
	t.Logf("agent sees: %s", summary)
	if int(risk.Total) != len(want) || risk.NoRedundancyLeft != len(want) || risk.WouldBeLost != 0 {
		t.Fatalf("at risk = %+v, want %d objects", risk, len(want))
	}
	for _, o := range risk.Objects {
		if !want[o.Key] || o.Bucket != "risk" || !o.IsCurrent {
			t.Fatalf("unexpected at-risk object %+v", o)
		}
	}
	if !strings.Contains(summary, "v3") || !strings.Contains(summary, fmt.Sprint(len(want))) {
		t.Fatalf("summary does not answer the question: %s", summary)
	}

	// The same answer, cross-checked with locate_object.
	for key := range want {
		var loc foreman.ObjectLocation
		call(t, cs, "locate_object", map[string]any{"bucket": "risk", "key": key}, &loc)
		if loc.Tolerance != 1 {
			t.Fatalf("%s tolerance %d, want 1 (two healthy copies)", key, loc.Tolerance)
		}
		break
	}

	// Read-only servers do not offer write tools.
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range tools.Tools {
		if tool.Name == "drain_vein" || tool.Name == "set_quota" {
			t.Fatalf("read-only Foreman exposes %s", tool.Name)
		}
		if !tool.Annotations.ReadOnlyHint {
			t.Fatalf("%s is not marked read-only", tool.Name)
		}
	}
	var cluster foreman.ClusterStatus
	call(t, cs, "cluster_status", nil, &cluster)
	if cluster.Veins != 4 || cluster.VeinsByState["down"] != 1 || cluster.Totals.Objects != 40 {
		t.Fatalf("cluster status = %+v", cluster)
	}
	res, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "lode://objects/risk/o00"})
	if err != nil || len(res.Contents) != 1 || !strings.Contains(res.Contents[0].Text, `"nuggets"`) {
		t.Fatalf("object resource: %v %+v", err, res)
	}
}

// Phase 7 exit criterion, part 2: an agent safely drains a vein end to end.
func TestForemanDrainsVeinEndToEnd(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/drainme", nil, nil), 200, "")
	data := map[string][]byte{}
	for i := range 20 {
		k := fmt.Sprintf("d%02d", i)
		data[k] = randomBytes(10_000 + i)
		h.expect(h.do("PUT", "/drainme/"+k, data[k], nil), 200, "")
	}
	cs := h.mcpSession(true)
	tools, _ := cs.ListTools(ctx, nil)
	destructive := 0
	for _, tool := range tools.Tools {
		if a := tool.Annotations; a.DestructiveHint != nil && *a.DestructiveHint {
			destructive++
		}
	}
	if destructive != 5 {
		t.Fatalf("%d destructive tools, want 5", destructive)
	}

	var before foreman.VeinDetail
	call(t, cs, "vein_detail", map[string]any{"vein": "v3"}, &before)
	if before.Holdings.Replicas == 0 {
		t.Skip("placement put nothing on v3")
	}
	var plan foreman.DrainPlan
	summary := call(t, cs, "drain_vein", map[string]any{"vein": "v3", "dry_run": true}, &plan)
	if !plan.Safe || plan.Started || plan.NuggetsToMove != before.Holdings.Replicas {
		t.Fatalf("dry run = %+v (%s)", plan, summary)
	}
	if vs, _ := h.ledger.ListVeins(ctx); vs[2].State != ledger.VeinHealthy {
		t.Fatal("dry run changed state")
	}
	summary = call(t, cs, "drain_vein", map[string]any{"vein": "v3"}, &plan)
	t.Logf("agent sees: %s", summary)
	if !plan.Started || plan.JobsQueued == 0 {
		t.Fatalf("drain = %+v", plan)
	}
	var repairs foreman.RepairsResult
	call(t, cs, "list_repairs", nil, &repairs)
	if len(repairs.Jobs) == 0 {
		t.Fatal("no repair jobs listed")
	}
	var why foreman.RepairExplanation
	if s := call(t, cs, "explain_repair", map[string]any{"id": repairs.Jobs[0].ID}, &why); !strings.Contains(s, "drained") {
		t.Fatalf("explain_repair: %s", s)
	}

	// The cluster does the work: Smelter moves copies, Canary retires the empty vein.
	h.drainQueue()
	if _, err := h.ledger.Pool().Exec(ctx, "UPDATE veins SET last_heartbeat = now()"); err != nil {
		t.Fatal(err)
	}
	if err := canary.New(canary.Options{Ledger: h.ledger, DownAfter: time.Hour}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var list foreman.VeinList
	call(t, cs, "list_veins", nil, &list)
	for _, v := range list.Veins {
		if v.ID == "v3" {
			t.Fatalf("v3 still listed as %s after drain", v.State)
		}
	}
	var status foreman.ClusterStatus
	call(t, cs, "cluster_status", nil, &status)
	if status.UnderReplicated != 0 || status.RepairsOpen != 0 {
		t.Fatalf("after drain: %+v", status)
	}
	h.veins[2].stop() // decommission
	for k, want := range data {
		if got := h.expect(h.do("GET", "/drainme/"+k, nil, nil), 200, ""); !bytes.Equal(got, want) {
			t.Fatalf("%s wrong after drain and decommission", k)
		}
	}
}

func TestForemanRefusesUnsafeDrainAndWriteTools(t *testing.T) {
	h := newClusterHarness(t, 6, 3)
	h.expect(h.do("PUT", "/wide", nil, map[string]string{"X-Lode-Storage-Class": "ec:4+2"}), 200, "")
	h.expect(h.do("PUT", "/wide/k", randomBytes(50_000), nil), 200, "")
	cs := h.mcpSession(true)
	var plan foreman.DrainPlan
	summary := call(t, cs, "drain_vein", map[string]any{"vein": "v1"}, &plan)
	if plan.Safe || plan.Started || plan.NeededVeins != 6 || plan.OtherHealthy != 5 {
		t.Fatalf("unsafe drain not refused: %+v (%s)", plan, summary)
	}
	if !strings.Contains(summary, "Refusing") {
		t.Fatalf("summary: %s", summary)
	}
	var q ledger.Quota
	call(t, cs, "set_quota", map[string]any{"scope": "bucket", "subject": "wide", "max_bytes": 1 << 20}, &q)
	var usage foreman.BucketUsage
	if s := call(t, cs, "bucket_usage", map[string]any{"bucket": "wide"}, &usage); usage.Quota == nil || !strings.Contains(s, "Quota") {
		t.Fatalf("bucket usage: %+v %s", usage, s)
	}
	var rules foreman.LifecycleRules
	call(t, cs, "put_lifecycle_rule", map[string]any{"bucket": "wide", "id": "cold", "enabled": true,
		"transition_days": 30, "transition_class": "STANDARD"}, &rules)
	call(t, cs, "list_lifecycle_rules", map[string]any{"bucket": "wide"}, &rules)
	if len(rules.Rules) != 1 || rules.Rules[0].Transitions[0].Days != 30 {
		t.Fatalf("rules = %+v", rules)
	}
	var sim foreman.Placement
	call(t, cs, "simulate_placement", map[string]any{"bucket": "wide", "sample": "hello"}, &sim)
	if len(sim.Targets) != 6 || sim.Short {
		t.Fatalf("simulate = %+v", sim)
	}
}
