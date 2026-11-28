package adit

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/strata"
	"github.com/404khai/lode/internal/tailings"
)

func i64(n int64) *int64 { return &n }

// Phase 6 exit criterion: quota overrun returns QuotaExceeded.
func TestQuotas(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.expect(h.do("PUT", "/quota", nil, nil), 200, "")
	h.expect(h.do("PUT", "/other", nil, nil), 200, "")
	if err := h.ledger.SetQuota(ctx, ledger.Quota{Scope: ledger.ScopeBucket, Subject: "quota", MaxBytes: i64(1000), MaxObjects: i64(3)}); err != nil {
		t.Fatal(err)
	}
	h.expect(h.do("PUT", "/quota/a", make([]byte, 600), nil), 200, "")
	h.expect(h.do("PUT", "/quota/b", make([]byte, 600), nil), 403, "QuotaExceeded")
	// Overwriting replaces the null version, which frees its bytes first.
	h.expect(h.do("PUT", "/quota/a", make([]byte, 900), nil), 200, "")
	h.expect(h.do("PUT", "/quota/s1", []byte("x"), nil), 200, "")
	h.expect(h.do("PUT", "/quota/s2", []byte("x"), nil), 200, "")
	h.expect(h.do("PUT", "/quota/s3", []byte("x"), nil), 403, "QuotaExceeded") // object limit
	h.expect(h.do("PUT", "/other/x", make([]byte, 5000), nil), 200, "")        // other bucket unaffected
	h.expect(h.do("DELETE", "/quota/a", nil, nil), 204, "")
	h.expect(h.do("PUT", "/quota/b", make([]byte, 600), nil), 200, "")
	u, _ := h.ledger.GetUsage(ctx, ledger.ScopeBucket, "quota")
	if u.Bytes != 602 || u.Objects != 3 {
		t.Fatalf("usage = %+v", u)
	}

	// Copies and multipart completions count too.
	h.expect(h.do("PUT", "/quota/c", nil, map[string]string{"X-Amz-Copy-Source": "/other/x"}), 403, "QuotaExceeded")

	// Per access key, across buckets.
	if err := h.ledger.SetQuota(ctx, ledger.Quota{Scope: ledger.ScopeKey, Subject: testAK, MaxBytes: i64(8000)}); err != nil {
		t.Fatal(err)
	}
	h.expect(h.do("PUT", "/other/y", make([]byte, 2000), nil), 200, "")              // 5602 + 2000 = 7602 <= 8000
	h.expect(h.do("PUT", "/other/z", make([]byte, 1000), nil), 403, "QuotaExceeded") // 8602 > 8000
}

func TestQuotaKeyAndReconcile(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.expect(h.do("PUT", "/keyq", nil, nil), 200, "")
	_ = h.ledger.SetQuota(ctx, ledger.Quota{Scope: ledger.ScopeKey, Subject: testAK, MaxBytes: i64(1500)})
	h.expect(h.do("PUT", "/keyq/a", make([]byte, 1000), nil), 200, "")
	h.expect(h.do("PUT", "/keyq/b", make([]byte, 1000), nil), 403, "QuotaExceeded")

	var init initiateResult
	_ = xml.Unmarshal(h.expect(h.do("POST", "/keyq/mp?uploads", nil, nil), 200, ""), &init)
	id := url.QueryEscape(init.UploadID)
	r := h.do("PUT", "/keyq/mp?partNumber=1&uploadId="+id, make([]byte, 400), nil)
	h.expect(r, 200, "")
	_ = h.ledger.SetQuota(ctx, ledger.Quota{Scope: ledger.ScopeKey, Subject: testAK, MaxBytes: i64(1200)})
	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", r.Header.Get("ETag"))
	h.expect(h.do("POST", "/keyq/mp?uploadId="+id, []byte(body), nil), 403, "QuotaExceeded")

	// Drift is repaired by reconciliation.
	if _, err := h.ledger.Pool().Exec(ctx, "UPDATE usage_counters SET bytes = 999999 WHERE scope = 'bucket' AND subject = 'keyq'"); err != nil {
		t.Fatal(err)
	}
	if n, err := h.ledger.ReconcileUsage(ctx); err != nil || n == 0 {
		t.Fatalf("reconcile: %d %v", n, err)
	}
	if u, _ := h.ledger.GetUsage(ctx, ledger.ScopeBucket, "keyq"); u.Bytes != 1000 || u.Objects != 1 {
		t.Fatalf("usage after reconcile = %+v", u)
	}
	if n, _ := h.ledger.ReconcileUsage(ctx); n != 0 {
		t.Fatalf("second reconcile changed %d", n)
	}
}

func lifecycleBody(rules string) []byte {
	return []byte(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` + rules + `</LifecycleConfiguration>`)
}

func TestLifecycleConfigurationAPI(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/lcapi", nil, nil), 200, "")
	h.expect(h.do("GET", "/lcapi?lifecycle", nil, nil), 404, "NoSuchLifecycleConfiguration")
	good := lifecycleBody(`<Rule><ID>logs</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>
		<Expiration><Days>30</Days></Expiration><Transition><Days>7</Days><StorageClass>EC_4_2</StorageClass></Transition>
		<NoncurrentVersionExpiration><NoncurrentDays>3</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration>
		<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>`)
	h.expect(h.do("PUT", "/lcapi?lifecycle", good, nil), 200, "")
	got := string(h.expect(h.do("GET", "/lcapi?lifecycle", nil, nil), 200, ""))
	for _, want := range []string{"<ID>logs</ID>", "<Prefix>logs/</Prefix>", "<Days>30</Days>", "<StorageClass>EC_4_2</StorageClass>", "<NewerNoncurrentVersions>2</NewerNoncurrentVersions>"} {
		if !strings.Contains(got, want) {
			t.Fatalf("lifecycle round trip missing %s:\n%s", want, got)
		}
	}
	for name, bad := range map[string]struct {
		body, code string
	}{
		"no action":   {`<Rule><ID>x</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status></Rule>`, "MalformedXML"},
		"bad status":  {`<Rule><Status>On</Status><Expiration><Days>1</Days></Expiration></Rule>`, "MalformedXML"},
		"zero days":   {`<Rule><Status>Enabled</Status><Expiration><Days>0</Days></Expiration></Rule>`, "InvalidArgument"},
		"dup id":      {`<Rule><ID>a</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule><Rule><ID>a</ID><Status>Enabled</Status><Expiration><Days>2</Days></Expiration></Rule>`, "InvalidArgument"},
		"bad class":   {`<Rule><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition></Rule>`, "InvalidStorageClass"},
		"days + date": {`<Rule><Status>Enabled</Status><Expiration><Days>1</Days><Date>2030-01-01T00:00:00Z</Date></Expiration></Rule>`, "MalformedXML"},
	} {
		resp := h.do("PUT", "/lcapi?lifecycle", lifecycleBody(bad.body), nil)
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d", name, resp.StatusCode)
		}
		h.expect(resp, 400, bad.code)
	}
	h.expect(h.do("DELETE", "/lcapi?lifecycle", nil, nil), 204, "")
	h.expect(h.do("GET", "/lcapi?lifecycle", nil, nil), 404, "NoSuchLifecycleConfiguration")
}

// Phase 6 exit criterion: lifecycle rules verified with a time-skewed clock.
func TestLifecycleRulesWithSkewedClock(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.expect(h.do("PUT", "/lcrun", nil, nil), 200, "")
	h.expect(h.do("PUT", "/lcrun?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil), 200, "")
	rules := lifecycleBody(`
		<Rule><ID>tmp</ID><Filter><Prefix>tmp/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>10</Days></Expiration></Rule>
		<Rule><ID>tagged</ID><Filter><Tag><Key>ttl</Key><Value>short</Value></Tag></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule>
		<Rule><ID>old</ID><Filter><Prefix>keep/</Prefix></Filter><Status>Enabled</Status>
			<NoncurrentVersionExpiration><NoncurrentDays>5</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule>
		<Rule><ID>markers</ID><Filter><Prefix>gone/</Prefix></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule>
		<Rule><ID>uploads</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule>
		<Rule><ID>off</ID><Filter><Prefix>keep/</Prefix></Filter><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule>`)
	h.expect(h.do("PUT", "/lcrun?lifecycle", rules, nil), 200, "")

	h.expect(h.do("PUT", "/lcrun/tmp/a", []byte("tmp"), nil), 200, "")
	h.expect(h.do("PUT", "/lcrun/x", []byte("tagged"), map[string]string{"X-Amz-Tagging": "ttl=short"}), 200, "")
	h.expect(h.do("PUT", "/lcrun/y", []byte("untagged"), nil), 200, "")
	for i := range 4 {
		h.expect(h.do("PUT", "/lcrun/keep/k", []byte(fmt.Sprintf("v%d", i)), nil), 200, "")
	}
	h.expect(h.do("PUT", "/lcrun/gone/g", []byte("g"), nil), 200, "")
	h.expect(h.do("DELETE", "/lcrun/gone/g?versionId="+h.currentVersion("lcrun", "gone/g"), nil, nil), 204, "")
	h.expect(h.do("DELETE", "/lcrun/gone/g", nil, nil), 204, "") // a delete marker with nothing under it
	var init initiateResult
	_ = xml.Unmarshal(h.expect(h.do("POST", "/lcrun/stuck?uploads", nil, nil), 200, ""), &init)

	run := func(offset time.Duration) strata.Stats {
		t.Helper()
		s := strata.New(strata.Options{Ledger: h.ledger, Cluster: h.cluster, ClockOffset: offset})
		st, err := s.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	if st := run(0); st != (strata.Stats{MarkersRemoved: 1}) {
		t.Fatalf("pass at day 0: %+v", st) // the orphan marker goes immediately
	}
	st := run(3 * day)
	if st.Expired != 1 || st.UploadsAborted != 1 || st.NoncurrentRemoved != 0 {
		t.Fatalf("pass at day 3: %+v", st)
	}
	h.expect(h.do("GET", "/lcrun/x", nil, nil), 404, "NoSuchKey") // tagged: expired (delete marker)
	h.expect(h.do("GET", "/lcrun/y", nil, nil), 200, "")
	h.expect(h.do("GET", "/lcrun/tmp/a", nil, nil), 200, "")
	h.expect(h.do("GET", "/lcrun/stuck?uploadId="+url.QueryEscape(init.UploadID), nil, nil), 404, "NoSuchUpload")

	st = run(11 * day)
	if st.Expired != 1 || st.NoncurrentRemoved != 2 {
		t.Fatalf("pass at day 11: %+v", st)
	}
	h.expect(h.do("GET", "/lcrun/tmp/a", nil, nil), 404, "NoSuchKey")
	raw := h.expect(h.do("GET", "/lcrun?versions&prefix=keep/", nil, nil), 200, "")
	if n := bytes.Count(raw, []byte("<Version>")); n != 2 { // current + 1 newer noncurrent kept
		t.Fatalf("keep/k versions left: %d", n)
	}
	if b := h.expect(h.do("GET", "/lcrun/keep/k", nil, nil), 200, ""); string(b) != "v3" {
		t.Fatalf("current version lost: %q", b)
	}
	if st := run(11 * day); st != (strata.Stats{}) {
		t.Fatalf("second pass not idempotent: %+v", st)
	}
}

func (h *harness) currentVersion(bucket, key string) string {
	h.t.Helper()
	o, err := h.ledger.GetObject(context.Background(), bucket, key, "")
	if err != nil {
		h.t.Fatal(err)
	}
	return o.VersionID
}

const day = 24 * time.Hour

func TestLifecycleTransitionsBetweenClasses(t *testing.T) {
	h := newClusterHarness(t, 6, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/tier", nil, nil), 200, "")
	h.expect(h.do("PUT", "/tier?lifecycle", lifecycleBody(`<Rule><ID>cold</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status>
		<Transition><Days>30</Days><StorageClass>EC_4_2</StorageClass></Transition></Rule>`), nil), 200, "")
	data := randomBytes(2*nugget.MaxSize + 99)
	h.expect(h.do("PUT", "/tier/obj", data, nil), 200, "")

	s := strata.New(strata.Options{Ledger: h.ledger, Cluster: h.cluster, ClockOffset: 31 * day, TransitionDelay: time.Millisecond})
	st, err := s.RunOnce(ctx)
	if err != nil || st.NuggetsTransitioned == 0 {
		t.Fatalf("transition pass: %+v %v", st, err)
	}
	o, _ := h.ledger.GetObject(ctx, "tier", "obj", "")
	for _, n := range o.Nuggets {
		if n.ECData != 4 || len(n.Shards) != 6 || len(n.Replicas) != 0 {
			t.Fatalf("nugget not transitioned: %d+%d, %d shards, %d replicas", n.ECData, n.ECParity, len(n.Shards), len(n.Replicas))
		}
	}
	if got := h.expect(h.do("GET", "/tier/obj", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object wrong after transition")
	}
	// Old replicas are deleted from the veins once the delay passes.
	time.Sleep(5 * time.Millisecond)
	gc := tailings.New(tailings.Options{Ledger: h.ledger, Veins: h.cluster, Grace: time.Hour})
	gst, err := gc.RunOnce(ctx)
	if err != nil || gst.BlobsDeleted == 0 {
		t.Fatalf("deferred deletes: %+v %v", gst, err)
	}
	for _, v := range h.veins {
		if ok, _ := v.store.Has(o.Nuggets[0].ID); ok {
			t.Fatalf("old replica still on %s", v.id)
		}
	}
	if st2, _ := s.RunOnce(ctx); st2.NuggetsTransitioned != 0 {
		t.Fatal("second transition pass moved data again")
	}
	// Back to replicated.
	h.expect(h.do("PUT", "/tier?lifecycle", lifecycleBody(`<Rule><ID>warm</ID><Filter><Prefix></Prefix></Filter><Status>Enabled</Status>
		<Transition><Days>1</Days><StorageClass>STANDARD</StorageClass></Transition></Rule>`), nil), 200, "")
	if st, err := s.RunOnce(ctx); err != nil || st.NuggetsTransitioned == 0 {
		t.Fatalf("transition back: %+v %v", st, err)
	}
	o, _ = h.ledger.GetObject(ctx, "tier", "obj", "")
	if o.Nuggets[0].ECData != 0 || len(o.Nuggets[0].Replicas) != 3 {
		t.Fatalf("not replicated again: %+v", o.Nuggets[0])
	}
	h.veins[0].stop()
	if got := h.expect(h.do("GET", "/tier/obj", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object wrong after transitioning back")
	}
}
