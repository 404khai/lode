// Package strata runs bucket lifecycle rules: expiring current versions,
// removing noncurrent versions and orphaned delete markers, aborting stale
// multipart uploads, and transitioning objects between storage classes
// (replicated and erasure-coded). Passes are idempotent and leader-elected.
//
// The clock can be offset (strata.clock_offset) so tests can watch rules
// fire without waiting days. Never set it in production.
package strata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/survey"
)

// LeaderLock is the advisory lock key held by the active Strata worker.
const LeaderLock = 0x6c6f6465_73747261 // "lodestra"

const day = 24 * time.Hour

type Options struct {
	Ledger     *ledger.Ledger
	Cluster    *cluster.Client
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	Interval           time.Duration
	ClockOffset        time.Duration // testing only: pretend it is this much later
	TransitionDelay    time.Duration // keep old copies this long after a transition
	DefaultReplication int
	Now                func() time.Time
}

// Stats counts what a pass did.
type Stats struct {
	Expired, NoncurrentRemoved, MarkersRemoved, UploadsAborted, NuggetsTransitioned int
}

type Strata struct {
	opts    Options
	log     *slog.Logger
	actions *prometheus.CounterVec
	passes  prometheus.Counter
}

func New(opts Options) *Strata {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Hour
	}
	if opts.TransitionDelay <= 0 {
		opts.TransitionDelay = 10 * time.Minute
	}
	if opts.DefaultReplication <= 0 {
		opts.DefaultReplication = 3
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	f := promauto.With(opts.Registerer)
	return &Strata{
		opts: opts, log: opts.Logger,
		actions: f.NewCounterVec(prometheus.CounterOpts{Namespace: "lode", Subsystem: "strata", Name: "actions_total",
			Help: "Lifecycle actions taken, by action."}, []string{"action"}),
		passes: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "strata", Name: "passes_total",
			Help: "Completed lifecycle passes."}),
	}
}

// now is the lifecycle clock.
func (s *Strata) now() time.Time { return s.opts.Now().Add(s.opts.ClockOffset) }

// Run executes passes until ctx is done while holding the leader lock.
func (s *Strata) Run(ctx context.Context) {
	if s.opts.ClockOffset != 0 {
		s.log.Warn("lifecycle clock is offset; this is for testing only", "offset", s.opts.ClockOffset)
	}
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		release, ok, err := s.opts.Ledger.TryLeader(ctx, LeaderLock)
		if err != nil || !ok {
			continue
		}
		st, err := s.RunOnce(ctx)
		release()
		if err != nil && ctx.Err() == nil {
			s.log.Error("lifecycle pass", "err", err)
		} else if st != (Stats{}) {
			s.log.Info("lifecycle pass", "expired", st.Expired, "noncurrent_removed", st.NoncurrentRemoved,
				"markers_removed", st.MarkersRemoved, "uploads_aborted", st.UploadsAborted, "nuggets_transitioned", st.NuggetsTransitioned)
		}
	}
}

func tagsMatch(want, have map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func (s *Strata) count(action string, n *int) {
	*n++
	s.actions.WithLabelValues(action).Inc()
}

// RunOnce applies every enabled rule of every bucket once.
func (s *Strata) RunOnce(ctx context.Context) (Stats, error) {
	var st Stats
	buckets, err := s.opts.Ledger.LifecycleBuckets(ctx)
	if err != nil {
		return st, err
	}
	now := s.now()
	var errs []error
	for bucket, rules := range buckets {
		for _, r := range rules {
			if !r.Enabled {
				continue
			}
			if err := s.applyRule(ctx, bucket, r, now, &st); err != nil {
				errs = append(errs, fmt.Errorf("bucket %s rule %s: %w", bucket, r.ID, err))
			}
		}
	}
	s.passes.Inc()
	if st != (Stats{}) {
		s.opts.Events.Publish(events.Event{Type: "lifecycle.pass", Component: "strata", Attrs: map[string]any{
			"expired": st.Expired, "noncurrent_removed": st.NoncurrentRemoved, "markers_removed": st.MarkersRemoved,
			"uploads_aborted": st.UploadsAborted, "nuggets_transitioned": st.NuggetsTransitioned,
		}})
	}
	return st, errors.Join(errs...)
}

func (s *Strata) applyRule(ctx context.Context, bucket string, r ledger.LifecycleRule, now time.Time, st *Stats) error {
	L := s.opts.Ledger
	if r.AbortIncompleteDays > 0 {
		ups, err := L.StaleUploads(ctx, bucket, r.Prefix, now.Add(-time.Duration(r.AbortIncompleteDays)*day), 1000)
		if err != nil {
			return err
		}
		for _, u := range ups {
			if err := L.AbortUpload(ctx, u.Bucket, u.Key, u.UploadID); err != nil && !errors.Is(err, ledger.ErrNoSuchUpload) {
				return err
			}
			s.count("abort_upload", &st.UploadsAborted)
		}
	}
	needsVersions := r.ExpirationDays > 0 || r.ExpirationDate != nil || r.ExpiredObjectDeleteMarker ||
		r.NoncurrentDays > 0 || len(r.Transitions) > 0
	if !needsVersions {
		return nil
	}
	var afterKey string
	var afterSeq int64
	for {
		page, last, err := L.LifecycleVersions(ctx, bucket, r.Prefix, afterKey, afterSeq, 1000)
		if err != nil {
			return err
		}
		for _, v := range page {
			if err := s.applyToVersion(ctx, bucket, r, v, now, st); err != nil {
				return err
			}
		}
		if len(page) < 1000 {
			return nil
		}
		afterKey, afterSeq = page[len(page)-1].Key, last
	}
}

func (s *Strata) applyToVersion(ctx context.Context, bucket string, r ledger.LifecycleRule, v ledger.VersionInfo, now time.Time, st *Stats) error {
	L := s.opts.Ledger
	if len(r.Tags) > 0 && (v.IsDeleteMarker || !tagsMatch(r.Tags, v.Tags)) {
		return nil
	}
	switch {
	case v.IsCurrent && !v.IsDeleteMarker:
		expired := (r.ExpirationDays > 0 && !now.Before(v.Created.Add(time.Duration(r.ExpirationDays)*day))) ||
			(r.ExpirationDate != nil && !now.Before(*r.ExpirationDate))
		if expired {
			if _, err := L.DeleteObject(ctx, bucket, v.Key, ""); err != nil {
				return err
			}
			s.count("expire", &st.Expired)
			return nil
		}
		// The latest transition whose age has passed wins.
		var target *ledger.Transition
		for i, t := range r.Transitions {
			if !now.Before(v.Created.Add(time.Duration(t.Days)*day)) && (target == nil || t.Days > target.Days) {
				target = &r.Transitions[i]
			}
		}
		if target != nil && v.ManifestID != nil {
			return s.transition(ctx, *v.ManifestID, *target, st)
		}
	case v.IsCurrent && v.IsDeleteMarker:
		if r.ExpiredObjectDeleteMarker && v.OnlyVersion {
			if _, err := L.DeleteObject(ctx, bucket, v.Key, v.VersionID); err != nil {
				return err
			}
			s.count("remove_delete_marker", &st.MarkersRemoved)
		}
	default: // noncurrent
		if r.NoncurrentDays > 0 && v.NoncurrentSince != nil && v.NoncurrentRank >= r.NewerNoncurrentVersions &&
			!now.Before(v.NoncurrentSince.Add(time.Duration(r.NoncurrentDays)*day)) {
			if _, err := L.DeleteObject(ctx, bucket, v.Key, v.VersionID); err != nil {
				return err
			}
			s.count("expire_noncurrent", &st.NoncurrentRemoved)
		}
	}
	return nil
}

// transition re-stores every nugget of a manifest in the target class.
func (s *Strata) transition(ctx context.Context, manifestID int64, t ledger.Transition, st *Stats) error {
	ids, err := s.opts.Ledger.VersionNuggets(ctx, manifestID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		moved, err := s.Transcode(ctx, id, t.ECData, t.ECParity)
		if err != nil {
			s.log.Warn("transition deferred", "nugget", id.String(), "class", t.Class, "err", err)
			continue // retried next pass
		}
		if moved {
			s.count("transition", &st.NuggetsTransitioned)
		}
	}
	return nil
}

// Transcode stores id in layout k+m (0+0 = replicated): it writes the full
// new layout, records it, then switches the nugget and schedules the old
// copies for deletion after TransitionDelay. It reports whether anything moved.
func (s *Strata) Transcode(ctx context.Context, id nugget.ID, k, m int) (bool, error) {
	L, C := s.opts.Ledger, s.opts.Cluster
	ns, ok, err := L.Nugget(ctx, id)
	if err != nil || !ok || ns.Refs == 0 {
		return false, err
	}
	if ns.ECData == k && ns.ECParity == m {
		return false, nil
	}
	data, err := C.Read(ctx, ledger.NuggetRef{ID: id, Length: ns.Length, Replicas: ns.Replicas,
		ECData: ns.ECData, ECParity: ns.ECParity, Shards: ns.Shards})
	if err != nil {
		return false, err
	}
	placer, _ := survey.ByName("", nil)
	replication := max(ns.Replication, s.opts.DefaultReplication)
	if k > 0 {
		var have []ledger.ShardLoc
		for _, sh := range ns.Shards { // leftovers from an interrupted transition
			if sh.Index < k+m {
				have = append(have, sh)
			}
		}
		written, err := C.StoreEC(ctx, id, data, have, k, m, k+m, placer)
		if aerr := L.AddShards(ctx, id, written); aerr != nil {
			return false, aerr
		}
		if err != nil {
			return false, err
		}
	} else {
		written, err := C.Store(ctx, id, data, ns.Replicas, replication, replication, placer)
		if aerr := L.AddReplicas(ctx, id, written); aerr != nil {
			return false, aerr
		}
		if err != nil {
			return false, err
		}
	}
	if err := L.SwitchLayout(ctx, id, k, m, replication, s.opts.TransitionDelay); err != nil {
		return false, err
	}
	s.opts.Events.Publish(events.Event{Type: "nugget.transitioned", Component: "strata", Attrs: map[string]any{
		"nugget": id.String(), "from": layoutName(ns.ECData, ns.ECParity), "to": layoutName(k, m)}})
	return true, nil
}

func layoutName(k, m int) string {
	if k == 0 {
		return "STANDARD"
	}
	return fmt.Sprintf("EC_%d_%d", k, m)
}
