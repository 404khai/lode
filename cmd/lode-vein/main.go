// Command lode-vein runs a storage node.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	lodev1 "github.com/404khai/lode/gen/lode/v1"
	"github.com/404khai/lode/internal/assay"
	"github.com/404khai/lode/internal/config"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/logging"
	"github.com/404khai/lode/internal/vein"
	"github.com/404khai/lode/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "lode-vein:", err)
		os.Exit(1)
	}
}

// veinID returns the configured ID, or one persisted in the data directory
// so a vein keeps its identity across restarts.
func veinID(cfg config.Vein) (string, error) {
	if cfg.ID != "" {
		return cfg.ID, nil
	}
	path := filepath.Join(cfg.DataDir, "VEIN_ID")
	if b, err := os.ReadFile(path); err == nil { // #nosec G304 -- inside the configured data dir
		return strings.TrimSpace(string(b)), nil
	}
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	id := "vein-" + hex.EncodeToString(b)
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		return "", err
	}
	return id, os.WriteFile(path, []byte(id+"\n"), 0o600)
}

func run() error {
	cfgPath := flag.String("config", os.Getenv("LODE_CONFIG"), "path to YAML config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version.Version, version.Commit)
		return nil
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	if err := cfg.ValidateVein(); err != nil {
		return err
	}
	id, err := veinID(cfg.Vein)
	if err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.Log.Level, cfg.Log.Format, "vein").With("vein", id)
	if cfg.Vein.Host == "" {
		cfg.Vein.Host, _ = os.Hostname()
	}
	advertise := cfg.Vein.Advertise
	if advertise == "" {
		advertise = cfg.Vein.Listen
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	store, err := vein.Open(vein.Options{Dir: cfg.Vein.DataDir, Logger: log, Registerer: reg})
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("close store", "err", err)
		}
	}()

	pool, err := pgxpool.New(ctx, cfg.Ledger.DSN)
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	defer pool.Close()
	led := ledger.New(pool)
	if err := waitLedger(ctx, led, log); err != nil {
		return err
	}
	if err := ledger.Migrate(ctx, pool); err != nil {
		return err
	}
	info := func() ledger.VeinInfo {
		capacity, used, _ := store.Usage()
		ioErrs, corrupt := store.Health()
		return ledger.VeinInfo{ID: id, Addr: advertise, Zone: cfg.Vein.Zone, Host: cfg.Vein.Host,
			CapacityBytes: capacity, UsedBytes: used, IOErrors: ioErrs, CorruptNuggets: corrupt}
	}
	if err := led.RegisterVein(ctx, info()); err != nil {
		return fmt.Errorf("register vein: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.Vein.Listen)
	if err != nil {
		return err
	}
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(4 << 20))
	lodev1.RegisterVeinServiceServer(gs, vein.NewServer(store, id))
	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(lis) }()
	log.Info("vein serving", "listen", cfg.Vein.Listen, "advertise", advertise, "zone", cfg.Vein.Zone, "host", cfg.Vein.Host,
		"data_dir", cfg.Vein.DataDir, "version", version.Version)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	ms := &http.Server{Addr: cfg.Metrics.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := ms.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	go heartbeat(ctx, led, cfg.Vein.Heartbeat, info, log)
	go compactLoop(ctx, store, cfg.Vein.CompactInterval, cfg.Vein.CompactRatio, log)
	if cfg.Vein.ScrubPause >= 0 {
		scrub := assay.New(assay.Options{
			Store: store, Ledger: led, VeinID: id, Logger: log.With("component", "assay"), Registerer: reg,
			BytesPerSecond: cfg.Vein.ScrubRate, Pause: cfg.Vein.ScrubPause,
		})
		go scrub.Run(ctx)
	}

	select {
	case <-ctx.Done():
	case err = <-errc:
		log.Error("server failed", "err", err)
	}
	gs.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ms.Shutdown(shutdownCtx)
	log.Info("stopped")
	return err
}

func waitLedger(ctx context.Context, led *ledger.Ledger, log *slog.Logger) error {
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		if err = led.Ping(ctx); err == nil {
			return nil
		}
		log.Warn("waiting for ledger", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("ledger: %w", err)
}

func heartbeat(ctx context.Context, led *ledger.Ledger, every time.Duration, info func() ledger.VeinInfo, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := led.Heartbeat(ctx, info()); err != nil && ctx.Err() == nil {
				log.Warn("heartbeat", "err", err)
			}
		}
	}
}

func compactLoop(ctx context.Context, store *vein.Store, every time.Duration, ratio float64, log *slog.Logger) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			st, err := store.Compact(ratio)
			if err != nil {
				log.Error("compaction", "err", err)
			} else if st.SegmentsRemoved > 0 {
				log.Info("compaction", "segments_removed", st.SegmentsRemoved, "bytes_reclaimed", st.BytesReclaimed, "nuggets_moved", st.NuggetsMoved)
			}
		}
	}
}
