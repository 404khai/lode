// Command lode-adit runs the S3-compatible gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/adit"
	"github.com/404khai/lode/internal/config"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/logging"
	"github.com/404khai/lode/internal/tailings"
	"github.com/404khai/lode/internal/vein"
	"github.com/404khai/lode/internal/version"
)

// connectLedger waits for Postgres to accept connections.
func connectLedger(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		if err = pool.Ping(ctx); err == nil {
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "lode-adit:", err)
		os.Exit(1)
	}
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
	if err := cfg.Validate(); err != nil {
		return err
	}
	log := logging.New(os.Stderr, cfg.Log.Level, cfg.Log.Format, "adit")

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.Ledger.DSN)
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	defer pool.Close()
	if err := connectLedger(ctx, pool, log); err != nil {
		return err
	}
	if err := ledger.Migrate(ctx, pool); err != nil {
		return err
	}

	bus := events.NewBus()
	store, err := vein.Open(vein.Options{Dir: cfg.Vein.DataDir, Logger: log.With("component", "vein"), Registerer: reg, Events: bus})
	if err != nil {
		return err
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("close vein", "err", err)
		}
	}()

	creds := make(map[string]string, len(cfg.Adit.AccessKeys))
	for _, k := range cfg.Adit.AccessKeys {
		creds[k.AccessKey] = k.SecretKey
	}
	led := ledger.New(pool)
	srv := adit.New(adit.Options{
		Logger: log, Registerer: reg, Ledger: led, Vein: store, Events: bus,
		Region: cfg.Adit.Region, Credentials: creds,
	})

	gc := tailings.New(tailings.Options{
		Ledger: led, Vein: store, Logger: log.With("component", "tailings"), Registerer: reg, Events: bus,
		Grace: cfg.Tailings.Grace, Interval: cfg.Tailings.Interval,
		UploadExpiry: cfg.Tailings.UploadExpiry, CompactRatio: cfg.Tailings.CompactRatio,
	})
	gcDone := make(chan struct{})
	go func() { defer close(gcDone); gc.Run(ctx) }()
	defer func() { stop(); <-gcDone }()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := led.Ping(r.Context()); err != nil {
			http.Error(w, "ledger unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})

	servers := []*http.Server{
		{Addr: cfg.Adit.Listen, Handler: srv, ReadHeaderTimeout: 10 * time.Second},
		{Addr: cfg.Metrics.Listen, Handler: metricsMux, ReadHeaderTimeout: 10 * time.Second},
	}

	errc := make(chan error, len(servers))
	for _, s := range servers {
		log.Info("listening", "addr", s.Addr, "version", version.Version)
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}(s)
	}

	select {
	case <-ctx.Done():
	case err = <-errc:
		log.Error("server failed", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdownCtx)
	}
	log.Info("stopped")
	return err
}
