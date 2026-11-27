// Command lode-adit runs the S3-compatible gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/404khai/lode/internal/adit"
	"github.com/404khai/lode/internal/config"
	"github.com/404khai/lode/internal/logging"
	"github.com/404khai/lode/internal/version"
)

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
	log := logging.New(os.Stderr, cfg.Log.Level, cfg.Log.Format, "adit")

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })

	servers := []*http.Server{
		{Addr: cfg.Adit.Listen, Handler: adit.New(log, reg), ReadHeaderTimeout: 10 * time.Second},
		{Addr: cfg.Metrics.Listen, Handler: metricsMux, ReadHeaderTimeout: 10 * time.Second},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
