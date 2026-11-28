// Command lode-lantern serves the Lantern cluster explorer.
//
//	lode-lantern -listen :8080 -ui web/dist
//
// Browsers sign in with HTTP basic auth using an access key and its secret.
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/config"
	"github.com/404khai/lode/internal/foreman"
	"github.com/404khai/lode/internal/lantern"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/logging"
	"github.com/404khai/lode/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "lode-lantern:", err)
		os.Exit(1)
	}
}

func basicAuth(keys []config.AccessKey, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ak, sk, ok := r.BasicAuth()
		if ok {
			for _, k := range keys {
				if subtle.ConstantTimeCompare([]byte(k.AccessKey), []byte(ak)) == 1 &&
					subtle.ConstantTimeCompare([]byte(k.SecretKey), []byte(sk)) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="Lantern (access key / secret key)"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func run() error {
	cfgPath := flag.String("config", os.Getenv("LODE_CONFIG"), "path to YAML config (ledger, adit.access_keys)")
	listen := flag.String("listen", ":8080", "HTTP listen address")
	uiDir := flag.String("ui", "web/dist", "directory with the built web UI")
	allowWrite := flag.Bool("allow-write", false, "let the Foreman panel run write tools")
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
	if cfg.Ledger.DSN == "" {
		return errors.New("ledger.dsn (LODE_LEDGER_DSN) is required")
	}
	log := logging.New(os.Stderr, cfg.Log.Level, cfg.Log.Format, "lantern")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.Ledger.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	led := ledger.New(pool)
	veins := cluster.New(cluster.Options{Ledger: led, Logger: log.With("component", "cluster"), StaleAfter: cfg.Cluster.StaleAfter})
	defer veins.Close()
	if err := veins.Refresh(ctx); err != nil {
		return err
	}
	go veins.Run(ctx)

	var ui http.Handler
	if st, err := os.Stat(*uiDir); err == nil && st.IsDir() {
		ui = http.FileServer(http.Dir(*uiDir))
	} else {
		log.Warn("UI directory not found; serving the API only", "ui", *uiDir)
	}
	srv := lantern.New(lantern.Options{
		Pool: pool, Ledger: led, Cluster: veins, Logger: log, UI: ui,
		Foreman: foreman.New(foreman.Options{Ledger: led, Cluster: veins, AllowWrite: *allowWrite, Logger: log,
			DefaultReplication: cfg.Adit.Replication, DefaultPlacement: cfg.Adit.Placement}),
	})
	go srv.Run(ctx)
	hs := &http.Server{Addr: *listen, Handler: basicAuth(cfg.Adit.AccessKeys, srv), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(c)
	}()
	log.Info("lantern serving", "listen", *listen, "ui", *uiDir, "version", version.Version)
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
