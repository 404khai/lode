// Command lode-foreman runs Foreman, Lode's MCP server for agents.
//
//	lode-foreman                       # stdio, read-only
//	lode-foreman -allow-write          # stdio with write tools
//	lode-foreman -http :9100           # streamable HTTP; clients send
//	                                   #   Authorization: Bearer <access-key>:<secret-key>
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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/config"
	"github.com/404khai/lode/internal/foreman"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/logging"
	"github.com/404khai/lode/internal/version"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "lode-foreman:", err)
		os.Exit(1)
	}
}

// authorize checks "Authorization: Bearer <access-key>:<secret-key>" against
// the configured Adit access keys.
func authorize(keys []config.AccessKey, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		ak, sk, ok2 := strings.Cut(token, ":")
		if ok && ok2 {
			for _, k := range keys {
				if subtle.ConstantTimeCompare([]byte(k.AccessKey), []byte(ak)) == 1 &&
					subtle.ConstantTimeCompare([]byte(k.SecretKey), []byte(sk)) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="lode-foreman"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func run() error {
	cfgPath := flag.String("config", os.Getenv("LODE_CONFIG"), "path to YAML config (ledger, adit.access_keys)")
	allowWrite := flag.Bool("allow-write", false, "register tools that change the cluster (drain, quotas, lifecycle, ...)")
	httpAddr := flag.String("http", "", "serve streamable HTTP on this address instead of stdio")
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
	if *httpAddr != "" && len(cfg.Adit.AccessKeys) == 0 {
		return errors.New("HTTP transport needs at least one access key (adit.access_keys or LODE_ACCESS_KEY/LODE_SECRET_KEY)")
	}
	// stdio carries the protocol on stdout, so logs always go to stderr.
	log := logging.New(os.Stderr, cfg.Log.Level, cfg.Log.Format, "foreman")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.Ledger.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	led := ledger.New(pool)
	if err := led.Ping(ctx); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	veins := cluster.New(cluster.Options{Ledger: led, Logger: log.With("component", "cluster"), StaleAfter: cfg.Cluster.StaleAfter})
	defer veins.Close()
	if err := veins.Refresh(ctx); err != nil {
		return err
	}
	go veins.Run(ctx)

	f := foreman.New(foreman.Options{
		Ledger: led, Cluster: veins, Logger: log, AllowWrite: *allowWrite,
		DefaultReplication: cfg.Adit.Replication, DefaultPlacement: cfg.Adit.Placement,
	})
	log.Info("foreman starting", "allow_write", *allowWrite, "http", *httpAddr, "version", version.Version)

	if *httpAddr == "" {
		return f.Server().Run(ctx, &mcp.StdioTransport{})
	}
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return f.Server() }, nil)
	srv := &http.Server{Addr: *httpAddr, Handler: authorize(cfg.Adit.AccessKeys, handler), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
