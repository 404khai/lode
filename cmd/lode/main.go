// Command lode is the operator CLI. It talks to the Ledger directly
// (LODE_LEDGER_DSN or -dsn).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/version"
)

const usage = `usage: lode [-dsn DSN] <command>

commands:
  veins                      list veins with state, usage and disk health
  vein drain <id>            move every copy off a vein, then retire it
  vein undrain <id>          return a draining vein to service
  vein remove <id> [-force]  retire a vein now and forget its copies
  repairs                    show the repair queue and redundancy
  version                    print the version
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "lode:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("lode", flag.ContinueOnError)
	dsn := fs.String("dsn", os.Getenv("LODE_LEDGER_DSN"), "Ledger Postgres DSN")
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return err
	}
	args = fs.Args()
	if len(args) == 0 {
		fs.Usage()
		return errors.New("missing command")
	}
	if args[0] == "version" {
		fmt.Println(version.Version, version.Commit)
		return nil
	}
	if *dsn == "" {
		return errors.New("set LODE_LEDGER_DSN or -dsn")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	led := ledger.New(pool)

	switch args[0] {
	case "veins":
		return listVeins(ctx, led)
	case "repairs":
		st, err := led.RepairStats(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("queued %d  leased %d  lost %d  under-replicated nuggets %d\n", st.Queued, st.Leased, st.Lost, st.UnderReplicated)
		return nil
	case "vein":
		return veinCommand(ctx, led, args[1:])
	}
	fs.Usage()
	return fmt.Errorf("unknown command %q", args[0])
}

func listVeins(ctx context.Context, led *ledger.Ledger) error {
	vs, err := led.ListVeins(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "ID\tSTATE\tZONE\tHOST\tADDR\tUSED\tCAPACITY\tIO ERRORS\tCORRUPT\tLAST HEARTBEAT")
	for _, v := range vs {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s ago\n", v.ID, v.State, v.Zone, v.Host, v.Addr,
			bytes(v.UsedBytes), bytes(v.CapacityBytes), v.IOErrors, v.CorruptNuggets, time.Since(v.LastHeartbeat).Round(time.Second))
	}
	return w.Flush()
}

func veinCommand(ctx context.Context, led *ledger.Ledger, args []string) error {
	fs := flag.NewFlagSet("vein", flag.ContinueOnError)
	force := fs.Bool("force", false, "remove even if the vein holds the last copy of some nuggets")
	if len(args) < 2 {
		return errors.New("usage: lode vein drain|undrain|remove <id>")
	}
	action, id := args[0], args[1]
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	switch action {
	case "drain":
		prev, err := led.SetVeinState(ctx, id, ledger.VeinDraining)
		if err != nil {
			return err
		}
		n, err := led.EnqueueVeinRepairs(ctx, id, ledger.ReasonDrain)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s -> draining; %d copies queued to move\n", id, prev, n)
	case "undrain":
		prev, err := led.SetVeinState(ctx, id, ledger.VeinHealthy)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %s -> healthy\n", id, prev)
	case "remove":
		if err := led.RemoveVein(ctx, id, *force); err != nil {
			return err
		}
		fmt.Printf("%s: removed\n", id)
	default:
		return fmt.Errorf("unknown vein action %q", action)
	}
	return nil
}

func bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
