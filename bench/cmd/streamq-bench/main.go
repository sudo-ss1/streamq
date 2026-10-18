// Command streamq-bench is the streamq load generator (SPEC §9 M10). It measures produce
// throughput and latency against in-process brokers or an external cluster; see bench/USAGE.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sudo-ss1/streamq/bench/internal/loadgen"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run returns the exit status: 0 OK, 1 INVALID or failed, 2 usage error.
func run(args []string) int {
	cfg, err := loadgen.Parse(args, os.Stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case err != nil:
		_, _ = fmt.Fprintf(os.Stderr, "streamq-bench: %v\n", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop) // a second signal terminates immediately

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	res, err := loadgen.Run(ctx, cfg, logger)
	if res != nil {
		loadgen.PrintTable(os.Stdout, res)
		if cfg.JSONPath != "" {
			if jerr := loadgen.WriteJSON(cfg.JSONPath, res); jerr != nil {
				err = errors.Join(err, jerr)
			}
		}
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "streamq-bench: %v\n", err)
		return 1
	}
	if res.Status != loadgen.StatusOK {
		return 1
	}
	return 0
}
