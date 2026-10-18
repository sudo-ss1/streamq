// Command streamq-broker runs a streamq broker node.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	var configPath string
	flagValues := map[string]*string{}
	cmd := &cobra.Command{
		Use:          "streamq-broker",
		Short:        "Run a streamq broker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			flags := map[string]string{}
			for key, v := range flagValues {
				if cmd.Flags().Changed(flagName(key)) {
					flags[key] = *v
				}
			}
			cfg, err := config.Load(configPath, os.Environ(), flags)
			if err != nil {
				return fmt.Errorf("load config: %w", err)
			}
			return run(cmd.Context(), cfg)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "path to the YAML config file")
	for _, key := range config.FlagKeys() {
		flagValues[key] = cmd.Flags().String(flagName(key), "", "overrides config key "+key)
	}
	return cmd
}

// flagName turns a dotted config key into a flag name: topic_defaults.partitions ->
// topic-defaults.partitions.
func flagName(key string) string { return strings.ReplaceAll(key, "_", "-") }

func run(ctx context.Context, cfg config.Config) error {
	level, err := config.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		return err // already validated by Load
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	n, err := node.Start(ctx, cfg, logger)
	if err != nil {
		logger.Error("startup failed", slog.Any("error", err))
		return err
	}
	<-ctx.Done()
	logger.Info("shutting down")
	if err := n.Close(context.WithoutCancel(ctx)); err != nil {
		logger.Error("shutdown failed", slog.Any("error", err))
		return err
	}
	logger.Info("stopped")
	return nil
}
