// Package testnode starts in-process single-node brokers for tests and examples.
package testnode

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
)

// Start runs a broker on 127.0.0.1 with a temp data dir (RF 1, min.insync 1 defaults) and stops it
// when the test ends. It returns the client address.
func Start(tb testing.TB) string {
	tb.Helper()
	n := StartIn(tb, tb.TempDir())
	tb.Cleanup(func() { require.NoError(tb, n.Close(context.Background())) })
	return n.Addr()
}

// StartIn runs a broker on dataDir; the caller closes it.
func StartIn(tb testing.TB, dataDir string) *node.Node {
	tb.Helper()
	return StartWith(tb, dataDir, nil)
}

// StartWith is StartIn with a config override applied after the test defaults.
func StartWith(tb testing.TB, dataDir string, mod func(*config.Config)) *node.Node {
	tb.Helper()
	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.AdvertisedHost = "127.0.0.1"
	cfg.TopicDefaults.ReplicationFactor = 1
	cfg.TopicDefaults.MinInsyncReplicas = 1
	if mod != nil {
		mod(&cfg)
	}
	n, err := node.Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(tb, err)
	return n
}
