package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
)

// TestAgainstRealBroker runs the CLI against an in-process broker: create, describe, produce,
// consume, delete.
func TestAgainstRealBroker(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TopicDefaults.ReplicationFactor = 1
	cfg.TopicDefaults.MinInsyncReplicas = 1
	n, err := node.Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer func() { require.NoError(t, n.Close(context.Background())) }()

	run := func(stdin string, args ...string) string {
		t.Helper()
		var out, errOut bytes.Buffer
		cmd := newRootCmd(strings.NewReader(stdin), &out, &errOut)
		cmd.SetArgs(append([]string{"--bootstrap", n.Addr()}, args...))
		require.NoError(t, cmd.ExecuteContext(context.Background()), errOut.String())
		return out.String()
	}
	run("", "topics", "create", "events", "--partitions", "2")
	require.Contains(t, run("", "topics", "list"), "events")
	require.Contains(t, run("", "topics", "describe", "events"), "events")
	run("k1:a\nk2:b\nk1:c\nnokey\n", "produce", "--topic", "events", "--key-separator", ":")
	got := run("", "consume", "--topic", "events", "--from-beginning")
	lines := strings.Fields(got)
	require.ElementsMatch(t, []string{"a", "b", "c", "nokey"}, lines)
	require.Less(t, strings.Index(got, "a"), strings.Index(got, "c"), "per-key order")
	run("", "topics", "delete", "events")
	require.NotContains(t, run("", "topics", "list"), "events")
}
