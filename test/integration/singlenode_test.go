//go:build integration

package integration

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
)

func startInProcess(t *testing.T, dir string) *node.Node {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = dir
	cfg.ListenAddr = "127.0.0.1:0"
	n, err := node.Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	return n
}

// TestSingleNode100kKeyed is the M2 acceptance test (SPEC §9): 100k keyed records across 6
// partitions; consume all; per-key order verified; restart the broker and re-read everything.
func TestSingleNode100kKeyed(t *testing.T) {
	const (
		topic      = "orders"
		partitions = 6
		total      = 100_000
		keys       = 1000
	)
	dir := t.TempDir()
	n := startInProcess(t, dir)
	c := dial(t, n.Addr())
	createTopic(t, c, topic, partitions)

	recs := generate(total, keys)
	produceKeyed(t, c, topic, partitions, recs, 500, nil)

	first := consumeAll(t, c, topic, partitions)
	require.Len(t, first, total)
	counts := verifyPerKeyOrder(t, first, partitions)
	require.Len(t, counts, keys)
	for _, n := range counts {
		require.Equal(t, total/keys, n)
	}
	perPartition := map[int32]int{}
	for _, r := range first {
		perPartition[r.partition]++
	}
	require.Len(t, perPartition, partitions, "every partition received data")

	// Graceful restart on the same data dir; everything must read back identically.
	require.NoError(t, n.Close(context.Background()))
	n = startInProcess(t, dir)
	defer func() { require.NoError(t, n.Close(context.Background())) }()
	c = dial(t, n.Addr())
	again := consumeAll(t, c, topic, partitions)
	require.Equal(t, first, again)

	// And the restarted broker keeps accepting writes after the existing data.
	produceKeyed(t, c, topic, partitions, []keyedRecord{{key: "key-1", seq: total / keys}}, 1, nil)
	after := consumeAll(t, c, topic, partitions)
	require.Len(t, after, total+1)
	verifyPerKeyOrder(t, after, partitions)
}
