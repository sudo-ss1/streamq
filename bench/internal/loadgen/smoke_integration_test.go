//go:build integration

package loadgen

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSmokeInProcess runs a 2 s workload against one in-process broker, with a consumer, and
// checks the result and that the run's data dir is removed.
func TestSmokeInProcess(t *testing.T) {
	parent := t.TempDir()
	jsonPath := filepath.Join(t.TempDir(), "result.json")
	cfg, err := Parse([]string{
		"-cluster", "inprocess:1", "-duration", "2s", "-warmup", "500ms", "-producers", "2",
		"-rate", "2000", "-consume", "-key-cardinality", "16", "-data-dir", parent, "-json", jsonPath,
	}, os.Stderr)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := Run(ctx, cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, StatusOK, res.Status, "reasons: %v", res.InvalidReasons)
	require.Positive(t, res.Produce.Count)
	require.Positive(t, res.Produce.MsgsPerSec)
	require.Zero(t, res.Produce.Timeouts)
	require.Empty(t, res.Produce.ErrorsByKind)
	require.InDelta(t, 2000, res.Produce.MsgsPerSec, 400, "paced at -rate")
	require.NotNil(t, res.EndToEnd)
	require.Positive(t, res.EndToEnd.Count)
	require.Equal(t, "admin-describe", res.ClusterChecks.Source)
	require.Equal(t, "ack latency from intended send time", res.Produce.LatencyKind)
	require.Equal(t, 6, res.Config.Partitions)

	require.NoError(t, WriteJSON(jsonPath, res))
	data, err := os.ReadFile(filepath.Clean(jsonPath))
	require.NoError(t, err)
	var back Result
	require.NoError(t, json.Unmarshal(data, &back))
	require.Equal(t, StatusOK, back.Status)

	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Empty(t, entries, "the run's broker data is removed on exit")
}

// TestSmokeInterrupted cancels the run during the window and expects partial INVALID results.
func TestSmokeInterrupted(t *testing.T) {
	cfg, err := Parse([]string{"-duration", "1m", "-warmup", "200ms", "-producers", "1", "-rate", "500",
		"-data-dir", t.TempDir()}, os.Stderr)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := time.AfterFunc(1500*time.Millisecond, cancel)
	defer stop.Stop()
	res, err := Run(ctx, cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, StatusInvalid, res.Status)
	require.Contains(t, res.InvalidReasons, "interrupted: partial results")
	require.Positive(t, res.Produce.Count)
	require.Less(t, res.Produce.WindowSeconds, 30.0)
}

// TestSmokeInProcessCluster exercises the 3-broker path: concurrent start, waiting for unfencing,
// and the metadata-store stability check (review).
func TestSmokeInProcessCluster(t *testing.T) {
	cfg, err := Parse([]string{
		"-cluster", "inprocess:3", "-duration", "1s", "-warmup", "500ms", "-producers", "1",
		"-rate", "500", "-partitions", "3", "-data-dir", t.TempDir(),
	}, os.Stderr)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	res, err := Run(ctx, cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, StatusOK, res.Status, "reasons: %v", res.InvalidReasons)
	require.Equal(t, "metadata-store", res.ClusterChecks.Source)
	require.Positive(t, res.Produce.Count)
}
