package node

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// D30: a data dir is never used by the other metadata mode.
func TestRefusesToMixMetadataModes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.MetricsAddr = "" // tests bind no metrics port (M8 plan R1)
	cfg.DataDir = dir
	cfg.ListenAddr = "127.0.0.1:0"

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft"), 0o750))
	_, err := Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.ErrorContains(t, err, "other metadata mode")

	dir2 := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir2, local.FileName), []byte("{}"), 0o600))
	cfg.DataDir = dir2
	cfg.Cluster = []config.ClusterMember{{ID: 1, Raft: "127.0.0.1:1", Internal: "127.0.0.1:2", Client: "127.0.0.1:3"}}
	_, err = Start(context.Background(), cfg, slog.New(slog.DiscardHandler))
	require.ErrorContains(t, err, "other metadata mode")
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(strings.TrimSpace(b.buf.String()), "\n")
}

// TestLogsCarryBrokerIDAndMetricsServe (M8 plan R1, R10): every structured log line of a node
// carries broker_id, and /metrics is served on a pre-bound listener.
func TestLogsCarryBrokerIDAndMetricsServe(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.MetricsAddr = ""
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.BrokerID = 7
	var logs syncBuffer
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	n, err := StartWithOptions(ctx, cfg, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), Options{Metrics: lis})
	require.NoError(t, err)
	require.Equal(t, lis.Addr().String(), n.MetricsAddr())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+n.MetricsAddr()+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NoError(t, n.Close(ctx))
	lines := logs.lines()
	require.NotEmpty(t, lines)
	for _, line := range lines {
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec), line)
		require.EqualValues(t, 7, rec["broker_id"], "log line without broker_id: %s", line)
		require.Equal(t, 1, strings.Count(line, `"broker_id"`), "duplicate broker_id: %s", line)
	}
}
