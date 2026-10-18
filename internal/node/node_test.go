package node

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
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
