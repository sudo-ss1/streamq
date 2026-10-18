//go:build integration

package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildBroker compiles cmd/streamq-broker into a temp dir.
func buildBroker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "streamq-broker")
	cmd := exec.CommandContext(testCtx(t), "go", "build", "-o", bin, "github.com/sudo-ss1/streamq/cmd/streamq-broker")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return bin
}

// brokerProc is a broker subprocess.
type brokerProc struct {
	cmd     *exec.Cmd
	addr    string
	drained sync.WaitGroup
}

// startProc runs the broker binary on dataDir and waits for its "serving" log line.
func startProc(t *testing.T, bin, dataDir string) *brokerProc {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "broker.yaml")
	cfg := fmt.Sprintf("broker_id: 1\nlisten_addr: \"127.0.0.1:0\"\nadvertised_host: \"127.0.0.1\"\ndata_dir: %q\n"+
		"topic_defaults:\n  replication_factor: 1\n  min_insync_replicas: 1\n  flush_policy: interval\n", dataDir)
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o600))
	cmd := exec.Command(bin, "--config", cfgPath) //nolint:noctx // lifetime managed explicitly via Kill/Wait in cleanup
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	p := &brokerProc{cmd: cmd}
	t.Cleanup(func() { p.kill(t) })

	addrCh := make(chan string, 1)
	p.drained.Add(1)
	go func() {
		defer p.drained.Done()
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			var line struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal(sc.Bytes(), &line) == nil && line.Msg == "serving" {
				select {
				case addrCh <- line.Addr:
				default:
				}
			}
		}
		_, _ = io.Copy(io.Discard, stderr)
	}()
	select {
	case p.addr = <-addrCh:
	case <-time.After(30 * time.Second):
		t.Fatal("broker did not start")
	}
	return p
}

// kill sends SIGKILL (no shutdown, no flush) and reaps the process. Idempotent.
func (p *brokerProc) kill(t *testing.T) {
	t.Helper()
	if p.cmd.ProcessState == nil {
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		_ = p.cmd.Wait()
	}
	p.drained.Wait()
}

// TestKillAndRestartKeepsAcknowledgedRecords kills the broker with SIGKILL (no flush, no
// checkpoint; the OS page cache survives a process crash) and verifies every acknowledged record is present and in per-key order after a
// restart on the same data dir (FR9; plan R8).
func TestKillAndRestartKeepsAcknowledgedRecords(t *testing.T) {
	const (
		topic      = "events"
		partitions = 6
	)
	bin := buildBroker(t)
	dir := t.TempDir()
	p := startProc(t, bin, dir)
	c := dial(t, p.addr)
	createTopic(t, c, topic, partitions)

	recs := generate(20_000, 200)
	var acked []keyedRecord
	produceKeyed(t, c, topic, partitions, recs, 100, func(b []keyedRecord) { acked = append(acked, b...) })
	p.kill(t)

	p = startProc(t, bin, dir)
	c = dial(t, p.addr)
	got := consumeAll(t, c, topic, partitions)
	require.Len(t, got, len(acked))
	verifyPerKeyOrder(t, got, partitions)

	// The restarted broker accepts more writes for the same keys, in order.
	produceKeyed(t, c, topic, partitions, generate(40_000, 200)[20_000:], 100, nil)
	got = consumeAll(t, c, topic, partitions)
	require.Len(t, got, 40_000)
	verifyPerKeyOrder(t, got, partitions)
}
