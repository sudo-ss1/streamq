package loadgen

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCompareEpochs(t *testing.T) {
	stable := map[int32]partitionEpochs{
		0: {LeaderEpoch: 1, PartitionEpoch: 2, ISR: []int32{1, 2, 3}, Replicas: 3},
		1: {LeaderEpoch: 0, PartitionEpoch: 0, ISR: []int32{1, 2, 3}, Replicas: 3},
	}
	with := func(p int32, f func(*partitionEpochs)) map[int32]partitionEpochs {
		out := map[int32]partitionEpochs{}
		for k, v := range stable {
			out[k] = v
		}
		e := out[p]
		f(&e)
		out[p] = e
		return out
	}
	admin := map[int32]partitionEpochs{0: {LeaderEpoch: 1, PartitionEpoch: -1, ISR: []int32{1, 2, 3}, Replicas: 3}}
	tests := []struct {
		name          string
		before, after map[int32]partitionEpochs
		leader, part  int
		shrunk        int
		valid         bool
	}{
		{"stable", stable, stable, 0, 0, 0, true},
		{"leader change", stable, with(0, func(e *partitionEpochs) { e.LeaderEpoch++; e.PartitionEpoch++ }), 1, 1, 0, false},
		{"temporary ISR change", stable, with(1, func(e *partitionEpochs) { e.PartitionEpoch += 2 }), 0, 1, 0, false},
		{"ISR below RF", stable, with(1, func(e *partitionEpochs) { e.ISR = []int32{1, 2}; e.PartitionEpoch++ }), 0, 1, 1, false},
		{"missing partition", stable, map[int32]partitionEpochs{0: stable[0]}, 0, 0, 0, false},
		{"admin stable", admin, admin, 0, 0, 0, true},
		{"admin ISR change", admin, map[int32]partitionEpochs{0: {LeaderEpoch: 1, PartitionEpoch: -1, ISR: []int32{1, 3}, Replicas: 3}}, 0, 1, 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cc := compareEpochs("src", tc.before, tc.after)
			require.Equal(t, tc.leader, cc.LeaderEpochChanges)
			require.Equal(t, tc.part, cc.PartitionEpochChanges)
			require.Equal(t, tc.shrunk, cc.ShrunkISR)
			require.Equal(t, tc.valid, cc.valid())
		})
	}
}

func sampleResult(t *testing.T) *Result {
	t.Helper()
	cfg := DefaultConfig()
	cfg.resolve()
	cfg.Acks, cfg.Consume = AcksNone, true
	base := time.Unix(1_700_000_000, 0)
	r := NewRecorder(Window{WarmupEnd: base, End: base.Add(time.Second)})
	tok := r.Sent(base, base, 1024)
	r.Done(tok, base.Add(3*time.Millisecond), nil)
	tot := Merge([]*Recorder{r})
	return &Result{
		Status:        StatusOK,
		Config:        configReport(cfg, 6),
		Environment:   Environment{CPU: "cpu", Cores: 4, Commit: "abc", Dirty: true},
		Produce:       produceReport(cfg, tot, time.Second, 0),
		EndToEnd:      &EndToEndReport{Count: 1, Latency: percentiles(tot.Latency)},
		ClusterChecks: compareEpochs("admin-describe", nil, nil),
		RateModel:     RateModel,
	}
}

func TestWriteJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.json")
	require.NoError(t, WriteJSON(path, sampleResult(t)))
	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), st.Mode().Perm())
	data, err := os.ReadFile(filepath.Clean(path))
	require.NoError(t, err)

	var top map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &top))
	for _, k := range []string{"status", "config", "environment", "produce", "end_to_end", "cluster_checks", "rate_model"} {
		require.Contains(t, top, k)
	}
	var doc struct {
		RateModel   string `json:"rate_model"`
		Environment map[string]any
		Produce     map[string]any
		EndToEnd    map[string]any `json:"end_to_end"`
		Cluster     map[string]any `json:"cluster_checks"`
		Config      map[string]any
	}
	require.NoError(t, json.Unmarshal(data, &doc))
	require.Equal(t, "shared-token-bucket", doc.RateModel)
	for _, k := range []string{"cpu", "cores", "ram", "os", "kernel", "go", "commit", "dirty", "filesystem", "flush_policy", "peak_rss"} {
		require.Contains(t, doc.Environment, k)
	}
	for _, k := range []string{"count", "errors_by_kind", "msgs_per_sec", "mib_per_sec", "latency_ms", "service_ms", "behind_schedule", "timeouts"} {
		require.Contains(t, doc.Produce, k)
	}
	require.Contains(t, doc.EndToEnd, "latency_ms")
	for _, k := range []string{"leader_epoch_changes", "partition_epoch_changes"} {
		require.Contains(t, doc.Cluster, k)
	}
	require.Contains(t, doc.Config, "producer_config")
	lat, ok := doc.Produce["latency_ms"].(map[string]any)
	require.True(t, ok)
	for _, k := range []string{"p50", "p99", "p999", "max"} {
		require.Contains(t, lat, k)
	}
	require.InEpsilon(t, 1.0, doc.Produce["msgs_per_sec"], 1e-9)
}

func TestPrintTable(t *testing.T) {
	r := sampleResult(t)
	r.Status, r.InvalidReasons = StatusInvalid, []string{"interrupted: partial results"}
	r.Produce.ErrorsByKind = map[string]int64{"not_leader": 2, "canceled": 1}
	var out bytes.Buffer
	PrintTable(&out, r)
	for _, s := range []string{
		"status INVALID", "INVALID: interrupted", "send latency (acks=none", "throughput", "p999",
		"buffer.memory=33554432", "max.request.size=1048576", "canceled=1 not_leader=2", "end-to-end",
		"leader_epoch_changes=0", "commit abc-dirty",
	} {
		require.Contains(t, out.String(), s)
	}
}

func TestFilesystemOf(t *testing.T) {
	mounts := "rootfs / ext4 rw 0 0\n" +
		"tmpfs /tmp tmpfs rw 0 0\n" +
		`/dev/x /mnt/my\040disk xfs rw 0 0` + "\n" +
		"/dev/y /tmpx ext4 rw 0 0\n" +
		"short line\n"
	tests := []struct{ dir, want string }{
		{"/tmp/a/b", "tmpfs (tmpfs on /tmp)"},
		{"/tmpx/a", "ext4 (/dev/y on /tmpx)"},
		{"/home/u", "ext4 (rootfs on /)"},
		{"/mnt/my disk/data", "xfs (/dev/x on /mnt/my disk)"},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, filesystemOf(mounts, tc.dir), tc.dir)
	}
}

func TestParseField(t *testing.T) {
	data := "Name:\tbench\nVmPeak:\t  2000 kB\nVmHWM:\t  1234 kB\n"
	require.Equal(t, "1234 kB", parseField(data, "VmHWM"))
	require.Equal(t, "unknown", parseField(data, "Missing"))
	require.Equal(t, "Intel X", parseField("processor\t: 0\nmodel name\t: Intel   X\n", "model name"))
}
