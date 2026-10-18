//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
	"github.com/sudo-ss1/streamq/internal/testnode"
)

// scrape fetches and parses a node's /metrics.
func scrape(t *testing.T, addr string) map[string]*dto.MetricFamily {
	t.Helper()
	req, err := http.NewRequestWithContext(testCtx(t), http.MethodGet, "http://"+addr+"/metrics", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(string(body)))
	require.NoError(t, err)
	return fams
}

func labelsMatch(m *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, l := range m.GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// value sums the samples of family name whose labels include want (counter, gauge or histogram
// count); ok is false if the family is absent.
func value(fams map[string]*dto.MetricFamily, name string, want map[string]string) (float64, bool) {
	f, ok := fams[name]
	if !ok {
		return 0, false
	}
	var sum float64
	for _, m := range f.GetMetric() {
		if !labelsMatch(m, want) {
			continue
		}
		switch {
		case m.GetCounter() != nil:
			sum += m.GetCounter().GetValue()
		case m.GetGauge() != nil:
			sum += m.GetGauge().GetValue()
		case m.GetHistogram() != nil:
			sum += float64(m.GetHistogram().GetSampleCount())
		}
	}
	return sum, true
}

func metricsCluster(t *testing.T) *testnode.Cluster {
	t.Helper()
	c := testnode.StartClusterWith(t, 3, func(cfg *config.Config) {
		cfg.TopicDefaults.MinInsyncReplicas = 2
		cfg.ReplicaLagMaxMs = 1500
	}, func(_ int32, o *node.Options) {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		o.Metrics = lis
	})
	waitAllUnfenced(t, c, []int32{1, 2, 3}, 3)
	return c
}

// TestMetricsEndToEnd (M8 plan R5): the metrics endpoint is wired on every node and reports
// request, byte, partition, replication and Raft state consistent with the cluster.
func TestMetricsEndToEnd(t *testing.T) {
	const topic = "metrics"
	c := metricsCluster(t)
	createSpread(t, c, topic, 3)
	leaderOf := map[int32]int32{}
	for _, p := range partitionsOf(metadataOf(t, c.Addr(1), topic), topic) {
		leaderOf[p.GetPartition()] = p.GetLeaderId()
		for range 5 {
			require.Equal(t, streamqv1.ErrorCode_ERROR_CODE_NONE, produceTo(t, c.Addr(p.GetLeaderId()), topic, p.GetPartition()))
		}
	}
	resp, err := dial(t, c.Addr(leaderOf[0])).Fetch(testCtx(t), &streamqv1.FetchRequest{MaxBytes: 1 << 20,
		Topics: []*streamqv1.FetchTopic{{Topic: topic, Partitions: []*streamqv1.FetchPartition{{Partition: 0, LeaderEpoch: -1, FetchOffset: 0, MaxBytes: 1 << 20}}}}})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetTopics()[0].GetPartitions()[0].GetRecords())

	leaders := 0.0
	for id := int32(1); id <= 3; id++ {
		fams := scrape(t, c.Node(id).MetricsAddr())
		v, ok := value(fams, "streamq_raft_state", map[string]string{"state": "leader"})
		require.True(t, ok, "raft metrics on broker %d", id)
		leaders += v
	}
	require.InDelta(t, 1.0, leaders, 0, "exactly one raft leader")

	l0 := scrape(t, c.Node(leaderOf[0]).MetricsAddr())
	v, _ := value(l0, "streamq_requests_total", map[string]string{"service": "client", "method": "Produce"})
	require.Positive(t, v)
	v, _ = value(l0, "streamq_request_duration_seconds", map[string]string{"service": "client", "method": "Produce"})
	require.Positive(t, v)
	v, _ = value(l0, "streamq_bytes_in_total", map[string]string{"topic": topic})
	require.Positive(t, v)
	v, _ = value(l0, "streamq_bytes_out_total", map[string]string{"topic": topic})
	require.Positive(t, v)
	v, _ = value(l0, "streamq_requests_total", map[string]string{"service": "internal", "method": "ReplicaFetch"})
	require.Positive(t, v, "internal API instrumented")

	// LEO/HW gauges match the leader's own offsets; ISR size is reported by the leader.
	require.Eventually(t, func() bool {
		fams := scrape(t, c.Node(leaderOf[0]).MetricsAddr())
		labels := map[string]string{"topic": topic, "partition": "0"}
		leo, _ := value(fams, "streamq_partition_log_end_offset", labels)
		hw, _ := value(fams, "streamq_partition_high_watermark", labels)
		isr, _ := value(fams, "streamq_partition_isr_size", labels)
		return leo == 5 && hw == 5 && isr == 3
	}, 10*time.Second, 50*time.Millisecond)

	// Stop a broker: some leader reports under-replicated partitions, and offline partitions are
	// reported (as 0) by the controller only.
	controller := metadataOf(t, c.Addr(1), topic).GetControllerId()
	victim := int32(1)
	if victim == controller {
		victim = 2
	}
	c.Stop(victim)
	survivors := []int32{}
	for id := int32(1); id <= 3; id++ {
		if id != victim {
			survivors = append(survivors, id)
		}
	}
	require.Eventually(t, func() bool {
		under := 0.0
		for _, id := range survivors {
			v, _ := value(scrape(t, c.Node(id).MetricsAddr()), "streamq_partitions_under_replicated", nil)
			under += v
		}
		return under > 0
	}, 20*time.Second, 100*time.Millisecond, "under-replicated partitions reported")
	reporters, offline := 0, 0.0
	for _, id := range survivors {
		if v, ok := value(scrape(t, c.Node(id).MetricsAddr()), "streamq_partitions_offline", nil); ok {
			reporters++
			offline += v
		}
	}
	require.Equal(t, 1, reporters, "only the controller reports offline partitions")
	require.Zero(t, offline)

	c.Restart(victim)
	require.Eventually(t, func() bool {
		for id := int32(1); id <= 3; id++ {
			if v, _ := value(scrape(t, c.Node(id).MetricsAddr()), "streamq_partitions_under_replicated", nil); v != 0 {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "fully replicated again")
}

// TestMetricsSingleNodeHasNoRaft: single-node mode exports no Raft series but does report offline
// partitions itself.
func TestMetricsSingleNodeHasNoRaft(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.MetricsAddr = ""
	cfg.DataDir = t.TempDir()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.TopicDefaults.ReplicationFactor, cfg.TopicDefaults.MinInsyncReplicas = 1, 1
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	n, err := node.StartWithOptions(ctx, cfg, slog.New(slog.DiscardHandler), node.Options{Metrics: lis})
	require.NoError(t, err)
	defer func() { require.NoError(t, n.Close(ctx)) }()
	fams := scrape(t, n.MetricsAddr())
	for name := range fams {
		require.False(t, strings.HasPrefix(name, "streamq_raft_"), name)
	}
	_, ok := value(fams, "streamq_partitions_offline", nil)
	require.True(t, ok)
}
