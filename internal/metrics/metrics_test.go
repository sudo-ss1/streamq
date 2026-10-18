package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type fakeBroker struct {
	parts      []PartitionStat
	lag        []LagStat
	offline    int
	reportsOff bool
}

func (f *fakeBroker) PartitionStats() []PartitionStat { return f.parts }
func (f *fakeBroker) ConsumerLag() []LagStat          { return f.lag }
func (f *fakeBroker) OfflinePartitions() (int, bool)  { return f.offline, f.reportsOff }

type fakeRaft struct{ st RaftStats }

func (f *fakeRaft) RaftStats() RaftStats { return f.st }

func TestUnaryServerInterceptor(t *testing.T) {
	m := New()
	ic := m.UnaryServerInterceptor("client")
	ok := func(context.Context, any) (any, error) { return "resp", nil }
	fail := func(context.Context, any) (any, error) { return nil, status.Error(codes.Unavailable, "down") }
	produce := &grpc.UnaryServerInfo{FullMethod: "/streamq.v1.StreamQ/Produce"}
	fetch := &grpc.UnaryServerInfo{FullMethod: "/streamq.v1.StreamQ/Fetch"}

	calls := []struct {
		info    *grpc.UnaryServerInfo
		handler grpc.UnaryHandler
		wantErr bool
	}{
		{produce, ok, false},
		{produce, ok, false},
		{produce, fail, true},
		{fetch, ok, false},
	}
	for _, c := range calls {
		resp, err := ic(context.Background(), nil, c.info, c.handler)
		if c.wantErr {
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, resp)
		} else {
			require.NoError(t, err)
			require.Equal(t, "resp", resp)
		}
	}

	tests := []struct {
		method, code string
		want         float64
	}{
		{"Produce", "OK", 2},
		{"Produce", "Unavailable", 1},
		{"Fetch", "OK", 1},
	}
	for _, tc := range tests {
		require.InDelta(t, tc.want, testutil.ToFloat64(m.requests.WithLabelValues("client", tc.method, tc.code)), 0,
			"%s %s", tc.method, tc.code)
	}
	require.Equal(t, 3, testutil.CollectAndCount(m.requests, "streamq_requests_total"))
	// One histogram series per (service, method).
	require.Equal(t, 2, testutil.CollectAndCount(m.requestDuration, "streamq_request_duration_seconds"))

	n, err := testutil.GatherAndCount(m.Registry(), "streamq_request_duration_seconds")
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

func TestByteAndErrorCounters(t *testing.T) {
	m := New()
	m.AddBytesIn("t1", 100)
	m.AddBytesIn("t1", 50)
	m.AddBytesIn("t2", 7)
	m.AddBytesIn("t2", 0) // ignored
	m.AddBytesOut("t1", 30)
	m.PartitionError("Produce", "NOT_LEADER_OR_FOLLOWER")
	m.PartitionError("Produce", "NOT_LEADER_OR_FOLLOWER")
	m.PartitionError("Fetch", "OFFSET_OUT_OF_RANGE")

	expected := `
# HELP streamq_bytes_in_total Record bytes appended by Produce requests, by topic.
# TYPE streamq_bytes_in_total counter
streamq_bytes_in_total{topic="t1"} 150
streamq_bytes_in_total{topic="t2"} 7
# HELP streamq_bytes_out_total Record bytes returned by Fetch responses, by topic.
# TYPE streamq_bytes_out_total counter
streamq_bytes_out_total{topic="t1"} 30
# HELP streamq_partition_errors_total Per-partition API error results, by method and error code.
# TYPE streamq_partition_errors_total counter
streamq_partition_errors_total{error_code="NOT_LEADER_OR_FOLLOWER",method="Produce"} 2
streamq_partition_errors_total{error_code="OFFSET_OUT_OF_RANGE",method="Fetch"} 1
`
	require.NoError(t, testutil.GatherAndCompare(m.Registry(), strings.NewReader(expected),
		"streamq_bytes_in_total", "streamq_bytes_out_total", "streamq_partition_errors_total"))
}

func TestNilReceiverSafety(t *testing.T) {
	var m *Metrics
	require.NotPanics(t, func() {
		m.AddBytesIn("t", 1)
		m.AddBytesOut("t", 1)
		m.PartitionError("Produce", "UNKNOWN")
	})
}

func TestBrokerCollector(t *testing.T) {
	tests := []struct {
		name     string
		src      *fakeBroker
		expected string
	}{
		{
			name: "leader and follower replicas, offline reporter",
			src: &fakeBroker{
				parts: []PartitionStat{
					{Topic: "a", Partition: 0, Leader: true, LEO: 10, HW: 8, ISRSize: 2, Replicas: 3},
					{Topic: "a", Partition: 1, Leader: true, LEO: 5, HW: 5, ISRSize: 3, Replicas: 3},
					{Topic: "b", Partition: 0, Leader: false, LEO: 4, HW: 3, ISRSize: 0, Replicas: 3},
				},
				lag:        []LagStat{{Group: "g", Topic: "a", Partition: 0, Lag: 6}},
				offline:    2,
				reportsOff: true,
			},
			expected: `
# HELP streamq_consumer_lag Consumer group lag (high watermark minus committed offset) on partitions this broker leads.
# TYPE streamq_consumer_lag gauge
streamq_consumer_lag{group="g",partition="0",topic="a"} 6
# HELP streamq_partition_high_watermark High watermark of each local replica.
# TYPE streamq_partition_high_watermark gauge
streamq_partition_high_watermark{partition="0",topic="a"} 8
streamq_partition_high_watermark{partition="1",topic="a"} 5
streamq_partition_high_watermark{partition="0",topic="b"} 3
# HELP streamq_partition_isr_size Committed ISR size of each partition this broker leads.
# TYPE streamq_partition_isr_size gauge
streamq_partition_isr_size{partition="0",topic="a"} 2
streamq_partition_isr_size{partition="1",topic="a"} 3
# HELP streamq_partition_log_end_offset Log end offset of each local replica.
# TYPE streamq_partition_log_end_offset gauge
streamq_partition_log_end_offset{partition="0",topic="a"} 10
streamq_partition_log_end_offset{partition="1",topic="a"} 5
streamq_partition_log_end_offset{partition="0",topic="b"} 4
# HELP streamq_partition_replicas Assigned replica count of each partition this broker leads.
# TYPE streamq_partition_replicas gauge
streamq_partition_replicas{partition="0",topic="a"} 3
streamq_partition_replicas{partition="1",topic="a"} 3
# HELP streamq_partitions_leader Number of partitions this broker leads.
# TYPE streamq_partitions_leader gauge
streamq_partitions_leader 2
# HELP streamq_partitions_offline Partitions without a leader (exported by the controller only).
# TYPE streamq_partitions_offline gauge
streamq_partitions_offline 2
# HELP streamq_partitions_under_replicated Partitions this broker leads whose ISR is smaller than the replica set.
# TYPE streamq_partitions_under_replicated gauge
streamq_partitions_under_replicated 1
`,
		},
		{
			name: "follower only, not the offline reporter",
			src: &fakeBroker{
				parts:   []PartitionStat{{Topic: "b", Partition: 2, LEO: 1, HW: 1, Replicas: 3}},
				offline: 5,
			},
			expected: `
# HELP streamq_partition_high_watermark High watermark of each local replica.
# TYPE streamq_partition_high_watermark gauge
streamq_partition_high_watermark{partition="2",topic="b"} 1
# HELP streamq_partition_log_end_offset Log end offset of each local replica.
# TYPE streamq_partition_log_end_offset gauge
streamq_partition_log_end_offset{partition="2",topic="b"} 1
# HELP streamq_partitions_leader Number of partitions this broker leads.
# TYPE streamq_partitions_leader gauge
streamq_partitions_leader 0
# HELP streamq_partitions_under_replicated Partitions this broker leads whose ISR is smaller than the replica set.
# TYPE streamq_partitions_under_replicated gauge
streamq_partitions_under_replicated 0
`,
		},
	}
	names := []string{
		"streamq_partition_log_end_offset", "streamq_partition_high_watermark",
		"streamq_partition_isr_size", "streamq_partition_replicas", "streamq_partitions_leader",
		"streamq_partitions_under_replicated", "streamq_partitions_offline", "streamq_consumer_lag",
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := New()
			m.RegisterBroker(tc.src)
			require.NoError(t, testutil.GatherAndCompare(m.Registry(), strings.NewReader(tc.expected), names...))
		})
	}
}

func TestRaftCollector(t *testing.T) {
	tests := []struct {
		name            string
		st              RaftStats
		wantState       string
		wantLastContact float64
	}{
		{"leader", RaftStats{State: "leader", LastContact: 3 * time.Second, Term: 4, CommitIndex: 10, AppliedIndex: 9}, "leader", 0},
		{"follower", RaftStats{State: "follower", LastContact: 1500 * time.Millisecond, Term: 4, CommitIndex: 10, AppliedIndex: 9}, "follower", 1.5},
		{"candidate", RaftStats{State: "candidate", Term: 5}, "candidate", 0},
		{"shutdown", RaftStats{State: "shutdown", LastContact: time.Second}, "shutdown", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := New()
			m.RegisterRaft(&fakeRaft{st: tc.st})
			fams, err := m.Registry().Gather()
			require.NoError(t, err)
			got := map[string]float64{}
			var states int
			for _, f := range fams {
				switch f.GetName() {
				case "streamq_raft_state":
					for _, mm := range f.GetMetric() {
						states++
						label := mm.GetLabel()[0].GetValue()
						want := 0.0
						if label == tc.wantState {
							want = 1
						}
						require.InDelta(t, want, mm.GetGauge().GetValue(), 0, label)
					}
				case "streamq_raft_last_contact_seconds", "streamq_raft_term",
					"streamq_raft_commit_index", "streamq_raft_applied_index":
					got[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
				}
			}
			require.Equal(t, 4, states)
			require.InDelta(t, tc.wantLastContact, got["streamq_raft_last_contact_seconds"], 1e-9)
			require.InDelta(t, float64(tc.st.Term), got["streamq_raft_term"], 0)
			require.InDelta(t, float64(tc.st.CommitIndex), got["streamq_raft_commit_index"], 0)
			require.InDelta(t, float64(tc.st.AppliedIndex), got["streamq_raft_applied_index"], 0)
		})
	}
}

func TestNoRaftSeriesWithoutRegisterRaft(t *testing.T) {
	m := New()
	m.RegisterBroker(&fakeBroker{reportsOff: true})
	fams, err := m.Registry().Gather()
	require.NoError(t, err)
	for _, f := range fams {
		require.False(t, strings.HasPrefix(f.GetName(), "streamq_raft_"), f.GetName())
	}
}

func TestHandlerServesParsableText(t *testing.T) {
	m := New()
	m.RegisterBroker(&fakeBroker{
		parts:      []PartitionStat{{Topic: "a", Partition: 0, Leader: true, LEO: 3, HW: 3, ISRSize: 1, Replicas: 1}},
		reportsOff: true,
	})
	m.RegisterRaft(&fakeRaft{st: RaftStats{State: "leader", Term: 1}})
	m.AddBytesIn("a", 10)
	_, err := m.UnaryServerInterceptor("internal")(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/streamq.v1.Internal/ReplicaFetch"},
		func(context.Context, any) (any, error) { return nil, nil })
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil)
	req.Header.Set("Accept", "text/plain")
	m.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(rec.Body.String()))
	require.NoError(t, err)
	for _, name := range []string{
		"streamq_requests_total", "streamq_request_duration_seconds", "streamq_bytes_in_total",
		"streamq_partition_log_end_offset", "streamq_partitions_offline", "streamq_raft_state",
		"go_goroutines",
	} {
		require.Contains(t, fams, name)
	}
	reqs := fams["streamq_requests_total"].GetMetric()
	require.Len(t, reqs, 1)
	labels := map[string]string{}
	for _, l := range reqs[0].GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	require.Equal(t, map[string]string{"service": "internal", "method": "ReplicaFetch", "code": "OK"}, labels)
}
