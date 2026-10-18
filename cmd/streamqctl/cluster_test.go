package main

import (
	"bytes"
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/testnode"
)

func TestClusterStatus(t *testing.T) {
	healthyPart := func(id, leader int32) *streamqv1.PartitionMetadata {
		return &streamqv1.PartitionMetadata{Partition: id, LeaderId: leader, Replicas: []int32{1, 2, 3}, Isr: []int32{1, 2, 3}}
	}
	brokers := func(ids ...int32) []*streamqv1.Broker {
		var bs []*streamqv1.Broker
		for _, id := range ids {
			bs = append(bs, &streamqv1.Broker{Id: id, Host: "localhost", Port: 9000 + id})
		}
		return bs
	}
	tests := []struct {
		name    string
		resp    *streamqv1.MetadataResponse
		strict  bool
		want    string
		wantErr string
	}{
		{
			name: "healthy",
			resp: &streamqv1.MetadataResponse{ControllerId: 2, Brokers: brokers(3, 1, 2), Topics: []*streamqv1.TopicMetadata{
				{Name: "b", Partitions: []*streamqv1.PartitionMetadata{healthyPart(0, 1)}},
				{Name: "a", Partitions: []*streamqv1.PartitionMetadata{healthyPart(0, 2), healthyPart(1, 3)}},
			}},
			strict: true,
			want: "CONTROLLER  2\n\n" +
				"BROKER  ADDRESS         STATE  CONTROLLER\n" +
				"1       localhost:9001  live   \n" +
				"2       localhost:9002  live   *\n" +
				"3       localhost:9003  live   \n" +
				"\n" +
				"TOPIC  PARTITIONS  UNDER_REPLICATED  OFFLINE  ERROR\n" +
				"a      2           0                 0        -\n" +
				"b      1           0                 0        -\n",
		},
		{
			name: "no topics no controller",
			resp: &streamqv1.MetadataResponse{ControllerId: -1, Brokers: brokers(1)},
			want: "CONTROLLER  none\n\n" +
				"BROKER  ADDRESS         STATE  CONTROLLER\n" +
				"1       localhost:9001  live   \n" +
				"\nno topics\n",
		},
		{
			name: "broker down and partitions under-replicated and offline",
			resp: &streamqv1.MetadataResponse{ControllerId: 1, Brokers: brokers(1, 3), Topics: []*streamqv1.TopicMetadata{
				{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
					healthyPart(0, 1),
					{Partition: 1, LeaderId: 3, Replicas: []int32{1, 2, 3}, Isr: []int32{1, 3}},
					{Partition: 2, LeaderId: -1, Replicas: []int32{2}, Isr: []int32{2}},
				}},
			}},
			want: "CONTROLLER  1\n\n" +
				"BROKER  ADDRESS         STATE  CONTROLLER\n" +
				"1       localhost:9001  live   *\n" +
				"2       -               down   \n" +
				"3       localhost:9003  live   \n" +
				"\n" +
				"TOPIC  PARTITIONS  UNDER_REPLICATED  OFFLINE  ERROR\n" +
				"t      3           1                 1        -\n",
		},
		{
			name: "strict fails on down broker",
			resp: &streamqv1.MetadataResponse{ControllerId: 1, Brokers: brokers(1), Topics: []*streamqv1.TopicMetadata{
				{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
					{Partition: 0, LeaderId: 1, Replicas: []int32{1, 2}, Isr: []int32{1, 2}},
				}},
			}},
			strict:  true,
			wantErr: "1 broker(s) down, 0 partition(s) under-replicated, 0 offline",
		},
		{
			name: "strict fails on offline partition",
			resp: &streamqv1.MetadataResponse{ControllerId: 1, Brokers: brokers(1), Topics: []*streamqv1.TopicMetadata{
				{Name: "t", Partitions: []*streamqv1.PartitionMetadata{
					{Partition: 0, LeaderId: -1, Replicas: []int32{1}, Isr: []int32{1}},
				}},
			}},
			strict:  true,
			wantErr: "0 broker(s) down, 0 partition(s) under-replicated, 1 offline",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotReq *streamqv1.MetadataRequest
			srv := &fakeServer{metadata: func(r *streamqv1.MetadataRequest) *streamqv1.MetadataResponse {
				gotReq = r
				return tc.resp
			}}
			args := []string{"cluster", "status"}
			if tc.strict {
				args = append(args, "--strict")
			}
			res := runCmd(t, srv, "", args...)
			require.Empty(t, gotReq.GetTopics(), "status must request all topics")
			if tc.wantErr != "" {
				require.ErrorIs(t, res.err, errClusterUnhealthy)
				require.ErrorContains(t, res.err, tc.wantErr)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, tc.want, res.out)
		})
	}
}

func TestClusterStatusTransportError(t *testing.T) {
	res := runCmd(t, &fakeServer{}, "", "cluster", "status")
	require.ErrorContains(t, res.err, "metadata")
}

func TestClusterStatusHelpListsLiveBrokersOnly(t *testing.T) {
	var out, errOut bytes.Buffer
	cmd := newRootCmd(strings.NewReader(""), &out, &errOut)
	cmd.SetArgs([]string{"cluster", "status", "--help"})
	require.NoError(t, cmd.Execute())
	require.Contains(t, out.String(), "Only live")
	require.Contains(t, out.String(), "--strict")
}

// controllerRe extracts the controller id from `cluster status` output.
var controllerRe = regexp.MustCompile(`(?m)^CONTROLLER\s+(\d+)$`)

// brokerState returns the STATE column of broker id's row, or "" if absent.
func brokerState(out string, id int32) string {
	re := regexp.MustCompile(`(?m)^` + strconv.Itoa(int(id)) + `\s+\S+\s+(live|down)\b`)
	if m := re.FindStringSubmatch(out); m != nil {
		return m[1]
	}
	return ""
}

// TestClusterStatusAgainstCluster runs `cluster status` against an in-process 3-node cluster,
// healthy and then with one broker stopped. It relies only on liveness fencing (short session
// timeout), so it holds with or without controlled shutdown.
func TestClusterStatusAgainstCluster(t *testing.T) {
	c := testnode.StartCluster(t, 3, func(cfg *config.Config) {
		cfg.BrokerHeartbeatIntervalMs, cfg.BrokerSessionTimeoutMs = 100, 1000
	})
	run := func(bootstrap string, args ...string) (string, error) {
		var out, errOut bytes.Buffer
		cmd := newRootCmd(strings.NewReader(""), &out, &errOut)
		cmd.SetArgs(append([]string{"--bootstrap", bootstrap, "--timeout", "5s"}, args...))
		err := cmd.ExecuteContext(context.Background())
		return out.String(), err
	}
	// Brokers become live (unfenced) shortly after startup; retry until all three are.
	var out string
	var err error
	require.Eventually(t, func() bool {
		out, err = run(c.Addr(1), "topics", "create", "events", "--partitions", "3", "--replication-factor", "3")
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "create topic: %v", err)

	require.Eventually(t, func() bool {
		out, err = run(c.Addr(1), "cluster", "status", "--strict")
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "healthy cluster: %v\n%s", err, out)
	for id := int32(1); id <= 3; id++ {
		require.Equal(t, "live", brokerState(out, id), out)
	}
	m := controllerRe.FindStringSubmatch(out)
	require.NotNil(t, m, out)
	ctrl64, err := strconv.ParseInt(m[1], 10, 32)
	require.NoError(t, err)
	require.Contains(t, out, "events")

	// Stop a broker that is not the controller and query a survivor.
	ctrl := int32(ctrl64)
	stopped := ctrl%3 + 1
	survivor := ctrl
	c.Stop(stopped)
	require.Eventually(t, func() bool {
		out, err = run(c.Addr(survivor), "cluster", "status")
		return err == nil && brokerState(out, stopped) == "down" &&
			regexp.MustCompile(`(?m)^events\s+3\s+3\s+0\s`).MatchString(out)
	}, 15*time.Second, 100*time.Millisecond, "stopped broker %d: %v\n%s", stopped, err, out)

	_, err = run(c.Addr(survivor), "cluster", "status", "--strict")
	require.ErrorIs(t, err, errClusterUnhealthy)
}
