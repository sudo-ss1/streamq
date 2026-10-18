package main

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

func TestTopicsCreate(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		code    streamqv1.ErrorCode
		want    *streamqv1.CreateTopicRequest // nil: no RPC expected
		wantOut string
		wantErr string
	}{
		{
			name:    "defaults",
			args:    []string{"topics", "create", "orders"},
			want:    &streamqv1.CreateTopicRequest{Name: "orders"},
			wantOut: "created topic orders\n",
		},
		{
			name: "flags and configs",
			args: []string{
				"topics", "create", "orders", "--partitions", "6", "--replication-factor", "1",
				"--config", "retention.ms=1000", "--config", "segment.bytes=1048576", "--config", "x=a=b",
			},
			want: &streamqv1.CreateTopicRequest{
				Name: "orders", Partitions: 6, ReplicationFactor: 1,
				Configs: map[string]string{"retention.ms": "1000", "segment.bytes": "1048576", "x": "a=b"},
			},
			wantOut: "created topic orders\n",
		},
		{
			name:    "error code reported",
			args:    []string{"topics", "create", "orders", "--partitions", "3"},
			code:    streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS,
			want:    &streamqv1.CreateTopicRequest{Name: "orders", Partitions: 3},
			wantErr: `create topic "orders": TOPIC_ALREADY_EXISTS: boom`,
		},
		{
			name:    "config without equals",
			args:    []string{"topics", "create", "orders", "--config", "retention.ms"},
			wantErr: `invalid --config "retention.ms"`,
		},
		{
			name:    "config empty key",
			args:    []string{"topics", "create", "orders", "--config", "=1"},
			wantErr: `invalid --config "=1"`,
		},
		{
			name:    "duplicate config key",
			args:    []string{"topics", "create", "orders", "--config", "a=1", "--config", "a=2"},
			wantErr: `duplicate key "a"`,
		},
		{
			name:    "missing name",
			args:    []string{"topics", "create"},
			wantErr: "accepts 1 arg(s)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu  sync.Mutex
				got []*streamqv1.CreateTopicRequest
			)
			srv := &fakeServer{createTopic: func(r *streamqv1.CreateTopicRequest) *streamqv1.CreateTopicResponse {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, r)
				resp := &streamqv1.CreateTopicResponse{ErrorCode: tt.code}
				if tt.code != streamqv1.ErrorCode_ERROR_CODE_NONE {
					resp.ErrorMessage = "boom"
				}
				return resp
			}}
			res := runCmd(t, srv, "", tt.args...)
			mu.Lock()
			defer mu.Unlock()
			if tt.want == nil {
				require.Empty(t, got)
			} else {
				require.Len(t, got, 1)
				require.True(t, proto.Equal(tt.want, got[0]), "got %v, want %v", got[0], tt.want)
			}
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				require.Contains(t, res.errOut, tt.wantErr)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, tt.wantOut, res.out)
		})
	}
}

func TestTopicsDelete(t *testing.T) {
	tests := []struct {
		name    string
		code    streamqv1.ErrorCode
		wantErr string
	}{
		{name: "ok"},
		{name: "unknown topic", code: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
			wantErr: `delete topic "orders": UNKNOWN_TOPIC_OR_PARTITION`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu   sync.Mutex
				name string
			)
			srv := &fakeServer{deleteTopic: func(r *streamqv1.DeleteTopicRequest) *streamqv1.DeleteTopicResponse {
				mu.Lock()
				defer mu.Unlock()
				name = r.GetName()
				return &streamqv1.DeleteTopicResponse{ErrorCode: tt.code}
			}}
			res := runCmd(t, srv, "", "topics", "delete", "orders")
			mu.Lock()
			require.Equal(t, "orders", name)
			mu.Unlock()
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				return
			}
			require.NoError(t, res.err)
			require.Equal(t, "deleted topic orders\n", res.out)
		})
	}
}

func TestTopicsList(t *testing.T) {
	srv := &fakeServer{listTopics: func(*streamqv1.ListTopicsRequest) *streamqv1.ListTopicsResponse {
		return &streamqv1.ListTopicsResponse{Names: []string{"zeta", "alpha", "mid"}}
	}}
	res := runCmd(t, srv, "", "topics", "list")
	require.NoError(t, res.err)
	require.Equal(t, "alpha\nmid\nzeta\n", res.out)
}

func TestTopicsListTransportError(t *testing.T) {
	res := runCmd(t, &fakeServer{}, "", "topics", "list")
	require.ErrorContains(t, res.err, "list topics")
	require.ErrorContains(t, res.err, "Unimplemented")
}

func describeServer(gotTopics *[]string, mu *sync.Mutex) *fakeServer {
	return &fakeServer{metadata: func(r *streamqv1.MetadataRequest) *streamqv1.MetadataResponse {
		mu.Lock()
		*gotTopics = append([]string(nil), r.GetTopics()...)
		mu.Unlock()
		all := []*streamqv1.TopicMetadata{
			{Name: "orders", Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 1, LeaderId: 2, LeaderEpoch: 4, Replicas: []int32{2, 3, 1}, Isr: []int32{2, 3}},
				{Partition: 0, LeaderId: 1, LeaderEpoch: 3, Replicas: []int32{1, 2, 3}, Isr: []int32{1, 2, 3}},
			}},
			{Name: "audit", Partitions: []*streamqv1.PartitionMetadata{
				{Partition: 0, LeaderId: 1, LeaderEpoch: 0, Replicas: []int32{1}, Isr: []int32{1},
					ErrorCode: streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER},
			}},
		}
		if len(r.GetTopics()) == 0 {
			return &streamqv1.MetadataResponse{Topics: all}
		}
		resp := &streamqv1.MetadataResponse{}
		for _, name := range r.GetTopics() {
			found := false
			for _, tm := range all {
				if tm.GetName() == name {
					resp.Topics = append(resp.Topics, tm)
					found = true
				}
			}
			if !found {
				resp.Topics = append(resp.Topics, &streamqv1.TopicMetadata{
					Name: name, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION,
				})
			}
		}
		return resp
	}}
}

// tableRows splits tabwriter output into whitespace-separated fields per line.
func tableRows(out string) [][]string {
	var rows [][]string
	for _, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		rows = append(rows, strings.Fields(line))
	}
	return rows
}

func TestTopicsDescribe(t *testing.T) {
	header := []string{"TOPIC", "PARTITION", "LEADER", "EPOCH", "REPLICAS", "ISR", "ERROR"}
	tests := []struct {
		name       string
		args       []string
		wantTopics []string
		wantRows   [][]string
		wantErr    string
	}{
		{
			name: "all topics",
			args: []string{"topics", "describe"},
			wantRows: [][]string{
				header,
				{"audit", "0", "1", "0", "1", "1", "NOT_LEADER"},
				{"orders", "0", "1", "3", "1,2,3", "1,2,3", "-"},
				{"orders", "1", "2", "4", "2,3,1", "2,3", "-"},
			},
		},
		{
			name:       "named topic",
			args:       []string{"topics", "describe", "orders"},
			wantTopics: []string{"orders"},
			wantRows: [][]string{
				header,
				{"orders", "0", "1", "3", "1,2,3", "1,2,3", "-"},
				{"orders", "1", "2", "4", "2,3,1", "2,3", "-"},
			},
		},
		{
			name:       "unknown topic",
			args:       []string{"topics", "describe", "nope"},
			wantTopics: []string{"nope"},
			wantRows: [][]string{
				header,
				{"nope", "-", "-", "-", "-", "-", "UNKNOWN_TOPIC_OR_PARTITION"},
			},
			wantErr: "nope: UNKNOWN_TOPIC_OR_PARTITION",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu     sync.Mutex
				topics []string
			)
			res := runCmd(t, describeServer(&topics, &mu), "", tt.args...)
			mu.Lock()
			require.Equal(t, tt.wantTopics, topics)
			mu.Unlock()
			require.Equal(t, tt.wantRows, tableRows(res.out))
			if tt.wantErr != "" {
				require.ErrorContains(t, res.err, tt.wantErr)
				return
			}
			require.NoError(t, res.err)
		})
	}
}
