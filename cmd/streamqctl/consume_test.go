package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// fakeLog is a fake broker holding pre-built batches per partition of one topic. Each Fetch
// returns at most one batch per partition: the first batch containing an offset >= fetch_offset,
// starting at its base offset (as a real broker does).
type fakeLog struct {
	t     *testing.T
	topic string
	parts [][]record.Batch // index = partition
	hw    map[int32]int64  // overrides the latest offset reported by ListOffsets
	// onFetch, if set, is called with the number of fetches served so far.
	onFetch  func(n int, req *streamqv1.FetchRequest)
	fetchErr streamqv1.ErrorCode

	mu      sync.Mutex
	fetches []*streamqv1.FetchRequest
}

// newFakeLog builds partition logs. Each inner slice of values is one batch; record i of
// partition p has key "k<p>" and the given value.
func newFakeLog(t *testing.T, topic string, batches ...[][]string) *fakeLog {
	t.Helper()
	l := &fakeLog{t: t, topic: topic}
	for p, part := range batches {
		var next int64
		var log []record.Batch
		for _, vals := range part {
			b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
			for _, v := range vals {
				b.Append(fixedNowMs, fmt.Appendf(nil, "k%d", p), []byte(v), nil)
			}
			batch, err := b.Build()
			require.NoError(t, err)
			batch.SetBaseOffset(next)
			next = batch.NextOffset()
			log = append(log, batch)
		}
		l.parts = append(l.parts, log)
	}
	return l
}

func (l *fakeLog) end(p int32) int64 {
	if hw, ok := l.hw[p]; ok {
		return hw
	}
	log := l.parts[p]
	if len(log) == 0 {
		return 0
	}
	return log[len(log)-1].NextOffset()
}

func (l *fakeLog) server() *fakeServer {
	return &fakeServer{
		metadata: func(*streamqv1.MetadataRequest) *streamqv1.MetadataResponse {
			tm := &streamqv1.TopicMetadata{Name: l.topic}
			for i := range l.parts {
				tm.Partitions = append(tm.Partitions, &streamqv1.PartitionMetadata{Partition: int32(i), LeaderId: 1})
			}
			return &streamqv1.MetadataResponse{Topics: []*streamqv1.TopicMetadata{tm}}
		},
		listOffsets: func(r *streamqv1.ListOffsetsRequest) *streamqv1.ListOffsetsResponse {
			resp := &streamqv1.ListOffsetsResponse{}
			for _, lt := range r.GetTopics() {
				tr := &streamqv1.ListOffsetsTopicResponse{Topic: lt.GetTopic()}
				for _, lp := range lt.GetPartitions() {
					if lp.GetLeaderEpoch() != -1 {
						l.t.Errorf("list offsets leader epoch %d, want -1", lp.GetLeaderEpoch())
					}
					off := int64(0) // earliest
					switch lp.GetTimestamp() {
					case -1:
						off = l.end(lp.GetPartition())
					case -2:
					default:
						l.t.Errorf("unexpected list offsets timestamp %d", lp.GetTimestamp())
					}
					tr.Partitions = append(tr.Partitions, &streamqv1.ListOffsetsPartitionResponse{
						Partition: lp.GetPartition(), Offset: off,
					})
				}
				resp.Topics = append(resp.Topics, tr)
			}
			return resp
		},
		fetch: l.handleFetch,
	}
}

func (l *fakeLog) handleFetch(r *streamqv1.FetchRequest) *streamqv1.FetchResponse {
	l.mu.Lock()
	l.fetches = append(l.fetches, r)
	n := len(l.fetches)
	l.mu.Unlock()
	if r.GetMaxWaitMs() != 500 {
		l.t.Errorf("max_wait_ms %d, want 500", r.GetMaxWaitMs())
	}
	resp := &streamqv1.FetchResponse{}
	for _, ft := range r.GetTopics() {
		tr := &streamqv1.FetchTopicResponse{Topic: ft.GetTopic()}
		for _, fp := range ft.GetPartitions() {
			if fp.GetLeaderEpoch() != -1 {
				l.t.Errorf("fetch leader epoch %d, want -1", fp.GetLeaderEpoch())
			}
			pr := &streamqv1.FetchPartitionResponse{Partition: fp.GetPartition(), ErrorCode: l.fetchErr,
				HighWatermark: l.end(fp.GetPartition())}
			for _, b := range l.parts[fp.GetPartition()] {
				if b.NextOffset() > fp.GetFetchOffset() {
					pr.Records = b
					break
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	if l.onFetch != nil {
		l.onFetch(n, r)
	}
	return resp
}

// fetchOffsets returns, per fetch, "p<partition>@<offset>" for every requested partition.
func (l *fakeLog) fetchOffsets() [][]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out [][]string
	for _, r := range l.fetches {
		var row []string
		for _, ft := range r.GetTopics() {
			for _, fp := range ft.GetPartitions() {
				row = append(row, fmt.Sprintf("p%d@%d", fp.GetPartition(), fp.GetFetchOffset()))
			}
		}
		out = append(out, row)
	}
	return out
}

// twoPartitionLog: partition 0 = batches [a0 a1 a2] [a3]; partition 1 = batch [b0 b1].
func twoPartitionLog(t *testing.T) *fakeLog {
	t.Helper()
	return newFakeLog(t, "orders",
		[][]string{{"a0", "a1", "a2"}, {"a3"}},
		[][]string{{"b0", "b1"}},
	)
}

func TestConsume(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		hw          map[int32]int64
		wantOut     string
		wantFetches [][]string
	}{
		{
			name:    "from beginning across two partitions",
			args:    []string{"--from-beginning"},
			wantOut: "a0\na1\na2\nb0\nb1\na3\n",
			wantFetches: [][]string{
				{"p0@0", "p1@0"},
				{"p0@3"},
			},
		},
		{
			name:    "print offset and key",
			args:    []string{"--from-beginning", "--print-offset", "--print-key"},
			wantOut: "0\tk0\ta0\n1\tk0\ta1\n2\tk0\ta2\n0\tk1\tb0\n1\tk1\tb1\n3\tk0\ta3\n",
			wantFetches: [][]string{
				{"p0@0", "p1@0"},
				{"p0@3"},
			},
		},
		{
			name:        "latest prints nothing and does not fetch",
			args:        nil,
			wantOut:     "",
			wantFetches: nil,
		},
		{
			name:        "start mid-batch drops earlier records",
			args:        []string{"--partition", "0", "--offset", "1"},
			wantOut:     "a1\na2\na3\n",
			wantFetches: [][]string{{"p0@1"}, {"p0@3"}},
		},
		{
			name:        "offset applies to every partition",
			args:        []string{"--offset", "1", "--print-offset"},
			wantOut:     "1\ta1\n2\ta2\n1\tb1\n3\ta3\n",
			wantFetches: [][]string{{"p0@1", "p1@1"}, {"p0@3"}},
		},
		{
			name:        "max messages",
			args:        []string{"--from-beginning", "--max-messages", "4"},
			wantOut:     "a0\na1\na2\nb0\n",
			wantFetches: [][]string{{"p0@0", "p1@0"}},
		},
		{
			name:        "max messages within one batch",
			args:        []string{"--from-beginning", "--max-messages", "2", "--partition", "0"},
			wantOut:     "a0\na1\n",
			wantFetches: [][]string{{"p0@0"}},
		},
		{
			name: "stops at the HW observed at start",
			args: []string{"--from-beginning"},
			// The log holds more than the reported HW, as if records arrived after start.
			hw:          map[int32]int64{0: 2, 1: 1},
			wantOut:     "a0\na1\nb0\n",
			wantFetches: [][]string{{"p0@0", "p1@0"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := twoPartitionLog(t)
			l.hw = tt.hw
			args := append([]string{"consume", "--topic", "orders"}, tt.args...)
			res := runCmd(t, l.server(), "", args...)
			require.NoError(t, res.err)
			require.Equal(t, tt.wantOut, res.out)
			require.Equal(t, tt.wantFetches, l.fetchOffsets())
		})
	}
}

func TestConsumeFromBeginningVsLatest(t *testing.T) {
	l := twoPartitionLog(t)
	res := runCmd(t, l.server(), "", "consume", "--topic", "orders", "--from-beginning", "--partition", "1")
	require.NoError(t, res.err)
	require.Equal(t, "b0\nb1\n", res.out)

	l = twoPartitionLog(t)
	res = runCmd(t, l.server(), "", "consume", "--topic", "orders", "--partition", "1")
	require.NoError(t, res.err)
	require.Empty(t, res.out)
	require.Empty(t, l.fetchOffsets())
}

func TestConsumeFollowUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := twoPartitionLog(t)
	// After the third fetch every record has been served; cancel the command's context the way
	// SIGINT does.
	l.onFetch = func(n int, _ *streamqv1.FetchRequest) {
		if n == 3 {
			cancel()
		}
	}
	res := runCmdCtx(ctx, t, l.server(), "", "consume", "--topic", "orders", "--from-beginning", "--follow")
	require.NoError(t, res.err)
	require.Equal(t, "a0\na1\na2\nb0\nb1\na3\n", res.out)
	fetches := l.fetchOffsets()
	require.GreaterOrEqual(t, len(fetches), 2)
	// --follow keeps fetching every partition, including caught-up ones.
	require.Equal(t, []string{"p0@0", "p1@0"}, fetches[0])
	require.Equal(t, []string{"p0@3", "p1@2"}, fetches[1])
}

func TestConsumeErrors(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		fetchErr streamqv1.ErrorCode
		wantErr  string
	}{
		{
			name:     "fetch error code",
			args:     []string{"--from-beginning"},
			fetchErr: streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE,
			wantErr:  `fetch from topic "orders" partition 0: OFFSET_OUT_OF_RANGE`,
		},
		{name: "unknown partition", args: []string{"--partition", "7"}, wantErr: `topic "orders" has no partition 7`},
		{name: "offset and from-beginning", args: []string{"--offset", "1", "--from-beginning"},
			wantErr: "none of the others can be"},
		{name: "negative offset", args: []string{"--offset", "-3"}, wantErr: "invalid --offset -3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := twoPartitionLog(t)
			l.fetchErr = tt.fetchErr
			args := append([]string{"consume", "--topic", "orders"}, tt.args...)
			res := runCmd(t, l.server(), "", args...)
			require.ErrorContains(t, res.err, tt.wantErr)
			require.Empty(t, res.out)
		})
	}
}
