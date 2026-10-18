package linearizability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// fetchOnly is a client whose Fetch serves canned batches; other methods are not used.
type fetchOnly struct {
	streamqv1.StreamQServiceClient
	hw   int64
	data []byte
	code streamqv1.ErrorCode
}

func (f fetchOnly) Fetch(context.Context, *streamqv1.FetchRequest, ...grpc.CallOption) (*streamqv1.FetchResponse, error) {
	return &streamqv1.FetchResponse{Topics: []*streamqv1.FetchTopicResponse{{Topic: "t", Partitions: []*streamqv1.FetchPartitionResponse{
		{Partition: 0, ErrorCode: f.code, HighWatermark: f.hw, Records: f.data},
	}}}}, nil
}

func batchAt(t *testing.T, base int64, values ...string) []byte {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, v := range values {
		b.Append(time.Now().UnixMilli(), nil, []byte(v), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	out.SetBaseOffset(base)
	return out
}

func TestReadAll(t *testing.T) {
	ctx := context.Background()
	ok := append(batchAt(t, 0, "a", "b"), batchAt(t, 2, "c")...)
	gapped := append(batchAt(t, 0, "a", "b"), batchAt(t, 3, "d")...)
	corrupt := append([]byte(nil), ok...)
	corrupt[len(corrupt)-1] ^= 0xff
	tests := []struct {
		name    string
		c       fetchOnly
		want    []string
		anomaly bool
		failed  bool
	}{
		{"whole partition", fetchOnly{hw: 3, data: ok}, []string{"a", "b", "c"}, false, false},
		{"only up to the HW", fetchOnly{hw: 2, data: ok}, []string{"a", "b"}, false, false},
		{"offset gap below the HW", fetchOnly{hw: 4, data: gapped}, nil, true, false},
		{"corrupt batch", fetchOnly{hw: 3, data: corrupt}, nil, true, false},
		{"no data below its own HW", fetchOnly{hw: 5, data: nil}, nil, true, false},
		{"overlapping batch hides a record", fetchOnly{hw: 4, data: append(batchAt(t, 0, "a", "b", "c"), batchAt(t, 2, "X", "d")...)}, nil, true, false},
		{"backward batch", fetchOnly{hw: 4, data: append(append(batchAt(t, 0, "a", "b"), batchAt(t, 2, "c")...), batchAt(t, 1, "x")...)}, nil, true, false},
		{"API error has no effect", fetchOnly{code: streamqv1.ErrorCode_ERROR_CODE_NOT_LEADER}, nil, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadAll(ctx, tt.c, "t", 0)
			var a *Anomaly
			switch {
			case tt.anomaly:
				require.ErrorAs(t, err, &a)
			case tt.failed:
				require.Error(t, err)
				require.NotErrorAs(t, err, &a)
			default:
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}

	rec := NewRecorder()
	leader := func(context.Context) (streamqv1.StreamQServiceClient, error) {
		return fetchOnly{hw: 4, data: gapped}, nil
	}
	require.False(t, rec.Read(ctx, 0, leader, "t", 0))
	require.Len(t, rec.Summary().Anomalies, 1, "anomalies are recorded, not discarded")
}

// scripted serves one canned response per Fetch call, in order.
type scripted struct {
	streamqv1.StreamQServiceClient
	resps []fetchOnly
	calls *int
}

func (s scripted) Fetch(ctx context.Context, r *streamqv1.FetchRequest, o ...grpc.CallOption) (*streamqv1.FetchResponse, error) {
	i := *s.calls
	*s.calls++
	return s.resps[min(i, len(s.resps)-1)].Fetch(ctx, r, o...)
}

// TestReadAllAcrossFetches covers multi-fetch reads: records before the requested offset (a
// fetch returns whole batches) are skipped, not reported as a gap; and a broker whose own HW
// fell below the first response's (leadership lost and regained mid-read) ends the read without
// an anomaly (plan R27).
func TestReadAllAcrossFetches(t *testing.T) {
	ctx := context.Background()
	calls := 0
	// The batch containing the fetch offset starts below it: earlier records are skipped.
	containing := scripted{calls: &calls, resps: []fetchOnly{
		{hw: 4, data: batchAt(t, 0, "a")},
		{hw: 4, data: batchAt(t, 0, "a", "b", "c", "d")},
	}}
	got, err := ReadAll(ctx, containing, "t", 0)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d"}, got)

	calls = 0
	var a *Anomaly
	stale := scripted{calls: &calls, resps: []fetchOnly{
		{hw: 4, data: batchAt(t, 0, "a", "b")},
		{hw: 4, data: batchAt(t, 0, "a", "b")}, // entirely below fetch offset 2
	}}
	_, err = ReadAll(ctx, stale, "t", 0)
	require.ErrorAs(t, err, &a)

	calls = 0
	hwDropped := scripted{calls: &calls, resps: []fetchOnly{
		{hw: 4, data: batchAt(t, 0, "a", "b")},
		{hw: 2, data: nil},
	}}
	_, err = ReadAll(ctx, hwDropped, "t", 0)
	require.Error(t, err)
	require.NotErrorAs(t, err, &a, "a lower HW on a later response is not an integrity violation")
}
