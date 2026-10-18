package client

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

// fakeCluster runs fake brokers on in-memory listeners. Addresses are arbitrary strings such as
// "b1:9092"; the client reaches them through the dialer from dialOption.
type fakeCluster struct {
	t         *testing.T
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
	servers   []*grpc.Server
}

func newFakeCluster(t *testing.T) *fakeCluster {
	t.Helper()
	fc := &fakeCluster{t: t, listeners: map[string]*bufconn.Listener{}}
	t.Cleanup(fc.stop)
	return fc
}

// add starts srv at addr.
func (fc *fakeCluster) add(addr string, srv streamqv1.StreamQServiceServer) {
	fc.t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	streamqv1.RegisterStreamQServiceServer(gs, srv)
	fc.mu.Lock()
	fc.listeners[addr] = lis
	fc.servers = append(fc.servers, gs)
	fc.mu.Unlock()
	go func() { _ = gs.Serve(lis) }()
}

func (fc *fakeCluster) stop() {
	fc.mu.Lock()
	servers := fc.servers
	fc.servers = nil
	fc.mu.Unlock()
	for _, gs := range servers {
		gs.Stop()
	}
}

// dialOption routes dials to the in-memory listeners; unknown addresses fail like a down host.
func (fc *fakeCluster) dialOption() grpc.DialOption {
	return grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		fc.mu.Lock()
		lis, ok := fc.listeners[addr]
		fc.mu.Unlock()
		if !ok {
			return nil, &net.OpError{Op: "dial", Net: "bufconn", Err: net.UnknownNetworkError(addr)}
		}
		return lis.DialContext(ctx)
	})
}

// newClient returns a Client for the fake cluster with fast, deterministic backoff.
func (fc *fakeCluster) newClient(bootstrap ...string) *Client {
	fc.t.Helper()
	c, err := New(Config{Bootstrap: bootstrap, DialOptions: []grpc.DialOption{fc.dialOption()}})
	require.NoError(fc.t, err)
	c.backoff.rand = func() float64 { return 0 } // retries without real delays
	fc.t.Cleanup(func() { require.NoError(fc.t, c.Close()) })
	return c
}

// fakeBroker is a scriptable StreamQServiceServer. Unset handlers fall back to an in-memory
// single-broker implementation (topics in `topics`, data in `logs`), so most tests only script
// the calls they care about. All fields are guarded by mu.
type fakeBroker struct {
	streamqv1.UnimplementedStreamQServiceServer

	mu     sync.Mutex
	id     int32
	host   string
	port   int32
	topics map[string]int32                   // name -> partitions (leader = id, epoch = 0)
	logs   map[TopicPartition][]record.Record // appended records with offsets
	calls  map[string]int                     // RPC name -> count

	produceReqs []*streamqv1.ProduceRequest
	fetchReqs   []*streamqv1.FetchRequest

	// Optional overrides; return (nil, nil) to fall through to the default behaviour.
	onMetadata    func(*streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error)
	onProduce     func(*streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error)
	onFetch       func(*streamqv1.FetchRequest) (*streamqv1.FetchResponse, error)
	onListOffsets func(*streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error)
}

func newFakeBroker(id int32, host string, port int32) *fakeBroker {
	return &fakeBroker{id: id, host: host, port: port, topics: map[string]int32{},
		logs: map[TopicPartition][]record.Record{}, calls: map[string]int{}}
}

func (f *fakeBroker) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[name]
}

func (f *fakeBroker) records(tp TopicPartition) []record.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]record.Record(nil), f.logs[tp]...)
}

func (f *fakeBroker) Metadata(_ context.Context, req *streamqv1.MetadataRequest) (*streamqv1.MetadataResponse, error) {
	f.mu.Lock()
	f.calls["Metadata"]++
	hook := f.onMetadata
	f.mu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &streamqv1.MetadataResponse{ControllerId: f.id, Brokers: []*streamqv1.Broker{{Id: f.id, Host: f.host, Port: f.port}}}
	names := req.GetTopics()
	if len(names) == 0 {
		for n := range f.topics {
			names = append(names, n)
		}
	}
	for _, n := range names {
		parts, ok := f.topics[n]
		if !ok {
			resp.Topics = append(resp.Topics, &streamqv1.TopicMetadata{Name: n, ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION})
			continue
		}
		tm := &streamqv1.TopicMetadata{Name: n}
		for p := range parts {
			tm.Partitions = append(tm.Partitions, &streamqv1.PartitionMetadata{Partition: p, LeaderId: f.id, Replicas: []int32{f.id}, Isr: []int32{f.id}})
		}
		resp.Topics = append(resp.Topics, tm)
	}
	return resp, nil
}

func (f *fakeBroker) Produce(_ context.Context, req *streamqv1.ProduceRequest) (*streamqv1.ProduceResponse, error) {
	f.mu.Lock()
	f.calls["Produce"]++
	f.produceReqs = append(f.produceReqs, req)
	hook := f.onProduce
	f.mu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &streamqv1.ProduceResponse{}
	for _, t := range req.GetTopics() {
		tr := &streamqv1.ProduceTopicResponse{Topic: t.GetTopic()}
		for _, p := range t.GetPartitions() {
			tp := TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}
			pr := &streamqv1.ProducePartitionResponse{Partition: p.GetPartition(), BaseOffset: -1}
			b, err := record.Validate(p.GetRecords(), 1<<20)
			switch {
			case f.topics[tp.Topic] <= tp.Partition:
				pr.ErrorCode = streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
			case err != nil:
				pr.ErrorCode = streamqv1.ErrorCode_ERROR_CODE_CORRUPT_MESSAGE
			default:
				base := int64(len(f.logs[tp]))
				recs, _ := b.Records()
				for i, r := range recs {
					r.Offset = base + int64(i)
					r.Key, r.Value = clone(r.Key), clone(r.Value)
					f.logs[tp] = append(f.logs[tp], r)
				}
				pr.BaseOffset = base
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, nil
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

// encode builds one batch holding recs with offsets starting at base.
func encode(recs []record.Record, base int64) []byte {
	if len(recs) == 0 {
		return nil
	}
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, r := range recs {
		b.Append(r.Timestamp, r.Key, r.Value, r.Headers)
	}
	out, err := b.Build()
	if err != nil {
		panic(err)
	}
	out.SetBaseOffset(base)
	return out
}

func (f *fakeBroker) Fetch(_ context.Context, req *streamqv1.FetchRequest) (*streamqv1.FetchResponse, error) {
	f.mu.Lock()
	f.calls["Fetch"]++
	f.fetchReqs = append(f.fetchReqs, req)
	hook := f.onFetch
	f.mu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &streamqv1.FetchResponse{}
	for _, t := range req.GetTopics() {
		tr := &streamqv1.FetchTopicResponse{Topic: t.GetTopic()}
		for _, p := range t.GetPartitions() {
			tp := TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}
			log := f.logs[tp]
			pr := &streamqv1.FetchPartitionResponse{Partition: p.GetPartition(), HighWatermark: int64(len(log))}
			switch off := p.GetFetchOffset(); {
			case f.topics[tp.Topic] <= tp.Partition:
				pr.ErrorCode = streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION
			case off < 0 || off > int64(len(log)):
				pr.ErrorCode = streamqv1.ErrorCode_ERROR_CODE_OFFSET_OUT_OF_RANGE
			default:
				// Whole "batches" of up to 3 records aligned at multiples of 3, like a real log:
				// a fetch may start before the requested offset.
				start := off - off%3
				pr.Records = encode(log[start:min(start+3, int64(len(log)))], start)
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, nil
}

func (f *fakeBroker) ListOffsets(_ context.Context, req *streamqv1.ListOffsetsRequest) (*streamqv1.ListOffsetsResponse, error) {
	f.mu.Lock()
	f.calls["ListOffsets"]++
	hook := f.onListOffsets
	f.mu.Unlock()
	if hook != nil {
		if resp, err := hook(req); resp != nil || err != nil {
			return resp, err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	resp := &streamqv1.ListOffsetsResponse{}
	for _, t := range req.GetTopics() {
		tr := &streamqv1.ListOffsetsTopicResponse{Topic: t.GetTopic()}
		for _, p := range t.GetPartitions() {
			tp := TopicPartition{Topic: t.GetTopic(), Partition: p.GetPartition()}
			pr := &streamqv1.ListOffsetsPartitionResponse{Partition: p.GetPartition(), Offset: -1, Timestamp: -1}
			switch p.GetTimestamp() {
			case -2:
				pr.Offset = 0
			case -1:
				pr.Offset = int64(len(f.logs[tp]))
			default:
				for _, r := range f.logs[tp] {
					if r.Timestamp >= p.GetTimestamp() {
						pr.Offset, pr.Timestamp = r.Offset, r.Timestamp
						break
					}
				}
			}
			tr.Partitions = append(tr.Partitions, pr)
		}
		resp.Topics = append(resp.Topics, tr)
	}
	return resp, nil
}

func (f *fakeBroker) CreateTopic(_ context.Context, req *streamqv1.CreateTopicRequest) (*streamqv1.CreateTopicResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["CreateTopic"]++
	if _, ok := f.topics[req.GetName()]; ok {
		return &streamqv1.CreateTopicResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS}, nil
	}
	f.topics[req.GetName()] = max(req.GetPartitions(), 1)
	return &streamqv1.CreateTopicResponse{}, nil
}

func (f *fakeBroker) DeleteTopic(_ context.Context, req *streamqv1.DeleteTopicRequest) (*streamqv1.DeleteTopicResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["DeleteTopic"]++
	if _, ok := f.topics[req.GetName()]; !ok {
		return &streamqv1.DeleteTopicResponse{ErrorCode: streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION}, nil
	}
	delete(f.topics, req.GetName())
	return &streamqv1.DeleteTopicResponse{}, nil
}

func (f *fakeBroker) ListTopics(context.Context, *streamqv1.ListTopicsRequest) (*streamqv1.ListTopicsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls["ListTopics"]++
	resp := &streamqv1.ListTopicsResponse{}
	for n := range f.topics {
		resp.Names = append(resp.Names, n)
	}
	return resp, nil
}

// singleBroker starts one fake broker "b1:9092" with the given topics and returns it and a
// Client bootstrapped to it.
func singleBroker(t *testing.T, topics map[string]int32) (*fakeBroker, *Client) {
	t.Helper()
	fc := newFakeCluster(t)
	fb := newFakeBroker(1, "b1", 9092)
	for n, p := range topics {
		fb.topics[n] = p
	}
	fc.add("b1:9092", fb)
	return fb, fc.newClient("b1:9092")
}
