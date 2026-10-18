package broker

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// fakeLeader serves follower RPCs from an in-process leader partition. The first failN calls
// fail with partition.ErrNotLeader.
type fakeLeader struct {
	p *partition.Partition

	mu    sync.Mutex
	failN int
	calls int
}

func (f *fakeLeader) fail() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failN > 0 {
		f.failN--
		return partition.ErrNotLeader
	}
	return nil
}

func (f *fakeLeader) ReplicaFetch(ctx context.Context, _ int32, req ReplicaFetchRequest) (partition.ReplicaFetchResult, error) {
	if err := f.fail(); err != nil {
		return partition.ReplicaFetchResult{}, err
	}
	return f.p.HandleReplicaFetch(ctx, partition.ReplicaFetchRequest{ReplicaID: req.ReplicaID, LeaderEpoch: req.LeaderEpoch,
		FetchOffset: req.FetchOffset, MaxBytes: req.MaxBytes, MaxWait: req.MaxWait})
}

func (f *fakeLeader) OffsetForLeaderEpoch(_ context.Context, _ int32, _ storage.TopicPartition, current, requested int32) (EpochEndOffset, error) {
	if err := f.fail(); err != nil {
		return EpochEndOffset{}, err
	}
	e, end, start, err := f.p.OffsetForLeaderEpoch(current, requested)
	return EpochEndOffset{Epoch: e, EndOffset: end, LogStartOffset: start}, err
}

// newReplica opens a partition (broker id) on its own storage manager.
func newReplica(t *testing.T, id int32) (*partition.Partition, *storage.Manager) {
	t.Helper()
	ctx := context.Background()
	m, err := storage.OpenManager(ctx, storage.DefaultManagerConfig(t.TempDir()), storage.SystemClock{}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
	l, err := m.GetOrCreateLog(ctx, tp("t", 0), storage.DefaultLogConfig())
	require.NoError(t, err)
	epochs, err := partition.OpenEpochCache(ctx, l.Dir(), l.LogEndOffset())
	require.NoError(t, err)
	return partition.New(tp("t", 0), "id", l, epochs, partition.Config{LocalID: id}, slog.New(slog.DiscardHandler)), m
}

func leadAt(t *testing.T, p *partition.Partition, epoch int32) {
	t.Helper()
	require.NoError(t, p.BecomeLeader(context.Background(), partition.State{Leader: 1, LeaderEpoch: epoch, PartitionEpoch: epoch,
		Replicas: []int32{1, 2}, ISR: []int32{1}, MinInsync: 1}))
}

func appendRecords(t *testing.T, p *partition.Partition, epoch int32, n int) {
	t.Helper()
	values := make([]string, n)
	for i := range values {
		values[i] = "v"
	}
	_, _, err := p.Append(context.Background(), batchOf(t, values...), epoch, partition.AcksLeader)
	require.NoError(t, err)
}

func followAt(p *partition.Partition, epoch int32) {
	p.BecomeFollower(partition.State{Leader: 1, LeaderEpoch: epoch, PartitionEpoch: epoch, Replicas: []int32{1, 2}, ISR: []int32{1}})
}

// stampedBatch is a batch as a leader would have written it.
func stampedBatch(t *testing.T, base int64, epoch int32, n int) record.Batch {
	t.Helper()
	values := make([]string, n)
	for i := range values {
		values[i] = "v"
	}
	b := record.Batch(batchOf(t, values...))
	b.SetBaseOffset(base)
	b.SetLeaderEpoch(epoch)
	return b
}

// fetcherBroker is a Broker with just what a fetcher uses.
func fetcherBroker(m *storage.Manager, follower *partition.Partition, leader LeaderClient) *Broker {
	return &Broker{
		cfg: Config{BrokerID: 2, DataDir: filepath.Dir(follower.Log().Dir()), LeaderClient: leader,
			ReplicaFetchMaxBytes: 1 << 20, ReplicaFetchWait: 20 * time.Millisecond},
		mgr: m, clock: SystemClock{}, logger: slog.New(slog.DiscardHandler),
		partitions: map[storage.TopicPartition]*partition.Partition{follower.TopicPartition(): follower},
		hwCkpt:     map[storage.TopicPartition]int64{},
	}
}

// runFetcherUntil runs a fetcher until cond holds, then stops it.
func runFetcherUntil(t *testing.T, b *Broker, p *partition.Partition, epoch int32, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.runFetcher(ctx, p, 1, epoch)
	}()
	require.Eventually(t, cond, 10*time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

func readAll(t *testing.T, p *partition.Partition) []byte {
	t.Helper()
	var out bytes.Buffer
	l := p.Log()
	for off := l.LogStartOffset(); off < l.LogEndOffset(); {
		res, err := l.Read(context.Background(), off, 1<<20, l.LogEndOffset())
		require.NoError(t, err)
		batches, err := record.SplitBatches(res.Data)
		require.NoError(t, err)
		out.Write(res.Data)
		off = batches[len(batches)-1].LastOffset() + 1
	}
	return out.Bytes()
}

func TestFetcherTruncatesDivergentFollower(t *testing.T) {
	ctx := context.Background()
	leader, _ := newReplica(t, 1)
	leadAt(t, leader, 0)
	appendRecords(t, leader, 0, 8)
	leadAt(t, leader, 1)
	appendRecords(t, leader, 1, 12)
	leadAt(t, leader, 3)
	appendRecords(t, leader, 3, 5) // leader epochs [(0,0),(1,8),(3,20)], LEO 25

	follower, fm := newReplica(t, 2)
	followAt(follower, 2)
	require.NoError(t, follower.AppendAsFollower(ctx, 2, stampedBatch(t, 0, 0, 8)))
	require.NoError(t, follower.AppendAsFollower(ctx, 2, stampedBatch(t, 8, 0, 2)))  // diverges at 8
	require.NoError(t, follower.AppendAsFollower(ctx, 2, stampedBatch(t, 10, 2, 5))) // follower epochs [(0,0),(2,10)]
	followAt(follower, 3)

	b := fetcherBroker(fm, follower, &fakeLeader{p: leader})
	runFetcherUntil(t, b, follower, 3, func() bool { return follower.Log().LogEndOffset() == 25 })
	require.Equal(t, readAll(t, leader), readAll(t, follower), "byte-identical after truncation and catch-up")
	// The truncation made the recovery point durable before the follower appended again (R20).
	rps, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(filepath.Dir(follower.Log().Dir()), storage.RecoveryPointCheckpointFile))
	require.NoError(t, err)
	rp, ok := rps[tp("t", 0)]
	require.True(t, ok, "recovery-point checkpoint written on truncation")
	require.LessOrEqual(t, rp, int64(8), "at or below the truncation point")
	hws, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(filepath.Dir(follower.Log().Dir()), HWCheckpointFile))
	require.NoError(t, err)
	require.Contains(t, hws, tp("t", 0), "HW checkpoint written on truncation")
	require.Equal(t, []int32{0, 1, 3}, epochsOf(follower))
}

func epochsOf(p *partition.Partition) []int32 {
	var out []int32
	for e := int32(0); e < 10; e++ {
		if got, _ := p.LocalEndOffsetFor(e); got == e {
			out = append(out, e)
		}
	}
	return out
}

// TestFetcherDropsEmptyEpoch: a follower that once became leader without appending holds an
// empty latest epoch the new leader never saw. Truncation must drop it and fetching resume
// (KIP-279: both logs contain the leader's answered epoch, so no further iteration).
func TestFetcherDropsEmptyEpoch(t *testing.T) {
	ctx := context.Background()
	leader, _ := newReplica(t, 1)
	leadAt(t, leader, 0)
	appendRecords(t, leader, 0, 5)
	leadAt(t, leader, 2)
	appendRecords(t, leader, 2, 3) // leader epochs [(0,0),(2,5)], LEO 8

	follower, fm := newReplica(t, 2)
	followAt(follower, 0)
	require.NoError(t, follower.AppendAsFollower(ctx, 0, stampedBatch(t, 0, 0, 5)))
	require.NoError(t, follower.BecomeLeader(ctx, partition.State{Leader: 2, LeaderEpoch: 1, Replicas: []int32{1, 2}, ISR: []int32{2}, MinInsync: 1}))
	followAt(follower, 2) // epochs [(0,0),(1,5)] with epoch 1 empty

	b := fetcherBroker(fm, follower, &fakeLeader{p: leader})
	runFetcherUntil(t, b, follower, 2, func() bool { return follower.Log().LogEndOffset() == 8 })
	require.Equal(t, readAll(t, leader), readAll(t, follower))
	require.Equal(t, []int32{0, 2}, epochsOf(follower))
}

func TestFetcherRetriesNotLeader(t *testing.T) {
	leader, _ := newReplica(t, 1)
	leadAt(t, leader, 0)
	appendRecords(t, leader, 0, 3)
	follower, fm := newReplica(t, 2)
	followAt(follower, 0)
	fl := &fakeLeader{p: leader, failN: 2}
	b := fetcherBroker(fm, follower, fl)
	runFetcherUntil(t, b, follower, 0, func() bool { return follower.Log().LogEndOffset() == 3 })
	require.Equal(t, int64(3), leader.HighWatermark(), "the leader saw the follower's progress")
	runFetcherUntil(t, b, follower, 0, func() bool { return follower.HighWatermark() == 3 })
}

func TestFetcherBelowLeaderLogStart(t *testing.T) {
	ctx := context.Background()
	leader, _ := newReplica(t, 1)
	followAt(leader, 0)
	require.NoError(t, leader.TruncateFullyAndStartAt(ctx, 0, 100)) // as if retention removed 0..99
	leadAt(t, leader, 1)
	appendRecords(t, leader, 1, 5)

	follower, fm := newReplica(t, 2)
	followAt(follower, 1)
	b := fetcherBroker(fm, follower, &fakeLeader{p: leader})
	runFetcherUntil(t, b, follower, 1, func() bool { return follower.Log().LogEndOffset() == 105 })
	require.Equal(t, int64(100), follower.Log().LogStartOffset())
	require.Equal(t, readAll(t, leader), readAll(t, follower))
}

// blockingLeader parks every RPC until ctx ends (a leader long-polling forever).
type blockingLeader struct{ calls chan struct{} }

func (l *blockingLeader) ReplicaFetch(ctx context.Context, _ int32, _ ReplicaFetchRequest) (partition.ReplicaFetchResult, error) {
	l.calls <- struct{}{}
	<-ctx.Done()
	return partition.ReplicaFetchResult{}, ctx.Err()
}

func (l *blockingLeader) OffsetForLeaderEpoch(ctx context.Context, _ int32, _ storage.TopicPartition, _, _ int32) (EpochEndOffset, error) {
	l.calls <- struct{}{}
	<-ctx.Done()
	return EpochEndOffset{}, ctx.Err()
}

// TestFollowerLifecycle checks that deleting a followed topic and closing a broker that follows
// partitions both stop the fetchers (goleak verifies none is left).
func TestFollowerLifecycle(t *testing.T) {
	for _, deleteFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "delete while following", false: "close while following"}[deleteFirst], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger := slog.New(slog.DiscardHandler)
			store, err := openLocalStore(t, dir)
			require.NoError(t, err)
			leader := &blockingLeader{calls: make(chan struct{}, 100)}
			b, err := Open(ctx, Config{BrokerID: 1, DataDir: dir, TopicDefaults: testDefaults(), FileDeleteDelay: time.Minute,
				LeaderClient: leader}, store, SystemClock{}, logger)
			require.NoError(t, err)
			// Broker 2 leads: broker 1 follows.
			cmd := &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
				CreateTopic: &streamqv1.CreateTopicCommand{Name: "f", TopicId: "fid", Partitions: []*streamqv1.PartitionAssignment{
					{Partition: 0, Replicas: []int32{2, 1}}, {Partition: 1, Replicas: []int32{2, 1}},
				}},
			}}
			require.NoError(t, store.Propose(ctx, cmd))
			require.NoError(t, b.WaitTopicReady(ctx, "f", "fid"))
			<-leader.calls
			<-leader.calls
			p, ok := b.Partition(tp("f", 0))
			require.True(t, ok)
			require.False(t, p.IsLeader())
			if deleteFirst {
				require.NoError(t, store.Propose(ctx, deleteCmd("f")))
				require.NoError(t, b.WaitTopicDeleted(ctx, "f", "fid"))
				b.fetchMu.Lock()
				require.Empty(t, b.fetchers)
				b.fetchMu.Unlock()
			}
			require.NoError(t, b.Close(ctx))
			require.NoError(t, store.Close())
		})
	}
}

// openLocalStore opens a local metadata store for broker 1 with an unfenced broker 2 registered.
func openLocalStore(t *testing.T, dir string) (*local.Store, error) {
	t.Helper()
	ctx := context.Background()
	store, err := local.Open(ctx, dir, metadata.Broker{ID: 1, Host: "localhost", Port: 9092}, slog.New(slog.DiscardHandler))
	if err != nil {
		return nil, err
	}
	require.NoError(t, store.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion,
		Cmd: &streamqv1.MetadataCommand_RegisterBroker{RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: 2, Host: "localhost", Port: 9093}}}))
	b2, ok := store.Snapshot().Broker(2)
	require.True(t, ok)
	if b2.Fenced {
		require.NoError(t, store.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion,
			Cmd: &streamqv1.MetadataCommand_UnfenceBroker{UnfenceBroker: &streamqv1.UnfenceBrokerCommand{BrokerId: 2, BrokerEpoch: b2.Epoch}}}))
	}
	return store, nil
}

// TestReplicaFetchDuringShutdownFails: a closing leader answers replica fetches with an error so
// remote fetchers back off instead of spinning on empty successes.
func TestReplicaFetchDuringShutdownFails(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("s", "sid", 1)
	e.b.BeginShutdown()
	_, err := e.b.ReplicaFetch(context.Background(), ReplicaFetchRequest{TP: tp("s", 0), ReplicaID: 2, LeaderEpoch: 0})
	require.ErrorIs(t, err, partition.ErrNotLeader)
	require.ErrorIs(t, err, ErrShuttingDown)
}
