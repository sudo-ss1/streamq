package broker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// fakeClock fires After channels only when the test calls fire.
type fakeClock struct {
	mu      sync.Mutex
	waiters []chan time.Time
	armed   chan struct{} // receives a value each time After is called
}

func newFakeClock() *fakeClock { return &fakeClock{armed: make(chan struct{}, 100)} }

func (c *fakeClock) Now() time.Time { return time.UnixMilli(1_700_000_000_000) }

func (c *fakeClock) After(time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, ch)
	c.armed <- struct{}{}
	return ch
}

func (c *fakeClock) fire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.waiters {
		ch <- time.Time{}
	}
	c.waiters = nil
}

type env struct {
	t     *testing.T
	dir   string
	store *local.Store
	b     *Broker
	clock *fakeClock
}

func testDefaults() config.TopicDefaults {
	d := config.Default().TopicDefaults
	d.ReplicationFactor, d.MinInsyncReplicas = 1, 1
	return d
}

func openEnv(t *testing.T, dir string) *env {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	store, err := local.Open(ctx, dir, metadata.Broker{ID: 1, Host: "localhost", Port: 9092}, logger)
	require.NoError(t, err)
	clock := newFakeClock()
	b, err := Open(ctx, Config{BrokerID: 1, DataDir: dir, TopicDefaults: testDefaults(), FileDeleteDelay: time.Minute}, store, clock, logger)
	require.NoError(t, err)
	e := &env{t: t, dir: dir, store: store, b: b, clock: clock}
	t.Cleanup(e.close)
	return e
}

func (e *env) close() {
	if e.b == nil {
		return
	}
	require.NoError(e.t, e.b.Close(context.Background()))
	require.NoError(e.t, e.store.Close())
	e.b = nil
}

func createCmd(name, id string, n int, configs map[string]string) *streamqv1.MetadataCommand {
	var parts []*streamqv1.PartitionAssignment
	for i := range n {
		parts = append(parts, &streamqv1.PartitionAssignment{Partition: int32(i), Replicas: []int32{1}})
	}
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: name, TopicId: id, Partitions: parts, Configs: configs},
	}}
}

func deleteCmd(name string) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_DeleteTopic{
		DeleteTopic: &streamqv1.DeleteTopicCommand{Name: name},
	}}
}

func (e *env) createTopic(name, id string, n int) {
	e.t.Helper()
	ctx := context.Background()
	require.NoError(e.t, e.store.Propose(ctx, createCmd(name, id, n, nil)))
	require.NoError(e.t, e.b.WaitTopicReady(ctx, name, id))
}

func tp(topic string, p int32) storage.TopicPartition {
	return storage.TopicPartition{Topic: topic, Partition: p}
}

func batchOf(t *testing.T, values ...string) []byte {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, v := range values {
		b.Append(1000, nil, []byte(v), nil)
	}
	out, err := b.Build()
	require.NoError(t, err)
	return out
}

func (e *env) produce(topic string, p int32, values ...string) ProduceResult {
	e.t.Helper()
	res := e.b.Produce(context.Background(), partition.AcksAll, 0, []ProduceRequest{{TP: tp(topic, p), LeaderEpoch: -1, Records: batchOf(e.t, values...)}})
	require.NoError(e.t, res[0].Err)
	return res[0]
}

func values(t *testing.T, data []byte) []string {
	t.Helper()
	batches, err := record.SplitBatches(data)
	require.NoError(t, err)
	var out []string
	for _, b := range batches {
		recs, err := b.Records()
		require.NoError(t, err)
		for _, r := range recs {
			out = append(out, string(r.Value))
		}
	}
	return out
}

func TestProduceAndFetchImmediately(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 2)
	res := e.produce("t", 1, "a", "b")
	require.Equal(t, int64(0), res.BaseOffset)
	require.Equal(t, int64(2), e.produce("t", 1, "c").BaseOffset)

	out := e.b.Fetch(context.Background(), time.Second, 1, 0, []FetchRequest{
		{TP: tp("t", 0), LeaderEpoch: -1}, {TP: tp("t", 1), LeaderEpoch: -1},
	})
	require.Len(t, out, 2)
	require.NoError(t, out[0].Err)
	require.Empty(t, out[0].Records)
	require.Equal(t, int64(3), out[1].HighWatermark)
	require.Equal(t, []string{"a", "b", "c"}, values(t, out[1].Records))
}

func TestFetchUnknownPartitionReturnsImmediately(t *testing.T) {
	e := openEnv(t, t.TempDir())
	out := e.b.Fetch(context.Background(), time.Hour, 1, 0, []FetchRequest{{TP: tp("nope", 0), LeaderEpoch: -1}})
	require.ErrorIs(t, out[0].Err, ErrUnknownPartition)
}

func TestFetchLongPollWakesOnAppend(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	// Append between the fetch's read and its wait: the captured channel must already be closed.
	var once sync.Once
	e.b.afterFetchRead = func() { once.Do(func() { e.produce("t", 0, "x") }) }
	out := e.b.Fetch(context.Background(), time.Hour, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	require.Equal(t, []string{"x"}, values(t, out[0].Records))
}

func TestFetchLongPollWakesOnConcurrentAppend(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	waiting := make(chan struct{})
	e.b.afterFetchRead = func() { close(waiting) }
	done := make(chan []FetchResult)
	go func() {
		done <- e.b.Fetch(context.Background(), time.Hour, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	}()
	<-waiting
	e.produce("t", 0, "y")
	require.Equal(t, []string{"y"}, values(t, (<-done)[0].Records))
}

func TestFetchTimesOutEmpty(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	done := make(chan []FetchResult)
	go func() {
		done <- e.b.Fetch(context.Background(), 500*time.Millisecond, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	}()
	<-e.clock.armed
	e.clock.fire()
	out := <-done
	require.NoError(t, out[0].Err)
	require.Empty(t, out[0].Records)
	require.Equal(t, int64(0), out[0].HighWatermark)
}

func TestFetchMinBytes(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	e.produce("t", 0, "small")
	reads := 0
	e.b.afterFetchRead = func() {
		reads++
		if reads == 1 {
			e.produce("t", 0, strings.Repeat("z", 500))
		}
	}
	out := e.b.Fetch(context.Background(), time.Hour, 400, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	require.Len(t, values(t, out[0].Records), 2)
	require.Equal(t, 1, reads)
}

func TestFetchByteBudget(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 2)
	e.produce("t", 0, strings.Repeat("a", 300))
	e.produce("t", 1, strings.Repeat("b", 300))
	req := []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}, {TP: tp("t", 1), LeaderEpoch: -1}}
	// The first partition's batch exceeds the budget but is returned whole; the second is dropped.
	out := e.b.Fetch(context.Background(), 0, 1, 100, req)
	require.Len(t, values(t, out[0].Records), 1)
	require.Empty(t, out[1].Records)
	require.Equal(t, int64(1), out[1].HighWatermark)
	// Enough budget for both.
	out = e.b.Fetch(context.Background(), 0, 1, 10_000, req)
	require.Len(t, values(t, out[1].Records), 1)
}

func TestFetchWokenByClose(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	waiting := make(chan struct{})
	e.b.afterFetchRead = func() { close(waiting) }
	done := make(chan []FetchResult)
	go func() {
		done <- e.b.Fetch(context.Background(), time.Hour, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	}()
	<-waiting
	e.close()
	<-done
}

func TestProduceErrors(t *testing.T) {
	e := openEnv(t, t.TempDir())
	ctx := context.Background()
	require.NoError(t, e.store.Propose(ctx, createCmd("strict", "id2", 1, map[string]string{"min_insync_replicas": "2"})))
	require.NoError(t, e.b.WaitTopicReady(ctx, "strict", "id2"))
	res := e.b.Produce(ctx, partition.AcksAll, 0, []ProduceRequest{
		{TP: tp("strict", 0), LeaderEpoch: -1, Records: batchOf(t, "x")},
		{TP: tp("strict", 0), LeaderEpoch: 3, Records: batchOf(t, "x")},
		{TP: tp("missing", 0), LeaderEpoch: -1, Records: batchOf(t, "x")},
	})
	res = append(res, e.b.Produce(ctx, partition.AcksLeader, 0, []ProduceRequest{
		{TP: tp("strict", 0), LeaderEpoch: -1, Records: []byte("garbage")},
	})...)
	require.ErrorIs(t, res[0].Err, partition.ErrNotEnoughReplicas)
	require.ErrorIs(t, res[1].Err, partition.ErrNotLeader)
	require.ErrorIs(t, res[2].Err, ErrUnknownPartition)
	require.ErrorIs(t, res[3].Err, storage.ErrCorruptBatch)
	p, _ := e.b.Partition(tp("strict", 0))
	require.Equal(t, int64(0), p.Log().LogEndOffset())
}

func TestListOffsets(t *testing.T) {
	e := openEnv(t, t.TempDir())
	e.createTopic("t", "id1", 1)
	e.produce("t", 0, "a", "b")
	out := e.b.ListOffsets(context.Background(), []ListOffsetsRequest{
		{TP: tp("t", 0), LeaderEpoch: -1, Timestamp: partition.EarliestTimestamp},
		{TP: tp("t", 0), LeaderEpoch: -1, Timestamp: partition.LatestTimestamp},
		{TP: tp("t", 0), LeaderEpoch: -1, Timestamp: 1000},
		{TP: tp("t", 0), LeaderEpoch: 9, Timestamp: 1000},
	})
	require.Equal(t, int64(0), out[0].Offset)
	require.Equal(t, int64(2), out[1].Offset)
	require.Equal(t, int64(0), out[2].Offset)
	require.ErrorIs(t, out[3].Err, partition.ErrNotLeader)
}

func TestRestartRestoresDataAndHW(t *testing.T) {
	dir := t.TempDir()
	e := openEnv(t, dir)
	e.createTopic("t", "id1", 1)
	e.produce("t", 0, "a", "b", "c")
	e.close()

	ckpt, err := storage.ReadOffsetCheckpoint(context.Background(), filepath.Join(dir, HWCheckpointFile))
	require.NoError(t, err)
	require.Equal(t, int64(3), ckpt[tp("t", 0)])

	e = openEnv(t, dir)
	p, ok := e.b.Partition(tp("t", 0))
	require.True(t, ok)
	require.Equal(t, int64(3), p.HighWatermark())
	out := e.b.Fetch(context.Background(), 0, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	require.Equal(t, []string{"a", "b", "c"}, values(t, out[0].Records))
}

func partitionDirs(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []string
	for _, e := range entries {
		if _, ok := storage.ParseDirName(e.Name()); ok && e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestDeleteTopicRemovesDataAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	e := openEnv(t, dir)
	ctx := context.Background()
	e.createTopic("t", "id1", 2)
	e.produce("t", 0, "a")
	require.NoError(t, e.b.writeHWCheckpoint(ctx))
	require.NoError(t, e.store.Propose(ctx, deleteCmd("t")))
	require.NoError(t, e.b.WaitTopicDeleted(ctx, "t", "id1"))
	_, ok := e.b.Partition(tp("t", 0))
	require.False(t, ok)
	require.Empty(t, partitionDirs(t, dir))
	ckpt, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(dir, HWCheckpointFile))
	require.NoError(t, err)
	require.Empty(t, ckpt)

	// Recreate: empty, and a full recovery runs on reopen without stale recovery points.
	e.createTopic("t", "id2", 2)
	p, _ := e.b.Partition(tp("t", 0))
	require.Equal(t, int64(0), p.Log().LogEndOffset())
	e.produce("t", 0, "new")
	e.close()
	e = openEnv(t, dir)
	out := e.b.Fetch(ctx, 0, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	require.Equal(t, []string{"new"}, values(t, out[0].Records))
}

func TestStrayDirectoriesRemovedAtStartup(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e := openEnv(t, dir)
	e.createTopic("t", "id1", 1)
	e.produce("t", 0, "old")
	e.close()

	// Simulate a crash after metadata recorded delete + recreate but before the old dir was
	// renamed: the metadata says topic "t" has id2, the directory still holds id1's data.
	store, err := local.Open(ctx, dir, metadata.Broker{ID: 1}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, store.Propose(ctx, deleteCmd("t")))
	require.NoError(t, store.Propose(ctx, createCmd("t", "id2", 1, nil)))
	require.NoError(t, store.Close())
	// Also a dir with no partition.metadata and a dir for a topic that doesn't exist.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "ghost-0"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "t-7"), 0o750))

	e = openEnv(t, dir)
	require.Equal(t, []string{"t-0"}, partitionDirs(t, dir))
	p, ok := e.b.Partition(tp("t", 0))
	require.True(t, ok)
	require.Equal(t, int64(0), p.Log().LogEndOffset(), "recreated topic must not see the old data")
	require.Equal(t, "id2", p.TopicID())
}

func TestCreateFailureIsReported(t *testing.T) {
	e := openEnv(t, t.TempDir())
	ctx := context.Background()
	// segment_bytes over the limit passes Apply (configs are opaque there) but fails to open.
	require.NoError(t, e.store.Propose(ctx, createCmd("bad", "id9", 1, map[string]string{"segment_bytes": "5000000000"})))
	err := e.b.WaitTopicReady(ctx, "bad", "id9")
	require.ErrorIs(t, err, config.ErrInvalidConfig)
}

func TestWaitTopicReadyRespectsContext(t *testing.T) {
	e := openEnv(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.b.WaitTopicReady(ctx, "never", "x"), context.Canceled)
}

func TestLongTopicNameDeleteRestartRecreate(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	name := strings.Repeat("x", 249)
	e := openEnv(t, dir)
	e.createTopic(name, "id1", 2)
	e.produce(name, 0, "a")
	require.NoError(t, e.store.Propose(ctx, deleteCmd(name)))
	require.NoError(t, e.b.WaitTopicDeleted(ctx, name, "id1"))
	require.Empty(t, partitionDirs(t, dir))
	e.close()
	e = openEnv(t, dir)
	e.createTopic(name, "id2", 2)
	p, ok := e.b.Partition(tp(name, 0))
	require.True(t, ok)
	require.Equal(t, int64(0), p.Log().LogEndOffset())
}

func TestDeleteDropsCheckpointEntriesUnderConcurrentCheckpointing(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	e := openEnv(t, dir)
	e.createTopic("t", "id1", 8)
	for p := range int32(8) {
		e.produce("t", p, "a", "b")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			_ = e.b.writeHWCheckpoint(ctx)
		}
	}()
	require.NoError(t, e.store.Propose(ctx, deleteCmd("t")))
	require.NoError(t, e.b.WaitTopicDeleted(ctx, "t", "id1"))
	<-done
	for _, file := range []string{HWCheckpointFile, storage.RecoveryPointCheckpointFile} {
		ckpt, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(dir, file))
		require.NoError(t, err)
		require.Empty(t, ckpt, file)
	}

	// Recreate: the new partitions must not inherit the old offsets in either checkpoint.
	e.createTopic("t", "id2", 8)
	e.produce("t", 0, "new")
	require.NoError(t, e.b.writeHWCheckpoint(ctx))
	ckpt, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(dir, HWCheckpointFile))
	require.NoError(t, err)
	require.Equal(t, int64(1), ckpt[tp("t", 0)])
	require.Equal(t, int64(0), ckpt[tp("t", 1)])
}

func TestFetchWokenByTopicDeletion(t *testing.T) {
	e := openEnv(t, t.TempDir())
	ctx := context.Background()
	e.createTopic("t", "id1", 1)
	waiting := make(chan struct{})
	var once sync.Once
	e.b.afterFetchRead = func() { once.Do(func() { close(waiting) }) }
	done := make(chan []FetchResult)
	go func() {
		done <- e.b.Fetch(ctx, time.Hour, 1, 0, []FetchRequest{{TP: tp("t", 0), LeaderEpoch: -1}})
	}()
	<-waiting
	require.NoError(t, e.store.Propose(ctx, deleteCmd("t")))
	require.ErrorIs(t, (<-done)[0].Err, ErrUnknownPartition)
}

func TestResyncFromRestoredSnapshot(t *testing.T) {
	e := openEnv(t, t.TempDir())
	ctx := context.Background()
	e.createTopic("keep", "id-keep", 1)
	e.createTopic("gone", "id-gone", 1)
	e.createTopic("recreated", "id-old", 1)
	e.produce("keep", 0, "a")

	// A restored snapshot: "gone" deleted, "recreated" has a new id, "new" appeared, and the
	// leadership of keep/0 moved to another broker at a higher epoch.
	snap := e.store.Snapshot()
	keep := snap.Topics["keep"]
	keep.Partitions[0].Leader = 2
	keep.Partitions[0].LeaderEpoch = 4
	keep.Partitions[0].PartitionEpoch = 4
	snap.Topics["keep"] = keep
	delete(snap.Topics, "gone")
	snap.Topics["recreated"] = metadata.Topic{Name: "recreated", ID: "id-new", Partitions: []metadata.PartitionState{
		{Partition: 0, Replicas: []int32{1}, Leader: 1, ISR: []int32{1}},
	}}
	snap.Topics["new"] = metadata.Topic{Name: "new", ID: "id-n", Partitions: []metadata.PartitionState{
		{Partition: 0, Replicas: []int32{1}, Leader: 1, ISR: []int32{1}},
	}}
	snap.Index = 500
	e.b.onChange(ctx, metadata.Change{Kind: metadata.Restored, Index: 500, Snapshot: &snap})

	_, ok := e.b.Partition(tp("gone", 0))
	require.False(t, ok)
	p, ok := e.b.Partition(tp("recreated", 0))
	require.True(t, ok)
	require.Equal(t, "id-new", p.TopicID())
	require.Equal(t, int64(0), p.Log().LogEndOffset())
	_, ok = e.b.Partition(tp("new", 0))
	require.True(t, ok)
	k, _ := e.b.Partition(tp("keep", 0))
	leader, epoch := k.Leader()
	require.Equal(t, int32(2), leader)
	require.Equal(t, int32(4), epoch)
	require.Equal(t, int64(1), k.Log().LogEndOffset(), "an existing partition keeps its data")
	require.Equal(t, uint64(500), e.b.ProcessedIndex())

	// An older PartitionChanged delivered after the restore is ignored (monotonic, plan R6).
	e.b.onChange(ctx, metadata.Change{Kind: metadata.PartitionChanged, Topic: "keep", TopicID: "id-keep", Index: 400,
		Partitions: []metadata.PartitionState{{Partition: 0, Replicas: []int32{1}, Leader: 1, LeaderEpoch: 1, PartitionEpoch: 1, ISR: []int32{1}}}})
	leader, epoch = k.Leader()
	require.Equal(t, int32(2), leader)
	require.Equal(t, int32(4), epoch)
	require.Equal(t, uint64(500), e.b.ProcessedIndex(), "the processed index never goes back")
}
