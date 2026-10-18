package local_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metadata/local"
)

const (
	testTimeout = 10 * time.Second
	testTick    = 5 * time.Millisecond
)

var self = metadata.Broker{ID: 1, Host: "127.0.0.1", Port: 9092} //nolint:gochecknoglobals // immutable test fixture

func createCmd(name string) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd: &streamqv1.MetadataCommand_CreateTopic{CreateTopic: &streamqv1.CreateTopicCommand{
			Name:       name,
			TopicId:    "id-" + name,
			Configs:    map[string]string{"retention.ms": "1000"},
			Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}, {Partition: 1, Replicas: []int32{1}}},
		}},
	}
}

func deleteCmd(name string) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd:     &streamqv1.MetadataCommand_DeleteTopic{DeleteTopic: &streamqv1.DeleteTopicCommand{Name: name}},
	}
}

func openStore(t *testing.T, dir string) *local.Store {
	t.Helper()
	s, err := local.Open(context.Background(), dir, self, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func TestOpenFresh(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	snap := s.Snapshot()
	require.Equal(t, []metadata.Broker{self}, snap.Brokers)
	require.Equal(t, self.ID, snap.ControllerID)
	require.Empty(t, snap.Topics)
}

func TestPersistAndReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	s, err := local.Open(ctx, dir, self, nil)
	require.NoError(t, err)
	require.NoError(t, s.Propose(ctx, createCmd("a")))
	require.NoError(t, s.Propose(ctx, createCmd("b")))
	require.NoError(t, s.Propose(ctx, deleteCmd("a")))
	want := s.Snapshot()
	require.NoError(t, s.Close())

	info, err := os.Stat(filepath.Join(dir, local.FileName))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	_, err = os.Stat(filepath.Join(dir, local.FileName+".tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// Reopen as a broker with a different address: topics survive, brokers are replaced.
	other := metadata.Broker{ID: 1, Host: "10.0.0.1", Port: 1234}
	s2, err := local.Open(ctx, dir, other, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s2.Close()) })
	got := s2.Snapshot()
	require.Equal(t, want.Topics, got.Topics)
	require.Equal(t, []metadata.Broker{other}, got.Brokers)
	require.Equal(t, int32(1), got.ControllerID)
	require.ErrorIs(t, s2.Propose(ctx, createCmd("b")), metadata.ErrTopicExists)
}

func TestOpenErrors(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := local.Open(ctx, t.TempDir(), self, nil)
	require.ErrorIs(t, err, context.Canceled)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, local.FileName), []byte("{not json"), 0o600))
	_, err = local.Open(context.Background(), dir, self, nil)
	require.Error(t, err)
}

func TestProposeErrorsLeaveStateUnchanged(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	ctx := context.Background()
	require.NoError(t, s.Propose(ctx, createCmd("a")))
	before := s.Snapshot()

	require.ErrorIs(t, s.Propose(ctx, createCmd("a")), metadata.ErrTopicExists)
	require.ErrorIs(t, s.Propose(ctx, deleteCmd("zzz")), metadata.ErrUnknownTopic)

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, s.Propose(cctx, createCmd("b")), context.Canceled)
	require.Equal(t, before, s.Snapshot())
}

// recorder collects the changes delivered to one subscriber.
type recorder struct {
	ch chan metadata.Change
}

func newRecorder(buf int) *recorder { return &recorder{ch: make(chan metadata.Change, buf)} }

func (r *recorder) fn(_ context.Context, c metadata.Change) { r.ch <- c }

func TestSubscribeOrdering(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	ctx := context.Background()
	const n = 100
	rec := newRecorder(n)
	snap, unsub := s.Subscribe(rec.fn)
	defer unsub()
	require.Empty(t, snap.Topics)

	for i := range n {
		require.NoError(t, s.Propose(ctx, createCmd(fmt.Sprintf("t%03d", i))))
	}
	for i := range n {
		c := <-rec.ch
		require.Equal(t, metadata.TopicCreated, c.Kind)
		require.Equal(t, fmt.Sprintf("t%03d", i), c.Topic)
		require.Equal(t, "id-"+c.Topic, c.TopicID)
		require.Len(t, c.Partitions, 2)
	}
}

func TestSubscribeAtomicity(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	ctx := context.Background()
	const n = 200
	const subscribers = 20

	type subState struct {
		mu    sync.Mutex
		seen  map[string]int
		count int
	}
	states := make([]*subState, subscribers)
	var unsubs []func()
	defer func() {
		for _, u := range unsubs {
			u()
		}
	}()

	proposed := make(chan int, n)
	errc := make(chan error, 1)
	go func() {
		for i := range n {
			if err := s.Propose(ctx, createCmd(fmt.Sprintf("t%03d", i))); err != nil {
				errc <- err
				return
			}
			proposed <- i
		}
		errc <- nil
	}()

	for k := range subscribers {
		// Spread subscriptions across the proposal stream.
		for range n / subscribers / 2 {
			<-proposed
		}
		st := &subState{seen: map[string]int{}}
		states[k] = st
		st.mu.Lock()
		snap, unsub := s.Subscribe(func(_ context.Context, c metadata.Change) {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.seen[c.Topic]++
			st.count++
		})
		for name := range snap.Topics {
			st.seen[name]++
			st.count++
		}
		st.mu.Unlock()
		unsubs = append(unsubs, unsub)
	}
	require.NoError(t, <-errc)

	for k, st := range states {
		require.Eventually(t, func() bool {
			st.mu.Lock()
			defer st.mu.Unlock()
			return st.count >= n
		}, testTimeout, testTick, "subscriber %d", k)
		st.mu.Lock()
		require.Len(t, st.seen, n, "subscriber %d", k)
		for name, c := range st.seen {
			require.Equal(t, 1, c, "subscriber %d saw %s %d times", k, name, c)
		}
		st.mu.Unlock()
	}
}

func TestUnsubscribeBlocksWhileCallbackInFlight(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	ctx := context.Background()

	entered := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	_, unsub := s.Subscribe(func(context.Context, metadata.Change) {
		calls++
		if calls == 1 {
			close(entered)
			<-release
		}
	})
	require.NoError(t, s.Propose(ctx, createCmd("a")))
	<-entered

	unsubDone := make(chan struct{})
	go func() {
		unsub()
		close(unsubDone)
	}()
	select {
	case <-unsubDone:
		t.Fatal("unsubscribe returned while the callback was in flight")
	default:
	}
	close(release)
	<-unsubDone
	unsub() // idempotent

	// Later changes are not delivered to the removed subscriber; a later subscriber sees them.
	rec := newRecorder(1)
	_, unsub2 := s.Subscribe(rec.fn)
	defer unsub2()
	require.NoError(t, s.Propose(ctx, createCmd("b")))
	require.Equal(t, "b", (<-rec.ch).Topic)
	require.Equal(t, 1, calls)
}

func TestUnsubscribeFromInsideCallback(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir())
	ctx := context.Background()

	var unsub func()
	got := make(chan string, 4)
	_, unsub = s.Subscribe(func(_ context.Context, c metadata.Change) {
		unsub() // must not deadlock
		unsub() // idempotent
		got <- c.Topic
	})
	rec := newRecorder(2)
	_, unsub2 := s.Subscribe(rec.fn)
	defer unsub2()

	require.NoError(t, s.Propose(ctx, createCmd("a")))
	require.NoError(t, s.Propose(ctx, createCmd("b")))
	require.Equal(t, "a", (<-rec.ch).Topic)
	require.Equal(t, "b", (<-rec.ch).Topic)
	unsub() // still idempotent from outside
	require.Equal(t, "a", <-got)
	require.Empty(t, got)
}

func TestPersistFailureTmpIsDirectory(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := openStore(t, dir)
	ctx := context.Background()
	require.NoError(t, s.Propose(ctx, createCmd("a")))
	rec := newRecorder(4)
	_, unsub := s.Subscribe(rec.fn)
	defer unsub()
	before := s.Snapshot()

	tmp := filepath.Join(dir, local.FileName+".tmp")
	require.NoError(t, os.Mkdir(tmp, 0o750))
	require.Error(t, s.Propose(ctx, createCmd("b")))
	require.Equal(t, before, s.Snapshot())

	// After the fault clears, the next proposal is the first change delivered.
	require.NoError(t, os.Remove(tmp))
	require.NoError(t, s.Propose(ctx, createCmd("c")))
	require.Equal(t, "c", (<-rec.ch).Topic)
	_, ok := s.Snapshot().Topic("b")
	require.False(t, ok)

	// The persisted file never saw "b".
	s2, err := local.Open(ctx, dir, self, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()
	require.Equal(t, s.Snapshot().Topics, s2.Snapshot().Topics)
	_, ok = s2.Snapshot().Topic("b")
	require.False(t, ok)
}

func TestPersistFailureReadOnlyDir(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	s := openStore(t, dir)
	ctx := context.Background()
	require.NoError(t, s.Propose(ctx, createCmd("a")))
	before := s.Snapshot()

	require.NoError(t, os.Chmod(dir, 0o500))       //nolint:gosec // test makes the dir read-only on purpose
	t.Cleanup(func() { _ = os.Chmod(dir, 0o750) }) //nolint:gosec // restore so TempDir cleanup works
	require.Error(t, s.Propose(ctx, createCmd("b")))
	require.Equal(t, before, s.Snapshot())

	require.NoError(t, os.Chmod(dir, 0o750)) //nolint:gosec // restore write permission
	s2, err := local.Open(ctx, dir, self, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()
	require.Equal(t, before.Topics, s2.Snapshot().Topics)
}

func TestClose(t *testing.T) {
	t.Parallel()
	s, err := local.Open(context.Background(), t.TempDir(), self, nil)
	require.NoError(t, err)

	ctxs := make(chan context.Context, 1)
	_, unsub := s.Subscribe(func(ctx context.Context, _ metadata.Change) { ctxs <- ctx })
	require.NoError(t, s.Propose(context.Background(), createCmd("a")))
	cbCtx := <-ctxs
	require.NoError(t, cbCtx.Err())

	require.NoError(t, s.Close())
	require.NoError(t, s.Close()) // idempotent
	require.ErrorIs(t, cbCtx.Err(), context.Canceled)
	require.ErrorIs(t, s.Propose(context.Background(), createCmd("b")), metadata.ErrClosed)
	unsub() // safe after Close
	_, ok := s.Snapshot().Topic("a")
	require.True(t, ok)
}

func TestCloseFromInsideCallback(t *testing.T) {
	t.Parallel()
	s, err := local.Open(context.Background(), t.TempDir(), self, nil)
	require.NoError(t, err)
	closeErr := make(chan error, 1)
	_, unsub := s.Subscribe(func(context.Context, metadata.Change) {
		closeErr <- s.Close() // must not deadlock on the notifier waiting for itself
	})
	require.NoError(t, s.Propose(context.Background(), createCmd("a")))
	require.NoError(t, <-closeErr)
	unsub()
	require.NoError(t, s.Close()) // waits for the notifier to exit
}
