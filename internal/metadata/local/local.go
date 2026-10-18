// Package local implements a single-node metadata.Store: in-memory state persisted as JSON in
// <data_dir>/metadata.json (SPEC §2.2, M2–M4).
//
// Lock ordering: the package has a single lock, Store.mu. It guards the state, the notification
// queue, the subscriber list, the in-flight delivery marker and the closed flag. Subscriber
// callbacks always run with mu released, so callbacks may call any Store method.
package local

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// FileName is the metadata file inside the data dir.
const FileName = "metadata.json"

// tmpSuffix is appended to FileName for the atomic-write temporary file.
const tmpSuffix = ".tmp"

// fileMode is the permission of the metadata file.
const fileMode fs.FileMode = 0o640

// queued is one change waiting for delivery, tagged with its apply sequence number.
type queued struct {
	seq    uint64
	change metadata.Change
}

// subscriber is one registered callback. removed is guarded by Store.mu.
type subscriber struct {
	fn       func(context.Context, metadata.Change)
	startSeq uint64 // first change sequence this subscriber receives
	removed  bool
}

// Store is the single-node metadata store. The local broker is the only registered broker and
// the controller.
//
// Propose applies each command to a deep copy of the state, persists the copy atomically (tmp,
// fsync, rename, dir fsync), and only then swaps it in and queues the changes. A persist failure
// returns an error and leaves the state unchanged. One owned notifier goroutine delivers changes
// to subscribers in order; Close stops it.
type Store struct {
	dir    string
	path   string
	logger *slog.Logger

	// ctx is passed to subscriber callbacks; cancel is called by Close.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the notifier goroutine exits

	// notifierGID is the goroutine id of the notifier, used to detect re-entrant Unsubscribe and
	// Close calls made from inside a callback.
	notifierGID atomic.Uint64

	mu         sync.Mutex
	cond       *sync.Cond // on mu; signalled on new changes, delivery completion and Close
	state      *metadata.State
	nextSeq    uint64 // sequence number of the next applied change
	applied    uint64 // number of commands applied since Open (Snapshot.Index / Change.Index)
	queue      []queued
	subs       []*subscriber
	delivering *subscriber // subscriber whose callback is running, or nil
	closed     bool
}

var _ metadata.Store = (*Store)(nil)

// Open loads <dataDir>/metadata.json (or starts empty), registers self as the only broker and
// controller, and starts the notifier goroutine.
func Open(ctx context.Context, dataDir string, self metadata.Broker, logger *slog.Logger) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("open metadata store: %w", err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	path := filepath.Join(dataDir, FileName)
	state, err := load(path, self)
	if err != nil {
		return nil, err
	}
	// The store's lifetime ctx is deliberately detached from the Open ctx: it lives until Close.
	storeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Store{
		dir:    dataDir,
		path:   path,
		logger: logger,
		ctx:    storeCtx,
		cancel: cancel,
		done:   make(chan struct{}),
		state:  state,
	}
	s.cond = sync.NewCond(&s.mu)
	started := make(chan struct{})
	go s.notify(started)
	<-started
	logger.LogAttrs(ctx, slog.LevelInfo, "metadata store opened",
		slog.Int("broker_id", int(self.ID)), slog.Int("topics", len(state.Topics)))
	return s, nil
}

// load reads the persisted state, or returns a fresh one if the file does not exist. The broker
// list and controller are always replaced with self.
func load(path string, self metadata.Broker) (*metadata.State, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return metadata.NewState([]metadata.Broker{self}, self.ID), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read metadata %s: %w", path, err)
	}
	var st metadata.State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", path, err)
	}
	if st.Topics == nil {
		st.Topics = make(map[string]*metadata.Topic)
	}
	if st.Groups == nil {
		st.Groups = make(map[string]metadata.GroupOffsets) // pre-M4 files have no "groups" (D28)
	}
	for g, offs := range st.Groups {
		for topic, parts := range offs {
			for p, off := range parts {
				if off < 0 || p < 0 {
					return nil, fmt.Errorf("decode %s: group %q offset %d for %s/%d is invalid", FileName, g, off, topic, p)
				}
			}
		}
	}
	for name, t := range st.Topics {
		if t == nil || t.Name != name {
			return nil, fmt.Errorf("decode metadata %s: malformed topic entry %q", path, name)
		}
	}
	st.Brokers = []metadata.Broker{self}
	st.ControllerID = self.ID
	return &st, nil
}

// Snapshot implements metadata.Store.
func (s *Store) Snapshot() metadata.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Store) snapshotLocked() metadata.Snapshot {
	snap := s.state.Snapshot()
	snap.Index = s.applied
	return snap
}

// ProposeAsLeader implements metadata.Store; the local broker is always the controller.
func (s *Store) ProposeAsLeader(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	return s.Propose(ctx, cmd)
}

// Propose implements metadata.Store.
func (s *Store) Propose(ctx context.Context, cmd *streamqv1.MetadataCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return metadata.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("propose: %w", err)
	}
	next := s.state.Clone()
	changes, err := next.Apply(cmd)
	if err != nil {
		return fmt.Errorf("propose: %w", err)
	}
	if err := s.persist(next); err != nil {
		return fmt.Errorf("propose: %w", err)
	}
	s.state = next
	s.applied++
	for _, c := range changes {
		c.Index = s.applied
		s.queue = append(s.queue, queued{seq: s.nextSeq, change: c})
		s.nextSeq++
	}
	if len(changes) > 0 {
		s.cond.Broadcast()
	}
	return nil
}

// persist writes st atomically: tmp file, fsync, rename over FileName, fsync of the directory.
func (s *Store) persist(st *metadata.State) (err error) {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	tmp := s.path + tmpSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = f.Close()      // best effort; the write already failed
			_ = os.Remove(tmp) // best effort cleanup of the partial file
		}
	}()
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	renamed = true
	if err := syncDir(s.dir); err != nil {
		return err
	}
	return nil
}

// syncDir fsyncs a directory so a preceding rename is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close() // the sync error is the one worth reporting
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", dir, err)
	}
	return nil
}

// Subscribe implements metadata.Store.
func (s *Store) Subscribe(fn func(context.Context, metadata.Change)) (metadata.Snapshot, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := &subscriber{fn: fn, startSeq: s.nextSeq}
	s.subs = append(s.subs, sub)
	var once sync.Once
	return s.snapshotLocked(), func() { once.Do(func() { s.unsubscribe(sub) }) }
}

// unsubscribe removes sub and waits for its in-flight callback, unless it is called from the
// notifier goroutine (i.e. from inside a callback), in which case it only marks sub removed.
func (s *Store) unsubscribe(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub.removed = true
	s.subs = slices.DeleteFunc(s.subs, func(x *subscriber) bool { return x == sub })
	if s.onNotifier() {
		return
	}
	for s.delivering == sub {
		s.cond.Wait()
	}
}

// Close implements metadata.Store. Undelivered changes are dropped.
func (s *Store) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.queue = nil
		s.cancel()
		s.cond.Broadcast()
	}
	s.mu.Unlock()
	if !s.onNotifier() {
		<-s.done
	}
	return nil
}

// notify is the notifier goroutine: it delivers queued changes to subscribers in order.
func (s *Store) notify(started chan<- struct{}) {
	defer close(s.done)
	s.notifierGID.Store(goroutineID())
	close(started)

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return
		}
		item := s.queue[0]
		s.queue[0] = queued{}
		s.queue = s.queue[1:]
		for _, sub := range slices.Clone(s.subs) {
			if s.closed {
				return
			}
			if sub.removed || item.seq < sub.startSeq {
				continue
			}
			s.delivering = sub
			s.mu.Unlock()
			sub.fn(s.ctx, item.change)
			s.mu.Lock()
			s.delivering = nil
			s.cond.Broadcast()
		}
	}
}

// onNotifier reports whether the caller runs on the notifier goroutine.
func (s *Store) onNotifier() bool {
	return s.notifierGID.Load() == goroutineID()
}

// goroutineID returns the current goroutine's id, parsed from the "goroutine N [" stack header.
// It is used only to detect re-entrant calls from subscriber callbacks.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	b := bytes.TrimPrefix(buf[:n], []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i >= 0 {
		b = b[:i]
	}
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
