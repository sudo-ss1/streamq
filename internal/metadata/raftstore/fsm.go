package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// snapshotVersion is the FSM snapshot format version (DECISIONS D37).
const snapshotVersion = 1

// snapshotFile is the persisted FSM snapshot.
type snapshotFile struct {
	Version int             `json:"version"`
	Index   uint64          `json:"index"`
	State   *metadata.State `json:"state"`
}

// fsm applies metadata commands deterministically (raft.FSM). Command errors are returned as the
// apply response on every node and leave the state unchanged.
type fsm struct {
	notifier *notifier

	mu      sync.Mutex
	state   *metadata.State
	index   uint64        // last applied log index
	applied chan struct{} // closed and replaced when index advances
}

func newFSM(n *notifier) *fsm {
	return &fsm{notifier: n, state: metadata.NewState(nil, 0), applied: make(chan struct{})}
}

// Apply implements raft.FSM.
func (f *fsm) Apply(l *raft.Log) any {
	cmd := &streamqv1.MetadataCommand{}
	unmarshalErr := proto.Unmarshal(l.Data, cmd)

	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.advance(l.Index)
	if unmarshalErr != nil {
		f.notifier.publish([]metadata.Change{{Kind: metadata.Applied, Index: l.Index}})
		return fmt.Errorf("decode metadata command: %w", metadata.ErrInvalidCommand)
	}
	next := f.state.Clone()
	changes, err := next.ApplyInTerm(cmd, l.Term)
	if err != nil {
		// Rejected deterministically: still let subscribers see the index advance.
		f.notifier.publish([]metadata.Change{{Kind: metadata.Applied, Index: l.Index}})
		return err
	}
	f.state = next
	if len(changes) == 0 {
		changes = []metadata.Change{{Kind: metadata.Applied}}
	}
	for i := range changes {
		changes[i].Index = l.Index
	}
	f.notifier.publish(changes)
	return nil
}

// advance records index as applied and wakes waiters. Caller holds mu.
func (f *fsm) advance(index uint64) {
	if index > f.index {
		f.index = index
		close(f.applied)
		f.applied = make(chan struct{})
	}
}

// snapshot returns the current state as a metadata.Snapshot with its index.
func (f *fsm) snapshot() metadata.Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := f.state.Snapshot()
	snap.Index = f.index
	return snap
}

func (f *fsm) appliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.index
}

// waitApplied blocks until index has been applied or ctx ends.
func (f *fsm) waitApplied(ctx context.Context, index uint64) error {
	for {
		f.mu.Lock()
		done := f.index >= index
		ch := f.applied
		f.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return fmt.Errorf("wait for metadata index %d: %w", index, ctx.Err())
		}
	}
}

// Snapshot implements raft.FSM.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(snapshotFile{Version: snapshotVersion, Index: f.index, State: f.state.Clone()})
	if err != nil {
		return nil, fmt.Errorf("encode metadata snapshot: %w", err)
	}
	return fsmSnapshot(data), nil
}

// Restore implements raft.FSM. Subscribers get a Restored change carrying the restored state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var sf snapshotFile
	if err := json.NewDecoder(rc).Decode(&sf); err != nil {
		return fmt.Errorf("decode metadata snapshot: %w", err)
	}
	if sf.Version != snapshotVersion || sf.State == nil {
		return fmt.Errorf("metadata snapshot version %d: %w", sf.Version, errors.ErrUnsupported)
	}
	if sf.State.Topics == nil {
		sf.State.Topics = map[string]*metadata.Topic{}
	}
	if sf.State.Groups == nil {
		sf.State.Groups = map[string]metadata.GroupOffsets{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = sf.State
	f.index = sf.Index
	close(f.applied)
	f.applied = make(chan struct{})
	snap := f.state.Snapshot()
	snap.Index = f.index
	f.notifier.publish([]metadata.Change{{Kind: metadata.Restored, Index: f.index, Snapshot: &snap}})
	return nil
}

// fsmSnapshot is a serialized state (raft.FSMSnapshot).
type fsmSnapshot []byte

// Persist implements raft.FSMSnapshot.
func (s fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write metadata snapshot: %w", err)
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (fsmSnapshot) Release() {}
