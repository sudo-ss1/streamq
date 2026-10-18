package raftstore

import (
	"bytes"
	"context"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/sudo-ss1/streamq/internal/metadata"
)

// notifier delivers changes to subscribers in order on one owned goroutine (the same contract as
// the local store): publish never blocks on subscribers; unsubscribe waits for an in-flight
// callback unless called from inside a callback.
//
// Lock ordering: fsm.mu → notifier.mu (publish and subscribe are called with fsm.mu held).
type notifier struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	gid    atomic.Uint64

	mu         sync.Mutex
	cond       *sync.Cond
	nextSeq    uint64
	queue      []queuedChange
	subs       []*subscriber
	delivering *subscriber
	closed     bool
}

type queuedChange struct {
	seq    uint64
	change metadata.Change
}

type subscriber struct {
	fn       func(context.Context, metadata.Change)
	startSeq uint64
	removed  bool
}

func newNotifier() *notifier {
	ctx, cancel := context.WithCancel(context.Background())
	n := &notifier{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	n.cond = sync.NewCond(&n.mu)
	started := make(chan struct{})
	go n.run(started)
	<-started
	return n
}

// publish queues changes for delivery.
func (n *notifier) publish(changes []metadata.Change) {
	if len(changes) == 0 {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	for _, c := range changes {
		n.queue = append(n.queue, queuedChange{seq: n.nextSeq, change: c})
		n.nextSeq++
	}
	n.cond.Broadcast()
}

// subscribe registers fn for every change published after this call.
func (n *notifier) subscribe(fn func(context.Context, metadata.Change)) func() {
	n.mu.Lock()
	defer n.mu.Unlock()
	sub := &subscriber{fn: fn, startSeq: n.nextSeq}
	n.subs = append(n.subs, sub)
	var once sync.Once
	return func() { once.Do(func() { n.unsubscribe(sub) }) }
}

func (n *notifier) unsubscribe(sub *subscriber) {
	n.mu.Lock()
	defer n.mu.Unlock()
	sub.removed = true
	n.subs = slices.DeleteFunc(n.subs, func(x *subscriber) bool { return x == sub })
	if n.onNotifier() {
		return
	}
	for n.delivering == sub {
		n.cond.Wait()
	}
}

func (n *notifier) close() {
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		n.queue = nil
		n.cancel()
		n.cond.Broadcast()
	}
	n.mu.Unlock()
	if !n.onNotifier() {
		<-n.done
	}
}

func (n *notifier) run(started chan<- struct{}) {
	defer close(n.done)
	n.gid.Store(goroutineID())
	close(started)
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for len(n.queue) == 0 && !n.closed {
			n.cond.Wait()
		}
		if n.closed {
			return
		}
		item := n.queue[0]
		n.queue[0] = queuedChange{}
		n.queue = n.queue[1:]
		for _, sub := range slices.Clone(n.subs) {
			if n.closed {
				return
			}
			if sub.removed || item.seq < sub.startSeq {
				continue
			}
			n.delivering = sub
			n.mu.Unlock()
			sub.fn(n.ctx, item.change)
			n.mu.Lock()
			n.delivering = nil
			n.cond.Broadcast()
		}
	}
}

func (n *notifier) onNotifier() bool { return n.gid.Load() == goroutineID() }

// goroutineID parses the current goroutine id from the stack header; it only detects re-entrant
// calls from subscriber callbacks.
func goroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i >= 0 {
		b = b[:i]
	}
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
