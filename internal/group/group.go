// Package group is the consumer group coordinator (SPEC §5): membership, generations, rebalances
// with server-side assignment (range and round-robin assignors), heartbeats, session expiry, and
// offset commits through the metadata store.
//
// See docs/plans/M4.md (Revisions 1 and 2) for the protocol rules this package implements.
//
// Lock ordering: Coordinator.mu → the store's internal mutex, and only through Store.Snapshot()
// while holding mu. Store.Propose, Store.Subscribe and the unsubscribe func are never called while
// holding mu. Clock.AfterFunc and its stop func are called under mu and must not wait for timer
// callbacks; callbacks take mu themselves, so Close waits for them (the timers WaitGroup) and for
// an in-flight store callback (unsubscribe) only after releasing mu.
package group

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

// Sentinel errors.
var (
	ErrUnknownMemberID     = errors.New("group: unknown member id")
	ErrIllegalGeneration   = errors.New("group: illegal generation")
	ErrRebalanceInProgress = errors.New("group: rebalance in progress")
	ErrNotCoordinator      = errors.New("group: not coordinator")
	ErrCoordinatorClosed   = errors.New("group: coordinator closed")
	// ErrInvalidGroup reports a malformed request: empty group id, no common assignor, unknown
	// assignor name.
	ErrInvalidGroup = errors.New("group: invalid request")
)

// Group states.
const (
	StateEmpty              = "Empty"
	StatePreparingRebalance = "PreparingRebalance"
	StateStable             = "Stable"
)

// Assignor names.
const (
	RangeAssignorName      = "range"
	RoundRobinAssignorName = "roundrobin"
)

// Defaults used when Config leaves a timeout at zero.
const (
	defaultMaxSessionTimeout = 10 * time.Second
	defaultRebalanceTimeout  = 30 * time.Second
)

// Clock abstracts time for session, rebalance and initial-delay timers.
type Clock interface {
	Now() time.Time
	// AfterFunc runs f in its own goroutine after d; stop cancels it if it has not started.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// AfterFunc wraps time.AfterFunc.
func (SystemClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

// Config configures a Coordinator.
type Config struct {
	BrokerID int32
	// MaxSessionTimeout clamps requested session timeouts and is the default for requests that
	// send <= 0 (group_session_timeout_ms).
	MaxSessionTimeout time.Duration
	// DefaultRebalanceTimeout is used for requests that send <= 0 (group_rebalance_timeout_ms).
	DefaultRebalanceTimeout time.Duration
	// InitialRebalanceDelay delays the first rebalance of an empty group
	// (group_initial_rebalance_delay_ms).
	InitialRebalanceDelay time.Duration
}

// TopicPartition identifies a partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Assignment maps topic to sorted partitions.
type Assignment map[string][]int32

// clone returns a deep copy (never nil).
func (a Assignment) clone() Assignment {
	out := make(Assignment, len(a))
	for t, ps := range a {
		out[t] = slices.Clone(ps)
	}
	return out
}

// JoinRequest is a JoinGroup call.
type JoinRequest struct {
	GroupID          string
	MemberID         string // empty for a new member
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	Topics           []string
	Assignors        []string // preference order
}

// JoinResult is the outcome of a completed rebalance for one member.
type JoinResult struct {
	GenerationID int32
	MemberID     string
	Assignor     string
	Assignment   Assignment
}

// MemberDescription describes a member.
type MemberDescription struct {
	MemberID   string
	Topics     []string
	Assignment Assignment
}

// GroupDescription describes a group.
type GroupDescription struct { //nolint:revive // name fixed by the M4 contract (docs/plans/M4.md)
	GroupID    string
	State      string
	Generation int32
	Assignor   string
	Members    []MemberDescription
	Committed  map[TopicPartition]int64
}

// joinWaiter is one parked Join request. Its fields are written under Coordinator.mu before done
// is closed.
type joinWaiter struct {
	done     chan struct{}
	created  bool // this request created the member
	finished bool
	result   JoinResult
	err      error
}

// member is one group member. Guarded by Coordinator.mu.
type member struct {
	id         string
	topics     []string // sorted, deduplicated
	assignors  []string // preference order, deduplicated
	session    time.Duration
	rebalance  time.Duration
	joinSeq    uint64 // order of first join, for the assignor choice (R16)
	assignment Assignment
	// waiter is the parked Join, or nil. A parked member is kept alive (R2).
	waiter      *joinWaiter
	sessTok     uint64
	stopSession func()
}

// group is one consumer group's in-memory state. Guarded by Coordinator.mu.
type group struct {
	id         string
	state      string
	generation int32
	assignor   string
	members    map[string]*member
	// initialPhase is set while the initial rebalance delay of a formerly empty group runs.
	initialPhase bool
	// rebDeadline caps the whole PreparingRebalance phase (M6).
	rebDeadline time.Time
	rebTok      uint64
	stopReb     func()
}

// Coordinator coordinates every group on this broker.
type Coordinator struct {
	cfg    Config
	store  metadata.Store
	clock  Clock
	logger *slog.Logger

	// ctx is the coordinator's lifetime context, used by timer callbacks (for logging); it is
	// cancelled by Close.
	ctx         context.Context
	cancel      context.CancelFunc
	unsubscribe func()
	closeOnce   sync.Once
	// timers counts scheduled-but-not-stopped timer callbacks; Add happens under mu while !closed.
	timers sync.WaitGroup

	mu     sync.Mutex
	groups map[string]*group
	// epoch is the controller epoch (Raft term) the in-memory groups belong to; groups are
	// cleared whenever it changes, and commits are stamped with it (DECISIONS D33).
	epoch      uint64
	closed     bool
	nextMember uint64
	nextJoin   uint64
}

// New returns a Coordinator and subscribes it to store (topic changes trigger rebalances).
func New(cfg Config, store metadata.Store, clock Clock, logger *slog.Logger) *Coordinator {
	if cfg.MaxSessionTimeout <= 0 {
		cfg.MaxSessionTimeout = defaultMaxSessionTimeout
	}
	if cfg.DefaultRebalanceTimeout <= 0 {
		cfg.DefaultRebalanceTimeout = defaultRebalanceTimeout
	}
	if cfg.InitialRebalanceDelay < 0 {
		cfg.InitialRebalanceDelay = 0
	}
	if clock == nil {
		clock = SystemClock{}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Coordinator{
		ctx:    ctx,
		cancel: cancel,
		cfg:    cfg,
		store:  store,
		clock:  clock,
		logger: logger.With(slog.Int("broker_id", int(cfg.BrokerID))),
		groups: make(map[string]*group),
	}
	_, c.unsubscribe = store.Subscribe(c.onChange)
	return c
}

// begin rejects calls after shutdown and on a broker that is not the coordinator (R11), and
// returns the snapshot it checked. It must not be called with mu held.
func (c *Coordinator) begin() (metadata.Snapshot, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return metadata.Snapshot{}, ErrCoordinatorClosed
	}
	snap := c.store.Snapshot()
	c.mu.Lock()
	defer c.mu.Unlock()
	if snap.ControllerID != c.cfg.BrokerID {
		// Not (or no longer) the coordinator: drop all membership; members rejoin elsewhere
		// (SPEC §5).
		c.resetLocked()
		c.epoch = 0
		return metadata.Snapshot{}, fmt.Errorf("controller is broker %d: %w", snap.ControllerID, ErrNotCoordinator)
	}
	if snap.ControllerEpoch != c.epoch {
		// Leadership was regained in a new term: membership from an earlier term is stale.
		c.resetLocked()
		c.epoch = snap.ControllerEpoch
	}
	snap.ControllerEpoch = c.epoch
	return snap, nil
}

// resetLocked drops every in-memory group, failing parked joins with ErrNotCoordinator. Timer
// callbacks of dropped groups see a different (or missing) group and do nothing. Caller holds mu.
func (c *Coordinator) resetLocked() {
	for _, g := range c.groups {
		c.stopRebalanceTimer(g)
		for _, m := range g.members {
			c.stopSessionTimer(m)
			if m.waiter != nil {
				c.wake(m.waiter, JoinResult{}, ErrNotCoordinator)
				m.waiter = nil
			}
		}
	}
	c.groups = map[string]*group{}
}

// Join joins (or rejoins) a group and blocks until the rebalance completes, ctx ends, or the
// coordinator shuts down.
func (c *Coordinator) Join(ctx context.Context, req JoinRequest) (JoinResult, error) {
	if req.GroupID == "" {
		return JoinResult{}, fmt.Errorf("join: empty group id: %w", ErrInvalidGroup)
	}
	assignors := req.Assignors
	if len(assignors) == 0 {
		assignors = []string{RangeAssignorName}
	}
	assignors = dedupe(assignors)
	for _, a := range assignors {
		if _, ok := assignorByName(a); !ok {
			return JoinResult{}, fmt.Errorf("join group %q: unknown assignor %q: %w", req.GroupID, a, ErrInvalidGroup)
		}
	}
	topics := slices.Clone(req.Topics)
	slices.Sort(topics)
	topics = slices.Compact(topics)
	session := req.SessionTimeout
	if session <= 0 || session > c.cfg.MaxSessionTimeout {
		session = c.cfg.MaxSessionTimeout
	}
	rebalance := req.RebalanceTimeout
	if rebalance <= 0 {
		rebalance = c.cfg.DefaultRebalanceTimeout
	}
	if _, err := c.begin(); err != nil {
		return JoinResult{}, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return JoinResult{}, ErrCoordinatorClosed
	}
	g := c.groups[req.GroupID]
	var m *member
	if req.MemberID != "" {
		if g != nil {
			m = g.members[req.MemberID]
		}
		if m == nil {
			c.mu.Unlock()
			return JoinResult{}, fmt.Errorf("join group %q member %q: %w", req.GroupID, req.MemberID, ErrUnknownMemberID)
		}
	}
	if g != nil && !commonAssignor(g, m, assignors) {
		c.mu.Unlock()
		return JoinResult{}, fmt.Errorf("join group %q: no assignor in common with the group's members: %w",
			req.GroupID, ErrInvalidGroup)
	}

	// Rejoin shortcut (R16): a Stable member with an unchanged subscription gets its current
	// assignment back without a rebalance.
	if m != nil && g.state == StateStable && slices.Equal(m.topics, topics) && slices.Equal(m.assignors, assignors) {
		m.session, m.rebalance = session, rebalance
		c.scheduleSession(g, m)
		res := JoinResult{GenerationID: g.generation, MemberID: m.id, Assignor: g.assignor, Assignment: m.assignment.clone()}
		c.mu.Unlock()
		return res, nil
	}

	if g == nil {
		g = &group{id: req.GroupID, state: StateEmpty, members: make(map[string]*member)}
		c.groups[req.GroupID] = g
	}
	created := m == nil
	if created {
		c.nextJoin++
		m = &member{id: c.newMemberID(), joinSeq: c.nextJoin}
		g.members[m.id] = m
	}
	m.topics, m.assignors, m.session, m.rebalance = topics, assignors, session, rebalance
	if m.waiter != nil {
		// A newer Join from the same member supersedes the parked one.
		c.wake(m.waiter, JoinResult{}, ErrRebalanceInProgress)
	}
	w := &joinWaiter{done: make(chan struct{}), created: created}
	m.waiter = w
	c.stopSessionTimer(m) // parked members are kept alive (R2)

	switch g.state {
	case StateEmpty:
		c.prepareRebalance(g, true)
	case StateStable:
		c.prepareRebalance(g, false)
	case StatePreparingRebalance:
		if created && g.initialPhase {
			c.restartInitialDelay(g)
		}
	}
	c.logger.LogAttrs(ctx, slog.LevelDebug, "member joined",
		slog.String("group", g.id), slog.String("member", m.id), slog.Bool("new", created))
	c.tryComplete(g)
	c.mu.Unlock()

	select {
	case <-w.done:
		return w.result, w.err
	case <-ctx.Done():
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if w.finished {
		return w.result, w.err
	}
	c.abandon(g, m, w)
	return JoinResult{}, fmt.Errorf("join group %q: %w", req.GroupID, ctx.Err())
}

// abandon handles a parked Join whose ctx ended (R3, M5). Caller holds mu.
func (c *Coordinator) abandon(g *group, m *member, w *joinWaiter) {
	if c.groups[g.id] != g || g.members[m.id] != m || m.waiter != w {
		return
	}
	m.waiter = nil
	w.finished = true
	if w.created {
		c.removeMember(g, m)
		c.memberGone(g)
		return
	}
	c.scheduleSession(g, m)
	c.tryComplete(g)
}

// Sync returns the member's assignment for generation gen.
func (c *Coordinator) Sync(groupID, memberID string, gen int32) (Assignment, error) {
	if _, err := c.begin(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g, m, err := c.lookup(groupID, memberID, gen)
	if err != nil {
		return nil, err
	}
	if m.waiter == nil {
		c.scheduleSession(g, m)
	}
	if g.state == StatePreparingRebalance {
		return nil, fmt.Errorf("sync group %q: %w", groupID, ErrRebalanceInProgress)
	}
	return m.assignment.clone(), nil
}

// Heartbeat refreshes a member's session.
func (c *Coordinator) Heartbeat(groupID, memberID string, gen int32) error {
	if _, err := c.begin(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g, m, err := c.lookup(groupID, memberID, gen)
	if err != nil {
		return err
	}
	if m.waiter == nil {
		c.scheduleSession(g, m)
	}
	if g.state == StatePreparingRebalance {
		return fmt.Errorf("heartbeat group %q: %w", groupID, ErrRebalanceInProgress)
	}
	return nil
}

// lookup finds a member and checks its generation (Kafka order: unknown member, then
// generation). Caller holds mu.
func (c *Coordinator) lookup(groupID, memberID string, gen int32) (*group, *member, error) {
	if c.closed {
		return nil, nil, ErrCoordinatorClosed
	}
	g := c.groups[groupID]
	if g == nil || g.members[memberID] == nil {
		return nil, nil, fmt.Errorf("group %q member %q: %w", groupID, memberID, ErrUnknownMemberID)
	}
	if gen != g.generation {
		return nil, nil, fmt.Errorf("group %q generation %d (current %d): %w", groupID, gen, g.generation, ErrIllegalGeneration)
	}
	return g, g.members[memberID], nil
}

// Leave removes a member.
func (c *Coordinator) Leave(groupID, memberID string) error {
	if _, err := c.begin(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCoordinatorClosed
	}
	g := c.groups[groupID]
	if g == nil || g.members[memberID] == nil {
		return fmt.Errorf("leave group %q member %q: %w", groupID, memberID, ErrUnknownMemberID)
	}
	c.removeMember(g, g.members[memberID])
	c.memberGone(g)
	return nil
}

// Commit commits offsets for a member's generation (or a standalone commit with gen -1 and an
// empty member id). The map holds per-partition errors (partitions of unknown topics); the error
// return is for request-level failures.
func (c *Coordinator) Commit(ctx context.Context, groupID, memberID string, gen int32, offsets map[TopicPartition]int64) (map[TopicPartition]error, error) {
	if groupID == "" {
		return nil, fmt.Errorf("commit: empty group id: %w", ErrInvalidGroup)
	}
	begun, err := c.begin()
	if err != nil {
		return nil, err
	}
	epoch := begun.ControllerEpoch
	if err := c.checkCommit(groupID, memberID, gen); err != nil {
		return nil, err
	}
	results := make(map[TopicPartition]error)
	if len(offsets) == 0 {
		return results, nil
	}
	tps := slices.Collect(maps.Keys(offsets))
	slices.SortFunc(tps, func(a, b TopicPartition) int {
		return cmp.Or(strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	// Filter + propose, retried once if a topic disappears in between (M4).
	for attempt := 0; ; attempt++ {
		clear(results)
		snap := c.store.Snapshot()
		commits := make([]*streamqv1.OffsetCommit, 0, len(tps))
		for _, tp := range tps {
			t, ok := snap.Topic(tp.Topic)
			if !ok || tp.Partition < 0 || int(tp.Partition) >= len(t.Partitions) {
				results[tp] = fmt.Errorf("commit %s/%d: %w", tp.Topic, tp.Partition, metadata.ErrUnknownTopic)
				continue
			}
			commits = append(commits, &streamqv1.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: offsets[tp]})
		}
		if len(commits) == 0 {
			return results, nil
		}
		cmd := &streamqv1.MetadataCommand{
			Version:         metadata.CommandVersion,
			TimestampMs:     c.clock.Now().UnixMilli(),
			ControllerEpoch: epoch,
			Cmd: &streamqv1.MetadataCommand_CommitOffsets{CommitOffsets: &streamqv1.CommitOffsetsCommand{
				GroupId: groupID,
				Offsets: commits,
			}},
		}
		err := c.store.ProposeAsLeader(ctx, cmd)
		if err == nil {
			return results, nil
		}
		if errors.Is(err, metadata.ErrNotController) {
			// Leadership moved between validation and apply: the commit was not applied.
			return nil, fmt.Errorf("commit offsets for group %q: %w (%w)", groupID, ErrNotCoordinator, err)
		}
		if attempt == 0 && errors.Is(err, metadata.ErrUnknownTopic) {
			continue
		}
		return nil, fmt.Errorf("commit offsets for group %q: %w", groupID, err)
	}
}

// checkCommit validates a commit's member and generation (R1).
func (c *Coordinator) checkCommit(groupID, memberID string, gen int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCoordinatorClosed
	}
	g := c.groups[groupID]
	if memberID == "" && gen == -1 {
		if g != nil && len(g.members) > 0 {
			return fmt.Errorf("standalone commit to group %q with members: %w", groupID, ErrUnknownMemberID)
		}
		return nil
	}
	_, _, err := c.lookup(groupID, memberID, gen)
	return err
}

// FetchOffsets returns committed offsets for tps (all committed partitions if tps is empty);
// partitions without a commit map to -1.
func (c *Coordinator) FetchOffsets(groupID string, tps []TopicPartition) (map[TopicPartition]int64, error) {
	if groupID == "" {
		return nil, fmt.Errorf("fetch offsets: empty group id: %w", ErrInvalidGroup)
	}
	snap, err := c.begin()
	if err != nil {
		return nil, err
	}
	if len(tps) == 0 {
		return committed(snap, groupID), nil
	}
	out := make(map[TopicPartition]int64, len(tps))
	for _, tp := range tps {
		off, ok := snap.Committed(groupID, tp.Topic, tp.Partition)
		if !ok {
			off = -1
		}
		out[tp] = off
	}
	return out, nil
}

// committed returns every committed offset of groupID in snap.
func committed(snap metadata.Snapshot, groupID string) map[TopicPartition]int64 {
	out := make(map[TopicPartition]int64)
	for topic, parts := range snap.Groups[groupID] {
		for p, off := range parts {
			out[TopicPartition{Topic: topic, Partition: p}] = off
		}
	}
	return out
}

// List returns every known group id (with members or with committed offsets), sorted.
func (c *Coordinator) List() ([]string, error) {
	snap, err := c.begin()
	if err != nil {
		return nil, err
	}
	ids := make(map[string]struct{})
	for id, offs := range snap.Groups {
		if len(offs) > 0 {
			ids[id] = struct{}{}
		}
	}
	c.mu.Lock()
	for id, g := range c.groups {
		if len(g.members) > 0 {
			ids[id] = struct{}{}
		}
	}
	c.mu.Unlock()
	out := slices.Collect(maps.Keys(ids))
	slices.Sort(out)
	return out, nil
}

// Describe describes a group.
func (c *Coordinator) Describe(groupID string) (GroupDescription, error) {
	if groupID == "" {
		return GroupDescription{}, fmt.Errorf("describe: empty group id: %w", ErrInvalidGroup)
	}
	snap, err := c.begin()
	if err != nil {
		return GroupDescription{}, err
	}
	d := GroupDescription{GroupID: groupID, State: StateEmpty, Committed: committed(snap, groupID)}
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groups[groupID]
	if g == nil {
		return d, nil
	}
	d.State, d.Generation, d.Assignor = g.state, g.generation, g.assignor
	for _, id := range sortedMemberIDs(g) {
		m := g.members[id]
		d.Members = append(d.Members, MemberDescription{
			MemberID:   m.id,
			Topics:     slices.Clone(m.topics),
			Assignment: m.assignment.clone(),
		})
	}
	return d, nil
}

// BeginShutdown fails parked joins and makes new requests fail with ErrCoordinatorClosed.
func (c *Coordinator) BeginShutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for _, g := range c.groups {
		for _, m := range g.members {
			if m.waiter != nil {
				c.wake(m.waiter, JoinResult{}, ErrCoordinatorClosed)
				m.waiter = nil
			}
		}
	}
}

// Close shuts down (as BeginShutdown), stops timers, waits for running timer callbacks and
// unsubscribes from the store. Idempotent.
func (c *Coordinator) Close() {
	c.closeOnce.Do(func() {
		c.BeginShutdown()
		c.mu.Lock()
		for _, g := range c.groups {
			c.stopRebalanceTimer(g)
			for _, m := range g.members {
				c.stopSessionTimer(m)
			}
		}
		c.mu.Unlock()
		// Neither wait holds mu: running timer callbacks and an in-flight store callback take it.
		c.timers.Wait()
		c.unsubscribe()
		c.cancel()
	})
}

// onChange is the store subscriber: topic creation or deletion for a subscribed topic triggers
// a rebalance of the subscribing groups (R6).
func (c *Coordinator) onChange(ctx context.Context, ch metadata.Change) {
	if ch.Kind != metadata.TopicCreated && ch.Kind != metadata.TopicDeleted {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for _, g := range c.groups {
		if g.state != StateStable || !subscribes(g, ch.Topic) {
			continue
		}
		c.logger.LogAttrs(ctx, slog.LevelInfo, "subscribed topic changed, rebalancing",
			slog.String("group", g.id), slog.String("topic", ch.Topic))
		c.prepareRebalance(g, false)
		c.tryComplete(g)
	}
}

func subscribes(g *group, topic string) bool {
	for _, m := range g.members {
		if _, ok := slices.BinarySearch(m.topics, topic); ok {
			return true
		}
	}
	return false
}

// --- state machine helpers; all are called with mu held ---

// prepareRebalance moves g to PreparingRebalance. initial selects the initial delay of a
// formerly empty group; either way the phase is capped at the members' max rebalance timeout.
func (c *Coordinator) prepareRebalance(g *group, initial bool) {
	now := c.clock.Now()
	g.state = StatePreparingRebalance
	g.rebDeadline = now.Add(maxRebalanceTimeout(g))
	g.initialPhase = initial && c.cfg.InitialRebalanceDelay > 0
	if g.initialPhase {
		c.restartInitialDelay(g)
		return
	}
	c.scheduleRebalanceTimer(g, g.rebDeadline.Sub(now))
}

// restartInitialDelay restarts the initial delay, capped at the phase deadline (M6).
func (c *Coordinator) restartInitialDelay(g *group) {
	c.scheduleRebalanceTimer(g, min(c.cfg.InitialRebalanceDelay, g.rebDeadline.Sub(c.clock.Now())))
}

func maxRebalanceTimeout(g *group) time.Duration {
	var d time.Duration
	for _, m := range g.members {
		d = max(d, m.rebalance)
	}
	return d
}

// onRebalanceTimer handles the end of the initial delay or the rebalance timeout.
func (c *Coordinator) onRebalanceTimer(g *group, tok uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.groups[g.id] != g || g.rebTok != tok || g.state != StatePreparingRebalance {
		return
	}
	g.stopReb = nil
	now := c.clock.Now()
	if g.initialPhase {
		g.initialPhase = false
		if now.Before(g.rebDeadline) {
			c.scheduleRebalanceTimer(g, g.rebDeadline.Sub(now))
			c.tryComplete(g)
			return
		}
	}
	c.complete(g)
}

// tryComplete completes the rebalance if every member is parked (and the initial delay is over).
func (c *Coordinator) tryComplete(g *group) {
	if g.state != StatePreparingRebalance {
		return
	}
	if len(g.members) > 0 {
		if g.initialPhase {
			return
		}
		for _, m := range g.members {
			if m.waiter == nil {
				return
			}
		}
	}
	c.complete(g)
}

// complete ends the rebalance: members not parked are removed, the generation is bumped and
// every parked Join is answered with its assignment.
func (c *Coordinator) complete(g *group) {
	c.stopRebalanceTimer(g)
	g.initialPhase = false
	for _, id := range sortedMemberIDs(g) {
		if m := g.members[id]; m.waiter == nil {
			c.logger.LogAttrs(c.ctx, slog.LevelInfo, "member did not rejoin within the rebalance timeout",
				slog.String("group", g.id), slog.String("member", id))
			c.removeMember(g, m)
		}
	}
	g.generation++
	if len(g.members) == 0 {
		g.state, g.assignor = StateEmpty, ""
		delete(c.groups, g.id)
		return
	}
	ids := sortedMemberIDs(g)
	g.assignor = chooseAssignor(g)
	a, _ := assignorByName(g.assignor)
	snap := c.store.Snapshot()
	subs := make([]MemberSubscription, 0, len(ids))
	counts := make(map[string]int32)
	for _, id := range ids {
		m := g.members[id]
		subs = append(subs, MemberSubscription{MemberID: id, Topics: m.topics})
		for _, t := range m.topics {
			if topic, ok := snap.Topic(t); ok {
				counts[t] = int32(len(topic.Partitions))
			}
		}
	}
	assignment := a.Assign(subs, counts)
	for _, id := range ids {
		m := g.members[id]
		m.assignment = assignment[id]
		if m.assignment == nil {
			m.assignment = Assignment{}
		}
		c.scheduleSession(g, m) // R2: every deadline restarts at completion
		w := m.waiter
		m.waiter = nil
		c.wake(w, JoinResult{GenerationID: g.generation, MemberID: id, Assignor: g.assignor, Assignment: m.assignment.clone()}, nil)
	}
	g.state = StateStable
	c.logger.LogAttrs(c.ctx, slog.LevelInfo, "rebalance completed",
		slog.String("group", g.id), slog.Int("generation", int(g.generation)),
		slog.Int("members", len(ids)), slog.String("assignor", g.assignor))
}

// chooseAssignor returns the first assignor of the earliest-joined member that every member
// supports (R16), or range if there is none.
func chooseAssignor(g *group) string {
	var first *member
	for _, m := range g.members {
		if first == nil || m.joinSeq < first.joinSeq {
			first = m
		}
	}
	for _, a := range first.assignors {
		all := true
		for _, m := range g.members {
			if !slices.Contains(m.assignors, a) {
				all = false
				break
			}
		}
		if all {
			return a
		}
	}
	return RangeAssignorName
}

// commonAssignor reports whether assignors shares an entry with every member other than self.
func commonAssignor(g *group, self *member, assignors []string) bool {
	for _, a := range assignors {
		all := true
		for _, m := range g.members {
			if m != self && !slices.Contains(m.assignors, a) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// removeMember deletes m; a parked Join of m gets ErrUnknownMemberID.
func (c *Coordinator) removeMember(g *group, m *member) {
	c.stopSessionTimer(m)
	delete(g.members, m.id)
	if m.waiter != nil {
		c.wake(m.waiter, JoinResult{}, fmt.Errorf("member %q removed: %w", m.id, ErrUnknownMemberID))
		m.waiter = nil
	}
}

// memberGone reacts to a removed member: a Stable group rebalances, a rebalancing one re-checks
// completion (R7).
func (c *Coordinator) memberGone(g *group) {
	if g.state == StateStable {
		c.prepareRebalance(g, false)
	}
	c.tryComplete(g)
}

// wake answers a parked Join.
func (c *Coordinator) wake(w *joinWaiter, res JoinResult, err error) {
	if w.finished {
		return
	}
	w.finished = true
	w.result, w.err = res, err
	close(w.done)
}

// onSessionTimeout removes a member whose session expired, unless it is parked (R2).
func (c *Coordinator) onSessionTimeout(g *group, m *member, tok uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.groups[g.id] != g || g.members[m.id] != m || m.sessTok != tok || m.waiter != nil {
		return
	}
	m.stopSession = nil
	c.logger.LogAttrs(c.ctx, slog.LevelInfo, "member session expired",
		slog.String("group", g.id), slog.String("member", m.id))
	c.removeMember(g, m)
	c.memberGone(g)
}

// scheduleSession (re)starts m's session timer at now + its session timeout.
func (c *Coordinator) scheduleSession(g *group, m *member) {
	c.stopSessionTimer(m)
	m.sessTok++
	tok := m.sessTok
	m.stopSession = c.afterFunc(m.session, func() { c.onSessionTimeout(g, m, tok) })
}

func (c *Coordinator) stopSessionTimer(m *member) {
	if m.stopSession != nil {
		m.stopSession()
		m.stopSession = nil
	}
	m.sessTok++
}

func (c *Coordinator) scheduleRebalanceTimer(g *group, d time.Duration) {
	c.stopRebalanceTimer(g)
	g.rebTok++
	tok := g.rebTok
	g.stopReb = c.afterFunc(max(d, 0), func() { c.onRebalanceTimer(g, tok) })
}

func (c *Coordinator) stopRebalanceTimer(g *group) {
	if g.stopReb != nil {
		g.stopReb()
		g.stopReb = nil
	}
	g.rebTok++
}

// afterFunc schedules f, tracked by c.timers, and returns its stop func. After shutdown it
// schedules nothing. Caller holds mu.
func (c *Coordinator) afterFunc(d time.Duration, f func()) func() {
	if c.closed {
		return func() {}
	}
	c.timers.Add(1)
	stop := c.clock.AfterFunc(d, func() {
		defer c.timers.Done()
		f()
	})
	return func() {
		if stop() {
			c.timers.Done()
		}
	}
}

// newMemberID returns a unique member id. Caller holds mu.
func (c *Coordinator) newMemberID() string {
	c.nextMember++
	var b [8]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	return "member-" + strconv.FormatUint(c.nextMember, 10) + "-" + hex.EncodeToString(b[:])
}

func sortedMemberIDs(g *group) []string {
	ids := slices.Collect(maps.Keys(g.members))
	slices.Sort(ids)
	return ids
}

// dedupe drops repeated entries, keeping the first occurrence's position.
func dedupe(xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	return out
}
