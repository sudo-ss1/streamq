package client

// Lock ordering: GroupConsumer.mu is a leaf; no RPC, Client lock or Consumer call runs under it.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// GroupConsumer defaults (SPEC §5, Kafka).
const (
	groupDefaultSessionTimeout     = 10 * time.Second
	groupDefaultRebalanceTimeout   = 30 * time.Second
	groupDefaultHeartbeatInterval  = 3 * time.Second
	groupDefaultAutoCommitInterval = 5 * time.Second
	// groupJoinSlack is added to RebalanceTimeout for the JoinGroup RPC deadline (M4 plan R3).
	groupJoinSlack = 5 * time.Second
)

// errGroupNotMember reports a Commit while the consumer is not a member of its group.
var errGroupNotMember = errors.New("client: group consumer is not a group member")

// GroupConsumerConfig configures a GroupConsumer. Zero values take the defaults.
type GroupConsumerConfig struct {
	// GroupID and Topics are required.
	GroupID string
	Topics  []string
	// SessionTimeout (default 10s), RebalanceTimeout (default 30s) and HeartbeatInterval
	// (default 3s) follow SPEC §5.
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	HeartbeatInterval time.Duration
	// DisableAutoCommit turns off committing positions every AutoCommitInterval (default 5s),
	// before rejoining, and on Close.
	DisableAutoCommit  bool
	AutoCommitInterval time.Duration
	// Assignors in preference order (default ["range"]).
	Assignors []string
	// Consumer configures fetching; its AutoOffsetReset applies when the group has no committed
	// offset for a partition.
	Consumer ConsumerConfig
}

// setDefaults fills zero values and validates the configuration.
func (cfg *GroupConsumerConfig) setDefaults() error {
	if cfg.GroupID == "" {
		return errors.New("client: group consumer: empty GroupID")
	}
	if len(cfg.Topics) == 0 {
		return errors.New("client: group consumer: no Topics")
	}
	topics := make([]string, 0, len(cfg.Topics))
	for _, t := range cfg.Topics {
		if t == "" {
			return errors.New("client: group consumer: empty topic name")
		}
		if !slices.Contains(topics, t) {
			topics = append(topics, t)
		}
	}
	slices.Sort(topics)
	cfg.Topics = topics
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = groupDefaultSessionTimeout
	}
	if cfg.RebalanceTimeout <= 0 {
		cfg.RebalanceTimeout = groupDefaultRebalanceTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = groupDefaultHeartbeatInterval
	}
	if cfg.HeartbeatInterval >= cfg.SessionTimeout {
		return fmt.Errorf("client: group consumer: HeartbeatInterval %v must be below SessionTimeout %v",
			cfg.HeartbeatInterval, cfg.SessionTimeout)
	}
	if cfg.AutoCommitInterval <= 0 {
		cfg.AutoCommitInterval = groupDefaultAutoCommitInterval
	}
	if len(cfg.Assignors) == 0 {
		cfg.Assignors = []string{"range"}
	} else {
		cfg.Assignors = slices.Clone(cfg.Assignors)
	}
	return nil
}

// GroupConsumer consumes as a member of a consumer group (SPEC §5): it joins through the
// coordinator, heartbeats in the background, consumes its assignment starting at committed
// offsets, and commits positions. Poll, Commit and Close must be called from one goroutine;
// Assignment, Generation and MemberID are safe for concurrent use.
type GroupConsumer struct {
	client *Client
	cfg    GroupConsumerConfig
	clock  clock
	inner  *Consumer
	logger *slog.Logger

	// Owned by the Poll/Commit/Close goroutine.
	closed     bool
	lastCommit time.Time
	hbCancel   context.CancelFunc
	hbWG       sync.WaitGroup

	// mu guards the membership state, which the heartbeat goroutine and the concurrent-safe
	// accessors also touch.
	mu          sync.Mutex
	coord       streamqv1.StreamQServiceClient // nil until discovered
	rediscover  bool                           // coord is stale
	memberID    string
	generation  int32 // -1 when not a member
	assignment  []TopicPartition
	needsRejoin bool
}

// NewGroupConsumer returns a GroupConsumer using c. It does no I/O until Poll.
func NewGroupConsumer(c *Client, cfg GroupConsumerConfig) (*GroupConsumer, error) {
	return newGroupConsumer(c, cfg, realClock{})
}

// newGroupConsumer is NewGroupConsumer with an injectable clock for heartbeats, auto-commit
// timing and retry backoff.
func newGroupConsumer(c *Client, cfg GroupConsumerConfig, clk clock) (*GroupConsumer, error) {
	cfg.Topics = slices.Clone(cfg.Topics)
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	inner, err := NewConsumer(c, cfg.Consumer)
	if err != nil {
		return nil, err
	}
	return &GroupConsumer{
		client:     c,
		cfg:        cfg,
		clock:      clk,
		inner:      inner,
		logger:     c.logger.With(slog.String("group", cfg.GroupID)),
		generation: -1,
	}, nil
}

// Poll joins or rejoins the group when needed, then returns the next records of the assignment.
// ctx must leave enough time for a join to complete (the broker's initial rebalance delay plus the
// rebalance itself): a join abandoned by its ctx removes the member it created, so a too-short
// ctx can never join a forming group.
// Joining retries retriable failures (coordinator moved or unreachable, stale membership) with
// backoff until ctx ends. With auto-commit on, positions are committed every AutoCommitInterval
// before fetching, so only records already returned by earlier Polls are committed.
func (g *GroupConsumer) Poll(ctx context.Context) ([]Record, error) {
	if g.closed {
		return nil, ErrClosed
	}
	if g.mustJoin() {
		if err := g.join(ctx); err != nil {
			return nil, err
		}
	}
	if !g.cfg.DisableAutoCommit && !g.clock.Now().Before(g.lastCommit.Add(g.cfg.AutoCommitInterval)) {
		if err := g.commit(ctx, false); err != nil && ctx.Err() == nil {
			g.logger.Warn("group auto-commit failed", slog.Any("error", err))
		}
		g.lastCommit = g.clock.Now()
	}
	return g.inner.Poll(ctx)
}

// Commit synchronously commits the current positions of the assignment. It retries while the
// coordinator moves or is unreachable, bounded by ctx (or the client's RequestTimeout). Stale
// membership errors (ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS) make the next
// Poll rejoin and are returned; the commit is lost (at-least-once). Per-partition failures are
// returned joined, while the other partitions are committed.
func (g *GroupConsumer) Commit(ctx context.Context) error {
	if g.closed {
		return ErrClosed
	}
	return g.commit(ctx, true)
}

// Assignment returns the current assignment (sorted).
func (g *GroupConsumer) Assignment() []TopicPartition {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.assignment)
}

// Generation returns the current generation, or -1 when not a member.
func (g *GroupConsumer) Generation() int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

// MemberID returns the coordinator-assigned member id, or "" when not a member.
func (g *GroupConsumer) MemberID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.memberID
}

// Close commits (unless auto-commit is disabled), leaves the group and stops heartbeats.
// Idempotent. It does not close the Client.
func (g *GroupConsumer) Close(ctx context.Context) error {
	if g.closed {
		return nil
	}
	g.closed = true
	var errs []error
	if !g.cfg.DisableAutoCommit {
		if err := g.commit(ctx, true); err != nil && !errors.Is(err, errGroupNotMember) {
			errs = append(errs, fmt.Errorf("close: commit: %w", err))
		}
	}
	g.stopHeartbeat()
	if err := g.leave(ctx); err != nil {
		errs = append(errs, fmt.Errorf("close: leave group: %w", err))
	}
	if err := g.inner.Close(); err != nil {
		errs = append(errs, err)
	}
	g.mu.Lock()
	g.memberID, g.generation, g.assignment, g.needsRejoin = "", -1, nil, false
	g.mu.Unlock()
	return errors.Join(errs...)
}

// mustJoin reports whether Poll has to (re)join before fetching.
func (g *GroupConsumer) mustJoin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation < 0 || g.needsRejoin
}

// membership returns the member id and generation, and whether they identify a current member.
func (g *GroupConsumer) membership() (string, int32, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.memberID, g.generation, g.memberID != "" && g.generation >= 0
}

// flagRejoin makes the next Poll rejoin; clearMember also forgets the member id (the
// coordinator no longer knows it), and rediscover forgets the coordinator.
func (g *GroupConsumer) flagRejoin(clearMember, rediscover bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.needsRejoin = true
	if clearMember {
		g.memberID, g.generation = "", -1
	}
	if rediscover {
		g.rediscover = true
	}
}

// flagRediscover makes the next coordinator RPC look the coordinator up again.
func (g *GroupConsumer) flagRediscover() {
	g.mu.Lock()
	g.rediscover = true
	g.mu.Unlock()
}

// coordinator returns the group coordinator, discovering it if unknown or stale.
func (g *GroupConsumer) coordinator(ctx context.Context) (streamqv1.StreamQServiceClient, error) {
	g.mu.Lock()
	api, stale := g.coord, g.rediscover
	g.mu.Unlock()
	if api != nil && !stale {
		return api, nil
	}
	var resp *streamqv1.FindCoordinatorResponse
	err := g.client.callAny(ctx, func(rctx context.Context, api streamqv1.StreamQServiceClient) error {
		var err error
		resp, err = api.FindCoordinator(rctx, &streamqv1.FindCoordinatorRequest{GroupId: g.cfg.GroupID})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("find coordinator: %w", err)
	}
	if err := serverError(resp.GetErrorCode(), resp.GetErrorMessage()); err != nil {
		return nil, fmt.Errorf("find coordinator: %w", err)
	}
	b := resp.GetCoordinator()
	addr := fmt.Sprintf("%s:%d", b.GetHost(), b.GetPort())
	api, err = g.client.api(addr)
	if err != nil {
		return nil, err
	}
	g.logger.Debug("group coordinator found", slog.Int("coordinator_id", int(b.GetId())), slog.String("addr", addr))
	g.mu.Lock()
	g.coord, g.rediscover = api, false
	g.mu.Unlock()
	return api, nil
}

// groupRetry classifies a join/commit failure: whether it is retriable, and whether the
// coordinator must be rediscovered or the member id forgotten first.
func groupRetry(err error) (retriable, rediscover, clearMember bool) {
	switch {
	case isTransportError(err), errors.Is(err, ErrNotCoordinator):
		return true, true, false
	case errors.Is(err, ErrUnknownMemberID):
		return true, false, true
	case errors.Is(err, ErrRebalanceInProgress), errors.Is(err, ErrIllegalGeneration),
		errors.Is(err, ErrRequestTimedOut), errors.Is(err, ErrUnknownTopicOrPartition):
		return true, false, false
	default:
		return false, false, false
	}
}

// wait sleeps for the backoff delay of attempt on the injected clock, or until ctx ends.
func (g *GroupConsumer) wait(ctx context.Context, attempt int) error {
	ch, stop := g.clock.NewTimer(g.client.backoff.delay(attempt))
	defer stop()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// join (re)joins the group: it commits the old generation's positions (if auto-commit is on),
// stops its heartbeats, then runs join attempts with backoff until one succeeds, ctx ends or a
// fatal error occurs.
func (g *GroupConsumer) join(ctx context.Context) error {
	if !g.cfg.DisableAutoCommit {
		// Allowed while the group is PreparingRebalance (M4 plan R1). A failure loses the commit.
		if err := g.commit(ctx, false); err != nil && !errors.Is(err, errGroupNotMember) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			g.logger.Warn("group commit before rejoin failed", slog.Any("error", err))
		}
	}
	g.stopHeartbeat()
	for attempt := 0; ; attempt++ {
		err := g.joinOnce(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		retriable, rediscover, clearMember := groupRetry(err)
		if !retriable {
			return fmt.Errorf("client: join group %q: %w", g.cfg.GroupID, err)
		}
		g.logger.Debug("group join attempt failed; retrying", slog.Int("attempt", attempt), slog.Any("error", err))
		g.mu.Lock()
		if rediscover {
			g.rediscover = true
		}
		if clearMember {
			g.memberID, g.generation = "", -1
		}
		g.mu.Unlock()
		if err := g.wait(ctx, attempt); err != nil {
			return err
		}
	}
}

// joinOnce runs one JoinGroup → SyncGroup → FetchOffset round and, on success, installs the new
// assignment and generation and starts its heartbeats.
func (g *GroupConsumer) joinOnce(ctx context.Context) error {
	api, err := g.coordinator(ctx)
	if err != nil {
		return err
	}
	g.mu.Lock()
	memberID := g.memberID
	g.mu.Unlock()

	// JoinGroup parks until the rebalance completes, so its deadline covers the rebalance
	// timeout (M4 plan R3).
	jctx, cancel := context.WithTimeout(ctx, max(g.client.cfg.RequestTimeout, g.cfg.RebalanceTimeout+groupJoinSlack))
	jresp, err := api.JoinGroup(jctx, &streamqv1.JoinGroupRequest{
		GroupId:            g.cfg.GroupID,
		MemberId:           memberID,
		SessionTimeoutMs:   int32(g.cfg.SessionTimeout.Milliseconds()),
		RebalanceTimeoutMs: int32(g.cfg.RebalanceTimeout.Milliseconds()),
		Topics:             g.cfg.Topics,
		Assignors:          g.cfg.Assignors,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	if err := serverError(jresp.GetErrorCode(), jresp.GetErrorMessage()); err != nil {
		return fmt.Errorf("join group: %w", err)
	}
	memberID, gen := jresp.GetMemberId(), jresp.GetGenerationId()
	g.mu.Lock()
	g.memberID = memberID // known to the coordinator from now on, even if Sync fails
	g.mu.Unlock()

	rctx, cancel := g.client.withTimeout(ctx)
	sresp, err := api.SyncGroup(rctx, &streamqv1.SyncGroupRequest{
		GroupId: g.cfg.GroupID, MemberId: memberID, GenerationId: gen,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("sync group: %w", err)
	}
	if err := serverError(sresp.GetErrorCode(), sresp.GetErrorMessage()); err != nil {
		return fmt.Errorf("sync group: %w", err)
	}
	assignment := groupFlatten(sresp.GetAssignment())

	committed, err := g.fetchCommitted(ctx, api, assignment)
	if err != nil {
		return err
	}
	// Eager rebalancing: every partition starts again from its committed offset (or the reset
	// policy), since another member may have consumed it in between.
	g.inner.Assign(nil)
	g.inner.Assign(assignment)
	for _, tp := range assignment {
		if off, ok := committed[tp]; ok && off >= 0 {
			if err := g.inner.Seek(tp, off); err != nil {
				return err
			}
		}
	}

	g.mu.Lock()
	g.memberID, g.generation, g.assignment, g.needsRejoin = memberID, gen, assignment, false
	g.mu.Unlock()
	g.lastCommit = g.clock.Now()
	g.logger.Info("joined group", slog.String("member_id", memberID), slog.Int("generation", int(gen)),
		slog.String("assignor", jresp.GetAssignor()), slog.Int("partitions", len(assignment)))
	g.startHeartbeat(ctx, api, memberID, gen)
	return nil
}

// groupFlatten converts a wire assignment to sorted, de-duplicated TopicPartitions.
func groupFlatten(tps []*streamqv1.TopicPartitions) []TopicPartition {
	var out []TopicPartition
	for _, t := range tps {
		for _, p := range t.GetPartitions() {
			out = append(out, TopicPartition{Topic: t.GetTopic(), Partition: p})
		}
	}
	slices.SortFunc(out, func(a, b TopicPartition) int {
		return cmp.Or(cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	return slices.Compact(out)
}

// groupWire converts sorted TopicPartitions to the wire form, one entry per topic.
func groupWire(tps []TopicPartition) []*streamqv1.TopicPartitions {
	var out []*streamqv1.TopicPartitions
	consumerGroupTopics(tps, func(topic string, idx []int) {
		e := &streamqv1.TopicPartitions{Topic: topic}
		for _, i := range idx {
			e.Partitions = append(e.Partitions, tps[i].Partition)
		}
		out = append(out, e)
	})
	return out
}

// fetchCommitted returns the group's committed offsets for tps (-1 or absent when none).
func (g *GroupConsumer) fetchCommitted(ctx context.Context, api streamqv1.StreamQServiceClient, tps []TopicPartition) (map[TopicPartition]int64, error) {
	out := map[TopicPartition]int64{}
	if len(tps) == 0 {
		return out, nil
	}
	rctx, cancel := g.client.withTimeout(ctx)
	resp, err := api.FetchOffset(rctx, &streamqv1.FetchOffsetRequest{GroupId: g.cfg.GroupID, Partitions: groupWire(tps)})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	if err := serverError(resp.GetErrorCode(), resp.GetErrorMessage()); err != nil {
		return nil, fmt.Errorf("fetch offsets: %w", err)
	}
	for _, o := range resp.GetOffsets() {
		out[TopicPartition{Topic: o.GetTopic(), Partition: o.GetPartition()}] = o.GetOffset()
	}
	return out, nil
}

// startHeartbeat starts the heartbeat goroutine of one generation.
func (g *GroupConsumer) startHeartbeat(ctx context.Context, api streamqv1.StreamQServiceClient, memberID string, gen int32) {
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	g.hbCancel = cancel
	g.hbWG.Add(1)
	go g.heartbeatLoop(hctx, api, memberID, gen)
}

// stopHeartbeat stops the current generation's heartbeat goroutine and waits for it.
func (g *GroupConsumer) stopHeartbeat() {
	if g.hbCancel == nil {
		return
	}
	g.hbCancel()
	g.hbWG.Wait()
	g.hbCancel = nil
}

// heartbeatLoop sends a heartbeat every HeartbeatInterval until ctx ends or the coordinator
// reports that the member must rejoin (or cannot be reached), which it flags for the next Poll.
func (g *GroupConsumer) heartbeatLoop(ctx context.Context, api streamqv1.StreamQServiceClient, memberID string, gen int32) {
	defer g.hbWG.Done()
	req := &streamqv1.HeartbeatRequest{GroupId: g.cfg.GroupID, MemberId: memberID, GenerationId: gen}
	for {
		ch, stop := g.clock.NewTimer(g.cfg.HeartbeatInterval)
		select {
		case <-ctx.Done():
			stop()
			return
		case <-ch:
		}
		rctx, cancel := g.client.withTimeout(ctx)
		resp, err := api.Heartbeat(rctx, req)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = serverError(resp.GetErrorCode(), resp.GetErrorMessage())
		}
		if err == nil {
			continue
		}
		log := g.logger.With(slog.String("member_id", memberID), slog.Int("generation", int(gen)), slog.Any("error", err))
		switch {
		case errors.Is(err, ErrRebalanceInProgress), errors.Is(err, ErrIllegalGeneration):
			log.Info("group rebalance; rejoining")
			g.flagRejoin(false, false)
		case errors.Is(err, ErrUnknownMemberID):
			log.Info("group member unknown to coordinator; rejoining")
			g.flagRejoin(true, false)
		case errors.Is(err, ErrNotCoordinator), isTransportError(err):
			log.Info("group coordinator lost; rediscovering and rejoining")
			g.flagRejoin(false, true)
		default:
			log.Warn("group heartbeat failed")
			continue
		}
		return
	}
}

// commit commits the current positions of the assignment. With retry, coordinator moves and
// transport failures are retried with backoff (bounded by ctx or RequestTimeout); otherwise they
// only flag rediscovery. Stale-membership errors flag a rejoin.
func (g *GroupConsumer) commit(ctx context.Context, retry bool) error {
	memberID, gen, ok := g.membership()
	if !ok {
		return errGroupNotMember
	}
	g.mu.Lock()
	assignment := g.assignment
	g.mu.Unlock()
	req := &streamqv1.CommitOffsetRequest{GroupId: g.cfg.GroupID, MemberId: memberID, GenerationId: gen}
	for _, tp := range assignment {
		if pos, ok := g.inner.Position(tp); ok {
			req.Offsets = append(req.Offsets, &streamqv1.OffsetCommit{Topic: tp.Topic, Partition: tp.Partition, Offset: pos})
		}
	}
	if len(req.Offsets) == 0 {
		return nil
	}
	ctx, cancel := g.client.withTimeout(ctx)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err := g.commitOnce(ctx, req)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("client: commit: %w", errors.Join(ctx.Err(), err))
		}
		switch {
		case errors.Is(err, ErrUnknownMemberID):
			g.flagRejoin(true, false)
		case errors.Is(err, ErrIllegalGeneration), errors.Is(err, ErrRebalanceInProgress):
			g.flagRejoin(false, false)
		case isTransportError(err), errors.Is(err, ErrNotCoordinator):
			g.flagRediscover()
			if retry {
				if werr := g.wait(ctx, attempt); werr == nil {
					continue
				}
			}
		}
		return fmt.Errorf("client: commit: %w", err)
	}
}

// commitOnce sends one CommitOffset request. A request-level error is returned as is;
// per-partition errors are returned joined.
func (g *GroupConsumer) commitOnce(ctx context.Context, req *streamqv1.CommitOffsetRequest) error {
	api, err := g.coordinator(ctx)
	if err != nil {
		return err
	}
	rctx, cancel := g.client.withTimeout(ctx)
	resp, err := api.CommitOffset(rctx, req)
	cancel()
	if err != nil {
		return err
	}
	if err := serverError(resp.GetErrorCode(), resp.GetErrorMessage()); err != nil {
		return err
	}
	var errs []error
	for _, r := range resp.GetResults() {
		if err := serverError(r.GetErrorCode(), ""); err != nil {
			errs = append(errs, fmt.Errorf("%s/%d: %w", r.GetTopic(), r.GetPartition(), err))
		}
	}
	return errors.Join(errs...)
}

// leave sends LeaveGroup if the consumer has a member id (rediscovering the coordinator if
// needed). An unknown member has already left.
func (g *GroupConsumer) leave(ctx context.Context) error {
	g.mu.Lock()
	memberID := g.memberID
	g.mu.Unlock()
	if memberID == "" {
		return nil
	}
	api, err := g.coordinator(ctx)
	if err != nil {
		return err
	}
	rctx, cancel := g.client.withTimeout(ctx)
	resp, err := api.LeaveGroup(rctx, &streamqv1.LeaveGroupRequest{GroupId: g.cfg.GroupID, MemberId: memberID})
	cancel()
	if err != nil {
		return err
	}
	if err := serverError(resp.GetErrorCode(), resp.GetErrorMessage()); err != nil && !errors.Is(err, ErrUnknownMemberID) {
		return err
	}
	return nil
}
