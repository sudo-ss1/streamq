package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// maxMessageSize matches the broker's gRPC limits (DECISIONS D19).
const maxMessageSize = 64 << 20

// Config configures a Client. Zero values take the documented defaults.
type Config struct {
	// Bootstrap lists "host:port" broker addresses used to discover the cluster. Required.
	Bootstrap []string
	// RequestTimeout bounds each RPC when the caller's context has no earlier deadline
	// (default 30s).
	RequestTimeout time.Duration
	// MetadataMaxAge is how long cached topic metadata is used before a refresh (default 5m).
	// Errors such as NOT_LEADER invalidate it immediately.
	MetadataMaxAge time.Duration
	// RetryBackoff and RetryBackoffMax bound the exponential, fully jittered retry delay
	// (defaults 100ms and 1s).
	RetryBackoff    time.Duration
	RetryBackoffMax time.Duration
	// DialOptions are appended to the client's own (tests inject in-memory dialers).
	DialOptions []grpc.DialOption
	// Logger receives client logs (default: discarded).
	Logger *slog.Logger
}

func (c *Config) setDefaults() error {
	if len(c.Bootstrap) == 0 {
		return errors.New("client: no bootstrap addresses")
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MetadataMaxAge <= 0 {
		c.MetadataMaxAge = 5 * time.Minute
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 100 * time.Millisecond
	}
	if c.RetryBackoffMax < c.RetryBackoff {
		c.RetryBackoffMax = max(time.Second, c.RetryBackoff)
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	return nil
}

// partitionMeta is the cached leadership of one partition.
type partitionMeta struct {
	leader   int32
	epoch    int32
	replicas []int32
	isr      []int32
}

// topicMeta is the cached metadata of one topic.
type topicMeta struct {
	partitions []partitionMeta // index = partition id
	configs    map[string]string
	fetched    time.Time
}

// refreshCall is an in-flight metadata refresh shared by concurrent callers.
type refreshCall struct {
	done    chan struct{}
	err     error
	waiters int // callers that joined after the first; guarded by Client.mu (tests observe it)
}

// Client holds connections and the metadata cache shared by Producer, Consumer and Admin. It is
// safe for concurrent use.
//
// Lock ordering: Client.mu is a leaf; RPCs never run under it.
type Client struct {
	cfg     Config
	logger  *slog.Logger
	backoff backoff
	now     func() time.Time

	// life is cancelled by Close; shared metadata refreshes run under it (not under any one
	// caller's ctx) in goroutines tracked by refreshes.
	life      context.Context
	cancel    context.CancelFunc
	refreshes sync.WaitGroup

	mu       sync.RWMutex
	closed   bool
	conns    map[string]*grpc.ClientConn
	brokers  map[int32]string
	topics   map[string]*topicMeta
	inflight map[string]*refreshCall
}

// New returns a Client. It does no I/O: connections are made lazily.
func New(cfg Config) (*Client, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &Client{
		life:     life,
		cancel:   cancel,
		cfg:      cfg,
		logger:   cfg.Logger,
		backoff:  newBackoff(cfg.RetryBackoff, cfg.RetryBackoffMax),
		now:      time.Now,
		conns:    map[string]*grpc.ClientConn{},
		brokers:  map[int32]string{},
		topics:   map[string]*topicMeta{},
		inflight: map[string]*refreshCall{},
	}, nil
}

// Close closes every connection. Producers and consumers using the Client must be closed first.
// Idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.cancel()
	c.refreshes.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for addr, conn := range c.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", addr, err))
		}
	}
	c.conns = nil
	return errors.Join(errs...)
}

// api returns a stub for addr, dialing (lazily) on first use.
func (c *Client) api(addr string) (streamqv1.StreamQServiceClient, error) {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if ok {
		return streamqv1.NewStreamQServiceClient(conn), nil
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
	}, c.cfg.DialOptions...)
	conn, err := grpc.NewClient("passthrough:///"+addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return nil, ErrClosed
	}
	if existing, ok := c.conns[addr]; ok {
		_ = conn.Close()
		conn = existing
	} else {
		c.conns[addr] = conn
	}
	return streamqv1.NewStreamQServiceClient(conn), nil
}

// withTimeout applies RequestTimeout unless ctx already has an earlier deadline.
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if d, ok := ctx.Deadline(); ok && time.Until(d) <= c.cfg.RequestTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.cfg.RequestTimeout)
}

// candidates returns broker addresses to try for cluster-wide calls: bootstrap first, then
// known brokers.
func (c *Client) candidates() []string {
	out := slices.Clone(c.cfg.Bootstrap)
	c.mu.RLock()
	for _, addr := range c.brokers {
		if !slices.Contains(out, addr) {
			out = append(out, addr)
		}
	}
	c.mu.RUnlock()
	return out
}

// callAny runs fn against each candidate broker until one does not fail with a transport error.
func (c *Client) callAny(ctx context.Context, fn func(context.Context, streamqv1.StreamQServiceClient) error) error {
	var last error
	for _, addr := range c.candidates() {
		api, err := c.api(addr)
		if err != nil {
			return err
		}
		rctx, cancel := c.withTimeout(ctx)
		err = fn(rctx, api)
		cancel()
		if err == nil || !isTransportError(err) || ctx.Err() != nil {
			return err
		}
		c.logger.Debug("broker unreachable; trying next", slog.String("addr", addr), slog.Any("error", err))
		last = err
	}
	return fmt.Errorf("client: no broker reachable: %w", last)
}

// refreshMetadata fetches metadata for topics (all topics if empty) and installs it. Concurrent
// refreshes for the same topic set share one RPC. The RPC runs in a client-owned goroutine under
// the client's lifetime context (bounded by RequestTimeout per broker), never under a caller's
// ctx, so one caller giving up cannot fail the others; each caller waits on its own ctx.
func (c *Client) refreshMetadata(ctx context.Context, topics []string) error {
	sorted := slices.Clone(topics)
	slices.Sort(sorted)
	key := strings.Join(sorted, "\x00")
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	call, ok := c.inflight[key]
	if ok {
		call.waiters++
	} else {
		call = &refreshCall{done: make(chan struct{})}
		c.inflight[key] = call
		c.refreshes.Add(1)
		go c.runRefresh(key, sorted, call)
	}
	c.mu.Unlock()
	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runRefresh performs one shared metadata RPC and publishes its result.
func (c *Client) runRefresh(key string, topics []string, call *refreshCall) {
	defer c.refreshes.Done()
	var resp *streamqv1.MetadataResponse
	err := c.callAny(c.life, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
		var err error
		resp, err = api.Metadata(ctx, &streamqv1.MetadataRequest{Topics: topics})
		return err
	})
	if err != nil && c.life.Err() != nil {
		err = ErrClosed
	}
	c.mu.Lock()
	delete(c.inflight, key)
	if err == nil && !c.closed {
		c.install(resp, topics)
	}
	call.err = err
	c.mu.Unlock()
	close(call.done)
}

// install applies a Metadata response. Caller holds mu.
func (c *Client) install(resp *streamqv1.MetadataResponse, requested []string) {
	now := c.now()
	for _, b := range resp.GetBrokers() {
		c.brokers[b.GetId()] = fmt.Sprintf("%s:%d", b.GetHost(), b.GetPort())
	}
	seen := map[string]bool{}
	for _, t := range resp.GetTopics() {
		seen[t.GetName()] = true
		if t.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
			delete(c.topics, t.GetName())
			continue
		}
		tm := &topicMeta{configs: t.GetConfigs(), fetched: now}
		for _, p := range t.GetPartitions() {
			id := int(p.GetPartition())
			for len(tm.partitions) <= id {
				tm.partitions = append(tm.partitions, partitionMeta{leader: -1, epoch: -1})
			}
			tm.partitions[id] = partitionMeta{
				leader: p.GetLeaderId(), epoch: p.GetLeaderEpoch(),
				replicas: p.GetReplicas(), isr: p.GetIsr(),
			}
		}
		c.topics[t.GetName()] = tm
	}
	for _, name := range requested {
		if !seen[name] {
			delete(c.topics, name)
		}
	}
}

// topic returns fresh-enough cached metadata for name, refreshing if needed. An unknown topic
// returns ErrUnknownTopicOrPartition.
func (c *Client) topic(ctx context.Context, name string) (*topicMeta, error) {
	c.mu.RLock()
	tm, ok := c.topics[name]
	c.mu.RUnlock()
	if ok && c.now().Sub(tm.fetched) < c.cfg.MetadataMaxAge {
		return tm, nil
	}
	if err := c.refreshMetadata(ctx, []string{name}); err != nil {
		return nil, err
	}
	c.mu.RLock()
	tm, ok = c.topics[name]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("topic %q: %w", name, ErrUnknownTopicOrPartition)
	}
	return tm, nil
}

// invalidate drops cached metadata for topic so the next use refreshes it.
func (c *Client) invalidate(topic string) {
	c.mu.Lock()
	delete(c.topics, topic)
	c.mu.Unlock()
}

// partitionCount returns the number of partitions of topic.
func (c *Client) partitionCount(ctx context.Context, topic string) (int32, error) {
	tm, err := c.topic(ctx, topic)
	if err != nil {
		return 0, err
	}
	return int32(len(tm.partitions)), nil
}

// leader returns a stub for tp's leader and the leader epoch to send with requests.
func (c *Client) leader(ctx context.Context, tp TopicPartition) (streamqv1.StreamQServiceClient, int32, error) {
	api, epoch, _, err := c.leaderInfo(ctx, tp)
	return api, epoch, err
}

// leaderInfo is leader plus the leader's broker id, all from one metadata snapshot.
func (c *Client) leaderInfo(ctx context.Context, tp TopicPartition) (streamqv1.StreamQServiceClient, int32, int32, error) {
	tm, err := c.topic(ctx, tp.Topic)
	if err != nil {
		return nil, 0, -1, err
	}
	if tp.Partition < 0 || int(tp.Partition) >= len(tm.partitions) {
		return nil, 0, -1, fmt.Errorf("%s/%d: %w", tp.Topic, tp.Partition, ErrUnknownTopicOrPartition)
	}
	pm := tm.partitions[tp.Partition]
	if pm.leader < 0 {
		return nil, 0, -1, fmt.Errorf("%s/%d has no leader: %w", tp.Topic, tp.Partition, ErrNotLeader)
	}
	c.mu.RLock()
	addr, ok := c.brokers[pm.leader]
	c.mu.RUnlock()
	if !ok {
		c.invalidate(tp.Topic)
		return nil, 0, -1, fmt.Errorf("%s/%d: leader %d address unknown: %w", tp.Topic, tp.Partition, pm.leader, ErrNotLeader)
	}
	api, err := c.api(addr)
	if err != nil {
		return nil, 0, -1, err
	}
	return api, pm.epoch, pm.leader, nil
}

// sleep waits for d or until ctx or abort is done; it reports whether the full delay elapsed.
func sleep(ctx context.Context, d time.Duration, abort <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	case <-abort:
		return false
	}
}
