package client

import (
	"context"
	"errors"
	"time"
)

var errGroupUnimplemented = errors.New("client: group consumer unimplemented")

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

// GroupConsumer consumes as a member of a consumer group (SPEC §5): it joins through the
// coordinator, heartbeats in the background, consumes its assignment starting at committed
// offsets, and commits positions. Poll, Commit and Close must be called from one goroutine;
// Assignment, Generation and MemberID are safe for concurrent use.
type GroupConsumer struct {
	client *Client
	cfg    GroupConsumerConfig
}

// NewGroupConsumer returns a GroupConsumer using c. It does no I/O until Poll.
func NewGroupConsumer(c *Client, cfg GroupConsumerConfig) (*GroupConsumer, error) {
	return &GroupConsumer{client: c, cfg: cfg}, errGroupUnimplemented
}

// Poll joins or rejoins the group when needed, then returns the next records of the assignment.
func (g *GroupConsumer) Poll(ctx context.Context) ([]Record, error) {
	_ = ctx
	return nil, errGroupUnimplemented
}

// Commit synchronously commits the current positions of the assignment.
func (g *GroupConsumer) Commit(ctx context.Context) error {
	_ = ctx
	return errGroupUnimplemented
}

// Assignment returns the current assignment (sorted).
func (g *GroupConsumer) Assignment() []TopicPartition { return nil }

// Generation returns the current generation, or -1 when not a member.
func (g *GroupConsumer) Generation() int32 { return -1 }

// MemberID returns the coordinator-assigned member id, or "" when not a member.
func (g *GroupConsumer) MemberID() string { return "" }

// Close commits (unless auto-commit is disabled), leaves the group and stops heartbeats.
// Idempotent. It does not close the Client.
func (g *GroupConsumer) Close(ctx context.Context) error {
	_ = ctx
	return nil
}
