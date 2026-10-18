// Package group is the consumer group coordinator (SPEC §5): membership, generations, rebalances
// with server-side assignment (range and round-robin assignors), heartbeats, session expiry, and
// offset commits through the metadata store.
//
// See docs/plans/M4.md (Revision 1) for the protocol rules this package implements.
package group

import (
	"context"
	"errors"
	"log/slog"
	"time"

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

var errUnimplemented = errors.New("group: unimplemented")

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
type GroupDescription struct {
	GroupID    string
	State      string
	Generation int32
	Assignor   string
	Members    []MemberDescription
	Committed  map[TopicPartition]int64
}

// Coordinator coordinates every group on this broker.
type Coordinator struct {
	cfg    Config
	store  metadata.Store
	clock  Clock
	logger *slog.Logger
}

// New returns a Coordinator and subscribes it to store (topic changes trigger rebalances).
func New(cfg Config, store metadata.Store, clock Clock, logger *slog.Logger) *Coordinator {
	return &Coordinator{cfg: cfg, store: store, clock: clock, logger: logger}
}

// Join joins (or rejoins) a group and blocks until the rebalance completes, ctx ends, or the
// coordinator shuts down.
func (c *Coordinator) Join(ctx context.Context, req JoinRequest) (JoinResult, error) {
	_, _ = ctx, req
	return JoinResult{}, errUnimplemented
}

// Sync returns the member's assignment for generation gen.
func (c *Coordinator) Sync(groupID, memberID string, gen int32) (Assignment, error) {
	_, _, _ = groupID, memberID, gen
	return nil, errUnimplemented
}

// Heartbeat refreshes a member's session.
func (c *Coordinator) Heartbeat(groupID, memberID string, gen int32) error {
	_, _, _ = groupID, memberID, gen
	return errUnimplemented
}

// Leave removes a member.
func (c *Coordinator) Leave(groupID, memberID string) error {
	_, _ = groupID, memberID
	return errUnimplemented
}

// Commit commits offsets for a member's generation (or a standalone commit with gen -1 and an
// empty member id). The map holds per-partition errors (partitions of unknown topics); the error
// return is for request-level failures.
func (c *Coordinator) Commit(ctx context.Context, groupID, memberID string, gen int32, offsets map[TopicPartition]int64) (map[TopicPartition]error, error) {
	_, _, _, _, _ = ctx, groupID, memberID, gen, offsets
	return nil, errUnimplemented
}

// FetchOffsets returns committed offsets for tps (all committed partitions if tps is empty);
// partitions without a commit map to -1.
func (c *Coordinator) FetchOffsets(groupID string, tps []TopicPartition) (map[TopicPartition]int64, error) {
	_, _ = groupID, tps
	return nil, errUnimplemented
}

// List returns every known group id (with members or with committed offsets), sorted.
func (c *Coordinator) List() ([]string, error) { return nil, errUnimplemented }

// Describe describes a group.
func (c *Coordinator) Describe(groupID string) (GroupDescription, error) {
	_ = groupID
	return GroupDescription{}, errUnimplemented
}

// BeginShutdown fails parked joins and makes new requests fail with ErrCoordinatorClosed.
func (c *Coordinator) BeginShutdown() {}

// Close shuts down (as BeginShutdown), stops timers, waits for running timer callbacks and
// unsubscribes from the store. Idempotent.
func (c *Coordinator) Close() {}
