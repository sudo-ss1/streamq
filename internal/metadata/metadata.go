// Package metadata defines the cluster metadata model, the Store abstraction (SPEC §2.2) and the
// deterministic state machine shared by the local (M2) and Raft (M5) stores.
package metadata

import (
	"context"
	"errors"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// CommandVersion is the only MetadataCommand version this code applies.
const CommandVersion = 1

// NoLeader is the leader ID of a partition without a leader.
const NoLeader int32 = -1

// Sentinel errors.
var (
	ErrTopicExists              = errors.New("metadata: topic already exists")
	ErrUnknownTopic             = errors.New("metadata: unknown topic")
	ErrInvalidReplicationFactor = errors.New("metadata: invalid replication factor")
	ErrInvalidTopic             = errors.New("metadata: invalid topic")
	ErrInvalidCommand           = errors.New("metadata: invalid command")
	ErrNotController            = errors.New("metadata: not the controller")
	ErrClosed                   = errors.New("metadata: store closed")
)

// Broker is a registered broker.
type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}

// PartitionState is the replicated state of one partition (SPEC §2.1).
type PartitionState struct {
	Partition      int32   `json:"partition"`
	Replicas       []int32 `json:"replicas"`
	Leader         int32   `json:"leader"`
	LeaderEpoch    int32   `json:"leader_epoch"`
	ISR            []int32 `json:"isr"`
	PartitionEpoch int32   `json:"partition_epoch"`
}

// Topic is one topic's metadata.
type Topic struct {
	Name       string            `json:"name"`
	ID         string            `json:"id"`
	Configs    map[string]string `json:"configs"`
	Partitions []PartitionState  `json:"partitions"`
}

// Snapshot is an immutable, consistent view of the metadata. Callers must not modify it.
type Snapshot struct {
	Brokers      []Broker
	ControllerID int32
	Topics       map[string]Topic
}

// Topic returns the named topic.
func (s Snapshot) Topic(name string) (Topic, bool) {
	t, ok := s.Topics[name]
	return t, ok
}

// ChangeKind classifies a Change.
type ChangeKind int

// Change kinds.
const (
	TopicCreated ChangeKind = iota + 1
	TopicDeleted
	PartitionChanged
)

// Change describes one applied state change, delivered to subscribers in apply order.
type Change struct {
	Kind       ChangeKind
	Topic      string
	TopicID    string
	Partitions []PartitionState  // created/changed partitions; for TopicDeleted, the deleted ones
	Configs    map[string]string // topic configs (a copy); set for TopicCreated
}

// Store is the metadata store (SPEC §2.2). Brokers and groups must not care which
// implementation is in use.
type Store interface {
	// Snapshot returns the current state.
	Snapshot() Snapshot
	// Propose applies cmd; when it returns nil the command is applied and persisted, and its
	// changes have been queued for subscribers.
	Propose(ctx context.Context, cmd *streamqv1.MetadataCommand) error
	// Subscribe atomically registers fn and returns the snapshot it starts from: every change
	// applied after that snapshot is delivered to fn exactly once, in order, on the store's
	// notifier goroutine, with the store's lifetime ctx. The returned func unsubscribes; it blocks
	// until any in-flight callback for this subscriber has returned, and is idempotent.
	Subscribe(fn func(context.Context, Change)) (Snapshot, func())
	// Close stops the notifier and releases resources.
	Close() error
}
