package metadata

import streamqv1 "github.com/sudo-ss1/streamq/gen/v1"

// State is the deterministic metadata state machine. Apply uses no clock or randomness; every
// input comes in the command. Fields are exported for JSON persistence by the local store.
type State struct {
	Brokers      []Broker          `json:"brokers"`
	ControllerID int32             `json:"controller_id"`
	Topics       map[string]*Topic `json:"topics"`
}

// NewState returns a state with the given registered brokers and controller.
func NewState(brokers []Broker, controllerID int32) *State {
	_, _ = brokers, controllerID
	return &State{}
}

// Clone returns a deep copy.
func (s *State) Clone() *State { return &State{} }

// Snapshot returns an immutable deep copy as a Snapshot.
func (s *State) Snapshot() Snapshot { return Snapshot{} }

// Apply validates and applies cmd, returning the resulting changes. On error the state is
// unchanged. Create: the topic name must be valid (ErrInvalidTopic), new (ErrTopicExists), have
// at least one partition numbered 0..n-1, and every partition's replicas must be distinct
// registered brokers with the same count for all partitions (ErrInvalidReplicationFactor). New
// partitions start with leader = replicas[0], leader epoch 0, ISR = replicas, partition epoch 0.
// Delete: the topic must exist (ErrUnknownTopic). A version other than CommandVersion or an empty
// command returns ErrInvalidCommand.
func (s *State) Apply(cmd *streamqv1.MetadataCommand) ([]Change, error) {
	_ = cmd
	return nil, errUnimplemented
}

// AssignReplicas assigns rf replicas to each of n partitions over brokerIDs, Kafka-style
// round-robin starting at a position derived deterministically from topic. It returns
// ErrInvalidReplicationFactor if rf < 1 or rf > len(brokerIDs), and ErrInvalidCommand if n < 1.
func AssignReplicas(topic string, brokerIDs []int32, n, rf int32) ([][]int32, error) {
	_, _, _, _ = topic, brokerIDs, n, rf
	return nil, errUnimplemented
}

// ValidateTopicName enforces Kafka's rules: 1–249 chars of [a-zA-Z0-9._-], not "." or "..".
func ValidateTopicName(name string) error {
	_ = name
	return errUnimplemented
}
