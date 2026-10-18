package metadata

import (
	"fmt"
	"hash/fnv"
	"maps"
	"slices"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// maxTopicNameLen is Kafka's topic name length limit.
const maxTopicNameLen = 249

// State is the deterministic metadata state machine. Apply uses no clock or randomness; every
// input comes in the command. Fields are exported for JSON persistence by the local store.
type State struct {
	Brokers      []Broker          `json:"brokers"`
	ControllerID int32             `json:"controller_id"`
	Topics       map[string]*Topic `json:"topics"`
}

// NewState returns a state with the given registered brokers and controller.
func NewState(brokers []Broker, controllerID int32) *State {
	return &State{
		Brokers:      slices.Clone(brokers),
		ControllerID: controllerID,
		Topics:       make(map[string]*Topic),
	}
}

// Clone returns a deep copy.
func (s *State) Clone() *State {
	c := &State{
		Brokers:      slices.Clone(s.Brokers),
		ControllerID: s.ControllerID,
		Topics:       make(map[string]*Topic, len(s.Topics)),
	}
	for name, t := range s.Topics {
		ct := cloneTopic(*t)
		c.Topics[name] = &ct
	}
	return c
}

// Snapshot returns an immutable deep copy as a Snapshot.
func (s *State) Snapshot() Snapshot {
	snap := Snapshot{
		Brokers:      slices.Clone(s.Brokers),
		ControllerID: s.ControllerID,
		Topics:       make(map[string]Topic, len(s.Topics)),
	}
	for name, t := range s.Topics {
		snap.Topics[name] = cloneTopic(*t)
	}
	return snap
}

func cloneTopic(t Topic) Topic {
	return Topic{Name: t.Name, ID: t.ID, Configs: maps.Clone(t.Configs), Partitions: clonePartitions(t.Partitions)}
}

func clonePartitions(ps []PartitionState) []PartitionState {
	if ps == nil {
		return nil
	}
	out := make([]PartitionState, len(ps))
	for i, p := range ps {
		out[i] = p
		out[i].Replicas = slices.Clone(p.Replicas)
		out[i].ISR = slices.Clone(p.ISR)
	}
	return out
}

// Apply validates and applies cmd, returning the resulting changes. On error the state is
// unchanged. Create: the topic name must be valid (ErrInvalidTopic), new (ErrTopicExists), have
// at least one partition numbered 0..n-1, and every partition's replicas must be distinct
// registered brokers with the same count for all partitions (ErrInvalidReplicationFactor). New
// partitions start with leader = replicas[0], leader epoch 0, ISR = replicas, partition epoch 0.
// Delete: the topic must exist (ErrUnknownTopic). A version other than CommandVersion or an empty
// command returns ErrInvalidCommand.
func (s *State) Apply(cmd *streamqv1.MetadataCommand) ([]Change, error) {
	if cmd == nil {
		return nil, fmt.Errorf("nil command: %w", ErrInvalidCommand)
	}
	if cmd.GetVersion() != CommandVersion {
		return nil, fmt.Errorf("command version %d (want %d): %w", cmd.GetVersion(), CommandVersion, ErrInvalidCommand)
	}
	switch c := cmd.GetCmd().(type) {
	case *streamqv1.MetadataCommand_CreateTopic:
		return s.applyCreate(c.CreateTopic)
	case *streamqv1.MetadataCommand_DeleteTopic:
		return s.applyDelete(c.DeleteTopic)
	default:
		return nil, fmt.Errorf("empty command: %w", ErrInvalidCommand)
	}
}

// applyCreate validates everything before mutating, so an error leaves s unchanged.
func (s *State) applyCreate(c *streamqv1.CreateTopicCommand) ([]Change, error) {
	if c == nil {
		return nil, fmt.Errorf("empty create_topic: %w", ErrInvalidCommand)
	}
	name := c.GetName()
	if err := ValidateTopicName(name); err != nil {
		return nil, err
	}
	if _, ok := s.Topics[name]; ok {
		return nil, fmt.Errorf("create topic %q: %w", name, ErrTopicExists)
	}
	if c.GetTopicId() == "" {
		return nil, fmt.Errorf("create topic %q: missing topic_id: %w", name, ErrInvalidCommand)
	}
	assignments := c.GetPartitions()
	if len(assignments) == 0 {
		return nil, fmt.Errorf("create topic %q: no partitions: %w", name, ErrInvalidCommand)
	}
	registered := make(map[int32]struct{}, len(s.Brokers))
	for _, b := range s.Brokers {
		registered[b.ID] = struct{}{}
	}
	rf := len(assignments[0].GetReplicas())
	parts := make([]PartitionState, 0, len(assignments))
	for i, a := range assignments {
		if a == nil || int(a.GetPartition()) != i {
			return nil, fmt.Errorf("create topic %q: partition at index %d is not numbered %d: %w",
				name, i, i, ErrInvalidCommand)
		}
		replicas := a.GetReplicas()
		if len(replicas) == 0 || len(replicas) != rf {
			return nil, fmt.Errorf("create topic %q partition %d: %d replicas (want %d, >= 1): %w",
				name, i, len(replicas), rf, ErrInvalidReplicationFactor)
		}
		seen := make(map[int32]struct{}, len(replicas))
		for _, id := range replicas {
			if _, ok := registered[id]; !ok {
				return nil, fmt.Errorf("create topic %q partition %d: unknown broker %d: %w",
					name, i, id, ErrInvalidReplicationFactor)
			}
			if _, dup := seen[id]; dup {
				return nil, fmt.Errorf("create topic %q partition %d: duplicate replica %d: %w",
					name, i, id, ErrInvalidReplicationFactor)
			}
			seen[id] = struct{}{}
		}
		parts = append(parts, PartitionState{
			Partition:      int32(i),
			Replicas:       slices.Clone(replicas),
			Leader:         replicas[0],
			LeaderEpoch:    0,
			ISR:            slices.Clone(replicas),
			PartitionEpoch: 0,
		})
	}
	configs := maps.Clone(c.GetConfigs())
	if configs == nil {
		configs = make(map[string]string)
	}
	t := &Topic{Name: name, ID: c.GetTopicId(), Configs: configs, Partitions: parts}
	if s.Topics == nil {
		s.Topics = make(map[string]*Topic)
	}
	s.Topics[name] = t
	return []Change{{
		Kind:       TopicCreated,
		Topic:      name,
		TopicID:    t.ID,
		Partitions: clonePartitions(parts),
		Configs:    maps.Clone(configs),
	}}, nil
}

func (s *State) applyDelete(c *streamqv1.DeleteTopicCommand) ([]Change, error) {
	if c == nil {
		return nil, fmt.Errorf("empty delete_topic: %w", ErrInvalidCommand)
	}
	name := c.GetName()
	t, ok := s.Topics[name]
	if !ok {
		return nil, fmt.Errorf("delete topic %q: %w", name, ErrUnknownTopic)
	}
	delete(s.Topics, name)
	return []Change{{Kind: TopicDeleted, Topic: name, TopicID: t.ID, Partitions: clonePartitions(t.Partitions)}}, nil
}

// AssignReplicas assigns rf replicas to each of n partitions over brokerIDs, Kafka-style
// round-robin starting at a position derived deterministically from topic. It returns
// ErrInvalidReplicationFactor if rf < 1 or rf > len(brokerIDs), and ErrInvalidCommand if n < 1.
//
// Broker IDs are sorted ascending (duplicates removed); the start index is fnv32a(topic) modulo
// the broker count, and partition p gets brokers[(start+p+j) % len] for j in 0..rf-1.
func AssignReplicas(topic string, brokerIDs []int32, n, rf int32) ([][]int32, error) {
	if n < 1 {
		return nil, fmt.Errorf("assign replicas: %d partitions: %w", n, ErrInvalidCommand)
	}
	ids := slices.Clone(brokerIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if rf < 1 || int(rf) > len(ids) {
		return nil, fmt.Errorf("assign replicas: replication factor %d with %d brokers: %w",
			rf, len(ids), ErrInvalidReplicationFactor)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic)) // hash.Hash.Write never returns an error
	size := uint64(len(ids))
	start := uint64(h.Sum32()) % size
	out := make([][]int32, n)
	for p := range uint64(n) {
		replicas := make([]int32, rf)
		for j := range uint64(rf) {
			replicas[j] = ids[(start+p+j)%size]
		}
		out[p] = replicas
	}
	return out, nil
}

// ValidateTopicName enforces Kafka's rules: 1–249 chars of [a-zA-Z0-9._-], not "." or "..".
func ValidateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name is empty: %w", ErrInvalidTopic)
	}
	if len(name) > maxTopicNameLen {
		return fmt.Errorf("topic name is %d chars (max %d): %w", len(name), maxTopicNameLen, ErrInvalidTopic)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name %q is not allowed: %w", name, ErrInvalidTopic)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		valid := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '.' || c == '_' || c == '-'
		if !valid {
			return fmt.Errorf("topic name %q has invalid character %q: %w", name, c, ErrInvalidTopic)
		}
	}
	return nil
}
