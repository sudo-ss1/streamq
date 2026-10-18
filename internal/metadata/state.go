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
	// Groups holds committed consumer group offsets (SPEC §5).
	Groups map[string]GroupOffsets `json:"groups,omitempty"`
}

// NewState returns a state with the given registered brokers and controller.
func NewState(brokers []Broker, controllerID int32) *State {
	return &State{
		Brokers:      slices.Clone(brokers),
		ControllerID: controllerID,
		Topics:       make(map[string]*Topic),
		Groups:       make(map[string]GroupOffsets),
	}
}

// Clone returns a deep copy.
func (s *State) Clone() *State {
	c := &State{
		Brokers:      slices.Clone(s.Brokers),
		ControllerID: s.ControllerID,
		Topics:       make(map[string]*Topic, len(s.Topics)),
		Groups:       cloneGroups(s.Groups),
	}
	for name, t := range s.Topics {
		ct := cloneTopic(*t)
		c.Topics[name] = &ct
	}
	return c
}

func cloneGroups(gs map[string]GroupOffsets) map[string]GroupOffsets {
	if gs == nil {
		return nil
	}
	out := make(map[string]GroupOffsets, len(gs))
	for g, offs := range gs {
		out[g] = offs.Clone()
	}
	return out
}

// Snapshot returns an immutable deep copy as a Snapshot.
func (s *State) Snapshot() Snapshot {
	snap := Snapshot{
		Brokers:      slices.Clone(s.Brokers),
		ControllerID: s.ControllerID,
		Topics:       make(map[string]Topic, len(s.Topics)),
		Groups:       cloneGroups(s.Groups),
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
// partitions start with ISR = the unfenced replicas in replica order and leader = the first of
// them (none unfenced is ErrInvalidReplicationFactor), leader epoch 0, partition epoch 0.
// Delete: the topic must exist (ErrUnknownTopic); its committed offsets are dropped from every
// group (DECISIONS D27). CommitOffsets: the group id must be non-empty and the offset list
// non-empty, every partition must exist (ErrUnknownTopic) and every offset be >= 0
// (ErrInvalidCommand). A version other than CommandVersion or an empty command returns
// ErrInvalidCommand.
func (s *State) Apply(cmd *streamqv1.MetadataCommand) ([]Change, error) {
	return s.ApplyInTerm(cmd, 0)
}

// ApplyInTerm is Apply for a command committed in Raft term term. A term-bound command (non-zero
// controller_epoch) issued in a different term is rejected with ErrNotController, so a deposed
// controller's or coordinator's write can never land in a later term (DECISIONS D33).
func (s *State) ApplyInTerm(cmd *streamqv1.MetadataCommand, term uint64) ([]Change, error) {
	if e := cmd.GetControllerEpoch(); e != 0 && e != term {
		return nil, fmt.Errorf("command issued in term %d committed in term %d: %w", e, term, ErrNotController)
	}
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
	case *streamqv1.MetadataCommand_CommitOffsets:
		return s.applyCommitOffsets(c.CommitOffsets)
	case *streamqv1.MetadataCommand_RegisterBroker:
		return s.applyRegisterBroker(c.RegisterBroker)
	case *streamqv1.MetadataCommand_FenceBroker:
		return s.applyFenceBroker(c.FenceBroker)
	case *streamqv1.MetadataCommand_UnfenceBroker:
		return s.applyUnfenceBroker(c.UnfenceBroker)
	case *streamqv1.MetadataCommand_AlterIsr:
		return s.applyAlterISR(c.AlterIsr)
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
		var isr []int32
		for _, id := range replicas {
			if !s.fenced(id) {
				isr = append(isr, id)
			}
		}
		if len(isr) == 0 {
			return nil, fmt.Errorf("create topic %q partition %d: every replica is fenced: %w",
				name, i, ErrInvalidReplicationFactor)
		}
		leader := isr[0]
		parts = append(parts, PartitionState{
			Partition:      int32(i),
			Replicas:       slices.Clone(replicas),
			Leader:         leader,
			LeaderEpoch:    0,
			ISR:            isr,
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
	if !ok || (c.GetTopicId() != "" && c.GetTopicId() != t.ID) {
		return nil, fmt.Errorf("delete topic %q (id %q): %w", name, c.GetTopicId(), ErrUnknownTopic)
	}
	delete(s.Topics, name)
	for g, offs := range s.Groups {
		delete(offs, name)
		if len(offs) == 0 {
			delete(s.Groups, g)
		}
	}
	return []Change{{Kind: TopicDeleted, Topic: name, TopicID: t.ID, Partitions: clonePartitions(t.Partitions)}}, nil
}

// applyCommitOffsets validates every offset before mutating, so an error leaves s unchanged.
func (s *State) applyCommitOffsets(c *streamqv1.CommitOffsetsCommand) ([]Change, error) {
	if c == nil || c.GetGroupId() == "" || len(c.GetOffsets()) == 0 {
		return nil, fmt.Errorf("commit_offsets needs a group id and offsets: %w", ErrInvalidCommand)
	}
	for _, o := range c.GetOffsets() {
		t, ok := s.Topics[o.GetTopic()]
		if !ok || o.GetPartition() < 0 || int(o.GetPartition()) >= len(t.Partitions) {
			return nil, fmt.Errorf("commit offset for %s/%d: %w", o.GetTopic(), o.GetPartition(), ErrUnknownTopic)
		}
		if o.GetOffset() < 0 {
			return nil, fmt.Errorf("commit offset %d for %s/%d: %w", o.GetOffset(), o.GetTopic(), o.GetPartition(), ErrInvalidCommand)
		}
	}
	if s.Groups == nil {
		s.Groups = make(map[string]GroupOffsets)
	}
	offs := s.Groups[c.GetGroupId()]
	if offs == nil {
		offs = GroupOffsets{}
		s.Groups[c.GetGroupId()] = offs
	}
	for _, o := range c.GetOffsets() {
		if offs[o.GetTopic()] == nil {
			offs[o.GetTopic()] = map[int32]int64{}
		}
		offs[o.GetTopic()][o.GetPartition()] = o.GetOffset()
	}
	return []Change{{Kind: OffsetsCommitted, Group: c.GetGroupId()}}, nil
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

// brokerIndex returns the index of broker id in s.Brokers, or -1.
func (s *State) brokerIndex(id int32) int {
	for i, b := range s.Brokers {
		if b.ID == id {
			return i
		}
	}
	return -1
}

func (s *State) fenced(id int32) bool {
	i := s.brokerIndex(id)
	return i < 0 || s.Brokers[i].Fenced
}

// electLeader returns the first replica (in replica order) that is in the ISR and unfenced, or
// NoLeader (SPEC §4.4; unclean election is disabled).
func (s *State) electLeader(ps PartitionState) int32 {
	for _, r := range ps.Replicas {
		if slices.Contains(ps.ISR, r) && !s.fenced(r) {
			return r
		}
	}
	return NoLeader
}

// sortedTopicNames returns topic names in order, so changes are emitted deterministically.
func (s *State) sortedTopicNames() []string {
	names := make([]string, 0, len(s.Topics))
	for n := range s.Topics {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// fencePartitions applies fencing of broker id to every partition: it leaves each ISR unless it is the
// last member (DECISIONS D34) and, where it led, a new leader is elected (leader epoch++). Any
// change bumps the partition epoch. The broker must already be marked fenced.
func (s *State) fencePartitions(id int32) []Change {
	var changes []Change
	for _, name := range s.sortedTopicNames() {
		t := s.Topics[name]
		var changed []PartitionState
		for i := range t.Partitions {
			ps := &t.Partitions[i]
			dirty := false
			if j := slices.Index(ps.ISR, id); j >= 0 && len(ps.ISR) > 1 {
				ps.ISR = slices.Delete(slices.Clone(ps.ISR), j, j+1)
				dirty = true
			}
			if ps.Leader == id {
				ps.Leader = s.electLeader(*ps)
				ps.LeaderEpoch++
				dirty = true
			}
			if dirty {
				ps.PartitionEpoch++
				changed = append(changed, clonePartitions([]PartitionState{*ps})...)
			}
		}
		if len(changed) > 0 {
			changes = append(changes, Change{Kind: PartitionChanged, Topic: name, TopicID: t.ID, Partitions: changed})
		}
	}
	return changes
}

// applyRegisterBroker adds or re-registers a broker. It is (re)registered fenced: a broker that
// re-registers lost its previous incarnation's state, so it gives up leadership and ISR
// membership exactly as if fenced, and is unfenced once caught up (SPEC §2.3, DECISIONS D35).
func (s *State) applyRegisterBroker(c *streamqv1.RegisterBrokerCommand) ([]Change, error) {
	if c == nil || c.GetBrokerId() <= 0 || c.GetHost() == "" || c.GetPort() <= 0 {
		return nil, fmt.Errorf("register_broker needs id, host and port: %w", ErrInvalidCommand)
	}
	b := Broker{ID: c.GetBrokerId(), Host: c.GetHost(), Port: c.GetPort(), InternalAddr: c.GetInternalAddr(), Fenced: true, Epoch: 1}
	if i := s.brokerIndex(b.ID); i >= 0 {
		b.Epoch = s.Brokers[i].Epoch + 1
		s.Brokers[i] = b
	} else {
		s.Brokers = append(s.Brokers, b)
		slices.SortFunc(s.Brokers, func(x, y Broker) int { return int(x.ID) - int(y.ID) })
	}
	changes := []Change{{Kind: BrokersChanged}}
	return append(changes, s.fencePartitions(b.ID)...), nil
}

// applyFenceBroker fences a registered broker (idempotent).
func (s *State) applyFenceBroker(c *streamqv1.FenceBrokerCommand) ([]Change, error) {
	i := s.brokerIndex(c.GetBrokerId())
	if c == nil || i < 0 {
		return nil, fmt.Errorf("fence unknown broker %d: %w", c.GetBrokerId(), ErrInvalidCommand)
	}
	if s.Brokers[i].Fenced || s.Brokers[i].Epoch != c.GetBrokerEpoch() {
		return nil, nil // already fenced, or a decision about an older incarnation (D34)
	}
	s.Brokers[i].Fenced = true
	changes := []Change{{Kind: BrokersChanged}}
	return append(changes, s.fencePartitions(c.GetBrokerId())...), nil
}

// applyUnfenceBroker unfences a registered broker; every offline partition whose ISR contains it
// elects a leader again (DECISIONS D34).
func (s *State) applyUnfenceBroker(c *streamqv1.UnfenceBrokerCommand) ([]Change, error) {
	i := s.brokerIndex(c.GetBrokerId())
	if c == nil || i < 0 {
		return nil, fmt.Errorf("unfence unknown broker %d: %w", c.GetBrokerId(), ErrInvalidCommand)
	}
	if !s.Brokers[i].Fenced || s.Brokers[i].Epoch != c.GetBrokerEpoch() {
		return nil, nil // already unfenced, or a decision about an older incarnation (D34)
	}
	s.Brokers[i].Fenced = false
	changes := []Change{{Kind: BrokersChanged}}
	for _, name := range s.sortedTopicNames() {
		t := s.Topics[name]
		var changed []PartitionState
		for j := range t.Partitions {
			ps := &t.Partitions[j]
			if ps.Leader != NoLeader {
				continue
			}
			if leader := s.electLeader(*ps); leader != NoLeader {
				ps.Leader = leader
				ps.LeaderEpoch++
				ps.PartitionEpoch++
				changed = append(changed, clonePartitions([]PartitionState{*ps})...)
			}
		}
		if len(changed) > 0 {
			changes = append(changes, Change{Kind: PartitionChanged, Topic: name, TopicID: t.ID, Partitions: changed})
		}
	}
	return changes, nil
}

// applyAlterISR changes a partition's ISR for its leader (SPEC §4.3). The leader and partition
// epochs must match (ErrFencedLeaderEpoch); the new ISR must be non-empty, ⊆ replicas, contain the
// leader, and every other member must be unfenced (ErrInvalidISR). The ISR is stored in replica
// order and the partition epoch is bumped.
func (s *State) applyAlterISR(c *streamqv1.AlterISRCommand) ([]Change, error) {
	t, ok := s.Topics[c.GetTopic()]
	if !ok || c.GetPartition() < 0 || int(c.GetPartition()) >= len(t.Partitions) {
		return nil, fmt.Errorf("alter isr %s/%d: %w", c.GetTopic(), c.GetPartition(), ErrUnknownTopic)
	}
	ps := &t.Partitions[c.GetPartition()]
	if c.GetLeaderEpoch() != ps.LeaderEpoch || c.GetPartitionEpoch() != ps.PartitionEpoch {
		return nil, fmt.Errorf("alter isr %s/%d: epochs (%d, %d) != current (%d, %d): %w", c.GetTopic(), c.GetPartition(),
			c.GetLeaderEpoch(), c.GetPartitionEpoch(), ps.LeaderEpoch, ps.PartitionEpoch, ErrFencedLeaderEpoch)
	}
	want := c.GetNewIsr()
	if len(want) == 0 || !slices.Contains(want, ps.Leader) {
		return nil, fmt.Errorf("alter isr %s/%d: %v must contain leader %d: %w", c.GetTopic(), c.GetPartition(), want, ps.Leader, ErrInvalidISR)
	}
	var isr []int32
	for _, r := range ps.Replicas {
		if !slices.Contains(want, r) {
			continue
		}
		if r != ps.Leader && s.fenced(r) {
			return nil, fmt.Errorf("alter isr %s/%d: broker %d is fenced: %w", c.GetTopic(), c.GetPartition(), r, ErrInvalidISR)
		}
		isr = append(isr, r)
	}
	if len(isr) != len(want) {
		return nil, fmt.Errorf("alter isr %s/%d: %v not a set of replicas %v: %w", c.GetTopic(), c.GetPartition(), want, ps.Replicas, ErrInvalidISR)
	}
	ps.ISR = isr
	ps.PartitionEpoch++
	return []Change{{Kind: PartitionChanged, Topic: c.GetTopic(), TopicID: t.ID, Partitions: clonePartitions([]PartitionState{*ps})}}, nil
}
