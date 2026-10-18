package metadata_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

func brokers(ids ...int32) []metadata.Broker {
	out := make([]metadata.Broker, len(ids))
	for i, id := range ids {
		out[i] = metadata.Broker{ID: id, Host: "localhost", Port: 9000 + id}
	}
	return out
}

func createCmd(name, id string, configs map[string]string, replicas ...[]int32) *streamqv1.MetadataCommand {
	parts := make([]*streamqv1.PartitionAssignment, len(replicas))
	for i, r := range replicas {
		parts[i] = &streamqv1.PartitionAssignment{Partition: int32(i), Replicas: r}
	}
	return &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd: &streamqv1.MetadataCommand_CreateTopic{CreateTopic: &streamqv1.CreateTopicCommand{
			Name: name, TopicId: id, Configs: configs, Partitions: parts,
		}},
	}
}

func deleteCmd(name string) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{
		Version: metadata.CommandVersion,
		Cmd:     &streamqv1.MetadataCommand_DeleteTopic{DeleteTopic: &streamqv1.DeleteTopicCommand{Name: name}},
	}
}

// baseState has brokers 1..3 and an existing topic "existing" with 2 partitions.
func baseState(t *testing.T) *metadata.State {
	t.Helper()
	s := metadata.NewState(brokers(1, 2, 3), 1)
	_, err := s.Apply(createCmd("existing", "id-existing", map[string]string{"retention.ms": "1000"},
		[]int32{1, 2}, []int32{2, 3}))
	require.NoError(t, err)
	return s
}

func TestApply(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cmd     *streamqv1.MetadataCommand
		wantErr error
		want    []metadata.Change
	}{
		{
			name: "valid create",
			cmd:  createCmd("orders", "id-orders", map[string]string{"a": "b"}, []int32{2, 3}, []int32{3, 1}, []int32{1, 2}),
			want: []metadata.Change{{
				Kind: metadata.TopicCreated, Topic: "orders", TopicID: "id-orders",
				Configs: map[string]string{"a": "b"},
				Partitions: []metadata.PartitionState{
					{Partition: 0, Replicas: []int32{2, 3}, Leader: 2, ISR: []int32{2, 3}},
					{Partition: 1, Replicas: []int32{3, 1}, Leader: 3, ISR: []int32{3, 1}},
					{Partition: 2, Replicas: []int32{1, 2}, Leader: 1, ISR: []int32{1, 2}},
				},
			}},
		},
		{
			name: "valid delete",
			cmd:  deleteCmd("existing"),
			want: []metadata.Change{{
				Kind: metadata.TopicDeleted, Topic: "existing", TopicID: "id-existing",
				Partitions: []metadata.PartitionState{
					{Partition: 0, Replicas: []int32{1, 2}, Leader: 1, ISR: []int32{1, 2}},
					{Partition: 1, Replicas: []int32{2, 3}, Leader: 2, ISR: []int32{2, 3}},
				},
			}},
		},
		{name: "duplicate topic", cmd: createCmd("existing", "id-x", nil, []int32{1}), wantErr: metadata.ErrTopicExists},
		{name: "delete unknown", cmd: deleteCmd("nope"), wantErr: metadata.ErrUnknownTopic},
		{name: "rf greater than brokers", cmd: createCmd("t", "id", nil, []int32{1, 2, 3, 4}), wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "unknown broker", cmd: createCmd("t", "id", nil, []int32{1, 9}), wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "duplicate replica", cmd: createCmd("t", "id", nil, []int32{1, 1}), wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "empty replicas", cmd: createCmd("t", "id", nil, []int32{}), wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "unequal replica counts", cmd: createCmd("t", "id", nil, []int32{1, 2}, []int32{3}), wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "empty name", cmd: createCmd("", "id", nil, []int32{1}), wantErr: metadata.ErrInvalidTopic},
		{name: "dot name", cmd: createCmd(".", "id", nil, []int32{1}), wantErr: metadata.ErrInvalidTopic},
		{name: "dotdot name", cmd: createCmd("..", "id", nil, []int32{1}), wantErr: metadata.ErrInvalidTopic},
		{name: "bad char", cmd: createCmd("a/b", "id", nil, []int32{1}), wantErr: metadata.ErrInvalidTopic},
		{name: "too long", cmd: createCmd(strings.Repeat("a", 250), "id", nil, []int32{1}), wantErr: metadata.ErrInvalidTopic},
		{name: "missing topic id", cmd: createCmd("t", "", nil, []int32{1}), wantErr: metadata.ErrInvalidCommand},
		{name: "no partitions", cmd: createCmd("t", "id", nil), wantErr: metadata.ErrInvalidCommand},
		{
			name: "partitions out of order",
			cmd: &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
				CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "id", Partitions: []*streamqv1.PartitionAssignment{
					{Partition: 1, Replicas: []int32{1}}, {Partition: 0, Replicas: []int32{2}},
				}},
			}},
			wantErr: metadata.ErrInvalidCommand,
		},
		{
			name:    "bad version",
			cmd:     &streamqv1.MetadataCommand{Version: 2, Cmd: deleteCmd("existing").Cmd},
			wantErr: metadata.ErrInvalidCommand,
		},
		{name: "empty oneof", cmd: &streamqv1.MetadataCommand{Version: metadata.CommandVersion}, wantErr: metadata.ErrInvalidCommand},
		{name: "nil command", cmd: nil, wantErr: metadata.ErrInvalidCommand},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := baseState(t)
			before := s.Clone()
			changes, err := s.Apply(tc.cmd)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, changes)
				require.Equal(t, before, s, "failed Apply must leave state unchanged")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, changes)
		})
	}
}

func TestApplyCreateStoresTopic(t *testing.T) {
	t.Parallel()
	s := metadata.NewState(brokers(1, 2), 1)
	configs := map[string]string{"retention.ms": "5"}
	replicas := []int32{2, 1}
	created, err := s.Apply(createCmd("t", "abc", configs, replicas))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"retention.ms": "5"}, created[0].Configs)

	// The command's inputs are copied, not aliased.
	configs["retention.ms"] = "999"
	replicas[0] = 7

	topic, ok := s.Snapshot().Topic("t")
	require.True(t, ok)
	require.Equal(t, metadata.Topic{
		Name: "t", ID: "abc", Configs: map[string]string{"retention.ms": "5"},
		Partitions: []metadata.PartitionState{{Partition: 0, Replicas: []int32{2, 1}, Leader: 2, ISR: []int32{2, 1}}},
	}, topic)

	// Delete then recreate with a new ID.
	_, err = s.Apply(deleteCmd("t"))
	require.NoError(t, err)
	_, ok = s.Snapshot().Topic("t")
	require.False(t, ok)
	changes, err := s.Apply(createCmd("t", "def", nil, []int32{1}))
	require.NoError(t, err)
	require.Equal(t, "def", changes[0].TopicID)
}

func TestChangeIsolatedFromState(t *testing.T) {
	t.Parallel()
	s := metadata.NewState(brokers(1), 1)
	changes, err := s.Apply(createCmd("t", "id", nil, []int32{1}))
	require.NoError(t, err)
	changes[0].Partitions[0].Replicas[0] = 42
	changes[0].Partitions[0].ISR[0] = 42
	require.Equal(t, map[string]string{}, changes[0].Configs)
	changes[0].Configs["k"] = "v"
	require.Empty(t, s.Snapshot().Topics["t"].Configs)
	p := s.Snapshot().Topics["t"].Partitions[0]
	require.Equal(t, []int32{1}, p.Replicas)
	require.Equal(t, []int32{1}, p.ISR)
}

func TestSnapshotAndCloneIsolation(t *testing.T) {
	t.Parallel()
	s := baseState(t)
	orig := s.Clone()

	snap := s.Snapshot()
	snap.Brokers[0].ID = 100
	snap.Topics["existing"].Configs["retention.ms"] = "x"
	snap.Topics["existing"].Partitions[0].Replicas[0] = 100
	snap.Topics["existing"].Partitions[0].ISR[0] = 100
	delete(snap.Topics, "existing")
	require.Equal(t, orig, s)

	_, err := s.Apply(&streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_AllocateProducerIds{
		AllocateProducerIds: &streamqv1.AllocateProducerIDsCommand{BrokerId: 2},
	}})
	require.NoError(t, err)
	orig = s.Clone()
	require.Equal(t, int64(metadata.ProducerIDBlockSize), orig.NextProducerID)
	snap3 := s.Snapshot()
	snap3.ProducerIDBlocks[2] = 99
	require.Equal(t, orig, s)

	c := s.Clone()
	c.ProducerIDBlocks[2] = 99
	c.NextProducerID = 7
	c.Brokers[0].ID = 100
	c.ControllerID = 100
	c.Topics["existing"].Configs["retention.ms"] = "x"
	c.Topics["existing"].Partitions[1].Leader = 100
	c.Topics["existing"].Partitions[1].Replicas[1] = 100
	c.Topics["existing"].Partitions[1].ISR[1] = 100
	c.Topics["existing"].Name = "renamed"
	_, err = c.Apply(deleteCmd("existing"))
	require.NoError(t, err)
	require.Equal(t, orig, s)

	// Mutating the state does not affect an earlier snapshot.
	snap2 := s.Snapshot()
	_, err = s.Apply(deleteCmd("existing"))
	require.NoError(t, err)
	_, ok := snap2.Topic("existing")
	require.True(t, ok)
}

func TestNewStateCopiesBrokers(t *testing.T) {
	t.Parallel()
	bs := brokers(1, 2)
	s := metadata.NewState(bs, 2)
	bs[0].ID = 99
	require.Equal(t, brokers(1, 2), s.Brokers)
	require.Equal(t, int32(2), s.ControllerID)
	require.NotNil(t, s.Topics)
}

func TestAssignReplicasProperties(t *testing.T) {
	t.Parallel()
	brokerSets := [][]int32{{1}, {1, 2}, {3, 1, 2}, {5, 9, 2, 7}, {10, 20, 30, 40, 50}, {0, 1, 2, 3, 4, 5, 6}}
	topics := []string{"a", "orders", "events.v1", "x-y_z"}
	for _, ids := range brokerSets {
		for _, topic := range topics {
			for n := int32(1); n <= 12; n++ {
				for rf := int32(1); int(rf) <= len(ids); rf++ {
					name := fmt.Sprintf("%v/%s/n%d/rf%d", ids, topic, n, rf)
					got, err := metadata.AssignReplicas(topic, ids, n, rf)
					require.NoError(t, err, name)
					require.Len(t, got, int(n), name)
					again, err := metadata.AssignReplicas(topic, slices.Clone(ids), n, rf)
					require.NoError(t, err, name)
					require.Equal(t, got, again, "deterministic: %s", name)

					sorted := slices.Sorted(slices.Values(ids))
					leaders := map[int32]int{}
					for p, r := range got {
						require.Len(t, r, int(rf), name)
						require.Len(t, uniq(r), int(rf), "distinct replicas: %s p%d", name, p)
						for _, id := range r {
							require.Contains(t, ids, id, name)
						}
						// Round robin: consecutive partitions shift by one broker.
						start := slices.Index(sorted, r[0])
						for j, id := range r {
							require.Equal(t, sorted[(start+j)%len(sorted)], id, name)
						}
						if p > 0 {
							prev := slices.Index(sorted, got[p-1][0])
							require.Equal(t, (prev+1)%len(sorted), start, name)
						}
						leaders[r[0]]++
					}
					// Leaders are balanced: counts differ by at most one.
					if int(n) >= len(ids) {
						lo, hi := int(n), 0
						for _, id := range ids {
							lo = min(lo, leaders[id])
							hi = max(hi, leaders[id])
						}
						require.LessOrEqual(t, hi-lo, 1, name)
					}
				}
			}
		}
	}
}

func TestAssignReplicasInputOrderIndependent(t *testing.T) {
	t.Parallel()
	a, err := metadata.AssignReplicas("orders", []int32{3, 1, 2}, 6, 2)
	require.NoError(t, err)
	b, err := metadata.AssignReplicas("orders", []int32{1, 2, 3}, 6, 2)
	require.NoError(t, err)
	require.Equal(t, a, b)

	ids := []int32{3, 1, 2}
	_, err = metadata.AssignReplicas("orders", ids, 1, 1)
	require.NoError(t, err)
	require.Equal(t, []int32{3, 1, 2}, ids, "input not mutated")
}

func TestAssignReplicasErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		ids     []int32
		n, rf   int32
		wantErr error
	}{
		{name: "rf zero", ids: []int32{1}, n: 1, rf: 0, wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "rf too large", ids: []int32{1, 2}, n: 1, rf: 3, wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "no brokers", ids: nil, n: 1, rf: 1, wantErr: metadata.ErrInvalidReplicationFactor},
		{name: "zero partitions", ids: []int32{1}, n: 0, rf: 1, wantErr: metadata.ErrInvalidCommand},
		{name: "negative partitions", ids: []int32{1}, n: -1, rf: 1, wantErr: metadata.ErrInvalidCommand},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := metadata.AssignReplicas("t", tc.ids, tc.n, tc.rf)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestValidateTopicName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		valid bool
	}{
		{"a", true},
		{"Orders_2024.v1-x", true},
		{strings.Repeat("z", 249), true},
		{"...", true},
		{"", false},
		{".", false},
		{"..", false},
		{strings.Repeat("z", 250), false},
		{"has space", false},
		{"slash/x", false},
		{"ümlaut", false},
		{"colon:x", false},
	}
	for _, tc := range tests {
		err := metadata.ValidateTopicName(tc.name)
		if tc.valid {
			require.NoError(t, err, tc.name)
		} else {
			require.ErrorIs(t, err, metadata.ErrInvalidTopic, tc.name)
		}
	}
}

func uniq(ids []int32) []int32 {
	s := slices.Sorted(slices.Values(ids))
	return slices.Compact(s)
}

func TestAllocateProducerIDs(t *testing.T) {
	t.Parallel()
	s := baseState(t)
	alloc := func(b int32) error {
		_, err := s.Apply(&streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_AllocateProducerIds{
			AllocateProducerIds: &streamqv1.AllocateProducerIDsCommand{BrokerId: b},
		}})
		return err
	}
	require.ErrorIs(t, alloc(9), metadata.ErrInvalidCommand, "unregistered broker")
	require.Zero(t, s.NextProducerID)
	require.NoError(t, alloc(1))
	require.NoError(t, alloc(2))
	require.NoError(t, alloc(1))
	require.Equal(t, map[int32]int64{1: 2000, 2: 1000}, s.ProducerIDBlocks)
	require.Equal(t, int64(3000), s.NextProducerID)
}
