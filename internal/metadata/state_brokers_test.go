package metadata

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

func registerCmd(id int32) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_RegisterBroker{
		RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: id, Host: "h", Port: 9000 + id, InternalAddr: "i"},
	}}
}

func fenceCmdE(id int32, epoch int64) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_FenceBroker{
		FenceBroker: &streamqv1.FenceBrokerCommand{BrokerId: id, BrokerEpoch: epoch},
	}}
}

func unfenceCmdE(id int32, epoch int64) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_UnfenceBroker{
		UnfenceBroker: &streamqv1.UnfenceBrokerCommand{BrokerId: id, BrokerEpoch: epoch},
	}}
}

// epoch returns broker id's current registration epoch in s (0 if unknown).
func epoch(s *State, id int32) int64 {
	if i := s.brokerIndex(id); i >= 0 {
		return s.Brokers[i].Epoch
	}
	return 0
}

// fence and unfence target the broker's current incarnation.
func fence(s *State, id int32) *streamqv1.MetadataCommand   { return fenceCmdE(id, epoch(s, id)) }
func unfence(s *State, id int32) *streamqv1.MetadataCommand { return unfenceCmdE(id, epoch(s, id)) }

// clusterState returns a state with brokers 1..3 registered and unfenced and topic "t" with
// partition 0 replicas [1 2 3] and partition 1 replicas [2 3 1].
func clusterState(t *testing.T) *State {
	t.Helper()
	s := NewState(nil, 0)
	for id := int32(1); id <= 3; id++ {
		mustApply(t, s, registerCmd(id))
		mustApply(t, s, unfence(s, id))
	}
	mustApply(t, s, createTopicCmd("t", [][]int32{{1, 2, 3}, {2, 3, 1}}))
	return s
}

func createTopicCmd(name string, replicas [][]int32) *streamqv1.MetadataCommand {
	var parts []*streamqv1.PartitionAssignment
	for i, r := range replicas {
		parts = append(parts, &streamqv1.PartitionAssignment{Partition: int32(i), Replicas: r})
	}
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: name, TopicId: "id-" + name, Partitions: parts},
	}}
}

func mustApply(t *testing.T, s *State, cmd *streamqv1.MetadataCommand) []Change {
	t.Helper()
	ch, err := s.Apply(cmd)
	require.NoError(t, err)
	return ch
}

func part(s *State, p int) PartitionState { return s.Topics["t"].Partitions[p] }

func TestRegisterBroker(t *testing.T) {
	s := NewState(nil, 0)
	ch := mustApply(t, s, registerCmd(2))
	require.Equal(t, BrokersChanged, ch[0].Kind)
	mustApply(t, s, registerCmd(1))
	snap := s.Snapshot()
	require.Equal(t, []int32{1, 2}, []int32{snap.Brokers[0].ID, snap.Brokers[1].ID}, "sorted by id")
	require.True(t, snap.Brokers[0].Fenced, "registered fenced")
	require.Empty(t, snap.LiveBrokers())
	mustApply(t, s, registerCmd(1))
	b, ok := s.Snapshot().Broker(1)
	require.True(t, ok)
	require.Equal(t, int64(2), b.Epoch)
	for _, bad := range []*streamqv1.MetadataCommand{
		{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_RegisterBroker{RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: 0, Host: "h", Port: 1}}},
		{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_RegisterBroker{RegisterBroker: &streamqv1.RegisterBrokerCommand{BrokerId: 5, Port: 1}}},
		fenceCmdE(9, 1), unfenceCmdE(9, 1),
	} {
		_, err := s.Apply(bad)
		require.ErrorIs(t, err, ErrInvalidCommand)
	}
}

func TestFenceElectsNextISRMemberInReplicaOrder(t *testing.T) {
	s := clusterState(t)
	ch := mustApply(t, s, fence(s, 1))
	p0, p1 := part(s, 0), part(s, 1)
	require.Equal(t, int32(2), p0.Leader, "first live ISR member in replica order")
	require.Equal(t, int32(1), p0.LeaderEpoch)
	require.Equal(t, []int32{2, 3}, p0.ISR)
	require.Equal(t, int32(1), p0.PartitionEpoch)
	require.Equal(t, int32(2), p1.Leader, "leader unchanged")
	require.Equal(t, int32(0), p1.LeaderEpoch, "no leader change, no leader epoch bump")
	require.Equal(t, []int32{2, 3}, p1.ISR)
	require.Equal(t, int32(1), p1.PartitionEpoch, "ISR change bumps the partition epoch")
	require.Len(t, ch, 2)
	require.Equal(t, PartitionChanged, ch[1].Kind)
	require.Len(t, ch[1].Partitions, 2)

	// Fencing again is a no-op.
	require.Empty(t, mustApply(t, s, fence(s, 1)))
}

func TestFenceAndUnfenceIgnoreStaleEpochs(t *testing.T) {
	s := clusterState(t)
	old := epoch(s, 2)
	mustApply(t, s, registerCmd(2)) // broker 2 restarted: epoch old+1, fenced
	mustApply(t, s, unfence(s, 2))
	require.Empty(t, mustApply(t, s, fenceCmdE(2, old)), "a decision about the old incarnation is a no-op")
	b, _ := s.Snapshot().Broker(2)
	require.False(t, b.Fenced)

	mustApply(t, s, registerCmd(3)) // fenced, new epoch
	require.Empty(t, mustApply(t, s, unfenceCmdE(3, epoch(s, 3)-1)), "stale unfence is a no-op")
	b, _ = s.Snapshot().Broker(3)
	require.True(t, b.Fenced)
	require.Empty(t, mustApply(t, s, unfenceCmdE(2, epoch(s, 2))), "unfencing an unfenced broker is a no-op")
}

func TestFenceLastISRMemberMakesPartitionOffline(t *testing.T) {
	s := clusterState(t)
	mustApply(t, s, fence(s, 2))
	mustApply(t, s, fence(s, 3))
	p1 := part(s, 1) // replicas [2 3 1]
	require.Equal(t, int32(1), p1.Leader)
	mustApply(t, s, fence(s, 1))
	p1 = part(s, 1)
	require.Equal(t, NoLeader, p1.Leader, "no live ISR member: offline, unclean election disabled")
	require.Equal(t, []int32{1}, p1.ISR, "the last ISR member stays (D34)")

	// Unfencing a broker that is not in the ISR does not elect it.
	mustApply(t, s, unfence(s, 2))
	require.Equal(t, NoLeader, part(s, 1).Leader)
	// Unfencing the ISR member brings the partition back.
	le := part(s, 1).LeaderEpoch
	ch := mustApply(t, s, unfence(s, 1))
	require.Equal(t, int32(1), part(s, 1).Leader)
	require.Equal(t, le+1, part(s, 1).LeaderEpoch)
	require.Equal(t, PartitionChanged, ch[len(ch)-1].Kind)
}

func TestReRegisterGivesUpLeadership(t *testing.T) {
	s := clusterState(t)
	mustApply(t, s, registerCmd(2)) // broker 2 restarted
	require.Equal(t, int32(3), part(s, 1).Leader)
	require.Equal(t, int32(1), part(s, 1).LeaderEpoch)
	require.NotContains(t, part(s, 1).ISR, int32(2))
	b, _ := s.Snapshot().Broker(2)
	require.True(t, b.Fenced)
}

func TestCreateTopicLeaderIsFirstUnfencedReplica(t *testing.T) {
	s := clusterState(t)
	mustApply(t, s, fence(s, 1))
	mustApply(t, s, createTopicCmd("u", [][]int32{{1, 2}}))
	ps := s.Topics["u"].Partitions[0]
	require.Equal(t, int32(2), ps.Leader)
	require.Equal(t, []int32{2}, ps.ISR, "fenced replicas start outside the ISR")
	mustApply(t, s, fence(s, 2))
	_, err := s.Apply(createTopicCmd("v", [][]int32{{1, 2}}))
	require.ErrorIs(t, err, ErrInvalidReplicationFactor)
}

func TestBrokerCommandsAreDeterministic(t *testing.T) {
	a, b := clusterState(t), clusterState(t)
	steps := []func(s *State) *streamqv1.MetadataCommand{
		func(s *State) *streamqv1.MetadataCommand { return fence(s, 1) },
		func(*State) *streamqv1.MetadataCommand { return registerCmd(1) },
		func(s *State) *streamqv1.MetadataCommand { return fence(s, 3) },
		func(s *State) *streamqv1.MetadataCommand { return unfence(s, 1) },
		func(s *State) *streamqv1.MetadataCommand { return fence(s, 2) },
		func(s *State) *streamqv1.MetadataCommand { return unfence(s, 3) },
	}
	for _, step := range steps {
		_, errA := a.Apply(step(a))
		_, errB := b.Apply(step(b))
		require.Equal(t, errA, errB)
	}
	ja, err := json.Marshal(a)
	require.NoError(t, err)
	jb, err := json.Marshal(b)
	require.NoError(t, err)
	require.JSONEq(t, string(ja), string(jb))
}

func TestTermBoundCommands(t *testing.T) {
	s := clusterState(t)
	cmd := fence(s, 1)
	cmd.ControllerEpoch = 4
	_, err := s.ApplyInTerm(cmd, 5)
	require.ErrorIs(t, err, ErrNotController)
	b, _ := s.Snapshot().Broker(1)
	require.False(t, b.Fenced, "rejected: state unchanged")
	_, err = s.ApplyInTerm(cmd, 4)
	require.NoError(t, err)
	b, _ = s.Snapshot().Broker(1)
	require.True(t, b.Fenced)
	// Untagged commands apply in any term.
	_, err = s.ApplyInTerm(unfence(s, 1), 9)
	require.NoError(t, err)
}

func TestDeleteWithStaleTopicIDKeepsRecreatedTopic(t *testing.T) {
	s := clusterState(t)
	del := func(id string) error {
		_, err := s.Apply(&streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_DeleteTopic{
			DeleteTopic: &streamqv1.DeleteTopicCommand{Name: "t", TopicId: id},
		}})
		return err
	}
	require.NoError(t, del("id-t"))
	mustApply(t, s, &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "id-new", Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}})
	require.ErrorIs(t, del("id-t"), ErrUnknownTopic, "a retried delete of the old topic leaves the new one")
	require.Equal(t, "id-new", s.Topics["t"].ID)
	require.NoError(t, del(""), "an unscoped delete still works")
}

func alterISR(epoch, pepoch int32, isr ...int32) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_AlterIsr{
		AlterIsr: &streamqv1.AlterISRCommand{Topic: "t", Partition: 0, LeaderEpoch: epoch, PartitionEpoch: pepoch, NewIsr: isr},
	}}
}

func TestAlterISR(t *testing.T) {
	s := clusterState(t) // t/0: replicas [1 2 3], leader 1, ISR [1 2 3], epochs (0, 0)
	ch := mustApply(t, s, alterISR(0, 0, 1, 3))
	require.Equal(t, []int32{1, 3}, part(s, 0).ISR)
	require.Equal(t, int32(1), part(s, 0).PartitionEpoch)
	require.Equal(t, int32(0), part(s, 0).LeaderEpoch)
	require.Equal(t, PartitionChanged, ch[0].Kind)
	mustApply(t, s, alterISR(0, 1, 3, 2, 1)) // any order; stored in replica order
	require.Equal(t, []int32{1, 2, 3}, part(s, 0).ISR)

	for name, tc := range map[string]struct {
		cmd  *streamqv1.MetadataCommand
		want error
	}{
		"stale partition epoch": {alterISR(0, 1, 1, 2), ErrFencedLeaderEpoch},
		"stale leader epoch":    {alterISR(9, 2, 1, 2), ErrFencedLeaderEpoch},
		"missing leader":        {alterISR(0, 2, 2, 3), ErrInvalidISR},
		"empty":                 {alterISR(0, 2), ErrInvalidISR},
		"not a replica":         {alterISR(0, 2, 1, 7), ErrInvalidISR},
		"duplicate":             {alterISR(0, 2, 1, 1), ErrInvalidISR},
	} {
		_, err := s.Apply(tc.cmd)
		require.ErrorIs(t, err, tc.want, name)
	}
	mustApply(t, s, fence(s, 3)) // epochs bump: ISR [1 2], partition epoch 3
	_, err := s.Apply(alterISR(0, 3, 1, 2, 3))
	require.ErrorIs(t, err, ErrInvalidISR, "a fenced broker cannot rejoin the ISR")
}
