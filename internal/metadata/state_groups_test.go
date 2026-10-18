package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

func commitCmd(group string, offs ...*streamqv1.OffsetCommit) *streamqv1.MetadataCommand {
	return &streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: group, Offsets: offs},
	}}
}

func off(topic string, p int32, o int64) *streamqv1.OffsetCommit {
	return &streamqv1.OffsetCommit{Topic: topic, Partition: p, Offset: o}
}

func stateWithTopics(t *testing.T) *State {
	t.Helper()
	s := NewState([]Broker{{ID: 1}}, 1)
	for _, name := range []string{"a", "b"} {
		_, err := s.Apply(&streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
			CreateTopic: &streamqv1.CreateTopicCommand{Name: name, TopicId: "id-" + name, Partitions: []*streamqv1.PartitionAssignment{
				{Partition: 0, Replicas: []int32{1}}, {Partition: 1, Replicas: []int32{1}},
			}},
		}})
		require.NoError(t, err)
	}
	return s
}

func TestApplyCommitOffsets(t *testing.T) {
	s := stateWithTopics(t)
	changes, err := s.Apply(commitCmd("g", off("a", 0, 5), off("a", 1, 7), off("b", 0, 1)))
	require.NoError(t, err)
	require.Equal(t, []Change{{Kind: OffsetsCommitted, Group: "g"}}, changes)
	_, err = s.Apply(commitCmd("g", off("a", 0, 9)))
	require.NoError(t, err)
	snap := s.Snapshot()
	got, ok := snap.Committed("g", "a", 0)
	require.True(t, ok)
	require.Equal(t, int64(9), got)
	got, _ = snap.Committed("g", "a", 1)
	require.Equal(t, int64(7), got)
	_, ok = snap.Committed("other", "a", 0)
	require.False(t, ok)

	// Snapshot isolation.
	snap.Groups["g"]["a"][0] = 100
	again, _ := s.Snapshot().Committed("g", "a", 0)
	require.Equal(t, int64(9), again)

	invalid := []*streamqv1.MetadataCommand{
		commitCmd("", off("a", 0, 1)),
		commitCmd("g"),
		commitCmd("g", off("a", 0, -1)),
		commitCmd("g", off("missing", 0, 1)),
		commitCmd("g", off("a", 2, 1)),
		commitCmd("g", off("a", 0, 50), off("a", 9, 1)), // second invalid: nothing applied
	}
	for _, cmd := range invalid {
		_, err := s.Apply(cmd)
		require.Error(t, err)
	}
	unchanged, _ := s.Snapshot().Committed("g", "a", 0)
	require.Equal(t, int64(9), unchanged)
}

func TestDeleteTopicDropsCommittedOffsets(t *testing.T) {
	s := stateWithTopics(t)
	_, err := s.Apply(commitCmd("g", off("a", 0, 5), off("b", 0, 1)))
	require.NoError(t, err)
	_, err = s.Apply(commitCmd("only-a", off("a", 1, 3)))
	require.NoError(t, err)
	_, err = s.Apply(&streamqv1.MetadataCommand{Version: CommandVersion, Cmd: &streamqv1.MetadataCommand_DeleteTopic{
		DeleteTopic: &streamqv1.DeleteTopicCommand{Name: "a"},
	}})
	require.NoError(t, err)
	snap := s.Snapshot()
	_, ok := snap.Committed("g", "a", 0)
	require.False(t, ok)
	got, ok := snap.Committed("g", "b", 0)
	require.True(t, ok)
	require.Equal(t, int64(1), got)
	require.NotContains(t, snap.Groups, "only-a", "a group with no offsets left is removed")
}

func TestCloneCopiesGroups(t *testing.T) {
	s := stateWithTopics(t)
	_, err := s.Apply(commitCmd("g", off("a", 0, 5)))
	require.NoError(t, err)
	c := s.Clone()
	_, err = c.Apply(commitCmd("g", off("a", 0, 6)))
	require.NoError(t, err)
	got, _ := s.Snapshot().Committed("g", "a", 0)
	require.Equal(t, int64(5), got)
}
