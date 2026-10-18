package local

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/metadata"
)

func TestCommittedOffsetsPersist(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	self := metadata.Broker{ID: 1, Host: "h", Port: 1}
	s, err := Open(ctx, dir, self, nil)
	require.NoError(t, err)
	require.NoError(t, s.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "x", Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}}))
	require.NoError(t, s.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: "g", Offsets: []*streamqv1.OffsetCommit{{Topic: "t", Offset: 42}}},
	}}))
	require.NoError(t, s.Close())
	s, err = Open(ctx, dir, self, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	got, ok := s.Snapshot().Committed("g", "t", 0)
	require.True(t, ok)
	require.Equal(t, int64(42), got)
}

func TestLoadPreM4MetadataWithoutGroups(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	old := `{"brokers":[{"id":1,"host":"h","port":1}],"controller_id":1,"topics":{"t":{"name":"t","id":"x","configs":{},` +
		`"partitions":[{"partition":0,"replicas":[1],"leader":1,"leader_epoch":0,"isr":[1],"partition_epoch":0}]}}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, FileName), []byte(old), 0o600))
	s, err := Open(ctx, dir, metadata.Broker{ID: 1, Host: "h", Port: 1}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()
	require.Empty(t, s.Snapshot().Groups)
	require.NoError(t, s.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: "g", Offsets: []*streamqv1.OffsetCommit{{Topic: "t", Offset: 3}}},
	}}))
	got, ok := s.Snapshot().Committed("g", "t", 0)
	require.True(t, ok)
	require.Equal(t, int64(3), got)
}

func TestPersistedJSONKeys(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(ctx, dir, metadata.Broker{ID: 1, Host: "h", Port: 1}, nil)
	require.NoError(t, err)
	require.NoError(t, s.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CreateTopic{
		CreateTopic: &streamqv1.CreateTopicCommand{Name: "t", TopicId: "x", Configs: map[string]string{"retention_ms": "1"},
			Partitions: []*streamqv1.PartitionAssignment{{Partition: 0, Replicas: []int32{1}}}},
	}}))
	require.NoError(t, s.Propose(ctx, &streamqv1.MetadataCommand{Version: metadata.CommandVersion, Cmd: &streamqv1.MetadataCommand_CommitOffsets{
		CommitOffsets: &streamqv1.CommitOffsetsCommand{GroupId: "g", Offsets: []*streamqv1.OffsetCommit{{Topic: "t", Offset: 3}}},
	}}))
	require.NoError(t, s.Close())
	data, err := os.ReadFile(filepath.Join(dir, FileName))
	require.NoError(t, err)
	var raw struct {
		Topics map[string]map[string]json.RawMessage `json:"topics"`
		Groups map[string]map[string]map[string]int64 `json:"groups"`
	}
	require.NoError(t, json.Unmarshal(data, &raw))
	keys := make([]string, 0)
	for k := range raw.Topics["t"] {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"name", "id", "configs", "partitions"}, keys, "persisted topic keys are stable")
	require.Equal(t, int64(3), raw.Groups["g"]["t"]["0"])
}
