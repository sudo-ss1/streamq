package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetCheckpointRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		offsets map[TopicPartition]int64
	}{
		{"empty", map[TopicPartition]int64{}},
		{"single", map[TopicPartition]int64{{Topic: "orders", Partition: 0}: 42}},
		{"many with dashes", map[TopicPartition]int64{
			{Topic: "a-b-c", Partition: 3}:    7,
			{Topic: "a-b-c", Partition: 10}:   0,
			{Topic: "orders", Partition: 1}:   1 << 40,
			{Topic: "orders-2", Partition: 0}: 5,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "recovery-point-offset-checkpoint")
			require.NoError(t, WriteOffsetCheckpoint(ctx, path, tc.offsets))
			got, err := ReadOffsetCheckpoint(ctx, path)
			require.NoError(t, err)
			require.Equal(t, tc.offsets, got)
			_, err = os.Stat(path + ".tmp")
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestOffsetCheckpointSortedOutput(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cp")
	require.NoError(t, WriteOffsetCheckpoint(ctx, path, map[TopicPartition]int64{
		{Topic: "b", Partition: 0}:      1,
		{Topic: "a-b-c", Partition: 10}: 2,
		{Topic: "a-b-c", Partition: 2}:  3,
		{Topic: "a", Partition: 1}:      4,
	}))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "0\n4\na 1 4\na-b-c 2 3\na-b-c 10 2\nb 0 1\n", string(got))
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0), fi.Mode().Perm()&^os.FileMode(fileMode))

	// Overwriting replaces the file.
	require.NoError(t, WriteOffsetCheckpoint(ctx, path, map[TopicPartition]int64{{Topic: "z", Partition: 0}: 9}))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "0\n1\nz 0 9\n", string(got))
}

func TestOffsetCheckpointInvalidTopic(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cp")
	for _, topic := range []string{"", "has space", "new\nline"} {
		err := WriteOffsetCheckpoint(ctx, path, map[TopicPartition]int64{{Topic: topic}: 1})
		require.Error(t, err, topic)
	}
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCheckpointMissingAndStrayTmp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "cp")

	offsets, err := ReadOffsetCheckpoint(ctx, path)
	require.NoError(t, err)
	require.NotNil(t, offsets)
	require.Empty(t, offsets)
	epochs, err := ReadLeaderEpochCheckpoint(ctx, path)
	require.NoError(t, err)
	require.Nil(t, epochs)

	// A stray .tmp left by a crash mid-write is ignored by reads and replaced by the next write.
	require.NoError(t, os.WriteFile(path+".tmp", []byte("garbage"), fileMode))
	offsets, err = ReadOffsetCheckpoint(ctx, path)
	require.NoError(t, err)
	require.Empty(t, offsets)

	require.NoError(t, WriteOffsetCheckpoint(ctx, path, map[TopicPartition]int64{{Topic: "t", Partition: 1}: 5}))
	require.NoError(t, os.WriteFile(path+".tmp", []byte("garbage"), fileMode))
	offsets, err = ReadOffsetCheckpoint(ctx, path)
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{{Topic: "t", Partition: 1}: 5}, offsets)
}

func TestOffsetCheckpointCorrupt(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"empty file", ""},
		{"no trailing newline", "0\n1\nt 0 5"},
		{"bad version", "1\n1\nt 0 5\n"},
		{"missing count", "0\n"},
		{"non-numeric count", "0\nx\n"},
		{"count too high", "0\n2\nt 0 5\n"},
		{"count too low", "0\n0\nt 0 5\n"},
		{"negative count", "0\n-1\n"},
		{"too few fields", "0\n1\nt 5\n"},
		{"too many fields", "0\n1\nt 0 5 6\n"},
		{"double space", "0\n1\nt  0 5\n"},
		{"empty topic", "0\n1\n 0 5\n"},
		{"bad partition", "0\n1\nt x 5\n"},
		{"negative partition", "0\n1\nt -1 5\n"},
		{"bad offset", "0\n1\nt 0 five\n"},
		{"duplicate entry", "0\n2\nt 0 5\nt 0 6\n"},
		{"blank line", "0\n1\n\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cp")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), fileMode))
			_, err := ReadOffsetCheckpoint(context.Background(), path)
			require.ErrorIs(t, err, ErrCorruptCheckpoint)
		})
	}
}

func TestLeaderEpochCheckpointRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		entries []EpochEntry
	}{
		{"empty", nil},
		{"single", []EpochEntry{{Epoch: 0, StartOffset: 0}}},
		{"order preserved", []EpochEntry{{Epoch: 3, StartOffset: 10}, {Epoch: 5, StartOffset: 20}, {Epoch: 9, StartOffset: 1 << 40}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "leader-epoch-checkpoint")
			require.NoError(t, WriteLeaderEpochCheckpoint(ctx, path, tc.entries))
			got, err := ReadLeaderEpochCheckpoint(ctx, path)
			require.NoError(t, err)
			require.Equal(t, tc.entries, got)
		})
	}

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "leader-epoch-checkpoint")
	require.NoError(t, WriteLeaderEpochCheckpoint(ctx, path, []EpochEntry{{Epoch: 2, StartOffset: 4}, {Epoch: 7, StartOffset: 9}}))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "0\n2\n2 4\n7 9\n", string(got))
}

func TestLeaderEpochCheckpointCorrupt(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"empty file", ""},
		{"bad version", "1\n0\n"},
		{"count mismatch", "0\n2\n1 5\n"},
		{"too many fields", "0\n1\n1 5 6\n"},
		{"too few fields", "0\n1\n1\n"},
		{"bad epoch", "0\n1\nx 5\n"},
		{"negative epoch", "0\n1\n-1 5\n"},
		{"epoch overflow", "0\n1\n4294967296 5\n"},
		{"bad offset", "0\n1\n1 y\n"},
		{"negative offset", "0\n1\n1 -5\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cp")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), fileMode))
			_, err := ReadLeaderEpochCheckpoint(context.Background(), path)
			require.ErrorIs(t, err, ErrCorruptCheckpoint)
		})
	}
}

func TestCheckpointContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "cp")
	require.ErrorIs(t, WriteOffsetCheckpoint(ctx, path, nil), context.Canceled)
	require.ErrorIs(t, WriteLeaderEpochCheckpoint(ctx, path, nil), context.Canceled)
	_, err := ReadOffsetCheckpoint(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	_, err = ReadLeaderEpochCheckpoint(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestCheckpointWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-dir", "cp")
	require.Error(t, WriteOffsetCheckpoint(context.Background(), path, nil))
	require.Error(t, WriteLeaderEpochCheckpoint(context.Background(), path, nil))
}
