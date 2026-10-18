package storage

import "context"

// Checkpoint files use Kafka's text format: line 1 the version "0", line 2 the entry count, then
// one entry per line. Writes are atomic: write "<path>.tmp", fsync, rename over path, fsync the
// directory. Entries are written in sorted order.

// EpochEntry is one leader-epoch-checkpoint line: the first offset written in an epoch.
type EpochEntry struct {
	Epoch       int32
	StartOffset int64
}

// WriteOffsetCheckpoint atomically writes "topic partition offset" lines.
func WriteOffsetCheckpoint(ctx context.Context, path string, offsets map[TopicPartition]int64) error {
	_, _, _ = ctx, path, offsets
	return errNotImplemented
}

// ReadOffsetCheckpoint reads an offset checkpoint. A missing file yields an empty map; a
// malformed file yields ErrCorruptCheckpoint.
func ReadOffsetCheckpoint(ctx context.Context, path string) (map[TopicPartition]int64, error) {
	_, _ = ctx, path
	return nil, errNotImplemented
}

// WriteLeaderEpochCheckpoint atomically writes "epoch startOffset" lines in the given order.
func WriteLeaderEpochCheckpoint(ctx context.Context, path string, entries []EpochEntry) error {
	_, _, _ = ctx, path, entries
	return errNotImplemented
}

// ReadLeaderEpochCheckpoint reads a leader-epoch checkpoint. A missing file yields no entries; a
// malformed file yields ErrCorruptCheckpoint.
func ReadLeaderEpochCheckpoint(ctx context.Context, path string) ([]EpochEntry, error) {
	_, _ = ctx, path
	return nil, errNotImplemented
}
