package storage

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Checkpoint files use Kafka's text format: line 1 the version "0", line 2 the entry count, then
// one entry per line. Writes are atomic: write "<path>.tmp", fsync, rename over path, fsync the
// directory. Entries are written in sorted order.

// checkpointVersion is the only checkpoint format version.
const checkpointVersion = "0"

// EpochEntry is one leader-epoch-checkpoint line: the first offset written in an epoch.
type EpochEntry struct {
	Epoch       int32
	StartOffset int64
}

// WriteOffsetCheckpoint atomically writes "topic partition offset" lines.
func WriteOffsetCheckpoint(ctx context.Context, path string, offsets map[TopicPartition]int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	keys := make([]TopicPartition, 0, len(offsets))
	for tp := range offsets {
		if tp.Topic == "" || strings.ContainsAny(tp.Topic, " \t\r\n") {
			return fmt.Errorf("storage: checkpoint %s: invalid topic name %q", path, tp.Topic)
		}
		keys = append(keys, tp)
	}
	slices.SortFunc(keys, func(a, b TopicPartition) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		return cmp.Compare(a.Partition, b.Partition)
	})
	var buf bytes.Buffer
	writeCheckpointHeader(&buf, len(keys))
	for _, tp := range keys {
		fmt.Fprintf(&buf, "%s %d %d\n", tp.Topic, tp.Partition, offsets[tp])
	}
	return writeCheckpointFile(path, buf.Bytes())
}

// ReadOffsetCheckpoint reads an offset checkpoint. A missing file yields an empty map; a
// malformed file yields ErrCorruptCheckpoint.
func ReadOffsetCheckpoint(ctx context.Context, path string) (map[TopicPartition]int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lines, err := readCheckpointLines(path)
	if err != nil {
		return nil, err
	}
	out := make(map[TopicPartition]int64, len(lines))
	for k, line := range lines {
		f := strings.Split(line, " ")
		if len(f) != 3 || f[0] == "" {
			return nil, corruptCheckpoint(path, k, line)
		}
		p, err := strconv.ParseInt(f[1], 10, 32)
		if err != nil || p < 0 {
			return nil, corruptCheckpoint(path, k, line)
		}
		off, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil {
			return nil, corruptCheckpoint(path, k, line)
		}
		tp := TopicPartition{Topic: f[0], Partition: int32(p)}
		if _, dup := out[tp]; dup {
			return nil, corruptCheckpoint(path, k, line)
		}
		out[tp] = off
	}
	return out, nil
}

// WriteLeaderEpochCheckpoint atomically writes "epoch startOffset" lines in the given order.
func WriteLeaderEpochCheckpoint(ctx context.Context, path string, entries []EpochEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var buf bytes.Buffer
	writeCheckpointHeader(&buf, len(entries))
	for _, e := range entries {
		fmt.Fprintf(&buf, "%d %d\n", e.Epoch, e.StartOffset)
	}
	return writeCheckpointFile(path, buf.Bytes())
}

// ReadLeaderEpochCheckpoint reads a leader-epoch checkpoint. A missing file yields no entries; a
// malformed file yields ErrCorruptCheckpoint.
func ReadLeaderEpochCheckpoint(ctx context.Context, path string) ([]EpochEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lines, err := readCheckpointLines(path)
	if err != nil {
		return nil, err
	}
	var out []EpochEntry
	for k, line := range lines {
		f := strings.Split(line, " ")
		if len(f) != 2 {
			return nil, corruptCheckpoint(path, k, line)
		}
		epoch, err := strconv.ParseInt(f[0], 10, 32)
		if err != nil || epoch < 0 {
			return nil, corruptCheckpoint(path, k, line)
		}
		start, err := strconv.ParseInt(f[1], 10, 64)
		if err != nil || start < 0 {
			return nil, corruptCheckpoint(path, k, line)
		}
		out = append(out, EpochEntry{Epoch: int32(epoch), StartOffset: start})
	}
	return out, nil
}

func writeCheckpointHeader(buf *bytes.Buffer, count int) {
	buf.WriteString(checkpointVersion)
	buf.WriteByte('\n')
	buf.WriteString(strconv.Itoa(count))
	buf.WriteByte('\n')
}

func corruptCheckpoint(path string, entry int, line string) error {
	return fmt.Errorf("storage: checkpoint %s: malformed entry %d %q: %w", path, entry, line, ErrCorruptCheckpoint)
}

// readCheckpointLines reads path, validates the version and count header and returns the entry
// lines. A missing file yields no lines.
func readCheckpointLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: read checkpoint %s: %w", path, err)
	}
	s, ok := strings.CutSuffix(string(data), "\n")
	if !ok {
		return nil, fmt.Errorf("storage: checkpoint %s: missing trailing newline: %w", path, ErrCorruptCheckpoint)
	}
	lines := strings.Split(s, "\n")
	if len(lines) < 2 || lines[0] != checkpointVersion {
		return nil, fmt.Errorf("storage: checkpoint %s: bad version: %w", path, ErrCorruptCheckpoint)
	}
	count, err := strconv.Atoi(lines[1])
	if err != nil || count < 0 || count != len(lines)-2 {
		return nil, fmt.Errorf("storage: checkpoint %s: count %q does not match %d entries: %w",
			path, lines[1], len(lines)-2, ErrCorruptCheckpoint)
	}
	return lines[2:], nil
}

// writeCheckpointFile writes data to path+".tmp", fsyncs it, renames it over path and fsyncs the
// parent directory.
func writeCheckpointFile(path string, data []byte) (err error) {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("storage: create %s: %w", tmp, err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return errors.Join(fmt.Errorf("storage: write %s: %w", tmp, err), f.Close())
	}
	if err = f.Sync(); err != nil {
		return errors.Join(fmt.Errorf("storage: fsync %s: %w", tmp, err), f.Close())
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("storage: close %s: %w", tmp, err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("storage: rename %s: %w", tmp, err)
	}
	return syncCheckpointDir(filepath.Dir(path))
}

// syncCheckpointDir fsyncs a directory so a rename or create inside it is durable.
func syncCheckpointDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open dir %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		return errors.Join(fmt.Errorf("storage: fsync dir %s: %w", dir, err), d.Close())
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("storage: close dir %s: %w", dir, err)
	}
	return nil
}

// WriteFileAtomic durably replaces path with data (tmp file, fsync, rename, dir fsync). Other
// packages use it for their own per-partition files (producer-state snapshots).
func WriteFileAtomic(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return writeCheckpointFile(path, data)
}

// SyncDir fsyncs dir so creates, renames and unlinks inside it are durable.
func SyncDir(dir string) error { return syncCheckpointDir(dir) }
