package partition

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"

	"github.com/sudo-ss1/streamq/internal/storage"
)

// EpochCheckpointFile is the per-partition leader-epoch checkpoint (SPEC §3.1).
const EpochCheckpointFile = "leader-epoch-checkpoint"

// EpochCache maps leader epochs to the first offset written in each (SPEC §4.4). Entries are
// ascending in both epoch and start offset. Every change is persisted to the checkpoint file
// before the method returns.
type EpochCache struct {
	mu      sync.Mutex
	path    string
	entries []storage.EpochEntry
}

// OpenEpochCache loads dir's checkpoint (an empty cache if missing) and drops entries at or after
// leo (a crash may leave the log shorter than the checkpoint).
func OpenEpochCache(ctx context.Context, dir string, leo int64) (*EpochCache, error) {
	path := filepath.Join(dir, EpochCheckpointFile)
	entries, err := storage.ReadLeaderEpochCheckpoint(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read leader epochs: %w", err)
	}
	c := &EpochCache{path: path, entries: entries}
	if err := c.TruncateFromEnd(ctx, leo); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *EpochCache) persist(ctx context.Context) error {
	if err := storage.WriteLeaderEpochCheckpoint(ctx, c.path, c.entries); err != nil {
		return fmt.Errorf("write leader epochs: %w", err)
	}
	return nil
}

// Assign records that epoch starts at startOffset. It is a no-op unless epoch is newer than the
// latest cached epoch.
func (c *EpochCache) Assign(ctx context.Context, epoch int32, startOffset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := len(c.entries); n > 0 && epoch <= c.entries[n-1].Epoch {
		return nil
	}
	// A consistent log never assigns below an existing start offset; drop any such entries.
	c.entries = slices.DeleteFunc(c.entries, func(e storage.EpochEntry) bool { return e.StartOffset > startOffset })
	c.entries = append(c.entries, storage.EpochEntry{Epoch: epoch, StartOffset: startOffset})
	return c.persist(ctx)
}

// LatestEpoch returns the newest cached epoch.
func (c *EpochCache) LatestEpoch() (int32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 {
		return -1, false
	}
	return c.entries[len(c.entries)-1].Epoch, true
}

// StartOffset returns the start offset of epoch, if cached.
func (c *EpochCache) StartOffset(epoch int32) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		if e.Epoch == epoch {
			return e.StartOffset, true
		}
	}
	return -1, false
}

// EndOffsetFor returns, for a follower's requested epoch, the largest cached epoch <= epoch and
// that epoch's end offset: the start offset of the next cached epoch, or leo if it is the
// latest (Kafka's OffsetsForLeaderEpoch). It returns (-1, -1) if epoch is below every entry.
func (c *EpochCache) EndOffsetFor(epoch int32, leo int64) (int32, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := len(c.entries) - 1
	for i >= 0 && c.entries[i].Epoch > epoch {
		i--
	}
	if i < 0 {
		return -1, -1
	}
	if i == len(c.entries)-1 {
		return c.entries[i].Epoch, leo
	}
	return c.entries[i].Epoch, c.entries[i+1].StartOffset
}

// TruncateFromEnd drops entries whose start offset is >= offset (after a log truncation).
func (c *EpochCache) TruncateFromEnd(ctx context.Context, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.entries)
	c.entries = slices.DeleteFunc(c.entries, func(e storage.EpochEntry) bool { return e.StartOffset >= offset })
	if len(c.entries) == n {
		return nil
	}
	return c.persist(ctx)
}

// TruncateFromStart collapses entries wholly below offset (after retention or a full truncation)
// so the earliest remaining epoch starts at offset.
func (c *EpochCache) TruncateFromStart(ctx context.Context, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	keep := -1
	for i, e := range c.entries {
		if e.StartOffset <= offset {
			keep = i
		}
	}
	if keep <= 0 && (keep < 0 || c.entries[0].StartOffset == offset) {
		return nil
	}
	c.entries = c.entries[keep:]
	c.entries[0].StartOffset = offset
	return c.persist(ctx)
}

// Entries returns a copy of the cache (tests and tooling).
func (c *EpochCache) Entries() []storage.EpochEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.entries)
}
