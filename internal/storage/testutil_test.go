package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

// fakeClock is a manually advanced Clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.UnixMilli(1_700_000_000_000)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// buildBatch builds a non-idempotent batch with one record per value, all with timestamp ts.
func buildBatch(t testing.TB, ts int64, values ...[]byte) record.Batch {
	t.Helper()
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for _, v := range values {
		b.Append(ts, nil, v, nil)
	}
	batch, err := b.Build()
	require.NoError(t, err)
	return batch
}
