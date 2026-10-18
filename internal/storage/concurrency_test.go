package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

// TestConcurrentReadsDuringAppendRollAndRetention runs one writer (appending with tiny segments so
// it rolls constantly, and applying size retention) against four lock-free readers. Every record a
// reader sees must carry its own offset, and offsets within a read must be contiguous. The only
// acceptable read error is ErrOffsetOutOfRange for data retention removed meanwhile.
func TestConcurrentReadsDuringAppendRollAndRetention(t *testing.T) {
	cfg := smallConfig()
	cfg.SegmentBytes = 1024
	cfg.MaxMessageBytes = 512
	cfg.RetentionMs = -1
	cfg.RetentionBytes = 8 * 1024
	clock := newFakeClock()
	l := openTestLog(t, t.TempDir(), cfg, clock, 0)
	defer l.close(t)

	const batches = 1500
	var done atomic.Bool
	var wg sync.WaitGroup
	errs := make(chan error, 8)

	// Offsets are deterministic with a single writer, so batches are built up front (require
	// must not run off the test goroutine).
	var all []record.Batch
	var next int64
	for i := range batches {
		vals := make([][]byte, i%3+1)
		for j := range vals {
			vals[j] = offsetValue(next)
			next++
		}
		all = append(all, buildBatch(t, 1, vals...))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer done.Store(true)
		ctx := context.Background()
		for i, b := range all {
			if _, err := l.Append(ctx, b, 1); err != nil {
				errs <- err
				return
			}
			if i%20 == 0 {
				if _, err := l.DeleteOldSegments(ctx, clock.Now(), l.LogEndOffset()); err != nil {
					errs <- err
					return
				}
				// No delete delay: close and unlink retired segments while readers may hold them.
				for _, s := range l.deleted {
					if err := s.deleteFiles(); err != nil {
						errs <- err
						return
					}
				}
				l.deleted = nil
			}
		}
	}()

	var reads atomic.Int64
	for r := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 42))
			ctx := context.Background()
			for !done.Load() || reads.Load() < 100 {
				start, leo := l.LogStartOffset(), l.LogEndOffset()
				if leo == start {
					continue
				}
				off := start + rng.Int64N(leo-start)
				res, err := l.Read(ctx, off, 1+rng.IntN(600), leo)
				if errors.Is(err, ErrOffsetOutOfRange) {
					continue
				}
				if err != nil {
					errs <- err
					return
				}
				if err := checkRead(res.Data, off); err != nil {
					errs <- err
					return
				}
				reads.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Positive(t, l.LogStartOffset(), "retention never ran")
	require.GreaterOrEqual(t, reads.Load(), int64(100))
}

// checkRead verifies that data is non-empty, starts with the batch containing off, and that the
// records are contiguous and carry their own offsets.
func checkRead(data []byte, off int64) error {
	recs, err := decodeChecked(data)
	if err != nil {
		return err
	}
	if len(recs) == 0 {
		return errors.New("empty read below LEO")
	}
	if recs[0].Offset > off || recs[len(recs)-1].Offset < off {
		return fmt.Errorf("read [%d, %d] does not contain offset %d", recs[0].Offset, recs[len(recs)-1].Offset, off)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Offset != recs[i-1].Offset+1 {
			return fmt.Errorf("non-contiguous offsets %d -> %d", recs[i-1].Offset, recs[i].Offset)
		}
	}
	return nil
}
