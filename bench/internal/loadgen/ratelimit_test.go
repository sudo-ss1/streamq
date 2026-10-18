package loadgen

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTokenBucketPacing(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	b := NewTokenBucket(clk, 1000, t0) // one token per ms
	ctx := context.Background()

	got, err := b.Take(ctx)
	require.NoError(t, err)
	require.Equal(t, t0, got, "the first token is due at start")

	done := make(chan time.Time, 1)
	go func() {
		slot, err := b.Take(ctx)
		if err == nil {
			done <- slot
		}
	}()
	require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
	select {
	case <-done:
		t.Fatal("the second token was released before its time")
	default:
	}
	clk.Advance(999 * time.Microsecond)
	select {
	case <-done:
		t.Fatal("the second token was released early")
	default:
	}
	clk.Advance(time.Microsecond)
	require.Equal(t, t0.Add(time.Millisecond), <-done)

	// A sender that falls behind keeps the schedule: tokens already due are returned at once with
	// their original intended times.
	clk.Advance(10 * time.Millisecond) // now t0+11ms
	for k := 2; k <= 11; k++ {
		slot, err := b.Take(ctx)
		require.NoError(t, err)
		require.Equal(t, t0.Add(time.Duration(k)*time.Millisecond), slot)
	}
	require.Equal(t, 0, clk.Waiters())
}

func TestTokenBucketSharedSchedule(t *testing.T) {
	clk := newFakeClock()
	t0 := clk.Now()
	const rate, takers, per = 5000, 4, 250
	b := NewTokenBucket(clk, rate, t0)
	clk.Advance(time.Second) // every token of the first second is due
	slots := make(chan time.Time, takers*per)
	var wg sync.WaitGroup
	for range takers {
		wg.Go(func() {
			for range per {
				s, err := b.Take(context.Background())
				if err == nil {
					slots <- s
				}
			}
		})
	}
	wg.Wait()
	close(slots)
	seen := map[time.Duration]bool{}
	for s := range slots {
		seen[s.Sub(t0)] = true
	}
	require.Len(t, seen, takers*per, "the producers share one schedule: every slot is issued once")
	for k := range takers * per {
		require.True(t, seen[time.Duration(k)*time.Second/rate], "slot %d", k)
	}
}

func TestTokenBucketCancel(t *testing.T) {
	clk := newFakeClock()
	b := NewTokenBucket(clk, 1, clk.Now())
	_, err := b.Take(context.Background())
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := b.Take(ctx)
		errc <- err
	}()
	require.Eventually(t, func() bool { return clk.Waiters() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-errc, context.Canceled)
}
