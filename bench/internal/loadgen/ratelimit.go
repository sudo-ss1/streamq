package loadgen

import (
	"context"
	"sync"
	"time"
)

// TokenBucket paces the total send rate of all producers (M10 plan R16: one shared bucket). Tokens
// accrue at rate per second from start with no capacity cap, so token k becomes available at the
// fixed time start + k/rate. That time is the record's intended send time: a sender that stalls
// keeps its schedule and later records are charged the delay (no coordinated omission, R3).
type TokenBucket struct {
	clk   Clock
	rate  int64
	start time.Time

	mu   sync.Mutex
	next int64 // index of the next token
}

// NewTokenBucket returns a bucket issuing rate tokens per second from start (rate > 0).
func NewTokenBucket(clk Clock, rate int64, start time.Time) *TokenBucket {
	return &TokenBucket{clk: clk, rate: rate, start: start}
}

// reserve claims the next token and returns its availability time.
func (b *TokenBucket) reserve() time.Time {
	b.mu.Lock()
	k := b.next
	b.next++
	b.mu.Unlock()
	return b.start.Add(time.Duration(k * int64(time.Second) / b.rate))
}

// Take claims the next token, waits until it is available and returns its intended send time.
// A token already due is returned at once. It returns ctx's error if ctx ends first.
func (b *TokenBucket) Take(ctx context.Context) (time.Time, error) {
	slot := b.reserve()
	if d := slot.Sub(b.clk.Now()); d > 0 {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-b.clk.After(d):
		}
	}
	return slot, nil
}
