package client

import (
	"math/rand/v2"
	"time"
)

// backoff computes retry delays: exponential with "full jitter", delay(n) is uniform in
// [0, min(max, base·2^n)] (FR12).
type backoff struct {
	base, max time.Duration
	rand      func() float64 // in [0, 1); injectable for tests
}

func newBackoff(base, maxDelay time.Duration) backoff {
	return backoff{base: base, max: maxDelay, rand: rand.Float64} //nolint:gosec // jitter, not security
}

// delay returns the delay before retry attempt n (n = 0 for the first retry).
func (b backoff) delay(n int) time.Duration {
	ceiling := b.max
	if n < 62 {
		if d := b.base << n; d > 0 && d < b.max {
			ceiling = d
		}
	}
	return time.Duration(b.rand() * float64(ceiling))
}
