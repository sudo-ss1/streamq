// Package loadgen implements streamq-bench (SPEC §9 M10, bench/USAGE.md): flag and preset
// parsing, in-process clusters, the rate-limited producer workload, windowed latency and
// throughput measurement, cluster stability checks and the table and JSON reports.
package loadgen

import "time"

// Clock is the time source of the workload, injected so tests can drive pacing and latency with a
// fake clock instead of sleeping.
type Clock interface {
	Now() time.Time
	// After returns a channel that receives once d has elapsed.
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
