package client

import "time"

// clock abstracts time for the producer's linger timers, retry backoff and delivery deadlines,
// so tests can control them (CLAUDE.md: inject a clock where timing matters).
type clock interface {
	Now() time.Time
	// AfterFunc runs f in its own goroutine after d; stop cancels it if it has not run.
	AfterFunc(d time.Duration, f func()) (stop func() bool)
	// NewTimer returns a channel that receives once after d, and a stop func.
	NewTimer(d time.Duration) (<-chan time.Time, func() bool)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) AfterFunc(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

func (realClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}
