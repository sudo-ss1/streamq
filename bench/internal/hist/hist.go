// Package hist is the bench tool's latency histogram (M10 plan R4, DECISIONS D61): log-spaced
// buckets with atomic counters, a fixed memory bound and a relative error of at most 1/256
// (well under 1 %). Each producer records into its own Shard; shards are merged at the end.
package hist

import (
	"math"
	"math/bits"
	"sync/atomic"
)

const (
	// subBits sets the precision: every power-of-two range [2^e, 2^(e+1)) at or above 2^subBits
	// is split into 2^subBits equal buckets, so a bucket is at most 1/128 of its lower bound wide.
	subBits  = 7
	subCount = 1 << subBits
	// NumBuckets is the bucket count: subCount exact buckets for [0, subCount), then subCount
	// buckets for each remaining power of two up to 2^62 (the largest a positive int64 reaches).
	NumBuckets = subCount + (63-subBits)*subCount
)

// MaxRelativeError bounds |Value(Index(v)) - v| / v for every v > 0.
const MaxRelativeError = 1.0 / (2 * subCount)

// Index returns v's bucket. Negative values are clamped to 0.
func Index(v int64) int {
	if v < subCount {
		if v < 0 {
			return 0
		}
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1 // e >= subBits
	shift := e - subBits
	m := int(v >> shift) // in [subCount, 2*subCount)
	return subCount + shift*subCount + (m - subCount)
}

// bounds returns bucket idx's lower bound and width.
func bounds(idx int) (low, width int64) {
	if idx < subCount {
		return int64(idx), 1
	}
	shift := (idx - subCount) / subCount
	m := int64(subCount + (idx-subCount)%subCount)
	return m << shift, int64(1) << shift
}

// Value returns the representative value of bucket idx: the middle of its range.
func Value(idx int) int64 {
	low, width := bounds(idx)
	return low + (width-1)/2
}

// Shard is a histogram safe for concurrent Record calls. Create it with NewShard.
type Shard struct {
	counts []atomic.Uint64
	total  atomic.Uint64
	max    atomic.Int64
}

// NewShard returns an empty shard (about 58 KiB).
func NewShard() *Shard {
	return &Shard{counts: make([]atomic.Uint64, NumBuckets)}
}

// Record adds one observation of v (negative values count as 0).
func (s *Shard) Record(v int64) {
	v = max(v, 0)
	s.counts[Index(v)].Add(1)
	s.total.Add(1)
	for {
		cur := s.max.Load()
		if v <= cur || s.max.CompareAndSwap(cur, v) {
			return
		}
	}
}

// Snapshot is an immutable merged view of one or more shards.
type Snapshot struct {
	counts []uint64
	total  uint64
	max    int64
}

// Merge sums shards into a Snapshot. Nil shards are skipped. Callers merge after recording has
// stopped; records racing with Merge may or may not be included.
func Merge(shards ...*Shard) Snapshot {
	out := Snapshot{counts: make([]uint64, NumBuckets)}
	for _, s := range shards {
		if s == nil {
			continue
		}
		for i := range s.counts {
			out.counts[i] += s.counts[i].Load()
		}
		out.total += s.total.Load()
		out.max = max(out.max, s.max.Load())
	}
	return out
}

// Count returns the number of observations.
func (s Snapshot) Count() uint64 { return s.total }

// Max returns the exact largest observation (0 if empty).
func (s Snapshot) Max() int64 { return s.max }

// Quantile returns the q-quantile (q in [0, 1]) by the nearest-rank method: the representative
// value of the bucket holding the ceil(q*n)-th smallest observation, capped at the exact max. It
// returns 0 for an empty snapshot.
func (s Snapshot) Quantile(q float64) int64 {
	if s.total == 0 {
		return 0
	}
	q = min(max(q, 0), 1)
	rank := uint64(math.Ceil(q * float64(s.total)))
	rank = min(max(rank, 1), s.total)
	var cum uint64
	for i, c := range s.counts {
		cum += c
		if cum >= rank {
			return min(Value(i), s.max)
		}
	}
	return s.max
}
