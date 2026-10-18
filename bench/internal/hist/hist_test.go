package hist

import (
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func relErr(got, want int64) float64 {
	if want == 0 {
		return math.Abs(float64(got))
	}
	return math.Abs(float64(got-want)) / float64(want)
}

func TestBucketErrorBound(t *testing.T) {
	require.Less(t, MaxRelativeError, 0.01)
	rng := rand.New(rand.NewPCG(1, 2))
	var values []int64
	for v := range int64(4096) {
		values = append(values, v)
	}
	for e := range 63 {
		p := int64(1) << e
		values = append(values, p-1, p, p+1)
	}
	values = append(values, math.MaxInt64, math.MaxInt64-1)
	for range 200_000 {
		values = append(values, rng.Int64N(math.MaxInt64))
		values = append(values, rng.Int64N(10_000_000_000)) // up to 10 s in ns
	}
	for _, v := range values {
		idx := Index(v)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, NumBuckets, "v=%d", v)
		low, width := bounds(idx)
		require.GreaterOrEqual(t, v, low, "v=%d idx=%d", v, idx)
		require.Less(t, uint64(v-low), uint64(width), "v=%d idx=%d", v, idx)
		require.LessOrEqual(t, relErr(Value(idx), v), MaxRelativeError, "v=%d", v)
	}
}

func TestIndexMonotonic(t *testing.T) {
	prev := -1
	for idx := range NumBuckets {
		low, _ := bounds(idx)
		require.Equal(t, idx, Index(low))
		require.Greater(t, idx, prev)
		prev = idx
	}
	require.Equal(t, 0, Index(-5))
}

func exactQuantile(sorted []int64, q float64) int64 {
	rank := int(math.Ceil(q * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1]
}

func TestQuantiles(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	tests := []struct {
		name string
		gen  func(i int) int64
		n    int
	}{
		{"uniform 1..100000", func(i int) int64 { return int64(i + 1) }, 100_000},
		{"constant 1ms", func(int) int64 { return 1_000_000 }, 1000},
		{"exponential mean 2ms", func(int) int64 { return int64(rng.ExpFloat64() * 2e6) }, 200_000},
		{"bimodal 1ms and 50ms", func(i int) int64 {
			if i%100 == 0 {
				return 50_000_000 + rng.Int64N(1_000_000)
			}
			return 1_000_000 + rng.Int64N(100_000)
		}, 100_000},
		{"small exact values", func(i int) int64 { return int64(i % 100) }, 10_000},
		{"single value", func(int) int64 { return 12345 }, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewShard()
			data := make([]int64, tc.n)
			for i := range tc.n {
				data[i] = tc.gen(i)
				s.Record(data[i])
			}
			slices.Sort(data)
			snap := Merge(s)
			require.Equal(t, uint64(tc.n), snap.Count())
			require.Equal(t, data[len(data)-1], snap.Max())
			for _, q := range []float64{0, 0.5, 0.9, 0.99, 0.999, 1} {
				want := exactQuantile(data, q)
				got := snap.Quantile(q)
				require.LessOrEqual(t, relErr(got, want), 0.01, "q=%v got=%d want=%d", q, got, want)
			}
		})
	}
}

func TestEmptyAndNegative(t *testing.T) {
	snap := Merge(NewShard(), nil)
	require.Equal(t, uint64(0), snap.Count())
	require.Equal(t, int64(0), snap.Quantile(0.99))
	require.Equal(t, int64(0), snap.Max())

	s := NewShard()
	s.Record(-10)
	snap = Merge(s)
	require.Equal(t, uint64(1), snap.Count())
	require.Equal(t, int64(0), snap.Quantile(0.5))
}

func TestMergeShards(t *testing.T) {
	a, b := NewShard(), NewShard()
	for i := range 1000 {
		a.Record(int64(i))
		b.Record(int64(1000 + i))
	}
	snap := Merge(a, b)
	require.Equal(t, uint64(2000), snap.Count())
	require.Equal(t, int64(1999), snap.Max())
	require.LessOrEqual(t, relErr(snap.Quantile(0.5), 1000), 0.01)
}

func TestConcurrentRecord(t *testing.T) {
	const writers, per = 8, 20_000
	shards := make([]*Shard, 2)
	for i := range shards {
		shards[i] = NewShard()
	}
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := shards[w%len(shards)] // several writers share a shard
			for i := range per {
				s.Record(int64(w*per + i))
			}
		}()
	}
	wg.Wait()
	snap := Merge(shards...)
	require.Equal(t, uint64(writers*per), snap.Count())
	require.Equal(t, int64(writers*per-1), snap.Max())
	require.LessOrEqual(t, relErr(snap.Quantile(0.5), writers*per/2), 0.01)
}
