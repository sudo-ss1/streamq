package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.uber.org/goleak"

	"github.com/sudo-ss1/streamq/test/linearizability"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestCheckBulk(t *testing.T) {
	acked := func(key string, seq int, p int32, off int64) *bulkValue {
		return &bulkValue{key: key, seq: seq, status: bulkAcked, partition: p, offset: off}
	}
	failed := func(key string, seq int) *bulkValue {
		return &bulkValue{key: key, seq: seq, status: bulkFailed, offset: -1}
	}
	tests := []struct {
		name   string
		values map[string]*bulkValue
		final  map[int32][]string
		check  func(t *testing.T, r bulkResult)
	}{
		{
			name:   "all acked present in order",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 0), "a:1": acked("a", 1, 0, 1), "b:0": acked("b", 0, 1, 0)},
			final:  map[int32][]string{0: {"a:0", "a:1"}, 1: {"b:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Empty(t, r.violations())
				require.Equal(t, 3, r.Acked)
				require.Equal(t, 3, r.AckedPresent)
			},
		},
		{
			name:   "failed values may appear once anywhere or not at all",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 1), "a:1": failed("a", 1), "a:2": failed("a", 2)},
			final:  map[int32][]string{0: {"a:1", "a:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Empty(t, r.violations())
				require.Equal(t, 1, r.FailedPresent)
				require.Equal(t, 2, r.Failed)
			},
		},
		{
			name:   "lost acked value",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 0), "a:1": acked("a", 1, 0, 1)},
			final:  map[int32][]string{0: {"a:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Equal(t, []string{"a:1"}, r.Lost)
				require.NotEmpty(t, r.violations())
			},
		},
		{
			name:   "duplicate of a failed value",
			values: map[string]*bulkValue{"a:0": failed("a", 0)},
			final:  map[int32][]string{0: {"a:0", "a:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Equal(t, []string{"a:0"}, r.Duplicates)
			},
		},
		{
			name:   "phantom",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 0)},
			final:  map[int32][]string{0: {"a:0", "zz:9"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Equal(t, []string{"zz:9"}, r.Phantoms)
			},
		},
		{
			name:   "per-key reorder of acked values",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 1), "a:1": acked("a", 1, 0, 0)},
			final:  map[int32][]string{0: {"a:1", "a:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Len(t, r.OrderViolations, 1)
				require.Empty(t, r.OffsetMismatch)
			},
		},
		{
			name:   "acked offset holds another value",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 0, 0), "b:0": acked("b", 0, 0, 0)},
			final:  map[int32][]string{0: {"a:0", "b:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Len(t, r.OffsetMismatch, 1)
			},
		},
		{
			name:   "wrong partition",
			values: map[string]*bulkValue{"a:0": acked("a", 0, 1, 0)},
			final:  map[int32][]string{0: {"a:0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Equal(t, []string{"a:0"}, r.WrongPartition)
			},
		},
		{
			name:   "sentinels are not order-checked",
			values: map[string]*bulkValue{"sentinel-p0-0": {status: bulkAcked, partition: 0, offset: 1}, "a:0": acked("a", 0, 0, 0)},
			final:  map[int32][]string{0: {"a:0", "sentinel-p0-0"}},
			check: func(t *testing.T, r bulkResult) {
				t.Helper()
				require.Empty(t, r.violations())
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, checkBulk(tc.values, tc.final))
		})
	}
}

func TestUnavailableWindows(t *testing.T) {
	tests := []struct {
		name string
		acks []float64
		end  float64
		want []window
	}{
		{name: "steady", acks: []float64{0.5, 1, 1.5, 2, 2.5}, end: 3, want: nil},
		{name: "gap in the middle", acks: []float64{0.5, 1, 4.5, 5}, end: 5.5, want: []window{{1, 4.5}}},
		{name: "nothing acked", acks: nil, end: 10, want: []window{{0, 10}}},
		{name: "silent tail", acks: []float64{1, 1.5}, end: 6, want: []window{{1.5, 6}}},
		{name: "unsorted input", acks: []float64{5, 1}, end: 5.5, want: []window{{1, 5}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, unavailableWindows(tc.acks, tc.end, 2))
		})
	}
}

func TestSelectScenarios(t *testing.T) {
	all, err := selectScenarios("")
	require.NoError(t, err)
	require.Len(t, all, 6)
	got, err := selectScenarios("latency, full-disk")
	require.NoError(t, err)
	require.Equal(t, "latency", got[0].name)
	require.True(t, got[1].fullDisk)
	_, err = selectScenarios("nope")
	require.Error(t, err)
}

func TestScenarioSeedIsStablePerName(t *testing.T) {
	first, again := scenarioSeed(7, "latency"), scenarioSeed(7, "latency")
	require.Equal(t, first, again)
	require.NotEqual(t, scenarioSeed(7, "latency"), scenarioSeed(7, "full-disk"))
	require.NotEqual(t, scenarioSeed(7, "latency"), scenarioSeed(8, "latency"))
}

func TestFinalStatus(t *testing.T) {
	enoughLin := linResult{Summary: linearizability.Summary{Acked: minLinAcked}}
	enoughBulk := bulkResult{Acked: minBulkAcked}
	tests := []struct {
		name string
		res  scenarioResult
		want string
	}{
		{name: "pass", res: scenarioResult{Status: StatusPass, Availability: availability{Healthy: true}, Lin: enoughLin, Bulk: enoughBulk}, want: StatusPass},
		{name: "violation", res: scenarioResult{Status: StatusPass, Violations: []string{"bulk lost"}, Availability: availability{Healthy: true}, Lin: enoughLin, Bulk: enoughBulk}, want: StatusFail},
		{name: "inconclusive", res: scenarioResult{Violations: []string{"lin porcupine check inconclusive"}, Availability: availability{Healthy: true}, Lin: enoughLin, Bulk: enoughBulk}, want: StatusInconclusive},
		{name: "unhealthy", res: scenarioResult{Status: StatusPass}, want: StatusUnhealthy},
		{name: "not exercised", res: scenarioResult{Status: StatusNotExercised, Availability: availability{Healthy: true}, Lin: enoughLin, Bulk: enoughBulk}, want: StatusNotExercised},
		{name: "violation beats not exercised", res: scenarioResult{Status: StatusNotExercised, Violations: []string{"x"}}, want: StatusFail},
		{name: "bulk not settled", res: scenarioResult{Status: StatusPass, Availability: availability{Healthy: true}, Lin: enoughLin,
			Bulk: bulkResult{Acked: minBulkAcked, SettleError: "partition 3: no sentinel acknowledged"}}, want: StatusFail},
		{name: "too little work", res: scenarioResult{Status: StatusPass, Availability: availability{Healthy: true}, Lin: enoughLin,
			Bulk: bulkResult{Acked: 10}}, want: StatusNotExercised},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, finalStatus(&tc.res))
		})
	}
}
