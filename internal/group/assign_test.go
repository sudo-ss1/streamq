package group

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeAssignor(t *testing.T) {
	tests := []struct {
		name       string
		members    []MemberSubscription
		partitions map[string]int32
		want       map[string]Assignment
	}{
		{
			name:       "even split",
			members:    []MemberSubscription{{"b", []string{"t"}}, {"a", []string{"t"}}, {"c", []string{"t"}}},
			partitions: map[string]int32{"t": 6},
			want: map[string]Assignment{
				"a": {"t": {0, 1}}, "b": {"t": {2, 3}}, "c": {"t": {4, 5}},
			},
		},
		{
			name:       "uneven split: first n%m members get one extra",
			members:    []MemberSubscription{{"a", []string{"t"}}, {"b", []string{"t"}}, {"c", []string{"t"}}},
			partitions: map[string]int32{"t": 7},
			want: map[string]Assignment{
				"a": {"t": {0, 1, 2}}, "b": {"t": {3, 4}}, "c": {"t": {5, 6}},
			},
		},
		{
			name:       "multiple topics are split independently",
			members:    []MemberSubscription{{"a", []string{"t0", "t1"}}, {"b", []string{"t1", "t0"}}},
			partitions: map[string]int32{"t0": 3, "t1": 3},
			want: map[string]Assignment{
				"a": {"t0": {0, 1}, "t1": {0, 1}}, "b": {"t0": {2}, "t1": {2}},
			},
		},
		{
			name: "different subscriptions",
			members: []MemberSubscription{
				{"a", []string{"t0"}}, {"b", []string{"t0", "t1"}}, {"c", []string{"t1"}},
			},
			partitions: map[string]int32{"t0": 3, "t1": 4},
			want: map[string]Assignment{
				"a": {"t0": {0, 1}}, "b": {"t0": {2}, "t1": {0, 1}}, "c": {"t1": {2, 3}},
			},
		},
		{
			name:       "more members than partitions",
			members:    []MemberSubscription{{"a", []string{"t"}}, {"b", []string{"t"}}, {"c", []string{"t"}}},
			partitions: map[string]int32{"t": 2},
			want:       map[string]Assignment{"a": {"t": {0}}, "b": {"t": {1}}, "c": {}},
		},
		{
			name:       "unknown topic contributes nothing",
			members:    []MemberSubscription{{"a", []string{"missing", "t"}}},
			partitions: map[string]int32{"t": 2},
			want:       map[string]Assignment{"a": {"t": {0, 1}}},
		},
		{
			name:       "no members",
			partitions: map[string]int32{"t": 2},
			want:       map[string]Assignment{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RangeAssignor().Assign(tc.members, tc.partitions))
		})
	}
}

func TestRoundRobinAssignor(t *testing.T) {
	tests := []struct {
		name       string
		members    []MemberSubscription
		partitions map[string]int32
		want       map[string]Assignment
	}{
		{
			// Kafka RoundRobinAssignor javadoc, example 1.
			name:       "same subscriptions",
			members:    []MemberSubscription{{"C1", []string{"t1", "t0"}}, {"C0", []string{"t0", "t1"}}},
			partitions: map[string]int32{"t0": 3, "t1": 3},
			want: map[string]Assignment{
				"C0": {"t0": {0, 2}, "t1": {1}}, "C1": {"t0": {1}, "t1": {0, 2}},
			},
		},
		{
			// Kafka RoundRobinAssignor javadoc, example 2.
			name: "different subscriptions skip unsubscribed members",
			members: []MemberSubscription{
				{"C0", []string{"t0"}}, {"C1", []string{"t0", "t1"}}, {"C2", []string{"t0", "t1", "t2"}},
			},
			partitions: map[string]int32{"t0": 1, "t1": 2, "t2": 3},
			want: map[string]Assignment{
				"C0": {"t0": {0}}, "C1": {"t1": {0}}, "C2": {"t1": {1}, "t2": {0, 1, 2}},
			},
		},
		{
			name:       "more members than partitions",
			members:    []MemberSubscription{{"a", []string{"t"}}, {"b", []string{"t"}}, {"c", []string{"t"}}},
			partitions: map[string]int32{"t": 2},
			want:       map[string]Assignment{"a": {"t": {0}}, "b": {"t": {1}}, "c": {}},
		},
		{
			name:       "no members",
			partitions: map[string]int32{"t": 2},
			want:       map[string]Assignment{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, RoundRobinAssignor().Assign(tc.members, tc.partitions))
		})
	}
}

// TestAssignorsProperties checks, over many generated inputs, that every partition of a topic
// with at least one subscriber is assigned exactly once, to a subscribed member, that range keeps
// per-topic counts within one, and that the result does not depend on member order.
func TestAssignorsProperties(t *testing.T) {
	topics := []string{"t0", "t1", "t2"}
	partitionSets := []map[string]int32{
		{"t0": 1, "t1": 2, "t2": 3},
		{"t0": 6, "t1": 0, "t2": 5},
		{"t0": 7, "t1": 4},
		{},
	}
	for _, assignor := range []Assignor{RangeAssignor(), RoundRobinAssignor()} {
		for nMembers := 1; nMembers <= 4; nMembers++ {
			for pi, parts := range partitionSets {
				// Each member subscribes to a subset of topics chosen by mask bits.
				for mask := 1; mask < 1<<(3*nMembers) && mask < 512; mask += 7 {
					var members []MemberSubscription
					for i := range nMembers {
						var subs []string
						for j, topic := range topics {
							if mask&(1<<(3*i+j)) != 0 {
								subs = append(subs, topic)
							}
						}
						members = append(members, MemberSubscription{MemberID: fmt.Sprintf("m%d", i), Topics: subs})
					}
					name := fmt.Sprintf("%s/members=%d/parts=%d/mask=%d", assignor.Name(), nMembers, pi, mask)
					checkAssignment(t, name, assignor, members, parts)
				}
			}
		}
	}
}

func checkAssignment(t *testing.T, name string, a Assignor, members []MemberSubscription, parts map[string]int32) {
	t.Helper()
	got := a.Assign(members, parts)
	require.Len(t, got, len(members), name)

	subscribed := map[string]map[string]bool{}
	for _, m := range members {
		subscribed[m.MemberID] = map[string]bool{}
		for _, topic := range m.Topics {
			subscribed[m.MemberID][topic] = true
		}
	}
	owners := map[TopicPartition]int{}
	for id, asg := range got {
		for topic, ps := range asg {
			require.True(t, subscribed[id][topic], "%s: %s got unsubscribed topic %s", name, id, topic)
			require.True(t, slices.IsSorted(ps), name)
			for _, p := range ps {
				owners[TopicPartition{topic, p}]++
			}
		}
	}
	for topic, n := range parts {
		hasSub := false
		var counts []int
		for _, m := range members {
			if subscribed[m.MemberID][topic] {
				hasSub = true
				counts = append(counts, len(got[m.MemberID][topic]))
			}
		}
		for p := range n {
			want := 0
			if hasSub {
				want = 1
			}
			require.Equal(t, want, owners[TopicPartition{topic, p}], "%s: %s/%d", name, topic, p)
		}
		if a.Name() == RangeAssignorName && len(counts) > 0 {
			require.LessOrEqual(t, slices.Max(counts)-slices.Min(counts), 1, name)
		}
	}
	require.Equal(t, len(owners), func() int {
		total := 0
		for _, ps := range got {
			for _, p := range ps {
				total += len(p)
			}
		}
		return total
	}(), "%s: a partition assigned twice", name)

	reversed := slices.Clone(members)
	slices.Reverse(reversed)
	require.Equal(t, got, a.Assign(reversed, parts), "%s: not deterministic", name)
}
