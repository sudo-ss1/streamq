package group

import (
	"slices"
)

// MemberSubscription is one member's input to an assignor.
type MemberSubscription struct {
	MemberID string
	Topics   []string
}

// Assignor computes assignments. Implementations are pure and deterministic.
type Assignor interface {
	Name() string
	// Assign returns an assignment per member id. partitions maps topic to partition count;
	// topics missing from it contribute nothing.
	Assign(members []MemberSubscription, partitions map[string]int32) map[string]Assignment
}

// RangeAssignor is Kafka's RangeAssignor.
func RangeAssignor() Assignor { return rangeAssignor{} }

// RoundRobinAssignor is Kafka's RoundRobinAssignor.
func RoundRobinAssignor() Assignor { return roundRobinAssignor{} }

// assignorByName returns the named built-in assignor.
func assignorByName(name string) (Assignor, bool) {
	switch name {
	case RangeAssignorName:
		return RangeAssignor(), true
	case RoundRobinAssignorName:
		return RoundRobinAssignor(), true
	default:
		return nil, false
	}
}

// normalized holds the sorted member ids, each member's subscription set, and the sorted topics
// that at least one member subscribes to and that have partitions.
type normalized struct {
	members []string
	subs    map[string]map[string]struct{}
	topics  []string
	out     map[string]Assignment
}

func normalize(members []MemberSubscription, partitions map[string]int32) normalized {
	n := normalized{
		subs: make(map[string]map[string]struct{}, len(members)),
		out:  make(map[string]Assignment, len(members)),
	}
	topicSet := make(map[string]struct{})
	for _, m := range members {
		set, ok := n.subs[m.MemberID]
		if !ok {
			set = make(map[string]struct{}, len(m.Topics))
			n.subs[m.MemberID] = set
			n.members = append(n.members, m.MemberID)
			n.out[m.MemberID] = Assignment{}
		}
		for _, t := range m.Topics {
			set[t] = struct{}{}
			if partitions[t] > 0 {
				topicSet[t] = struct{}{}
			}
		}
	}
	slices.Sort(n.members)
	for t := range topicSet {
		n.topics = append(n.topics, t)
	}
	slices.Sort(n.topics)
	return n
}

// subscribers returns the sorted members subscribed to topic.
func (n normalized) subscribers(topic string) []string {
	var out []string
	for _, m := range n.members {
		if _, ok := n.subs[m][topic]; ok {
			out = append(out, m)
		}
	}
	return out
}

type rangeAssignor struct{}

func (rangeAssignor) Name() string { return RangeAssignorName }

// Assign splits each topic's partitions into contiguous ranges over the members subscribed to it,
// sorted by member id; the first n%m members get one extra partition.
func (rangeAssignor) Assign(members []MemberSubscription, partitions map[string]int32) map[string]Assignment {
	n := normalize(members, partitions)
	for _, topic := range n.topics {
		subs := n.subscribers(topic)
		count := partitions[topic]
		m := int32(len(subs))
		per, extra := count/m, count%m
		for i, id := range subs {
			idx := int32(i)
			start := idx*per + min(idx, extra)
			length := per
			if idx < extra {
				length++
			}
			if length == 0 {
				continue
			}
			ps := make([]int32, 0, length)
			for p := start; p < start+length; p++ {
				ps = append(ps, p)
			}
			n.out[id][topic] = ps
		}
	}
	return n.out
}

type roundRobinAssignor struct{}

func (roundRobinAssignor) Name() string { return RoundRobinAssignorName }

// Assign deals every (topic, partition), sorted, round-robin over the sorted members, skipping
// members not subscribed to the partition's topic (Kafka's RoundRobinAssignor).
func (roundRobinAssignor) Assign(members []MemberSubscription, partitions map[string]int32) map[string]Assignment {
	n := normalize(members, partitions)
	if len(n.members) == 0 {
		return n.out
	}
	next := 0
	for _, topic := range n.topics {
		for p := range partitions[topic] {
			// Every topic in n.topics has at least one subscriber, so this terminates.
			for {
				if _, ok := n.subs[n.members[next]][topic]; ok {
					break
				}
				next = (next + 1) % len(n.members)
			}
			id := n.members[next]
			n.out[id][topic] = append(n.out[id][topic], p)
			next = (next + 1) % len(n.members)
		}
	}
	return n.out
}
