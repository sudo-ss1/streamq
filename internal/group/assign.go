package group

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

type rangeAssignor struct{}

func (rangeAssignor) Name() string { return RangeAssignorName }

func (rangeAssignor) Assign(members []MemberSubscription, partitions map[string]int32) map[string]Assignment {
	_, _ = members, partitions
	return nil
}

type roundRobinAssignor struct{}

func (roundRobinAssignor) Name() string { return RoundRobinAssignorName }

func (roundRobinAssignor) Assign(members []MemberSubscription, partitions map[string]int32) map[string]Assignment {
	_, _ = members, partitions
	return nil
}
