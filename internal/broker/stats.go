package broker

// Lock ordering: these methods never hold two locks at once (plan R22). They copy the partition
// map under Broker.mu (read lock), release it, and only then call the per-partition accessors,
// each of which takes and releases its own partition lock. Metadata is read through
// store.Snapshot(), which takes no broker lock. Every method tolerates a closed broker: closed
// logs report their last values.

import (
	"cmp"
	"slices"

	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/metrics"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

type partitionRef struct {
	tp storage.TopicPartition
	p  *partition.Partition
}

// localPartitions returns a copy of the partition map sorted by topic and partition.
func (b *Broker) localPartitions() []partitionRef {
	b.mu.RLock()
	refs := make([]partitionRef, 0, len(b.partitions))
	for tp, p := range b.partitions {
		refs = append(refs, partitionRef{tp: tp, p: p})
	}
	b.mu.RUnlock()
	slices.SortFunc(refs, func(a, c partitionRef) int {
		return cmp.Or(cmp.Compare(a.tp.Topic, c.tp.Topic), cmp.Compare(a.tp.Partition, c.tp.Partition))
	})
	return refs
}

// PartitionStats implements metrics.BrokerSource.
func (b *Broker) PartitionStats() []metrics.PartitionStat {
	refs := b.localPartitions()
	out := make([]metrics.PartitionStat, 0, len(refs))
	for _, r := range refs {
		out = append(out, metrics.PartitionStat{
			Topic:     r.tp.Topic,
			Partition: r.tp.Partition,
			Leader:    r.p.IsLeader(),
			LEO:       r.p.Log().LogEndOffset(),
			HW:        r.p.HighWatermark(),
			ISRSize:   r.p.ISRSize(),
			Replicas:  r.p.ReplicaCount(),
		})
	}
	return out
}

// ConsumerLag implements metrics.BrokerSource. Lag is reported only for partitions this broker
// leads, as max(HW - committed, 0) for every group with a committed offset on the partition.
func (b *Broker) ConsumerLag() []metrics.LagStat {
	snap := b.store.Snapshot()
	if len(snap.Groups) == 0 {
		return nil
	}
	groups := make([]string, 0, len(snap.Groups))
	for g := range snap.Groups {
		groups = append(groups, g)
	}
	slices.Sort(groups)
	var out []metrics.LagStat
	for _, r := range b.localPartitions() {
		if !r.p.IsLeader() {
			continue
		}
		hw := r.p.HighWatermark()
		for _, g := range groups {
			committed, ok := snap.Committed(g, r.tp.Topic, r.tp.Partition)
			if !ok {
				continue
			}
			out = append(out, metrics.LagStat{
				Group: g, Topic: r.tp.Topic, Partition: r.tp.Partition, Lag: max(hw-committed, 0),
			})
		}
	}
	return out
}

// OfflinePartitions implements metrics.BrokerSource. It counts every partition in the metadata
// without a leader and reports the count only when this broker is the controller (a single-node
// local store names its own broker as controller), so a sum across brokers is correct.
func (b *Broker) OfflinePartitions() (int, bool) {
	snap := b.store.Snapshot()
	if snap.ControllerID != b.cfg.BrokerID {
		return 0, false
	}
	n := 0
	for _, t := range snap.Topics {
		for _, ps := range t.Partitions {
			if ps.Leader == metadata.NoLeader {
				n++
			}
		}
	}
	return n, true
}
