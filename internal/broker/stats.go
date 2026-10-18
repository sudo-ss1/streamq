package broker

import "github.com/sudo-ss1/streamq/internal/metrics"

// PartitionStats implements metrics.BrokerSource.
func (b *Broker) PartitionStats() []metrics.PartitionStat { return nil }

// ConsumerLag implements metrics.BrokerSource.
func (b *Broker) ConsumerLag() []metrics.LagStat { return nil }

// OfflinePartitions implements metrics.BrokerSource.
func (b *Broker) OfflinePartitions() (int, bool) { return 0, false }
