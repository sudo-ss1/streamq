package client

import (
	"math/rand/v2"
	"sync"

	"github.com/sudo-ss1/streamq/internal/partitioner"
)

// Partitioner chooses partitions for records that do not set UsePartition.
//
// Implementations must be safe for concurrent use. The producer never holds its own locks while
// calling them.
type Partitioner interface {
	// Partition returns a partition in [0, numPartitions) for a record of topic with key
	// (nil = no key).
	Partition(topic string, key []byte, numPartitions int32) int32
	// OnNewBatch is called after the producer sealed a batch for partition prev of topic, so a
	// sticky partitioner can move on (SPEC §7.4).
	OnNewBatch(topic string, prev, numPartitions int32)
}

// DefaultPartitioner implements SPEC §7.4: murmur2(key) for keyed records; for records without
// a key, a sticky partition per topic that changes each time its batch is sealed.
type DefaultPartitioner struct {
	mu     sync.Mutex // leaf lock
	sticky map[string]int32
	intN   func(n int32) int32
}

// NewDefaultPartitioner returns the SPEC §7.4 partitioner.
func NewDefaultPartitioner() *DefaultPartitioner {
	return &DefaultPartitioner{
		sticky: map[string]int32{},
		intN:   func(n int32) int32 { return rand.Int32N(n) }, //nolint:gosec // G404: non-cryptographic partition choice
	}
}

// Partition implements Partitioner.
func (d *DefaultPartitioner) Partition(topic string, key []byte, numPartitions int32) int32 {
	if key != nil {
		return partitioner.ForKey(key, numPartitions)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if p, ok := d.sticky[topic]; ok && p < numPartitions {
		return p
	}
	p := d.intN(numPartitions)
	d.sticky[topic] = p
	return p
}

// OnNewBatch implements Partitioner: if prev is the sticky partition, pick a different one.
func (d *DefaultPartitioner) OnNewBatch(topic string, prev, numPartitions int32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if cur, ok := d.sticky[topic]; !ok || cur != prev {
		return
	}
	if numPartitions <= 1 {
		return
	}
	next := d.intN(numPartitions - 1)
	if next >= prev {
		next++
	}
	d.sticky[topic] = next
}
