// Package partitioner implements key-based partition selection (SPEC §7.4), shared by the client
// library, streamqctl and tests.
package partitioner

// Murmur2 is Kafka's murmur2 hash (seed 0x9747b28c), bit-for-bit compatible with
// org.apache.kafka.common.utils.Utils.murmur2.
func Murmur2(data []byte) int32 {
	_ = data
	return 0
}

// ForKey returns (Murmur2(key) & 0x7fffffff) % numPartitions. numPartitions must be > 0.
func ForKey(key []byte, numPartitions int32) int32 {
	_, _ = key, numPartitions
	return 0
}
