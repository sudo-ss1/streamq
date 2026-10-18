// Package partitioner implements key-based partition selection (SPEC §7.4), shared by the client
// library, streamqctl and tests.
package partitioner

import "encoding/binary"

const (
	murmurSeed uint32 = 0x9747b28c
	murmurM    uint32 = 0x5bd1e995
	murmurR           = 24
)

// Murmur2 is Kafka's murmur2 hash (seed 0x9747b28c), bit-for-bit compatible with
// org.apache.kafka.common.utils.Utils.murmur2.
func Murmur2(data []byte) int32 {
	length := len(data)
	h := murmurSeed ^ uint32(length)

	tail := length &^ 3
	for i := 0; i < tail; i += 4 {
		k := binary.LittleEndian.Uint32(data[i:])
		k *= murmurM
		k ^= k >> murmurR
		k *= murmurM
		h *= murmurM
		h ^= k
	}

	// Kafka's switch falls through from case 3 to case 1.
	switch length % 4 {
	case 3:
		h ^= uint32(data[tail+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[tail+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[tail])
		h *= murmurM
	}

	h ^= h >> 13
	h *= murmurM
	h ^= h >> 15
	return int32(h)
}

// ForKey returns (Murmur2(key) & 0x7fffffff) % numPartitions. numPartitions must be > 0.
func ForKey(key []byte, numPartitions int32) int32 {
	return (Murmur2(key) & 0x7fffffff) % numPartitions
}
