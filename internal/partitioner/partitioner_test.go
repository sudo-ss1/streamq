package partitioner

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Vectors from Kafka's UtilsTest.testMurmur2 and DefaultPartitionerTest.
func TestMurmur2KafkaVectors(t *testing.T) {
	tests := []struct {
		in   string
		want int32
	}{
		{"21", -973932308},
		{"foobar", -790332482},
		{"a-little-bit-long-string", -985981536},
		{"a-little-bit-longer-string", -1486304829},
		{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971},
		{"abc", 479470107},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.want, Murmur2([]byte(tt.in)))
		})
	}
}

func TestMurmur2TailLengths(t *testing.T) {
	// Every tail length (0..3) must be handled without panicking and deterministically.
	for n := 0; n <= 8; n++ {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(0xF0 + i) // high bit set: bytes must be treated as unsigned
		}
		require.Equal(t, Murmur2(b), Murmur2(append([]byte(nil), b...)))
	}
	require.Equal(t, Murmur2(nil), Murmur2([]byte{}))
}

func TestForKey(t *testing.T) {
	tests := []struct {
		key  string
		n    int32
		want int32
	}{
		// (-973932308 & 0x7fffffff) % n, computed by hand from the Kafka vector.
		{"21", 1, 0},
		{"21", 6, (-973932308 & 0x7fffffff) % 6},
		{"foobar", 10, (-790332482 & 0x7fffffff) % 10},
		{"abc", 3, 479470107 % 3},
	}
	for _, tt := range tests {
		t.Run(tt.key+"/"+strconv.Itoa(int(tt.n)), func(t *testing.T) {
			got := ForKey([]byte(tt.key), tt.n)
			require.Equal(t, tt.want, got)
			require.GreaterOrEqual(t, got, int32(0))
			require.Less(t, got, tt.n)
		})
	}
}

func TestForKeyDistribution(t *testing.T) {
	const (
		partitions = 8
		keys       = 80000
	)
	counts := make([]int, partitions)
	for i := range keys {
		p := ForKey([]byte("key-"+strconv.Itoa(i)), partitions)
		require.GreaterOrEqual(t, p, int32(0))
		require.Less(t, p, int32(partitions))
		counts[p]++
	}
	expected := keys / partitions
	for p, c := range counts {
		// Allow ±10% around the uniform expectation.
		require.InDelta(t, expected, c, float64(expected)/10, "partition %d", p)
	}
}
