package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

func BenchmarkAppend(b *testing.B) {
	value := bytes.Repeat([]byte{'v'}, 1024)
	for _, perBatch := range []int{1, 100} {
		for _, policy := range []FlushPolicy{FlushNever, FlushInterval} {
			b.Run(fmt.Sprintf("records=%d/flush=%s", perBatch, policy), func(b *testing.B) {
				cfg := DefaultLogConfig()
				cfg.FlushPolicy = policy
				l, err := openLog(context.Background(), b.TempDir(), TopicPartition{Topic: "bench", Partition: 0},
					cfg, 0, SystemClock{}, discardLogger(), func(*segment) {})
				require.NoError(b, err)
				defer func() { require.NoError(b, l.Close()) }()
				vals := make([][]byte, perBatch)
				for i := range vals {
					vals[i] = value
				}
				template := buildBatch(b, 1, vals...)
				batch := make(record.Batch, len(template))
				ctx := context.Background()
				b.SetBytes(int64(len(template)))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					copy(batch, template)
					if _, err := l.Append(ctx, batch, 0); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N*perBatch)/b.Elapsed().Seconds(), "records/s")
			})
		}
	}
}

func BenchmarkRead(b *testing.B) {
	cfg := DefaultLogConfig()
	l, err := openLog(context.Background(), b.TempDir(), TopicPartition{Topic: "bench", Partition: 0},
		cfg, 0, SystemClock{}, discardLogger(), func(*segment) {})
	require.NoError(b, err)
	defer func() { require.NoError(b, l.Close()) }()
	ctx := context.Background()
	vals := make([][]byte, 100)
	for i := range vals {
		vals[i] = bytes.Repeat([]byte{'v'}, 1024)
	}
	for range 1000 {
		_, err := l.Append(ctx, buildBatch(b, 1, vals...), 0)
		require.NoError(b, err)
	}
	leo := l.LogEndOffset()
	b.ReportAllocs()
	b.ResetTimer()
	var n int64
	for i := range b.N {
		res, err := l.Read(ctx, int64(i*100)%leo, 1<<20, leo)
		if err != nil {
			b.Fatal(err)
		}
		n += int64(len(res.Data))
	}
	b.SetBytes(n / int64(b.N))
}
