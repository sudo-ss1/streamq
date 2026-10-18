//go:build integration

package integration

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/partitioner"
	"github.com/sudo-ss1/streamq/internal/testnode"
	"github.com/sudo-ss1/streamq/pkg/client"
)

// TestClientRoundTrip produces 10k keyed records with the client library's batching producer and
// reads them back with the standalone consumer, verifying placement and per-key order (M3).
func TestClientRoundTrip(t *testing.T) {
	const (
		topic      = "client-rt"
		partitions = 4
		total      = 10_000
		keys       = 100
	)
	addr := testnode.Start(t)
	ctx := testCtx(t)
	c, err := client.New(client.Config{Bootstrap: []string{addr}})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	require.NoError(t, client.NewAdmin(c).CreateTopic(ctx, topic, partitions, 1, map[string]string{"min_insync_replicas": "1"}))

	p, err := client.NewProducer(c, client.ProducerConfig{})
	require.NoError(t, err)
	var mu sync.Mutex
	var errs []error
	for i := range total {
		k := i % keys
		rec := client.Record{Topic: topic, Key: []byte("key-" + strconv.Itoa(k)), Value: []byte(fmt.Sprintf("key-%d/%d", k, i/keys))}
		require.NoError(t, p.Produce(ctx, rec, func(_ client.RecordMetadata, err error) {
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}))
	}
	require.NoError(t, p.Close(ctx))
	require.Empty(t, errs)

	cons, err := client.NewConsumer(c, client.ConsumerConfig{AutoOffsetReset: client.ResetEarliest})
	require.NoError(t, err)
	defer func() { require.NoError(t, cons.Close()) }()
	var tps []client.TopicPartition
	for part := range int32(partitions) {
		tps = append(tps, client.TopicPartition{Topic: topic, Partition: part})
	}
	cons.Assign(tps)
	next := map[string]int{}
	n := 0
	for n < total {
		recs, err := cons.Poll(ctx)
		require.NoError(t, err)
		for _, r := range recs {
			key, seq, ok := strings.Cut(string(r.Value), "/")
			require.True(t, ok)
			require.Equal(t, key, string(r.Key))
			require.Equal(t, partitioner.ForKey(r.Key, partitions), r.Partition)
			require.Equal(t, strconv.Itoa(next[key]), seq, "per-key order")
			next[key]++
			n++
		}
	}
	require.Equal(t, total, n)
}
