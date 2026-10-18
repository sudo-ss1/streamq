package broker

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/metadata"
)

func TestProducerIDs(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	next, fail := int64(0), true
	a := NewProducerIDs(func(context.Context) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if fail {
			fail = false
			return 0, errors.New("controller unreachable")
		}
		first := next
		next += metadata.ProducerIDBlockSize
		return first, nil
	})
	_, err := a.Next(ctx)
	require.Error(t, err, "a failed reservation is returned")

	ids := make([][]int64, 8)
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 300 {
				id, err := a.Next(ctx)
				if err != nil {
					errs[g] = err
					return
				}
				ids[g] = append(ids[g], id)
			}
		}()
	}
	wg.Wait()
	seen := map[int64]bool{}
	for g := range 8 {
		require.NoError(t, errs[g])
		for _, id := range ids[g] {
			require.False(t, seen[id], "id %d handed out twice", id)
			seen[id] = true
		}
	}
	require.Len(t, seen, 2400)
	require.Equal(t, int64(3*metadata.ProducerIDBlockSize), next, "rolled to new blocks")
}

func TestLocalIDBlockSource(t *testing.T) {
	e := openEnv(t, t.TempDir())
	src := LocalIDBlockSource(e.store, 1)
	a, err := src(context.Background())
	require.NoError(t, err)
	b, err := src(context.Background())
	require.NoError(t, err)
	require.Equal(t, a+metadata.ProducerIDBlockSize, b)
}
