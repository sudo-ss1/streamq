package partition

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/storage"
)

func TestEpochCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c, err := OpenEpochCache(ctx, dir, 0)
	require.NoError(t, err)
	_, ok := c.LatestEpoch()
	require.False(t, ok)
	require.NoError(t, c.Assign(ctx, 1, 0))
	require.NoError(t, c.Assign(ctx, 3, 100))
	require.NoError(t, c.Assign(ctx, 3, 150), "same epoch: no-op")
	require.NoError(t, c.Assign(ctx, 2, 160), "older epoch: no-op")
	require.NoError(t, c.Assign(ctx, 5, 200))
	require.Equal(t, []storage.EpochEntry{{Epoch: 1, StartOffset: 0}, {Epoch: 3, StartOffset: 100}, {Epoch: 5, StartOffset: 200}}, c.Entries())

	tests := []struct {
		req       int32
		wantEpoch int32
		wantEnd   int64
	}{
		{1, 1, 100}, // exact
		{2, 1, 100}, // between: the largest epoch <= 2 is 1
		{3, 3, 200}, // exact
		{4, 3, 200}, // between
		{5, 5, 250}, // latest ends at the LEO
		{9, 5, 250}, // newer than any
		{0, -1, -1}, // below every entry
	}
	for _, tt := range tests {
		e, end := c.EndOffsetFor(tt.req, 250)
		require.Equal(t, tt.wantEpoch, e, "epoch %d", tt.req)
		require.Equal(t, tt.wantEnd, end, "epoch %d", tt.req)
	}

	// Persistence, and open truncates entries beyond the recovered LEO.
	c2, err := OpenEpochCache(ctx, dir, 150)
	require.NoError(t, err)
	require.Equal(t, []storage.EpochEntry{{Epoch: 1, StartOffset: 0}, {Epoch: 3, StartOffset: 100}}, c2.Entries())
	start, ok := c2.StartOffset(3)
	require.True(t, ok)
	require.Equal(t, int64(100), start)

	require.NoError(t, c2.TruncateFromEnd(ctx, 100))
	require.Equal(t, []storage.EpochEntry{{Epoch: 1, StartOffset: 0}}, c2.Entries())
	require.NoError(t, c2.Assign(ctx, 4, 120))
	require.NoError(t, c2.TruncateFromStart(ctx, 130))
	require.Equal(t, []storage.EpochEntry{{Epoch: 4, StartOffset: 130}}, c2.Entries())
	require.NoError(t, c2.TruncateFromStart(ctx, 10), "below the first entry: no-op")
	require.Equal(t, []storage.EpochEntry{{Epoch: 4, StartOffset: 130}}, c2.Entries())

	c3, err := OpenEpochCache(ctx, dir, 1000)
	require.NoError(t, err)
	require.Equal(t, c2.Entries(), c3.Entries())
}
