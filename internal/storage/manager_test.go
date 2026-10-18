package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagerGetOrCreateAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	clock := newFakeClock()
	cfg := smallConfig()
	m, err := OpenManager(ctx, DefaultManagerConfig(dir), clock, discardLogger())
	require.NoError(t, err)
	a := TopicPartition{Topic: "a-b", Partition: 0}
	b := TopicPartition{Topic: "a-b", Partition: 1}
	la, err := m.GetOrCreateLog(ctx, a, cfg)
	require.NoError(t, err)
	again, err := m.GetOrCreateLog(ctx, a, cfg)
	require.NoError(t, err)
	require.Same(t, la, again)
	lb, err := m.GetOrCreateLog(ctx, b, cfg)
	require.NoError(t, err)
	appendRecords(t, la, 5, 1)
	appendRecords(t, lb, 2, 1)
	got, ok := m.Log(b)
	require.True(t, ok)
	require.Same(t, lb, got)
	require.Len(t, m.Logs(), 2)

	require.NoError(t, m.Close(ctx))
	_, err = m.GetOrCreateLog(ctx, a, cfg)
	require.ErrorIs(t, err, ErrClosed)
	rps, err := ReadOffsetCheckpoint(ctx, filepath.Join(dir, RecoveryPointCheckpointFile))
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{a: 5, b: 2}, rps)

	// Reopen: only "a" is opened again; "b" keeps its checkpointed recovery point.
	m, err = OpenManager(ctx, DefaultManagerConfig(dir), clock, discardLogger())
	require.NoError(t, err)
	la, err = m.GetOrCreateLog(ctx, a, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(5), la.LogEndOffset())
	require.Equal(t, int64(5), la.RecoveryPoint())
	appendRecords(t, la, 1, 1)
	require.NoError(t, m.CheckpointRecoveryPoints(ctx))
	rps, err = ReadOffsetCheckpoint(ctx, filepath.Join(dir, RecoveryPointCheckpointFile))
	require.NoError(t, err)
	require.Equal(t, map[TopicPartition]int64{a: 5, b: 2}, rps, "unflushed data must not advance the recovery point")
	require.NoError(t, m.Close(ctx))
}

func TestManagerFlushDirty(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	m, err := OpenManager(ctx, DefaultManagerConfig(t.TempDir()), clock, discardLogger())
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close(ctx)) }()
	cfg := smallConfig()
	cfg.FlushPolicy = FlushInterval
	cfg.FlushMs = 500
	l, err := m.GetOrCreateLog(ctx, TopicPartition{Topic: "t", Partition: 0}, cfg)
	require.NoError(t, err)
	appendRecords(t, l, 3, 1)
	require.NoError(t, m.FlushDirty(ctx, clock.Now()))
	require.Equal(t, int64(0), l.RecoveryPoint())
	clock.Advance(500 * time.Millisecond)
	require.NoError(t, m.FlushDirty(ctx, clock.Now()))
	require.Equal(t, int64(3), l.RecoveryPoint())
}

func TestManagerRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := DefaultManagerConfig(t.TempDir())
	cfg.FlushCheckInterval = time.Millisecond
	cfg.CheckpointInterval = time.Millisecond
	cfg.RetentionCheckInterval = time.Millisecond
	m, err := OpenManager(ctx, cfg, SystemClock{}, discardLogger())
	require.NoError(t, err)
	lcfg := smallConfig()
	lcfg.FlushPolicy = FlushInterval
	lcfg.FlushMs = 1
	l, err := m.GetOrCreateLog(ctx, TopicPartition{Topic: "t", Partition: 0}, lcfg)
	require.NoError(t, err)

	done := make(chan error)
	go func() { done <- m.Run(ctx) }()
	appendRecords(t, l, 3, time.Now().UnixMilli())
	require.Eventually(t, func() bool { return l.RecoveryPoint() == 3 }, 5*time.Second, time.Millisecond)
	cancel()
	require.NoError(t, <-done)
	require.NoError(t, m.Close(context.Background()))
}
