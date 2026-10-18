package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/record"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// smallConfig returns a config with tiny segments and dense indexes so tests exercise rolls and
// index lookups with little data.
func smallConfig() LogConfig {
	cfg := DefaultLogConfig()
	cfg.SegmentBytes = 4096
	cfg.IndexIntervalBytes = 128
	cfg.MaxIndexBytes = 1200
	cfg.MaxMessageBytes = 2048
	cfg.FlushPolicy = FlushNever
	return cfg
}

type testLog struct {
	*Log
	deleted []*segment
}

func openTestLog(t *testing.T, dir string, cfg LogConfig, clock Clock, recoveryPoint int64) *testLog {
	t.Helper()
	tl := &testLog{}
	l, err := openLog(context.Background(), dir, TopicPartition{Topic: "orders-eu", Partition: 3}, cfg, recoveryPoint, clock, discardLogger(),
		func(s *segment) { tl.deleted = append(tl.deleted, s) })
	require.NoError(t, err)
	tl.Log = l
	return tl
}

// closeTestLog closes the log and any segments retired by retention.
func (tl *testLog) close(t *testing.T) {
	t.Helper()
	require.NoError(t, tl.Close())
	for _, s := range tl.deleted {
		require.NoError(t, s.deleteFiles())
	}
	tl.deleted = nil
}

// offsetValue encodes an offset as a record value so readers can verify what they got.
func offsetValue(off int64) []byte {
	v := make([]byte, 8, 40)
	binary.BigEndian.PutUint64(v, uint64(off))
	return append(v, bytes.Repeat([]byte{'x'}, 32)...)
}

// appendRecords appends one batch of n records whose values encode their offsets.
func appendRecords(t *testing.T, l *Log, n int, ts int64) AppendInfo {
	t.Helper()
	next := l.LogEndOffset()
	vals := make([][]byte, n)
	for i := range vals {
		vals[i] = offsetValue(next + int64(i))
	}
	info, err := l.Append(context.Background(), buildBatch(t, ts, vals...), 7)
	require.NoError(t, err)
	require.Equal(t, next, info.FirstOffset)
	require.Equal(t, next+int64(n)-1, info.LastOffset)
	return info
}

// decodeChecked decodes concatenated batches, checking each batch is valid and each record's
// value encodes its offset. It returns an error instead of failing so goroutines can use it.
func decodeChecked(data []byte) ([]record.Record, error) {
	batches, err := record.SplitBatches(data)
	if err != nil {
		return nil, err
	}
	var out []record.Record
	for _, b := range batches {
		if _, err := record.Validate(b, len(b)); err != nil {
			return nil, err
		}
		recs, err := b.Records()
		if err != nil {
			return nil, err
		}
		for _, r := range recs {
			if len(r.Value) < 8 || uint64(r.Offset) != binary.BigEndian.Uint64(r.Value) {
				return nil, fmt.Errorf("record at offset %d carries the wrong value", r.Offset)
			}
		}
		out = append(out, recs...)
	}
	return out, nil
}

// decodeAll is decodeChecked for the test goroutine.
func decodeAll(tb testing.TB, data []byte) []record.Record {
	tb.Helper()
	recs, err := decodeChecked(data)
	require.NoError(tb, err)
	return recs
}

// readAll reads every record in [from, maxOffset) by repeated Reads.
func readAll(t *testing.T, l *Log, from, maxOffset int64) []record.Record {
	t.Helper()
	var out []record.Record
	for from < maxOffset {
		res, err := l.Read(context.Background(), from, 1024, maxOffset)
		require.NoError(t, err)
		if len(res.Data) == 0 {
			break
		}
		recs := decodeAll(t, res.Data)
		for _, r := range recs {
			if r.Offset >= from {
				out = append(out, r)
			}
		}
		from = recs[len(recs)-1].Offset + 1
	}
	return out
}

func requireContiguous(t *testing.T, recs []record.Record, from, to int64) {
	t.Helper()
	require.Len(t, recs, int(to-from))
	for i, r := range recs {
		require.Equal(t, from+int64(i), r.Offset)
	}
}

func TestLogAppendAssignsContiguousOffsets(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	for i := 1; i <= 20; i++ {
		appendRecords(t, l.Log, i%4+1, 1000)
	}
	require.Equal(t, int64(0), l.LogStartOffset())
	leo := l.LogEndOffset()
	recs := readAll(t, l.Log, 0, leo)
	requireContiguous(t, recs, 0, leo)
	res, err := l.Read(context.Background(), 0, 1<<20, leo)
	require.NoError(t, err)
	b, err := record.SplitBatches(res.Data)
	require.NoError(t, err)
	require.Equal(t, int32(7), b[0].LeaderEpoch())
}

func TestLogRead(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	// Batches: [0..4] [5..9] [10..14] [15..19], each 5 records.
	for range 4 {
		appendRecords(t, l.Log, 5, 1000)
	}
	ctx := context.Background()
	res, err := l.Read(ctx, 0, 1<<20, 20)
	require.NoError(t, err)
	batches, err := record.SplitBatches(res.Data)
	require.NoError(t, err)
	batchSize := len(batches[0])

	tests := []struct {
		name        string
		start       int64
		maxBytes    int
		maxOffset   int64
		wantOffsets []int64 // base offsets of returned batches
		wantErr     error
	}{
		{name: "from start", start: 0, maxBytes: 1 << 20, maxOffset: 20, wantOffsets: []int64{0, 5, 10, 15}},
		{name: "inside batch returns containing batch", start: 7, maxBytes: 1 << 20, maxOffset: 20, wantOffsets: []int64{5, 10, 15}},
		{name: "maxBytes smaller than first batch still returns it whole", start: 0, maxBytes: 1, maxOffset: 20, wantOffsets: []int64{0}},
		{name: "maxBytes cuts at whole batches", start: 0, maxBytes: 2*batchSize + batchSize/2, maxOffset: 20, wantOffsets: []int64{0, 5}},
		{name: "maxOffset at batch boundary", start: 0, maxBytes: 1 << 20, maxOffset: 10, wantOffsets: []int64{0, 5}},
		{name: "maxOffset inside batch excludes it", start: 0, maxBytes: 1 << 20, maxOffset: 12, wantOffsets: []int64{0, 5}},
		{name: "start at maxOffset is empty", start: 10, maxBytes: 1 << 20, maxOffset: 10},
		{name: "start at LEO is empty", start: 20, maxBytes: 1 << 20, maxOffset: 20},
		{name: "start above maxOffset but below LEO is empty", start: 15, maxBytes: 1 << 20, maxOffset: 10},
		{name: "above LEO", start: 21, maxBytes: 1 << 20, maxOffset: 21, wantErr: ErrOffsetOutOfRange},
		{name: "negative", start: -1, maxBytes: 1 << 20, maxOffset: 20, wantErr: ErrOffsetOutOfRange},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := l.Read(ctx, tt.start, tt.maxBytes, tt.maxOffset)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(20), res.LogEndOffset)
			batches, err := record.SplitBatches(res.Data)
			require.NoError(t, err)
			var got []int64
			for _, b := range batches {
				got = append(got, b.BaseOffset())
			}
			require.Equal(t, tt.wantOffsets, got)
			decodeAll(t, res.Data)
		})
	}
}

func TestLogAppendRejectsInvalidBatches(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	ctx := context.Background()

	big := buildBatch(t, 1, bytes.Repeat([]byte{1}, 3000))
	_, err := l.Append(ctx, big, 0)
	require.ErrorIs(t, err, ErrMessageTooLarge)

	bad := buildBatch(t, 1, []byte("hello"))
	bad[len(bad)-1] ^= 0xff
	_, err = l.Append(ctx, bad, 0)
	require.ErrorIs(t, err, ErrCorruptBatch)

	require.Equal(t, int64(0), l.LogEndOffset())
	require.Equal(t, int64(0), l.Size())
}

func TestLogRollBySize(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	for range 200 {
		appendRecords(t, l.Log, 3, 1000)
	}
	leo := l.LogEndOffset()
	require.Greater(t, l.SegmentCount(), 5)
	for _, s := range l.loadSegments() {
		require.LessOrEqual(t, s.size.Load(), cfg.SegmentBytes)
		_, err := os.Stat(filepath.Join(dir, fmt.Sprintf("%020d.log", s.baseOffset)))
		require.NoError(t, err)
	}
	requireContiguous(t, readAll(t, l.Log, 0, leo), 0, leo)
	l.close(t)

	// A clean reopen sees the same data and segments.
	l = openTestLog(t, dir, cfg, newFakeClock(), leo)
	defer l.close(t)
	require.Equal(t, leo, l.LogEndOffset())
	requireContiguous(t, readAll(t, l.Log, 0, leo), 0, leo)
	appendRecords(t, l.Log, 2, 1000)
	require.Equal(t, leo+2, l.LogEndOffset())
}

func TestLogRollByTime(t *testing.T) {
	cfg := smallConfig()
	cfg.SegmentBytes = 1 << 20
	cfg.SegmentMs = 1000
	dir := t.TempDir()
	clock := newFakeClock()
	l := openTestLog(t, dir, cfg, clock, 0)

	// Timestamp-0 (and otherwise old) batches never cause a roll storm.
	for range 200 {
		appendRecords(t, l.Log, 1, 0)
	}
	require.Equal(t, 1, l.SegmentCount())

	// A batch whose timestamp is more than SegmentMs past the first batch rolls.
	appendRecords(t, l.Log, 1, cfg.SegmentMs+1)
	require.Equal(t, 2, l.SegmentCount())
	require.Equal(t, int64(200), l.active().baseOffset)

	// The rolling base timestamp survives a reopen.
	l.close(t)
	l = openTestLog(t, dir, cfg, clock, 0)
	defer l.close(t)
	appendRecords(t, l.Log, 1, cfg.SegmentMs+1)
	require.Equal(t, 2, l.SegmentCount())
	appendRecords(t, l.Log, 1, 2*cfg.SegmentMs+2)
	require.Equal(t, 3, l.SegmentCount())
}

func TestLogRollByTimeEmptySegmentUsesWallClock(t *testing.T) {
	cfg := smallConfig()
	cfg.SegmentBytes = 1 << 20
	cfg.SegmentMs = 1000
	clock := newFakeClock()
	l := openTestLog(t, t.TempDir(), cfg, clock, 0)
	defer l.close(t)
	// An empty segment never rolls no matter how old.
	clock.Advance(time.Hour)
	appendRecords(t, l.Log, 1, 5)
	require.Equal(t, 1, l.SegmentCount())
}

func TestLogRollWhenIndexFull(t *testing.T) {
	cfg := smallConfig()
	cfg.SegmentBytes = 1 << 20
	cfg.IndexIntervalBytes = 1 // an index entry for every batch after the first
	cfg.MaxIndexBytes = 48     // 6 offset entries, 4 time entries
	l := openTestLog(t, t.TempDir(), cfg, newFakeClock(), 0)
	defer l.close(t)
	for i := range 40 {
		appendRecords(t, l.Log, 1, int64(1000+i))
	}
	require.Greater(t, l.SegmentCount(), 3)
	requireContiguous(t, readAll(t, l.Log, 0, 40), 0, 40)
}

func TestLogOffsetForTimestamp(t *testing.T) {
	cfg := smallConfig()
	cfg.IndexIntervalBytes = 1
	l := openTestLog(t, t.TempDir(), cfg, newFakeClock(), 0)
	defer l.close(t)
	ctx := context.Background()
	// One record per batch; timestamps out of order and with repeats spanning index entries.
	tss := []int64{100, 300, 200, 300, 300, 250, 400, 400, 150, 500}
	for _, ts := range tss {
		appendRecords(t, l.Log, 1, ts)
	}
	// A multi-record batch with distinct timestamps.
	b := record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
	for i, ts := range []int64{600, 550, 700} {
		b.Append(ts, nil, offsetValue(int64(10+i)), nil)
	}
	batch, err := b.Build()
	require.NoError(t, err)
	_, err = l.Append(ctx, batch, 0)
	require.NoError(t, err)

	tests := []struct {
		ts        int64
		maxOffset int64
		want      int64
		found     bool
	}{
		{ts: math.MinInt64, maxOffset: 13, want: 0, found: true},
		{ts: 0, maxOffset: 13, want: 0, found: true},
		{ts: 100, maxOffset: 13, want: 0, found: true},
		{ts: 101, maxOffset: 13, want: 1, found: true},
		{ts: 300, maxOffset: 13, want: 1, found: true},
		{ts: 301, maxOffset: 13, want: 6, found: true},
		{ts: 450, maxOffset: 13, want: 9, found: true},
		{ts: 550, maxOffset: 13, want: 10, found: true},
		{ts: 650, maxOffset: 13, want: 12, found: true},
		{ts: 701, maxOffset: 13},
		{ts: 450, maxOffset: 9},                // the match at 9 is above the bound
		{ts: 550, maxOffset: 12, found: false}, // the batch straddles the bound
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("ts=%d,max=%d", tt.ts, tt.maxOffset), func(t *testing.T) {
			off, recTs, found, err := l.OffsetForTimestamp(ctx, tt.ts, tt.maxOffset)
			require.NoError(t, err)
			require.Equal(t, tt.found, found)
			if tt.found {
				require.Equal(t, tt.want, off)
				require.GreaterOrEqual(t, recTs, tt.ts)
			}
		})
	}
}

func TestLogFlushPolicies(t *testing.T) {
	ctx := context.Background()
	t.Run("always", func(t *testing.T) {
		cfg := smallConfig()
		cfg.FlushPolicy = FlushAlways
		l := openTestLog(t, t.TempDir(), cfg, newFakeClock(), 0)
		defer l.close(t)
		for range 5 {
			appendRecords(t, l.Log, 2, 1)
			require.Equal(t, l.LogEndOffset(), l.RecoveryPoint())
		}
	})
	t.Run("interval", func(t *testing.T) {
		cfg := smallConfig()
		cfg.FlushPolicy = FlushInterval
		cfg.FlushMs = 1000
		clock := newFakeClock()
		l := openTestLog(t, t.TempDir(), cfg, clock, 0)
		defer l.close(t)
		appendRecords(t, l.Log, 2, 1)
		require.NoError(t, l.MaybeFlush(ctx, clock.Now()))
		require.Equal(t, int64(0), l.RecoveryPoint())
		clock.Advance(999 * time.Millisecond)
		require.NoError(t, l.MaybeFlush(ctx, clock.Now()))
		require.Equal(t, int64(0), l.RecoveryPoint())
		clock.Advance(time.Millisecond)
		require.NoError(t, l.MaybeFlush(ctx, clock.Now()))
		require.Equal(t, int64(2), l.RecoveryPoint())
	})
	t.Run("never", func(t *testing.T) {
		l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
		appendRecords(t, l.Log, 2, 1)
		require.NoError(t, l.MaybeFlush(ctx, time.Now().Add(time.Hour)))
		require.Equal(t, int64(0), l.RecoveryPoint())
		require.NoError(t, l.Close())
		require.Equal(t, int64(2), l.RecoveryPoint())
	})
}

func TestLogClosed(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	appendRecords(t, l.Log, 2, 1)
	require.NoError(t, l.Close())
	require.ErrorIs(t, l.Close(), ErrClosed)
	_, err := l.Append(context.Background(), buildBatch(t, 1, []byte("x")), 0)
	require.ErrorIs(t, err, ErrClosed)
	_, err = l.Read(context.Background(), 0, 100, 2)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, l.Flush(context.Background()), ErrClosed)
}

func TestLogContextCancelled(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := l.Append(ctx, buildBatch(t, 1, []byte("x")), 0)
	require.ErrorIs(t, err, context.Canceled)
	_, err = l.Read(ctx, 0, 100, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestLogConfigValidate(t *testing.T) {
	require.NoError(t, DefaultLogConfig().Validate())
	tests := []struct {
		name string
		mod  func(*LogConfig)
	}{
		{"segment too large", func(c *LogConfig) { c.SegmentBytes = MaxSegmentBytes + 1 }},
		{"segment too small", func(c *LogConfig) { c.SegmentBytes = 10 }},
		{"message larger than segment", func(c *LogConfig) { c.MaxMessageBytes = int(c.SegmentBytes) + 1 }},
		{"zero retention", func(c *LogConfig) { c.RetentionMs = 0 }},
		{"bad retention bytes", func(c *LogConfig) { c.RetentionBytes = -2 }},
		{"index too small", func(c *LogConfig) { c.MaxIndexBytes = 12 }},
		{"bad flush policy", func(c *LogConfig) { c.FlushPolicy = "sometimes" }},
		{"zero flush interval", func(c *LogConfig) { c.FlushMs = 0 }},
		{"zero index interval", func(c *LogConfig) { c.IndexIntervalBytes = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultLogConfig()
			tt.mod(&c)
			require.Error(t, c.Validate())
		})
	}
	c := DefaultLogConfig()
	c.SegmentBytes = MaxSegmentBytes
	require.NoError(t, c.Validate())
}

func TestTopicPartitionDirName(t *testing.T) {
	tests := []struct {
		name string
		tp   TopicPartition
		ok   bool
	}{
		{"orders-0", TopicPartition{"orders", 0}, true},
		{"a-b-c-12", TopicPartition{"a-b-c", 12}, true},
		{"orders", TopicPartition{}, false},
		{"-1", TopicPartition{}, false},
		{"orders-", TopicPartition{}, false},
		{"orders-x", TopicPartition{}, false},
		{"orders--1", TopicPartition{"orders-", 1}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp, ok := ParseDirName(tt.name)
			require.Equal(t, tt.ok, ok)
			if ok {
				require.Equal(t, tt.tp, tp)
				require.Equal(t, tt.name, tp.DirName())
			}
		})
	}
}

func TestLogFailedStateIsSticky(t *testing.T) {
	dir := t.TempDir()
	cfg := smallConfig()
	l := openTestLog(t, dir, cfg, newFakeClock(), 0)
	appendRecords(t, l.Log, 3, 1)
	require.NoError(t, l.Flush(context.Background()))
	require.Equal(t, int64(3), l.RecoveryPoint())
	appendRecords(t, l.Log, 2, 1)

	// Pull the file out from under the log: the write and its rollback both fail.
	require.NoError(t, l.active().log.Close())
	_, err := l.Append(context.Background(), buildBatch(t, 1, []byte("x")), 0)
	require.ErrorIs(t, err, ErrLogFailed)
	_, err = l.Append(context.Background(), buildBatch(t, 1, []byte("x")), 0)
	require.ErrorIs(t, err, ErrLogFailed)
	require.ErrorIs(t, l.Flush(context.Background()), ErrLogFailed)
	require.Equal(t, int64(3), l.RecoveryPoint(), "recovery point must not pass a failure")
	require.Equal(t, int64(5), l.LogEndOffset())
	require.NoError(t, l.Close())
	require.Equal(t, int64(3), l.RecoveryPoint())

	// Reopening recovers everything that reached the file.
	l = openTestLog(t, dir, cfg, newFakeClock(), 3)
	defer l.close(t)
	require.Equal(t, int64(5), l.LogEndOffset())
	requireContiguous(t, readAll(t, l.Log, 0, 5), 0, 5)
}

func TestLogFlushDoesNotAdvanceAfterConcurrentFailure(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	appendRecords(t, l.Log, 3, 1)
	// Another fsync fails while this Flush is between its fsyncs and the recovery-point update.
	l.testHookAfterFsync = func() { l.failed.Store(true) }
	require.ErrorIs(t, l.Flush(context.Background()), ErrLogFailed)
	require.Equal(t, int64(0), l.RecoveryPoint())
}

func TestLogFlushDoesNotAdvancePastConcurrentTruncation(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	for range 5 {
		appendRecords(t, l.Log, 3, 1)
	}
	// A Flush captured LEO 15, then a follower truncation ran before it committed.
	leo, gen, segs := l.LogEndOffset(), l.truncGen.Load(), l.loadSegments()
	newLEO, err := l.TruncateTo(context.Background(), 6)
	require.NoError(t, err)
	require.NoError(t, l.flushTo(leo, gen, segs))
	require.LessOrEqual(t, l.RecoveryPoint(), newLEO, "recovery point never above the LEO")
}

func TestLogFlushToleratesRetiredClosedSegment(t *testing.T) {
	l := openTestLog(t, t.TempDir(), smallConfig(), newFakeClock(), 0)
	defer l.close(t)
	for l.SegmentCount() < 2 {
		appendRecords(t, l.Log, 4, 1)
	}
	// Simulate Flush capturing a segment that retention then retires and DeleteExpired closes.
	segs := l.loadSegments()
	retired := segs[0]
	remaining := []*segment{segs[1]}
	l.segments.Store(&remaining)
	require.NoError(t, retired.index.close()) // index closed first: storage.ErrClosed
	require.NoError(t, l.flushTo(l.LogEndOffset(), l.truncGen.Load(), segs))
	require.False(t, l.failed.Load())
	require.Equal(t, l.LogEndOffset(), l.RecoveryPoint())
	require.NoError(t, retired.close())
}
