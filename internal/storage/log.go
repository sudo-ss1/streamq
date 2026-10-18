package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
)

// Log is the append-only log of one partition: an ordered list of segments, the last of which is
// active (SPEC §3.2).
//
// Publication order (readers are lock-free, see doc.go): retention stores logStartOffset before
// publishing the shortened segment slice; a roll publishes the grown slice before the append
// stores the new LEO. Read loads logStartOffset, then LEO, then the slice.
type Log struct {
	mu sync.Mutex // single writer; see the lock ordering in doc.go
	// fsyncMu serializes every fsync with the failure check that follows it, so a Flush can never
	// advance the recovery point between another fsync's error and its fail() call (D7).
	fsyncMu  sync.Mutex
	segments atomic.Pointer[[]*segment]

	logStartOffset atomic.Int64
	leo            atomic.Int64
	recoveryPoint  atomic.Int64
	lastFlushMs    atomic.Int64
	failed         atomic.Bool
	closed         atomic.Bool // set under mu

	tp       TopicPartition
	dir      string
	cfg      LogConfig
	clock    Clock
	logger   *slog.Logger
	onDelete func(*segment)

	// testHookAfterFsync, if set, runs in flushTo after the fsyncs and before the recovery point
	// is advanced.
	testHookAfterFsync func()
}

// AppendInfo describes an appended batch.
type AppendInfo struct {
	FirstOffset  int64
	LastOffset   int64
	MaxTimestamp int64
}

// ReadResult is the result of a Read. Data holds zero or more whole batches.
type ReadResult struct {
	Data           []byte
	LogStartOffset int64
	LogEndOffset   int64
}

func (l *Log) loadSegments() []*segment { return *l.segments.Load() }

func (l *Log) active() *segment {
	segs := l.loadSegments()
	return segs[len(segs)-1]
}

// TopicPartition returns the partition this log belongs to.
func (l *Log) TopicPartition() TopicPartition { return l.tp }

// Dir returns the log's directory.
func (l *Log) Dir() string { return l.dir }

// Config returns the log's configuration.
func (l *Log) Config() LogConfig { return l.cfg }

// LogStartOffset returns the first offset still in the log.
func (l *Log) LogStartOffset() int64 { return l.logStartOffset.Load() }

// LogEndOffset returns the next offset to be written (LEO).
func (l *Log) LogEndOffset() int64 { return l.leo.Load() }

// RecoveryPoint returns the offset below which all data is known to be fsynced.
func (l *Log) RecoveryPoint() int64 { return l.recoveryPoint.Load() }

// Size returns the total size of all segments in bytes.
func (l *Log) Size() int64 {
	var n int64
	for _, s := range l.loadSegments() {
		n += s.size.Load()
	}
	return n
}

// SegmentCount returns the number of live segments.
func (l *Log) SegmentCount() int { return len(l.loadSegments()) }

// syncAndCheck runs an fsync under fsyncMu and marks the log failed (inside the lock) if it
// errors. A failed log rejects further syncs.
func (l *Log) syncAndCheck(sync func() error) error {
	l.fsyncMu.Lock()
	defer l.fsyncMu.Unlock()
	if l.failed.Load() {
		return ErrLogFailed
	}
	if err := sync(); err != nil {
		return l.fail(err)
	}
	return nil
}

// fail puts the log into the failed state (DECISIONS D7) and returns err wrapped in ErrLogFailed.
func (l *Log) fail(err error) error {
	if l.failed.CompareAndSwap(false, true) {
		l.logger.Error("log failed; rejecting further writes", slog.Any("error", err))
	}
	return fmt.Errorf("%w: %w", ErrLogFailed, err)
}

// Append validates b, assigns its offsets starting at the LEO and stamps leaderEpoch, both in
// place in b, and appends it. With FlushAlways it is fsynced before Append returns.
func (l *Log) Append(ctx context.Context, b record.Batch, leaderEpoch int32) (AppendInfo, error) {
	if err := ctx.Err(); err != nil {
		return AppendInfo{}, err
	}
	batch, err := record.Validate(b, l.cfg.MaxMessageBytes)
	switch {
	case errors.Is(err, record.ErrTooLarge):
		return AppendInfo{}, fmt.Errorf("%w: %d bytes > %d", ErrMessageTooLarge, len(b), l.cfg.MaxMessageBytes)
	case err != nil:
		return AppendInfo{}, fmt.Errorf("%w: %w", ErrCorruptBatch, err)
	}
	if batch.MaxTimestamp() < 0 {
		return AppendInfo{}, fmt.Errorf("%w: maxTimestamp %d", ErrInvalidTimestamp, batch.MaxTimestamp())
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return AppendInfo{}, ErrClosed
	}
	if l.failed.Load() {
		return AppendInfo{}, ErrLogFailed
	}
	now := l.clock.Now()
	seg := l.active()
	if seg.needsRoll(batch, now) {
		if seg, err = l.roll(now); err != nil {
			return AppendInfo{}, l.fail(err)
		}
	}
	batch.SetBaseOffset(l.leo.Load())
	batch.SetLeaderEpoch(leaderEpoch)
	if err := seg.append(batch); err != nil {
		return AppendInfo{}, l.fail(err)
	}
	l.leo.Store(batch.NextOffset())
	if l.cfg.FlushPolicy == FlushAlways {
		if err := l.syncAndCheck(func() error { return seg.log.Sync() }); err != nil {
			return AppendInfo{}, fmt.Errorf("sync segment %d: %w", seg.baseOffset, err)
		}
		l.recoveryPoint.Store(batch.NextOffset())
		l.lastFlushMs.Store(now.UnixMilli())
	}
	return AppendInfo{FirstOffset: batch.BaseOffset(), LastOffset: batch.LastOffset(), MaxTimestamp: batch.MaxTimestamp()}, nil
}

// roll seals the active segment and publishes a new one starting at the LEO. Caller holds mu.
func (l *Log) roll(now time.Time) (*segment, error) {
	old := l.active()
	if err := l.syncAndCheck(old.onRoll); err != nil {
		return nil, fmt.Errorf("seal segment %d: %w", old.baseOffset, err)
	}
	seg, err := createSegment(l.dir, l.leo.Load(), l.cfg, now)
	if err != nil {
		return nil, fmt.Errorf("create segment %d: %w", l.leo.Load(), err)
	}
	if err := syncDir(l.dir); err != nil {
		_ = seg.deleteFiles()
		return nil, err
	}
	segs := l.loadSegments()
	next := make([]*segment, len(segs), len(segs)+1)
	copy(next, segs)
	next = append(next, seg)
	l.segments.Store(&next)
	l.logger.Debug("rolled segment", slog.Int64("base_offset", seg.baseOffset))
	return seg, nil
}

// Read returns whole batches starting with the one containing startOffset, bounded by maxBytes
// (the first batch is always returned whole) and by maxOffset: only batches whose last offset is
// below maxOffset are returned. Consumers pass the high watermark; replica fetches pass the LEO.
// It is lock-free and may run concurrently with Append and retention.
func (l *Log) Read(ctx context.Context, startOffset int64, maxBytes int, maxOffset int64) (ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return ReadResult{}, err
	}
	logStart := l.logStartOffset.Load()
	leo := l.leo.Load()
	res := ReadResult{LogStartOffset: logStart, LogEndOffset: leo}
	if startOffset < logStart || startOffset > leo {
		return res, fmt.Errorf("%w: offset %d not in [%d, %d]", ErrOffsetOutOfRange, startOffset, logStart, leo)
	}
	maxOffset = min(maxOffset, leo)
	if startOffset >= maxOffset {
		return res, nil
	}
	segs := l.loadSegments()
	i := sort.Search(len(segs), func(i int) bool { return segs[i].baseOffset > startOffset }) - 1
	if i < 0 {
		return res, fmt.Errorf("%w: offset %d was deleted", ErrOffsetOutOfRange, startOffset)
	}
	for ; i < len(segs); i++ {
		data, err := segs[i].read(startOffset, maxBytes, maxOffset)
		if err != nil {
			return res, l.readError(startOffset, err)
		}
		if len(data) > 0 {
			res.Data = data
			return res, nil
		}
	}
	return res, nil
}

// readError maps a read against a closed file to ErrOffsetOutOfRange (segment removed by
// retention) or ErrClosed (log closed) — DECISIONS D7.
func (l *Log) readError(offset int64, err error) error {
	if !errors.Is(err, os.ErrClosed) {
		return err
	}
	if offset < l.logStartOffset.Load() {
		return fmt.Errorf("%w: offset %d was deleted", ErrOffsetOutOfRange, offset)
	}
	return fmt.Errorf("%w: %w", ErrClosed, err)
}

// OffsetForTimestamp returns the earliest offset whose record timestamp is >= ts, considering
// only batches whose last offset is below maxOffset. found is false if there is none.
func (l *Log) OffsetForTimestamp(ctx context.Context, ts int64, maxOffset int64) (offset int64, found bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	maxOffset = min(maxOffset, l.leo.Load())
	for _, s := range l.loadSegments() {
		if s.baseOffset >= maxOffset {
			break
		}
		if s.maxTimestamp.Load() < ts {
			continue
		}
		off, ok, err := s.findOffsetByTimestamp(ts, maxOffset)
		if err != nil {
			if errors.Is(err, os.ErrClosed) && !l.closed.Load() && !l.isLive(s) {
				continue // removed by retention during the scan
			}
			return 0, false, l.readError(s.baseOffset, err)
		}
		if ok {
			return off, true, nil
		}
	}
	return 0, false, nil
}

// Flush fsyncs every segment holding data above the recovery point and then advances the
// recovery point to the LEO observed at the start. fsync runs outside mu so appends continue.
func (l *Log) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.closed.Load() {
		l.mu.Unlock()
		return ErrClosed
	}
	leo := l.leo.Load()
	segs := l.loadSegments()
	l.mu.Unlock()
	return l.flushTo(leo, segs)
}

// flushTo fsyncs segs holding data in [recoveryPoint, leo) and advances the recovery point.
func (l *Log) flushTo(leo int64, segs []*segment) error {
	if l.failed.Load() {
		return ErrLogFailed
	}
	rp := l.recoveryPoint.Load()
	if leo <= rp {
		return nil
	}
	l.fsyncMu.Lock()
	defer l.fsyncMu.Unlock()
	for _, s := range segs {
		if s.nextOffset.Load() <= rp {
			continue
		}
		if err := s.flush(); err != nil {
			closedFile := errors.Is(err, os.ErrClosed) || errors.Is(err, ErrClosed)
			if closedFile && !l.failed.Load() && (l.closed.Load() || !l.isLive(s)) {
				continue // deleted by retention, or Close flushed it; nothing left to protect
			}
			return l.fail(err)
		}
	}
	if l.testHookAfterFsync != nil {
		l.testHookAfterFsync()
	}
	// Another fsync (roll, FlushAlways append) may have failed before we took fsyncMu; Linux
	// reports a writeback error to only one fsync caller, so ours succeeding proves nothing (D7).
	if l.failed.Load() {
		return ErrLogFailed
	}
	for {
		cur := l.recoveryPoint.Load()
		if cur >= leo || l.recoveryPoint.CompareAndSwap(cur, leo) {
			break
		}
	}
	l.lastFlushMs.Store(l.clock.Now().UnixMilli())
	return nil
}

func (l *Log) isLive(s *segment) bool {
	for _, x := range l.loadSegments() {
		if x == s {
			return true
		}
	}
	return false
}

// MaybeFlush flushes when the policy is FlushInterval, there is unflushed data and at least
// FlushMs passed since the last flush.
func (l *Log) MaybeFlush(ctx context.Context, now time.Time) error {
	if l.cfg.FlushPolicy != FlushInterval || l.leo.Load() <= l.recoveryPoint.Load() {
		return nil
	}
	if now.UnixMilli()-l.lastFlushMs.Load() < l.cfg.FlushMs {
		return nil
	}
	return l.Flush(ctx)
}

// DeleteOldSegments applies time and size retention (SPEC §3.2) to non-active segments whose
// data lies entirely below limitOffset (the high watermark). It only ever removes a contiguous
// prefix of the oldest segments, and deletes them safely (SPEC §3.3): they leave the segment list
// first, their files are renamed with a ".deleted" suffix, and the Manager unlinks them after
// file.delete.delay.ms. It returns the number of segments removed.
func (l *Log) DeleteOldSegments(ctx context.Context, now time.Time, limitOffset int64) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return 0, ErrClosed
	}
	segs := l.loadSegments()
	var total int64
	for _, s := range segs {
		total += s.size.Load()
	}
	n := 0
	for ; n < len(segs)-1; n++ {
		s := segs[n]
		if s.nextOffset.Load() > limitOffset {
			break
		}
		expired := l.cfg.RetentionMs >= 0 && s.maxTimestamp.Load() < now.UnixMilli()-l.cfg.RetentionMs
		oversize := l.cfg.RetentionBytes >= 0 && total-s.size.Load() >= l.cfg.RetentionBytes
		if !expired && !oversize {
			break
		}
		total -= s.size.Load()
	}
	if n == 0 {
		return 0, nil
	}
	removed := segs[:n]
	remaining := make([]*segment, len(segs)-n)
	copy(remaining, segs[n:])
	l.logStartOffset.Store(remaining[0].baseOffset)
	l.segments.Store(&remaining)
	var errs []error
	for _, s := range removed {
		if err := s.renameForDelete(); err != nil {
			errs = append(errs, err)
		}
		// Make each rename durable before the next, oldest first, so a crash can never leave an
		// older segment un-renamed next to a renamed newer one (an offset gap that recovery
		// would refuse to open, D9).
		if err := syncDir(l.dir); err != nil {
			errs = append(errs, err)
		}
		l.onDelete(s)
	}
	l.logger.Info("deleted old segments",
		slog.Int("count", n), slog.Int64("log_start_offset", remaining[0].baseOffset))
	if err := errors.Join(errs...); err != nil {
		return n, fmt.Errorf("rename deleted segments: %w", err)
	}
	return n, nil
}

// Close flushes and closes every segment. Later calls return ErrClosed.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return ErrClosed
	}
	l.closed.Store(true)
	segs := l.loadSegments()
	var errs []error
	if !l.failed.Load() {
		if err := l.flushTo(l.leo.Load(), segs); err != nil {
			errs = append(errs, err)
		}
	}
	for _, s := range segs {
		if err := s.close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// syncDir fsyncs a directory so that file creations and renames in it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for sync: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir %s: %w", dir, err)
	}
	return nil
}
