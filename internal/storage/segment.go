package storage

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
)

const (
	logSuffix       = ".log"
	indexSuffix     = ".index"
	timeIndexSuffix = ".timeindex"
	deletedSuffix   = ".deleted"
)

// segmentFileName returns "<20-digit base><suffix>".
func segmentFileName(base int64, suffix string) string {
	return fmt.Sprintf("%020d%s", base, suffix)
}

// segment is one .log file plus its offset and time indexes.
//
// Fields marked "writer" are only touched by the log's single writer (under Log.mu) or during
// open/recovery, before the segment is published to readers.
type segment struct {
	baseOffset int64
	dir        string
	cfg        LogConfig

	log       *os.File
	logPath   string // writer; gains ".deleted" on renameForDelete
	index     *offsetIndex
	timeIndex *timeIndex

	size         atomic.Int64 // published byte size; readers never read past it
	nextOffset   atomic.Int64 // offset after the last published batch
	maxTimestamp atomic.Int64 // largest batch maxTimestamp, -1 if empty

	offsetOfMaxTs   int64     // writer: baseOffset of the batch where maxTimestamp was first seen
	rollingBaseTs   int64     // writer: first batch's maxTimestamp, -1 if unknown (Kafka)
	createdAt       time.Time // writer: open/creation time, used while rollingBaseTs is unknown
	bytesSinceIndex int       // writer
}

// createSegment creates a new, empty segment at base, replacing any stale files with that base.
func createSegment(dir string, base int64, cfg LogConfig, now time.Time) (*segment, error) {
	for _, suffix := range []string{indexSuffix, timeIndexSuffix} {
		if err := os.Remove(filepath.Join(dir, segmentFileName(base, suffix))); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove stale index: %w", err)
		}
	}
	return newSegment(dir, base, cfg, now, os.O_CREATE|os.O_RDWR|os.O_TRUNC)
}

// openSegment opens an existing segment. Its state is unknown until loadState or recover runs.
func openSegment(dir string, base int64, cfg LogConfig, now time.Time) (*segment, error) {
	return newSegment(dir, base, cfg, now, os.O_CREATE|os.O_RDWR)
}

func newSegment(dir string, base int64, cfg LogConfig, now time.Time, flag int) (s *segment, err error) {
	s = &segment{baseOffset: base, dir: dir, cfg: cfg, rollingBaseTs: -1, createdAt: now, offsetOfMaxTs: base}
	s.nextOffset.Store(base)
	s.maxTimestamp.Store(-1)
	s.logPath = filepath.Join(dir, segmentFileName(base, logSuffix))
	s.log, err = os.OpenFile(s.logPath, flag, fileMode)
	if err != nil {
		return nil, fmt.Errorf("open segment log: %w", err)
	}
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	s.index, err = openOffsetIndex(filepath.Join(dir, segmentFileName(base, indexSuffix)), base, cfg.MaxIndexBytes)
	if err != nil {
		return nil, fmt.Errorf("open offset index: %w", err)
	}
	s.timeIndex, err = openTimeIndex(filepath.Join(dir, segmentFileName(base, timeIndexSuffix)), base, cfg.MaxIndexBytes)
	if err != nil {
		return nil, fmt.Errorf("open time index: %w", err)
	}
	return s, nil
}

// needsRoll reports whether appending batch b (offsets not yet assigned; it would start at
// nextOffset) requires a new segment first. An empty segment never rolls. The age test is Kafka's
// timeWaitedForRoll: incoming maxTimestamp minus the first batch's maxTimestamp, or wall-clock
// time since creation when the segment has no rolling base timestamp.
func (s *segment) needsRoll(b record.Batch, now time.Time) bool {
	size := s.size.Load()
	if size == 0 {
		return false
	}
	waited := now.Sub(s.createdAt).Milliseconds()
	if s.rollingBaseTs >= 0 {
		waited = b.MaxTimestamp() - s.rollingBaseTs
	}
	lastOffset := s.nextOffset.Load() + int64(b.LastOffsetDelta())
	return size+int64(len(b)) > s.cfg.SegmentBytes ||
		waited > s.cfg.SegmentMs ||
		s.index.isFull() || s.timeIndex.isFull() ||
		lastOffset-s.baseOffset > math.MaxUint32
}

// append writes one batch whose offsets are already assigned. On a failed write the file is cut
// back to its previous size so no partial batch remains.
func (s *segment) append(b record.Batch) error {
	pos := s.size.Load()
	if _, err := s.log.WriteAt(b, pos); err != nil {
		if terr := s.log.Truncate(pos); terr != nil {
			return fmt.Errorf("write batch: %w (truncate back failed: %w)", err, terr)
		}
		return fmt.Errorf("write batch: %w", err)
	}
	if err := s.indexBatch(b, pos, false); err != nil {
		if terr := s.log.Truncate(pos); terr != nil {
			return fmt.Errorf("%w (truncate back failed: %w)", err, terr)
		}
		return err
	}
	if pos == 0 {
		s.rollingBaseTs = b.MaxTimestamp()
	}
	s.size.Store(pos + int64(len(b)))
	s.nextOffset.Store(b.NextOffset())
	return nil
}

// indexBatch adds sparse index entries for a batch at position pos and updates the in-memory max
// timestamp. Entries are added before the batch's own bytes are counted, so the first batch of a
// segment (position 0) is never indexed (DECISIONS D3). It is shared by append and recover. When
// recovering, a full index simply stops receiving entries (lookups stay correct, only slower), so a
// lowered index.interval.bytes or max.index.bytes never makes a log unopenable; the next append
// then rolls because needsRoll checks isFull.
func (s *segment) indexBatch(b record.Batch, pos int64, recovering bool) error {
	if ts := b.MaxTimestamp(); ts > s.maxTimestamp.Load() {
		s.maxTimestamp.Store(ts)
		s.offsetOfMaxTs = b.BaseOffset()
	}
	if recovering && (s.index.isFull() || s.timeIndex.isFull()) {
		s.bytesSinceIndex += len(b)
		return nil
	}
	if s.bytesSinceIndex >= s.cfg.IndexIntervalBytes {
		if err := s.index.append(b.BaseOffset(), uint32(pos)); err != nil {
			return fmt.Errorf("append offset index: %w", err)
		}
		if err := s.timeIndex.maybeAppend(s.maxTimestamp.Load(), s.offsetOfMaxTs, false); err != nil {
			return fmt.Errorf("append time index: %w", err)
		}
		s.bytesSinceIndex = 0
	}
	s.bytesSinceIndex += len(b)
	return nil
}

// batchInfo is the framing of one batch read from a segment file.
type batchInfo struct {
	pos          int64
	size         int64
	baseOffset   int64
	lastOffset   int64
	maxTimestamp int64
}

// isFramingError reports whether err from readBatchInfo means the bytes are not a valid batch
// (as opposed to an I/O failure, which must never cause truncation).
func isFramingError(err error) bool {
	return errors.Is(err, record.ErrTruncated) || errors.Is(err, record.ErrCorrupt) || errors.Is(err, io.EOF)
}

// readBatchInfo reads the header of the batch at pos; end bounds the readable region.
func (s *segment) readBatchInfo(pos, end int64) (batchInfo, error) {
	if end-pos < record.HeaderSize {
		return batchInfo{}, fmt.Errorf("segment %d position %d: %w", s.baseOffset, pos, record.ErrTruncated)
	}
	var buf [record.HeaderSize]byte
	if _, err := s.log.ReadAt(buf[:], pos); err != nil {
		return batchInfo{}, fmt.Errorf("read batch header: %w", err)
	}
	h, err := record.ParseHeader(buf[:])
	if err != nil {
		return batchInfo{}, fmt.Errorf("segment %d position %d: %w", s.baseOffset, pos, err)
	}
	size := int64(record.LogOverhead) + int64(h.BatchLength)
	if size > end-pos {
		return batchInfo{}, fmt.Errorf("segment %d position %d: %w", s.baseOffset, pos, record.ErrTruncated)
	}
	return batchInfo{
		pos:          pos,
		size:         size,
		baseOffset:   h.BaseOffset,
		lastOffset:   h.BaseOffset + int64(h.LastOffsetDelta),
		maxTimestamp: h.MaxTimestamp,
	}, nil
}

// read returns whole batches starting with the one containing startOffset. It stops before any
// batch with lastOffset >= maxOffset and before exceeding maxBytes, except that the first batch is
// always returned whole. An empty result means nothing at or after startOffset is readable here.
func (s *segment) read(startOffset int64, maxBytes int, maxOffset int64) ([]byte, error) {
	end := s.size.Load()
	_, pos := s.index.lookup(startOffset)
	start := int64(-1)
	var total int64
	for p := int64(pos); p < end; {
		bi, err := s.readBatchInfo(p, end)
		if err != nil {
			return nil, err
		}
		if start < 0 {
			if bi.lastOffset < startOffset {
				p += bi.size
				continue
			}
			start = p
		}
		if bi.lastOffset >= maxOffset || (total > 0 && total+bi.size > int64(maxBytes)) {
			break
		}
		total += bi.size
		p += bi.size
	}
	if total == 0 {
		return nil, nil
	}
	buf := make([]byte, total)
	if _, err := s.log.ReadAt(buf, start); err != nil {
		return nil, fmt.Errorf("read segment %d: %w", s.baseOffset, err)
	}
	return buf, nil
}

// findOffsetByTimestamp returns the earliest offset whose record timestamp is >= ts, considering
// only batches with lastOffset < maxOffset, if this segment holds one.
func (s *segment) findOffsetByTimestamp(ts, maxOffset int64) (int64, bool, error) {
	end := s.size.Load()
	// The largest entry with timestamp < ts: every record before its offset has a timestamp < ts,
	// so the earliest match is at or after that offset (DECISIONS D3).
	var pos uint32
	if ts > math.MinInt64 {
		_, off := s.timeIndex.lookup(ts - 1)
		_, pos = s.index.lookup(off)
	}
	for p := int64(pos); p < end; {
		bi, err := s.readBatchInfo(p, end)
		if err != nil {
			return 0, false, err
		}
		if bi.lastOffset >= maxOffset {
			return 0, false, nil
		}
		if bi.maxTimestamp >= ts {
			buf := make([]byte, bi.size)
			if _, err := s.log.ReadAt(buf, p); err != nil {
				return 0, false, fmt.Errorf("read segment %d: %w", s.baseOffset, err)
			}
			recs, err := record.Batch(buf).Records()
			if err != nil {
				return 0, false, fmt.Errorf("decode batch at offset %d: %w", bi.baseOffset, err)
			}
			for _, r := range recs {
				if r.Timestamp >= ts {
					return r.Offset, true, nil
				}
			}
		}
		p += bi.size
	}
	return 0, false, nil
}

// recover rebuilds both indexes by validating every batch from the start of the file and cuts
// the file at the first invalid, partial or non-contiguous batch (SPEC §3.4). It reports whether
// the file was truncated. With allowTruncate false (a segment below the recovery point, whose data
// was fsynced and acknowledged) it never modifies the file and returns ErrCorruptLog instead.
func (s *segment) recover(allowTruncate bool) (truncated bool, err error) {
	s.index.reset()
	s.timeIndex.reset()
	s.bytesSinceIndex = 0
	s.maxTimestamp.Store(-1)
	s.offsetOfMaxTs = s.baseOffset
	st, err := s.log.Stat()
	if err != nil {
		return false, fmt.Errorf("stat segment: %w", err)
	}
	fileSize := st.Size()
	expected := s.baseOffset
	var pos int64
	var buf []byte
	for pos < fileSize {
		bi, err := s.readBatchInfo(pos, fileSize)
		if isFramingError(err) {
			break
		}
		if err != nil {
			return false, err
		}
		if cap(buf) < int(bi.size) {
			buf = make([]byte, bi.size)
		}
		buf = buf[:bi.size]
		if _, err := s.log.ReadAt(buf, pos); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return false, fmt.Errorf("read segment %d: %w", s.baseOffset, err)
		}
		b, err := record.Validate(buf, math.MaxInt)
		if err != nil || b.BaseOffset() != expected {
			break
		}
		if pos == 0 {
			s.rollingBaseTs = b.MaxTimestamp()
		}
		if err := s.indexBatch(b, pos, true); err != nil {
			return false, err
		}
		expected = b.NextOffset()
		pos += bi.size
	}
	if pos < fileSize {
		if !allowTruncate {
			return false, fmt.Errorf("%w: segment %d invalid at position %d of %d", ErrCorruptLog, s.baseOffset, pos, fileSize)
		}
		if err := s.log.Truncate(pos); err != nil {
			return false, fmt.Errorf("truncate segment %d at %d: %w", s.baseOffset, pos, err)
		}
		if err := s.log.Sync(); err != nil {
			return false, fmt.Errorf("sync segment %d: %w", s.baseOffset, err)
		}
		truncated = true
	}
	s.size.Store(pos)
	s.nextOffset.Store(expected)
	return truncated, nil
}

// loadState restores in-memory state for a segment wholly below the recovery point without
// validating every batch: it sanity-checks the indexes and scans only the tail after the last
// offset-index entry. Any inconsistency falls back to a non-truncating recover (indexes are
// rebuilt; damaged log data yields ErrCorruptLog).
func (s *segment) loadState() (truncated bool, err error) {
	st, err := s.log.Stat()
	if err != nil {
		return false, fmt.Errorf("stat segment: %w", err)
	}
	fileSize := st.Size()
	if err := s.index.sanityCheck(fileSize); err != nil {
		return s.recover(false)
	}
	if s.index.isFull() || s.timeIndex.isFull() {
		// Possibly capped by a lowered max.index.bytes: the kept entries would understate the max
		// timestamp and leave no slot for the sealing entry, so rebuild from the data.
		return s.recover(false)
	}
	_, pos := s.index.lookup(math.MaxInt64)
	next := s.baseOffset
	maxTs, offOfMax := s.timeIndex.lastEntry()
	p := int64(pos)
	for p < fileSize {
		bi, err := s.readBatchInfo(p, fileSize)
		if isFramingError(err) {
			return s.recover(false)
		}
		if err != nil {
			return false, err
		}
		if bi.maxTimestamp > maxTs {
			maxTs, offOfMax = bi.maxTimestamp, bi.baseOffset
		}
		next = bi.lastOffset + 1
		p += bi.size
	}
	if err := s.timeIndex.sanityCheck(next); err != nil {
		return s.recover(false)
	}
	if fileSize > 0 {
		first, err := s.readBatchInfo(0, fileSize)
		if isFramingError(err) {
			return s.recover(false)
		}
		if err != nil {
			return false, err
		}
		s.rollingBaseTs = first.maxTimestamp
	}
	s.size.Store(fileSize)
	s.nextOffset.Store(next)
	s.maxTimestamp.Store(maxTs)
	s.offsetOfMaxTs = offOfMax
	return false, nil
}

// onRoll seals the segment when it stops being active: it records the final max timestamp in the
// time index, trims both indexes and fsyncs all three files.
func (s *segment) onRoll() error {
	if ts := s.maxTimestamp.Load(); ts >= 0 {
		if err := s.timeIndex.maybeAppend(ts, s.offsetOfMaxTs, true); err != nil {
			return fmt.Errorf("final time index entry: %w", err)
		}
	}
	if err := s.index.trim(); err != nil {
		return fmt.Errorf("trim offset index: %w", err)
	}
	if err := s.timeIndex.trim(); err != nil {
		return fmt.Errorf("trim time index: %w", err)
	}
	return s.flush()
}

// flush fsyncs the log and both index files.
func (s *segment) flush() error {
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("sync segment %d log: %w", s.baseOffset, err)
	}
	if err := s.index.flush(); err != nil {
		return fmt.Errorf("sync segment %d index: %w", s.baseOffset, err)
	}
	if err := s.timeIndex.flush(); err != nil {
		return fmt.Errorf("sync segment %d time index: %w", s.baseOffset, err)
	}
	return nil
}

// renameForDelete appends ".deleted" to all three file names (SPEC §3.3). Open handles and
// mappings stay valid, so readers already holding the segment can finish.
func (s *segment) renameForDelete() error {
	newLog := s.logPath + deletedSuffix
	if err := os.Rename(s.logPath, newLog); err != nil {
		return fmt.Errorf("rename segment log: %w", err)
	}
	s.logPath = newLog
	if err := s.index.rename(s.index.path + deletedSuffix); err != nil {
		return fmt.Errorf("rename offset index: %w", err)
	}
	if err := s.timeIndex.rename(s.timeIndex.path + deletedSuffix); err != nil {
		return fmt.Errorf("rename time index: %w", err)
	}
	return nil
}

// deleteFiles closes the segment and unlinks its files.
func (s *segment) deleteFiles() error {
	paths := []string{s.logPath, s.index.path, s.timeIndex.path}
	errs := []error{s.close()}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("remove %s: %w", p, err))
		}
	}
	return errors.Join(errs...)
}

// close trims the indexes and closes all files. It is safe on a partially opened segment and
// idempotent.
func (s *segment) close() error {
	var errs []error
	if s.log != nil {
		errs = append(errs, s.log.Close())
	}
	if s.index != nil {
		errs = append(errs, s.index.trim(), s.index.close())
	}
	if s.timeIndex != nil {
		errs = append(errs, s.timeIndex.trim(), s.timeIndex.close())
	}
	for i, err := range errs {
		if errors.Is(err, ErrClosed) || errors.Is(err, os.ErrClosed) {
			errs[i] = nil
		}
	}
	return errors.Join(errs...)
}
