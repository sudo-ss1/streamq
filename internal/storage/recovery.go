package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// openLog opens (creating if needed) the log in dir and recovers it (SPEC §3.4).
//
// Leftover ".deleted" files and index files without a ".log" are removed. Segments wholly below
// recoveryPoint are trusted (indexes sanity-checked, tail scanned); the last segment and every
// segment that may hold data at or above recoveryPoint are fully validated and their indexes
// rebuilt. The log is cut at the first invalid, partial or non-contiguous batch and any later
// segments are deleted. onDelete receives segments removed by retention.
func openLog(ctx context.Context, dir string, tp TopicPartition, cfg LogConfig, recoveryPoint int64,
	clock Clock, logger *slog.Logger, onDelete func(*segment),
) (*Log, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	_, statErr := os.Stat(dir)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}
	if errors.Is(statErr, os.ErrNotExist) {
		// Make the new partition directory's entry durable (Kafka flushes the parent dir).
		if err := syncDir(filepath.Dir(dir)); err != nil {
			return nil, err
		}
	}
	l := &Log{
		tp:       tp,
		dir:      dir,
		cfg:      cfg,
		clock:    clock,
		logger:   logger.With(slog.String("topic", tp.Topic), slog.Int("partition", int(tp.Partition))),
		onDelete: onDelete,
	}
	bases, err := cleanDir(dir)
	if err != nil {
		return nil, err
	}
	now := clock.Now()
	var segs []*segment
	closeAll := func() {
		for _, s := range segs {
			_ = s.close()
		}
	}
	if len(bases) == 0 {
		s, err := createSegment(dir, 0, cfg, now)
		if err != nil {
			return nil, err
		}
		segs = append(segs, s)
		if err := syncDir(dir); err != nil {
			closeAll()
			return nil, err
		}
	}
	for i, base := range bases {
		s, err := openSegment(dir, base, cfg, now)
		if err != nil {
			closeAll()
			return nil, err
		}
		segs = append(segs, s)
		last := i == len(bases)-1
		trusted := !last && bases[i+1] <= recoveryPoint // wholly below the recovery point
		var truncated bool
		if trusted {
			truncated, err = s.loadState()
		} else {
			truncated, err = s.recover(true)
		}
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("recover segment %d: %w", base, err)
		}
		if truncated {
			l.logger.Warn("truncated corrupt or partial log tail", slog.Int64("offset", s.nextOffset.Load()))
		}
		gap := !last && s.nextOffset.Load() != bases[i+1]
		if trusted && gap {
			// Fsynced data is inconsistent. Deleting every newer segment to "repair" it would
			// destroy acknowledged data, so refuse to open and leave it to the operator.
			closeAll()
			return nil, fmt.Errorf("%w: segment %d ends at offset %d but the next segment starts at %d",
				ErrCorruptLog, base, s.nextOffset.Load(), bases[i+1])
		}
		if !last && (truncated || gap) {
			if gap {
				l.logger.Warn("offset gap between segments; deleting later segments",
					slog.Int64("expected", s.nextOffset.Load()), slog.Int64("next_base", bases[i+1]))
			}
			if err := deleteSegmentFiles(dir, bases[i+1:]); err != nil {
				closeAll()
				return nil, err
			}
			break
		}
	}
	// Recovered segments other than the active one are sealed again (trimmed indexes, fsynced).
	for _, s := range segs[:len(segs)-1] {
		if err := s.onRoll(); err != nil {
			closeAll()
			return nil, fmt.Errorf("seal segment %d: %w", s.baseOffset, err)
		}
	}
	if err := segs[len(segs)-1].flush(); err != nil {
		closeAll()
		return nil, err
	}
	l.segments.Store(&segs)
	l.logStartOffset.Store(segs[0].baseOffset)
	leo := segs[len(segs)-1].nextOffset.Load()
	l.leo.Store(leo)
	l.recoveryPoint.Store(leo)
	l.lastFlushMs.Store(now.UnixMilli())
	return l, nil
}

// cleanDir removes leftover ".deleted" files, temp files and orphan index files, and returns the
// sorted base offsets of the ".log" files.
func cleanDir(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read log dir: %w", err)
	}
	logs := map[int64]bool{}
	var indexes []string
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		switch {
		case strings.HasSuffix(name, deletedSuffix):
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
		case strings.HasSuffix(name, logSuffix):
			if base, ok := parseBase(name, logSuffix); ok {
				logs[base] = true
			}
		case strings.HasSuffix(name, indexSuffix), strings.HasSuffix(name, timeIndexSuffix):
			indexes = append(indexes, name)
		}
	}
	for _, name := range indexes {
		suffix := indexSuffix
		if strings.HasSuffix(name, timeIndexSuffix) {
			suffix = timeIndexSuffix
		}
		if base, ok := parseBase(name, suffix); ok && logs[base] {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("clean log dir: %w", err)
	}
	bases := make([]int64, 0, len(logs))
	for b := range logs {
		bases = append(bases, b)
	}
	slices.Sort(bases)
	return bases, nil
}

// parseBase parses "<20-digit base><suffix>".
func parseBase(name, suffix string) (int64, bool) {
	digits := strings.TrimSuffix(name, suffix)
	if len(digits) != 20 {
		return 0, false
	}
	base, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || base < 0 {
		return 0, false
	}
	return base, true
}

// deleteSegmentFiles unlinks the files of segments that were never opened.
func deleteSegmentFiles(dir string, bases []int64) error {
	var errs []error
	for _, b := range bases {
		for _, suffix := range []string{logSuffix, indexSuffix, timeIndexSuffix} {
			if err := os.Remove(filepath.Join(dir, segmentFileName(b, suffix))); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("delete segments after truncation: %w", err)
	}
	return syncDir(dir)
}
