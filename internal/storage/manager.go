package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RecoveryPointCheckpointFile is the per-data-dir file holding each partition's recovery point.
const RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"

// ManagerConfig configures a Manager.
type ManagerConfig struct {
	DataDir                string
	FileDeleteDelay        time.Duration // file.delete.delay.ms
	FlushCheckInterval     time.Duration // how often interval-policy logs are checked for flushing
	RetentionCheckInterval time.Duration
	CheckpointInterval     time.Duration // recovery-point checkpoint period
}

// DefaultManagerConfig returns the defaults for dataDir.
func DefaultManagerConfig(dataDir string) ManagerConfig {
	return ManagerConfig{
		DataDir:                dataDir,
		FileDeleteDelay:        60 * time.Second,
		FlushCheckInterval:     100 * time.Millisecond,
		RetentionCheckInterval: 5 * time.Minute,
		CheckpointInterval:     5 * time.Second,
	}
}

type pendingDelete struct {
	seg   *segment
	dueAt time.Time
}

// Manager owns every partition log in one data directory: it opens logs with their checkpointed
// recovery points, runs the background flush/retention/checkpoint/delete passes, and performs
// delayed deletion of retired segments (SPEC §3.3).
//
// Lock ordering: Manager.mu → Log.mu → Manager.pendingMu. Manager.mu is never held across log
// I/O: opens/recovery run outside it and background passes work on a snapshot of the logs.
type Manager struct {
	mu             sync.Mutex
	logs           map[TopicPartition]*Log
	opening        map[TopicPartition]chan struct{} // closed when the open finishes
	recoveryPoints map[TopicPartition]int64         // from the checkpoint read at open
	closed         bool

	pendingMu sync.Mutex
	pending   []pendingDelete

	cfg    ManagerConfig
	clock  Clock
	logger *slog.Logger
}

// OpenManager creates the data dir if needed, reads the recovery-point checkpoint and removes
// leftover ".deleted" files from every partition directory.
func OpenManager(ctx context.Context, cfg ManagerConfig, clock Clock, logger *slog.Logger) (*Manager, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("storage: empty data dir")
	}
	if cfg.FileDeleteDelay < 0 || cfg.FlushCheckInterval <= 0 || cfg.RetentionCheckInterval <= 0 || cfg.CheckpointInterval <= 0 {
		return nil, fmt.Errorf("storage: invalid manager config: intervals must be > 0 and delete delay >= 0: %+v", cfg)
	}
	if err := os.MkdirAll(cfg.DataDir, dirMode); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	rps, err := ReadOffsetCheckpoint(ctx, filepath.Join(cfg.DataDir, RecoveryPointCheckpointFile))
	if err != nil {
		return nil, fmt.Errorf("read recovery points: %w", err)
	}
	if err := removeDeletedFiles(cfg.DataDir); err != nil {
		return nil, err
	}
	return &Manager{
		logs:           map[TopicPartition]*Log{},
		opening:        map[TopicPartition]chan struct{}{},
		recoveryPoints: rps,
		cfg:            cfg,
		clock:          clock,
		logger:         logger,
	}, nil
}

// removeDeletedFiles unlinks "*.deleted" files left in partition directories by a previous run.
func removeDeletedFiles(dataDir string) error {
	dirs, err := os.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}
	var errs []error
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		if _, ok := ParseDirName(d.Name()); !ok {
			continue
		}
		files, err := os.ReadDir(filepath.Join(dataDir, d.Name()))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), deletedSuffix) {
				if err := os.Remove(filepath.Join(dataDir, d.Name(), f.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					errs = append(errs, err)
				}
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("remove leftover deleted segments: %w", err)
	}
	return nil
}

// GetOrCreateLog returns the open log for tp, opening (and recovering) or creating it with cfg
// if needed. cfg is ignored when the log is already open. Recovery runs outside Manager.mu, so
// other partitions are not stalled; concurrent callers for the same tp wait for one open.
func (m *Manager) GetOrCreateLog(ctx context.Context, tp TopicPartition, cfg LogConfig) (*Log, error) {
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, ErrClosed
		}
		if l, ok := m.logs[tp]; ok {
			m.mu.Unlock()
			return l, nil
		}
		wait, busy := m.opening[tp]
		if !busy {
			break // holding m.mu
		}
		m.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	done := make(chan struct{})
	m.opening[tp] = done
	rp := m.recoveryPoints[tp]
	m.mu.Unlock()

	dir := filepath.Join(m.cfg.DataDir, tp.DirName())
	l, err := openLog(ctx, dir, tp, cfg, rp, m.clock, m.logger, m.scheduleDelete)

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.opening, tp)
	close(done)
	if err != nil {
		return nil, fmt.Errorf("open log %s: %w", tp, err)
	}
	if m.closed {
		return nil, errors.Join(ErrClosed, l.Close())
	}
	m.logs[tp] = l
	return l, nil
}

// Log returns the open log for tp.
func (m *Manager) Log(tp TopicPartition) (*Log, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.logs[tp]
	return l, ok
}

// Logs returns a snapshot of all open logs.
func (m *Manager) Logs() []*Log {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Log, 0, len(m.logs))
	for _, l := range m.logs {
		out = append(out, l)
	}
	return out
}

// scheduleDelete is the Log onDelete callback. It runs under Log.mu and takes only pendingMu.
func (m *Manager) scheduleDelete(s *segment) {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	m.pending = append(m.pending, pendingDelete{seg: s, dueAt: m.clock.Now().Add(m.cfg.FileDeleteDelay)})
}

// FlushDirty runs one interval-flush pass over all logs.
func (m *Manager) FlushDirty(ctx context.Context, now time.Time) error {
	var errs []error
	for _, l := range m.Logs() {
		if err := l.MaybeFlush(ctx, now); err != nil && !errors.Is(err, ErrClosed) {
			errs = append(errs, fmt.Errorf("flush %s: %w", l.tp, err))
		}
	}
	return errors.Join(errs...)
}

// RunRetention runs one retention pass. limit returns the offset below which segments may be
// deleted for a partition (its high watermark); nil means the LEO.
func (m *Manager) RunRetention(ctx context.Context, now time.Time, limit func(TopicPartition) int64) error {
	var errs []error
	for _, l := range m.Logs() {
		lim := l.LogEndOffset()
		if limit != nil {
			lim = limit(l.tp)
		}
		if _, err := l.DeleteOldSegments(ctx, now, lim); err != nil && !errors.Is(err, ErrClosed) {
			errs = append(errs, fmt.Errorf("retention %s: %w", l.tp, err))
		}
	}
	return errors.Join(errs...)
}

// CheckpointRecoveryPoints writes the recovery-point checkpoint. Partitions present in the
// checkpoint read at open but not opened since keep their old value.
func (m *Manager) CheckpointRecoveryPoints(ctx context.Context) error {
	m.mu.Lock()
	rps := make(map[TopicPartition]int64, len(m.recoveryPoints)+len(m.logs))
	for tp, rp := range m.recoveryPoints {
		rps[tp] = rp
	}
	for tp, l := range m.logs {
		rps[tp] = l.RecoveryPoint()
	}
	m.mu.Unlock()
	if err := WriteOffsetCheckpoint(ctx, filepath.Join(m.cfg.DataDir, RecoveryPointCheckpointFile), rps); err != nil {
		return fmt.Errorf("write recovery points: %w", err)
	}
	return nil
}

// DeleteExpired closes and unlinks retired segments whose delete delay has passed.
func (m *Manager) DeleteExpired(_ context.Context, now time.Time) error {
	m.pendingMu.Lock()
	var due []*segment
	keep := m.pending[:0]
	for _, p := range m.pending {
		if !now.Before(p.dueAt) {
			due = append(due, p.seg)
		} else {
			keep = append(keep, p)
		}
	}
	m.pending = keep
	m.pendingMu.Unlock()
	var errs []error
	for _, s := range due {
		if err := s.deleteFiles(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PendingDeletes returns the number of retired segments not yet unlinked.
func (m *Manager) PendingDeletes() int {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return len(m.pending)
}

// Run runs the background passes until ctx is cancelled. Pass errors are logged, not returned.
func (m *Manager) Run(ctx context.Context) error {
	flush := time.NewTicker(m.cfg.FlushCheckInterval)
	defer flush.Stop()
	retention := time.NewTicker(m.cfg.RetentionCheckInterval)
	defer retention.Stop()
	checkpoint := time.NewTicker(m.cfg.CheckpointInterval)
	defer checkpoint.Stop()
	for {
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-flush.C:
			err = errors.Join(m.FlushDirty(ctx, m.clock.Now()), m.DeleteExpired(ctx, m.clock.Now()))
		case <-retention.C:
			err = m.RunRetention(ctx, m.clock.Now(), nil)
		case <-checkpoint.C:
			err = m.CheckpointRecoveryPoints(ctx)
		}
		if err != nil && ctx.Err() == nil {
			m.logger.Error("storage background pass failed", slog.Any("error", err))
		}
	}
}

// Close closes every log (flushing it), writes the recovery-point checkpoint and unlinks all
// retired segments. Run must have returned first: both write the checkpoint file.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	m.closed = true
	logs := make([]*Log, 0, len(m.logs))
	for _, l := range m.logs {
		logs = append(logs, l)
	}
	m.mu.Unlock()
	var errs []error
	for _, l := range logs {
		if err := l.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", l.tp, err))
		}
	}
	errs = append(errs, m.CheckpointRecoveryPoints(ctx))
	m.pendingMu.Lock()
	pending := m.pending
	m.pending = nil
	m.pendingMu.Unlock()
	for _, p := range pending {
		errs = append(errs, p.seg.deleteFiles())
	}
	return errors.Join(errs...)
}
