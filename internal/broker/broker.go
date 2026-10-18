// Package broker is the replica manager of one broker (DECISIONS D14). It owns the storage
// manager and the local partitions, follows the metadata store (opening and deleting partitions as
// topics change), writes the high-watermark checkpoint, and implements produce, the long-poll
// fetch and list-offsets for the gRPC layer.
//
// Lock ordering: Broker.applyMu → Broker.ckptMu → Broker.mu → (partition and storage locks).
// applyMu serializes Open's processing of the initial metadata snapshot with every metadata change
// handler, so changes are applied strictly in order and never interleave with startup. mu guards
// the partitions and topics maps and the change-notification channel and is never held across I/O.
package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/metadata"
	"github.com/sudo-ss1/streamq/internal/partition"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// HWCheckpointFile is the per-data-dir high watermark checkpoint (SPEC §3.1).
const HWCheckpointFile = "replication-offset-checkpoint"

// maxTopicWait bounds WaitTopicReady/WaitTopicDeleted when the caller's ctx has no deadline.
const maxTopicWait = 30 * time.Second

// Sentinel errors.
var (
	// ErrUnknownPartition reports a request for a partition this broker does not host.
	ErrUnknownPartition = errors.New("broker: unknown topic or partition")
	// ErrShuttingDown reports a request that arrived while the broker is closing.
	ErrShuttingDown = errors.New("broker: shutting down")
)

// Clock is the broker's time source; tests inject a fake to control long-poll timeouts.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// After returns time.After(d).
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Config configures a Broker.
type Config struct {
	BrokerID             int32
	DataDir              string
	TopicDefaults        config.TopicDefaults
	FileDeleteDelay      time.Duration
	HWCheckpointInterval time.Duration // default 5s
}

// topicState is the local view of one topic.
type topicState struct {
	id      string
	want    int   // local partitions expected
	open    int   // local partitions open
	openErr error // first error opening a partition, if any
}

// Broker is the replica manager.
type Broker struct {
	cfg    Config
	store  metadata.Store
	mgr    *storage.Manager
	clock  Clock
	logger *slog.Logger

	applyMu sync.Mutex

	mu         sync.RWMutex
	partitions map[storage.TopicPartition]*partition.Partition
	topics     map[string]*topicState
	changed    chan struct{} // closed and replaced whenever partitions/topics change
	hwCkpt     map[storage.TopicPartition]int64

	unsubscribe func()
	done        chan struct{}
	doneOnce    sync.Once
	closeOnce   sync.Once

	// ckptMu serializes writeHWCheckpoint from the map copy through the rename, so a periodic
	// write can never resurrect an entry deletePartitionStorage just removed.
	ckptMu sync.Mutex
	cancel context.CancelFunc
	group  *errgroup.Group

	afterFetchRead func() // test hook: runs between a fetch's read and its wait
}

// Open opens the storage under cfg.DataDir, subscribes to store, removes stray partition
// directories, opens every local partition of the current metadata, and starts the background
// loops. The broker is ready to serve when Open returns.
func Open(ctx context.Context, cfg Config, store metadata.Store, clock Clock, logger *slog.Logger) (*Broker, error) {
	if cfg.HWCheckpointInterval <= 0 {
		cfg.HWCheckpointInterval = 5 * time.Second
	}
	logger = logger.With(slog.Int("broker_id", int(cfg.BrokerID)))
	mcfg := storage.DefaultManagerConfig(cfg.DataDir)
	mcfg.FileDeleteDelay = cfg.FileDeleteDelay
	mgr, err := storage.OpenManager(ctx, mcfg, storage.SystemClock{}, logger)
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	hw, err := storage.ReadOffsetCheckpoint(ctx, filepath.Join(cfg.DataDir, HWCheckpointFile))
	if err != nil {
		return nil, errors.Join(fmt.Errorf("read hw checkpoint: %w", err), mgr.Close(ctx))
	}
	b := &Broker{
		cfg:        cfg,
		store:      store,
		mgr:        mgr,
		clock:      clock,
		logger:     logger,
		partitions: map[storage.TopicPartition]*partition.Partition{},
		topics:     map[string]*topicState{},
		changed:    make(chan struct{}),
		hwCkpt:     hw,
		done:       make(chan struct{}),
	}

	b.applyMu.Lock()
	snap, unsubscribe := store.Subscribe(b.onChange)
	b.unsubscribe = unsubscribe
	err = b.applySnapshot(ctx, snap)
	b.applyMu.Unlock()
	if err != nil {
		unsubscribe()
		return nil, errors.Join(err, mgr.Close(ctx))
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	b.cancel = cancel
	b.group, runCtx = errgroup.WithContext(runCtx)
	b.group.Go(func() error { return mgr.Run(runCtx) })
	b.group.Go(func() error { return b.runHWCheckpoints(runCtx) })
	return b, nil
}

// applySnapshot removes stray partition dirs and opens every local partition. Caller holds applyMu.
func (b *Broker) applySnapshot(ctx context.Context, snap metadata.Snapshot) error {
	dirs, err := b.mgr.PartitionDirs()
	if err != nil {
		return err
	}
	for _, tp := range dirs {
		dir := filepath.Join(b.cfg.DataDir, tp.DirName())
		id, idErr := readPartitionMetadata(dir)
		t, ok := snap.Topic(tp.Topic)
		if ok && idErr == nil && id == t.ID && b.isLocalReplica(t, tp.Partition) {
			continue
		}
		tag := "stray"
		if idErr == nil {
			tag = id
		}
		b.logger.Warn("removing stray partition directory", slog.String("topic", tp.Topic),
			slog.Int("partition", int(tp.Partition)), slog.String("dir_topic_id", id))
		if err := b.deletePartitionStorage(ctx, tp, tag); err != nil {
			// Never block startup on cleanup; the directory is simply not opened.
			b.logger.Error("failed to remove stray partition directory", slog.String("topic", tp.Topic),
				slog.Int("partition", int(tp.Partition)), slog.Any("error", err))
		}
	}
	names := make([]string, 0, len(snap.Topics))
	for name := range snap.Topics {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		t := snap.Topics[name]
		b.createTopic(ctx, t.Name, t.ID, t.Configs, t.Partitions)
	}
	return nil
}

func (b *Broker) isLocalReplica(t metadata.Topic, p int32) bool {
	for _, ps := range t.Partitions {
		if ps.Partition == p {
			return slices.Contains(ps.Replicas, b.cfg.BrokerID)
		}
	}
	return false
}

// onChange is the metadata subscription callback.
func (b *Broker) onChange(ctx context.Context, ch metadata.Change) {
	b.applyMu.Lock()
	defer b.applyMu.Unlock()
	switch ch.Kind {
	case metadata.TopicCreated:
		b.createTopic(ctx, ch.Topic, ch.TopicID, ch.Configs, ch.Partitions)
	case metadata.TopicDeleted:
		b.deleteTopic(ctx, ch.Topic, ch.TopicID)
	case metadata.PartitionChanged:
		// Leadership changes arrive with replication (M6).
	}
}

// createTopic opens the local partitions of a topic. Errors are recorded per topic (and returned
// by WaitTopicReady) rather than aborting, so one bad partition does not block others. It is
// idempotent. Caller holds applyMu.
func (b *Broker) createTopic(ctx context.Context, name, id string, configs map[string]string, parts []metadata.PartitionState) {
	b.mu.Lock()
	if ts, ok := b.topics[name]; ok && ts.id == id {
		b.mu.Unlock()
		return
	}
	ts := &topicState{id: id}
	for _, ps := range parts {
		if slices.Contains(ps.Replicas, b.cfg.BrokerID) {
			ts.want++
		}
	}
	b.topics[name] = ts
	b.mu.Unlock()

	tcfg, err := config.ResolveTopicConfig(b.cfg.TopicDefaults, configs)
	for _, ps := range parts {
		if err != nil {
			break
		}
		if !slices.Contains(ps.Replicas, b.cfg.BrokerID) {
			continue
		}
		var p *partition.Partition
		p, err = b.openPartition(ctx, name, id, tcfg, ps)
		if err != nil {
			break
		}
		b.mu.Lock()
		b.partitions[p.TopicPartition()] = p
		ts.open++
		b.mu.Unlock()
	}
	if err != nil {
		b.logger.Error("failed to open topic partitions", slog.String("topic", name), slog.Any("error", err))
	}
	b.mu.Lock()
	ts.openErr = err
	b.notifyLocked()
	b.mu.Unlock()
}

// openPartition writes partition.metadata (before the log creates any data), opens the log and
// applies the partition's leadership.
func (b *Broker) openPartition(ctx context.Context, topic, id string, tcfg config.TopicConfig, ps metadata.PartitionState) (*partition.Partition, error) {
	tp := storage.TopicPartition{Topic: topic, Partition: ps.Partition}
	dir := filepath.Join(b.cfg.DataDir, tp.DirName())
	if err := ensurePartitionMetadata(dir, id); err != nil {
		return nil, fmt.Errorf("partition %s: %w", tp, err)
	}
	l, err := b.mgr.GetOrCreateLog(ctx, tp, tcfg.Log)
	if err != nil {
		return nil, err
	}
	p := partition.New(tp, id, l, b.cfg.BrokerID, b.logger)
	b.mu.RLock()
	hw := b.hwCkpt[tp]
	b.mu.RUnlock()
	if ps.Leader == b.cfg.BrokerID {
		p.BecomeLeader(ps.LeaderEpoch, ps.ISR, tcfg.MinInsyncReplicas, hw)
	}
	return p, nil
}

// deleteTopic removes the local partitions of a topic with the given id. Idempotent. Caller holds
// applyMu.
func (b *Broker) deleteTopic(ctx context.Context, name, id string) {
	b.mu.Lock()
	var tps []storage.TopicPartition
	for tp, p := range b.partitions {
		if tp.Topic == name && p.TopicID() == id {
			tps = append(tps, tp)
			delete(b.partitions, tp)
		}
	}
	b.mu.Unlock()
	for _, tp := range tps {
		if err := b.deletePartitionStorage(ctx, tp, id); err != nil {
			b.logger.Error("failed to delete partition", slog.String("topic", tp.Topic),
				slog.Int("partition", int(tp.Partition)), slog.Any("error", err))
		}
	}
	b.mu.Lock()
	if ts, ok := b.topics[name]; ok && ts.id == id {
		delete(b.topics, name) // only now, so WaitTopicDeleted means the storage is gone
	}
	b.notifyLocked()
	b.mu.Unlock()
}

// deletePartitionStorage drops tp's HW checkpoint entry (rewriting the file) and then deletes its
// log and directory, so a recreated partition never inherits stale offsets (DECISIONS D17).
func (b *Broker) deletePartitionStorage(ctx context.Context, tp storage.TopicPartition, tag string) error {
	b.ckptMu.Lock()
	b.mu.Lock()
	delete(b.hwCkpt, tp)
	b.mu.Unlock()
	b.ckptMu.Unlock()
	if err := b.writeHWCheckpoint(ctx); err != nil {
		return err
	}
	return b.mgr.DeleteLog(ctx, tp, tag)
}

// notifyLocked wakes WaitTopicReady/WaitTopicDeleted callers. Caller holds mu.
func (b *Broker) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// WaitTopicReady blocks until every local partition of topic (with id) is open, the open failed,
// ctx ends, or the broker closes (DECISIONS D18).
func (b *Broker) WaitTopicReady(ctx context.Context, topic, id string) error {
	return b.waitTopic(ctx, func() (bool, error) {
		ts, ok := b.topics[topic]
		if !ok || ts.id != id {
			return false, nil
		}
		if ts.openErr != nil {
			return true, ts.openErr
		}
		return ts.open == ts.want, nil
	})
}

// WaitTopicDeleted blocks until no local state for topic with id remains.
func (b *Broker) WaitTopicDeleted(ctx context.Context, topic, id string) error {
	return b.waitTopic(ctx, func() (bool, error) {
		ts, ok := b.topics[topic]
		return !ok || ts.id != id, nil
	})
}

func (b *Broker) waitTopic(ctx context.Context, done func() (bool, error)) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxTopicWait)
		defer cancel()
	}
	for {
		b.mu.RLock()
		ch := b.changed // captured before checking, so no change is missed
		ok, err := done()
		b.mu.RUnlock()
		if ok || err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-b.done:
			return ErrShuttingDown
		}
	}
}

// Partition returns the local replica of tp.
func (b *Broker) Partition(tp storage.TopicPartition) (*partition.Partition, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	p, ok := b.partitions[tp]
	return p, ok
}

// runHWCheckpoints writes the HW checkpoint every HWCheckpointInterval until ctx ends.
func (b *Broker) runHWCheckpoints(ctx context.Context) error {
	t := time.NewTicker(b.cfg.HWCheckpointInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := b.writeHWCheckpoint(ctx); err != nil && ctx.Err() == nil {
				b.logger.Error("hw checkpoint failed", slog.Any("error", err))
			}
		}
	}
}

// writeHWCheckpoint writes every open partition's HW, keeping entries of partitions not open.
func (b *Broker) writeHWCheckpoint(ctx context.Context) error {
	b.ckptMu.Lock()
	defer b.ckptMu.Unlock()
	b.mu.Lock()
	for tp, p := range b.partitions {
		b.hwCkpt[tp] = p.HighWatermark()
	}
	out := make(map[storage.TopicPartition]int64, len(b.hwCkpt))
	for tp, hw := range b.hwCkpt {
		out[tp] = hw
	}
	b.mu.Unlock()
	if err := storage.WriteOffsetCheckpoint(ctx, filepath.Join(b.cfg.DataDir, HWCheckpointFile), out); err != nil {
		return fmt.Errorf("write hw checkpoint: %w", err)
	}
	return nil
}

// BeginShutdown wakes every waiting long-poll fetch (they return what they have) and makes new
// ones return immediately, so the gRPC server can drain quickly. Idempotent; Close calls it.
func (b *Broker) BeginShutdown() {
	b.doneOnce.Do(func() { close(b.done) })
}

// Close stops following metadata, wakes waiting fetches, stops the background loops, writes the
// final HW checkpoint and closes storage (flushing every log). Idempotent.
func (b *Broker) Close(ctx context.Context) error {
	var err error
	b.closeOnce.Do(func() {
		b.unsubscribe()
		b.BeginShutdown()
		b.cancel()
		errs := []error{b.group.Wait()}
		errs = append(errs, b.writeHWCheckpoint(ctx), b.mgr.Close(ctx))
		err = errors.Join(errs...)
	})
	return err
}

// partitionMetadataFile records which topic id a partition directory belongs to (DECISIONS D17).
const partitionMetadataFile = "partition.metadata"

// File and directory modes, matching internal/storage (DECISIONS D8).
const (
	fileMode = 0o640
	dirMode  = 0o750
)

// ensurePartitionMetadata creates dir and writes partition.metadata atomically if missing.
func ensurePartitionMetadata(dir, id string) error {
	if got, err := readPartitionMetadata(dir); err == nil {
		if got != id {
			return fmt.Errorf("directory belongs to topic id %s, not %s", got, id)
		}
		return nil
	}
	_, statErr := os.Stat(dir)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("create partition dir: %w", err)
	}
	if errors.Is(statErr, os.ErrNotExist) {
		if err := syncDir(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	path := filepath.Join(dir, partitionMetadataFile)
	tmp := path + ".tmp"
	content := fmt.Sprintf("version: 0\ntopic_id: %s\n", id)
	if err := writeFileSync(tmp, []byte(content)); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename partition metadata: %w", err)
	}
	return syncDir(dir)
}

// readPartitionMetadata returns the topic id recorded in dir.
func readPartitionMetadata(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, partitionMetadataFile))
	if err != nil {
		return "", fmt.Errorf("read partition metadata: %w", err)
	}
	var version int
	var id string
	if _, err := fmt.Sscanf(string(data), "version: %d\ntopic_id: %s\n", &version, &id); err != nil || version != 0 || id == "" {
		return "", fmt.Errorf("malformed %s in %s", partitionMetadataFile, dir)
	}
	return id, nil
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("close dir: %w", err)
	}
	return nil
}

// NewTopicID returns 16 random bytes as 32 hex characters (DECISIONS D17).
func NewTopicID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate topic id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
