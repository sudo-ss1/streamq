// Package producerstate tracks idempotent producers per partition (SPEC §6): for each producer ID
// the metadata of its last five appended batches, used by the leader to drop retried duplicates
// and reject sequence gaps. Followers maintain the same state as they replicate, it is snapshotted
// to "<offset>.snapshot" files on segment roll, and rebuilt on recovery or truncation from the
// newest valid snapshot plus a replay of later batch headers.
//
// A State is not safe for concurrent use; the owning partition serializes access.
package producerstate

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
	"github.com/sudo-ss1/streamq/internal/storage"
)

// Sentinel errors.
var (
	// ErrOutOfOrderSequence reports a batch whose sequence neither continues the producer's last
	// batch nor duplicates one of its last five.
	ErrOutOfOrderSequence = errors.New("producerstate: out of order sequence")
	// ErrUnknownProducerID reports a non-zero sequence from a producer with no state (expired, or
	// never seen).
	ErrUnknownProducerID = errors.New("producerstate: unknown producer id")
	// ErrInvalidProducerBatch reports inconsistent idempotence fields (a producer ID without a
	// sequence or the reverse).
	ErrInvalidProducerBatch = errors.New("producerstate: invalid producer fields")
)

// MaxBatchesPerProducer is how many recent batches are kept per producer for duplicate detection.
const MaxBatchesPerProducer = 5

// errCorruptSnapshot marks a snapshot whose content is invalid (it is deleted); other read errors
// are returned so the rebuild fails and is retried (review finding).
var errCorruptSnapshot = errors.New("producerstate: corrupt snapshot")

const (
	snapshotSuffix  = ".snapshot"
	tmpSuffix       = ".tmp"
	snapshotVersion = 0
	replayChunk     = 1 << 20
)

// BatchMeta describes one appended batch of a producer.
type BatchMeta struct {
	FirstSeq     int32 `json:"first_seq"`
	LastSeq      int32 `json:"last_seq"`
	FirstOffset  int64 `json:"first_offset"`
	LastOffset   int64 `json:"last_offset"`
	MaxTimestamp int64 `json:"max_timestamp"`
}

type entry struct {
	epoch         int16
	batches       []BatchMeta // oldest first, at most MaxBatchesPerProducer
	lastTimestamp int64       // max batch timestamp seen (ms), for expiration
}

// BatchReader reads whole batches from a log (storage.Log satisfies it).
type BatchReader interface {
	Read(ctx context.Context, startOffset int64, maxBytes int, maxOffset int64) (storage.ReadResult, error)
	LogStartOffset() int64
}

// State is one partition's producer state.
type State struct {
	dir        string
	expiration time.Duration
	producers  map[int64]*entry
}

// New returns an empty State whose snapshots live in dir.
func New(dir string, expiration time.Duration) *State {
	return &State{dir: dir, expiration: expiration, producers: map[int64]*entry{}}
}

// Len returns the number of tracked producers.
func (s *State) Len() int { return len(s.producers) }

// LastSeq returns the sequence of the last record of a batch of count records starting at first,
// wrapping from MaxInt32 to 0 (Kafka).
func LastSeq(first, count int32) int32 {
	return int32((int64(first) + int64(count) - 1) % (math.MaxInt32 + 1))
}

func nextSeq(seq int32) int32 {
	if seq == math.MaxInt32 {
		return 0
	}
	return seq + 1
}

// ValidateFields checks that a batch's idempotence fields are consistent.
func ValidateFields(b record.Batch) error {
	pid, seq := b.ProducerID(), b.BaseSequence()
	if (pid >= 0) != (seq >= 0) || (pid < 0 && pid != record.NoProducerID) || (seq < 0 && seq != record.NoSequence) {
		return fmt.Errorf("%w: producer id %d, base sequence %d", ErrInvalidProducerBatch, pid, seq)
	}
	return nil
}

// FromBatch returns the producer fields and metadata of an appended batch (offsets stamped); ok
// is false for a non-idempotent batch.
func FromBatch(b record.Batch) (pid int64, epoch int16, m BatchMeta, ok bool) {
	pid = b.ProducerID()
	if pid < 0 || b.BaseSequence() < 0 {
		return 0, 0, BatchMeta{}, false
	}
	return pid, b.ProducerEpoch(), BatchMeta{
		FirstSeq: b.BaseSequence(), LastSeq: LastSeq(b.BaseSequence(), b.RecordCount()),
		FirstOffset: b.BaseOffset(), LastOffset: b.LastOffset(), MaxTimestamp: b.MaxTimestamp(),
	}, true
}

// Check validates an idempotent batch on the leader before it is appended. It returns the
// original batch and true for an exact duplicate of one of the producer's last five batches,
// false when the batch continues the sequence (or starts a new producer at 0), and
// ErrOutOfOrderSequence or ErrUnknownProducerID otherwise. The epoch is not checked (always 0,
// SPEC §3.5).
func (s *State) Check(pid int64, firstSeq, lastSeq int32) (BatchMeta, bool, error) {
	e, ok := s.producers[pid]
	if !ok || len(e.batches) == 0 {
		if firstSeq == 0 {
			return BatchMeta{}, false, nil
		}
		return BatchMeta{}, false, fmt.Errorf("%w: producer %d, sequence %d", ErrUnknownProducerID, pid, firstSeq)
	}
	for _, m := range e.batches {
		if m.FirstSeq == firstSeq && m.LastSeq == lastSeq {
			return m, true, nil
		}
	}
	last := e.batches[len(e.batches)-1]
	if firstSeq == nextSeq(last.LastSeq) {
		return BatchMeta{}, false, nil
	}
	return BatchMeta{}, false, fmt.Errorf("%w: producer %d, sequence %d after %d", ErrOutOfOrderSequence, pid, firstSeq, last.LastSeq)
}

// Update records an appended batch of producer pid (leader, follower and replay).
func (s *State) Update(pid int64, epoch int16, m BatchMeta) {
	update(s.producers, pid, epoch, m)
}

func update(producers map[int64]*entry, pid int64, epoch int16, m BatchMeta) {
	e, ok := producers[pid]
	if !ok || e.epoch != epoch {
		e = &entry{epoch: epoch}
		producers[pid] = e
	}
	e.batches = append(e.batches, m)
	if n := len(e.batches); n > MaxBatchesPerProducer {
		e.batches = slices.Clone(e.batches[n-MaxBatchesPerProducer:])
	}
	e.lastTimestamp = max(e.lastTimestamp, m.MaxTimestamp)
}

// UpdateFromBatch records an appended batch if it is idempotent.
func (s *State) UpdateFromBatch(b record.Batch) {
	if pid, epoch, m, ok := FromBatch(b); ok {
		s.Update(pid, epoch, m)
	}
}

// ExpireBefore drops producers whose newest batch timestamp is older than nowMs minus the
// expiration (producer_id_expiration_ms, D49).
func (s *State) ExpireBefore(nowMs int64) {
	cutoff := nowMs - s.expiration.Milliseconds()
	for pid, e := range s.producers {
		if e.lastTimestamp < cutoff {
			delete(s.producers, pid)
		}
	}
}

// --- snapshots ---

type snapshotProducer struct {
	ID            int64       `json:"id"`
	Epoch         int16       `json:"epoch"`
	LastTimestamp int64       `json:"last_timestamp"`
	Batches       []BatchMeta `json:"batches"`
}

type snapshotFile struct {
	Version   int                `json:"version"`
	Offset    int64              `json:"offset"`
	Producers []snapshotProducer `json:"producers"`
}

func snapshotName(offset int64) string { return fmt.Sprintf("%020d%s", offset, snapshotSuffix) }

func crcTable() *crc32.Table { return crc32.MakeTable(crc32.Castagnoli) }

// WriteSnapshot durably writes the current state as the snapshot at offset: the state of every
// batch whose last offset is below offset (D50).
func (s *State) WriteSnapshot(ctx context.Context, offset int64) error {
	f := snapshotFile{Version: snapshotVersion, Offset: offset, Producers: make([]snapshotProducer, 0, len(s.producers))}
	for pid, e := range s.producers {
		f.Producers = append(f.Producers, snapshotProducer{ID: pid, Epoch: e.epoch, LastTimestamp: e.lastTimestamp, Batches: e.batches})
	}
	slices.SortFunc(f.Producers, func(a, b snapshotProducer) int { return cmp.Compare(a.ID, b.ID) })
	body, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("encode producer snapshot: %w", err)
	}
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, crc32.Checksum(body, crcTable()))
	buf.Write(body)
	if err := storage.WriteFileAtomic(ctx, filepath.Join(s.dir, snapshotName(offset)), buf.Bytes()); err != nil {
		return fmt.Errorf("write producer snapshot: %w", err)
	}
	return nil
}

// readSnapshot loads one snapshot file.
func readSnapshot(path string, offset int64) (map[int64]*entry, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is built from the partition dir and a parsed offset
	if err != nil {
		return nil, fmt.Errorf("read producer snapshot: %w", err)
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("%w %s: too short", errCorruptSnapshot, path)
	}
	if crc32.Checksum(data[4:], crcTable()) != binary.BigEndian.Uint32(data) {
		return nil, fmt.Errorf("%w %s: crc mismatch", errCorruptSnapshot, path)
	}
	var f snapshotFile
	if err := json.Unmarshal(data[4:], &f); err != nil {
		return nil, fmt.Errorf("%w %s: %w", errCorruptSnapshot, path, err)
	}
	if f.Version != snapshotVersion || f.Offset != offset {
		return nil, fmt.Errorf("%w %s: version %d offset %d", errCorruptSnapshot, path, f.Version, f.Offset)
	}
	out := make(map[int64]*entry, len(f.Producers))
	for _, p := range f.Producers {
		if len(p.Batches) == 0 || len(p.Batches) > MaxBatchesPerProducer {
			return nil, fmt.Errorf("%w %s: producer %d has %d batches", errCorruptSnapshot, path, p.ID, len(p.Batches))
		}
		out[p.ID] = &entry{epoch: p.Epoch, batches: p.Batches, lastTimestamp: p.LastTimestamp}
	}
	return out, nil
}

// snapshotOffsets lists snapshot offsets in ascending order, removing leftover temp files.
func (s *State) snapshotOffsets() ([]int64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list producer snapshots: %w", err)
	}
	var out []int64
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(name, snapshotSuffix+tmpSuffix) {
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, err)
			}
			continue
		}
		digits, ok := strings.CutSuffix(name, snapshotSuffix)
		if !ok || len(digits) != 20 {
			continue
		}
		off, err := strconv.ParseInt(digits, 10, 64)
		if err != nil || off < 0 {
			continue
		}
		out = append(out, off)
	}
	slices.Sort(out)
	return out, errors.Join(errs...)
}

// deleteSnapshots removes the snapshots for which drop is true, then fsyncs the directory.
func (s *State) deleteSnapshots(ctx context.Context, drop func(int64) bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	offs, err := s.snapshotOffsets()
	if err != nil {
		return err
	}
	removed := false
	var errs []error
	for _, off := range offs {
		if !drop(off) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, snapshotName(off))); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
			continue
		}
		removed = true
	}
	if removed {
		errs = append(errs, storage.SyncDir(s.dir))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("delete producer snapshots: %w", err)
	}
	return nil
}

// TruncateSnapshots deletes snapshots above offset (after a log truncation).
func (s *State) TruncateSnapshots(ctx context.Context, offset int64) error {
	return s.deleteSnapshots(ctx, func(off int64) bool { return off > offset })
}

// DeleteSnapshotsBelow deletes snapshots below logStart (after retention).
func (s *State) DeleteSnapshotsBelow(ctx context.Context, logStart int64) error {
	return s.deleteSnapshots(ctx, func(off int64) bool { return off < logStart })
}

// Clear deletes every snapshot (durably) and then empties the state (a full truncation; called
// before the log is emptied, plan R2).
func (s *State) Clear(ctx context.Context) error {
	if err := s.deleteSnapshots(ctx, func(int64) bool { return true }); err != nil {
		return err
	}
	s.producers = map[int64]*entry{}
	return nil
}

// Rebuild reconstructs the state for a log holding [logStart, leo): it deletes snapshots outside
// that range and temp files, loads the newest valid snapshot in it (deleting corrupt ones), and
// replays later batch headers from src. The result replaces the state only on success (plan R1);
// on error the state is unchanged and the caller must treat it as invalid. If retention moves the
// log start during the replay, it retries once from the new start (plan R20).
func (s *State) Rebuild(ctx context.Context, src BatchReader, logStart, leo int64) error {
	err := s.rebuild(ctx, src, logStart, leo)
	if errors.Is(err, storage.ErrOffsetOutOfRange) {
		if start := src.LogStartOffset(); start > logStart {
			err = s.rebuild(ctx, src, start, leo)
		}
	}
	return err
}

func (s *State) rebuild(ctx context.Context, src BatchReader, logStart, leo int64) error {
	if err := s.deleteSnapshots(ctx, func(off int64) bool { return off < logStart || off > leo }); err != nil {
		return err
	}
	offs, err := s.snapshotOffsets()
	if err != nil {
		return err
	}
	producers, from := map[int64]*entry{}, logStart
	for i := len(offs) - 1; i >= 0; i-- {
		loaded, err := readSnapshot(filepath.Join(s.dir, snapshotName(offs[i])), offs[i])
		if err == nil {
			producers, from = loaded, offs[i]
			break
		}
		if !errors.Is(err, errCorruptSnapshot) {
			return err // transient I/O error: keep the snapshot, fail the rebuild, retry later
		}
		// Corrupt: drop it and fall back to an older one (or a full replay).
		off := offs[i]
		if derr := s.deleteSnapshots(ctx, func(o int64) bool { return o == off }); derr != nil {
			return errors.Join(err, derr)
		}
	}
	for off := from; off < leo; {
		res, err := src.Read(ctx, off, replayChunk, leo)
		if err != nil {
			return fmt.Errorf("replay producer state at %d: %w", off, err)
		}
		batches, err := record.SplitBatches(res.Data)
		if err != nil {
			return fmt.Errorf("replay producer state at %d: %w", off, err)
		}
		if len(batches) == 0 {
			return fmt.Errorf("replay producer state: no batch at %d below %d", off, leo)
		}
		for _, b := range batches {
			if b.LastOffset() < off {
				continue
			}
			if pid, epoch, m, ok := FromBatch(b); ok {
				update(producers, pid, epoch, m)
			}
		}
		off = batches[len(batches)-1].NextOffset()
	}
	s.producers = producers
	return nil
}
