package record

import (
	"encoding/binary"
	"errors"
)

// Batch header layout (SPEC §3.5). Each constant is the byte offset of a field.
const (
	BaseOffsetOffset      = 0
	BatchLengthOffset     = 8
	LeaderEpochOffset     = 12
	MagicOffset           = 16
	CRCOffset             = 17
	AttributesOffset      = 21
	LastOffsetDeltaOffset = 23
	FirstTimestampOffset  = 27
	MaxTimestampOffset    = 35
	ProducerIDOffset      = 43
	ProducerEpochOffset   = 51
	BaseSequenceOffset    = 53
	RecordCountOffset     = 57

	// HeaderSize is the size of the fixed batch header in bytes.
	HeaderSize = 61
	// LogOverhead is the number of bytes before the batchLength-counted region
	// (baseOffset + batchLength).
	LogOverhead = 12
)

const (
	// Magic is the only supported batch format version.
	Magic int8 = 1
	// NoProducerID marks a batch from a non-idempotent producer.
	NoProducerID int64 = -1
	// NoSequence marks a batch without a producer sequence.
	NoSequence int32 = -1
)

// Sentinel errors returned by this package.
var (
	// ErrCorrupt reports a batch whose CRC, length, magic or record framing is invalid.
	ErrCorrupt = errors.New("record: corrupt batch")
	// ErrTruncated reports a buffer shorter than the batch length it declares.
	ErrTruncated = errors.New("record: truncated batch")
	// ErrTooLarge reports a batch larger than the allowed maximum (max.message.bytes).
	ErrTooLarge = errors.New("record: batch too large")
)

// BatchHeader holds the decoded fixed-size batch header.
type BatchHeader struct {
	BaseOffset      int64
	BatchLength     int32
	LeaderEpoch     int32
	Magic           int8
	CRC             uint32
	Attributes      int16
	LastOffsetDelta int32
	FirstTimestamp  int64
	MaxTimestamp    int64
	ProducerID      int64
	ProducerEpoch   int16
	BaseSequence    int32
	RecordCount     int32
}

// Header is one key/value header attached to a record.
type Header struct {
	Key   string
	Value []byte
}

// Record is one decoded record with absolute offset and timestamp. A nil Key or Value is
// encoded as null (length -1); a non-nil empty slice is encoded with length 0.
type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Batch is one encoded record batch. Accessors assume len(b) >= HeaderSize, which holds for any
// batch returned by ParseHeader-checked framing, Validate, SplitBatches or Builder.Build.
type Batch []byte

// BaseOffset returns the offset of the first record in the batch.
func (b Batch) BaseOffset() int64 { return int64(binary.BigEndian.Uint64(b[BaseOffsetOffset:])) }

// SetBaseOffset stamps the base offset in place. It is outside the CRC range.
func (b Batch) SetBaseOffset(off int64) {
	binary.BigEndian.PutUint64(b[BaseOffsetOffset:], uint64(off))
}

// LeaderEpoch returns the leader epoch stamped on the batch.
func (b Batch) LeaderEpoch() int32 { return int32(binary.BigEndian.Uint32(b[LeaderEpochOffset:])) }

// SetLeaderEpoch stamps the leader epoch in place. It is outside the CRC range.
func (b Batch) SetLeaderEpoch(epoch int32) {
	binary.BigEndian.PutUint32(b[LeaderEpochOffset:], uint32(epoch))
}

// LastOffsetDelta returns the offset delta of the last record.
func (b Batch) LastOffsetDelta() int32 {
	return int32(binary.BigEndian.Uint32(b[LastOffsetDeltaOffset:]))
}

// LastOffset returns the offset of the last record in the batch.
func (b Batch) LastOffset() int64 { return b.BaseOffset() + int64(b.LastOffsetDelta()) }

// NextOffset returns the offset following the last record in the batch.
func (b Batch) NextOffset() int64 { return b.LastOffset() + 1 }

// RecordCount returns the number of records in the batch.
func (b Batch) RecordCount() int32 { return int32(binary.BigEndian.Uint32(b[RecordCountOffset:])) }

// FirstTimestamp returns the timestamp of the first record (unix ms).
func (b Batch) FirstTimestamp() int64 {
	return int64(binary.BigEndian.Uint64(b[FirstTimestampOffset:]))
}

// MaxTimestamp returns the largest record timestamp in the batch (unix ms).
func (b Batch) MaxTimestamp() int64 { return int64(binary.BigEndian.Uint64(b[MaxTimestampOffset:])) }

// ProducerID returns the idempotent producer ID, or NoProducerID.
func (b Batch) ProducerID() int64 { return int64(binary.BigEndian.Uint64(b[ProducerIDOffset:])) }

// ProducerEpoch returns the producer epoch.
func (b Batch) ProducerEpoch() int16 {
	return int16(binary.BigEndian.Uint16(b[ProducerEpochOffset:]))
}

// BaseSequence returns the producer sequence of the first record, or NoSequence.
func (b Batch) BaseSequence() int32 { return int32(binary.BigEndian.Uint32(b[BaseSequenceOffset:])) }

// Size returns the total encoded size of the batch in bytes (LogOverhead + batchLength).
func (b Batch) Size() int {
	return LogOverhead + int(int32(binary.BigEndian.Uint32(b[BatchLengthOffset:])))
}
