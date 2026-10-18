package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

var errEmptyBatch = errors.New("record: batch has no records")

// BuilderOptions sets the producer fields of a batch. The zero value is not a valid
// non-idempotent batch; use NoProducerID and NoSequence for that.
type BuilderOptions struct {
	ProducerID    int64
	ProducerEpoch int16
	BaseSequence  int32
}

// Builder accumulates records and encodes them as one batch with baseOffset 0 and
// leaderEpoch 0 (the leader assigns both on append).
type Builder struct {
	opts    BuilderOptions
	records []byte // encoded records, in order
	count   int
	firstTs int64
	maxTs   int64
}

// NewBuilder returns an empty Builder.
func NewBuilder(opts BuilderOptions) *Builder { return &Builder{opts: opts} }

// Append adds one record. timestamp is unix ms. nil key/value encode as null.
// A nil Header.Value encodes as null; header keys are never null.
func (b *Builder) Append(timestamp int64, key, value []byte, headers []Header) {
	if b.count == 0 {
		b.firstTs = timestamp
		b.maxTs = timestamp
	} else if timestamp > b.maxTs {
		b.maxTs = timestamp
	}
	tsDelta := timestamp - b.firstTs
	offDelta := int64(b.count)

	body := 1 + varintSize(tsDelta) + varintSize(offDelta) +
		nullableSize(key) + nullableSize(value) + varintSize(int64(len(headers)))
	for _, h := range headers {
		body += varintSize(int64(len(h.Key))) + len(h.Key) + nullableSize(h.Value)
	}

	r := b.records
	r = binary.AppendVarint(r, int64(body))
	r = append(r, 0) // attributes
	r = binary.AppendVarint(r, tsDelta)
	r = binary.AppendVarint(r, offDelta)
	r = appendNullable(r, key)
	r = appendNullable(r, value)
	r = binary.AppendVarint(r, int64(len(headers)))
	for _, h := range headers {
		r = binary.AppendVarint(r, int64(len(h.Key)))
		r = append(r, h.Key...)
		r = appendNullable(r, h.Value)
	}
	b.records = r
	b.count++
}

// nullableSize returns the encoded size of a length-prefixed, nullable byte field.
func nullableSize(v []byte) int {
	if v == nil {
		return varintSize(-1)
	}
	return varintSize(int64(len(v))) + len(v)
}

// appendNullable appends a length-prefixed byte field, encoding nil as length -1.
func appendNullable(dst, v []byte) []byte {
	if v == nil {
		return binary.AppendVarint(dst, -1)
	}
	dst = binary.AppendVarint(dst, int64(len(v)))
	return append(dst, v...)
}

// Len returns the number of records appended so far.
func (b *Builder) Len() int { return b.count }

// Size returns the encoded size the batch would have if built now.
func (b *Builder) Size() int { return HeaderSize + len(b.records) }

// Build encodes the batch. It fails if no records were appended, or if the batch would exceed
// the int32 batchLength field. The Builder is left unchanged, and the returned Batch does not
// share memory with it.
func (b *Builder) Build() (Batch, error) {
	if b.count == 0 {
		return nil, errEmptyBatch
	}
	batchLength := int64(HeaderSize-LogOverhead) + int64(len(b.records))
	if batchLength > math.MaxInt32 || int64(b.count) > math.MaxInt32 {
		return nil, fmt.Errorf("%w: %d records, batchLength %d", ErrTooLarge, b.count, batchLength)
	}
	out := make([]byte, HeaderSize, HeaderSize+len(b.records))
	be := binary.BigEndian
	be.PutUint64(out[BaseOffsetOffset:], 0)
	be.PutUint32(out[BatchLengthOffset:], uint32(batchLength))
	be.PutUint32(out[LeaderEpochOffset:], 0)
	out[MagicOffset] = byte(Magic)
	be.PutUint16(out[AttributesOffset:], 0)
	be.PutUint32(out[LastOffsetDeltaOffset:], uint32(b.count-1))
	be.PutUint64(out[FirstTimestampOffset:], uint64(b.firstTs))
	be.PutUint64(out[MaxTimestampOffset:], uint64(b.maxTs))
	be.PutUint64(out[ProducerIDOffset:], uint64(b.opts.ProducerID))
	be.PutUint16(out[ProducerEpochOffset:], uint16(b.opts.ProducerEpoch))
	be.PutUint32(out[BaseSequenceOffset:], uint32(b.opts.BaseSequence))
	be.PutUint32(out[RecordCountOffset:], uint32(b.count))
	out = append(out, b.records...)
	be.PutUint32(out[CRCOffset:], crc32.Checksum(out[AttributesOffset:], crc32.MakeTable(crc32.Castagnoli)))
	return Batch(out), nil
}
