package record

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// batchHeaderTail is the number of header bytes counted by batchLength (everything after it).
const batchHeaderTail = HeaderSize - LogOverhead

// minRecordSize is the smallest possible encoded record: a one-byte length followed by
// attributes, timestampDelta, offsetDelta, keyLength, valueLength and headerCount, one byte each.
const minRecordSize = 7

// minHeaderSize is the smallest possible encoded record header: an empty key and a null value.
const minHeaderSize = 2

// ParseHeader decodes the fixed header at the start of buf. It needs at least HeaderSize bytes
// (ErrTruncated otherwise) and checks the magic byte, that batchLength covers at least the rest
// of the header, that recordCount >= 1 and that lastOffsetDelta == recordCount-1 (ErrCorrupt).
// It does not check the CRC or that buf holds the whole batch.
//
// It also rejects a recordCount larger than the record bytes could possibly hold, so callers
// can size allocations from it.
func ParseHeader(buf []byte) (BatchHeader, error) {
	if len(buf) < HeaderSize {
		return BatchHeader{}, fmt.Errorf("%w: %d bytes, header needs %d", ErrTruncated, len(buf), HeaderSize)
	}
	be := binary.BigEndian
	h := BatchHeader{
		BaseOffset:      int64(be.Uint64(buf[BaseOffsetOffset:])),
		BatchLength:     int32(be.Uint32(buf[BatchLengthOffset:])),
		LeaderEpoch:     int32(be.Uint32(buf[LeaderEpochOffset:])),
		Magic:           int8(buf[MagicOffset]),
		CRC:             be.Uint32(buf[CRCOffset:]),
		Attributes:      int16(be.Uint16(buf[AttributesOffset:])),
		LastOffsetDelta: int32(be.Uint32(buf[LastOffsetDeltaOffset:])),
		FirstTimestamp:  int64(be.Uint64(buf[FirstTimestampOffset:])),
		MaxTimestamp:    int64(be.Uint64(buf[MaxTimestampOffset:])),
		ProducerID:      int64(be.Uint64(buf[ProducerIDOffset:])),
		ProducerEpoch:   int16(be.Uint16(buf[ProducerEpochOffset:])),
		BaseSequence:    int32(be.Uint32(buf[BaseSequenceOffset:])),
		RecordCount:     int32(be.Uint32(buf[RecordCountOffset:])),
	}
	switch {
	case h.Magic != Magic:
		return BatchHeader{}, fmt.Errorf("%w: magic %d, want %d", ErrCorrupt, h.Magic, Magic)
	case h.BatchLength < batchHeaderTail:
		return BatchHeader{}, fmt.Errorf("%w: batchLength %d below minimum %d", ErrCorrupt, h.BatchLength, batchHeaderTail)
	case h.RecordCount < 1:
		return BatchHeader{}, fmt.Errorf("%w: recordCount %d", ErrCorrupt, h.RecordCount)
	case h.LastOffsetDelta != h.RecordCount-1:
		return BatchHeader{}, fmt.Errorf("%w: lastOffsetDelta %d with recordCount %d", ErrCorrupt, h.LastOffsetDelta, h.RecordCount)
	case int64(h.RecordCount) > int64(h.BatchLength-batchHeaderTail)/minRecordSize:
		return BatchHeader{}, fmt.Errorf("%w: recordCount %d cannot fit in batchLength %d", ErrCorrupt, h.RecordCount, h.BatchLength)
	}
	return h, nil
}

// checkLength compares the declared batch size with the buffer: short is ErrTruncated, long
// (trailing bytes) is ErrCorrupt.
func checkLength(h BatchHeader, n int) error {
	size := int64(LogOverhead) + int64(h.BatchLength)
	switch {
	case int64(n) < size:
		return fmt.Errorf("%w: have %d bytes, batch declares %d", ErrTruncated, n, size)
	case int64(n) > size:
		return fmt.Errorf("%w: %d trailing bytes after batch", ErrCorrupt, int64(n)-size)
	}
	return nil
}

// Validate fully checks the single batch in b: header (as ParseHeader), exact length, CRC-32C,
// that every record decodes with offset deltas 0..n-1 in order, and that no bytes trail the last
// record. A batch longer than maxBytes fails with ErrTooLarge before any other check. On success
// it returns b as a Batch.
//
// Only canonical batches (exactly what Builder produces, apart from baseOffset and leaderEpoch)
// are accepted: minimal varints, zero batch and record attributes, a first timestampDelta of 0,
// maxTimestamp equal to the largest record timestamp, exact record lengths and non-null header
// keys. Anything else is ErrCorrupt.
func Validate(b []byte, maxBytes int) (Batch, error) {
	if len(b) > maxBytes {
		return nil, fmt.Errorf("%w: %d bytes, max %d", ErrTooLarge, len(b), maxBytes)
	}
	h, err := ParseHeader(b)
	if err != nil {
		return nil, err
	}
	if err := checkLength(h, len(b)); err != nil {
		return nil, err
	}
	if got := crc32.Checksum(b[AttributesOffset:], crc32.MakeTable(crc32.Castagnoli)); got != h.CRC {
		return nil, fmt.Errorf("%w: crc %#08x, header says %#08x", ErrCorrupt, got, h.CRC)
	}
	if h.Attributes != 0 {
		return nil, fmt.Errorf("%w: batch attributes %#04x", ErrCorrupt, h.Attributes)
	}
	_, maxTs, err := decodeRecords(b[HeaderSize:], h)
	if err != nil {
		return nil, err
	}
	if maxTs != h.MaxTimestamp {
		return nil, fmt.Errorf("%w: maxTimestamp %d, records say %d", ErrCorrupt, h.MaxTimestamp, maxTs)
	}
	return Batch(b), nil
}

// SplitBatches splits a buffer of concatenated batches using header framing only (no CRC check).
// A cut-off final batch yields ErrTruncated together with the complete batches before it.
// A header that fails ParseHeader yields ErrCorrupt together with the batches before it.
// Each returned Batch aliases buf.
func SplitBatches(buf []byte) ([]Batch, error) {
	var out []Batch
	for len(buf) > 0 {
		h, err := ParseHeader(buf)
		if err != nil {
			return out, fmt.Errorf("batch %d: %w", len(out), err)
		}
		size := int64(LogOverhead) + int64(h.BatchLength)
		if int64(len(buf)) < size {
			return out, fmt.Errorf("batch %d: %w: have %d bytes, batch declares %d", len(out), ErrTruncated, len(buf), size)
		}
		out = append(out, Batch(buf[:size:size]))
		buf = buf[size:]
	}
	return out, nil
}

// Records decodes every record in the batch with absolute offsets and timestamps. It validates
// record framing but not the CRC.
//
// The returned Key, Value and header Value slices alias the batch buffer: they stay valid only as
// long as the batch bytes are not modified or reused. Copy them to keep them longer. A null key or
// value decodes as nil; an empty one decodes as a non-nil empty slice.
func (b Batch) Records() ([]Record, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return nil, err
	}
	if err := checkLength(h, len(b)); err != nil {
		return nil, err
	}
	recs, _, err := decodeRecords(b[HeaderSize:], h)
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// decodeRecords decodes exactly h.RecordCount canonical records filling buf and returns them with
// the largest record timestamp. ParseHeader has bounded h.RecordCount by len(buf), so the
// preallocation below cannot be driven beyond the input size.
func decodeRecords(buf []byte, h BatchHeader) ([]Record, int64, error) {
	recs := make([]Record, 0, h.RecordCount)
	maxTs := h.FirstTimestamp
	for i := int32(0); i < h.RecordCount; i++ {
		length, n, err := readVarint32(buf, "record length")
		if err != nil {
			return nil, 0, fmt.Errorf("record %d: %w", i, err)
		}
		buf = buf[n:]
		if length < 0 || int64(length) > int64(len(buf)) {
			return nil, 0, fmt.Errorf("record %d: %w: length %d with %d bytes left", i, ErrCorrupt, length, len(buf))
		}
		rec, err := decodeRecord(buf[:length:length], i, h)
		if err != nil {
			return nil, 0, fmt.Errorf("record %d: %w", i, err)
		}
		buf = buf[length:]
		if i == 0 || rec.Timestamp > maxTs {
			maxTs = rec.Timestamp
		}
		recs = append(recs, rec)
	}
	if len(buf) != 0 {
		return nil, 0, fmt.Errorf("%w: %d trailing bytes after last record", ErrCorrupt, len(buf))
	}
	return recs, maxTs, nil
}

// cursor reads fields from one record body, recording the first error.
type cursor struct {
	buf []byte
	err error
}

func (c *cursor) varint(field string) int64 {
	if c.err != nil {
		return 0
	}
	v, n, err := readVarint(c.buf, field)
	if err != nil {
		c.err = err
		return 0
	}
	c.buf = c.buf[n:]
	return v
}

func (c *cursor) varint32(field string) int32 {
	if c.err != nil {
		return 0
	}
	v, n, err := readVarint32(c.buf, field)
	if err != nil {
		c.err = err
		return 0
	}
	c.buf = c.buf[n:]
	return v
}

// bytes reads a length-prefixed byte field. Length -1 is null (nil) unless nullable is false.
func (c *cursor) bytes(field string, nullable bool) []byte {
	n := c.varint32(field + " length")
	if c.err != nil {
		return nil
	}
	switch {
	case n == -1 && nullable:
		return nil
	case n < 0:
		c.err = fmt.Errorf("%w: %s length %d", ErrCorrupt, field, n)
		return nil
	case int64(n) > int64(len(c.buf)):
		c.err = fmt.Errorf("%w: %s length %d with %d bytes left", ErrCorrupt, field, n, len(c.buf))
		return nil
	}
	v := c.buf[:n:n]
	c.buf = c.buf[n:]
	return v
}

// decodeRecord decodes the canonical record body at index i.
func decodeRecord(body []byte, i int32, h BatchHeader) (Record, error) {
	if len(body) < 1 {
		return Record{}, fmt.Errorf("%w: empty record body", ErrCorrupt)
	}
	if body[0] != 0 {
		return Record{}, fmt.Errorf("%w: record attributes %#02x", ErrCorrupt, body[0])
	}
	c := cursor{buf: body[1:]}
	tsDelta := c.varint("timestampDelta")
	offDelta := c.varint32("offsetDelta")
	if c.err != nil {
		return Record{}, c.err
	}
	if i == 0 && tsDelta != 0 {
		return Record{}, fmt.Errorf("%w: first record timestampDelta %d", ErrCorrupt, tsDelta)
	}
	if offDelta != i {
		return Record{}, fmt.Errorf("%w: offsetDelta %d, want %d", ErrCorrupt, offDelta, i)
	}
	rec := Record{
		Offset:    h.BaseOffset + int64(offDelta),
		Timestamp: h.FirstTimestamp + tsDelta,
	}
	rec.Key = c.bytes("key", true)
	rec.Value = c.bytes("value", true)
	count := c.varint32("headerCount")
	if c.err != nil {
		return Record{}, c.err
	}
	if count < 0 || int64(count) > int64(len(c.buf))/minHeaderSize {
		return Record{}, fmt.Errorf("%w: headerCount %d with %d bytes left", ErrCorrupt, count, len(c.buf))
	}
	for j := int32(0); j < count; j++ {
		key := c.bytes("header key", false)
		val := c.bytes("header value", true)
		if c.err != nil {
			return Record{}, fmt.Errorf("header %d: %w", j, c.err)
		}
		rec.Headers = append(rec.Headers, Header{Key: string(key), Value: val})
	}
	if len(c.buf) != 0 {
		return Record{}, fmt.Errorf("%w: %d bytes left in record after headers", ErrCorrupt, len(c.buf))
	}
	return rec, nil
}
