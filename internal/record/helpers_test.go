package record

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

const testMaxBytes = 1 << 20

func noProducer() BuilderOptions {
	return BuilderOptions{ProducerID: NoProducerID, BaseSequence: NoSequence}
}

// input is one record to append through a Builder.
type input struct {
	ts      int64
	key     []byte
	value   []byte
	headers []Header
}

func buildBatch(tb testing.TB, opts BuilderOptions, in []input) Batch {
	tb.Helper()
	bld := NewBuilder(opts)
	for _, r := range in {
		bld.Append(r.ts, r.key, r.value, r.headers)
	}
	b, err := bld.Build()
	require.NoError(tb, err)
	require.Len(tb, b, bld.Size())
	return b
}

func simpleBatch(tb testing.TB, n int) Batch {
	tb.Helper()
	in := make([]input, n)
	for i := range in {
		in[i] = input{ts: 1000 + int64(i), key: []byte{byte(i)}, value: bytes.Repeat([]byte{'v'}, i%7)}
	}
	return buildBatch(tb, noProducer(), in)
}

// fixCRC recomputes the CRC of a hand-modified batch in place.
func fixCRC(b []byte) {
	binary.BigEndian.PutUint32(b[CRCOffset:], crc32.Checksum(b[AttributesOffset:], crc32.MakeTable(crc32.Castagnoli)))
}

// rawBatch builds a batch around hand-encoded record bytes, with a correct batchLength and CRC.
func rawBatch(records []byte, count int32, firstTs, maxTs int64) []byte {
	b := make([]byte, HeaderSize, HeaderSize+len(records))
	be := binary.BigEndian
	be.PutUint32(b[BatchLengthOffset:], uint32(HeaderSize-LogOverhead+len(records)))
	b[MagicOffset] = byte(Magic)
	be.PutUint32(b[LastOffsetDeltaOffset:], uint32(count-1))
	be.PutUint64(b[FirstTimestampOffset:], uint64(firstTs))
	be.PutUint64(b[MaxTimestampOffset:], uint64(maxTs))
	be.PutUint64(b[ProducerIDOffset:], math.MaxUint64)
	be.PutUint32(b[BaseSequenceOffset:], math.MaxUint32)
	be.PutUint32(b[RecordCountOffset:], uint32(count))
	b = append(b, records...)
	fixCRC(b)
	return b
}

// rawRecord encodes one record from its body fields, prefixing the correct length.
func rawRecord(body ...[]byte) []byte {
	var flat []byte
	for _, p := range body {
		flat = append(flat, p...)
	}
	out := binary.AppendVarint(nil, int64(len(flat)))
	return append(out, flat...)
}

func vi(v int64) []byte { return binary.AppendVarint(nil, v) }
