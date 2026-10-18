package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	big := bytes.Repeat([]byte{0xab}, 512<<10)
	many := make([]input, 1000)
	for i := range many {
		many[i] = input{ts: 5000 + int64(i), key: []byte("k"), value: []byte{byte(i), byte(i >> 8)}}
	}
	tests := []struct {
		name string
		opts BuilderOptions
		in   []input
	}{
		{"single", noProducer(), []input{{ts: 1, key: []byte("k"), value: []byte("v")}}},
		{"null key and value", noProducer(), []input{{ts: 1}}},
		{"empty key and value", noProducer(), []input{{ts: 1, key: []byte{}, value: []byte{}}}},
		{"mixed null and empty", noProducer(), []input{
			{ts: 1, key: nil, value: []byte{}},
			{ts: 2, key: []byte{}, value: nil},
		}},
		{"headers", noProducer(), []input{{ts: 1, value: []byte("v"), headers: []Header{
			{Key: "a", Value: []byte("1")},
			{Key: "", Value: []byte{}},
			{Key: "null", Value: nil},
		}}}},
		{"1000 records", noProducer(), many},
		{"out of order timestamps", noProducer(), []input{
			{ts: 100, value: []byte("a")}, {ts: 50, value: []byte("b")},
			{ts: 300, value: []byte("c")}, {ts: -7, value: []byte("d")},
		}},
		{"extreme timestamps", noProducer(), []input{
			{ts: math.MaxInt64}, {ts: math.MinInt64}, {ts: 0},
		}},
		{"512 KiB value", noProducer(), []input{{ts: 9, key: []byte("big"), value: big}}},
		{"idempotent producer", BuilderOptions{ProducerID: 42, ProducerEpoch: 3, BaseSequence: 17},
			[]input{{ts: 1, value: []byte("x")}, {ts: 2, value: []byte("y")}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := buildBatch(t, tc.opts, tc.in)
			got, err := Validate(b, testMaxBytes)
			require.NoError(t, err)
			require.Equal(t, b, got)

			require.Equal(t, int64(0), b.BaseOffset())
			require.Equal(t, int32(0), b.LeaderEpoch())
			require.Equal(t, int32(len(tc.in)), b.RecordCount())
			require.Equal(t, int32(len(tc.in)-1), b.LastOffsetDelta())
			require.Equal(t, int64(len(tc.in)), b.NextOffset())
			require.Equal(t, len(b), b.Size())
			require.Equal(t, tc.opts.ProducerID, b.ProducerID())
			require.Equal(t, tc.opts.ProducerEpoch, b.ProducerEpoch())
			require.Equal(t, tc.opts.BaseSequence, b.BaseSequence())
			require.Equal(t, tc.in[0].ts, b.FirstTimestamp())
			maxTs := tc.in[0].ts
			for _, r := range tc.in {
				maxTs = max(maxTs, r.ts)
			}
			require.Equal(t, maxTs, b.MaxTimestamp())

			const base = 1 << 40
			b.SetBaseOffset(base)
			recs, err := b.Records()
			require.NoError(t, err)
			require.Len(t, recs, len(tc.in))
			for i, r := range recs {
				want := tc.in[i]
				require.Equal(t, int64(base+i), r.Offset)
				require.Equal(t, want.ts, r.Timestamp)
				requireNullable(t, want.key, r.Key)
				requireNullable(t, want.value, r.Value)
				require.Len(t, r.Headers, len(want.headers))
				for j, h := range r.Headers {
					require.Equal(t, want.headers[j].Key, h.Key)
					requireNullable(t, want.headers[j].Value, h.Value)
				}
			}
		})
	}
}

func requireNullable(t *testing.T, want, got []byte) {
	t.Helper()
	if want == nil {
		require.Nil(t, got)
		return
	}
	require.NotNil(t, got)
	require.Equal(t, want, got)
}

func TestRecordsAliasBatch(t *testing.T) {
	b := buildBatch(t, noProducer(), []input{{ts: 1, key: []byte("k"), value: []byte("v")}})
	recs, err := b.Records()
	require.NoError(t, err)
	b[len(b)-2] = 'X' // the value byte (last byte is headerCount)
	require.Equal(t, []byte("X"), recs[0].Value)
}

func TestBuilder(t *testing.T) {
	bld := NewBuilder(noProducer())
	require.Equal(t, 0, bld.Len())
	require.Equal(t, HeaderSize, bld.Size())
	_, err := bld.Build()
	require.Error(t, err)

	bld.Append(1, []byte("k"), []byte("v"), nil)
	bld.Append(2, nil, nil, []Header{{Key: "h", Value: nil}})
	require.Equal(t, 2, bld.Len())
	first, err := bld.Build()
	require.NoError(t, err)
	require.Len(t, first, bld.Size())
	second, err := bld.Build()
	require.NoError(t, err)
	require.Equal(t, first, second)
	first[HeaderSize] ^= 0xff
	require.NotEqual(t, first, second, "Build must not share memory between calls")
}

func TestStampKeepsCRC(t *testing.T) {
	tests := []struct {
		name   string
		offset int64
		epoch  int32
	}{
		{"zero", 0, 0},
		{"typical", 12345, 7},
		{"max", math.MaxInt64 - 10, math.MaxInt32},
		{"negative epoch", 1, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := simpleBatch(t, 5)
			b.SetBaseOffset(tc.offset)
			b.SetLeaderEpoch(tc.epoch)
			_, err := Validate(b, testMaxBytes)
			require.NoError(t, err)
			require.Equal(t, tc.offset, b.BaseOffset())
			require.Equal(t, tc.epoch, b.LeaderEpoch())
			require.Equal(t, tc.offset+4, b.LastOffset())
		})
	}
}

func TestEveryCRCByteFlipped(t *testing.T) {
	orig := buildBatch(t, BuilderOptions{ProducerID: 9, ProducerEpoch: 1, BaseSequence: 3}, []input{
		{ts: 10, key: []byte("key"), value: []byte("value"), headers: []Header{{Key: "h", Value: []byte("x")}}},
		{ts: 5, key: nil, value: []byte{}},
		{ts: 20, key: []byte{}, value: nil},
	})
	for i := AttributesOffset; i < len(orig); i++ {
		for _, mask := range []byte{0x01, 0x80, 0xff} {
			b := bytes.Clone(orig)
			b[i] ^= mask
			_, err := Validate(b, testMaxBytes)
			require.ErrorIs(t, err, ErrCorrupt, "byte %d mask %#x", i, mask)
		}
	}
}

func TestHeaderBytesOutsideCRC(t *testing.T) {
	orig := simpleBatch(t, 3)
	for i := range AttributesOffset {
		b := bytes.Clone(orig)
		b[i] ^= 0xff
		_, err := Validate(b, testMaxBytes)
		switch {
		case i < BatchLengthOffset || (i >= LeaderEpochOffset && i < MagicOffset):
			require.NoError(t, err, "byte %d", i) // baseOffset, leaderEpoch
		case i >= BatchLengthOffset && i < LeaderEpochOffset:
			require.Error(t, err, "byte %d", i)
			require.True(t, isFramingErr(err), "byte %d: %v", i, err)
		default:
			require.ErrorIs(t, err, ErrCorrupt, "byte %d", i) // magic, crc
		}
	}
}

func isFramingErr(err error) bool {
	return errors.Is(err, ErrCorrupt) || errors.Is(err, ErrTruncated)
}

func TestTruncation(t *testing.T) {
	orig := buildBatch(t, noProducer(), []input{
		{ts: 1, key: []byte("k"), value: []byte("v"), headers: []Header{{Key: "h", Value: []byte("1")}}},
		{ts: 2, value: bytes.Repeat([]byte("z"), 300)},
	})
	for n := range len(orig) {
		b := bytes.Clone(orig[:n])
		_, err := Validate(b, testMaxBytes)
		require.ErrorIs(t, err, ErrTruncated, "len %d", n)
		_, err = Batch(b).Records()
		require.ErrorIs(t, err, ErrTruncated, "len %d", n)
		_, err = ParseHeader(b)
		if n < HeaderSize {
			require.ErrorIs(t, err, ErrTruncated, "len %d", n)
		} else {
			require.NoError(t, err, "len %d", n)
		}
	}
}

func TestTooLarge(t *testing.T) {
	b := simpleBatch(t, 10)
	_, err := Validate(b, len(b))
	require.NoError(t, err)
	_, err = Validate(b, len(b)-1)
	require.ErrorIs(t, err, ErrTooLarge)

	// Size is checked before anything else, even on garbage.
	_, err = Validate(make([]byte, 100), 99)
	require.ErrorIs(t, err, ErrTooLarge)
}

func TestValidateRejectsNonCanonical(t *testing.T) {
	zero := []byte{0}
	null := vi(-1)
	// okRec is a canonical record body for index i with null key/value and no headers.
	okRec := func(i int64) []byte { return rawRecord(zero, vi(0), vi(i), null, null, vi(0)) }
	batches := map[int]Batch{1: simpleBatch(t, 1), 2: simpleBatch(t, 2), 3: simpleBatch(t, 3)}
	// Large values so a recordCount of 3 passes ParseHeader's size bound and fails in decoding.
	wide := buildBatch(t, noProducer(), []input{{ts: 1, value: make([]byte, 64)}, {ts: 2, value: make([]byte, 64)}})

	tests := []struct {
		name string
		buf  func() []byte
	}{
		{"recordCount above actual", func() []byte {
			b := bytes.Clone(wide)
			binary.BigEndian.PutUint32(b[RecordCountOffset:], 3)
			binary.BigEndian.PutUint32(b[LastOffsetDeltaOffset:], 2)
			fixCRC(b)
			return b
		}},
		{"recordCount below actual", func() []byte {
			b := bytes.Clone(batches[3])
			binary.BigEndian.PutUint32(b[RecordCountOffset:], 2)
			binary.BigEndian.PutUint32(b[LastOffsetDeltaOffset:], 1)
			fixCRC(b)
			return b
		}},
		{"lastOffsetDelta mismatch", func() []byte {
			b := bytes.Clone(batches[3])
			binary.BigEndian.PutUint32(b[LastOffsetDeltaOffset:], 5)
			fixCRC(b)
			return b
		}},
		{"zero recordCount", func() []byte {
			b := bytes.Clone(batches[1])
			binary.BigEndian.PutUint32(b[RecordCountOffset:], 0)
			binary.BigEndian.PutUint32(b[LastOffsetDeltaOffset:], math.MaxUint32)
			fixCRC(b)
			return b
		}},
		{"bad magic", func() []byte {
			b := bytes.Clone(batches[1])
			b[MagicOffset] = 2
			return b
		}},
		{"batchLength below header", func() []byte {
			b := rawBatch(nil, 1, 0, 0)
			binary.BigEndian.PutUint32(b[BatchLengthOffset:], 10)
			return b
		}},
		{"nonzero batch attributes", func() []byte {
			b := bytes.Clone(batches[1])
			binary.BigEndian.PutUint16(b[AttributesOffset:], 1)
			fixCRC(b)
			return b
		}},
		{"wrong maxTimestamp", func() []byte {
			b := bytes.Clone(batches[3])
			binary.BigEndian.PutUint64(b[MaxTimestampOffset:], uint64(Batch(b).MaxTimestamp()+1))
			fixCRC(b)
			return b
		}},
		{"trailing bytes after batch", func() []byte {
			return append(bytes.Clone(batches[2]), 0)
		}},
		{"trailing bytes after last record", func() []byte {
			return rawBatch(append(okRec(0), 0), 1, 0, 0)
		}},
		{"nonzero record attributes", func() []byte {
			return rawBatch(rawRecord([]byte{1}, vi(0), vi(0), null, null, vi(0)), 1, 0, 0)
		}},
		{"first timestampDelta nonzero", func() []byte {
			return rawBatch(rawRecord(zero, vi(3), vi(0), null, null, vi(0)), 1, 0, 3)
		}},
		{"offsetDelta out of order", func() []byte {
			return rawBatch(append(okRec(0), okRec(0)...), 2, 0, 0)
		}},
		{"offsetDelta skips", func() []byte {
			return rawBatch(append(okRec(0), okRec(2)...), 2, 0, 0)
		}},
		{"offsetDelta overflows int32", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(1<<40), null, null, vi(0)), 1, 0, 0)
		}},
		{"non-minimal varint", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), []byte{0x80, 0x00}, null, null, vi(0)), 1, 0, 0)
		}},
		{"non-minimal record length", func() []byte {
			body := []byte{0, 0, 0, 1, 1, 0}
			return rawBatch(append([]byte{byte(len(body)<<1) | 0x80, 0x00}, body...), 1, 0, 0)
		}},
		{"varint overflows int64", func() []byte {
			over := bytes.Repeat([]byte{0xff}, 10)
			over = append(over, 0x01)
			return rawBatch(rawRecord(zero, over, vi(0), null, null, vi(0)), 1, 0, 0)
		}},
		{"record length longer than body", func() []byte {
			rec := rawRecord(zero, vi(0), vi(0), null, null, vi(0), zero)
			return rawBatch(rec, 1, 0, 0)
		}},
		{"record length shorter than body", func() []byte {
			body := append(append([]byte{0}, vi(0)...), vi(0)...)
			body = append(body, null...)
			rec := append(vi(int64(len(body))), body...)
			rec = append(rec, null...)
			rec = append(rec, vi(0)...)
			return rawBatch(rec, 1, 0, 0)
		}},
		{"record length past end of batch", func() []byte {
			rec := append(vi(100), okRec(0)[1:]...)
			return rawBatch(rec, 1, 0, 0)
		}},
		{"negative record length", func() []byte {
			rec := append(vi(-2), okRec(0)[1:]...)
			return rawBatch(rec, 1, 0, 0)
		}},
		{"empty record body", func() []byte {
			return rawBatch(append(vi(0), make([]byte, 6)...), 1, 0, 0)
		}},
		{"key length below -1", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), vi(-2), null, vi(0)), 1, 0, 0)
		}},
		{"key length past record", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), vi(50), null, vi(0)), 1, 0, 0)
		}},
		{"null header key", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), null, null, vi(1), null, null), 1, 0, 0)
		}},
		{"negative headerCount", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), null, null, vi(-1)), 1, 0, 0)
		}},
		{"huge headerCount", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), null, null, vi(math.MaxInt32), vi(0), null), 1, 0, 0)
		}},
		{"headerCount above actual", func() []byte {
			return rawBatch(rawRecord(zero, vi(0), vi(0), null, null, vi(2), vi(0), null), 1, 0, 0)
		}},
		{"huge recordCount", func() []byte {
			b := rawBatch(okRec(0), math.MaxInt32, 0, 0)
			return b
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := tc.buf()
			_, err := Validate(buf, testMaxBytes)
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestRawHelpersProduceValidBatch(t *testing.T) {
	// Guards the negative tests above: the same helpers build a batch Validate accepts and that
	// equals the Builder's output.
	rec0 := rawRecord([]byte{0}, vi(0), vi(0), vi(-1), vi(-1), vi(0))
	rec1 := rawRecord([]byte{0}, vi(0), vi(1), vi(-1), vi(-1), vi(1), vi(1), []byte("h"), vi(-1))
	raw := rawBatch(append(rec0, rec1...), 2, 7, 7)
	_, err := Validate(raw, testMaxBytes)
	require.NoError(t, err)
	built := buildBatch(t, noProducer(), []input{{ts: 7}, {ts: 7, headers: []Header{{Key: "h"}}}})
	require.Equal(t, []byte(built), raw)
}

func TestHugeRecordCountDoesNotAllocate(t *testing.T) {
	zero := []byte{0}
	rec := rawRecord(zero, vi(0), vi(0), vi(-1), vi(-1), vi(0))
	b := rawBatch(rec, math.MaxInt32, 0, 0)
	allocs := testing.AllocsPerRun(10, func() {
		_, err := Validate(b, testMaxBytes)
		require.ErrorIs(t, err, ErrCorrupt)
		_, err = Batch(b).Records()
		require.ErrorIs(t, err, ErrCorrupt)
	})
	require.Less(t, allocs, 20.0)

	// Three minimal records fill exactly 3*minRecordSize bytes; a count of 4 cannot fit.
	var records []byte
	for i := range int64(3) {
		records = append(records, rawRecord(zero, vi(0), vi(i), vi(-1), vi(-1), vi(0))...)
	}
	_, err := Validate(rawBatch(records, 3, 0, 0), testMaxBytes)
	require.NoError(t, err)
	_, err = ParseHeader(rawBatch(records, 4, 0, 0))
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestVarint(t *testing.T) {
	values := []int64{
		0, 1, -1, 63, -64, 64, -65, 8191, -8192, 8192,
		math.MaxInt32, math.MinInt32, math.MaxInt32 + 1, math.MinInt32 - 1,
		math.MaxInt64, math.MinInt64,
	}
	for _, v := range values {
		enc := binary.AppendVarint(nil, v)
		require.Len(t, enc, varintSize(v), "value %d", v)
		got, n, err := readVarint(enc, "v")
		require.NoError(t, err)
		require.Equal(t, v, got)
		require.Equal(t, len(enc), n)

		got32, _, err := readVarint32(enc, "v")
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			require.NoError(t, err, "value %d", v)
			require.Equal(t, int32(v), got32)
		} else {
			require.ErrorIs(t, err, ErrCorrupt, "value %d", v)
		}

		// Every truncation of the encoding is rejected.
		for i := range len(enc) {
			_, _, err := readVarint(enc[:i], "v")
			require.ErrorIs(t, err, ErrCorrupt)
		}
	}

	nonMinimal := [][]byte{{0x80, 0x00}, {0x81, 0x80, 0x00}, {0xff, 0x00}}
	for _, enc := range nonMinimal {
		_, _, err := readVarint(enc, "v")
		require.ErrorIs(t, err, ErrCorrupt, "%x", enc)
	}

	// Record fields at int32/int64 boundaries survive a batch round trip.
	b := buildBatch(t, BuilderOptions{ProducerID: math.MaxInt64, ProducerEpoch: math.MaxInt16, BaseSequence: math.MaxInt32},
		[]input{{ts: math.MinInt64}, {ts: math.MaxInt64}})
	_, err := Validate(b, testMaxBytes)
	require.NoError(t, err)
}

func TestSplitBatches(t *testing.T) {
	b1, b2, b3 := simpleBatch(t, 1), simpleBatch(t, 5), simpleBatch(t, 20)
	all := bytes.Join([][]byte{b1, b2, b3}, nil)

	t.Run("empty", func(t *testing.T) {
		got, err := SplitBatches(nil)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("three batches", func(t *testing.T) {
		got, err := SplitBatches(all)
		require.NoError(t, err)
		require.Equal(t, []Batch{b1, b2, b3}, got)
		for _, b := range got {
			require.Equal(t, len(b), cap(b))
		}
	})

	t.Run("cut-off tail", func(t *testing.T) {
		start := len(b1) + len(b2)
		for cut := start + 1; cut < len(all); cut++ {
			got, err := SplitBatches(all[:cut])
			require.ErrorIs(t, err, ErrTruncated, "cut %d", cut)
			require.Equal(t, []Batch{b1, b2}, got, "cut %d", cut)
		}
	})

	t.Run("corrupt header", func(t *testing.T) {
		buf := bytes.Clone(all)
		buf[len(b1)+MagicOffset] = 0
		got, err := SplitBatches(buf)
		require.ErrorIs(t, err, ErrCorrupt)
		require.Equal(t, []Batch{b1}, got)
	})

	t.Run("no crc check", func(t *testing.T) {
		buf := bytes.Clone(all)
		buf[len(b1)+len(b2)-1] ^= 0xff
		got, err := SplitBatches(buf)
		require.NoError(t, err)
		require.Len(t, got, 3)
	})
}
