package record

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func FuzzValidate(f *testing.F) {
	seeds := []Batch{
		simpleBatch(f, 1),
		simpleBatch(f, 17),
		buildBatch(f, BuilderOptions{ProducerID: 7, ProducerEpoch: 2, BaseSequence: 40}, []input{
			{ts: 100, key: []byte("k"), value: []byte("v"), headers: []Header{
				{Key: "a", Value: []byte("1")}, {Key: "", Value: nil}, {Key: "e", Value: []byte{}},
			}},
			{ts: 50, key: nil, value: []byte{}},
			{ts: 200, key: []byte{}, value: nil},
		}),
		buildBatch(f, noProducer(), []input{{ts: math.MinInt64}, {ts: math.MaxInt64}}),
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	// Hostile record count: a single-record batch claiming MaxInt32 records.
	f.Add(rawBatch(rawRecord([]byte{0}, vi(0), vi(0), vi(-1), vi(-1), vi(0)), math.MaxInt32, 0, 0))
	// Hostile header count.
	f.Add(rawBatch(rawRecord([]byte{0}, vi(0), vi(0), vi(-1), vi(-1), vi(math.MaxInt32), vi(0), vi(-1)), 1, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		checkValidateProperty(t, data)
		// Mutations almost never keep the CRC valid, so also check a copy with the CRC fixed up,
		// which lets the fuzzer reach the record decoder.
		if len(data) >= HeaderSize {
			fixed := bytes.Clone(data)
			fixCRC(fixed)
			checkValidateProperty(t, fixed)
		}
	})
}

// checkValidateProperty checks that decoding data never panics, fails only with this package's
// sentinel errors, and that a batch Validate accepts rebuilds to identical CRC-covered bytes.
func checkValidateProperty(t *testing.T, data []byte) {
	t.Helper()
	recsOnly, recErr := Batch(data).Records()
	_, _ = SplitBatches(data)
	_, _ = ParseHeader(data)

	b, err := Validate(data, testMaxBytes)
	if err != nil {
		if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrTooLarge) {
			t.Fatalf("unexpected error kind: %v", err)
		}
		return
	}
	require.NoError(t, recErr)
	recs, err := b.Records()
	require.NoError(t, err)
	require.Equal(t, recsOnly, recs)

	bld := NewBuilder(BuilderOptions{
		ProducerID:    b.ProducerID(),
		ProducerEpoch: b.ProducerEpoch(),
		BaseSequence:  b.BaseSequence(),
	})
	for i, r := range recs {
		require.Equal(t, b.BaseOffset()+int64(i), r.Offset)
		bld.Append(r.Timestamp, r.Key, r.Value, r.Headers)
	}
	rebuilt, err := bld.Build()
	require.NoError(t, err)
	if !bytes.Equal(rebuilt[AttributesOffset:], data[AttributesOffset:]) {
		t.Fatalf("rebuild mismatch:\n got  %x\n want %x", rebuilt[AttributesOffset:], data[AttributesOffset:])
	}
	require.Equal(t, data[BatchLengthOffset:LeaderEpochOffset], []byte(rebuilt[BatchLengthOffset:LeaderEpochOffset]))
}
