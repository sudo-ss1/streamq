package record

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendedSizeIsExact(t *testing.T) {
	b := NewBuilder(BuilderOptions{ProducerID: NoProducerID, BaseSequence: NoSequence})
	cases := []struct {
		ts         int64
		key, value []byte
		headers    []Header
	}{
		{1000, nil, []byte("v"), nil},
		{900, []byte("key"), nil, []Header{{Key: "h", Value: nil}}},
		{5_000_000, []byte{}, make([]byte, 300), []Header{{Key: "a", Value: []byte("b")}, {Key: "", Value: []byte{}}}},
	}
	for _, c := range cases {
		want := b.AppendedSize(c.ts, c.key, c.value, c.headers)
		b.Append(c.ts, c.key, c.value, c.headers)
		require.Equal(t, want, b.Size())
	}
	batch, err := b.Build()
	require.NoError(t, err)
	require.Len(t, batch, b.Size())
}
