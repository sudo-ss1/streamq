package record

import (
	"encoding/binary"
	"fmt"
	"math"
)

// varintSize returns the number of bytes binary.AppendVarint uses to encode v (zig-zag).
func varintSize(v int64) int {
	ux := uint64(v) << 1
	if v < 0 {
		ux = ^ux
	}
	n := 1
	for ux >= 0x80 {
		ux >>= 7
		n++
	}
	return n
}

// readVarint decodes one zig-zag varint from the start of buf. It rejects a buffer that ends
// mid-varint, a value that overflows int64 and a non-minimal encoding, all as ErrCorrupt: the
// caller only reads varints inside a batch whose total length is already known.
func readVarint(buf []byte, field string) (int64, int, error) {
	v, n := binary.Varint(buf)
	switch {
	case n == 0:
		return 0, 0, fmt.Errorf("%w: %s: varint runs past end of record", ErrCorrupt, field)
	case n < 0:
		return 0, 0, fmt.Errorf("%w: %s: varint overflows 64 bits", ErrCorrupt, field)
	case n != varintSize(v):
		return 0, 0, fmt.Errorf("%w: %s: non-minimal varint", ErrCorrupt, field)
	}
	return v, n, nil
}

// readVarint32 is readVarint restricted to the int32 range.
func readVarint32(buf []byte, field string) (int32, int, error) {
	v, n, err := readVarint(buf, field)
	if err != nil {
		return 0, 0, err
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, 0, fmt.Errorf("%w: %s: %d out of int32 range", ErrCorrupt, field, v)
	}
	return int32(v), n, nil
}
