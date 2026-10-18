package record

import "errors"

var errNotImplemented = errors.New("record: not implemented")

// ParseHeader decodes the fixed header at the start of buf. It needs at least HeaderSize bytes
// (ErrTruncated otherwise) and checks the magic byte, that batchLength covers at least the rest
// of the header, that recordCount >= 1 and that lastOffsetDelta == recordCount-1 (ErrCorrupt).
// It does not check the CRC or that buf holds the whole batch.
func ParseHeader(buf []byte) (BatchHeader, error) {
	_ = buf
	return BatchHeader{}, errNotImplemented
}

// Validate fully checks the single batch in b: header (as ParseHeader), exact length, CRC-32C,
// that every record decodes with offset deltas 0..n-1 in order, and that no bytes trail the last
// record. A batch longer than maxBytes fails with ErrTooLarge before any other check. On success
// it returns b as a Batch.
func Validate(b []byte, maxBytes int) (Batch, error) {
	_, _ = b, maxBytes
	return nil, errNotImplemented
}

// SplitBatches splits a buffer of concatenated batches using header framing only (no CRC check).
// A cut-off final batch yields ErrTruncated together with the complete batches before it.
func SplitBatches(buf []byte) ([]Batch, error) {
	_ = buf
	return nil, errNotImplemented
}

// Records decodes every record in the batch with absolute offsets and timestamps. It validates
// record framing but not the CRC.
func (b Batch) Records() ([]Record, error) {
	return nil, errNotImplemented
}
