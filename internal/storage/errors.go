package storage

import (
	"errors"
	"fmt"

	"github.com/sudo-ss1/streamq/internal/record"
)

// Sentinel errors returned by this package.
var (
	// ErrOffsetOutOfRange reports a read below the log start offset or above the log end offset.
	ErrOffsetOutOfRange = errors.New("storage: offset out of range")
	// ErrMessageTooLarge reports a batch larger than max.message.bytes.
	ErrMessageTooLarge = fmt.Errorf("storage: message too large: %w", record.ErrTooLarge)
	// ErrCorruptBatch reports a batch that fails validation on append.
	ErrCorruptBatch = fmt.Errorf("storage: corrupt batch: %w", record.ErrCorrupt)
	// ErrInvalidTimestamp reports a batch whose maxTimestamp is negative; time-based retention and
	// rolling depend on real timestamps (DECISIONS D9).
	ErrInvalidTimestamp = errors.New("storage: invalid timestamp")
	// ErrCorruptLog reports corruption in data below the recovery point (data that was fsynced and
	// is trusted); the log is not opened so newer acknowledged data is never deleted to "repair" it.
	ErrCorruptLog = errors.New("storage: corrupt log below recovery point")
	// ErrOffsetMismatch reports a replicated batch whose base offset is not the log end offset.
	ErrOffsetMismatch = errors.New("storage: offset mismatch")
	// ErrClosed reports use of a closed log, segment or index.
	ErrClosed = errors.New("storage: closed")
	// ErrIndexFull reports an index with no room for another entry.
	ErrIndexFull = errors.New("storage: index full")
	// ErrCorruptIndex reports an index file that fails its sanity check.
	ErrCorruptIndex = errors.New("storage: corrupt index")
	// ErrLogFailed reports a log that hit an unrecoverable write or fsync error; it accepts no
	// further appends or flushes until reopened (Kafka takes the log dir offline).
	ErrLogFailed = errors.New("storage: log failed")
	// ErrCorruptCheckpoint reports a malformed checkpoint file.
	ErrCorruptCheckpoint = errors.New("storage: corrupt checkpoint")
)
