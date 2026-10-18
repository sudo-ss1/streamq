package storage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
)

// Clock abstracts wall-clock time so tests can control rolling, flushing and retention.
type Clock interface {
	Now() time.Time
}

// SystemClock is the real wall clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// FlushPolicy controls when appended data is fsynced (SPEC §3.2).
type FlushPolicy string

// Flush policies.
const (
	FlushAlways   FlushPolicy = "always"
	FlushInterval FlushPolicy = "interval"
	FlushNever    FlushPolicy = "never"
)

// Size limits.
const (
	// MaxSegmentBytes is the largest allowed segment: index positions are uint32.
	MaxSegmentBytes      = 1 << 30
	offsetIndexEntrySize = 8
	timeIndexEntrySize   = 12

	// fileMode and dirMode are used for every file and directory the package creates.
	fileMode = 0o640
	dirMode  = 0o750
)

// LogConfig is the per-partition storage configuration (topic configs in SPEC §10).
type LogConfig struct {
	SegmentBytes       int64       // roll when the active segment would exceed this
	SegmentMs          int64       // roll when the active segment is older than this
	RetentionMs        int64       // -1 = keep forever
	RetentionBytes     int64       // -1 = unlimited
	MaxMessageBytes    int         // largest accepted batch
	IndexIntervalBytes int         // bytes between sparse index entries
	MaxIndexBytes      int         // preallocated size of each index file
	FlushPolicy        FlushPolicy // always | interval | never
	FlushMs            int64       // fsync interval for FlushInterval
}

// DefaultLogConfig returns the SPEC §10 defaults.
func DefaultLogConfig() LogConfig {
	return LogConfig{
		SegmentBytes:       64 << 20,
		SegmentMs:          7 * 24 * time.Hour.Milliseconds(),
		RetentionMs:        7 * 24 * time.Hour.Milliseconds(),
		RetentionBytes:     -1,
		MaxMessageBytes:    1 << 20,
		IndexIntervalBytes: 4096,
		MaxIndexBytes:      10 << 20,
		FlushPolicy:        FlushInterval,
		FlushMs:            1000,
	}
}

// Validate reports the first invalid field.
func (c LogConfig) Validate() error {
	var errs []error
	if c.SegmentBytes < record.HeaderSize || c.SegmentBytes > MaxSegmentBytes {
		errs = append(errs, fmt.Errorf("segment_bytes %d must be in [%d, %d]", c.SegmentBytes, record.HeaderSize, MaxSegmentBytes))
	}
	if c.SegmentMs <= 0 {
		errs = append(errs, fmt.Errorf("segment_ms %d must be > 0", c.SegmentMs))
	}
	if c.RetentionMs < -1 || c.RetentionMs == 0 {
		errs = append(errs, fmt.Errorf("retention_ms %d must be > 0 or -1", c.RetentionMs))
	}
	if c.RetentionBytes < -1 || c.RetentionBytes == 0 {
		errs = append(errs, fmt.Errorf("retention_bytes %d must be > 0 or -1", c.RetentionBytes))
	}
	if c.MaxMessageBytes < record.HeaderSize || int64(c.MaxMessageBytes) > c.SegmentBytes {
		errs = append(errs, fmt.Errorf("max_message_bytes %d must be in [%d, segment_bytes]", c.MaxMessageBytes, record.HeaderSize))
	}
	if c.IndexIntervalBytes <= 0 {
		errs = append(errs, fmt.Errorf("index_interval_bytes %d must be > 0", c.IndexIntervalBytes))
	}
	if c.MaxIndexBytes < 2*timeIndexEntrySize {
		errs = append(errs, fmt.Errorf("max_index_bytes %d must be >= %d", c.MaxIndexBytes, 2*timeIndexEntrySize))
	}
	switch c.FlushPolicy {
	case FlushAlways, FlushNever:
	case FlushInterval:
		if c.FlushMs <= 0 {
			errs = append(errs, fmt.Errorf("flush_ms %d must be > 0", c.FlushMs))
		}
	default:
		errs = append(errs, fmt.Errorf("flush_policy %q must be always, interval or never", c.FlushPolicy))
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid log config: %w", errors.Join(errs...))
	}
	return nil
}

// TopicPartition identifies one partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// String returns "topic-partition".
func (tp TopicPartition) String() string { return tp.DirName() }

// DirName returns the partition's directory name, "<topic>-<partition>".
func (tp TopicPartition) DirName() string {
	return tp.Topic + "-" + strconv.FormatInt(int64(tp.Partition), 10)
}

// ParseDirName parses a "<topic>-<partition>" directory name, splitting on the last '-'.
func ParseDirName(name string) (TopicPartition, bool) {
	i := strings.LastIndexByte(name, '-')
	if i <= 0 || i == len(name)-1 {
		return TopicPartition{}, false
	}
	p, err := strconv.ParseInt(name[i+1:], 10, 32)
	if err != nil || p < 0 {
		return TopicPartition{}, false
	}
	return TopicPartition{Topic: name[:i], Partition: int32(p)}, true
}
