package client

import (
	"time"

	"github.com/sudo-ss1/streamq/internal/record"
)

// TopicPartition identifies one partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Header is a record header.
type Header struct {
	Key   string
	Value []byte
}

// Record is a record to produce or a consumed record.
type Record struct {
	Topic string
	// Partition is set on consumed records. When producing it is used only if UsePartition is
	// true; otherwise the producer's partitioner chooses (murmur2 of the key, or sticky for
	// records without a key).
	Partition    int32
	UsePartition bool
	Key          []byte // nil = no key
	Value        []byte
	Headers      []Header
	// Timestamp is the record's CreateTime; the zero value means "now" when producing.
	Timestamp time.Time
	// Offset is set on consumed records.
	Offset int64
}

// RecordMetadata describes where a produced record was written.
type RecordMetadata struct {
	Topic     string
	Partition int32
	// Offset is the record's offset, or -1 with acks=none.
	Offset    int64
	Timestamp time.Time
}

// toRecordHeaders converts client headers to the record package's form.
func toRecordHeaders(hs []Header) []record.Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]record.Header, len(hs))
	for i, h := range hs {
		out[i] = record.Header{Key: h.Key, Value: h.Value}
	}
	return out
}

// fromRecordHeaders converts decoded record headers to client headers.
func fromRecordHeaders(hs []record.Header) []Header {
	if len(hs) == 0 {
		return nil
	}
	out := make([]Header, len(hs))
	for i, h := range hs {
		out[i] = Header{Key: h.Key, Value: h.Value}
	}
	return out
}
