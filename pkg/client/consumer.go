package client

import (
	"context"
	"errors"
	"time"
)

var errConsumerUnimplemented = errors.New("client: consumer unimplemented")

// OffsetReset is the consumer's policy when it has no valid position.
type OffsetReset int

// Offset reset policies.
const (
	// ResetLatest starts at the high watermark (Kafka default).
	ResetLatest OffsetReset = iota
	// ResetEarliest starts at the log start offset.
	ResetEarliest
)

// ConsumerConfig configures a Consumer. Zero values take the documented defaults.
type ConsumerConfig struct {
	MaxWait           time.Duration // fetch long-poll wait, default 500ms
	MinBytes          int           // default 1
	MaxBytes          int           // per fetch request, default 16 MiB
	MaxPartitionBytes int           // per partition, default 1 MiB
	AutoOffsetReset   OffsetReset   // default ResetLatest
}

// Consumer reads manually assigned partitions (no group membership; see GroupConsumer in M4). It
// is not safe for concurrent use, like Kafka's consumer.
type Consumer struct {
	client *Client
	cfg    ConsumerConfig
}

// NewConsumer returns a Consumer using c.
func NewConsumer(c *Client, cfg ConsumerConfig) (*Consumer, error) {
	return &Consumer{client: c, cfg: cfg}, errConsumerUnimplemented
}

// Assign replaces the assignment. Positions of newly assigned partitions are unset and resolved
// with AutoOffsetReset on the next Poll unless Seek is called.
func (c *Consumer) Assign(tps []TopicPartition) { _ = tps }

// Assignment returns the current assignment.
func (c *Consumer) Assignment() []TopicPartition { return nil }

// Seek sets the next offset to read for an assigned partition.
func (c *Consumer) Seek(tp TopicPartition, offset int64) error {
	_, _ = tp, offset
	return errConsumerUnimplemented
}

// SeekToBeginning moves the given (or, if none, all) assigned partitions to their log start.
func (c *Consumer) SeekToBeginning(ctx context.Context, tps ...TopicPartition) error {
	_, _ = ctx, tps
	return errConsumerUnimplemented
}

// SeekToEnd moves the given (or, if none, all) assigned partitions to their high watermark.
func (c *Consumer) SeekToEnd(ctx context.Context, tps ...TopicPartition) error {
	_, _ = ctx, tps
	return errConsumerUnimplemented
}

// Position returns the next offset to read for tp, if known.
func (c *Consumer) Position(tp TopicPartition) (int64, bool) {
	_ = tp
	return 0, false
}

// Poll fetches the next records from the assigned partitions, waiting up to MaxWait for data.
// Returned records are in offset order per partition and their positions are advanced.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	_ = ctx
	return nil, errConsumerUnimplemented
}

// Close releases the consumer. Idempotent. It does not close the Client.
func (c *Consumer) Close() error { return nil }
