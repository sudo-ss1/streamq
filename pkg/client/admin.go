package client

import (
	"context"
	"errors"
)

var errAdminUnimplemented = errors.New("client: admin unimplemented")

// TopicDescription describes a topic.
type TopicDescription struct {
	Name       string
	Configs    map[string]string
	Partitions []PartitionDescription
}

// PartitionDescription describes one partition.
type PartitionDescription struct {
	Partition   int32
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
}

// Admin manages topics.
type Admin struct {
	client *Client
}

// NewAdmin returns an Admin using c.
func NewAdmin(c *Client) *Admin { return &Admin{client: c} }

// CreateTopic creates a topic. partitions or rf <= 0 use the broker defaults.
func (a *Admin) CreateTopic(ctx context.Context, name string, partitions, rf int32, configs map[string]string) error {
	_, _, _, _, _ = ctx, name, partitions, rf, configs
	return errAdminUnimplemented
}

// DeleteTopic deletes a topic.
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	_, _ = ctx, name
	return errAdminUnimplemented
}

// ListTopics returns topic names, sorted.
func (a *Admin) ListTopics(ctx context.Context) ([]string, error) {
	_ = ctx
	return nil, errAdminUnimplemented
}

// DescribeTopics describes the named topics (all if none). Unknown names return
// ErrUnknownTopicOrPartition.
func (a *Admin) DescribeTopics(ctx context.Context, names ...string) ([]TopicDescription, error) {
	_, _ = ctx, names
	return nil, errAdminUnimplemented
}

// ListOffsets resolves an offset for tp: timestamp -2 = earliest, -1 = latest (high watermark),
// >= 0 = first offset with a timestamp >= it (-1 if none).
func (a *Admin) ListOffsets(ctx context.Context, tp TopicPartition, timestamp int64) (int64, error) {
	_, _, _ = ctx, tp, timestamp
	return 0, errAdminUnimplemented
}
