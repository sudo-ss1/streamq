package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

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

// Admin manages topics. It is safe for concurrent use.
//
// Every call is bounded by the Client's RequestTimeout (or ctx, if earlier). Transport errors
// are retried with backoff, on any reachable broker, within that bound.
type Admin struct {
	client *Client
}

// NewAdmin returns an Admin using c.
func NewAdmin(c *Client) *Admin { return &Admin{client: c} }

// call runs fn on any broker (see Client.callAny), retrying transport errors with backoff until
// ctx, bounded by RequestTimeout, ends. sawTransport reports whether some attempt failed with a
// transport error, i.e. a mutation may have been applied without the client seeing the answer.
func (a *Admin) call(ctx context.Context, fn func(context.Context, streamqv1.StreamQServiceClient) error) (sawTransport bool, err error) {
	ctx, cancel := a.client.withTimeout(ctx)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err = a.client.callAny(ctx, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
			ferr := fn(ctx, api)
			if isTransportError(ferr) {
				sawTransport = true
			}
			return ferr
		})
		if err == nil || !isTransportError(err) || ctx.Err() != nil {
			return sawTransport, err
		}
		if !sleep(ctx, a.client.backoff.delay(attempt), nil) {
			return sawTransport, err
		}
	}
}

// CreateTopic creates a topic. partitions or rf <= 0 use the broker defaults.
//
// If an attempt failed with a transport error, the topic may have been created even though the
// client never saw the answer. A retry that then gets TOPIC_ALREADY_EXISTS is reported as success
// when the existing topic has the requested partition count (any count if partitions <= 0);
// otherwise ErrTopicAlreadyExists is returned.
func (a *Admin) CreateTopic(ctx context.Context, name string, partitions, rf int32, configs map[string]string) error {
	req := &streamqv1.CreateTopicRequest{Name: name, Partitions: partitions, ReplicationFactor: rf, Configs: configs}
	sawTransport, err := a.call(ctx, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
		resp, err := api.CreateTopic(ctx, req)
		if err != nil {
			return err
		}
		return serverError(resp.GetErrorCode(), resp.GetErrorMessage())
	})
	if err == nil {
		return nil
	}
	if !sawTransport || !errors.Is(err, ErrTopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", name, err)
	}
	descs, derr := a.DescribeTopics(ctx, name)
	if derr != nil {
		return fmt.Errorf("create topic %q: %w (describe after retry: %w)", name, err, derr)
	}
	if partitions <= 0 || int32(len(descs[0].Partitions)) == partitions {
		return nil
	}
	return fmt.Errorf("create topic %q: %w with %d partitions, requested %d", name, err, len(descs[0].Partitions), partitions)
}

// DeleteTopic deletes a topic.
//
// As with CreateTopic, if an attempt failed with a transport error and a retry then gets
// UNKNOWN_TOPIC_OR_PARTITION, the earlier attempt is taken to have deleted it and nil is returned.
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	sawTransport, err := a.call(ctx, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
		resp, err := api.DeleteTopic(ctx, &streamqv1.DeleteTopicRequest{Name: name})
		if err != nil {
			return err
		}
		return serverError(resp.GetErrorCode(), resp.GetErrorMessage())
	})
	if err == nil || (sawTransport && errors.Is(err, ErrUnknownTopicOrPartition)) {
		a.client.invalidate(name)
		return nil
	}
	return fmt.Errorf("delete topic %q: %w", name, err)
}

// ListTopics returns topic names, sorted.
func (a *Admin) ListTopics(ctx context.Context) ([]string, error) {
	var names []string
	_, err := a.call(ctx, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
		resp, err := api.ListTopics(ctx, &streamqv1.ListTopicsRequest{})
		if err != nil {
			return err
		}
		names = slices.Clone(resp.GetNames())
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	slices.Sort(names)
	return names, nil
}

// DescribeTopics describes the named topics (all if none). Unknown names return
// ErrUnknownTopicOrPartition.
//
// Named topics are returned in the requested order (duplicates removed); with no names, all
// topics sorted by name. Partitions are sorted by id.
func (a *Admin) DescribeTopics(ctx context.Context, names ...string) ([]TopicDescription, error) {
	var resp *streamqv1.MetadataResponse
	_, err := a.call(ctx, func(ctx context.Context, api streamqv1.StreamQServiceClient) error {
		var err error
		resp, err = api.Metadata(ctx, &streamqv1.MetadataRequest{Topics: names})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("describe topics: %w", err)
	}
	byName := map[string]TopicDescription{}
	var all []string
	for _, t := range resp.GetTopics() {
		if serr := serverError(t.GetErrorCode(), ""); serr != nil {
			return nil, fmt.Errorf("describe topic %q: %w", t.GetName(), serr)
		}
		d := TopicDescription{Name: t.GetName(), Configs: t.GetConfigs()}
		for _, p := range t.GetPartitions() {
			d.Partitions = append(d.Partitions, PartitionDescription{
				Partition: p.GetPartition(), Leader: p.GetLeaderId(), LeaderEpoch: p.GetLeaderEpoch(),
				Replicas: p.GetReplicas(), ISR: p.GetIsr(),
			})
		}
		sort.Slice(d.Partitions, func(i, j int) bool { return d.Partitions[i].Partition < d.Partitions[j].Partition })
		if _, dup := byName[d.Name]; !dup {
			all = append(all, d.Name)
		}
		byName[d.Name] = d
	}
	if len(names) == 0 {
		slices.Sort(all)
		names = all
	}
	out := make([]TopicDescription, 0, len(names))
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			continue
		}
		seen[n] = true
		d, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("describe topic %q: %w", n, ErrUnknownTopicOrPartition)
		}
		out = append(out, d)
	}
	return out, nil
}

// ListOffsets resolves an offset for tp: timestamp -2 = earliest, -1 = latest (high watermark),
// >= 0 = first offset with a timestamp >= it (-1 if none).
//
// The request goes to tp's leader. Stale leadership (NOT_LEADER, FENCED_LEADER_EPOCH, a server
// UNKNOWN_TOPIC_OR_PARTITION) and transport errors refresh metadata and retry with backoff,
// bounded by RequestTimeout or ctx; a topic or partition that metadata says does not exist fails
// at once with ErrUnknownTopicOrPartition.
func (a *Admin) ListOffsets(ctx context.Context, tp TopicPartition, timestamp int64) (int64, error) {
	ctx, cancel := a.client.withTimeout(ctx)
	defer cancel()
	for attempt := 0; ; attempt++ {
		api, epoch, err := a.client.leader(ctx, tp)
		if err != nil {
			if errors.Is(err, ErrUnknownTopicOrPartition) || !IsRetriable(err) {
				return -1, fmt.Errorf("list offsets %s/%d: %w", tp.Topic, tp.Partition, err)
			}
		} else {
			var off int64
			off, err = a.listOffsetsOnce(ctx, api, tp, epoch, timestamp)
			if err == nil {
				return off, nil
			}
			if !IsRetriable(err) {
				return -1, fmt.Errorf("list offsets %s/%d: %w", tp.Topic, tp.Partition, err)
			}
		}
		if NeedsMetadataRefresh(err) {
			a.client.invalidate(tp.Topic)
		}
		if ctx.Err() != nil || !sleep(ctx, a.client.backoff.delay(attempt), nil) {
			return -1, fmt.Errorf("list offsets %s/%d: %w", tp.Topic, tp.Partition, err)
		}
	}
}

// listOffsetsOnce sends one ListOffsets for tp to api.
func (a *Admin) listOffsetsOnce(ctx context.Context, api streamqv1.StreamQServiceClient, tp TopicPartition, epoch int32, timestamp int64) (int64, error) {
	resp, err := api.ListOffsets(ctx, &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{{
		Topic:      tp.Topic,
		Partitions: []*streamqv1.ListOffsetsPartition{{Partition: tp.Partition, LeaderEpoch: epoch, Timestamp: timestamp}},
	}}})
	if err != nil {
		return -1, err
	}
	for _, t := range resp.GetTopics() {
		if t.GetTopic() != tp.Topic {
			continue
		}
		for _, p := range t.GetPartitions() {
			if p.GetPartition() != tp.Partition {
				continue
			}
			if serr := serverError(p.GetErrorCode(), ""); serr != nil {
				return -1, serr
			}
			return p.GetOffset(), nil
		}
	}
	return -1, fmt.Errorf("%s/%d missing from response: %w", tp.Topic, tp.Partition, ErrUnknownServerError)
}
