package main

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/spf13/cobra"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/record"
)

const (
	// ListOffsets timestamps (SPEC §7.1).
	earliestTimestamp int64 = -2
	latestTimestamp   int64 = -1

	fetchMaxWaitMs        = 500
	fetchMaxBytes         = 16 << 20
	fetchPartitionMaxByte = 1 << 20
)

type consumeFlags struct {
	topic         string
	partition     int32
	offset        int64
	fromBeginning bool
	maxMessages   int
	follow        bool
	printKey      bool
	printOffset   bool
}

func (a *app) consumeCmd() *cobra.Command {
	var f consumeFlags
	cmd := &cobra.Command{
		Use:   "consume --topic T",
		Short: "Print records from a topic, one value per line",
		Long: "Prints records up to the high watermark observed at start (or forever with --follow). " +
			"Starts at the latest offset unless --from-beginning or --offset is given.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.consume(cmd.Context(), f, cmd.Flags().Changed("offset"))
		},
	}
	cmd.Flags().StringVar(&f.topic, "topic", "", "topic to consume (required)")
	cmd.Flags().Int32Var(&f.partition, "partition", -1, "partition to consume (-1 = all)")
	cmd.Flags().Int64Var(&f.offset, "offset", 0, "start offset (applies to every consumed partition)")
	cmd.Flags().BoolVar(&f.fromBeginning, "from-beginning", false, "start at the earliest offset")
	cmd.Flags().IntVar(&f.maxMessages, "max-messages", 0, "stop after this many records (0 = no limit)")
	cmd.Flags().BoolVar(&f.follow, "follow", false, "keep waiting for new records until interrupted")
	cmd.Flags().BoolVar(&f.printKey, "print-key", false, "print the key before the value")
	cmd.Flags().BoolVar(&f.printOffset, "print-offset", false, "print the offset first")
	cmd.MarkFlagsMutuallyExclusive("offset", "from-beginning")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

// partitionCursor tracks one consumed partition.
type partitionCursor struct {
	partition int32
	position  int64 // next offset to print
	end       int64 // HW observed at start; only used without --follow
}

func (a *app) consume(ctx context.Context, f consumeFlags, offsetSet bool) error {
	if f.maxMessages < 0 {
		return fmt.Errorf("invalid --max-messages %d: must be >= 0", f.maxMessages)
	}
	if offsetSet && f.offset < 0 {
		return fmt.Errorf("invalid --offset %d: must be >= 0", f.offset)
	}
	c, closeConn, err := a.client(ctx)
	if err != nil {
		return err
	}
	defer closeConn()
	rt, md, err := a.newRouter(ctx, c, f.topic)
	if err != nil {
		return err
	}
	defer rt.close()
	var parts []int32
	for _, p := range md.GetPartitions() {
		if f.partition < 0 || p.GetPartition() == f.partition {
			parts = append(parts, p.GetPartition())
		}
	}
	if len(parts) == 0 {
		return fmt.Errorf("topic %q has no partition %d", f.topic, f.partition)
	}
	slices.Sort(parts)

	ends, err := a.listOffsets(ctx, rt, f.topic, parts, latestTimestamp)
	if err != nil {
		return err
	}
	var starts map[int32]int64
	switch {
	case offsetSet:
		starts = make(map[int32]int64, len(parts))
		for _, p := range parts {
			starts[p] = f.offset
		}
	case f.fromBeginning:
		if starts, err = a.listOffsets(ctx, rt, f.topic, parts, earliestTimestamp); err != nil {
			return err
		}
	default:
		starts = ends
	}

	cursors := make([]*partitionCursor, 0, len(parts))
	for _, p := range parts {
		cursors = append(cursors, &partitionCursor{partition: p, position: starts[p], end: ends[p]})
	}

	printed := 0
	for {
		active := cursors
		if !f.follow {
			active = slices.DeleteFunc(slices.Clone(cursors), func(pc *partitionCursor) bool { return pc.position >= pc.end })
			if len(active) == 0 {
				return nil
			}
		}
		done, err := a.fetchOnce(ctx, rt, f, active, &printed)
		if err != nil {
			if f.follow && ctx.Err() != nil {
				return nil // interrupted: a normal way to end --follow
			}
			return err
		}
		if done {
			return nil
		}
	}
}

// fetchOnce issues one Fetch per leader for the active partitions and prints the records they
// return. It reports done once --max-messages records have been printed.
func (a *app) fetchOnce(ctx context.Context, rt *router, f consumeFlags, active []*partitionCursor, printed *int) (bool, error) {
	byPart := make(map[int32]*partitionCursor, len(active))
	parts := make([]int32, 0, len(active))
	for _, pc := range active {
		byPart[pc.partition] = pc
		parts = append(parts, pc.partition)
	}
	order, groups := rt.group(ctx, parts)
	for _, c := range order {
		var group []*partitionCursor
		for _, p := range groups[c] {
			group = append(group, byPart[p])
		}
		done, err := a.fetchFrom(ctx, c, f, group, printed)
		if err != nil || done {
			return done, err
		}
	}
	return false, nil
}

// fetchFrom issues one Fetch to c for the given partitions and prints the records it returns.
func (a *app) fetchFrom(ctx context.Context, c streamqv1.StreamQServiceClient, f consumeFlags,
	active []*partitionCursor, printed *int,
) (bool, error) {
	ft := &streamqv1.FetchTopic{Topic: f.topic}
	byPart := make(map[int32]*partitionCursor, len(active))
	for _, pc := range active {
		byPart[pc.partition] = pc
		ft.Partitions = append(ft.Partitions, &streamqv1.FetchPartition{
			Partition: pc.partition, LeaderEpoch: noEpoch, FetchOffset: pc.position, MaxBytes: fetchPartitionMaxByte,
		})
	}
	rctx, cancel := a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.Fetch(rctx, &streamqv1.FetchRequest{
		MaxWaitMs: fetchMaxWaitMs, MinBytes: 1, MaxBytes: fetchMaxBytes, Topics: []*streamqv1.FetchTopic{ft},
	})
	if err != nil {
		return false, fmt.Errorf("fetch from topic %q: %w", f.topic, err)
	}
	for _, t := range resp.GetTopics() {
		if t.GetTopic() != f.topic {
			continue
		}
		for _, pr := range t.GetPartitions() {
			pc := byPart[pr.GetPartition()]
			if pc == nil {
				continue
			}
			if code := pr.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return false, apiError(fmt.Sprintf("fetch from topic %q partition %d", f.topic, pc.partition), code, "")
			}
			done, err := a.printRecords(f, pc, pr.GetRecords(), printed)
			if err != nil || done {
				return done, err
			}
		}
	}
	return false, nil
}

// printRecords decodes the fetched batches of one partition and prints every record at or above
// the cursor position (a fetch returns whole batches, so it may start before the requested
// offset) and, without --follow, below the HW observed at start.
func (a *app) printRecords(f consumeFlags, pc *partitionCursor, data []byte, printed *int) (bool, error) {
	batches, err := record.SplitBatches(data)
	if err != nil && (len(batches) == 0 || !errors.Is(err, record.ErrTruncated)) {
		return false, fmt.Errorf("decode fetch from topic %q partition %d: %w", f.topic, pc.partition, err)
	}
	for _, b := range batches {
		recs, err := b.Records()
		if err != nil {
			return false, fmt.Errorf("decode batch at offset %d of topic %q partition %d: %w",
				b.BaseOffset(), f.topic, pc.partition, err)
		}
		for _, r := range recs {
			if r.Offset < pc.position {
				continue
			}
			if !f.follow && r.Offset >= pc.end {
				// Offsets only grow, so nothing below the start HW remains: the partition is done.
				pc.position = max(pc.position, pc.end)
				return false, nil
			}
			if err := a.printRecord(f, r); err != nil {
				return false, err
			}
			pc.position = r.Offset + 1
			*printed++
			if f.maxMessages > 0 && *printed >= f.maxMessages {
				return true, nil
			}
		}
		pc.position = max(pc.position, b.NextOffset())
	}
	return false, nil
}

// printRecord writes one record: [offset<TAB>][key<TAB>]value. A null key prints as "null".
func (a *app) printRecord(f consumeFlags, r record.Record) error {
	line := make([]byte, 0, len(r.Value)+32)
	if f.printOffset {
		line = fmt.Appendf(line, "%d\t", r.Offset)
	}
	if f.printKey {
		if r.Key == nil {
			line = append(line, "null"...)
		} else {
			line = append(line, r.Key...)
		}
		line = append(line, '\t')
	}
	line = append(line, r.Value...)
	line = append(line, '\n')
	_, err := a.out.Write(line)
	return err
}

// listOffsets resolves the offset for timestamp on each partition, asking each partition's leader.
func (a *app) listOffsets(ctx context.Context, rt *router, topic string, parts []int32, timestamp int64) (map[int32]int64, error) {
	out := make(map[int32]int64, len(parts))
	order, groups := rt.group(ctx, parts)
	for _, c := range order {
		got, err := a.listOffsetsFrom(ctx, c, topic, groups[c], timestamp)
		if err != nil {
			return nil, err
		}
		for p, off := range got {
			out[p] = off
		}
	}
	return out, nil
}

// listOffsetsFrom asks c for the offsets of parts.
func (a *app) listOffsetsFrom(ctx context.Context, c streamqv1.StreamQServiceClient, topic string, parts []int32,
	timestamp int64,
) (map[int32]int64, error) {
	lt := &streamqv1.ListOffsetsTopic{Topic: topic}
	for _, p := range parts {
		lt.Partitions = append(lt.Partitions, &streamqv1.ListOffsetsPartition{Partition: p, LeaderEpoch: noEpoch, Timestamp: timestamp})
	}
	rctx, cancel := a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.ListOffsets(rctx, &streamqv1.ListOffsetsRequest{Topics: []*streamqv1.ListOffsetsTopic{lt}})
	if err != nil {
		return nil, fmt.Errorf("list offsets for topic %q: %w", topic, err)
	}
	out := make(map[int32]int64, len(parts))
	for _, t := range resp.GetTopics() {
		if t.GetTopic() != topic {
			continue
		}
		for _, pr := range t.GetPartitions() {
			if code := pr.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return nil, apiError(fmt.Sprintf("list offsets for topic %q partition %d", topic, pr.GetPartition()), code, "")
			}
			out[pr.GetPartition()] = pr.GetOffset()
		}
	}
	for _, p := range parts {
		if _, ok := out[p]; !ok {
			return nil, fmt.Errorf("list offsets for topic %q partition %d: no response for partition", topic, p)
		}
	}
	return out, nil
}
