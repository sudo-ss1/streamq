package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/partitioner"
	"github.com/sudo-ss1/streamq/internal/record"
)

const (
	// softBatchBytes flushes a batch once it reaches this many bytes, regardless of --batch-size,
	// so batches stay below the broker's default max.message.bytes (1 MiB).
	softBatchBytes = 512 << 10
	// maxLineBytes is the longest input line accepted.
	maxLineBytes = 16 << 20
)

// noEpoch skips the leader-epoch check (DECISIONS D11).
const noEpoch int32 = -1

// parseAcks maps the --acks flag to the API enum.
func parseAcks(s string) (streamqv1.Acks, error) {
	switch s {
	case "none", "0":
		return streamqv1.Acks_ACKS_NONE, nil
	case "leader", "1":
		return streamqv1.Acks_ACKS_LEADER, nil
	case "all", "-1":
		return streamqv1.Acks_ACKS_ALL, nil
	default:
		return streamqv1.Acks_ACKS_UNSPECIFIED, fmt.Errorf("invalid --acks %q: want none, leader or all", s)
	}
}

type produceFlags struct {
	topic     string
	keySep    string
	acks      string
	batchSize int
}

func (a *app) produceCmd() *cobra.Command {
	var f produceFlags
	cmd := &cobra.Command{
		Use:   "produce --topic T",
		Short: "Produce one record per input line",
		Long: "Reads lines from stdin and produces one record per line. With --key-separator, the text " +
			"before the first separator is the record key (keyed records are partitioned with murmur2); " +
			"unkeyed records go round-robin, one partition per batch.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.produce(cmd.Context(), f)
		},
	}
	cmd.Flags().StringVar(&f.topic, "topic", "", "topic to produce to (required)")
	cmd.Flags().StringVar(&f.keySep, "key-separator", "", "separator between key and value (no keys if empty)")
	cmd.Flags().StringVar(&f.acks, "acks", "all", "acknowledgements: none, leader or all")
	cmd.Flags().IntVar(&f.batchSize, "batch-size", 100, "maximum records per batch")
	_ = cmd.MarkFlagRequired("topic")
	return cmd
}

// producer accumulates per-partition batches and sends them.
type producer struct {
	a        *app
	rt       *router
	topic    string
	acks     streamqv1.Acks
	builders map[int32]*record.Builder
	produced int
}

func (a *app) produce(ctx context.Context, f produceFlags) error {
	acks, err := parseAcks(f.acks)
	if err != nil {
		return err
	}
	if f.batchSize <= 0 {
		return fmt.Errorf("invalid --batch-size %d: must be > 0", f.batchSize)
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
	numPartitions := int32(len(md.GetPartitions()))

	p := &producer{a: a, rt: rt, topic: f.topic, acks: acks, builders: make(map[int32]*record.Builder)}
	var rr int32 // partition receiving unkeyed records; advances when its batch is sent
	sc := bufio.NewScanner(a.in)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	for sc.Scan() {
		line := sc.Text()
		var key []byte
		value := []byte(line)
		part := rr
		if f.keySep != "" {
			if k, v, ok := strings.Cut(line, f.keySep); ok {
				key, value = []byte(k), []byte(v)
				part = partitioner.ForKey(key, numPartitions)
			}
		}
		b := p.builders[part]
		if b == nil {
			b = record.NewBuilder(record.BuilderOptions{ProducerID: record.NoProducerID, BaseSequence: record.NoSequence})
			p.builders[part] = b
		}
		b.Append(a.opts.now().UnixMilli(), key, value, nil)
		if b.Len() >= f.batchSize || b.Size() >= softBatchBytes {
			if err := p.send(ctx, []int32{part}); err != nil {
				return err
			}
			if part == rr {
				rr = (rr + 1) % numPartitions
			}
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	// Flush the remainder in one request, in partition order.
	var rest []int32
	for part := range numPartitions {
		if b := p.builders[part]; b != nil && b.Len() > 0 {
			rest = append(rest, part)
		}
	}
	if len(rest) > 0 {
		if err := p.send(ctx, rest); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(a.out, "produced %d records\n", p.produced)
	return err
}

// send sends the pending batches of the listed partitions, one ProduceRequest per leader.
func (p *producer) send(ctx context.Context, parts []int32) error {
	order, groups := p.rt.group(ctx, parts)
	var errs []error
	for _, c := range order {
		errs = append(errs, p.sendTo(ctx, c, groups[c]))
	}
	return errors.Join(errs...)
}

// sendTo builds and sends the pending batch of each listed partition in one ProduceRequest to c.
func (p *producer) sendTo(ctx context.Context, c streamqv1.StreamQServiceClient, parts []int32) error {
	req := &streamqv1.ProduceRequest{
		Acks:      p.acks,
		TimeoutMs: int32(p.a.timeout.Milliseconds()),
		Topics:    []*streamqv1.ProduceTopic{{Topic: p.topic}},
	}
	counts := make(map[int32]int, len(parts))
	for _, part := range parts {
		b := p.builders[part]
		batch, err := b.Build()
		if err != nil {
			return fmt.Errorf("build batch for %s/%d: %w", p.topic, part, err)
		}
		counts[part] = b.Len()
		req.Topics[0].Partitions = append(req.Topics[0].Partitions, &streamqv1.ProducePartition{
			Partition: part, LeaderEpoch: noEpoch, Records: batch,
		})
		delete(p.builders, part)
	}

	rctx, cancel := p.a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.Produce(rctx, req)
	if err != nil {
		return fmt.Errorf("produce to topic %q: %w", p.topic, err)
	}
	acked := make(map[int32]bool, len(parts))
	var errs []error
	for _, t := range resp.GetTopics() {
		if t.GetTopic() != p.topic {
			continue
		}
		for _, pr := range t.GetPartitions() {
			if _, ok := counts[pr.GetPartition()]; !ok {
				continue
			}
			acked[pr.GetPartition()] = true
			if code := pr.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				errs = append(errs, apiError(fmt.Sprintf("produce to topic %q partition %d", p.topic, pr.GetPartition()),
					code, pr.GetErrorMessage()))
				continue
			}
			p.produced += counts[pr.GetPartition()]
		}
	}
	for _, part := range parts {
		if !acked[part] {
			errs = append(errs, fmt.Errorf("produce to topic %q partition %d: no response for partition", p.topic, part))
		}
	}
	return errors.Join(errs...)
}
