package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// groupsCmd builds `groups list|describe|lag` (FR11).
func (a *app) groupsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "groups", Short: "Inspect consumer groups"}
	cmd.AddCommand(a.groupsListCmd(), a.groupsDescribeCmd(), a.groupsLagCmd())
	return cmd
}

func (a *app) groupsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List consumer group ids",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			rctx, cancel := a.rpcCtx(ctx)
			defer cancel()
			resp, err := c.ListGroups(rctx, &streamqv1.ListGroupsRequest{})
			if err != nil {
				return fmt.Errorf("list groups: %w", err)
			}
			if code := resp.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return apiError("list groups", code, resp.GetErrorMessage())
			}
			ids := slices.Clone(resp.GetGroupIds())
			slices.Sort(ids)
			for _, id := range ids {
				if _, err := fmt.Fprintln(a.out, id); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// describeGroup fetches a group's description and fails on a non-NONE error code.
func (a *app) describeGroup(ctx context.Context, c streamqv1.StreamQServiceClient, group string) (*streamqv1.DescribeGroupResponse, error) {
	rctx, cancel := a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.DescribeGroup(rctx, &streamqv1.DescribeGroupRequest{GroupId: group})
	if err != nil {
		return nil, fmt.Errorf("describe group %q: %w", group, err)
	}
	if code := resp.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
		return nil, apiError(fmt.Sprintf("describe group %q", group), code, resp.GetErrorMessage())
	}
	return resp, nil
}

func (a *app) groupsDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe GROUP",
		Short: "Describe a consumer group's state, members and assignments",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			resp, err := a.describeGroup(ctx, c, args[0])
			if err != nil {
				return err
			}
			return writeGroupDescribe(a, args[0], resp)
		},
	}
}

// writeGroupDescribe prints the group header and one row per member per assigned topic, sorted
// by member id then topic, with the partitions sorted and comma-separated.
func writeGroupDescribe(a *app, group string, resp *streamqv1.DescribeGroupResponse) error {
	name := resp.GetGroupId()
	if name == "" {
		name = group
	}
	assignor := resp.GetAssignor()
	if assignor == "" {
		assignor = "-"
	}
	if _, err := fmt.Fprintf(a.out, "GROUP       %s\nSTATE       %s\nGENERATION  %d\nASSIGNOR    %s\n",
		name, resp.GetState(), resp.GetGenerationId(), assignor); err != nil {
		return err
	}
	members := slices.Clone(resp.GetMembers())
	if len(members) == 0 {
		_, err := fmt.Fprintln(a.out, "no active members")
		return err
	}
	slices.SortFunc(members, func(x, y *streamqv1.GroupMember) int {
		return strings.Compare(x.GetMemberId(), y.GetMemberId())
	})
	if _, err := fmt.Fprintln(a.out); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MEMBER\tTOPIC\tPARTITIONS")
	for _, m := range members {
		tps := slices.Clone(m.GetAssignment())
		if len(tps) == 0 {
			_, _ = fmt.Fprintf(tw, "%s\t-\t-\n", m.GetMemberId())
			continue
		}
		slices.SortFunc(tps, func(x, y *streamqv1.TopicPartitions) int {
			return strings.Compare(x.GetTopic(), y.GetTopic())
		})
		for _, tp := range tps {
			parts := slices.Clone(tp.GetPartitions())
			slices.Sort(parts)
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\n", m.GetMemberId(), tp.GetTopic(), joinIDs(parts))
		}
	}
	return tw.Flush()
}

func (a *app) groupsLagCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "lag GROUP",
		Short: "Show per-partition committed offset, high watermark and lag for a group",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			group := args[0]
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			resp, err := a.describeGroup(ctx, c, group)
			if err != nil {
				return err
			}
			committed := slices.Clone(resp.GetCommitted())
			slices.SortFunc(committed, func(x, y *streamqv1.OffsetCommit) int {
				return cmp.Or(strings.Compare(x.GetTopic(), y.GetTopic()), cmp.Compare(x.GetPartition(), y.GetPartition()))
			})
			hws, err := a.highWatermarks(ctx, c, committed)
			if err != nil {
				return err
			}
			return writeLag(a, committed, hws)
		},
	}
}

// topicPartition identifies one partition.
type topicPartition struct {
	topic     string
	partition int32
}

// highWatermarks issues one ListOffsets (latest, no epoch check) for every committed partition
// and returns the high watermark of each partition that answered without an error. Partitions
// that errored or are missing from the response are absent from the map.
func (a *app) highWatermarks(ctx context.Context, c streamqv1.StreamQServiceClient, committed []*streamqv1.OffsetCommit) (map[topicPartition]int64, error) {
	hws := make(map[topicPartition]int64, len(committed))
	if len(committed) == 0 {
		return hws, nil
	}
	req := &streamqv1.ListOffsetsRequest{}
	byTopic := make(map[string]*streamqv1.ListOffsetsTopic)
	for _, oc := range committed {
		t, ok := byTopic[oc.GetTopic()]
		if !ok {
			t = &streamqv1.ListOffsetsTopic{Topic: oc.GetTopic()}
			byTopic[oc.GetTopic()] = t
			req.Topics = append(req.Topics, t)
		}
		t.Partitions = append(t.Partitions, &streamqv1.ListOffsetsPartition{
			Partition: oc.GetPartition(), LeaderEpoch: -1, Timestamp: -1,
		})
	}
	rctx, cancel := a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.ListOffsets(rctx, req)
	if err != nil {
		return nil, fmt.Errorf("list offsets: %w", err)
	}
	for _, t := range resp.GetTopics() {
		for _, p := range t.GetPartitions() {
			if p.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE || p.GetOffset() < 0 {
				continue
			}
			hws[topicPartition{t.GetTopic(), p.GetPartition()}] = p.GetOffset()
		}
	}
	return hws, nil
}

// writeLag prints one row per committed partition (already sorted) and the total lag over the
// partitions whose high watermark is known. Lag never goes below zero.
func writeLag(a *app, committed []*streamqv1.OffsetCommit, hws map[topicPartition]int64) error {
	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TOPIC\tPARTITION\tCOMMITTED\tHIGH-WATERMARK\tLAG")
	var total int64
	for _, oc := range committed {
		hwCol, lagCol := "-", "-"
		if hw, ok := hws[topicPartition{oc.GetTopic(), oc.GetPartition()}]; ok {
			lag := max(hw-oc.GetOffset(), 0)
			total += lag
			hwCol, lagCol = strconv.FormatInt(hw, 10), strconv.FormatInt(lag, 10)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%s\n", oc.GetTopic(), oc.GetPartition(), oc.GetOffset(), hwCol, lagCol)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(a.out, "TOTAL LAG %d\n", total)
	return err
}
