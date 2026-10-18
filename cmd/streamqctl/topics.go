package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// topicsCmd builds `topics create|delete|list|describe`.
func (a *app) topicsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "topics", Short: "Manage topics"}
	cmd.AddCommand(a.topicsCreateCmd(), a.topicsDeleteCmd(), a.topicsListCmd(), a.topicsDescribeCmd())
	return cmd
}

func (a *app) topicsCreateCmd() *cobra.Command {
	var (
		partitions, rf int32
		configs        []string
	)
	cmd := &cobra.Command{
		Use:   "create NAME",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			cfg, err := parseConfigs(configs)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			rctx, cancel := a.rpcCtx(ctx)
			defer cancel()
			resp, err := c.CreateTopic(rctx, &streamqv1.CreateTopicRequest{
				Name: name, Partitions: partitions, ReplicationFactor: rf, Configs: cfg,
			})
			if err != nil {
				return fmt.Errorf("create topic %q: %w", name, err)
			}
			if code := resp.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return apiError(fmt.Sprintf("create topic %q", name), code, resp.GetErrorMessage())
			}
			_, err = fmt.Fprintf(a.out, "created topic %s\n", name)
			return err
		},
	}
	cmd.Flags().Int32Var(&partitions, "partitions", 0, "number of partitions (0 = broker default)")
	cmd.Flags().Int32Var(&rf, "replication-factor", 0, "replication factor (0 = broker default)")
	cmd.Flags().StringArrayVar(&configs, "config", nil, "topic config override key=value (repeatable)")
	return cmd
}

// parseConfigs parses key=value pairs. Keys must be non-empty and unique.
func parseConfigs(kvs []string) (map[string]string, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid --config %q: want key=value", kv)
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("invalid --config %q: duplicate key %q", kv, k)
		}
		out[k] = v
	}
	return out, nil
}

func (a *app) topicsDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a topic and its data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			rctx, cancel := a.rpcCtx(ctx)
			defer cancel()
			resp, err := c.DeleteTopic(rctx, &streamqv1.DeleteTopicRequest{Name: name})
			if err != nil {
				return fmt.Errorf("delete topic %q: %w", name, err)
			}
			if code := resp.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
				return apiError(fmt.Sprintf("delete topic %q", name), code, resp.GetErrorMessage())
			}
			_, err = fmt.Fprintf(a.out, "deleted topic %s\n", name)
			return err
		},
	}
}

func (a *app) topicsListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List topic names",
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
			resp, err := c.ListTopics(rctx, &streamqv1.ListTopicsRequest{})
			if err != nil {
				return fmt.Errorf("list topics: %w", err)
			}
			names := slices.Clone(resp.GetNames())
			slices.Sort(names)
			for _, n := range names {
				if _, err := fmt.Fprintln(a.out, n); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func (a *app) topicsDescribeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "describe [NAME...]",
		Short: "Describe topics (all topics when no name is given)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			rctx, cancel := a.rpcCtx(ctx)
			defer cancel()
			resp, err := c.Metadata(rctx, &streamqv1.MetadataRequest{Topics: args})
			if err != nil {
				return fmt.Errorf("describe topics: %w", err)
			}
			return writeDescribe(a, resp.GetTopics())
		},
	}
}

// writeDescribe prints one table row per partition, sorted by topic and partition. Topics with
// an error get a single row naming the error, and the command then fails.
func writeDescribe(a *app, topics []*streamqv1.TopicMetadata) error {
	topics = slices.Clone(topics)
	slices.SortFunc(topics, func(x, y *streamqv1.TopicMetadata) int { return strings.Compare(x.GetName(), y.GetName()) })

	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TOPIC\tPARTITION\tLEADER\tEPOCH\tREPLICAS\tISR\tERROR")
	var failed []string
	for _, t := range topics {
		if code := t.GetErrorCode(); code != streamqv1.ErrorCode_ERROR_CODE_NONE {
			failed = append(failed, fmt.Sprintf("%s: %s", t.GetName(), codeName(code)))
			_, _ = fmt.Fprintf(tw, "%s\t-\t-\t-\t-\t-\t%s\n", t.GetName(), codeName(code))
			continue
		}
		parts := slices.Clone(t.GetPartitions())
		slices.SortFunc(parts, func(x, y *streamqv1.PartitionMetadata) int {
			return int(x.GetPartition()) - int(y.GetPartition())
		})
		for _, p := range parts {
			errCol := "-"
			if p.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
				errCol = codeName(p.GetErrorCode())
			}
			_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\t%s\t%s\n", t.GetName(), p.GetPartition(), p.GetLeaderId(),
				p.GetLeaderEpoch(), joinIDs(p.GetReplicas()), joinIDs(p.GetIsr()), errCol)
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(failed) > 0 {
		return fmt.Errorf("describe topics: %s", strings.Join(failed, "; "))
	}
	return nil
}

// joinIDs renders broker IDs as a comma-separated list.
func joinIDs(ids []int32) string {
	s := make([]string, len(ids))
	for i, id := range ids {
		s[i] = strconv.Itoa(int(id))
	}
	return strings.Join(s, ",")
}
