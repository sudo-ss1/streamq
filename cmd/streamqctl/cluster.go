package main

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// errClusterUnhealthy is returned by `cluster status --strict` when a broker is down or a
// partition is under-replicated or offline.
var errClusterUnhealthy = errors.New("cluster is unhealthy")

// clusterCmd builds `cluster status` (FR11).
func (a *app) clusterCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "cluster", Short: "Inspect the cluster"}
	cmd.AddCommand(a.clusterStatusCmd())
	return cmd
}

func (a *app) clusterStatusCmd() *cobra.Command {
	var strict bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show brokers, the controller and partition health",
		Long: `Show the controller, the brokers and per-topic partition health.

Only live (registered and unfenced) brokers are reported by the cluster, as in Kafka's
describeCluster. A broker that is referenced as a partition replica but is not live is listed
as "down"; a stopped broker that hosts no replicas cannot be seen and is not listed.

A partition is under-replicated when its ISR is smaller than its replica set, and offline when
it has no leader. With --strict the command fails when no broker is live, there is no
controller, any broker is down, or any partition is under-replicated or offline.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			c, closeConn, err := a.client(ctx)
			if err != nil {
				return err
			}
			defer closeConn()
			rctx, cancel := a.rpcCtx(ctx)
			defer cancel()
			resp, err := c.Metadata(rctx, &streamqv1.MetadataRequest{})
			if err != nil {
				return fmt.Errorf("metadata: %w", err)
			}
			st := summarizeCluster(resp)
			if err := writeClusterStatus(a, st); err != nil {
				return err
			}
			if strict && !st.healthy() {
				return fmt.Errorf("%w: %d live broker(s), controller %d, %d broker(s) down, %d partition(s) under-replicated, %d offline",
					errClusterUnhealthy, st.live(), st.controller, st.down, st.underReplicated, st.offline)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&strict, "strict", false,
		"exit non-zero unless brokers are live with a controller, none is down, and no partition is under-replicated or offline")
	return cmd
}

// brokerRow is one line of the broker table.
type brokerRow struct {
	id   int32
	addr string
	live bool
}

// topicHealth is the partition health of one topic.
type topicHealth struct {
	name            string
	errCode         streamqv1.ErrorCode
	partitions      int
	underReplicated int
	offline         int
}

// clusterStatus is the summarized Metadata response.
type clusterStatus struct {
	controller      int32
	brokers         []brokerRow
	topics          []topicHealth
	down            int
	underReplicated int
	offline         int
}

func (s clusterStatus) healthy() bool {
	return s.down == 0 && s.underReplicated == 0 && s.offline == 0 && s.live() > 0 && s.controller >= 0
}

// live counts brokers in the live state.
func (s clusterStatus) live() int {
	n := 0
	for _, b := range s.brokers {
		if b.live {
			n++
		}
	}
	return n
}

// summarizeCluster builds the broker table (live brokers plus replicas absent from the live
// list, marked down) and per-topic partition health, sorted by broker id and topic name.
func summarizeCluster(resp *streamqv1.MetadataResponse) clusterStatus {
	st := clusterStatus{controller: resp.GetControllerId()}
	known := make(map[int32]bool)
	for _, b := range resp.GetBrokers() {
		known[b.GetId()] = true
		st.brokers = append(st.brokers, brokerRow{
			id: b.GetId(), addr: net.JoinHostPort(b.GetHost(), strconv.Itoa(int(b.GetPort()))), live: true,
		})
	}
	for _, t := range resp.GetTopics() {
		th := topicHealth{name: t.GetName(), errCode: t.GetErrorCode(), partitions: len(t.GetPartitions())}
		for _, p := range t.GetPartitions() {
			for _, r := range p.GetReplicas() {
				if !known[r] {
					known[r] = true
					st.brokers = append(st.brokers, brokerRow{id: r, addr: "-"})
					st.down++
				}
			}
			if p.GetLeaderId() < 0 {
				th.offline++
			}
			if len(p.GetIsr()) < len(p.GetReplicas()) {
				th.underReplicated++
			}
		}
		st.underReplicated += th.underReplicated
		st.offline += th.offline
		st.topics = append(st.topics, th)
	}
	slices.SortFunc(st.brokers, func(x, y brokerRow) int { return cmp.Compare(x.id, y.id) })
	slices.SortFunc(st.topics, func(x, y topicHealth) int { return strings.Compare(x.name, y.name) })
	return st
}

// writeClusterStatus prints the controller, the broker table and the topic health table.
func writeClusterStatus(a *app, st clusterStatus) error {
	controller := "none"
	if st.controller >= 0 {
		controller = strconv.Itoa(int(st.controller))
	}
	if _, err := fmt.Fprintf(a.out, "CONTROLLER  %s\n\n", controller); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "BROKER\tADDRESS\tSTATE\tCONTROLLER")
	for _, b := range st.brokers {
		state := "down"
		if b.live {
			state = "live"
		}
		mark := ""
		if b.id == st.controller {
			mark = "*"
		}
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", b.id, b.addr, state, mark)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(st.topics) == 0 {
		_, err := fmt.Fprintln(a.out, "\nno topics")
		return err
	}
	if _, err := fmt.Fprintln(a.out); err != nil {
		return err
	}
	tw = tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tUNDER_REPLICATED\tOFFLINE\tERROR")
	for _, t := range st.topics {
		errCol := "-"
		if t.errCode != streamqv1.ErrorCode_ERROR_CODE_NONE {
			errCol = codeName(t.errCode)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\n", t.name, t.partitions, t.underReplicated, t.offline, errCol)
	}
	return tw.Flush()
}
