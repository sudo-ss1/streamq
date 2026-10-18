package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/test/linearizability"
)

// brokerIDs are the compose cluster's brokers.
func brokerIDs() []int32 { return []int32{1, 2, 3} }

// brokerAddr is the host address of a broker's client API (D54: published on localhost).
func brokerAddr(id int32) string { return fmt.Sprintf("localhost:%d", 9092+100*(id-1)) }

// bootstrap lists every broker's client address.
func bootstrap() []string {
	out := make([]string, 0, 3)
	for _, id := range brokerIDs() {
		out = append(out, brokerAddr(id))
	}
	return out
}

// metadataTimeout bounds each Metadata call; a paused broker accepts connections but never
// answers.
const metadataTimeout = time.Second

// cluster holds one gRPC connection per broker of the compose cluster.
type cluster struct {
	mu    sync.Mutex
	conns map[int32]*grpc.ClientConn
}

func newCluster() *cluster { return &cluster{conns: map[int32]*grpc.ClientConn{}} }

// api returns a client for broker id.
func (c *cluster) api(id int32) (streamqv1.StreamQServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[id]; ok {
		return streamqv1.NewStreamQServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(brokerAddr(id), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		return nil, err
	}
	c.conns[id] = conn
	return streamqv1.NewStreamQServiceClient(conn), nil
}

// close closes every connection.
func (c *cluster) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, id)
	}
}

// metadataAll asks every broker for metadata in parallel and returns the answers by broker.
func (c *cluster) metadataAll(ctx context.Context, topics []string) map[int32]*streamqv1.MetadataResponse {
	var mu sync.Mutex
	out := map[int32]*streamqv1.MetadataResponse{}
	var wg sync.WaitGroup
	for _, id := range brokerIDs() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			api, err := c.api(id)
			if err != nil {
				return
			}
			mctx, cancel := context.WithTimeout(ctx, metadataTimeout)
			defer cancel()
			resp, err := api.Metadata(mctx, &streamqv1.MetadataRequest{Topics: topics})
			if err != nil {
				return
			}
			mu.Lock()
			out[id] = resp
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// partitionView is one partition's metadata as reported by a broker.
type partitionView struct {
	From     int32 `json:"from"`
	Leader   int32 `json:"leader"`
	Epoch    int32 `json:"epoch"`
	Replicas []int32
	ISR      []int32 `json:"isr"`
}

// partitionsOf returns the topic's partitions from one metadata response, indexed by id.
func partitionsOf(from int32, resp *streamqv1.MetadataResponse, topic string) map[int32]partitionView {
	out := map[int32]partitionView{}
	for _, t := range resp.GetTopics() {
		if t.GetName() != topic || t.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
			continue
		}
		for _, p := range t.GetPartitions() {
			out[p.GetPartition()] = partitionView{From: from, Leader: p.GetLeaderId(), Epoch: p.GetLeaderEpoch(),
				Replicas: slices.Clone(p.GetReplicas()), ISR: slices.Clone(p.GetIsr())}
		}
	}
	return out
}

// newestViews merges the brokers' answers, keeping per partition the view with the highest
// leader epoch (brokers apply the metadata log at different times; a deposed leader may lag).
func newestViews(resps map[int32]*streamqv1.MetadataResponse, topic string) map[int32]partitionView {
	out := map[int32]partitionView{}
	for _, from := range brokerIDs() {
		resp, ok := resps[from]
		if !ok {
			continue
		}
		for p, v := range partitionsOf(from, resp, topic) {
			if cur, ok := out[p]; !ok || v.Epoch > cur.Epoch || (v.Epoch == cur.Epoch && len(v.ISR) < len(cur.ISR)) {
				out[p] = v
			}
		}
	}
	return out
}

// partition returns the newest view of one partition.
func (c *cluster) partition(ctx context.Context, topic string, p int32) (partitionView, error) {
	v, ok := newestViews(c.metadataAll(ctx, []string{topic}), topic)[p]
	if !ok {
		return partitionView{}, fmt.Errorf("no broker reported %s-%d", topic, p)
	}
	return v, nil
}

// controller returns the controller most brokers report, or -1.
func (c *cluster) controller(ctx context.Context) int32 {
	votes := map[int32]int{}
	for _, resp := range c.metadataAll(ctx, nil) {
		if id := resp.GetControllerId(); id > 0 {
			votes[id]++
		}
	}
	best, n := int32(-1), 0
	for _, id := range brokerIDs() {
		if votes[id] > n {
			best, n = id, votes[id]
		}
	}
	return best
}

// liveBrokers returns the live (registered, unfenced) brokers, taking the most pessimistic
// answer: a broker still listed by a lagging peer is not trusted.
func liveBrokers(resps map[int32]*streamqv1.MetadataResponse) []int32 {
	var out []int32
	for _, resp := range resps {
		var ids []int32
		for _, b := range resp.GetBrokers() {
			ids = append(ids, b.GetId())
		}
		slices.Sort(ids)
		if out == nil || len(ids) < len(out) {
			out = ids
		}
	}
	return out
}

// leaderFunc returns a LeaderFunc that routes to the partition leader of the newest view.
func (c *cluster) leaderFunc(topic string, p int32) linearizability.LeaderFunc {
	return func(ctx context.Context) (streamqv1.StreamQServiceClient, error) {
		v, err := c.partition(ctx, topic, p)
		if err != nil {
			return nil, err
		}
		if v.Leader < 1 || v.Leader > 3 {
			return nil, fmt.Errorf("%s-%d has no leader", topic, p)
		}
		return c.api(v.Leader)
	}
}

// errNotHealthy reports a cluster that did not become healthy in time.
var errNotHealthy = errors.New("cluster not healthy")

// waitHealthy waits until `streamqctl cluster status --strict` succeeds against some broker and
// every broker is live (plan M9 §2: the runner uses the CLI's health definition).
func (r *runner) waitHealthy(ctx context.Context, cl *cluster, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last string
	for i := 0; ; i++ {
		addr := bootstrap()[i%3]
		out, err := r.ctl.clusterStatusStrict(ctx, addr)
		if err == nil {
			if resps := cl.metadataAll(ctx, nil); len(resps) == 3 && len(liveBrokers(resps)) == 3 {
				return nil
			}
			err = errors.New("not every broker answers and is live")
		}
		last = fmt.Sprintf("%s: %v\n%s", addr, err, out)
		if !wait(ctx, 500*time.Millisecond) {
			return fmt.Errorf("%w after %v; last status:\n%s", errNotHealthy, timeout, last)
		}
	}
}

// wait sleeps for d or until ctx ends; it reports whether the full duration elapsed.
func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
