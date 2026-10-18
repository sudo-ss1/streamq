//go:build integration

package linearizability

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/testnode"
)

const (
	topic     = "lin"
	clients   = 4
	totalOps  = 1200
	faultEach = 150
	faults    = 8
)

// conns caches one gRPC connection per broker address.
type conns struct {
	mu sync.Mutex
	m  map[string]*grpc.ClientConn
}

func (c *conns) get(addr string) (streamqv1.StreamQServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.m[addr]; ok {
		return streamqv1.NewStreamQServiceClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.m[addr] = conn
	return streamqv1.NewStreamQServiceClient(conn), nil
}

func (c *conns) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.m {
		_ = conn.Close()
	}
}

// leaderFinder asks the brokers for the partition's leader and returns a client for it.
func leaderFinder(c *testnode.Cluster, cs *conns) LeaderFunc {
	return func(ctx context.Context) (streamqv1.StreamQServiceClient, error) {
		for id := int32(1); id <= 3; id++ {
			api, err := cs.get(c.Addr(id))
			if err != nil {
				continue
			}
			mctx, cancel := context.WithTimeout(ctx, time.Second)
			resp, err := api.Metadata(mctx, &streamqv1.MetadataRequest{Topics: []string{topic}})
			cancel()
			if err != nil || len(resp.GetTopics()) != 1 || len(resp.GetTopics()[0].GetPartitions()) != 1 {
				continue
			}
			leader := resp.GetTopics()[0].GetPartitions()[0].GetLeaderId()
			if leader < 1 || leader > 3 {
				continue
			}
			return cs.get(c.Addr(leader))
		}
		return nil, fmt.Errorf("no leader known for %s-0", topic)
	}
}

// waitISR waits until the partition has a leader and a full ISR, as seen by some broker.
func waitISR(t *testing.T, c *testnode.Cluster, cs *conns) {
	t.Helper()
	require.Eventually(t, func() bool {
		for id := int32(1); id <= 3; id++ {
			if c.Node(id) == nil {
				continue
			}
			api, err := cs.get(c.Addr(id))
			if err != nil {
				return false
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			resp, err := api.Metadata(ctx, &streamqv1.MetadataRequest{Topics: []string{topic}})
			cancel()
			if err != nil || len(resp.GetTopics()) != 1 || len(resp.GetTopics()[0].GetPartitions()) != 1 {
				return false
			}
			p := resp.GetTopics()[0].GetPartitions()[0]
			return p.GetLeaderId() > 0 && len(p.GetIsr()) == 3
		}
		return false
	}, 60*time.Second, 50*time.Millisecond, "partition healthy")
}

func currentLeader(t *testing.T, c *testnode.Cluster, cs *conns) int32 {
	t.Helper()
	for id := int32(1); id <= 3; id++ {
		api, err := cs.get(c.Addr(id))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := api.Metadata(ctx, &streamqv1.MetadataRequest{Topics: []string{topic}})
		cancel()
		if err == nil && len(resp.GetTopics()) == 1 && len(resp.GetTopics()[0].GetPartitions()) == 1 {
			return resp.GetTopics()[0].GetPartitions()[0].GetLeaderId()
		}
	}
	t.Fatal("no broker answered metadata")
	return -1
}

// TestLinearizability (SPEC §9 M9, plan §1/R12): concurrent single-attempt acks=all appends and
// whole-partition reads on one partition, while leaders and followers are stopped and restarted
// (graceful; crashes are covered by the chaos runner), must be linearizable against the log model
// and durable in the settled final log.
func TestLinearizability(t *testing.T) {
	c := testnode.StartCluster(t, 3, func(cfg *config.Config) {
		cfg.TopicDefaults.MinInsyncReplicas = 2
		cfg.ReplicaLagMaxMs = 1500
	})
	cs := &conns{m: map[string]*grpc.ClientConn{}}
	defer cs.close()
	require.Eventually(t, func() bool {
		api, err := cs.get(c.Addr(1))
		if err != nil {
			return false
		}
		resp, err := api.CreateTopic(context.Background(), &streamqv1.CreateTopicRequest{Name: topic, Partitions: 1, ReplicationFactor: 3,
			Configs: map[string]string{"min_insync_replicas": "2"}})
		return err == nil && (resp.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_NONE ||
			resp.GetErrorCode() == streamqv1.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS)
	}, 30*time.Second, 50*time.Millisecond)
	waitISR(t, c, cs)

	seed := uint64(42)
	if v := os.Getenv("STREAMQ_LIN_SEED"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err)
		seed = parsed
	}
	t.Logf("seed %d (set STREAMQ_LIN_SEED to reproduce)", seed)
	rec := NewRecorder()
	leader := leaderFinder(c, cs)
	var done atomic.Int64
	ctx, stop := context.WithCancel(context.Background())
	var g errgroup.Group
	for id := range clients {
		g.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(id), seed))
			for n := 0; ctx.Err() == nil; n++ {
				var ok bool
				if rng.IntN(10) < 7 {
					ok = rec.Append(ctx, id, leader, topic, 0, fmt.Sprintf("%d-%d", id, n))
				} else {
					ok = rec.Read(ctx, id, leader, topic, 0)
				}
				done.Add(1)
				if !ok {
					select { // back off after errors (D58)
					case <-ctx.Done():
					case <-time.After(time.Duration(50+rng.IntN(150)) * time.Millisecond):
					}
				}
			}
			return nil
		})
	}

	// Faults paced by completed operations (plan R12): before each fault, at least faultEach more
	// operations complete; alternately the leader and a random broker are stopped and restarted.
	frng := rand.New(rand.NewPCG(7, seed))
	waitOps := func(target int64) {
		require.Eventually(t, func() bool { return done.Load() >= target }, 60*time.Second, 5*time.Millisecond)
	}
	for fault := range faults {
		waitOps(done.Load() + faultEach)
		victim := currentLeader(t, c, cs)
		if fault%2 == 1 || victim < 1 {
			victim = int32(1 + frng.IntN(3))
		}
		t.Logf("fault %d: stop and restart broker %d (after %d ops)", fault+1, victim, done.Load())
		c.Stop(victim)
		c.Restart(victim)
		waitISR(t, c, cs)
	}
	waitOps(max(done.Load()+faultEach, totalOps))
	stop()
	require.NoError(t, g.Wait())

	// Settle (plan R16): sentinel appends with fresh values until one is acknowledged at offset s,
	// then a final read that reaches past s.
	sentinelAt := int64(-1)
	for k := 0; sentinelAt < 0; k++ {
		require.Less(t, k, 300, "a sentinel is acknowledged")
		if rec.Append(context.Background(), clients, leader, topic, 0, fmt.Sprintf("sentinel-%d", k)) {
			for _, op := range rec.History() {
				if op.Input.(Input).Value == fmt.Sprintf("sentinel-%d", k) {
					sentinelAt = op.Output.(Output).Offset
				}
			}
			continue
		}
		<-time.After(100 * time.Millisecond)
	}
	var final []string
	var anomaly *Anomaly
	require.Eventually(t, func() bool {
		api, err := leader(context.Background())
		if err != nil {
			return false
		}
		values, err := ReadAll(context.Background(), api, topic, 0)
		if errors.As(err, &anomaly) {
			return true // reported below, on the test goroutine
		}
		if err != nil {
			return false
		}
		final = values
		return int64(len(values)) > sentinelAt
	}, 30*time.Second, 50*time.Millisecond, "final read reaches the sentinel")
	require.Nil(t, anomaly, "final read anomaly")

	ops := rec.History()
	sum := rec.Summary()
	require.Empty(t, sum.Anomalies, "read integrity anomalies")
	require.NoError(t, VerifyFinal(final, ops))
	prepared := Prepare(ops, final)
	t.Logf("summary %+v, %d unknown appends dropped, final log %d entries", sum, len(ops)-len(prepared), len(final))
	require.GreaterOrEqual(t, sum.Acked+sum.Unknown+sum.Reads, totalOps/2, "enough recorded operations")
	res, info := Check(prepared, 60*time.Second)
	if res != porcupine.Ok {
		dir := os.Getenv("STREAMQ_LIN_OUT")
		if dir == "" {
			dir = os.TempDir()
		}
		path := filepath.Join(dir, fmt.Sprintf("linearizability-%d.html", time.Now().UnixNano()))
		require.NoError(t, WriteVisualization(path, info))
		t.Fatalf("history is not linearizable (%v, inconclusive counts as failure); visualization: %s", res, path)
	}
}
