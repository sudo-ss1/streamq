package testnode

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
)

// Cluster is an in-process multi-broker cluster on loopback with fast timeouts.
type Cluster struct {
	tb      testing.TB
	configs []config.Config
	opt     func(id int32, o *node.Options) // per-node options hook (may be nil)
	mu      sync.Mutex
	nodes   []*node.Node // nil while stopped
}

// clusterListeners holds one node's pre-bound listeners.
type clusterListeners struct{ client, internal, raft net.Listener }

func bind(tb testing.TB, addr string) net.Listener {
	tb.Helper()
	var lis net.Listener
	var err error
	for range 100 { // a just-stopped node's port can take a moment to be reusable
		lis, err = net.Listen("tcp", addr) //nolint:noctx // test setup
		if err == nil {
			return lis
		}
		<-time.After(50 * time.Millisecond)
	}
	require.NoError(tb, err)
	return nil
}

// StartCluster starts n brokers concurrently (cluster members block until a quorum elects a
// controller) and stops them when the test ends. mod may adjust each config.
func StartCluster(tb testing.TB, n int, mod func(*config.Config)) *Cluster {
	tb.Helper()
	return StartClusterWith(tb, n, mod, nil)
}

// StartClusterWith is StartCluster with a hook that adjusts each node's Options (test hooks such
// as BeforeBrokerClose); it is applied again on Restart.
func StartClusterWith(tb testing.TB, n int, mod func(*config.Config), opt func(id int32, o *node.Options)) *Cluster {
	tb.Helper()
	c := &Cluster{tb: tb, nodes: make([]*node.Node, n), opt: opt}
	lis := make([]clusterListeners, n)
	var members []config.ClusterMember
	for i := range n {
		lis[i] = clusterListeners{bind(tb, "127.0.0.1:0"), bind(tb, "127.0.0.1:0"), bind(tb, "127.0.0.1:0")}
		members = append(members, config.ClusterMember{
			ID: int32(i + 1), Client: lis[i].client.Addr().String(), Internal: lis[i].internal.Addr().String(), Raft: lis[i].raft.Addr().String(),
		})
	}
	for i := range n {
		cfg := config.Default()
		cfg.MetricsAddr = ""                   // tests bind no metrics port (M8 plan R1)
		cfg.ControlledShutdownTimeoutMs = 2000 // fast teardown (M8 plan R2)
		cfg.BrokerID = int32(i + 1)
		cfg.DataDir = tb.TempDir()
		cfg.ListenAddr, cfg.InternalAddr, cfg.RaftAddr = members[i].Client, members[i].Internal, members[i].Raft
		cfg.AdvertisedHost = "127.0.0.1"
		cfg.Cluster = members
		cfg.RaftHeartbeatTimeoutMs, cfg.RaftElectionTimeoutMs = 300, 300
		cfg.BrokerHeartbeatIntervalMs, cfg.BrokerSessionTimeoutMs = 300, 2000
		cfg.GroupInitialRebalanceDelayMs = 200
		cfg.TopicDefaults.ReplicationFactor = int32(min(n, 3))
		cfg.TopicDefaults.MinInsyncReplicas = 1
		if mod != nil {
			mod(&cfg)
		}
		c.configs = append(c.configs, cfg)
	}
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opts := node.Options{Client: lis[i].client, Internal: lis[i].internal, Raft: lis[i].raft}
			if c.opt != nil {
				c.opt(int32(i+1), &opts)
			}
			c.nodes[i], errs[i] = node.StartWithOptions(context.Background(), c.configs[i], slog.New(slog.DiscardHandler), opts)
		}()
	}
	wg.Wait()
	tb.Cleanup(c.Close)
	for _, err := range errs {
		require.NoError(tb, err)
	}
	return c
}

// Node returns broker id's node (nil if stopped).
func (c *Cluster) Node(id int32) *node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id-1]
}

// Config returns broker id's config.
func (c *Cluster) Config(id int32) config.Config { return c.configs[id-1] }

// Addr returns broker id's client address.
func (c *Cluster) Addr(id int32) string { return c.configs[id-1].ListenAddr }

// Stop gracefully stops broker id.
func (c *Cluster) Stop(id int32) {
	c.tb.Helper()
	c.mu.Lock()
	n := c.nodes[id-1]
	c.nodes[id-1] = nil
	c.mu.Unlock()
	require.NoError(c.tb, n.Close(context.Background()))
}

// Restart starts a stopped broker again on its data dir and ports.
func (c *Cluster) Restart(id int32) {
	c.tb.Helper()
	cfg := c.configs[id-1]
	opts := node.Options{Client: bind(c.tb, cfg.ListenAddr), Internal: bind(c.tb, cfg.InternalAddr), Raft: bind(c.tb, cfg.RaftAddr)}
	if c.opt != nil {
		c.opt(id, &opts)
	}
	n, err := node.StartWithOptions(context.Background(), cfg, slog.New(slog.DiscardHandler), opts)
	require.NoError(c.tb, err)
	c.mu.Lock()
	c.nodes[id-1] = n
	c.mu.Unlock()
}

// Close stops every running broker.
func (c *Cluster) Close() {
	c.mu.Lock()
	nodes := c.nodes
	c.nodes = make([]*node.Node, len(nodes))
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range nodes {
		if n != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = n.Close(context.Background())
			}()
		}
	}
	wg.Wait()
}
