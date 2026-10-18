package loadgen

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/internal/node"
	"github.com/sudo-ss1/streamq/pkg/client"
)

// startupTimeout bounds in-process cluster startup.
const startupTimeout = 2 * time.Minute

// controlledShutdownTimeout keeps teardown short (a harmless override, M10 plan R2).
const controlledShutdownTimeoutMs = 2000

// inProcessCluster is a set of brokers running in this process (M10 plan R2, R6).
type inProcessCluster struct {
	nodes []*node.Node
	cfgs  []config.Config
}

// brokerConfigs returns validated production-default configs for n brokers on the given
// pre-bound listeners. Only listeners, metrics, data dir, advertised host, cluster members and
// the controlled-shutdown timeout are overridden.
func brokerConfigs(n int, parentDir string, lis [][3]net.Listener) ([]config.Config, error) {
	var members []config.ClusterMember
	if n > 1 {
		for i := range n {
			members = append(members, config.ClusterMember{
				ID:       int32(i + 1),
				Client:   lis[i][0].Addr().String(),
				Internal: lis[i][1].Addr().String(),
				Raft:     lis[i][2].Addr().String(),
			})
		}
	}
	cfgs := make([]config.Config, n)
	for i := range n {
		cfg := config.Default()
		cfg.BrokerID = int32(i + 1)
		cfg.ListenAddr = lis[i][0].Addr().String()
		cfg.InternalAddr = lis[i][1].Addr().String()
		cfg.RaftAddr = lis[i][2].Addr().String()
		cfg.AdvertisedHost = "127.0.0.1"
		cfg.MetricsAddr = ""
		cfg.DataDir = filepath.Join(parentDir, fmt.Sprintf("broker-%d", i+1))
		cfg.ControlledShutdownTimeoutMs = controlledShutdownTimeoutMs
		cfg.Cluster = members
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("broker %d config: %w", i+1, err)
		}
		cfgs[i] = cfg
	}
	return cfgs, nil
}

// startInProcess pre-binds every listener on 127.0.0.1:0, builds the member list from them and
// starts n brokers concurrently (cluster members block until a quorum elects a controller).
func startInProcess(ctx context.Context, n int, parentDir string, logger *slog.Logger) (*inProcessCluster, error) {
	lis := make([][3]net.Listener, n)
	closeAll := func() {
		for _, l := range lis {
			for _, x := range l {
				if x != nil {
					_ = x.Close()
				}
			}
		}
	}
	var lc net.ListenConfig
	for i := range n {
		for j := range 3 {
			l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
			if err != nil {
				closeAll()
				return nil, fmt.Errorf("bind listener: %w", err)
			}
			lis[i][j] = l
		}
	}
	cfgs, err := brokerConfigs(n, parentDir, lis)
	if err != nil {
		closeAll()
		return nil, err
	}
	sctx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	c := &inProcessCluster{nodes: make([]*node.Node, n), cfgs: cfgs}
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opts := node.Options{Client: lis[i][0], Internal: lis[i][1], Raft: lis[i][2]}
			c.nodes[i], errs[i] = node.StartWithOptions(sctx, cfgs[i], logger, opts)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		c.close(context.WithoutCancel(ctx))
		closeAll() // listeners of nodes that never took ownership
		return nil, fmt.Errorf("start brokers: %w", err)
	}
	if err := c.waitUnfenced(sctx); err != nil {
		c.close(context.WithoutCancel(ctx))
		return nil, err
	}
	return c, nil
}

// waitUnfenced waits until every member's metadata store lists all brokers as live: a registered
// broker stays fenced until its first heartbeat, and topic creation counts only live brokers.
func (c *inProcessCluster) waitUnfenced(ctx context.Context) error {
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		ready := true
		for _, n := range c.nodes {
			if n.Raft() != nil && len(n.Raft().Snapshot().LiveBrokers()) < len(c.nodes) {
				ready = false
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("brokers not live: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// addrs returns the client addresses.
func (c *inProcessCluster) addrs() []string {
	out := make([]string, 0, len(c.nodes))
	for _, cfg := range c.cfgs {
		out = append(out, cfg.ListenAddr)
	}
	return out
}

// close stops every running broker concurrently.
func (c *inProcessCluster) close(ctx context.Context) {
	var wg sync.WaitGroup
	for i, n := range c.nodes {
		if n == nil {
			continue
		}
		c.nodes[i] = nil
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = n.Close(ctx)
		}()
	}
	wg.Wait()
}

// partitionEpochs is one partition's stability fingerprint (M10 plan R13).
type partitionEpochs struct {
	LeaderEpoch    int32
	PartitionEpoch int32   // -1 when unknown (admin describe)
	ISR            []int32 // sorted
	Replicas       int
}

// epochSource snapshots every partition of a topic.
type epochSource interface {
	epochs(ctx context.Context, topic string) (map[int32]partitionEpochs, error)
}

// raftEpochs reads the metadata store of every in-process cluster member, keeping the highest
// epochs any member has applied.
type raftEpochs struct{ c *inProcessCluster }

func (r raftEpochs) epochs(_ context.Context, topic string) (map[int32]partitionEpochs, error) {
	out := map[int32]partitionEpochs{}
	for _, n := range r.c.nodes {
		if n == nil || n.Raft() == nil {
			continue
		}
		t, ok := n.Raft().Snapshot().Topic(topic)
		if !ok {
			continue
		}
		for _, ps := range t.Partitions {
			cur, seen := out[ps.Partition]
			if seen && (cur.LeaderEpoch > ps.LeaderEpoch ||
				(cur.LeaderEpoch == ps.LeaderEpoch && cur.PartitionEpoch >= ps.PartitionEpoch)) {
				continue
			}
			isr := slices.Clone(ps.ISR)
			slices.Sort(isr)
			out[ps.Partition] = partitionEpochs{LeaderEpoch: ps.LeaderEpoch, PartitionEpoch: ps.PartitionEpoch, ISR: isr, Replicas: len(ps.Replicas)}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("topic %q not found in the metadata store", topic)
	}
	return out, nil
}

// adminEpochs uses describe (leader epoch and ISR; the partition epoch is not exposed, so an ISR
// change is detected by comparing ISR sets).
type adminEpochs struct{ admin *client.Admin }

func (a adminEpochs) epochs(ctx context.Context, topic string) (map[int32]partitionEpochs, error) {
	ds, err := a.admin.DescribeTopics(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := map[int32]partitionEpochs{}
	for _, d := range ds {
		if d.Name != topic {
			continue
		}
		for _, p := range d.Partitions {
			isr := slices.Clone(p.ISR)
			slices.Sort(isr)
			out[p.Partition] = partitionEpochs{LeaderEpoch: p.LeaderEpoch, PartitionEpoch: -1, ISR: isr, Replicas: len(p.Replicas)}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("topic %q not found", topic)
	}
	return out, nil
}

// ClusterChecks is the stability verdict of the window (M10 plan R2, R13).
type ClusterChecks struct {
	Source                string   `json:"source"` // "metadata-store" or "admin-describe"
	LeaderEpochChanges    int      `json:"leader_epoch_changes"`
	PartitionEpochChanges int      `json:"partition_epoch_changes"`
	ShrunkISR             int      `json:"isr_below_rf"`
	Problems              []string `json:"problems,omitempty"`
}

// compareEpochs compares the before and after snapshots of the window.
func compareEpochs(source string, before, after map[int32]partitionEpochs) ClusterChecks {
	cc := ClusterChecks{Source: source}
	parts := make([]int32, 0, len(before))
	for p := range before {
		parts = append(parts, p)
	}
	slices.Sort(parts)
	for _, p := range parts {
		b := before[p]
		a, ok := after[p]
		if !ok {
			cc.Problems = append(cc.Problems, fmt.Sprintf("partition %d missing after the window", p))
			continue
		}
		if a.LeaderEpoch != b.LeaderEpoch {
			cc.LeaderEpochChanges++
			cc.Problems = append(cc.Problems, fmt.Sprintf("partition %d leader epoch %d -> %d", p, b.LeaderEpoch, a.LeaderEpoch))
		}
		changed := a.PartitionEpoch != b.PartitionEpoch
		if b.PartitionEpoch < 0 { // admin describe: compare ISR sets instead
			changed = !slices.Equal(a.ISR, b.ISR)
		}
		if changed {
			cc.PartitionEpochChanges++
			cc.Problems = append(cc.Problems, fmt.Sprintf("partition %d ISR/partition epoch changed (ISR %v -> %v)", p, b.ISR, a.ISR))
		}
		if rf := b.Replicas; len(b.ISR) < rf || len(a.ISR) < rf {
			cc.ShrunkISR++
			cc.Problems = append(cc.Problems, fmt.Sprintf("partition %d ISR below RF %d (before %v, after %v)", p, rf, b.ISR, a.ISR))
		}
	}
	return cc
}

// valid reports whether the checks passed.
func (cc ClusterChecks) valid() bool { return len(cc.Problems) == 0 }
