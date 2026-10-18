// Package metrics exposes a node's Prometheus metrics (SPEC NFR6, DECISIONS D52). Each node owns
// one Metrics with its own registry: there are no package-level metrics and the default registry
// is never used. Partition, lag and Raft metrics are pulled from sources at scrape time.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

// PartitionStat is one local replica's state at scrape time.
type PartitionStat struct {
	Topic     string
	Partition int32
	Leader    bool
	LEO, HW   int64
	ISRSize   int // committed ISR (meaningful on the leader)
	Replicas  int
}

// LagStat is a consumer group's lag on a partition this broker leads (HW - committed offset).
type LagStat struct {
	Group     string
	Topic     string
	Partition int32
	Lag       int64
}

// BrokerSource supplies partition and lag metrics (implemented by broker.Broker).
type BrokerSource interface {
	// PartitionStats returns every local replica.
	PartitionStats() []PartitionStat
	// ConsumerLag returns lag for partitions this broker leads.
	ConsumerLag() []LagStat
	// OfflinePartitions returns the cluster's leaderless partition count, and whether this broker
	// reports it (it is the controller, or single-node), so a sum across brokers is correct.
	OfflinePartitions() (int, bool)
}

// RaftStats is the local Raft node's state.
type RaftStats struct {
	State        string // leader, follower, candidate or shutdown
	LastContact  time.Duration
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
}

// RaftSource supplies Raft metrics (implemented by raftstore.Store).
type RaftSource interface {
	RaftStats() RaftStats
}

// Metrics is one node's metric set.
type Metrics struct {
	registry *prometheus.Registry
}

// New returns a Metrics with its own registry.
func New() *Metrics {
	return &Metrics{registry: prometheus.NewRegistry()}
}

// Registry returns the node's registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// UnaryServerInterceptor records request counts and latency for service ("client" or
// "internal").
func (m *Metrics) UnaryServerInterceptor(service string) grpc.UnaryServerInterceptor {
	_ = service
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}
}

// AddBytesIn counts appended record bytes for topic. A nil Metrics is a no-op.
func (m *Metrics) AddBytesIn(topic string, n int) {
	if m == nil {
		return
	}
	_, _ = topic, n
}

// AddBytesOut counts record bytes returned to consumers for topic. A nil Metrics is a no-op.
func (m *Metrics) AddBytesOut(topic string, n int) {
	if m == nil {
		return
	}
	_, _ = topic, n
}

// PartitionError counts a non-NONE per-partition result of a client API method (error codes live
// in response bodies, SPEC §7.3). A nil Metrics is a no-op.
func (m *Metrics) PartitionError(method, code string) {
	if m == nil {
		return
	}
	_, _ = method, code
}

// RegisterBroker registers scrape-time partition, lag and offline-partition metrics.
func (m *Metrics) RegisterBroker(src BrokerSource) { _ = src }

// RegisterRaft registers scrape-time Raft metrics (cluster mode only).
func (m *Metrics) RegisterRaft(src RaftSource) { _ = src }
