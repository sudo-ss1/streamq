// Package metrics exposes a node's Prometheus metrics (SPEC NFR6, DECISIONS D52). Each node owns
// one Metrics with its own registry: there are no package-level metrics and the default registry
// is never used. Partition, lag and Raft metrics are pulled from sources at scrape time.
package metrics

import (
	"context"
	"net/http"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
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

	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	bytesIn         *prometheus.CounterVec
	bytesOut        *prometheus.CounterVec
	partitionErrors *prometheus.CounterVec
}

// latencyBuckets returns the request latency buckets: 0.5 ms to 60 s (D52; long-polls last up to
// 30 s).
func latencyBuckets() []float64 {
	return []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30, 60,
	}
}

// New returns a Metrics with its own registry, including the Go runtime and process collectors.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamq_requests_total",
			Help: "gRPC requests handled, by service, method and gRPC status code.",
		}, []string{"service", "method", "code"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streamq_request_duration_seconds",
			Help:    "gRPC request latency in seconds, by service and method.",
			Buckets: latencyBuckets(),
		}, []string{"service", "method"}),
		bytesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamq_bytes_in_total",
			Help: "Record bytes appended by Produce requests, by topic.",
		}, []string{"topic"}),
		bytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamq_bytes_out_total",
			Help: "Record bytes returned by Fetch responses, by topic.",
		}, []string{"topic"}),
		partitionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamq_partition_errors_total",
			Help: "Per-partition API error results, by method and error code.",
		}, []string{"method", "error_code"}),
	}
	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.requests, m.requestDuration, m.bytesIn, m.bytesOut, m.partitionErrors,
	)
	return m
}

// Registry returns the node's registry.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// UnaryServerInterceptor records request counts and latency for service ("client" or
// "internal"). The method label is the short RPC name (e.g. "Produce"); code is the gRPC status
// code of the handler's error.
func (m *Metrics) UnaryServerInterceptor(service string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		method := "unknown"
		if info != nil && info.FullMethod != "" {
			method = path.Base(info.FullMethod)
		}
		m.requests.WithLabelValues(service, method, status.Code(err).String()).Inc()
		m.requestDuration.WithLabelValues(service, method).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// AddBytesIn counts appended record bytes for topic. A nil Metrics is a no-op.
func (m *Metrics) AddBytesIn(topic string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesIn.WithLabelValues(topic).Add(float64(n))
}

// AddBytesOut counts record bytes returned to consumers for topic. A nil Metrics is a no-op.
func (m *Metrics) AddBytesOut(topic string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesOut.WithLabelValues(topic).Add(float64(n))
}

// PartitionError counts a non-NONE per-partition result of a client API method (error codes live
// in response bodies, SPEC §7.3). A nil Metrics is a no-op.
func (m *Metrics) PartitionError(method, code string) {
	if m == nil {
		return
	}
	m.partitionErrors.WithLabelValues(method, code).Inc()
}

// RegisterBroker registers scrape-time partition, lag and offline-partition metrics.
func (m *Metrics) RegisterBroker(src BrokerSource) {
	m.registry.MustRegister(newBrokerCollector(src))
}

// RegisterRaft registers scrape-time Raft metrics (cluster mode only).
func (m *Metrics) RegisterRaft(src RaftSource) {
	m.registry.MustRegister(newRaftCollector(src))
}
