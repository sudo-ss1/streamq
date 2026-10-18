package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// brokerCollector reads a BrokerSource at scrape time.
type brokerCollector struct {
	src BrokerSource

	leo, hw, isrSize, replicas *prometheus.Desc
	leader, underReplicated    *prometheus.Desc
	offline, consumerLag       *prometheus.Desc
}

func newBrokerCollector(src BrokerSource) *brokerCollector {
	tp := []string{"topic", "partition"}
	return &brokerCollector{
		src: src,
		leo: prometheus.NewDesc("streamq_partition_log_end_offset",
			"Log end offset of each local replica.", tp, nil),
		hw: prometheus.NewDesc("streamq_partition_high_watermark",
			"High watermark of each local replica.", tp, nil),
		isrSize: prometheus.NewDesc("streamq_partition_isr_size",
			"Committed ISR size of each partition this broker leads.", tp, nil),
		replicas: prometheus.NewDesc("streamq_partition_replicas",
			"Assigned replica count of each partition this broker leads.", tp, nil),
		leader: prometheus.NewDesc("streamq_partitions_leader",
			"Number of partitions this broker leads.", nil, nil),
		underReplicated: prometheus.NewDesc("streamq_partitions_under_replicated",
			"Partitions this broker leads whose ISR is smaller than the replica set.", nil, nil),
		offline: prometheus.NewDesc("streamq_partitions_offline",
			"Partitions without a leader (exported by the controller only).", nil, nil),
		consumerLag: prometheus.NewDesc("streamq_consumer_lag",
			"Consumer group lag (high watermark minus committed offset) on partitions this broker leads.",
			[]string{"group", "topic", "partition"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *brokerCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.leo, c.hw, c.isrSize, c.replicas, c.leader, c.underReplicated, c.offline, c.consumerLag,
	} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *brokerCollector) Collect(ch chan<- prometheus.Metric) {
	var led, under int
	for _, p := range c.src.PartitionStats() {
		part := strconv.FormatInt(int64(p.Partition), 10)
		ch <- prometheus.MustNewConstMetric(c.leo, prometheus.GaugeValue, float64(p.LEO), p.Topic, part)
		ch <- prometheus.MustNewConstMetric(c.hw, prometheus.GaugeValue, float64(p.HW), p.Topic, part)
		if !p.Leader {
			continue
		}
		led++
		if p.ISRSize < p.Replicas {
			under++
		}
		ch <- prometheus.MustNewConstMetric(c.isrSize, prometheus.GaugeValue, float64(p.ISRSize), p.Topic, part)
		ch <- prometheus.MustNewConstMetric(c.replicas, prometheus.GaugeValue, float64(p.Replicas), p.Topic, part)
	}
	ch <- prometheus.MustNewConstMetric(c.leader, prometheus.GaugeValue, float64(led))
	ch <- prometheus.MustNewConstMetric(c.underReplicated, prometheus.GaugeValue, float64(under))
	if n, ok := c.src.OfflinePartitions(); ok {
		ch <- prometheus.MustNewConstMetric(c.offline, prometheus.GaugeValue, float64(n))
	}
	for _, l := range c.src.ConsumerLag() {
		ch <- prometheus.MustNewConstMetric(c.consumerLag, prometheus.GaugeValue, float64(l.Lag),
			l.Group, l.Topic, strconv.FormatInt(int64(l.Partition), 10))
	}
}

// raftStates returns the values of the streamq_raft_state label.
func raftStates() []string { return []string{"leader", "follower", "candidate", "shutdown"} }

// raftCollector reads a RaftSource at scrape time.
type raftCollector struct {
	src RaftSource

	state, lastContact, term, commitIndex, appliedIndex *prometheus.Desc
}

func newRaftCollector(src RaftSource) *raftCollector {
	return &raftCollector{
		src: src,
		state: prometheus.NewDesc("streamq_raft_state",
			"Raft state of this node: 1 for the current state, 0 for the others.", []string{"state"}, nil),
		lastContact: prometheus.NewDesc("streamq_raft_last_contact_seconds",
			"Seconds since this node last heard from the Raft leader (0 on the leader).", nil, nil),
		term:         prometheus.NewDesc("streamq_raft_term", "Current Raft term.", nil, nil),
		commitIndex:  prometheus.NewDesc("streamq_raft_commit_index", "Raft commit index.", nil, nil),
		appliedIndex: prometheus.NewDesc("streamq_raft_applied_index", "Raft applied index.", nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *raftCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.state, c.lastContact, c.term, c.commitIndex, c.appliedIndex} {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *raftCollector) Collect(ch chan<- prometheus.Metric) {
	st := c.src.RaftStats()
	for _, s := range raftStates() {
		v := 0.0
		if s == st.State {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(c.state, prometheus.GaugeValue, v, s)
	}
	lastContact := st.LastContact.Seconds()
	if st.State == "leader" {
		lastContact = 0
	}
	ch <- prometheus.MustNewConstMetric(c.lastContact, prometheus.GaugeValue, lastContact)
	ch <- prometheus.MustNewConstMetric(c.term, prometheus.GaugeValue, float64(st.Term))
	ch <- prometheus.MustNewConstMetric(c.commitIndex, prometheus.GaugeValue, float64(st.CommitIndex))
	ch <- prometheus.MustNewConstMetric(c.appliedIndex, prometheus.GaugeValue, float64(st.AppliedIndex))
}
