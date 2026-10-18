package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/sudo-ss1/streamq/internal/storage"
)

// Validate reports every problem, joined and wrapped in ErrInvalidConfig.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.BrokerID <= 0 {
		add("broker_id %d must be > 0", c.BrokerID)
	}
	for _, a := range []struct{ name, addr string }{
		{"listen_addr", c.ListenAddr},
		{"internal_addr", c.InternalAddr},
		{"raft_addr", c.RaftAddr},
	} {
		if err := checkAddr(a.addr); err != nil {
			add("%s: %w", a.name, err)
		}
	}
	if c.MetricsAddr != "" {
		if err := checkAddr(c.MetricsAddr); err != nil {
			add("metrics_addr: %w", err)
		}
	}
	if c.AdvertisedHost == "" {
		add("advertised_host must not be empty")
	}
	if c.DataDir == "" {
		add("data_dir must not be empty")
	}

	td := c.TopicDefaults
	if td.Partitions < 1 {
		add("topic_defaults.partitions %d must be >= 1", td.Partitions)
	}
	if td.ReplicationFactor < 1 {
		add("topic_defaults.replication_factor %d must be >= 1", td.ReplicationFactor)
	}
	if td.MinInsyncReplicas < 1 || td.MinInsyncReplicas > td.ReplicationFactor {
		add("topic_defaults.min_insync_replicas %d must be in [1, replication_factor %d]",
			td.MinInsyncReplicas, td.ReplicationFactor)
	}
	if err := logConfig(td).Validate(); err != nil {
		add("topic_defaults: %w", err)
	}

	for _, t := range []struct {
		name string
		ms   int64
	}{
		{"replica_lag_max_ms", c.ReplicaLagMaxMs},
		{"broker_heartbeat_interval_ms", c.BrokerHeartbeatIntervalMs},
		{"broker_session_timeout_ms", c.BrokerSessionTimeoutMs},
		{"group_session_timeout_ms", c.GroupSessionTimeoutMs},
		{"group_rebalance_timeout_ms", c.GroupRebalanceTimeoutMs},
		{"producer_id_expiration_ms", c.ProducerIDExpirationMs},
	} {
		if t.ms <= 0 {
			add("%s %d must be > 0", t.name, t.ms)
		}
	}
	if c.BrokerHeartbeatIntervalMs >= c.BrokerSessionTimeoutMs {
		add("broker_heartbeat_interval_ms %d must be < broker_session_timeout_ms %d",
			c.BrokerHeartbeatIntervalMs, c.BrokerSessionTimeoutMs)
	}
	if c.FileDeleteDelayMs < 0 {
		add("file_delete_delay_ms %d must be >= 0", c.FileDeleteDelayMs)
	}
	if c.RaftHeartbeatTimeoutMs <= 0 || c.RaftElectionTimeoutMs < c.RaftHeartbeatTimeoutMs {
		add("raft_heartbeat_timeout_ms %d must be > 0 and <= raft_election_timeout_ms %d",
			c.RaftHeartbeatTimeoutMs, c.RaftElectionTimeoutMs)
	}
	if c.RaftSnapshotThreshold <= 0 {
		add("raft_snapshot_threshold %d must be > 0", c.RaftSnapshotThreshold)
	}
	if c.GroupInitialRebalanceDelayMs < 0 || c.GroupInitialRebalanceDelayMs > c.GroupRebalanceTimeoutMs {
		add("group_initial_rebalance_delay_ms %d must be in [0, group_rebalance_timeout_ms]", c.GroupInitialRebalanceDelayMs)
	}

	errs = append(errs, c.validateCluster()...)

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return nil
}

// validateCluster checks the static member list, if any.
func (c Config) validateCluster() []error {
	if len(c.Cluster) == 0 {
		return nil
	}
	var errs []error
	seen := make(map[int32]bool, len(c.Cluster))
	self := false
	for i, m := range c.Cluster {
		if m.ID <= 0 {
			errs = append(errs, fmt.Errorf("cluster[%d].id %d must be > 0", i, m.ID))
		}
		if seen[m.ID] {
			errs = append(errs, fmt.Errorf("cluster[%d].id %d is duplicated", i, m.ID))
		}
		seen[m.ID] = true
		if m.ID == c.BrokerID {
			self = true
		}
		for _, a := range []struct{ name, addr string }{
			{"raft", m.Raft}, {"internal", m.Internal}, {"client", m.Client},
		} {
			if err := checkAddr(a.addr); err != nil {
				errs = append(errs, fmt.Errorf("cluster[%d].%s: %w", i, a.name, err))
			}
		}
	}
	if !self {
		errs = append(errs, fmt.Errorf("cluster does not contain broker_id %d", c.BrokerID))
	}
	return errs
}

// checkAddr requires host:port with a numeric port in [0, 65535]; the host may be empty.
func checkAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return fmt.Errorf("invalid port in address %q: %w", addr, err)
	}
	return nil
}

// logConfig converts topic defaults to a storage.LogConfig; index sizing comes from
// storage.DefaultLogConfig.
func logConfig(td TopicDefaults) storage.LogConfig {
	lc := storage.DefaultLogConfig()
	lc.SegmentBytes = td.SegmentBytes
	lc.SegmentMs = td.SegmentMs
	lc.RetentionMs = td.RetentionMs
	lc.RetentionBytes = td.RetentionBytes
	lc.MaxMessageBytes = int(td.MaxMessageBytes)
	lc.FlushPolicy = storage.FlushPolicy(td.FlushPolicy)
	lc.FlushMs = td.FlushMs
	return lc
}
