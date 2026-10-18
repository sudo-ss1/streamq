// Package config loads and validates broker configuration (SPEC §10).
//
// Precedence: flags > environment (STREAMQ_<UPPER_SNAKE_KEY>) > YAML file > defaults.
package config

import (
	"errors"

	"gopkg.in/yaml.v3"

	"github.com/sudo-ss1/streamq/internal/storage"
)

// ErrInvalidConfig wraps every validation failure.
var ErrInvalidConfig = errors.New("config: invalid")

var errUnimplemented = errors.New("config: unimplemented")

// ClusterMember is one static cluster member (identical list on every broker).
type ClusterMember struct {
	ID       int32  `yaml:"id"`
	Raft     string `yaml:"raft"`
	Internal string `yaml:"internal"`
	Client   string `yaml:"client"`
}

// TopicDefaults are the broker-wide topic defaults; topic configs override them per topic.
type TopicDefaults struct {
	Partitions        int32  `yaml:"partitions"`
	ReplicationFactor int32  `yaml:"replication_factor"`
	MinInsyncReplicas int32  `yaml:"min_insync_replicas"`
	SegmentBytes      int64  `yaml:"segment_bytes"`
	SegmentMs         int64  `yaml:"segment_ms"`
	RetentionMs       int64  `yaml:"retention_ms"`
	RetentionBytes    int64  `yaml:"retention_bytes"`
	MaxMessageBytes   int32  `yaml:"max_message_bytes"`
	FlushPolicy       string `yaml:"flush_policy"`
	FlushMs           int64  `yaml:"flush_ms"`
}

// Config is the broker configuration (SPEC §10).
type Config struct {
	BrokerID                  int32           `yaml:"broker_id"`
	ListenAddr                string          `yaml:"listen_addr"`
	InternalAddr              string          `yaml:"internal_addr"`
	RaftAddr                  string          `yaml:"raft_addr"`
	AdvertisedHost            string          `yaml:"advertised_host"`
	Cluster                   []ClusterMember `yaml:"cluster"`
	DataDir                   string          `yaml:"data_dir"`
	MetricsAddr               string          `yaml:"metrics_addr"`
	TopicDefaults             TopicDefaults   `yaml:"topic_defaults"`
	ReplicaLagMaxMs           int64           `yaml:"replica_lag_max_ms"`
	BrokerHeartbeatIntervalMs int64           `yaml:"broker_heartbeat_interval_ms"`
	BrokerSessionTimeoutMs    int64           `yaml:"broker_session_timeout_ms"`
	GroupSessionTimeoutMs     int64           `yaml:"group_session_timeout_ms"`
	GroupRebalanceTimeoutMs   int64           `yaml:"group_rebalance_timeout_ms"`
	FileDeleteDelayMs         int64           `yaml:"file_delete_delay_ms"`
	ProducerIDExpirationMs    int64           `yaml:"producer_id_expiration_ms"`
}

// Default returns the SPEC §10 defaults.
func Default() Config { return Config{} }

// Load builds a Config from defaults, then the YAML file at path (if non-empty; unknown keys are
// an error), then env (a list of "KEY=value" as from os.Environ; only STREAMQ_* keys are used),
// then flags (dotted yaml paths such as "topic_defaults.partitions" mapped to values), and
// validates the result. Env and flags set scalar keys only; "cluster" is file-only.
func Load(path string, env []string, flags map[string]string) (Config, error) {
	_, _, _ = path, env, flags
	_ = yaml.Unmarshal
	return Config{}, errUnimplemented
}

// Validate reports every problem, joined and wrapped in ErrInvalidConfig.
func (c Config) Validate() error { return errUnimplemented }

// TopicConfig is the resolved configuration of one topic.
type TopicConfig struct {
	Log               storage.LogConfig
	MinInsyncReplicas int32
}

// TopicConfigKeys returns the keys accepted as topic config overrides (the SPEC §10
// topic_defaults keys other than partitions and replication_factor).
func TopicConfigKeys() []string { return nil }

// ResolveTopicConfig applies per-topic overrides (keys from TopicConfigKeys, values as decimal
// strings or flush policy names) over the defaults and validates the result. Unknown keys or bad
// values return an error wrapping ErrInvalidConfig.
func ResolveTopicConfig(defaults TopicDefaults, overrides map[string]string) (TopicConfig, error) {
	_, _ = defaults, overrides
	return TopicConfig{}, errUnimplemented
}
