// Package config loads and validates broker configuration (SPEC §10).
//
// Precedence: flags > environment (STREAMQ_<UPPER_SNAKE_KEY>) > YAML file > defaults.
package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/sudo-ss1/streamq/internal/storage"
)

// ErrInvalidConfig wraps every validation failure.
var ErrInvalidConfig = errors.New("config: invalid")

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
func Default() Config {
	lc := storage.DefaultLogConfig()
	return Config{
		BrokerID:       1,
		ListenAddr:     ":9092",
		InternalAddr:   ":9093",
		RaftAddr:       ":9094",
		AdvertisedHost: "localhost",
		DataDir:        "/var/lib/streamq",
		MetricsAddr:    ":9100",
		TopicDefaults: TopicDefaults{
			Partitions:        3,
			ReplicationFactor: 3,
			MinInsyncReplicas: 2,
			SegmentBytes:      lc.SegmentBytes,
			SegmentMs:         lc.SegmentMs,
			RetentionMs:       lc.RetentionMs,
			RetentionBytes:    lc.RetentionBytes,
			MaxMessageBytes:   int32(lc.MaxMessageBytes),
			FlushPolicy:       string(lc.FlushPolicy),
			FlushMs:           lc.FlushMs,
		},
		ReplicaLagMaxMs:           10_000,
		BrokerHeartbeatIntervalMs: 2_000,
		BrokerSessionTimeoutMs:    9_000,
		GroupSessionTimeoutMs:     10_000,
		GroupRebalanceTimeoutMs:   30_000,
		FileDeleteDelayMs:         60_000,
		ProducerIDExpirationMs:    604_800_000,
	}
}

// Load builds a Config from defaults, then the YAML file at path (if non-empty; unknown keys are
// an error), then env (a list of "KEY=value" as from os.Environ; only STREAMQ_* keys are used),
// then flags (dotted yaml paths such as "topic_defaults.partitions" mapped to values), and
// validates the result. Env and flags set scalar keys only; "cluster" is file-only.
func Load(path string, env []string, flags map[string]string) (Config, error) {
	cfg := Default()
	if path != "" {
		if err := loadFile(path, &cfg); err != nil {
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg, env); err != nil {
		return Config{}, err
	}
	if err := applyFlags(&cfg, flags); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// loadFile decodes the YAML file at path over cfg, rejecting unknown keys.
func loadFile(path string, cfg *Config) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil // empty file: keep defaults
		}
		return fmt.Errorf("%w: file %s: %w", ErrInvalidConfig, path, err)
	}
	return nil
}
