package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/storage"
)

func writeYAML(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "streamq.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestDefault(t *testing.T) {
	c := Default()
	require.NoError(t, c.Validate())

	want := Config{
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
			SegmentBytes:      67108864,
			SegmentMs:         604800000,
			RetentionMs:       604800000,
			RetentionBytes:    -1,
			MaxMessageBytes:   1048576,
			FlushPolicy:       "interval",
			FlushMs:           1000,
		},
		ReplicaLagMaxMs:              10000,
		BrokerHeartbeatIntervalMs:    2000,
		BrokerSessionTimeoutMs:       9000,
		GroupSessionTimeoutMs:        10000,
		GroupRebalanceTimeoutMs:      30000,
		GroupInitialRebalanceDelayMs: 3000,
		RaftHeartbeatTimeoutMs:       1000,
		RaftElectionTimeoutMs:        1000,
		RaftSnapshotThreshold:        8192,
		FileDeleteDelayMs:            60000,
		ProducerIDExpirationMs:       604800000,
	}
	require.Equal(t, want, c)
}

func TestLoadPrecedence(t *testing.T) {
	file := writeYAML(t, `
broker_id: 2
data_dir: /data/file
advertised_host: filehost
topic_defaults:
  partitions: 5
  flush_policy: always
cluster:
  - {id: 2, raft: "b2:9094", internal: "b2:9093", client: "b2:9092"}
`)
	tests := []struct {
		name  string
		path  string
		env   []string
		flags map[string]string
		check func(t *testing.T, c Config)
	}{
		{
			name: "defaults only",
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, Default(), c)
			},
		},
		{
			name: "file only",
			path: file,
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, int32(2), c.BrokerID)
				require.Equal(t, "/data/file", c.DataDir)
				require.Equal(t, "filehost", c.AdvertisedHost)
				require.Equal(t, int32(5), c.TopicDefaults.Partitions)
				require.Equal(t, "always", c.TopicDefaults.FlushPolicy)
				require.Equal(t, int32(3), c.TopicDefaults.ReplicationFactor, "unset nested keys keep defaults")
				require.Equal(t, ":9092", c.ListenAddr, "unset keys keep defaults")
				require.Equal(t, []ClusterMember{{ID: 2, Raft: "b2:9094", Internal: "b2:9093", Client: "b2:9092"}}, c.Cluster)
			},
		},
		{
			name: "env over file",
			path: file,
			env: []string{
				"PATH=/usr/bin",
				"STREAMQ_DATA_DIR=/data/env",
				"STREAMQ_TOPIC_DEFAULTS_PARTITIONS=7",
				"STREAMQ_TOPIC_DEFAULTS_SEGMENT_BYTES=1048576",
				"STREAMQ_UNRELATED=ignored",
			},
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, "/data/env", c.DataDir)
				require.Equal(t, int32(7), c.TopicDefaults.Partitions)
				require.Equal(t, int64(1048576), c.TopicDefaults.SegmentBytes)
				require.Equal(t, int32(2), c.BrokerID)
			},
		},
		{
			name: "flag over env",
			path: file,
			env:  []string{"STREAMQ_DATA_DIR=/data/env", "STREAMQ_TOPIC_DEFAULTS_PARTITIONS=7"},
			flags: map[string]string{
				"data_dir":                  "/data/flag",
				"topic_defaults.partitions": "9",
				"listen_addr":               "127.0.0.1:0",
			},
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, "/data/flag", c.DataDir)
				require.Equal(t, int32(9), c.TopicDefaults.Partitions)
				require.Equal(t, "127.0.0.1:0", c.ListenAddr)
			},
		},
		{
			name: "env without file",
			env:  []string{"STREAMQ_BROKER_ID=4", "STREAMQ_TOPIC_DEFAULTS_FLUSH_POLICY=never"},
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, int32(4), c.BrokerID)
				require.Equal(t, "never", c.TopicDefaults.FlushPolicy)
			},
		},
		{
			name: "nested flags",
			flags: map[string]string{
				"topic_defaults.replication_factor":  "1",
				"topic_defaults.min_insync_replicas": "1",
				"broker_session_timeout_ms":          "12000",
			},
			check: func(t *testing.T, c Config) {
				t.Helper()
				require.Equal(t, int32(1), c.TopicDefaults.ReplicationFactor)
				require.Equal(t, int32(1), c.TopicDefaults.MinInsyncReplicas)
				require.Equal(t, int64(12000), c.BrokerSessionTimeoutMs)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := Load(tt.path, tt.env, tt.flags)
			require.NoError(t, err)
			tt.check(t, c)
		})
	}
}

func TestLoadEmptyFile(t *testing.T) {
	c, err := Load(writeYAML(t, ""), nil, nil)
	require.NoError(t, err)
	require.Equal(t, Default(), c)
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"), nil, nil)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		env     []string
		flags   map[string]string
		wantMsg string
	}{
		{name: "unknown top-level yaml key", yaml: "brokerid: 1\n", wantMsg: "brokerid"},
		{name: "unknown nested yaml key", yaml: "topic_defaults:\n  partition: 1\n", wantMsg: "partition"},
		{name: "bad yaml int", yaml: "broker_id: one\n", wantMsg: "line 1: cannot unmarshal"},
		{name: "unknown flag key", flags: map[string]string{"topic_defaults.bogus": "1"}, wantMsg: `"topic_defaults.bogus"`},
		{name: "cluster flag rejected", flags: map[string]string{"cluster": "x"}, wantMsg: `"cluster"`},
		{name: "bad flag int", flags: map[string]string{"broker_id": "abc"}, wantMsg: "broker_id"},
		{name: "flag int32 overflow", flags: map[string]string{"topic_defaults.partitions": "4294967296"}, wantMsg: "topic_defaults.partitions"},
		{name: "bad env int", env: []string{"STREAMQ_TOPIC_DEFAULTS_SEGMENT_MS=soon"}, wantMsg: "STREAMQ_TOPIC_DEFAULTS_SEGMENT_MS"},
		{name: "validation runs after overrides", flags: map[string]string{"broker_id": "0"}, wantMsg: "broker_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := ""
			if tt.yaml != "" {
				path = writeYAML(t, tt.yaml)
			}
			_, err := Load(path, tt.env, tt.flags)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}

func TestValidate(t *testing.T) {
	member := func(id int32, host string) ClusterMember {
		return ClusterMember{ID: id, Raft: host + ":9094", Internal: host + ":9093", Client: host + ":9092"}
	}
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantMsg string
	}{
		{"broker_id zero", func(c *Config) { c.BrokerID = 0 }, "broker_id"},
		{"broker_id negative", func(c *Config) { c.BrokerID = -1 }, "broker_id"},
		{"listen_addr no port", func(c *Config) { c.ListenAddr = "localhost" }, "listen_addr"},
		{"listen_addr bad port", func(c *Config) { c.ListenAddr = ":99999" }, "listen_addr"},
		{"internal_addr", func(c *Config) { c.InternalAddr = "" }, "internal_addr"},
		{"raft_addr", func(c *Config) { c.RaftAddr = "host:port" }, "raft_addr"},
		{"metrics_addr", func(c *Config) { c.MetricsAddr = "nope" }, "metrics_addr"},
		{"advertised_host", func(c *Config) { c.AdvertisedHost = "" }, "advertised_host"},
		{"data_dir", func(c *Config) { c.DataDir = "" }, "data_dir"},
		{"partitions", func(c *Config) { c.TopicDefaults.Partitions = 0 }, "topic_defaults.partitions"},
		{"replication_factor", func(c *Config) { c.TopicDefaults.ReplicationFactor = 0 }, "topic_defaults.replication_factor"},
		{"min_insync zero", func(c *Config) { c.TopicDefaults.MinInsyncReplicas = 0 }, "topic_defaults.min_insync_replicas"},
		{"min_insync above rf", func(c *Config) { c.TopicDefaults.MinInsyncReplicas = 4 }, "topic_defaults.min_insync_replicas"},
		{"segment_bytes above 1GiB", func(c *Config) { c.TopicDefaults.SegmentBytes = storage.MaxSegmentBytes + 1 }, "segment_bytes"},
		{"segment_ms", func(c *Config) { c.TopicDefaults.SegmentMs = 0 }, "segment_ms"},
		{"retention_ms", func(c *Config) { c.TopicDefaults.RetentionMs = 0 }, "retention_ms"},
		{"retention_bytes", func(c *Config) { c.TopicDefaults.RetentionBytes = -2 }, "retention_bytes"},
		{"max_message_bytes", func(c *Config) { c.TopicDefaults.MaxMessageBytes = 1 }, "max_message_bytes"},
		{"flush_policy", func(c *Config) { c.TopicDefaults.FlushPolicy = "sometimes" }, "flush_policy"},
		{"flush_ms", func(c *Config) { c.TopicDefaults.FlushMs = 0 }, "flush_ms"},
		{"replica_lag_max_ms", func(c *Config) { c.ReplicaLagMaxMs = 0 }, "replica_lag_max_ms"},
		{"broker_heartbeat_interval_ms", func(c *Config) { c.BrokerHeartbeatIntervalMs = 0 }, "broker_heartbeat_interval_ms"},
		{"broker_session_timeout_ms", func(c *Config) { c.BrokerSessionTimeoutMs = -1 }, "broker_session_timeout_ms"},
		{"heartbeat not below session", func(c *Config) { c.BrokerHeartbeatIntervalMs = c.BrokerSessionTimeoutMs }, "must be < broker_session_timeout_ms"},
		{"group_session_timeout_ms", func(c *Config) { c.GroupSessionTimeoutMs = 0 }, "group_session_timeout_ms"},
		{"group_rebalance_timeout_ms", func(c *Config) { c.GroupRebalanceTimeoutMs = 0 }, "group_rebalance_timeout_ms"},
		{"group_initial_rebalance_delay_ms", func(c *Config) { c.GroupInitialRebalanceDelayMs = -1 }, "group_initial_rebalance_delay_ms"},
		{"raft election below heartbeat", func(c *Config) { c.RaftElectionTimeoutMs = 10 }, "raft_heartbeat_timeout_ms"},
		{"raft snapshot threshold", func(c *Config) { c.RaftSnapshotThreshold = 0 }, "raft_snapshot_threshold"},
		{"producer_id_expiration_ms", func(c *Config) { c.ProducerIDExpirationMs = 0 }, "producer_id_expiration_ms"},
		{"file_delete_delay_ms", func(c *Config) { c.FileDeleteDelayMs = -1 }, "file_delete_delay_ms"},
		{"cluster id zero", func(c *Config) { c.Cluster = []ClusterMember{member(1, "a"), member(0, "b")} }, "cluster[1].id"},
		{"cluster duplicate id", func(c *Config) { c.Cluster = []ClusterMember{member(1, "a"), member(1, "b")} }, "cluster[1].id 1 is duplicated"},
		{"cluster raft addr", func(c *Config) {
			m := member(1, "a")
			m.Raft = "a"
			c.Cluster = []ClusterMember{m}
		}, "cluster[0].raft"},
		{"cluster internal addr", func(c *Config) {
			m := member(1, "a")
			m.Internal = ""
			c.Cluster = []ClusterMember{m}
		}, "cluster[0].internal"},
		{"cluster client addr", func(c *Config) {
			m := member(1, "a")
			m.Client = "a:b"
			c.Cluster = []ClusterMember{m}
		}, "cluster[0].client"},
		{"cluster missing self", func(c *Config) { c.Cluster = []ClusterMember{member(2, "b"), member(3, "c")} }, "cluster does not contain broker_id 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			tt.mutate(&c)
			err := c.Validate()
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}

func TestValidateAccepts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *Config)
	}{
		{"port zero", func(c *Config) { c.ListenAddr = "127.0.0.1:0" }},
		{"empty metrics addr", func(c *Config) { c.MetricsAddr = "" }},
		{"file delete delay zero", func(c *Config) { c.FileDeleteDelayMs = 0 }},
		{"single node rf 1", func(c *Config) {
			c.TopicDefaults.ReplicationFactor = 1
			c.TopicDefaults.MinInsyncReplicas = 1
		}},
		{"valid cluster", func(c *Config) {
			c.Cluster = []ClusterMember{
				{ID: 1, Raft: "b1:9094", Internal: "b1:9093", Client: "b1:9092"},
				{ID: 2, Raft: "b2:9094", Internal: "b2:9093", Client: "b2:9092"},
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			tt.mutate(&c)
			require.NoError(t, c.Validate())
		})
	}
}

func TestValidateReportsEveryProblem(t *testing.T) {
	c := Default()
	c.BrokerID = 0
	c.DataDir = ""
	c.TopicDefaults.Partitions = 0
	err := c.Validate()
	require.ErrorIs(t, err, ErrInvalidConfig)
	for _, field := range []string{"broker_id", "data_dir", "topic_defaults.partitions"} {
		require.ErrorContains(t, err, field)
	}
}

func TestFlagKeys(t *testing.T) {
	want := []string{
		"advertised_host",
		"broker_heartbeat_interval_ms",
		"broker_id",
		"broker_session_timeout_ms",
		"data_dir",
		"file_delete_delay_ms",
		"group_initial_rebalance_delay_ms",
		"group_rebalance_timeout_ms",
		"group_session_timeout_ms",
		"internal_addr",
		"listen_addr",
		"metrics_addr",
		"producer_id_expiration_ms",
		"raft_addr",
		"raft_election_timeout_ms",
		"raft_heartbeat_timeout_ms",
		"raft_snapshot_threshold",
		"replica_lag_max_ms",
		"topic_defaults.flush_ms",
		"topic_defaults.flush_policy",
		"topic_defaults.max_message_bytes",
		"topic_defaults.min_insync_replicas",
		"topic_defaults.partitions",
		"topic_defaults.replication_factor",
		"topic_defaults.retention_bytes",
		"topic_defaults.retention_ms",
		"topic_defaults.segment_bytes",
		"topic_defaults.segment_ms",
	}
	require.Equal(t, want, FlagKeys())

	// Every flag key is accepted by Load as a flag; a bad value names the key.
	for _, k := range FlagKeys() {
		_, err := Load("", nil, map[string]string{k: "x"})
		if err != nil {
			require.ErrorIs(t, err, ErrInvalidConfig, k)
			require.NotContains(t, err.Error(), "unknown flag key", k)
		}
	}
}

func TestEnvKeysMirrorFlagKeys(t *testing.T) {
	tests := []struct {
		env  string
		flag string
	}{
		{"STREAMQ_BROKER_ID", "broker_id"},
		{"STREAMQ_TOPIC_DEFAULTS_PARTITIONS", "topic_defaults.partitions"},
		{"STREAMQ_TOPIC_DEFAULTS_MIN_INSYNC_REPLICAS", "topic_defaults.min_insync_replicas"},
		{"STREAMQ_PRODUCER_ID_EXPIRATION_MS", "producer_id_expiration_ms"},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			_, err := Load("", []string{tt.env + "=x"}, nil)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.env)
			require.ErrorContains(t, err, tt.flag)
		})
	}
}

func TestTopicConfigKeys(t *testing.T) {
	require.Equal(t, []string{
		"flush_ms",
		"flush_policy",
		"max_message_bytes",
		"min_insync_replicas",
		"retention_bytes",
		"retention_ms",
		"segment_bytes",
		"segment_ms",
	}, TopicConfigKeys())
}

func TestResolveTopicConfig(t *testing.T) {
	defaults := Default().TopicDefaults
	base := storage.DefaultLogConfig()

	t.Run("no overrides", func(t *testing.T) {
		tc, err := ResolveTopicConfig(defaults, nil)
		require.NoError(t, err)
		require.Equal(t, TopicConfig{Log: base, MinInsyncReplicas: 2}, tc)
	})

	tests := []struct {
		key, value string
		check      func(t *testing.T, tc TopicConfig)
	}{
		{"flush_ms", "250", func(t *testing.T, tc TopicConfig) { t.Helper(); require.Equal(t, int64(250), tc.Log.FlushMs) }},
		{"flush_policy", "always", func(t *testing.T, tc TopicConfig) {
			t.Helper()
			require.Equal(t, storage.FlushAlways, tc.Log.FlushPolicy)
		}},
		{"flush_policy", "never", func(t *testing.T, tc TopicConfig) {
			t.Helper()
			require.Equal(t, storage.FlushNever, tc.Log.FlushPolicy)
		}},
		{"max_message_bytes", "2048", func(t *testing.T, tc TopicConfig) { t.Helper(); require.Equal(t, 2048, tc.Log.MaxMessageBytes) }},
		{"min_insync_replicas", "5", func(t *testing.T, tc TopicConfig) {
			t.Helper()
			require.Equal(t, int32(5), tc.MinInsyncReplicas, "not checked against RF (D16)")
		}},
		{"retention_bytes", "1000000", func(t *testing.T, tc TopicConfig) {
			t.Helper()
			require.Equal(t, int64(1000000), tc.Log.RetentionBytes)
		}},
		{"retention_ms", "-1", func(t *testing.T, tc TopicConfig) { t.Helper(); require.Equal(t, int64(-1), tc.Log.RetentionMs) }},
		{"segment_bytes", "1073741824", func(t *testing.T, tc TopicConfig) {
			t.Helper()
			require.Equal(t, int64(storage.MaxSegmentBytes), tc.Log.SegmentBytes)
		}},
		{"segment_ms", "60000", func(t *testing.T, tc TopicConfig) { t.Helper(); require.Equal(t, int64(60000), tc.Log.SegmentMs) }},
	}
	for _, tt := range tests {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			tc, err := ResolveTopicConfig(defaults, map[string]string{tt.key: tt.value})
			require.NoError(t, err)
			require.Equal(t, base.IndexIntervalBytes, tc.Log.IndexIntervalBytes)
			require.Equal(t, base.MaxIndexBytes, tc.Log.MaxIndexBytes)
			tt.check(t, tc)
		})
	}

	t.Run("defaults are not mutated", func(t *testing.T) {
		d := Default().TopicDefaults
		_, err := ResolveTopicConfig(d, map[string]string{"segment_ms": "1"})
		require.NoError(t, err)
		require.Equal(t, Default().TopicDefaults, d)
	})
}

func TestResolveTopicConfigErrors(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		wantMsg   string
	}{
		{"unknown key", map[string]string{"cleanup.policy": "compact"}, `"cleanup.policy"`},
		{"partitions not a topic config", map[string]string{"partitions": "3"}, `"partitions"`},
		{"replication_factor not a topic config", map[string]string{"replication_factor": "3"}, `"replication_factor"`},
		{"bad int", map[string]string{"retention_ms": "forever"}, "retention_ms"},
		{"int32 overflow", map[string]string{"max_message_bytes": "3000000000"}, "max_message_bytes"},
		{"bad flush policy", map[string]string{"flush_policy": "sometimes"}, "flush_policy"},
		{"segment_bytes above 1GiB", map[string]string{"segment_bytes": "1073741825"}, "segment_bytes"},
		{"max_message_bytes above segment_bytes", map[string]string{"segment_bytes": "1024", "max_message_bytes": "2048"}, "max_message_bytes"},
		{"retention_bytes zero", map[string]string{"retention_bytes": "0"}, "retention_bytes"},
		{"min_insync zero", map[string]string{"min_insync_replicas": "0"}, "min_insync_replicas"},
		{"flush_ms zero with interval", map[string]string{"flush_ms": "0"}, "flush_ms"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ResolveTopicConfig(Default().TopicDefaults, tt.overrides)
			require.ErrorIs(t, err, ErrInvalidConfig)
			require.ErrorContains(t, err, tt.wantMsg)
		})
	}
}
