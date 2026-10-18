package loadgen

import (
	"bytes"
	"flag"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/pkg/client"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		check func(t *testing.T, c Config)
	}{
		{"defaults", nil, func(t *testing.T, c Config) {
			t.Helper()
			want := DefaultConfig()
			want.RF, want.MinInsync = 1, 1
			require.Equal(t, want, c)
			require.True(t, c.InProcess())
			require.Equal(t, 1, c.Brokers())
		}},
		{"quick", []string{"-preset", "quick"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, Config{
				Preset: "quick", Cluster: ClusterInProcess1, Topic: "bench", Partitions: 6, RF: 1, MinInsync: 1,
				Acks: AcksLeader, Idempotent: true, RecordSize: 1024, Producers: 4, Linger: 5 * time.Millisecond,
				BatchSize: 16 << 10, Duration: 10 * time.Second, Warmup: 2 * time.Second,
			}, c)
			require.False(t, c.IdempotenceEnabled(), "idempotence applies with acks=all only")
		}},
		{"nfr1", []string{"-preset", "nfr1"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, Config{
				Preset: "nfr1", Cluster: ClusterInProcess3, Topic: "bench", Partitions: 6, RF: 3, MinInsync: 2,
				Acks: AcksAll, Idempotent: true, RecordSize: 1024, Producers: 8, Linger: 10 * time.Millisecond,
				BatchSize: 512 << 10, Duration: 30 * time.Second, Warmup: 5 * time.Second,
			}, c)
			require.True(t, c.IdempotenceEnabled())
			require.Equal(t, 3, c.Brokers())
		}},
		{"nfr1-nonidem", []string{"-preset", "nfr1-nonidem"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, ClusterInProcess3, c.Cluster)
			require.False(t, c.Idempotent)
			require.False(t, c.IdempotenceEnabled())
			require.True(t, c.ProducerConfig().DisableIdempotence)
			require.Equal(t, 8, c.Producers)
		}},
		{"nfr2", []string{"-preset", "nfr2"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, 5000, c.Rate)
			require.Equal(t, time.Millisecond, c.Linger)
			require.Equal(t, AcksLeader, c.Acks)
			require.Equal(t, 1, c.RF)
			require.Equal(t, 30*time.Second, c.Duration)
		}},
		{"explicit flags override the preset", []string{"-duration", "5s", "-preset", "nfr1", "-warmup", "1s", "-producers", "2"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, 5*time.Second, c.Duration)
			require.Equal(t, time.Second, c.Warmup)
			require.Equal(t, 2, c.Producers)
			require.Equal(t, 512<<10, c.BatchSize, "unset flags keep the preset")
		}},
		{"rf and min.insync follow the cluster", []string{"-cluster", "inprocess:3"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, 3, c.RF)
			require.Equal(t, 2, c.MinInsync)
		}},
		{"explicit rf 1 on three brokers", []string{"-cluster", "inprocess:3", "-rf", "1"}, func(t *testing.T, c Config) {
			t.Helper()
			require.Equal(t, 1, c.RF)
			require.Equal(t, 1, c.MinInsync)
		}},
		{"bootstrap replaces cluster", []string{"-bootstrap", "a:1,b:2"}, func(t *testing.T, c Config) {
			t.Helper()
			require.False(t, c.InProcess())
			require.Equal(t, 0, c.Brokers())
			require.Equal(t, 3, c.RF)
			require.Equal(t, 2, c.MinInsync)
		}},
		{"acks none", []string{"-acks", "none", "-rate", "100", "-consume", "-record-size", "16"}, func(t *testing.T, c Config) {
			t.Helper()
			acks, err := c.ClientAcks()
			require.NoError(t, err)
			require.Equal(t, client.AcksNone, acks)
			require.Equal(t, "send latency (acks=none, no broker ack) from intended send time", latencyKind(c))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Parse(tc.args, io.Discard)
			require.NoError(t, err)
			tc.check(t, c)
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown preset", []string{"-preset", "bogus"}},
		{"unknown flag", []string{"-bogus"}},
		{"positional args", []string{"extra"}},
		{"bad cluster", []string{"-cluster", "inprocess:2"}},
		{"bad acks", []string{"-acks", "1"}},
		{"rf above brokers", []string{"-rf", "3"}},
		{"min.insync above rf", []string{"-min-insync", "2"}},
		{"consume needs 16 bytes", []string{"-consume", "-record-size", "8"}},
		{"zero producers", []string{"-producers", "0"}},
		{"negative rate", []string{"-rate", "-1"}},
		{"zero duration", []string{"-duration", "0s"}},
		{"zero linger", []string{"-linger", "0s"}},
		{"negative warmup", []string{"-warmup", "-1s"}},
		{"zero partitions", []string{"-partitions", "0"}},
		{"empty topic", []string{"-topic", ""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.args, io.Discard)
			require.ErrorIs(t, err, ErrUsage)
		})
	}
}

func TestHelpShowsDiskEstimate(t *testing.T) {
	var out bytes.Buffer
	_, err := Parse([]string{"-help"}, &out)
	require.ErrorIs(t, err, flag.ErrHelp)
	for _, s := range []string{"-preset", "-rate", "Disk use", "-preset nfr1", "-preset quick", "rate × record size × RF"} {
		require.Contains(t, out.String(), s)
	}
}

func TestEstimatedDiskBytes(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want int64
	}{
		{"paced", Config{Rate: 1000, RecordSize: 1000, RF: 3, Duration: 10 * time.Second}, 1000 * 1000 * 3 * 10},
		{"max rate", Config{RecordSize: 100, RF: 1, Duration: time.Second, Warmup: time.Second}, assumedMaxRate * 100 * 2},
	}
	for _, tc := range tests {
		require.Equal(t, tc.want, tc.cfg.EstimatedDiskBytes(), tc.name)
	}
}

func TestProducerConfigExplicit(t *testing.T) {
	c, err := Parse([]string{"-preset", "nfr1"}, io.Discard)
	require.NoError(t, err)
	pc := c.ProducerConfig()
	require.Equal(t, client.ProducerConfig{
		Acks: client.AcksAll, Linger: 10 * time.Millisecond, BatchSize: 512 << 10, BufferMemory: 32 << 20,
		MaxRequestSize: 1 << 20, DeliveryTimeout: 2 * time.Minute, MaxBlock: time.Minute,
	}, pc)
	require.Equal(t, 30*time.Second, c.DrainTimeout())
}

func TestHumanBytes(t *testing.T) {
	require.Equal(t, "512 B", humanBytes(512))
	require.Equal(t, "1.0 KiB", humanBytes(1024))
	require.Equal(t, "1.5 MiB", humanBytes(3<<19))
	require.Equal(t, "10.0 GiB", humanBytes(10<<30))
}
