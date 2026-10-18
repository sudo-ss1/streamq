package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/sudo-ss1/streamq/bench/internal/hist"
)

// Statuses.
const (
	StatusOK      = "OK"
	StatusInvalid = "INVALID"
)

// RateModel names how -rate is enforced (M10 plan R16).
const RateModel = "shared-token-bucket"

// Result is the full outcome of a run; its JSON form follows bench/USAGE.md.
type Result struct {
	Status         string          `json:"status"`
	InvalidReasons []string        `json:"invalid_reasons,omitempty"`
	Config         ConfigReport    `json:"config"`
	Environment    Environment     `json:"environment"`
	Produce        ProduceReport   `json:"produce"`
	EndToEnd       *EndToEndReport `json:"end_to_end,omitempty"`
	ClusterChecks  ClusterChecks   `json:"cluster_checks"`
	RateModel      string          `json:"rate_model"`
}

// ConfigReport is the effective workload and ProducerConfig.
type ConfigReport struct {
	Preset         string         `json:"preset,omitempty"`
	Cluster        string         `json:"cluster"`
	Brokers        int            `json:"brokers,omitempty"`
	Topic          string         `json:"topic"`
	Partitions     int            `json:"partitions"`
	RF             int            `json:"rf"`
	MinInsync      int            `json:"min_insync"`
	Producers      int            `json:"producers"`
	RecordSize     int            `json:"record_size"`
	KeyCardinality int            `json:"key_cardinality"`
	Rate           int            `json:"rate"`
	WarmupSeconds  float64        `json:"warmup_s"`
	DurationSec    float64        `json:"duration_s"`
	DrainTimeoutS  float64        `json:"drain_timeout_s"`
	Consume        bool           `json:"consume"`
	Producer       ProducerReport `json:"producer_config"`
}

// ProducerReport lists every effective ProducerConfig value (M10 plan R1).
type ProducerReport struct {
	Acks              string  `json:"acks"`
	Idempotence       bool    `json:"idempotence"`
	LingerMs          float64 `json:"linger_ms"`
	BatchSize         int     `json:"batch_size"`
	BufferMemory      int     `json:"buffer_memory"`
	MaxRequestSize    int     `json:"max_request_size"`
	DeliveryTimeoutMs int64   `json:"delivery_timeout_ms"`
	MaxBlockMs        int64   `json:"max_block_ms"`
}

// Percentiles are latency percentiles in milliseconds.
type Percentiles struct {
	P50  float64 `json:"p50"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p999"`
	Max  float64 `json:"max"`
}

// ProduceReport is the produce-side measurement.
type ProduceReport struct {
	Count          int64            `json:"count"` // records with a measured latency
	Sent           int64            `json:"sent"`  // records whose intended send time is in the window
	AckedInWindow  int64            `json:"acked_in_window"`
	Timeouts       int64            `json:"timeouts"` // still unacked after the drain
	ErrorsByKind   map[string]int64 `json:"errors_by_kind"`
	MsgsPerSec     float64          `json:"msgs_per_sec"`
	MiBPerSec      float64          `json:"mib_per_sec"`
	LatencyKind    string           `json:"latency_kind"`
	Latency        Percentiles      `json:"latency_ms"`
	Service        Percentiles      `json:"service_ms"`
	BehindSchedule int64            `json:"behind_schedule"`
	WindowSeconds  float64          `json:"window_s"`
	DrainSeconds   float64          `json:"drain_s"`
}

// EndToEndReport is the consumer-side measurement (-consume).
type EndToEndReport struct {
	Count   int64       `json:"count"`
	Latency Percentiles `json:"latency_ms"`
}

// percentiles converts a nanosecond snapshot to millisecond percentiles.
func percentiles(s hist.Snapshot) Percentiles {
	ms := func(ns int64) float64 { return float64(ns) / float64(time.Millisecond) }
	return Percentiles{P50: ms(s.Quantile(0.5)), P99: ms(s.Quantile(0.99)), P999: ms(s.Quantile(0.999)), Max: ms(s.Max())}
}

// configReport builds the config block.
func configReport(cfg Config, partitions int) ConfigReport {
	pc := cfg.ProducerConfig()
	cluster := cfg.Cluster
	if !cfg.InProcess() {
		cluster = "bootstrap:" + cfg.Bootstrap
	}
	return ConfigReport{
		Preset: cfg.Preset, Cluster: cluster, Brokers: cfg.Brokers(), Topic: cfg.Topic, Partitions: partitions,
		RF: cfg.RF, MinInsync: cfg.MinInsync, Producers: cfg.Producers, RecordSize: cfg.RecordSize,
		KeyCardinality: cfg.KeyCardinality, Rate: cfg.Rate, WarmupSeconds: cfg.Warmup.Seconds(),
		DurationSec: cfg.Duration.Seconds(), DrainTimeoutS: cfg.DrainTimeout().Seconds(), Consume: cfg.Consume,
		Producer: ProducerReport{
			Acks: cfg.Acks, Idempotence: cfg.IdempotenceEnabled(), LingerMs: float64(pc.Linger) / float64(time.Millisecond),
			BatchSize: pc.BatchSize, BufferMemory: pc.BufferMemory, MaxRequestSize: pc.MaxRequestSize,
			DeliveryTimeoutMs: pc.DeliveryTimeout.Milliseconds(), MaxBlockMs: pc.MaxBlock.Milliseconds(),
		},
	}
}

// latencyKind labels what the latency measures (M10 plan R3, R11).
func latencyKind(cfg Config) string {
	from := "Produce call"
	if cfg.Rate > 0 {
		from = "intended send time"
	}
	if cfg.Acks == AcksNone {
		return "send latency (acks=none, no broker ack) from " + from
	}
	return "ack latency from " + from
}

// produceReport builds the produce block from merged totals.
func produceReport(cfg Config, t Totals, window, drain time.Duration) ProduceReport {
	pr := ProduceReport{
		Count: int64(t.Latency.Count()), Sent: t.Sent, AckedInWindow: t.AckedInWindow, Timeouts: t.Timeouts,
		ErrorsByKind: maps.Clone(t.Errors), LatencyKind: latencyKind(cfg),
		Latency: percentiles(t.Latency), Service: percentiles(t.Service), BehindSchedule: t.Behind,
		WindowSeconds: window.Seconds(), DrainSeconds: drain.Seconds(),
	}
	if pr.ErrorsByKind == nil {
		pr.ErrorsByKind = map[string]int64{}
	}
	if s := window.Seconds(); s > 0 {
		pr.MsgsPerSec = float64(t.AckedInWindow) / s
		pr.MiBPerSec = float64(t.BytesInWindow) / s / (1 << 20)
	}
	return pr
}

// WriteJSON writes r to path with mode 0600.
func WriteJSON(path string, r *Result) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	if err := os.WriteFile(filepath.Clean(path), append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// PrintTable writes the human-readable report.
func PrintTable(w io.Writer, r *Result) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	c, pr, pc := r.Config, r.Produce, r.Config.Producer
	pct := func(x Percentiles) string {
		return fmt.Sprintf("p50 %.3f ms  p99 %.3f ms  p999 %.3f ms  max %.3f ms", x.P50, x.P99, x.P999, x.Max)
	}
	rate := "max"
	if c.Rate > 0 {
		rate = fmt.Sprintf("%d msgs/s total (%s)", c.Rate, r.RateModel)
	}
	keys := "unkeyed (sticky)"
	if c.KeyCardinality > 0 {
		keys = fmt.Sprintf("%d keys", c.KeyCardinality)
	}
	p("streamq-bench  status %s", r.Status)
	if c.Preset != "" {
		p("  preset %s", c.Preset)
	}
	p("\n")
	for _, reason := range r.InvalidReasons {
		p("  INVALID: %s\n", reason)
	}
	p("%-12s %s  topic %s  partitions %d  rf %d  min.insync %d\n", "cluster", c.Cluster, c.Topic, c.Partitions, c.RF, c.MinInsync)
	p("%-12s acks=%s idempotence=%t linger=%gms batch.size=%d buffer.memory=%d max.request.size=%d delivery.timeout=%dms max.block=%dms\n",
		"producer", pc.Acks, pc.Idempotence, pc.LingerMs, pc.BatchSize, pc.BufferMemory, pc.MaxRequestSize, pc.DeliveryTimeoutMs, pc.MaxBlockMs)
	p("%-12s producers=%d record.size=%dB %s rate=%s warmup=%gs duration=%gs consume=%t\n",
		"workload", c.Producers, c.RecordSize, keys, rate, c.WarmupSeconds, c.DurationSec, c.Consume)
	p("%-12s %.0f msgs/s  %.2f MiB/s  (acks inside the %.1fs window)\n", "throughput", pr.MsgsPerSec, pr.MiBPerSec, pr.WindowSeconds)
	p("%-12s %s  [%s]\n", "latency", pct(pr.Latency), pr.LatencyKind)
	p("%-12s %s  [from the Produce call]\n", "service", pct(pr.Service))
	p("%-12s measured %d  sent %d  timeouts %d  behind_schedule %d  drain %.2fs\n",
		"records", pr.Count, pr.Sent, pr.Timeouts, pr.BehindSchedule, pr.DrainSeconds)
	p("%-12s %s\n", "errors", formatErrors(pr.ErrorsByKind))
	if r.EndToEnd != nil {
		p("%-12s %s  (%d records)\n", "end-to-end", pct(r.EndToEnd.Latency), r.EndToEnd.Count)
	}
	cc := r.ClusterChecks
	p("%-12s leader_epoch_changes=%d partition_epoch_changes=%d isr_below_rf=%d (%s)\n",
		"stability", cc.LeaderEpochChanges, cc.PartitionEpochChanges, cc.ShrunkISR, cc.Source)
	e := r.Environment
	dirty := ""
	if e.Dirty {
		dirty = "-dirty"
	}
	p("%-12s %s, %d cores, RAM %s, %s %s, %s, commit %s%s\n", "environment", e.CPU, e.Cores, e.RAM, e.OS, e.Kernel, e.Go, e.Commit, dirty)
	p("%-12s fs %s, flush_policy %s, peak RSS %s\n", "", e.Filesystem, e.FlushPolicy, e.PeakRSS)
}

// formatErrors renders errors-by-kind sorted by kind.
func formatErrors(m map[string]int64) string {
	if len(m) == 0 {
		return "none"
	}
	keys := slices.Sorted(maps.Keys(m))
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}
