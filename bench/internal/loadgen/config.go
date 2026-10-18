package loadgen

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/sudo-ss1/streamq/pkg/client"
)

// Cluster modes.
const (
	ClusterInProcess1 = "inprocess:1"
	ClusterInProcess3 = "inprocess:3"
)

// Acks flag values.
const (
	AcksAll    = "all"
	AcksLeader = "leader"
	AcksNone   = "none"
)

// assumedMaxRate is the send rate assumed for disk estimates when -rate is 0 (as fast as
// possible).
const assumedMaxRate = 250_000

// Client defaults set explicitly so every effective ProducerConfig value is known and reported
// (DECISIONS D22, M10 plan R1).
const (
	bufferMemory    = 32 << 20
	maxRequestSize  = 1 << 20
	deliveryTimeout = 2 * time.Minute
	maxBlock        = time.Minute
	maxDrain        = 30 * time.Second
)

// ErrUsage reports invalid flags; the caller exits with status 2.
var ErrUsage = errors.New("usage")

// Config is the effective bench configuration (bench/USAGE.md).
type Config struct {
	Preset         string
	Cluster        string
	Bootstrap      string
	DataDir        string
	Topic          string
	Partitions     int
	RF             int // 0 = 1 for inprocess:1, else 3
	MinInsync      int // 0 = 1 for RF 1, else 2
	Acks           string
	Idempotent     bool
	RecordSize     int
	KeyCardinality int
	Rate           int
	Producers      int
	Linger         time.Duration
	BatchSize      int
	Duration       time.Duration
	Warmup         time.Duration
	Consume        bool
	JSONPath       string
	CPUProfile     string
}

// DefaultConfig returns the flag defaults of bench/USAGE.md.
func DefaultConfig() Config {
	return Config{
		Cluster:    ClusterInProcess1,
		Topic:      "bench",
		Partitions: 6,
		Acks:       AcksAll,
		Idempotent: true,
		RecordSize: 1024,
		Producers:  4,
		Linger:     5 * time.Millisecond,
		BatchSize:  16 << 10,
		Duration:   30 * time.Second,
		Warmup:     5 * time.Second,
	}
}

// Presets returns the named presets of bench/USAGE.md as edits applied over DefaultConfig.
func Presets() map[string]func(*Config) {
	nfr1 := func(c *Config) {
		c.Cluster, c.RF, c.MinInsync = ClusterInProcess3, 3, 2
		c.Acks, c.Idempotent = AcksAll, true
		c.RecordSize, c.Rate, c.Partitions, c.Producers = 1024, 0, 6, 8
		c.Linger, c.BatchSize = 10*time.Millisecond, 512<<10
		c.Duration = 30 * time.Second
	}
	return map[string]func(*Config){
		"quick": func(c *Config) {
			c.Cluster, c.RF, c.MinInsync = ClusterInProcess1, 1, 1
			c.Acks = AcksLeader
			c.RecordSize, c.Rate, c.Partitions, c.Producers = 1024, 0, 6, 4
			c.Linger, c.BatchSize = 5*time.Millisecond, 16<<10
			c.Duration, c.Warmup = 10*time.Second, 2*time.Second
		},
		"nfr1": nfr1,
		"nfr1-nonidem": func(c *Config) {
			nfr1(c)
			c.Idempotent = false
		},
		"nfr2": func(c *Config) {
			c.Cluster, c.RF, c.MinInsync = ClusterInProcess1, 1, 1
			c.Acks = AcksLeader
			c.RecordSize, c.Rate, c.Partitions, c.Producers = 1024, 5000, 6, 4
			c.Linger, c.BatchSize = time.Millisecond, 16<<10
			c.Duration = 30 * time.Second
		},
	}
}

// presetNames returns the preset names, sorted.
func presetNames() []string {
	var names []string
	for n := range Presets() {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// bindFlags registers every flag of bench/USAGE.md on fs, writing into c (whose current values are
// the defaults).
func bindFlags(fs *flag.FlagSet, c *Config) {
	fs.StringVar(&c.Preset, "preset", c.Preset, "preset: "+strings.Join(presetNames(), ", ")+"; explicit flags override preset values")
	fs.StringVar(&c.Cluster, "cluster", c.Cluster, "in-process cluster: inprocess:1 or inprocess:3")
	fs.StringVar(&c.Bootstrap, "bootstrap", c.Bootstrap, "host:port,... of an external cluster; replaces -cluster")
	fs.StringVar(&c.DataDir, "data-dir", c.DataDir, "parent dir for in-process broker data (default a temp dir; the run's data is removed on exit)")
	fs.StringVar(&c.Topic, "topic", c.Topic, "topic (created if missing)")
	fs.IntVar(&c.Partitions, "partitions", c.Partitions, "partitions of a created topic")
	fs.IntVar(&c.RF, "rf", c.RF, "replication factor of a created topic (0 = 1 for inprocess:1, else 3)")
	fs.IntVar(&c.MinInsync, "min-insync", c.MinInsync, "min.insync.replicas of a created topic (0 = 1 for RF 1, else 2)")
	fs.StringVar(&c.Acks, "acks", c.Acks, "all, leader or none")
	fs.BoolVar(&c.Idempotent, "idempotent", c.Idempotent, "idempotence (applies with acks=all only, D48)")
	fs.IntVar(&c.RecordSize, "record-size", c.RecordSize, "value bytes per record (>= 16 with -consume)")
	fs.IntVar(&c.KeyCardinality, "key-cardinality", c.KeyCardinality, "number of distinct keys; 0 = unkeyed (sticky partitioner)")
	fs.IntVar(&c.Rate, "rate", c.Rate, "total target msgs/s across all producers (one shared token bucket); 0 = as fast as possible")
	fs.IntVar(&c.Producers, "producers", c.Producers, "independent Client+Producer pairs, one goroutine each")
	fs.DurationVar(&c.Linger, "linger", c.Linger, "ProducerConfig.Linger")
	fs.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "ProducerConfig.BatchSize in bytes")
	fs.DurationVar(&c.Duration, "duration", c.Duration, "measurement window")
	fs.DurationVar(&c.Warmup, "warmup", c.Warmup, "excluded from results")
	fs.BoolVar(&c.Consume, "consume", c.Consume, "also consume and report end-to-end latency")
	fs.StringVar(&c.JSONPath, "json", c.JSONPath, "write the full result as JSON to this path (mode 0600)")
	fs.StringVar(&c.CPUProfile, "cpuprofile", c.CPUProfile, "write a CPU profile of the run to this path")
}

// Parse parses args (without the program name). Values come from DefaultConfig, then -preset,
// then explicitly set flags. -help prints usage, including disk estimates, to out and returns
// flag.ErrHelp; invalid flags return an error wrapping ErrUsage.
func Parse(args []string, out io.Writer) (Config, error) {
	// Pass 1: find -preset (other flags are parsed and discarded).
	probe := DefaultConfig()
	fs := flag.NewFlagSet("streamq-bench", flag.ContinueOnError)
	fs.SetOutput(out)
	bindFlags(fs, &probe)
	fs.Usage = func() { usage(fs, out) }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return Config{}, err
		}
		return Config{}, fmt.Errorf("%w: %w", ErrUsage, err)
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("%w: unexpected arguments %q", ErrUsage, fs.Args())
	}
	cfg := DefaultConfig()
	if probe.Preset != "" {
		apply, ok := Presets()[probe.Preset]
		if !ok {
			return Config{}, fmt.Errorf("%w: unknown preset %q (want one of %s)", ErrUsage, probe.Preset, strings.Join(presetNames(), ", "))
		}
		apply(&cfg)
	}
	// Pass 2: explicit flags override the preset.
	fs2 := flag.NewFlagSet("streamq-bench", flag.ContinueOnError)
	fs2.SetOutput(io.Discard)
	bindFlags(fs2, &cfg)
	if err := fs2.Parse(args); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrUsage, err)
	}
	cfg.resolve()
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrUsage, err)
	}
	return cfg, nil
}

// InProcess reports whether brokers run in this process.
func (c Config) InProcess() bool { return c.Bootstrap == "" }

// Brokers returns the in-process broker count (0 with -bootstrap).
func (c Config) Brokers() int {
	switch {
	case !c.InProcess():
		return 0
	case c.Cluster == ClusterInProcess3:
		return 3
	default:
		return 1
	}
}

// resolve fills the derived defaults of -rf and -min-insync.
func (c *Config) resolve() {
	if c.RF == 0 {
		c.RF = 3
		if c.InProcess() && c.Cluster == ClusterInProcess1 {
			c.RF = 1
		}
	}
	if c.MinInsync == 0 {
		c.MinInsync = 2
		if c.RF == 1 {
			c.MinInsync = 1
		}
	}
}

// Validate checks a resolved config.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.InProcess() && c.Cluster != ClusterInProcess1 && c.Cluster != ClusterInProcess3 {
		add("-cluster %q: want %s or %s", c.Cluster, ClusterInProcess1, ClusterInProcess3)
	}
	if c.Topic == "" {
		add("-topic must not be empty")
	}
	if c.Partitions < 1 {
		add("-partitions %d must be >= 1", c.Partitions)
	}
	if c.RF < 1 || (c.InProcess() && c.RF > c.Brokers()) {
		add("-rf %d must be in [1, brokers]", c.RF)
	}
	if c.MinInsync < 1 || c.MinInsync > c.RF {
		add("-min-insync %d must be in [1, rf %d]", c.MinInsync, c.RF)
	}
	if _, err := c.ClientAcks(); err != nil {
		errs = append(errs, err)
	}
	minSize := 1
	if c.Consume {
		minSize = 16
	}
	if c.RecordSize < minSize || c.RecordSize > maxRequestSize/2 {
		add("-record-size %d must be in [%d, %d]", c.RecordSize, minSize, maxRequestSize/2)
	}
	if c.KeyCardinality < 0 || c.KeyCardinality > 1_000_000 {
		add("-key-cardinality %d must be in [0, 1000000]", c.KeyCardinality)
	}
	if c.Rate < 0 {
		add("-rate %d must be >= 0", c.Rate)
	}
	if c.Producers < 1 || c.Producers > 1024 {
		add("-producers %d must be in [1, 1024]", c.Producers)
	}
	if c.Linger <= 0 {
		add("-linger %v must be > 0", c.Linger)
	}
	if c.BatchSize < 1 {
		add("-batch-size %d must be >= 1", c.BatchSize)
	}
	if c.Duration <= 0 {
		add("-duration %v must be > 0", c.Duration)
	}
	if c.Warmup < 0 {
		add("-warmup %v must be >= 0", c.Warmup)
	}
	return errors.Join(errs...)
}

// ClientAcks maps -acks to the client's Acks.
func (c Config) ClientAcks() (client.Acks, error) {
	switch c.Acks {
	case AcksAll:
		return client.AcksAll, nil
	case AcksLeader:
		return client.AcksLeader, nil
	case AcksNone:
		return client.AcksNone, nil
	default:
		return 0, fmt.Errorf("-acks %q: want all, leader or none", c.Acks)
	}
}

// IdempotenceEnabled reports the effective idempotence (acks=all and -idempotent, D48).
func (c Config) IdempotenceEnabled() bool { return c.Acks == AcksAll && c.Idempotent }

// ProducerConfig returns the effective client ProducerConfig, every value explicit.
func (c Config) ProducerConfig() client.ProducerConfig {
	acks, _ := c.ClientAcks() // validated
	return client.ProducerConfig{
		Acks:               acks,
		Linger:             c.Linger,
		BatchSize:          c.BatchSize,
		BufferMemory:       bufferMemory,
		MaxRequestSize:     maxRequestSize,
		DeliveryTimeout:    deliveryTimeout,
		MaxBlock:           maxBlock,
		DisableIdempotence: !c.Idempotent,
	}
}

// DrainTimeout is how long acks are collected after the window: the lesser of the delivery
// timeout and 30 s (M10 plan R12).
func (c Config) DrainTimeout() time.Duration { return min(deliveryTimeout, maxDrain) }

// EstimatedDiskBytes estimates the run's disk use as rate × record size × RF × (warmup +
// duration), assuming assumedMaxRate msgs/s when -rate is 0 (M10 plan R14). It is a heuristic
// (max-rate throughput depends on the machine); runs measured up to ~163k msgs/s.
func (c Config) EstimatedDiskBytes() int64 {
	rate := int64(c.Rate)
	if rate == 0 {
		rate = assumedMaxRate
	}
	secs := (c.Warmup + c.Duration).Seconds()
	return int64(float64(rate) * float64(c.RecordSize) * float64(max(c.RF, 1)) * secs)
}

// usage prints the flags and the disk estimate of each preset.
func usage(fs *flag.FlagSet, out io.Writer) {
	_, _ = fmt.Fprintf(out, "Usage: streamq-bench [flags]\n\nFlags:\n")
	fs.PrintDefaults()
	_, _ = fmt.Fprintf(out, "\nDisk use: about rate × record size × RF × (warmup + duration), with rate %d msgs/s\n"+
		"assumed for -rate 0. In-process runs refuse to start without that much free space.\n", assumedMaxRate)
	presets := Presets()
	for _, name := range presetNames() {
		c := DefaultConfig()
		presets[name](&c)
		c.resolve()
		_, _ = fmt.Fprintf(out, "  -preset %-13s ~%s\n", name, humanBytes(c.EstimatedDiskBytes()))
	}
}

// humanBytes formats n with a binary unit.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
