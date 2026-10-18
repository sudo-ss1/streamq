package loadgen

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sudo-ss1/streamq/bench/internal/hist"
	"github.com/sudo-ss1/streamq/internal/config"
	"github.com/sudo-ss1/streamq/pkg/client"
)

const (
	// topicReadyTimeout bounds waiting for every partition of the bench topic to have a leader.
	topicReadyTimeout = time.Minute
	// interruptDrain bounds the drain after SIGINT.
	interruptDrain = 2 * time.Second
	// e2eCatchUp bounds waiting for the consumer to read every acked in-window record.
	e2eCatchUp = 10 * time.Second
	// closeTimeout bounds closing producers and taking the final snapshot.
	closeTimeout = 10 * time.Second
	// pollInterval paces the readiness and catch-up polls.
	pollInterval = 20 * time.Millisecond
)

// Run executes one benchmark. It returns the result (partial when ctx is cancelled, with status
// INVALID) or an error if the run could not start. Every resource it creates, including the
// in-process brokers and their data, is released before it returns (M10 plan R5).
func Run(ctx context.Context, cfg Config, logger *slog.Logger) (res *Result, err error) {
	if cfg.CPUProfile != "" {
		stop, err := startCPUProfile(cfg.CPUProfile)
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, stop()) }()
	}
	var (
		cluster     *inProcessCluster
		addrs       []string
		dataDir     string
		flushPolicy = "broker default (external cluster)"
	)
	if cfg.InProcess() {
		if dataDir, err = makeDataDir(cfg); err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, os.RemoveAll(dataDir)) }()
		if cluster, err = startInProcess(ctx, cfg.Brokers(), dataDir, logger); err != nil {
			return nil, err
		}
		defer cluster.close(context.WithoutCancel(ctx))
		addrs = cluster.addrs()
		flushPolicy = config.Default().TopicDefaults.FlushPolicy
	} else {
		for _, a := range strings.Split(cfg.Bootstrap, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
	}

	adminClient, err := client.New(client.Config{Bootstrap: addrs, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("admin client: %w", err)
	}
	defer func() { _ = adminClient.Close() }()
	admin := client.NewAdmin(adminClient)
	desc, err := ensureTopic(ctx, admin, cfg)
	if err != nil {
		return nil, err
	}
	if fp, ok := desc.Configs["flush_policy"]; ok && fp != "" {
		flushPolicy = fp
	}
	var src epochSource = adminEpochs{admin}
	srcName := "admin-describe"
	if cluster != nil && cfg.Brokers() > 1 {
		src, srcName = raftEpochs{cluster}, "metadata-store"
	}

	m, err := measure(ctx, cfg, addrs, desc, src, logger)
	if err != nil {
		return nil, err
	}
	res = &Result{
		Config:        configReport(cfg, len(desc.Partitions)),
		Produce:       produceReport(cfg, m.totals, m.window, m.drain),
		ClusterChecks: compareEpochs(srcName, m.before, m.after),
		RateModel:     RateModel,
	}
	if cfg.Consume {
		res.EndToEnd = &EndToEndReport{Count: int64(m.e2e.Count()), Latency: percentiles(m.e2e)}
	}
	reasons := m.reasons
	if !res.ClusterChecks.valid() {
		reasons = append(reasons, "cluster changed during the window: "+strings.Join(res.ClusterChecks.Problems, "; "))
	}
	if res.Produce.Count == 0 {
		reasons = append(reasons, "no record was acked in the window")
	}
	// A fault-free benchmark must have no produce errors or timeouts: they would distort the
	// numbers, so the run is not valid (review).
	if res.Produce.Timeouts > 0 {
		reasons = append(reasons, fmt.Sprintf("%d records timed out", res.Produce.Timeouts))
	}
	if len(res.Produce.ErrorsByKind) > 0 {
		reasons = append(reasons, fmt.Sprintf("produce errors: %v", res.Produce.ErrorsByKind))
	}
	res.Status, res.InvalidReasons = StatusOK, reasons
	if len(reasons) > 0 {
		res.Status = StatusInvalid
	}
	res.Environment = captureEnvironment(context.WithoutCancel(ctx), dataDir, flushPolicy)
	res.Environment.PeakRSS = peakRSS()
	return res, nil
}

// startCPUProfile starts a CPU profile to path; stop ends it and closes the file.
func startCPUProfile(path string) (stop func() error, err error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cpu profile: %w", err)
	}
	return func() error {
		pprof.StopCPUProfile()
		return f.Close()
	}, nil
}

// makeDataDir creates the run's data dir under -data-dir (or the system temp dir) and refuses
// to continue if the estimated disk use exceeds the free space (M10 plan R14).
func makeDataDir(cfg Config) (string, error) {
	parent := cfg.DataDir
	if parent == "" {
		parent = os.TempDir()
	} else if err := os.MkdirAll(parent, 0o750); err != nil {
		return "", fmt.Errorf("data dir: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "streamq-bench-*")
	if err != nil {
		return "", fmt.Errorf("data dir: %w", err)
	}
	need := cfg.EstimatedDiskBytes()
	if free, err := freeBytes(dir); err == nil && need > free {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("estimated disk use %s exceeds the %s free under %s (use -data-dir, -duration or -rate)",
			humanBytes(need), humanBytes(free), parent)
	}
	return dir, nil
}

// ensureTopic creates the bench topic if missing and waits until every partition has a leader.
func ensureTopic(ctx context.Context, admin *client.Admin, cfg Config) (client.TopicDescription, error) {
	cctx, cancel := context.WithTimeout(ctx, topicReadyTimeout)
	defer cancel()
	err := admin.CreateTopic(cctx, cfg.Topic, int32(cfg.Partitions), int32(cfg.RF),
		map[string]string{"min_insync_replicas": strconv.Itoa(cfg.MinInsync)})
	if err != nil && !errors.Is(err, client.ErrTopicAlreadyExists) {
		return client.TopicDescription{}, fmt.Errorf("create topic %s: %w", cfg.Topic, err)
	}
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		ds, err := admin.DescribeTopics(cctx, cfg.Topic)
		if err == nil && len(ds) == 1 && topicReady(ds[0]) {
			return ds[0], nil
		}
		select {
		case <-cctx.Done():
			return client.TopicDescription{}, fmt.Errorf("topic %s not ready: %w", cfg.Topic, errors.Join(err, cctx.Err()))
		case <-tick.C:
		}
	}
}

// topicReady reports whether every partition has a leader.
func topicReady(d client.TopicDescription) bool {
	if len(d.Partitions) == 0 {
		return false
	}
	for _, p := range d.Partitions {
		if p.Leader < 0 {
			return false
		}
	}
	return true
}

// measurement is the raw outcome of the workload.
type measurement struct {
	totals        Totals
	e2e           hist.Snapshot
	before, after map[int32]partitionEpochs
	window        time.Duration // the measured part of the window (shorter if interrupted)
	drain         time.Duration
	reasons       []string
}

// producerPair is one independent Client+Producer (M10 plan R1).
type producerPair struct {
	c *client.Client
	p *client.Producer
}

// measure runs the producers (and consumer) through warmup, window and drain.
func measure(ctx context.Context, cfg Config, addrs []string, desc client.TopicDescription,
	src epochSource, logger *slog.Logger,
) (m measurement, err error) {
	pairs := make([]producerPair, 0, cfg.Producers)
	defer func() { closeProducers(context.WithoutCancel(ctx), pairs) }()
	for range cfg.Producers {
		c, err := client.New(client.Config{Bootstrap: addrs, Logger: logger})
		if err != nil {
			return m, fmt.Errorf("producer client: %w", err)
		}
		p, err := client.NewProducer(c, cfg.ProducerConfig())
		if err != nil {
			_ = c.Close()
			return m, fmt.Errorf("producer: %w", err)
		}
		pairs = append(pairs, producerPair{c: c, p: p})
	}

	clk := SystemClock{}
	var cons *e2eConsumer
	if cfg.Consume {
		if cons, err = newE2EConsumer(ctx, addrs, desc, logger); err != nil {
			return m, err
		}
	}

	t0 := clk.Now()
	win := Window{WarmupEnd: t0.Add(cfg.Warmup), End: t0.Add(cfg.Warmup + cfg.Duration)}
	var bucket *TokenBucket
	if cfg.Rate > 0 {
		bucket = NewTokenBucket(clk, int64(cfg.Rate), t0)
	}
	recs := make([]*Recorder, len(pairs))
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var workers sync.WaitGroup
	for i, pp := range pairs {
		recs[i] = NewRecorder(win)
		w := &worker{id: i, topic: cfg.Topic, p: pp.p, clk: clk, bucket: bucket, rec: recs[i],
			pay: newPayloads(i, cfg.RecordSize, cfg.KeyCardinality, cfg.Producers, cfg.Consume), end: win.End}
		workers.Go(func() { w.run(runCtx) })
	}
	var consDone sync.WaitGroup
	consCtx, cancelCons := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelCons()
	if cons != nil {
		consDone.Go(func() { cons.run(consCtx, clk, win) })
	}

	interrupted := !waitUntil(ctx, clk, win.WarmupEnd)
	if !interrupted {
		if m.before, err = src.epochs(ctx, cfg.Topic); err != nil {
			m.reasons = append(m.reasons, "cluster snapshot before the window: "+err.Error())
		}
		interrupted = !waitUntil(ctx, clk, win.End)
	}
	stopAt := clk.Now()
	if interrupted {
		m.reasons = append(m.reasons, "interrupted: partial results")
	}

	// Drain (M10 plan R12): collect acks for in-window records, bounded.
	drainFor := cfg.DrainTimeout()
	if interrupted {
		drainFor = interruptDrain
		cancelRun()
	}
	drainCtx, cancelDrain := context.WithTimeout(context.WithoutCancel(ctx), drainFor)
	defer cancelDrain()
	stopWorkers := context.AfterFunc(drainCtx, cancelRun)
	workers.Wait()
	stopWorkers()
	var flushes sync.WaitGroup
	for _, pp := range pairs {
		flushes.Go(func() { _ = pp.p.Flush(drainCtx) })
	}
	flushes.Wait()
	m.totals = Merge(recs)
	m.drain = clk.Now().Sub(stopAt)
	end := stopAt
	if win.End.Before(end) {
		end = win.End
	}
	if end.After(win.WarmupEnd) {
		m.window = end.Sub(win.WarmupEnd)
	}

	if cons != nil {
		if !interrupted {
			cons.waitFor(ctx, clk, m.totals.Acked, e2eCatchUp)
		}
		cancelCons()
		consDone.Wait()
		m.e2e = hist.Merge(cons.lat)
		_ = cons.close()
	}

	if m.before != nil {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		m.after, err = src.epochs(sctx, cfg.Topic)
		cancel()
		if err != nil {
			m.reasons = append(m.reasons, "cluster snapshot after the window: "+err.Error())
		}
	}
	return m, nil
}

// waitUntil blocks until t or ctx ends; it reports whether t was reached.
func waitUntil(ctx context.Context, clk Clock, t time.Time) bool {
	d := t.Sub(clk.Now())
	if d <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-clk.After(d):
		return true
	}
}

// closeProducers closes every producer (bounded, in parallel), then its client.
func closeProducers(ctx context.Context, pairs []producerPair) {
	ctx, cancel := context.WithTimeout(ctx, closeTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, pp := range pairs {
		wg.Go(func() {
			_ = pp.p.Close(ctx)
			_ = pp.c.Close()
		})
	}
	wg.Wait()
}

// e2eConsumer reads every partition from its end and records end-to-end latency from the
// intended send time embedded in each value.
type e2eConsumer struct {
	c        *client.Client
	consumer *client.Consumer
	lat      *hist.Shard
	count    atomic.Int64 // in-window records seen
}

func newE2EConsumer(ctx context.Context, addrs []string, desc client.TopicDescription, logger *slog.Logger) (*e2eConsumer, error) {
	c, err := client.New(client.Config{Bootstrap: addrs, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("consumer client: %w", err)
	}
	cons, err := client.NewConsumer(c, client.ConsumerConfig{})
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("consumer: %w", err)
	}
	tps := make([]client.TopicPartition, 0, len(desc.Partitions))
	for _, p := range desc.Partitions {
		tps = append(tps, client.TopicPartition{Topic: desc.Name, Partition: p.Partition})
	}
	cons.Assign(tps)
	if err := cons.SeekToEnd(ctx); err != nil {
		_ = cons.Close()
		_ = c.Close()
		return nil, fmt.Errorf("consumer seek: %w", err)
	}
	return &e2eConsumer{c: c, consumer: cons, lat: hist.NewShard()}, nil
}

// run polls until ctx ends.
func (e *e2eConsumer) run(ctx context.Context, clk Clock, win Window) {
	for ctx.Err() == nil {
		recs, err := e.consumer.Poll(ctx)
		now := clk.Now()
		for _, r := range recs {
			if t, ok := stampOf(r.Value); ok && win.contains(t) {
				e.lat.Record(int64(now.Sub(t)))
				e.count.Add(1)
			}
		}
		if err != nil && ctx.Err() == nil {
			waitUntil(ctx, clk, now.Add(100*time.Millisecond)) // back off on persistent errors
		}
	}
}

// waitFor waits until the consumer saw n in-window records, ctx ends or limit passes.
func (e *e2eConsumer) waitFor(ctx context.Context, clk Clock, n int64, limit time.Duration) {
	deadline := clk.Now().Add(limit)
	for e.count.Load() < n && clk.Now().Before(deadline) {
		if !waitUntil(ctx, clk, clk.Now().Add(pollInterval)) {
			return
		}
	}
}

func (e *e2eConsumer) close() error {
	return errors.Join(e.consumer.Close(), e.c.Close())
}
