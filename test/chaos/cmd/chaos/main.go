// Command chaos runs streamq's Docker chaos scenarios (SPEC §9 M9, docs/plans/M9.md §2 and
// Revisions 1–3): it builds the broker image, and for each scenario resets a dedicated compose
// project, runs a single-partition linearizability workload (test/linearizability) and an
// idempotent bulk workload while injecting faults (SIGKILL, docker pause, toxiproxy cuts and
// toxics, a full disk), heals, settles, and checks the safety invariants. It writes a JSON results
// file per scenario plus a Markdown report into -out and exits non-zero on any violation.
//
//	make chaos
//	go run ./test/chaos/cmd/chaos -compose deploy/docker-compose.yml \
//	    -override deploy/docker-compose.chaos.yml -project streamq-chaos -ctl ./bin/streamqctl \
//	    -out test/chaos/results [-scenarios kill9-random,latency] [-seed 42]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/sudo-ss1/streamq/pkg/client"
	"github.com/sudo-ss1/streamq/test/linearizability"
)

// Timing budgets.
const (
	healthyTimeout    = 180 * time.Second // initial health and recovery after healing
	topicCreateWait   = 60 * time.Second
	warmup            = 5 * time.Second
	postHeal          = 10 * time.Second // workloads keep running after the cluster is healthy again
	gapThreshold      = 2.0              // seconds without an acked lin append counted as unavailability
	settleBudget      = 6 * time.Minute
	toxiproxyReadyFor = 60 * time.Second
)

// options are the command-line flags.
type options struct {
	compose   string
	override  string
	project   string
	ctl       string
	out       string
	scenarios string
	seed      uint64
}

// runner runs scenarios against one compose project.
type runner struct {
	opts    options
	out     io.Writer
	compose *compose
	toxi    *toxiproxy
	ctl     ctl
	runID   string
	gitSHA  string
	imageID string
	env     environment
}

func (r *runner) logf(format string, args ...any) {
	_, _ = fmt.Fprintf(r.out, format+"\n", args...)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

func run(args []string, stdout io.Writer) int {
	var o options
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.StringVar(&o.compose, "compose", "deploy/docker-compose.yml", "base compose file")
	fs.StringVar(&o.override, "override", "deploy/docker-compose.chaos.yml", "full-disk compose override (used only by full-disk)")
	fs.StringVar(&o.project, "project", "streamq-chaos", "compose project name")
	fs.StringVar(&o.ctl, "ctl", "./bin/streamqctl", "streamqctl binary")
	fs.StringVar(&o.out, "out", "test/chaos/results", "results directory")
	fs.StringVar(&o.scenarios, "scenarios", "", "comma-separated scenarios to run (default: all)")
	fs.Uint64Var(&o.seed, "seed", 0, "schedule seed (default: derived from the clock; printed and recorded)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if o.seed == 0 {
		o.seed = uint64(time.Now().UnixNano() % 1_000_000_000)
	}
	selected, err := selectScenarios(o.scenarios)
	if err != nil {
		_, _ = fmt.Fprintf(stdout, "chaos: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := &runner{opts: o, out: stdout, ctl: ctl{path: o.ctl}, toxi: newToxiproxy("http://localhost:8474"),
		runID: time.Now().UTC().Format("20060102T150405Z")}
	results, err := r.runAll(ctx, selected)
	if err != nil {
		r.logf("chaos: %v", err)
		return 1
	}
	writeSummary(stdout, results)
	reportPath := filepath.Join(o.out, fmt.Sprintf("REPORT-%s.md", r.runID))
	if err := writeReportFile(reportPath, r, results); err != nil {
		r.logf("chaos: write report: %v", err)
		return 1
	}
	r.logf("\nresults: %s (report %s)", o.out, reportPath)
	failed := 0
	for _, res := range results {
		if res.Status != StatusPass {
			failed++
		}
	}
	if failed > 0 {
		r.logf("chaos: %d of %d scenario(s) did not pass", failed, len(results))
		return 1
	}
	r.logf("chaos: all %d scenario(s) passed (seed %d)", len(results), o.seed)
	return 0
}

// selectScenarios resolves the -scenarios flag.
func selectScenarios(list string) ([]scenario, error) {
	all := scenarios()
	if strings.TrimSpace(list) == "" {
		return all, nil
	}
	var out []scenario
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		i := slices.IndexFunc(all, func(s scenario) bool { return s.name == name })
		if i < 0 {
			names := make([]string, len(all))
			for j, s := range all {
				names[j] = s.name
			}
			return nil, fmt.Errorf("unknown scenario %q (known: %s)", name, strings.Join(names, ", "))
		}
		out = append(out, all[i])
	}
	return out, nil
}

// runAll prepares the image and runs the scenarios in order.
func (r *runner) runAll(ctx context.Context, selected []scenario) ([]scenarioResult, error) {
	if err := os.MkdirAll(r.opts.out, 0o750); err != nil {
		return nil, err
	}
	if _, err := os.Stat(r.opts.ctl); err != nil {
		return nil, fmt.Errorf("streamqctl binary %s: %w (run make build)", r.opts.ctl, err)
	}
	img, err := writeImageOverride(r.opts.out)
	if err != nil {
		return nil, err
	}
	base, err := filepath.Abs(r.opts.compose)
	if err != nil {
		return nil, err
	}
	override, err := filepath.Abs(r.opts.override)
	if err != nil {
		return nil, err
	}
	r.compose = &compose{project: r.opts.project, base: []string{base, img}, override: override}
	r.env = collectEnv(ctx)
	r.gitSHA = gitSHA(ctx)
	r.logf("chaos run %s: seed %d, git %s, project %s", r.runID, r.opts.seed, r.gitSHA, r.opts.project)
	r.logf("environment: %s (%d CPUs), RAM %s, docker %s, compose %s", r.env.CPU, r.env.CPUs, r.env.MemTotal, r.env.DockerVersion, r.env.ComposeVersion)

	// A stale chaos project would hold the ports; anything else holding them is an error (R25).
	if err := r.compose.down(ctx); err != nil {
		return nil, err
	}
	if err := checkPortsFree(ctx); err != nil {
		return nil, err
	}
	r.logf("building the broker image (streamq:chaos)...")
	if _, err := r.compose.run(ctx, false, "build", "broker1"); err != nil {
		return nil, err
	}
	if r.imageID, err = imageID(ctx, "streamq:chaos"); err != nil {
		return nil, err
	}
	r.logf("image %s", r.imageID)
	if slices.ContainsFunc(selected, func(s scenario) bool { return s.fullDisk }) {
		if _, err := docker(ctx, "pull", "-q", busyboxImage); err != nil {
			return nil, err
		}
	}

	var results []scenarioResult
	for _, sc := range selected {
		res := r.runScenario(ctx, sc, scenarioSeed(r.opts.seed, sc.name))
		path := filepath.Join(r.opts.out, fmt.Sprintf("%s-%s.json", r.runID, sc.name))
		if err := writeJSON(path, res); err != nil {
			return results, err
		}
		r.logf("scenario %s: %s (%s)", sc.name, res.Status, path)
		results = append(results, res)
		if ctx.Err() != nil {
			break
		}
	}
	if err := r.compose.down(context.WithoutCancel(ctx)); err != nil {
		r.logf("chaos: final down: %v", err)
	}
	return results, nil
}

// scenarioSeed derives a scenario's schedule seed from the run seed and its name, so
// `-scenarios x -seed N` reproduces the schedule scenario x had in a full run with seed N.
func scenarioSeed(seed uint64, name string) uint64 {
	h := seed
	for _, c := range name {
		h = h*1099511628211 + uint64(c)
	}
	return h
}

// runScenario runs one scenario end to end and never panics on harness errors: they are
// recorded with StatusError.
func (r *runner) runScenario(ctx context.Context, sc scenario, seed uint64) scenarioResult {
	start := time.Now()
	res := scenarioResult{Scenario: sc.name, Status: StatusPass, Seed: r.opts.seed, ScheduleSeed: seed, RunID: r.runID, GitSHA: r.gitSHA,
		ImageID: r.imageID, Env: r.env, StartedAt: start.UTC(), Invariants: append(commonInvariants(), sc.invariants...),
		Violations: []string{}, Observations: []string{}}
	r.logf("\n=== scenario %s (seed %d, schedule seed %d)", sc.name, r.opts.seed, seed)
	cl := newCluster()
	defer cl.close()
	s := &scenarioRun{r: r, sc: sc, cl: cl, rng: rand.New(rand.NewPCG(seed, 0x5eed)), res: &res} //nolint:gosec // seeded schedule
	err := r.scenarioBody(ctx, s)
	if err != nil {
		res.HarnessError = err.Error()
		res.Status = StatusError
		r.logf("  harness error: %v", err)
	}
	logs := filepath.Join(r.opts.out, fmt.Sprintf("%s-%s.log", r.runID, sc.name))
	bg := context.WithoutCancel(ctx)
	if err := r.compose.saveLogs(bg, sc.fullDisk, logs); err == nil {
		res.LogsFile = filepath.Base(logs)
	}
	_ = r.toxi.reset(bg)
	if err := r.compose.down(bg); err != nil {
		r.logf("  down: %v", err)
	}
	res.TotalSec = round1(time.Since(start).Seconds())
	return res
}

// scenarioBody is runScenario's work: setup, workloads + faults, heal, settle, check.
func (r *runner) scenarioBody(ctx context.Context, s *scenarioRun) error {
	sc, res, cl := s.sc, s.res, s.cl
	if err := r.compose.down(ctx); err != nil {
		return err
	}
	if err := r.compose.up(ctx, sc.fullDisk); err != nil {
		return err
	}
	if err := r.waitToxiproxy(ctx); err != nil {
		return err
	}
	if err := r.waitHealthy(ctx, cl, healthyTimeout); err != nil {
		return fmt.Errorf("initial health: %w", err)
	}
	if err := createTopics(ctx); err != nil {
		return err
	}
	if err := r.waitHealthy(ctx, cl, healthyTimeout); err != nil {
		return fmt.Errorf("health after creating topics: %w", err)
	}
	if err := checkFresh(ctx, cl); err != nil {
		return err
	}

	// Workloads, then the fault schedule.
	s.t0 = time.Now()
	lin := startLin(ctx, cl, res.ScheduleSeed)
	bulk, err := startBulk(ctx)
	if err != nil {
		lin.stop()
		return err
	}
	s.bulk = bulk
	stopped := false
	stopWorkloads := func() {
		if !stopped {
			stopped = true
			lin.stop()
			bulk.stopProducing()
		}
	}
	defer stopWorkloads()
	wait(ctx, warmup)
	if err := sc.run(ctx, s); err != nil {
		stopWorkloads()
		_ = r.toxi.reset(context.WithoutCancel(ctx))
		bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bulkCloseWait)
		bulk.settle(bctx)
		cancel()
		return fmt.Errorf("fault schedule: %w", err)
	}
	healAt := time.Now() // schedules return right after their last healing action
	herr := r.waitHealthy(ctx, cl, healthyTimeout)
	res.Availability.RecoverySec = round1(time.Since(healAt).Seconds())
	res.Availability.Healthy = herr == nil
	if herr != nil {
		res.Availability.HealthError = herr.Error()
		r.logf("  not healthy after healing: %v", herr)
	} else {
		r.logf("  healthy %.1fs after healing", res.Availability.RecoverySec)
	}
	wait(ctx, postHeal) // the workloads keep running on the healed cluster
	stopWorkloads()
	res.WorkloadSec = round1(time.Since(s.t0).Seconds())

	// Settle and check (R16, R25, R23, D58).
	sctx, cancel := context.WithTimeout(ctx, settleBudget)
	defer cancel()
	viz := filepath.Join(r.opts.out, fmt.Sprintf("%s-%s-lin.html", r.runID, sc.name))
	linRes, linViolations := lin.settleAndCheck(sctx, viz)
	res.Lin = linRes
	res.Violations = append(res.Violations, linViolations...)
	sentinels, settleErr, closeErr := bulk.settle(sctx)
	final, anomalies, readErr := bulk.finalRead(sctx, cl, sentinels)
	res.Bulk = bulk.result(final)
	res.Bulk.Anomalies = anomalies
	res.Bulk.SettleError = strings.Trim(settleErr+"; "+readErr, "; ")
	res.Bulk.CloseError = closeErr
	res.Violations = append(res.Violations, res.Bulk.violations()...)

	acks := lin.ackTimes()
	res.Availability.LinUnavailable = unavailableWindows(acks, res.WorkloadSec, gapThreshold)
	for _, w := range res.Availability.LinUnavailable {
		res.Availability.LinUnavailableSec += w.ToSec - w.FromSec
	}
	res.Availability.LinUnavailableSec = round1(res.Availability.LinUnavailableSec)
	if res.WorkloadSec > 0 {
		res.Availability.LinAcksPerSec = round1(float64(len(acks)) / res.WorkloadSec)
		res.Availability.BulkAcksPerSec = round1(float64(len(bulk.ackEvents())) / res.WorkloadSec)
	}
	r.logf("  lin: %+v; final %d; verify %s; porcupine %s (%.1fs)", summaryLine(res.Lin), res.Lin.FinalLen, res.Lin.VerifyFinal,
		res.Lin.Porcupine, res.Lin.CheckSec)
	r.logf("  bulk: acked %d, failed %d (present %d), rejected %d, lost %d, dup %d, phantom %d, order %d, final records %d",
		res.Bulk.Acked, res.Bulk.Failed, res.Bulk.FailedPresent, res.Bulk.Rejected, len(res.Bulk.Lost), len(res.Bulk.Duplicates),
		len(res.Bulk.Phantoms), len(res.Bulk.OrderViolations), res.Bulk.FinalRecords)
	for _, v := range res.Violations {
		r.logf("  VIOLATION: %s", v)
	}
	res.Status = finalStatus(res)
	return nil
}

// finalStatus derives a scenario's status from its checks.
func finalStatus(res *scenarioResult) string {
	switch {
	case slices.ContainsFunc(res.Violations, func(v string) bool { return strings.Contains(v, "inconclusive") }) &&
		len(res.Violations) == 1:
		return StatusInconclusive
	case len(res.Violations) > 0 || res.Lin.SettleError != "" || res.Bulk.SettleError != "": // unsettled reads prove nothing (R16)
		return StatusFail
	case !res.Availability.Healthy:
		return StatusUnhealthy
	case res.Status == StatusNotExercised:
		return StatusNotExercised
	}
	return StatusPass
}

func summaryLine(l linResult) string {
	return fmt.Sprintf("acked %d, unknown %d (in final %d, pruned %d), definite %d, reads %d, failed reads %d, no-leader %d, anomalies %d",
		l.Summary.Acked, l.Summary.Unknown, l.UnknownInFinal, l.Pruned, l.Summary.DefiniteFail, l.Summary.Reads, l.Summary.FailedReads,
		l.Summary.NoLeader, len(l.Summary.Anomalies))
}

// waitToxiproxy waits for the toxiproxy API and resets it.
func (r *runner) waitToxiproxy(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, toxiproxyReadyFor)
	defer cancel()
	for r.toxi.ready(ctx) != nil {
		if !wait(ctx, 250*time.Millisecond) {
			return errors.New("toxiproxy API not ready")
		}
	}
	return r.toxi.reset(ctx)
}

// createTopics creates lin (1 partition) and bulk (6 partitions), both RF 3 with
// min.insync.replicas 2.
func createTopics(ctx context.Context) error {
	c, err := client.New(client.Config{Bootstrap: bootstrap(), RequestTimeout: topicCreateWait})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	admin := client.NewAdmin(c)
	cfg := map[string]string{"min_insync_replicas": "2"}
	for _, t := range []struct {
		name  string
		parts int32
	}{{linTopic, 1}, {bulkTopic, bulkPartitions}} {
		if err := admin.CreateTopic(ctx, t.name, t.parts, 3, cfg); err != nil && !errors.Is(err, client.ErrTopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.name, err)
		}
	}
	return nil
}

// checkFresh fails unless the lin partition is empty: every scenario must start from a reset
// cluster, or the checks would see a previous scenario's values.
func checkFresh(ctx context.Context, cl *cluster) error {
	api, err := cl.leaderFunc(linTopic, 0)(ctx)
	if err != nil {
		return fmt.Errorf("fresh-cluster check: %w", err)
	}
	values, err := linearizability.ReadAll(ctx, api, linTopic, 0)
	if err != nil {
		return fmt.Errorf("fresh-cluster check: %w", err)
	}
	if len(values) > 0 {
		return fmt.Errorf("cluster is not fresh: %s-0 already holds %d records (stale volumes?)", linTopic, len(values))
	}
	return nil
}

// collectEnv describes the machine and tool versions.
func collectEnv(ctx context.Context) environment {
	env := environment{CPUs: runtime.NumCPU(), GoVersion: runtime.Version()}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
				env.CPU = strings.TrimSpace(v)
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if k, v, ok := strings.Cut(line, ":"); ok && k == "MemTotal" {
				env.MemTotal = strings.TrimSpace(v)
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		env.Kernel = strings.TrimSpace(string(b))
	}
	if out, err := docker(ctx, "version", "-f", "{{.Server.Version}}"); err == nil {
		env.DockerVersion = strings.TrimSpace(out)
	}
	if out, err := docker(ctx, "compose", "version", "--short"); err == nil {
		env.ComposeVersion = strings.TrimSpace(out)
	}
	return env
}
