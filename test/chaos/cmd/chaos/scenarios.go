package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// scenario is one chaos scenario: a fault schedule run while the lin and bulk workloads run. The
// schedule returns once every fault it injected is healed.
type scenario struct {
	name       string
	fullDisk   bool // uses deploy/docker-compose.chaos.yml (broker3 logs on a 64 MiB tmpfs)
	faults     string
	invariants []string // extra, scenario-specific invariants (on top of the common ones)
	run        func(ctx context.Context, s *scenarioRun) error
}

// commonInvariants are checked by every scenario.
func commonInvariants() []string {
	return []string{
		"lin: no read integrity anomaly (gap, offset going backwards, CRC/decode failure, no progress below the HW)",
		"lin: VerifyFinal: every acked (value, offset) is at that offset of the settled final log, every read is a prefix of it, no phantom, no duplicate",
		"lin: porcupine check of the prepared history is Ok (Illegal = violation, Unknown = inconclusive = failure)",
		"bulk: every acked value present exactly once at its acked partition/offset; acked values of a key in sequence order; failed values at most once; nothing never produced",
		"availability (not safety): the cluster passes `streamqctl cluster status --strict` after healing",
	}
}

// scenarios returns every scenario in run order.
func scenarios() []scenario {
	return []scenario{
		{
			name:   "kill9-random",
			faults: "4 rounds: after 10–20 s, SIGKILL a random broker other than the previous round's (docker compose kill -s SIGKILL), start it again after 5–10 s",
			run:    runKill9Random,
		},
		{
			name:   "kill9-controller",
			faults: "3 rounds: after 10–20 s, SIGKILL the current controller (majority answer of Metadata.controller_id), start it again after 5–10 s",
			run:    runKill9Controller,
		},
		{
			name: "inbound-isolation",
			faults: "3 rounds: after 10–15 s, disable both toxiproxy proxies in front of the lin leader (brokerN_internal, brokerN_raft) for 15 s, " +
				"then re-enable them. This is an inbound-only cut: peers cannot open connections to the leader's internal/Raft ports (existing ones are closed), " +
				"but the leader's own outbound connections to its peers' proxies still work, and its client port stays reachable",
			run: runInboundIsolation,
		},
		{
			name: "pause-leader",
			faults: "2 rounds: after 10–15 s, docker compose pause the lin leader for 20 s (> 9 s broker session timeout), then unpause it " +
				"(a GC-pause style zombie; its sockets stay open but nothing answers). A round in which the lin leader epoch did not change is 'fault not exercised'",
			invariants: []string{"exercised: the lin leader epoch changes during at least one pause (R28)"},
			run:        runPauseLeader,
		},
		{
			name: "latency",
			faults: "after 10 s, on all 6 broker-to-broker proxies for 60 s: latency toxic (downstream, 200 ms ± 100 ms jitter) and slicer toxic " +
				"(upstream: TCP data cut into ~1 KiB ± 512 B fragments with 50 µs delay between them, i.e. fragmentation plus delay); then all toxics removed",
			run: runLatency,
		},
		{
			name:     "full-disk",
			fullDisk: true,
			faults: "broker3's log dir is a 64 MiB tmpfs volume (Raft state on a separate volume). After 15 s the volume is filled with " +
				"busybox dd until ENOSPC and held full for 30 s; healing replaces the disk: SIGKILL broker3, wait (bounded) until it is fenced and out of every ISR, " +
				"remove and recreate the log volume, start broker3 empty",
			invariants: []string{
				"other brokers keep serving: partitions not led by broker3 acknowledge bulk records while the disk is full",
				"after the disk replacement broker3 catches up and rejoins every ISR (cluster status --strict)",
			},
			run: runFullDisk,
		},
	}
}

// scenarioRun is the state of one running scenario.
type scenarioRun struct {
	r    *runner
	sc   scenario
	cl   *cluster
	rng  *rand.Rand
	t0   time.Time
	res  *scenarioResult
	bulk *bulkWorkload
}

// since returns seconds since the workload started.
func (s *scenarioRun) since() float64 { return round1(time.Since(s.t0).Seconds()) }

// event records and prints one fault-schedule action.
func (s *scenarioRun) event(action, target, detail string) {
	e := faultEvent{AtSec: s.since(), Action: action, Target: target, Detail: detail}
	s.res.Faults = append(s.res.Faults, e)
	s.r.logf("  [%6.1fs] %-24s %-8s %s", e.AtSec, action, target, detail)
}

// observe records an observation.
func (s *scenarioRun) observe(format string, args ...any) {
	o := fmt.Sprintf(format, args...)
	s.res.Observations = append(s.res.Observations, o)
	s.r.logf("  observation: %s", o)
}

// sleepRange waits a seeded random duration in [lo, hi).
func (s *scenarioRun) sleepRange(ctx context.Context, lo, hi time.Duration) error {
	d := lo + time.Duration(s.rng.Int64N(int64(hi-lo)))
	if !wait(ctx, d) {
		return ctx.Err()
	}
	return nil
}

// sleep waits d.
func (s *scenarioRun) sleep(ctx context.Context, d time.Duration) error {
	if !wait(ctx, d) {
		return ctx.Err()
	}
	return nil
}

// linView returns the newest view of the lin partition as a short string, plus the view.
func (s *scenarioRun) linView(ctx context.Context) (partitionView, string) {
	v, err := s.cl.partition(ctx, linTopic, 0)
	if err != nil {
		return partitionView{Leader: -1, Epoch: -1}, "unknown (" + err.Error() + ")"
	}
	return v, fmt.Sprintf("leader %d epoch %d isr %v (from broker %d)", v.Leader, v.Epoch, v.ISR, v.From)
}

// killAndRestart SIGKILLs a broker, waits 5–10 s and starts it again.
func (s *scenarioRun) killAndRestart(ctx context.Context, victim int32, why string) error {
	_, before := s.linView(ctx)
	if err := s.r.compose.kill9(ctx, s.sc.fullDisk, victim); err != nil {
		return err
	}
	s.event("kill -9", service(victim), why+"; lin before: "+before)
	if err := s.sleepRange(ctx, 5*time.Second, 10*time.Second); err != nil {
		return err
	}
	_, during := s.linView(ctx)
	if err := s.r.compose.start(ctx, s.sc.fullDisk, victim); err != nil {
		return err
	}
	s.event("start", service(victim), "lin while down: "+during)
	return nil
}

func runKill9Random(ctx context.Context, s *scenarioRun) error {
	prev := int32(0)
	for round := 1; round <= 4; round++ {
		if err := s.sleepRange(ctx, 10*time.Second, 20*time.Second); err != nil {
			return err
		}
		// Uniform over the brokers other than the previous victim, so the rounds spread over
		// leaders and followers instead of possibly hitting one broker four times.
		victim := int32(1 + s.rng.IntN(3))
		for victim == prev {
			victim = int32(1 + s.rng.IntN(3))
		}
		prev = victim
		if err := s.killAndRestart(ctx, victim, fmt.Sprintf("round %d: random broker", round)); err != nil {
			return err
		}
	}
	return nil
}

func runKill9Controller(ctx context.Context, s *scenarioRun) error {
	for round := 1; round <= 3; round++ {
		if err := s.sleepRange(ctx, 10*time.Second, 20*time.Second); err != nil {
			return err
		}
		ctrl := s.cl.controller(ctx)
		if ctrl < 1 {
			s.event("skip", "-", fmt.Sprintf("round %d: no controller known", round))
			continue
		}
		if err := s.killAndRestart(ctx, ctrl, fmt.Sprintf("round %d: controller", round)); err != nil {
			return err
		}
		if next := s.cl.controller(ctx); next > 0 {
			s.observe("round %d: controller %d killed; controller now %d", round, ctrl, next)
		}
	}
	return nil
}

func runInboundIsolation(ctx context.Context, s *scenarioRun) error {
	for round := 1; round <= 3; round++ {
		if err := s.sleepRange(ctx, 10*time.Second, 15*time.Second); err != nil {
			return err
		}
		v, before := s.linView(ctx)
		if v.Leader < 1 {
			s.event("skip", "-", fmt.Sprintf("round %d: lin has no leader: %s", round, before))
			continue
		}
		ctrl := s.cl.controller(ctx)
		for _, p := range proxyNames(v.Leader) {
			if err := s.r.toxi.setEnabled(ctx, p, false); err != nil {
				return err
			}
		}
		s.event("isolate inbound", service(v.Leader), fmt.Sprintf("round %d: proxies %v disabled; controller %d; lin before: %s",
			round, proxyNames(v.Leader), ctrl, before))
		if err := s.sleep(ctx, 15*time.Second); err != nil {
			return err
		}
		after, during := s.linView(ctx)
		for _, p := range proxyNames(v.Leader) {
			if err := s.r.toxi.setEnabled(ctx, p, true); err != nil {
				return err
			}
		}
		s.event("heal", service(v.Leader), "proxies re-enabled; lin at end of isolation: "+during)
		s.observe("round %d: isolated lin leader %d (epoch %d, controller %d); after 15 s: leader %d epoch %d isr %v",
			round, v.Leader, v.Epoch, ctrl, after.Leader, after.Epoch, after.ISR)
	}
	return nil
}

func runPauseLeader(ctx context.Context, s *scenarioRun) error {
	exercised := 0
	const rounds = 2
	for round := 1; round <= rounds; round++ {
		if err := s.sleepRange(ctx, 10*time.Second, 15*time.Second); err != nil {
			return err
		}
		v, before := s.linView(ctx)
		if v.Leader < 1 {
			s.event("skip", "-", fmt.Sprintf("round %d: lin has no leader: %s", round, before))
			continue
		}
		ctrl := s.cl.controller(ctx)
		if err := s.r.compose.pause(ctx, v.Leader); err != nil {
			return err
		}
		s.event("pause", service(v.Leader), fmt.Sprintf("round %d: controller %d; lin before: %s", round, ctrl, before))
		if err := s.sleep(ctx, 20*time.Second); err != nil {
			_ = s.r.compose.unpause(context.WithoutCancel(ctx), v.Leader)
			return err
		}
		during, desc := s.linView(ctx)
		if err := s.r.compose.unpause(ctx, v.Leader); err != nil {
			return err
		}
		s.event("unpause", service(v.Leader), "lin at end of pause: "+desc)
		if during.Epoch > v.Epoch {
			exercised++
			s.observe("round %d: paused lin leader %d (epoch %d, controller %d); leadership moved to %d at epoch %d during the pause",
				round, v.Leader, v.Epoch, ctrl, during.Leader, during.Epoch)
		} else {
			s.observe("round %d: fault not exercised: lin leader epoch stayed %d during the 20 s pause of broker %d", round, v.Epoch, v.Leader)
		}
	}
	if exercised == 0 {
		s.res.Status = StatusNotExercised
	}
	return nil
}

func runLatency(ctx context.Context, s *scenarioRun) error {
	if err := s.sleep(ctx, 10*time.Second); err != nil {
		return err
	}
	for _, p := range allProxies() {
		if err := s.r.toxi.addToxic(ctx, p, toxic{Name: "latency", Type: "latency", Stream: "downstream", Toxicity: 1,
			Attributes: map[string]int{"latency": 200, "jitter": 100}}); err != nil {
			return err
		}
		if err := s.r.toxi.addToxic(ctx, p, toxic{Name: "slicer", Type: "slicer", Stream: "upstream", Toxicity: 1,
			Attributes: map[string]int{"average_size": 1024, "size_variation": 512, "delay": 50}}); err != nil {
			return err
		}
	}
	s.event("add toxics", "all", "latency downstream 200±100 ms; slicer upstream 1024±512 B, 50 µs")
	for i := range 6 {
		if err := s.sleep(ctx, 10*time.Second); err != nil {
			return err
		}
		_, desc := s.linView(ctx)
		s.observe("latency +%ds: lin %s; controller %d", (i+1)*10, desc, s.cl.controller(ctx))
	}
	if err := s.r.toxi.reset(ctx); err != nil {
		return err
	}
	s.event("remove toxics", "all", "toxiproxy reset")
	return nil
}

// views returns the newest view of every lin and bulk partition, by "topic-partition".
func (s *scenarioRun) views(ctx context.Context) map[string]partitionView {
	resps := s.cl.metadataAll(ctx, []string{linTopic, bulkTopic})
	out := map[string]partitionView{}
	for _, t := range []string{linTopic, bulkTopic} {
		for p, v := range newestViews(resps, t) {
			out[fmt.Sprintf("%s-%d", t, p)] = v
		}
	}
	return out
}

func runFullDisk(ctx context.Context, s *scenarioRun) error {
	vol := s.r.compose.volumeName("broker3-logs")
	if err := s.sleep(ctx, 15*time.Second); err != nil {
		return err
	}
	// Which partitions does broker3 lead right now?
	ledBy3 := map[int32]bool{}
	for p, v := range newestViews(s.cl.metadataAll(ctx, []string{bulkTopic}), bulkTopic) {
		if v.Leader == 3 {
			ledBy3[p] = true
		}
	}
	out, err := docker(ctx, "run", "--rm", "-v", vol+":/d", busyboxImage, "dd", "if=/dev/zero", "of=/d/filler", "bs=1M")
	if err != nil && !strings.Contains(out, "No space left") {
		return fmt.Errorf("fill broker3 log volume: %w", err)
	}
	df, _ := docker(ctx, "run", "--rm", "-v", vol+":/d", busyboxImage, "df", "-k", "/d")
	fillAt := s.since()
	s.event("fill disk", "broker3", "dd until ENOSPC; "+lastLines(strings.TrimSpace(out), 1)+"; df: "+lastLines(strings.TrimSpace(df), 1))
	if !strings.Contains(out, "No space left") {
		s.observe("fault not exercised: dd did not report ENOSPC")
		s.res.Status = StatusNotExercised
	}
	for i := range 6 {
		if err := s.sleep(ctx, 5*time.Second); err != nil {
			return err
		}
		state, _ := s.r.compose.containerState(ctx, true, 3)
		var led []string
		for tp, v := range s.views(ctx) {
			if v.Leader == 3 {
				led = append(led, tp)
			}
		}
		slices.Sort(led)
		_, lin := s.linView(ctx)
		s.observe("disk full +%ds: broker3 container %s; partitions led by broker3: %v; lin %s", (i+1)*5, state, led, lin)
	}
	fullUntil := s.since()
	// Other brokers keep serving: acks on partitions not led by broker3 while the disk was full.
	served := map[int32]int{}
	for _, a := range s.bulk.ackEvents() {
		if a.At >= fillAt && a.At <= fullUntil && !ledBy3[a.Partition] {
			served[a.Partition]++
		}
	}
	total := 0
	for _, n := range served {
		total += n
	}
	s.observe("while full (%.1f–%.1fs): bulk acks on partitions not led by broker3 at fill time: %d (%v); broker3 led %v", fillAt, fullUntil, total, served, keys(ledBy3))
	if total == 0 && len(ledBy3) < bulkPartitions {
		s.res.Violations = append(s.res.Violations, "full-disk: no partition led by brokers 1/2 acknowledged a record while broker3's disk was full (other brokers must keep serving)")
	}

	// Heal: replace broker3's disk (R26, D57).
	state, _ := s.r.compose.containerState(ctx, true, 3)
	s.observe("before healing, broker3 container: %s", state)
	if strings.HasPrefix(state, "running") {
		if err := s.r.compose.kill9(ctx, true, 3); err != nil {
			return err
		}
	}
	s.event("kill -9", "broker3", "disk replacement starts; container was "+state)
	offline, ok := s.waitFenced(ctx, 60*time.Second)
	if !ok {
		s.observe("partition offline (availability): broker3 still in an ISR or partitions leaderless after 60 s: %v; skipping the empty restart, freeing the disk instead", offline)
		if _, err := docker(ctx, "run", "--rm", "-v", vol+":/d", busyboxImage, "rm", "-f", "/d/filler"); err != nil {
			return err
		}
		if err := s.r.compose.start(ctx, true, 3); err != nil {
			return err
		}
		s.event("start", "broker3", "filler removed, same disk")
		return nil
	}
	s.event("fenced", "broker3", "broker3 not live and out of every ISR")
	s.saveBroker3Logs(ctx)
	if _, err := s.r.compose.run(ctx, true, "rm", "-f", "-s", "broker3"); err != nil {
		return err
	}
	if _, err := docker(ctx, "volume", "rm", vol); err != nil {
		return err
	}
	if _, err := s.r.compose.run(ctx, true, "up", "-d", "--no-build", "broker3"); err != nil {
		return err
	}
	s.event("replace disk", "broker3", "container removed, log volume recreated empty, broker3 started")
	return nil
}

// saveBroker3Logs keeps the full-disk broker3 container's logs before the container is replaced
// (the scenario's final log capture only sees the new container) and records how its log
// failures surfaced.
func (s *scenarioRun) saveBroker3Logs(ctx context.Context) {
	out, err := s.r.compose.run(ctx, true, "logs", "--no-color", "--timestamps", "broker3")
	if err != nil {
		s.observe("could not capture broker3's logs before the disk replacement: %v", err)
		return
	}
	name := fmt.Sprintf("%s-%s-broker3-before-replacement.log", s.r.runID, s.sc.name)
	if err := os.WriteFile(filepath.Join(s.r.opts.out, name), []byte(out), 0o600); err != nil {
		s.observe("could not save broker3's logs: %v", err)
		return
	}
	var enospc, errs int
	var first string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, `"level":"ERROR"`) || strings.Contains(line, `"level":"WARN"`) {
			errs++
		}
		if strings.Contains(strings.ToLower(line), "no space left") {
			enospc++
			if first == "" {
				first = line
			}
		}
	}
	s.observe("broker3 log before the replacement (%s): %d WARN/ERROR lines, %d mentioning ENOSPC; first: %s", name, errs, enospc,
		truncate(first, 400))
}

// truncate shortens s to at most n bytes.
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// waitFenced waits until broker3 is not live and in no ISR of lin or bulk, and every partition
// has a leader. On timeout it returns the partitions still holding it (or leaderless).
func (s *scenarioRun) waitFenced(ctx context.Context, timeout time.Duration) ([]string, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		var bad []string
		resps := s.cl.metadataAll(ctx, nil)
		live := len(resps) == 0
		for _, resp := range resps {
			for _, b := range resp.GetBrokers() {
				live = live || b.GetId() == 3
			}
		}
		for tp, v := range s.views(ctx) {
			if slices.Contains(v.ISR, 3) || v.Leader < 1 {
				bad = append(bad, fmt.Sprintf("%s(leader %d isr %v)", tp, v.Leader, v.ISR))
			}
		}
		slices.Sort(bad)
		if !live && len(bad) == 0 {
			return nil, true
		}
		if live {
			bad = append(bad, "broker3 still live")
		}
		if !wait(ctx, 500*time.Millisecond) {
			return bad, false
		}
	}
}

// keys returns a map's keys, sorted.
func keys(m map[int32]bool) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
