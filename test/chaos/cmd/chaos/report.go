package main

import (
	"fmt"
	"os"
	"slices"
	"strings"
)

// writeReportFile writes a Markdown report of a run: the environment, a summary table and one
// section per scenario. test/chaos/REPORT.md is assembled from it plus written observations.
func writeReportFile(path string, r *runner, results []scenarioResult) error {
	return os.WriteFile(path, []byte(renderReport(r, results)), 0o600)
}

func renderReport(r *runner, results []scenarioResult) string {
	var b strings.Builder
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(&b, format+"\n", args...) }
	p("# streamq chaos run %s", r.runID)
	p("")
	p("- git SHA: `%s`", r.gitSHA)
	p("- image: `streamq:chaos` `%s`", r.imageID)
	p("- seed: `%d` (rerun: `go run ./test/chaos/cmd/chaos ... -seed %d -scenarios <name>`)", r.opts.seed, r.opts.seed)
	p("- environment: %s, %d CPUs, RAM %s, kernel %s, Docker %s, Compose %s, %s", r.env.CPU, r.env.CPUs, r.env.MemTotal,
		r.env.Kernel, r.env.DockerVersion, r.env.ComposeVersion, r.env.GoVersion)
	p("")
	p("| scenario | status | lin acked | lin unknown (in final / pruned) | lin definite | lin reads (failed) | anomalies | porcupine | bulk acked | bulk failed (present) | bulk lost | recovery | lin unavailable |")
	p("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, res := range results {
		l, bk := res.Lin, res.Bulk
		p("| %s | %s | %d | %d (%d / %d) | %d | %d (%d) | %d | %s (%.1fs) | %d | %d (%d) | %d | %.1fs | %.1fs |", res.Scenario, res.Status,
			l.Summary.Acked, l.Summary.Unknown, l.UnknownInFinal, l.Pruned, l.Summary.DefiniteFail, l.Summary.Reads,
			l.Summary.FailedReads, len(l.Summary.Anomalies)+len(bk.Anomalies), l.Porcupine, l.CheckSec, bk.Acked, bk.Failed,
			bk.FailedPresent, len(bk.Lost), res.Availability.RecoverySec, res.Availability.LinUnavailableSec)
	}
	for _, res := range results {
		p("")
		p("## %s: %s", res.Scenario, res.Status)
		p("")
		sc := slices.IndexFunc(scenarios(), func(s scenario) bool { return s.name == res.Scenario })
		if sc >= 0 {
			p("**Faults injected.** %s.", scenarios()[sc].faults)
			p("")
		}
		p("Results file `%s-%s.json`, broker logs `%s`, schedule seed %d, workload %.1fs, total %.1fs.", res.RunID, res.Scenario,
			res.LogsFile, res.ScheduleSeed, res.WorkloadSec, res.TotalSec)
		if res.HarnessError != "" {
			p("")
			p("**Harness error:** %s", res.HarnessError)
		}
		p("")
		p("**Invariants checked.**")
		for _, inv := range res.Invariants {
			p("- %s", inv)
		}
		p("")
		p("**Fault timeline** (seconds since the workloads started):")
		p("")
		for _, f := range res.Faults {
			p("- %.1fs %s %s: %s", f.AtSec, f.Action, f.Target, f.Detail)
		}
		l := res.Lin
		p("")
		p("**Safety.** lin: %s; final log %d entries (sentinel at %d); VerifyFinal %s; porcupine %s in %.1fs.", summaryLine(l),
			l.FinalLen, l.SentinelOffset, l.VerifyFinal, l.Porcupine, l.CheckSec)
		bk := res.Bulk
		p("bulk: produced %d, acked %d (present once %d), failed %d (present %d), rejected by the producer %d, lost %d, duplicates %d, "+
			"phantoms %d, per-key order violations %d, offset mismatches %d, final records %d.", bk.Produced, bk.Acked, bk.AckedPresent,
			bk.Failed, bk.FailedPresent, bk.Rejected, len(bk.Lost), len(bk.Duplicates), len(bk.Phantoms), len(bk.OrderViolations),
			len(bk.OffsetMismatch), bk.FinalRecords)
		if len(bk.ErrorsByKind) > 0 {
			var kinds []string
			for k, n := range bk.ErrorsByKind {
				kinds = append(kinds, fmt.Sprintf("%s ×%d", k, n))
			}
			slices.Sort(kinds)
			p("bulk producer errors: %s.", strings.Join(kinds, ", "))
		}
		if l.Visualization != "" {
			p("porcupine visualization: `%s`.", l.Visualization)
		}
		if len(res.Violations) == 0 {
			p("No violations.")
		} else {
			p("")
			p("**Violations:**")
			for _, v := range res.Violations {
				p("- %s", v)
			}
		}
		a := res.Availability
		p("")
		p("**Availability.** healthy after healing: %v (%.1fs after the last heal)%s. lin appends acked %.1f/s, bulk %.1f/s. "+
			"Windows of ≥ %.0fs without an acked lin append: %s (total %.1fs).", a.Healthy, a.RecoverySec, errSuffix(a.HealthError),
			a.LinAcksPerSec, a.BulkAcksPerSec, gapThreshold, windows(a.LinUnavailable), a.LinUnavailableSec)
		if len(res.Observations) > 0 {
			p("")
			p("**Observations.**")
			for _, o := range res.Observations {
				p("- %s", o)
			}
		}
	}
	return b.String()
}

func errSuffix(s string) string {
	if s == "" {
		return ""
	}
	return " — " + strings.SplitN(s, "\n", 2)[0]
}

func windows(ws []window) string {
	if len(ws) == 0 {
		return "none"
	}
	parts := make([]string, len(ws))
	for i, w := range ws {
		parts[i] = fmt.Sprintf("%.1f–%.1f", w.FromSec, w.ToSec)
	}
	return strings.Join(parts, ", ")
}
