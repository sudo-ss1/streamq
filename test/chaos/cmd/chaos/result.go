package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/sudo-ss1/streamq/test/linearizability"
)

// Scenario statuses. Anything but StatusPass makes the runner exit non-zero.
const (
	StatusPass         = "PASS"
	StatusFail         = "FAIL"          // a safety invariant was violated
	StatusInconclusive = "INCONCLUSIVE"  // the porcupine check timed out (D58)
	StatusUnhealthy    = "UNHEALTHY"     // safety held but the cluster did not recover in time
	StatusNotExercised = "NOT_EXERCISED" // safety held but the fault never took effect (R28)
	StatusError        = "ERROR"         // the harness itself failed (docker, setup)
)

// environment describes the machine and tool versions of a run.
type environment struct {
	CPU            string `json:"cpu"`
	CPUs           int    `json:"cpus"`
	MemTotal       string `json:"mem_total"`
	Kernel         string `json:"kernel"`
	DockerVersion  string `json:"docker_version"`
	ComposeVersion string `json:"compose_version"`
	GoVersion      string `json:"go_version"`
}

// faultEvent is one action of a scenario's fault schedule.
type faultEvent struct {
	AtSec  float64 `json:"at_sec"` // seconds since the workload started
	Action string  `json:"action"`
	Target string  `json:"target,omitempty"`
	Detail string  `json:"detail,omitempty"`
}

// window is an interval in seconds since the workload started.
type window struct {
	FromSec float64 `json:"from_sec"`
	ToSec   float64 `json:"to_sec"`
}

// linResult is the outcome of the lin (single-partition linearizability) workload.
type linResult struct {
	Summary        linearizability.Summary `json:"summary"`
	Ops            int                     `json:"ops"` // recorded operations
	Pruned         int                     `json:"pruned_unknown"`
	UnknownInFinal int                     `json:"unknown_in_final"`
	FinalLen       int                     `json:"final_len"`
	SentinelOffset int64                   `json:"sentinel_offset"`
	SettleError    string                  `json:"settle_error,omitempty"`
	FinalAnomaly   string                  `json:"final_anomaly,omitempty"`
	VerifyFinal    string                  `json:"verify_final"` // "ok" or the error
	Porcupine      string                  `json:"porcupine"`    // Ok, Illegal, Unknown, or skipped
	CheckSec       float64                 `json:"check_sec"`
	Visualization  string                  `json:"visualization,omitempty"`
}

// bulkResult is the outcome of the bulk (idempotent producer) workload (plan M9 R7).
type bulkResult struct {
	Produced        int            `json:"produced"`         // accepted by the producer
	Rejected        int            `json:"rejected"`         // refused by Produce (never sent)
	Acked           int            `json:"acked"`            // acknowledged
	Failed          int            `json:"failed"`           // completed with an error (or unsent at close)
	AckedPresent    int            `json:"acked_present"`    // acked and present exactly once
	FailedPresent   int            `json:"failed_present"`   // failed but present (allowed, at most once)
	Lost            []string       `json:"lost"`             // acked but absent
	Duplicates      []string       `json:"duplicates"`       // present more than once
	Phantoms        []string       `json:"phantoms"`         // present but never accepted
	OrderViolations []string       `json:"order_violations"` // acked values of a key out of sequence order
	OffsetMismatch  []string       `json:"offset_mismatch"`  // acked offset holds another value
	WrongPartition  []string       `json:"wrong_partition"`  // found in another partition than acked
	Anomalies       []string       `json:"anomalies"`        // read integrity anomalies in the final read
	FinalRecords    int            `json:"final_records"`
	SettleError     string         `json:"settle_error,omitempty"`
	CloseError      string         `json:"close_error,omitempty"`
	ErrorsByKind    map[string]int `json:"errors_by_kind"`
}

// violations lists the bulk safety violations.
func (b bulkResult) violations() []string {
	var out []string
	add := func(name string, vs []string) {
		if len(vs) > 0 {
			out = append(out, fmt.Sprintf("bulk %s: %d (e.g. %s)", name, len(vs), strings.Join(vs[:min(5, len(vs))], ", ")))
		}
	}
	add("lost acked values", b.Lost)
	add("duplicates", b.Duplicates)
	add("phantoms", b.Phantoms)
	add("per-key order violations", b.OrderViolations)
	add("offset mismatches", b.OffsetMismatch)
	add("values in the wrong partition", b.WrongPartition)
	add("read anomalies", b.Anomalies)
	return out
}

// availability separates liveness observations from safety (plan M9 R15).
type availability struct {
	// LinUnavailable lists intervals of at least gapThreshold with no acknowledged lin append.
	LinUnavailable []window `json:"lin_unavailable"`
	// LinUnavailableSec is their total length.
	LinUnavailableSec float64 `json:"lin_unavailable_sec"`
	// RecoverySec is the time from the final heal to `cluster status --strict` passing.
	RecoverySec float64 `json:"recovery_sec"`
	Healthy     bool    `json:"healthy"`
	HealthError string  `json:"health_error,omitempty"`
	// LinAcksPerSec is the acknowledged lin append rate over the workload.
	LinAcksPerSec  float64 `json:"lin_acks_per_sec"`
	BulkAcksPerSec float64 `json:"bulk_acks_per_sec"`
}

// scenarioResult is one scenario's results file.
type scenarioResult struct {
	Scenario     string       `json:"scenario"`
	Status       string       `json:"status"`
	Seed         uint64       `json:"seed"`          // the run's -seed
	ScheduleSeed uint64       `json:"schedule_seed"` // derived from seed and the scenario name
	RunID        string       `json:"run_id"`
	GitSHA       string       `json:"git_sha"`
	ImageID      string       `json:"image_id"`
	Env          environment  `json:"environment"`
	StartedAt    time.Time    `json:"started_at"`
	WorkloadSec  float64      `json:"workload_sec"`
	TotalSec     float64      `json:"total_sec"`
	Faults       []faultEvent `json:"faults"`
	Invariants   []string     `json:"invariants"`
	Lin          linResult    `json:"lin"`
	Bulk         bulkResult   `json:"bulk"`
	Availability availability `json:"availability"`
	Violations   []string     `json:"violations"`
	Observations []string     `json:"observations"`
	HarnessError string       `json:"harness_error,omitempty"`
	LogsFile     string       `json:"logs_file,omitempty"`
}

// writeJSON writes v as indented JSON to path.
func writeJSON(path string, v any) error {
	buf, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(buf, '\n'), 0o600)
}

// unavailableWindows returns the gaps of at least threshold between consecutive acknowledgement
// times (seconds), and before the first / after the last within [0, end].
func unavailableWindows(acks []float64, end, threshold float64) []window {
	acks = slices.Clone(acks)
	slices.Sort(acks)
	var out []window
	prev := 0.0
	for _, a := range append(acks, end) {
		if a-prev >= threshold {
			out = append(out, window{FromSec: round1(prev), ToSec: round1(a)})
		}
		prev = max(prev, a)
	}
	return out
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

// writeSummary prints a one-line-per-scenario summary table.
func writeSummary(w io.Writer, results []scenarioResult) {
	_, _ = fmt.Fprintf(w, "\n%-18s %-13s %7s %7s %7s %7s %5s %-9s %7s %7s %5s %8s\n", "SCENARIO", "STATUS",
		"ACKED", "UNKNOWN", "DEFFAIL", "READS", "ANOM", "PORCUPINE", "B_ACKED", "B_FAILP", "LOST", "RECOVERY")
	for _, r := range results {
		_, _ = fmt.Fprintf(w, "%-18s %-13s %7d %7d %7d %7d %5d %-9s %7d %7d %5d %7.1fs\n", r.Scenario, r.Status,
			r.Lin.Summary.Acked, r.Lin.Summary.Unknown, r.Lin.Summary.DefiniteFail, r.Lin.Summary.Reads,
			len(r.Lin.Summary.Anomalies), r.Lin.Porcupine, r.Bulk.Acked, r.Bulk.FailedPresent, len(r.Bulk.Lost),
			r.Availability.RecoverySec)
	}
}
