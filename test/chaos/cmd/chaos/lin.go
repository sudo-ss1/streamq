package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"
	"golang.org/x/sync/errgroup"

	"github.com/sudo-ss1/streamq/test/linearizability"
)

// lin workload parameters (plan M9 §2, D58).
const (
	linTopic      = "lin"
	linClients    = 4
	linReadPct    = 30                     // percentage of operations that are whole-partition reads
	linPaceMin    = 60 * time.Millisecond  // pause between a client's successful operations,
	linPaceJitter = 60 * time.Millisecond  // bounding the history so the check stays tractable
	linBackoffMin = 50 * time.Millisecond  // backoff after an error: 50–200 ms (R2b)
	linBackoffJit = 150 * time.Millisecond //
	linCheckLimit = 60 * time.Second       // porcupine budget (D58)
	linSentinels  = 300                    // bounded sentinel attempts (R16)
	linFinalWait  = 60 * time.Second       // bounded wait for the final read to reach the sentinel
)

// linWorkload runs the recorder's clients against the lin partition.
type linWorkload struct {
	rec    *linearizability.Recorder
	leader linearizability.LeaderFunc
	cancel context.CancelFunc
	g      *errgroup.Group
}

// startLin starts linClients concurrent clients; each one appends unique values "<client>-<n>"
// or reads the whole partition, single attempt, backing off 50–200 ms (seeded) after an error.
func startLin(parent context.Context, cl *cluster, seed uint64) *linWorkload {
	ctx, cancel := context.WithCancel(parent)
	w := &linWorkload{rec: linearizability.NewRecorder(), leader: cl.leaderFunc(linTopic, 0), cancel: cancel, g: &errgroup.Group{}}
	for id := range linClients {
		w.g.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(id), seed)) //nolint:gosec // a seeded, reproducible schedule, not security
			for n := 0; ctx.Err() == nil; n++ {
				var ok bool
				if rng.IntN(100) < linReadPct {
					ok = w.rec.Read(ctx, id, w.leader, linTopic, 0)
				} else {
					ok = w.rec.Append(ctx, id, w.leader, linTopic, 0, fmt.Sprintf("%d-%d", id, n))
				}
				d := linPaceMin + time.Duration(rng.Int64N(int64(linPaceJitter)))
				if !ok {
					d = linBackoffMin + time.Duration(rng.Int64N(int64(linBackoffJit)))
				}
				wait(ctx, d)
			}
			return nil
		})
	}
	return w
}

// stop stops and joins the clients (R16: before settling).
func (w *linWorkload) stop() {
	w.cancel()
	_ = w.g.Wait()
}

// ackTimes returns the return times (seconds since the recorder started) of acknowledged appends.
func (w *linWorkload) ackTimes() []float64 {
	var out []float64
	for _, op := range w.rec.History() {
		in, o := op.Input.(linearizability.Input), op.Output.(linearizability.Output) //nolint:forcetypeassert // recorded by the recorder
		if in.Kind == linearizability.AppendOp && !o.Unknown {
			out = append(out, time.Duration(op.Return).Seconds())
		}
	}
	return out
}

// settleAndCheck appends unique sentinels until one is acknowledged at offset s, reads the whole
// partition until it reaches past s, then runs the anomaly, VerifyFinal and porcupine checks in
// that order (R16, R23, D58). vizPath receives the visualization when the check fails.
func (w *linWorkload) settleAndCheck(ctx context.Context, vizPath string) (linResult, []string) {
	res := linResult{SentinelOffset: -1, Porcupine: "skipped", VerifyFinal: "skipped"}
	var violations []string
	for k := 0; res.SentinelOffset < 0 && k < linSentinels && ctx.Err() == nil; k++ {
		v := fmt.Sprintf("sentinel-%d", k)
		if w.rec.Append(ctx, linClients, w.leader, linTopic, 0, v) {
			for _, op := range w.rec.History() {
				if op.Input.(linearizability.Input).Value == v { //nolint:forcetypeassert // recorded by the recorder
					res.SentinelOffset = op.Output.(linearizability.Output).Offset //nolint:forcetypeassert // see above
				}
			}
			continue
		}
		wait(ctx, 200*time.Millisecond)
	}
	ops := w.rec.History()
	res.Summary = w.rec.Summary()
	res.Ops = len(ops)
	for _, a := range res.Summary.Anomalies {
		violations = append(violations, "lin read anomaly: "+a.Error())
	}
	if res.SentinelOffset < 0 {
		res.SettleError = fmt.Sprintf("no sentinel acknowledged in %d attempts", linSentinels)
		return res, violations
	}

	var final []string
	fctx, cancel := context.WithTimeout(ctx, linFinalWait)
	defer cancel()
	for {
		api, err := w.leader(fctx)
		if err == nil {
			values, rerr := linearizability.ReadAll(fctx, api, linTopic, 0)
			var anomaly *linearizability.Anomaly
			if errors.As(rerr, &anomaly) {
				res.FinalAnomaly = anomaly.Error()
				violations = append(violations, "lin final read anomaly: "+anomaly.Error())
				return res, violations
			}
			if rerr == nil && int64(len(values)) > res.SentinelOffset {
				final = values
				break
			}
		}
		if !wait(fctx, 100*time.Millisecond) {
			res.SettleError = fmt.Sprintf("final read did not reach the sentinel at offset %d within %v", res.SentinelOffset, linFinalWait)
			violations = append(violations, "lin: "+res.SettleError)
			return res, violations
		}
	}
	res.FinalLen = len(final)

	if err := linearizability.VerifyFinal(final, ops); err != nil {
		res.VerifyFinal = err.Error()
		violations = append(violations, "lin VerifyFinal: "+err.Error())
		return res, violations
	}
	res.VerifyFinal = "ok"
	prepared := linearizability.Prepare(ops, final)
	res.Pruned = len(ops) - len(prepared)
	res.UnknownInFinal = res.Summary.Unknown - res.Pruned
	start := time.Now()
	check, info := linearizability.Check(prepared, linCheckLimit)
	res.CheckSec = round1(time.Since(start).Seconds())
	switch check {
	case porcupine.Ok:
		res.Porcupine = "Ok"
	case porcupine.Illegal:
		res.Porcupine = "Illegal"
		violations = append(violations, "lin history is not linearizable")
	default:
		res.Porcupine = "Unknown"
		violations = append(violations, fmt.Sprintf("lin porcupine check inconclusive after %v (D58: counts as a failure)", linCheckLimit))
	}
	if check != porcupine.Ok {
		if err := linearizability.WriteVisualization(vizPath, info); err == nil {
			res.Visualization = filepath.Base(vizPath)
		} else {
			res.Visualization = "error: " + err.Error()
		}
	}
	return res, violations
}
