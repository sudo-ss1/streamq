// Package linearizability checks single-partition produce (acks=all) / fetch histories against a
// sequential log model with porcupine (SPEC §9 M9, DECISIONS D56–D58). It is shared by the
// in-process test and the chaos runner.
package linearizability

import (
	"fmt"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

// sep terminates every value in the encoded log, so a prefix of the encoding at an entry
// boundary is exactly a prefix of the log.
const sep = "\x00"

// state is the committed log: n entries, encoded as value+sep for each. It is immutable and
// comparable with ==, so porcupine's search branches never share mutable state.
type state struct {
	n   int
	log string
}

// Kind is an operation kind.
type Kind int

// Operation kinds.
const (
	AppendOp Kind = iota
	ReadOp
)

// Input is an operation's input.
type Input struct {
	Kind  Kind
	Value string // AppendOp
}

// Output is an operation's output.
type Output struct {
	Unknown bool     // AppendOp: the outcome is not known; recorded with Return = ∞
	Offset  int64    // AppendOp, when known: the record's offset
	Values  []string // ReadOp: the partition's values from offset 0, in order
}

func encode(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, sep) + sep
}

// Model is the deterministic log model (D56):
//   - Append acknowledged at offset o: legal iff the log has o entries; the value is appended.
//   - Append with an unknown outcome: applies (its return is at infinity, so linearizing it at the
//     end of the history means it never happened).
//   - Read: legal iff the values read are a prefix of the log. Reads check order and phantoms;
//     durability comes from the acknowledged offsets plus VerifyFinal.
func Model() porcupine.Model {
	return porcupine.Model{
		Init: func() any { return state{} },
		Step: func(s, in, out any) (bool, any) {
			st := s.(state)   //nolint:forcetypeassert // the model only ever holds state
			i := in.(Input)   //nolint:forcetypeassert // inputs are recorded by this package
			o := out.(Output) //nolint:forcetypeassert // outputs are recorded by this package
			switch i.Kind {
			case AppendOp:
				if !o.Unknown && o.Offset != int64(st.n) {
					return false, st
				}
				return true, state{n: st.n + 1, log: st.log + i.Value + sep}
			case ReadOp:
				return len(o.Values) <= st.n && strings.HasPrefix(st.log, encode(o.Values)), st
			}
			return false, st
		},
		DescribeOperation: func(in, out any) string {
			i, o := in.(Input), out.(Output) //nolint:forcetypeassert // see Step
			if i.Kind == ReadOp {
				return fmt.Sprintf("read() -> %d values", len(o.Values))
			}
			if o.Unknown {
				return fmt.Sprintf("append(%s) -> ?", i.Value)
			}
			return fmt.Sprintf("append(%s) -> %d", i.Value, o.Offset)
		},
		DescribeState: func(s any) string {
			return fmt.Sprintf("%d entries", s.(state).n) //nolint:forcetypeassert // model states
		},
	}
}

// Check checks ops for linearizability against Model within timeout. A timeout returns
// porcupine.Unknown, which callers must treat as a failure (inconclusive, D58).
func Check(ops []porcupine.Operation, timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	return porcupine.CheckOperationsVerbose(Model(), ops, timeout)
}

// WriteVisualization writes porcupine's HTML visualization of a check to path.
func WriteVisualization(path string, info porcupine.LinearizationInfo) error {
	return porcupine.VisualizePath(Model(), info, path)
}

// Prepare readies a history whose settled final log already passed VerifyFinal for the porcupine
// check (D56, D58):
//   - an unknown append whose value is in final is pinned to its offset there, so the search
//     never has to permute it (a bad history then fails precisely instead of timing out);
//   - an unknown append whose value is not in final is dropped: it never happened (VerifyFinal
//     guarantees no read saw it);
//   - unknown returns are set to just after the last finite timestamp, which is equivalent to
//     infinity for the check and keeps the visualization readable.
func Prepare(ops []porcupine.Operation, final []string) []porcupine.Operation {
	index := make(map[string]int64, len(final))
	for i, v := range final {
		index[v] = int64(i)
	}
	var end int64
	for _, op := range ops {
		if !op.Output.(Output).Unknown { //nolint:forcetypeassert // recorded by this package
			end = max(end, op.Return)
		}
	}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		in, o := op.Input.(Input), op.Output.(Output) //nolint:forcetypeassert // recorded by this package
		if in.Kind == AppendOp && o.Unknown {
			off, ok := index[in.Value]
			if !ok {
				continue
			}
			op.Output = Output{Offset: off}
			op.Return = max(end+1, op.Call+1)
		}
		out = append(out, op)
	}
	return out
}

// VerifyFinal checks a settled final read of the whole partition for durability (D56): every
// acknowledged (value, offset) is at that offset, every read is a prefix of it, every value in it
// was appended by some operation, and no value appears twice.
func VerifyFinal(final []string, ops []porcupine.Operation) error {
	known := map[string]bool{}
	seen := map[string]bool{}
	for _, v := range final {
		if seen[v] {
			return fmt.Errorf("value %q appears twice", v)
		}
		seen[v] = true
	}
	finalEnc := encode(final)
	for _, op := range ops {
		in, out := op.Input.(Input), op.Output.(Output) //nolint:forcetypeassert // recorded by this package
		switch in.Kind {
		case AppendOp:
			known[in.Value] = true
			if !out.Unknown && (out.Offset >= int64(len(final)) || final[out.Offset] != in.Value) {
				return fmt.Errorf("acknowledged value %q at offset %d is not there in the final log", in.Value, out.Offset)
			}
		case ReadOp:
			if !strings.HasPrefix(finalEnc, encode(out.Values)) {
				return fmt.Errorf("a read of %d values is not a prefix of the final log", len(out.Values))
			}
		}
	}
	for _, v := range final {
		if !known[v] {
			return fmt.Errorf("value %q in the log was never appended", v)
		}
	}
	return nil
}
