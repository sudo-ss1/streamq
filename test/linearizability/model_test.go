package linearizability

import (
	"math"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
)

func appendOp(client int, v string, call, ret int64, off int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: Input{Kind: AppendOp, Value: v}, Call: call, Output: Output{Offset: off}, Return: ret}
}

func unknownOp(client int, v string, call int64) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: Input{Kind: AppendOp, Value: v}, Call: call, Output: Output{Unknown: true}, Return: math.MaxInt64}
}

func readOp(client int, call, ret int64, values ...string) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Input: Input{Kind: ReadOp}, Call: call, Output: Output{Values: values}, Return: ret}
}

func TestModel(t *testing.T) {
	tests := []struct {
		name string
		ops  []porcupine.Operation
		want porcupine.CheckResult
	}{
		{"sequential", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 1), readOp(1, 4, 5, "a", "b"),
		}, porcupine.Ok},
		{"concurrent read misses in-flight append", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 10, 1), readOp(1, 3, 4, "a"),
		}, porcupine.Ok},
		{"concurrent read sees in-flight append", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 10, 1), readOp(1, 3, 4, "a", "b"),
		}, porcupine.Ok},
		{"stale read: shorter committed prefix", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 1), readOp(1, 4, 5, "a"),
		}, porcupine.Ok},
		{"unknown append later appears", []porcupine.Operation{
			unknownOp(0, "a", 0), appendOp(1, "b", 5, 6, 1), readOp(2, 7, 8, "a", "b"),
		}, porcupine.Ok},
		{"unknown append never appears", []porcupine.Operation{
			unknownOp(0, "a", 0), appendOp(1, "b", 5, 6, 0), readOp(2, 7, 8, "b"),
		}, porcupine.Ok},
		{"unknown append lands after later acked appends (client gave up first)", []porcupine.Operation{
			unknownOp(0, "a", 0), appendOp(1, "w", 6, 7, 0), readOp(2, 9, 10, "w", "a"),
		}, porcupine.Ok},
		{"two concurrent unknown appends branch from the same state", []porcupine.Operation{
			unknownOp(0, "a", 0), unknownOp(1, "b", 0), readOp(2, 5, 6, "b"), readOp(2, 7, 8, "b", "a"),
		}, porcupine.Ok},
		{"lost ack: a later read lacks an acknowledged value", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 1), readOp(1, 4, 5, "b"),
		}, porcupine.Illegal},
		{"reordered", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 1), readOp(1, 4, 5, "b", "a"),
		}, porcupine.Illegal},
		{"phantom value", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), readOp(1, 2, 3, "a", "x"),
		}, porcupine.Illegal},
		{"wrong offset", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 0),
		}, porcupine.Illegal},
		{"non-monotonic reads are allowed (prefix reads, D56)", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 1), readOp(1, 4, 5, "a", "b"), readOp(1, 6, 7, "a"),
		}, porcupine.Ok},
		{"acknowledged offsets must respect real-time order", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), appendOp(0, "b", 2, 3, 2), appendOp(1, "c", 4, 5, 1),
		}, porcupine.Illegal},
		{"read regressing past an acknowledged value it saw", []porcupine.Operation{
			appendOp(0, "a", 0, 1, 0), readOp(1, 2, 3, "a"), appendOp(0, "c", 4, 5, 1), readOp(1, 6, 7, "c"),
		}, porcupine.Illegal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, _ := Check(tt.ops, 10*time.Second)
			require.Equal(t, tt.want, res)
		})
	}
}

func TestVerifyFinal(t *testing.T) {
	ops := []porcupine.Operation{appendOp(0, "a", 0, 1, 0), unknownOp(1, "u", 0), appendOp(0, "b", 2, 3, 1), readOp(2, 4, 5, "a")}
	tests := []struct {
		name  string
		final []string
		ok    bool
	}{
		{"acked at their offsets, unknown absent", []string{"a", "b"}, true},
		{"unknown present after the acked values", []string{"a", "b", "u"}, true},
		{"acked value lost", []string{"a"}, false},
		{"acked value moved to another offset", []string{"a", "u", "b"}, false},
		{"read is not a prefix", []string{"b", "a"}, false},
		{"phantom", []string{"a", "b", "z"}, false},
		{"duplicate", []string{"a", "b", "b"}, false},
	}
	for _, tt := range tests {
		err := VerifyFinal(tt.final, ops)
		if tt.ok {
			require.NoError(t, err, tt.name)
		} else {
			require.Error(t, err, tt.name)
		}
	}
}

func TestPrepare(t *testing.T) {
	ops := []porcupine.Operation{
		appendOp(0, "a", 0, 10, 0), unknownOp(1, "landed", 2), unknownOp(1, "never", 3), readOp(2, 4, 5, "a"),
	}
	final := []string{"a", "landed"}
	require.NoError(t, VerifyFinal(final, ops))
	got := Prepare(ops, final)
	require.Len(t, got, 3)
	for _, op := range got {
		in, out := op.Input.(Input), op.Output.(Output)
		require.False(t, out.Unknown)
		require.NotEqual(t, "never", in.Value)
		if in.Value == "landed" {
			require.Equal(t, int64(1), out.Offset, "pinned to its final offset")
			require.Equal(t, int64(11), op.Return, "just after the last finite return")
		}
	}
	res, _ := Check(got, 10*time.Second)
	require.Equal(t, porcupine.Ok, res)
}
