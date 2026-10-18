package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/testnode"
)

// TestProduceConsumeRouteToLeaders: on a 3-broker cluster most partitions are led by brokers
// other than the bootstrap, so produce and consume must route to leaders.
func TestProduceConsumeRouteToLeaders(t *testing.T) {
	c := testnode.StartCluster(t, 3, nil)
	run := func(in string, args ...string) (string, error) {
		var out, errOut bytes.Buffer
		cmd := newRootCmd(strings.NewReader(in), &out, &errOut)
		cmd.SetArgs(append([]string{"--bootstrap", c.Addr(1), "--timeout", "10s"}, args...))
		err := cmd.ExecuteContext(context.Background())
		return out.String(), err
	}
	var err error
	require.Eventually(t, func() bool {
		_, err = run("", "topics", "create", "routed", "--partitions", "6", "--replication-factor", "3")
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "create topic: %v", err)

	var lines, want []string
	for i := range 60 {
		lines = append(lines, fmt.Sprintf("k%d:v%d", i%12, i))
		want = append(want, fmt.Sprintf("v%d", i))
	}
	// Produce exactly once (a retry could duplicate records), after every partition is healthy.
	require.Eventually(t, func() bool {
		_, err = run("", "cluster", "status", "--strict")
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "healthy: %v", err)
	require.Eventually(t, func() bool {
		_, err = run("", "consume", "--topic", "routed", "--from-beginning") // every leader has opened its partition
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "partitions open: %v", err)
	_, err = run(strings.Join(lines, "\n")+"\n", "produce", "--topic", "routed", "--key-separator", ":")
	require.NoError(t, err)

	out, err := run("", "consume", "--topic", "routed", "--from-beginning")
	require.NoError(t, err)
	got := strings.Split(strings.TrimSpace(out), "\n")
	slices.Sort(got)
	slices.Sort(want)
	require.Equal(t, want, got)
}
