package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/testnode"
)

func TestProducerExampleEndToEnd(t *testing.T) {
	addr := testnode.Start(t)
	var out bytes.Buffer
	args := []string{"--bootstrap", addr, "--topic", "demo", "--count", "500"}
	require.NoError(t, run(context.Background(), args, &out))
	require.Equal(t, "produced 500 records (0 failed) to demo\n", out.String())
	// Running again reuses the existing topic.
	out.Reset()
	require.NoError(t, run(context.Background(), args, &out))
	require.Contains(t, out.String(), "produced 500")
}
