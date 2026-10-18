package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/testnode"
)

func TestAdminExampleEndToEnd(t *testing.T) {
	addr := testnode.Start(t)
	var out bytes.Buffer
	require.NoError(t, run(context.Background(), []string{"--bootstrap", addr, "--topic", "demo"}, &out))
	require.Contains(t, out.String(), "created demo")
	require.Contains(t, out.String(), "partition 1 leader 1")
	require.Contains(t, out.String(), "topics: [demo]")
	require.Contains(t, out.String(), "deleted demo")
}
