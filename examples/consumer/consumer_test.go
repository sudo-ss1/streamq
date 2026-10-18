package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sudo-ss1/streamq/internal/testnode"
	"github.com/sudo-ss1/streamq/pkg/client"
)

func TestConsumerExampleEndToEnd(t *testing.T) {
	addr := testnode.Start(t)
	ctx := context.Background()
	c, err := client.New(client.Config{Bootstrap: []string{addr}})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()
	require.NoError(t, client.NewAdmin(c).CreateTopic(ctx, "demo", 3, 1, map[string]string{"min_insync_replicas": "1"}))
	p, err := client.NewProducer(c, client.ProducerConfig{})
	require.NoError(t, err)
	for i := range 30 {
		require.NoError(t, p.Produce(ctx, client.Record{Topic: "demo", Key: []byte(fmt.Sprint("k", i%3)), Value: []byte(fmt.Sprint("v", i))}, nil))
	}
	require.NoError(t, p.Close(ctx))

	var out bytes.Buffer
	require.NoError(t, run(ctx, []string{"--bootstrap", addr, "--topic", "demo"}, &out))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Equal(t, "consumed 30 records from demo", lines[len(lines)-1])
	require.Len(t, lines, 31)
}
