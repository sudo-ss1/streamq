// Command admin is an example of the streamq Admin API: it creates a topic, describes and lists
// topics, and deletes the topic again.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/sudo-ss1/streamq/pkg/client"
)

func main() { os.Exit(mainCode()) }

func mainCode() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		say(os.Stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

// say writes example output; write errors to a terminal are not actionable here.
func say(w io.Writer, format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }

func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("admin", flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "localhost:9092", "broker address")
	topic := fs.String("topic", "admin-example", "topic to create and delete")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := client.New(client.Config{Bootstrap: []string{*bootstrap}})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	admin := client.NewAdmin(c)

	if err := admin.CreateTopic(ctx, *topic, 2, 1, map[string]string{"retention_ms": "3600000"}); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	say(out, "created %s\n", *topic)
	desc, err := admin.DescribeTopics(ctx, *topic)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	for _, p := range desc[0].Partitions {
		say(out, "partition %d leader %d epoch %d replicas %v isr %v\n", p.Partition, p.Leader, p.LeaderEpoch, p.Replicas, p.ISR)
	}
	names, err := admin.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	say(out, "topics: %v\n", names)
	if err := admin.DeleteTopic(ctx, *topic); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	say(out, "deleted %s\n", *topic)
	return nil
}
