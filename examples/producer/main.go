// Command producer is an example streamq producer: it creates a topic if needed and produces
// keyed records asynchronously, then closes the producer (which flushes).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
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
	fs := flag.NewFlagSet("producer", flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "localhost:9092", "broker address")
	topic := fs.String("topic", "example", "topic")
	partitions := fs.Int("partitions", 3, "partitions if the topic is created")
	count := fs.Int("count", 1000, "records to produce")
	keys := fs.Int("keys", 10, "distinct keys")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := client.New(client.Config{Bootstrap: []string{*bootstrap}})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	// Single-node friendly: RF 1 and min.insync 1 so acks=all works with one broker (D16).
	err = client.NewAdmin(c).CreateTopic(ctx, *topic, int32(*partitions), 1, map[string]string{"min_insync_replicas": "1"})
	if err != nil && !errors.Is(err, client.ErrTopicAlreadyExists) {
		return fmt.Errorf("create topic: %w", err)
	}

	p, err := client.NewProducer(c, client.ProducerConfig{})
	if err != nil {
		return err
	}
	var acked, failed atomic.Int64
	for i := range *count {
		rec := client.Record{
			Topic: *topic,
			Key:   []byte(fmt.Sprintf("key-%d", i%*keys)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		err := p.Produce(ctx, rec, func(_ client.RecordMetadata, err error) {
			if err != nil {
				failed.Add(1)
				return
			}
			acked.Add(1)
		})
		if err != nil {
			_ = p.Close(ctx)
			return fmt.Errorf("produce: %w", err)
		}
	}
	if err := p.Close(ctx); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}
	say(out, "produced %d records (%d failed) to %s\n", acked.Load(), failed.Load(), *topic)
	if failed.Load() > 0 {
		return fmt.Errorf("%d records failed", failed.Load())
	}
	return nil
}
