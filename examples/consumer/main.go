// Command consumer is an example streamq consumer: it reads every partition of a topic from the
// beginning up to the high watermark seen at start, printing key/value pairs.
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
	fs := flag.NewFlagSet("consumer", flag.ContinueOnError)
	bootstrap := fs.String("bootstrap", "localhost:9092", "broker address")
	topic := fs.String("topic", "example", "topic")
	quiet := fs.Bool("quiet", false, "print only the summary")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := client.New(client.Config{Bootstrap: []string{*bootstrap}})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()

	admin := client.NewAdmin(c)
	desc, err := admin.DescribeTopics(ctx, *topic)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	var tps []client.TopicPartition
	end := map[client.TopicPartition]int64{}
	for _, p := range desc[0].Partitions {
		tp := client.TopicPartition{Topic: *topic, Partition: p.Partition}
		hw, err := admin.ListOffsets(ctx, tp, -1)
		if err != nil {
			return fmt.Errorf("list offsets: %w", err)
		}
		tps = append(tps, tp)
		end[tp] = hw
	}

	cons, err := client.NewConsumer(c, client.ConsumerConfig{})
	if err != nil {
		return err
	}
	defer func() { _ = cons.Close() }()
	cons.Assign(tps)
	if err := cons.SeekToBeginning(ctx); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	done := func() bool {
		for tp, hw := range end {
			if pos, ok := cons.Position(tp); !ok || pos < hw {
				return false
			}
		}
		return true
	}
	n := 0
	for !done() {
		recs, err := cons.Poll(ctx)
		if err != nil {
			return fmt.Errorf("poll: %w", err)
		}
		for _, r := range recs {
			n++
			if !*quiet {
				say(out, "%d/%d %s=%s\n", r.Partition, r.Offset, r.Key, r.Value)
			}
		}
	}
	say(out, "consumed %d records from %s\n", n, *topic)
	return nil
}
