package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// maxMsgSize is the gRPC send/receive limit, matching the broker (plan M2 R12).
const maxMsgSize = 64 << 20

// dialFunc opens a client connection to addr.
type dialFunc func(ctx context.Context, addr string) (*grpc.ClientConn, error)

// options holds the dependencies of the command tree that tests replace.
type options struct {
	dial dialFunc
	now  func() time.Time
}

// app holds the global flags and I/O shared by every subcommand.
type app struct {
	opts      options
	in        io.Reader
	out       io.Writer
	bootstrap string
	timeout   time.Duration
}

// newRootCmd builds the streamqctl command tree. Input for produce is read from in; all output
// goes to out (and diagnostics to errOut) via fmt.Fprint*, never fmt.Print*.
func newRootCmd(in io.Reader, out, errOut io.Writer) *cobra.Command {
	return newRootCmdWithOptions(in, out, errOut, options{dial: dial, now: time.Now})
}

// newRootCmdWithOptions is newRootCmd with injectable dependencies.
func newRootCmdWithOptions(in io.Reader, out, errOut io.Writer, opts options) *cobra.Command {
	a := &app{opts: opts, in: in, out: out}
	root := &cobra.Command{
		Use:          "streamqctl",
		Short:        "Command-line client for streamq",
		SilenceUsage: true,
	}
	root.SetIn(in)
	root.SetOut(out)
	root.SetErr(errOut)
	root.PersistentFlags().StringVar(&a.bootstrap, "bootstrap", "localhost:9092", "broker address (host:port)")
	root.PersistentFlags().DurationVar(&a.timeout, "timeout", 10*time.Second, "timeout for each request")
	root.AddCommand(a.topicsCmd(), a.produceCmd(), a.consumeCmd(), a.groupsCmd(), a.clusterCmd())
	return root
}

// dial connects to addr with insecure credentials and 64 MiB message limits. grpc.NewClient
// connects lazily, so ctx is unused; it is part of the signature for test dialers.
func dial(_ context.Context, addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}
	return conn, nil
}

// client dials the bootstrap broker. The caller must call the returned close func.
func (a *app) client(ctx context.Context) (streamqv1.StreamQServiceClient, func(), error) {
	conn, err := a.opts.dial(ctx, a.bootstrap)
	if err != nil {
		return nil, nil, err
	}
	return streamqv1.NewStreamQServiceClient(conn), func() { _ = conn.Close() }, nil
}

// rpcCtx bounds a single request by --timeout.
func (a *app) rpcCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, a.timeout)
}

// codeName renders an ErrorCode without its ERROR_CODE_ prefix.
func codeName(c streamqv1.ErrorCode) string {
	return strings.TrimPrefix(c.String(), "ERROR_CODE_")
}

// apiError formats a non-NONE error code with its optional message.
func apiError(what string, code streamqv1.ErrorCode, msg string) error {
	if msg != "" {
		return fmt.Errorf("%s: %s: %s", what, codeName(code), msg)
	}
	return fmt.Errorf("%s: %s", what, codeName(code))
}

// topicMetadata fetches metadata for one topic and fails on a topic-level error.
func (a *app) topicMetadata(ctx context.Context, c streamqv1.StreamQServiceClient, topic string) (*streamqv1.TopicMetadata, error) {
	rctx, cancel := a.rpcCtx(ctx)
	defer cancel()
	resp, err := c.Metadata(rctx, &streamqv1.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("metadata for topic %q: %w", topic, err)
	}
	for _, t := range resp.GetTopics() {
		if t.GetName() != topic {
			continue
		}
		if t.GetErrorCode() != streamqv1.ErrorCode_ERROR_CODE_NONE {
			return nil, apiError(fmt.Sprintf("metadata for topic %q", topic), t.GetErrorCode(), "")
		}
		if len(t.GetPartitions()) == 0 {
			return nil, fmt.Errorf("metadata for topic %q: no partitions", topic)
		}
		return t, nil
	}
	return nil, apiError(fmt.Sprintf("metadata for topic %q", topic), streamqv1.ErrorCode_ERROR_CODE_UNKNOWN_TOPIC_OR_PARTITION, "")
}
