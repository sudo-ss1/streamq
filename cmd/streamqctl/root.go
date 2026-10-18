package main

import (
	"io"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	streamqv1 "github.com/sudo-ss1/streamq/gen/v1"
)

// newRootCmd builds the streamqctl command tree. Input for produce is read from in; all output
// goes to out (and diagnostics to errOut) via fmt.Fprint*, never fmt.Print*.
func newRootCmd(in io.Reader, out, errOut io.Writer) *cobra.Command {
	_, _ = in, out
	root := &cobra.Command{Use: "streamqctl", Short: "Command-line client for streamq", SilenceUsage: true}
	root.SetErr(errOut)
	var _ grpc.ClientConnInterface
	var _ streamqv1.StreamQServiceClient
	return root
}
