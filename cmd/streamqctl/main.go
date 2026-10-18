// Command streamqctl is the command-line client for administering and using a streamq cluster.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	os.Exit(run())
}

// run executes the command tree with a context cancelled on SIGINT/SIGTERM (which ends
// `consume --follow` cleanly) and returns the process exit code.
func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newRootCmd(os.Stdin, os.Stdout, os.Stderr).ExecuteContext(ctx); err != nil {
		return 1
	}
	return 0
}
