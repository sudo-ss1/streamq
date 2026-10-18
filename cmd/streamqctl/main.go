// Command streamqctl is the command-line client for administering and using a streamq cluster.
package main

import (
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	logger.Info("streamqctl: not implemented yet")
}
