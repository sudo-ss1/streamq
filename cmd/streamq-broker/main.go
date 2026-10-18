// Command streamq-broker runs a streamq broker node.
package main

import (
	"log/slog"
	"os"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	logger.Info("streamq-broker: not implemented yet")
}
