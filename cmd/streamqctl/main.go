// Command streamqctl is the command-line client for administering and using a streamq cluster.
package main

import (
	"os"
)

func main() {
	if err := newRootCmd(os.Stdin, os.Stdout, os.Stderr).Execute(); err != nil {
		os.Exit(1)
	}
}
