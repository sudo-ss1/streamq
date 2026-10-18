package main

import (
	"errors"

	"github.com/spf13/cobra"
)

// clusterCmd builds `cluster status` (FR11). Contract stub: package B implements it.
func (a *app) clusterCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "cluster", Short: "Inspect the cluster"}
	cmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show brokers, the controller and partition health",
		Args:  cobra.NoArgs,
		RunE:  func(*cobra.Command, []string) error { return errors.New("not implemented") },
	})
	return cmd
}
