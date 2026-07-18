package cmd

import "github.com/spf13/cobra"

var updateCmd = &cobra.Command{
	Use:   "update",
	Short: "update something",
	Long:  `update something`,
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
