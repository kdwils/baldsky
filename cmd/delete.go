package cmd

import "github.com/spf13/cobra"

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "delete something",
	Long:  `delete something`,
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
