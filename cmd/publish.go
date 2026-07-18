package cmd

import "github.com/spf13/cobra"

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "publish something",
	Long:  `publish something`,
}

func init() {
	rootCmd.AddCommand(publishCmd)
}
