package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints version, git hash and build time",
	Long:  `Pitreos version, based on provided flags on build`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Version: %s\nGit Hash: %s\nBuild time: %s\n", Version, BuildHash, BuildTime)
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
