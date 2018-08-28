package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var timestampString string

var restoreCmd = &cobra.Command{
	Use:   "restore [-t tagname|backup name] {destination path}",
	Short: "Restores your files to a specified point in time (default: latest available)",
	Example: `
  pitreos restore 2018-08-28-18-15-45--default ../mydata -c 
  pitreos restore -t default ../mydata -c
`,
	Long: `Restores your files to the closest available backup before
the requested timestamp (default: now).
It compares existing chunks of data in your files and downloads only the necessary data.
This is optimized for large and sparse files, like virtual machines disks or nodeos state.`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		tag := viper.GetBool("restoretag")

		if tag {
			lastBackup, err := pitr.GetLatestBackup(args[0])
			errorCheck("Getting last available backup", err)
			if lastBackup == "" {
				fmt.Printf("ERROR: %s: %s\n", "Last available backup found empty string", err)
				os.Exit(1)
			}
			err = pitr.RestoreFromBackup(args[1], lastBackup)
			errorCheck("restoring from backup", err)
			return
		}
		err := pitr.RestoreFromBackup(args[1], args[0])
		errorCheck("restoring from backup", err)
	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().BoolP("tag", "t", false, "If set, source will be treated as a tag instead of a full backup name")

	for _, flag := range []string{"tag"} {
		if err := viper.BindPFlag("restore"+flag, restoreCmd.Flags().Lookup(flag)); err != nil {
			panic(err)
		}
	}

}
