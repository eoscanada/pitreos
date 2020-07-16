package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/eoscanada/pitreos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var timestampString string

var restoreCmd = &cobra.Command{
	Use:   "restore [tag|backup name] {destination path} <filter>",
	Short: "Restores your files to a specified point in time (default: latest available)",
	Example: `
  pitreos restore 2018-08-28-18-15-45--default ../mydata -c
  pitreos restore default ../mydata -c
`,
	Long: `Restores your files to the closest available backup before
the requested timestamp (default: now).
It compares existing chunks of data in your files and downloads only the necessary data.
This is optimized for large and sparse files, like virtual machines disks or nodeos state.

Optionally specify a 'filter' argument to only download files matching the filter arguments.
The 'filter' argument is interpreted as a Golang Regexp (Perl compatible) when provided.`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		backupName := args[0]
		destPath := args[1]
		stringFilter := ""

		if len(args) > 2 {
			stringFilter = args[2]
		}

		filter, err := pitreos.NewIncludeThanExcludeFilter(stringFilter, "")
		errorCheck("unable to create include filter", err)

		if !strings.Contains(args[0], "--") {
			fmt.Println("Getting lastest backup from storage")
			lastBackup, err := pitr.GetLatestBackup(backupName)
			errorCheck("Getting last available backup", err)

			if lastBackup == "" {
				errorCheck("getting last backups", errors.New("last available backup found empty"))
			}

			backupName = lastBackup
		}

		fmt.Printf("Restoring backup %q to destination %q (filter %s)\n", backupName, destPath, filter)
		err = pitr.RestoreFromBackup(destPath, backupName, filter)
		errorCheck("restoring from backup", err)

		fmt.Printf("Restoration of backup completed\n")
	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)
}
