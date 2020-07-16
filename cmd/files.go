package cmd

import (
	"fmt"

	"github.com/eoscanada/pitreos"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var filesCmd = &cobra.Command{
	Use:   "files [tag|backup name] <filter>",
	Short: "Lists available files in the specified backup on the selected storage",
	Example: `
  pitreos files 2018-08-28-18-15-45--default
`,
	Long: `List available files in the closest available backup before
the requested timestamp (default: now).

Optionally specify a 'filter' argument to only show files matching the filter arguments.
The 'filter' argument is interpreted as a Golang Regexp (Perl compatible) when provided.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		backupName := args[0]
		stringFilter := ""

		if len(args) > 1 {
			stringFilter = args[1]
		}

		filter, err := pitreos.NewIncludeThanExcludeFilter(stringFilter, "")
		errorCheck("unable to create include filter", err)

		fmt.Printf("Listing backup %q files (filter %s)\n", backupName, filter)
		resolvedName := resolveBackupName(pitr, backupName)
		if resolvedName != backupName {
			fmt.Printf("Resolved backup name input to %q\n", resolvedName)
		}

		err = pitr.ListBackupFiles(resolvedName, filter)
		errorCheck("listing backup's files", err)
	},
}

func init() {
	RootCmd.AddCommand(filesCmd)
}
