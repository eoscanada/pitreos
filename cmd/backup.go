package cmd

import (
	"encoding/json"

	pitreos "github.com/eoscanada/pitreos/lib"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var metadataJSON string
var backupTag string

var backupCmd = &cobra.Command{
	Use:   "backup {local_dir} <filter>",
	Short: "Backs up your files differentially",
	Example: `  pitreos backup mydata -s gs://mybackups/projectname -t dev -c --metadata '{"blocknum": 123456, "version": "1.2.1"}'

    This will back up everything under 'mydata' to Google Storage at 'gs://mybackups/projectname'.
    The uploaded chunks will be kept in a local cache for faster restore.
    The "dev" tag can be used to differentiate backups that will share their chunks.
    The metadata will be attached to the backup and shown when listing backups with '--long' flag.
`,
	Long: `Backs up your files by slicing them into chunks and comparing
their hashes with those present at the destination.
This approach is optimized for large files.

Optionally specify a 'filter' argument to only show files matching the filter arguments.
The 'filter' argument is interpreted as a Golang Regexp (Perl compatible) when provided.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		var metadata map[string]interface{}
		err := json.Unmarshal([]byte(viper.GetString("meta")), &metadata)
		errorCheck("unmarshaling --meta", err)

		pitr := getPITR(viper.GetString("store"))

		stringFilter := ""
		if len(args) > 1 {
			stringFilter = args[1]
		}

		filter, err := pitreos.NewIncludeThanExcludeFilter(stringFilter, "")
		errorCheck("unable to create include filter", err)

		err = pitr.GenerateBackup(args[0], viper.GetString("tag"), metadata, filter)
		errorCheck("storing backup", err)
	},
}

func init() {
	RootCmd.AddCommand(backupCmd)

	backupCmd.Flags().StringP("meta", "m", `{}`, "Additional metadata in JSON format to store with backup")
	backupCmd.Flags().StringP("tag", "t", "default", "Backup tag, appended to timestamp")

	for _, flag := range []string{"meta", "tag"} {
		if err := viper.BindPFlag(flag, backupCmd.Flags().Lookup(flag)); err != nil {
			panic(err)
		}
	}
}
