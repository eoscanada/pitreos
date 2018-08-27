package cmd

import (
	"encoding/json"

	"github.com/spf13/cobra"
)

var metadataJSON string
var backupTag string

var backupCmd = &cobra.Command{
	Use:     "backup [local_dir]",
	Short:   "Backs up your files differentially",
	Example: `  pitreos backup /home/nodeos/data gs://mybackups/projectname -c --metadata '{"blocknum": 123456, "version": "1.2.1"}'`,
	Long: `Backs up your files by slicing them into chunks and comparing
their hashes with those present at the destination.
This approach is optimized for large files`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		var metadata map[string]interface{}
		err := json.Unmarshal([]byte(metadataJSON), &metadata)
		errorCheck("unmarshaling --meta", err)

		pitr := getPITR(backupStorageURL)

		err = pitr.GenerateBackup(args[0], backupTag, metadata)
		errorCheck("storing backup", err)
	},
}

func init() {
	backupCmd.Flags().StringVarP(&metadataJSON, "meta", "m", `{}`, "Additional metadata in JSON format to store with backup")
	backupCmd.Flags().StringVarP(&backupTag, "tag", "t", "default", "Backup tag, appended to timestamp")
	backupCmd.SetUsageTemplate(backupUsageTemplate)
}

// adding the "Args" definition (SOURCE / DESTINATION) right below the USAGE definition
var backupUsageTemplate = `Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine "[flags]"}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{ .CommandPath}} [command]{{end}}
  * SOURCE: File path (ex: ../mydata)
  * DESTINATION: File path (ex: /var/backups) or Google Storage URL (ex: gs://mybackups/projectname)
  {{if gt .Aliases 0}}
Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}
Examples:
{{ .Example }}{{end}}{{ if .HasAvailableSubCommands}}
Available Commands:{{range .Commands}}{{if .IsAvailableCommand}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableLocalFlags}}
Flags:
{{.LocalFlags.FlagUsages | trimRightSpace}}{{end}}{{ if .HasAvailableInheritedFlags}}
Global Flags:
{{.InheritedFlags.FlagUsages | trimRightSpace}}{{end}}{{if .HasHelpSubCommands}}
Additional help topics:{{range .Commands}}{{if .IsHelpCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{ if .HasAvailableSubCommands }}
Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`
