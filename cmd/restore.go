package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var timestampString string

var restoreCmd = &cobra.Command{
	Use:     "restore [backup name] [destination path]",
	Short:   "Restores your files to a specified point in time (default: latest available)",
	Example: `  pitreos restore gs://mybackups/projectname file:///home/nodeos/data -c --timestamp $(date -d "2 hours ago" +%s)`,
	Long: `Restores your files to the closest available backup before
the requested timestamp (default: now).
It compares existing chunks of data in your files and downloads only the necessary data.
This is optimized for large and sparse files, like virtual machines disks or nodeos state.`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		err := pitr.RestoreFromBackup(args[1], args[0])
		errorCheck("restoring from backup", err)
	},
}

func init() {
	RootCmd.AddCommand(restoreCmd)

	restoreCmd.SetUsageTemplate(restoreUsageTemplate)
}

// adding the "Args" definition (SOURCE / DESTINATION) right below the USAGE definition
var restoreUsageTemplate = `Usage:{{if .Runnable}}
  {{if .HasAvailableFlags}}{{appendIfNotPresent .UseLine "[flags]"}}{{else}}{{.UseLine}}{{end}}{{end}}{{if .HasAvailableSubCommands}}
  {{ .CommandPath}} [command]{{end}}
  * SOURCE: File path (ex: /var/backups) or Google Storage URL (ex: gs://mybackups/projectname)
  * DESTINATION: File path (ex: ../mydata)
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
