package cmd

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

var timestampString string

var restoreCmd = &cobra.Command{
	Use:   "restore {SOURCE} {DESTINATION}",
	Short: "Restores your files to a specified point in time (default: latest available)",
	Long:  "Restores your files to the closest available backup before the requested timestamp (default: now) by comparing existing chunks of data in your files and downloading only the necessary data. This is optimized for large files.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			fmt.Println("This command requires two arguments: SOURCE and DESTINATION")
			os.Exit(1)
		}

		pitr := getPITR()
		t, err := parseUnixTimestamp(timestampString)
		if err != nil {
			fmt.Printf("Got error: %s\n", err)
			os.Exit(1)
		}
		err = pitr.RestoreFromBackup(args[0], args[1], t)
		if err != nil {
			fmt.Printf("Got error: %s\n", err)
			os.Exit(1)
		}
	},
	Example: `  pitreos restore gs://mybackups/projectname file:///home/nodeos/data -c --timestamp $(date -d "2 hours ago" +%s)`,
}

func parseUnixTimestamp(unixTimeStamp string) (tm time.Time, err error) {
	i, err := strconv.ParseInt(unixTimeStamp, 10, 64)
	if err != nil {
		return
	}
	tm = time.Unix(i, 0)
	return
}

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

func initRestore() {
	restoreCmd.Flags().StringVarP(&timestampString, "timestamp", "t", "", "Timestamp before which we want the latest available backup")
	restoreCmd.SetUsageTemplate(restoreUsageTemplate)
	if timestampString == "" {
		timestampString = strconv.FormatInt(time.Now().Unix(), 10)
	}
}
