package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists available backups on the selected storage",
	Example: `  pitreos list -l 30 -s gs://mybackups/nodeos

    This will list the 30 most recent backups in Google Storage at gs://mybackups/nodeos
`,
	Long: `Lists available backups on the selected storage`,
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		howMany := viper.GetInt("limit")
		detailed := viper.GetBool("long")
		err := pitr.ListBackups(howMany, detailed)
		errorCheck("listing backups", err)
	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	listCmd.Flags().IntP("limit", "l", 20, "Limit on how many backups to return")
	listCmd.Flags().Bool("long", false, "use detailed (long) output")

	for _, flag := range []string{"limit", "long"} {
		if err := viper.BindPFlag(flag, listCmd.Flags().Lookup(flag)); err != nil {
			panic(err)
		}
	}
}
