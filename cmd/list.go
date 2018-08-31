package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var listCmd = &cobra.Command{
	Use:   "list [prefix]",
	Short: "Lists available backups on the selected storage",
	Example: `  pitreos list -l 30 -s gs://mybackups/nodeos

    This will list the 30 most recent backups in Google Storage at gs://mybackups/nodeos

  pitreos list 2018-08 -l 30

    Lists 20 backups from the month of August 2018.
`,
	Long: `Lists available backups on the selected storage`,
	Run: func(cmd *cobra.Command, args []string) {

		pitr := getPITR(viper.GetString("store"))

		limit := viper.GetInt("limit")
		offset := viper.GetInt("offset")
		long := viper.GetBool("long")

		var prefix string
		if len(args) == 1 {
			prefix = args[0]
		}

		list, err := pitr.ListBackups(limit, offset, prefix, long)
		errorCheck("listing backups", err)

		fmt.Println("")
		fmt.Printf("Backups found:\n")
		for _, b := range list {
			if b.Meta != nil {
				cnt, err := json.Marshal(b.Meta)
				if err != nil {
					fmt.Println("  ERROR decoding following backup's meta:", err)
				}
				fmt.Printf("- %s\t%s\n", b.Name, string(cnt))
			} else {
				fmt.Printf("- %s\n", b.Name)
			}
		}
		fmt.Println("")
		fmt.Printf("Total: %d\n", len(list))
		fmt.Println("")
	},
}

func init() {
	RootCmd.AddCommand(listCmd)

	listCmd.Flags().IntP("limit", "l", 20, "Limit on how many backups to return")
	listCmd.Flags().IntP("offset", "o", 0, "List backups starting at offset")
	listCmd.Flags().Bool("long", false, "Print metadata for each backup")

	for _, flag := range []string{"limit", "offset", "prefix", "long"} {
		if err := viper.BindPFlag(flag, listCmd.Flags().Lookup(flag)); err != nil {
			panic(err)
		}
	}
}
