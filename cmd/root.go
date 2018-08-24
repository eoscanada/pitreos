package cmd

import (
	"fmt"
	"os"
	"path"

	pitreos "github.com/eoscanada/pitreos/lib"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

var (
	Version   = "No Version Provided"
	BuildHash = "No BuildHash Provided"
	BuildTime = "No BuildTime Provided"
)

var (
	cfgFile                string
	cacheDir               string
	caching                bool
	appendonlyOptimization bool
	appendonlyFiles        []string
)

var RootCmd = &cobra.Command{
	Use:   "pitreos",
	Short: "Point-in-time Recovery Tool by EOS Canada",
	Long: `Pitreos - Point-in-time Recovery Tool by EOS Canada
	
Pitreos is a very fast backup and restore command based on chunks 
comparison, optimized for very large files, sparse files and append-only 
files like the ones that you get when running Nodeos.
Supports local storage, Google Cloud Storage and local caching.`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initCaching)

	initBackup()
	RootCmd.AddCommand(backupCmd)
	initRestore()
	RootCmd.AddCommand(restoreCmd)
	RootCmd.AddCommand(versionCmd)

	RootCmd.PersistentFlags().StringVar(&cacheDir, "cache-dir", "", "Cache directory (default is $HOME/.pitreos/cache)")
	RootCmd.PersistentFlags().BoolVarP(&caching, "enable-caching", "c", false, "Keep/use a copy of every block file sent")
	RootCmd.PersistentFlags().BoolVar(&appendonlyOptimization, "use-appendonly", true, "Use the optimizations on 'appendonly-files'")
	RootCmd.PersistentFlags().StringSliceVarP(&appendonlyFiles, "appendonly-files", "a", []string{"blocks/blocks.log"}, "Files treated as appendonly")

}

func initCaching() {
	if cacheDir != "" {
		return
	}
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	cacheDir = path.Join(home, ".pitreos", "cache")
}

func getPITR() *pitreos.PITR {
	pitr := pitreos.New()
	pitr.CacheDir = cacheDir
	pitr.Caching = caching
	pitr.AppendonlyOptimization = appendonlyOptimization
	pitr.AppendonlyFiles = appendonlyFiles
	return pitr
}
