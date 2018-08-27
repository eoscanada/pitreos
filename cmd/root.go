package cmd

import (
	"fmt"
	"os"
	"path"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

var (
	Version   = "Development version"
	BuildHash = "No BuildHash Provided"
	BuildTime = "No BuildTime Provided"
)

var (
	chunkSizeMiB           int64
	threads                int
	transferTimeoutSeconds int
	cfgFile                string
	cacheDir               string
	caching                bool
	appendonlyOptimization bool
	appendonlyFiles        []string
	backupStorageURL string
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

	RootCmd.AddCommand(backupCmd)
	RootCmd.AddCommand(restoreCmd)
	RootCmd.AddCommand(versionCmd)

	RootCmd.PersistentFlags().StringVarP(&backupStorageURL, "store", "s", "", "Storage URL, like gs://bucket/path or file:///path/to/storage")

	RootCmd.PersistentFlags().Int64Var(&chunkSizeMiB, "chunk-size", 50, "Size in MiB of the chunks when splitting the file")
	RootCmd.PersistentFlags().IntVar(&threads, "threads", 24, "Number of threads for concurrent hashing and transfer")
	RootCmd.PersistentFlags().IntVar(&transferTimeoutSeconds, "timeout", 300, "Timeout in seconds for each and every chunk transfer")

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
