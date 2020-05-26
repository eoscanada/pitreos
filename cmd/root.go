package cmd

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	Version   = "Development version"
	BuildHash = "No BuildHash Provided"
	BuildTime = "No BuildTime Provided"
)

var (
	cacheDir               string
	caching                bool
	appendonlyOptimization bool
	appendonlyFiles        []string
	backupStorageURL       string
)

var RootCmd = &cobra.Command{
	Use:   "pitreos",
	Short: "Point-in-time Recovery Tool by dfuse.io",
	Long: `Pitreos - Point-in-time Recovery by dfuse.io

Pitreos is a very fast backup and restore command based on chunks
comparison, optimized for very large files, sparse files and append-only
files like the ones that you get when running Nodeos.
Supports local storage, and several objects stores (GCP, AWS, AZ) in
addition to local caching.`,
}

func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defaultBackupURL := url.URL{Scheme: "file", Host: "", Path: path.Join(home, ".pitreos", "backups")}
	RootCmd.PersistentFlags().StringP("store", "s", defaultBackupURL.String(), "Storage URL, like gs://bucket/path")

	RootCmd.PersistentFlags().Int64("chunk-size", 50, "Size in MiB of the chunks when splitting the file")
	RootCmd.PersistentFlags().Int("threads", 24, "Number of threads for concurrent hashing and transfer")
	RootCmd.PersistentFlags().Int("timeout", 300, "Timeout in seconds for each and every chunk transfer")

	RootCmd.PersistentFlags().String("cache-dir", path.Join(home, ".pitreos", "cache"), "Cache directory")
	RootCmd.PersistentFlags().BoolP("enable-caching", "c", false, "Keep/use a copy of every block file sent")
	RootCmd.PersistentFlags().StringSliceP("appendonly-files", "a", []string{}, "Files treated as append-only (ex: blocks/blocks.log)")

	for _, flag := range []string{"store", "chunk-size", "threads", "timeout", "cache-dir", "enable-caching", "appendonly-files"} {
		if err := viper.BindPFlag(flag, RootCmd.PersistentFlags().Lookup(flag)); err != nil {
			panic(err)
		}
	}

}

func initConfig() {
	viper.SetEnvPrefix("PITREOS")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	viper.SetConfigName(".pitreos")

	dir, err := os.Getwd()
	if err != nil {
		log.Println("Couldn't find cwd:", err)
		os.Exit(1)
	}

	home, err := homedir.Dir()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	viper.AddConfigPath(dir)
	viper.AddConfigPath(path.Join(home))
	if err := viper.ReadInConfig(); err != nil {
		log.Println("Not using config file:", err)
		return
	}
	log.Printf("Getting configuration from file: %s\n", viper.ConfigFileUsed())
}
