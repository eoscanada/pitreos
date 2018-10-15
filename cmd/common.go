package cmd

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	pitreos "github.com/eoscanada/pitreos/lib"
	"github.com/spf13/viper"
)

func errorCheck(prefix string, err error) {
	if err != nil {
		fmt.Printf("ERROR: %s: %s\n", prefix, err)
		os.Exit(1)
	}
}

func getPITR(storageURL string) *pitreos.PITR {
	storage, err := pitreos.SetupStorage(storageURL)
	errorCheck("setting up storage", err)

	log.Println("Using storage:", storageURL)

	appendonlyFiles := viper.GetStringSlice("appendonly-files")

	pitr := pitreos.New(viper.GetInt64("chunk-size"), viper.GetInt("threads"), time.Second*time.Duration(viper.GetInt("timeout")), storage)
	pitr.AppendonlyFiles = appendonlyFiles

	if viper.GetBool("enable-caching") {
		fmt.Println("Cache enabled")
		cacheURL, err := url.Parse(viper.GetString("cache-dir"))
		errorCheck("--cache-storage path invalid", err)

		cacheStorage, _ := pitreos.NewFSStorage(cacheURL)
		pitr.SetCacheStorage(cacheStorage)
	}

	return pitr
}
