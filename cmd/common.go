package cmd

import (
	"fmt"
	"net/url"
	"os"
	"time"

	pitreos "github.com/eoscanada/pitreos/lib"
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

	pitr := pitreos.New(chunkSizeMiB, threads, time.Second*time.Duration(transferTimeoutSeconds), storage)
	pitr.AppendonlyFiles = appendonlyFiles

	if caching {
		cacheURL, err := url.Parse(cacheDir)
		errorCheck("--cache-storage path invalid", err)

		cacheStorage, _ := pitreos.NewFSStorage(cacheURL)
		pitr.SetCacheStorage(cacheStorage)
	}

	return pitr
}
