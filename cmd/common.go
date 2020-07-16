package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/eoscanada/pitreos"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func errorCheck(prefix string, err error) {
	if err != nil {
		fmt.Printf("ERROR: %s: %s\n", prefix, err)
		os.Exit(1)
	}
}

func getPITR(storageURL string) *pitreos.PITR {
	ctx := context.Background()
	storage, err := pitreos.NewDStoreStorage(ctx, storageURL)
	errorCheck("setting up storage", err)

	appendonlyFiles := viper.GetStringSlice("appendonly-files")
	chunkSize := viper.GetInt64("chunk-size")
	threads := viper.GetInt("threads")
	transferTimeout := time.Second * time.Duration(viper.GetInt("timeout"))

	zlog.Info("creating instance",
		zap.String("store_url", storageURL),
		zap.Strings("files", appendonlyFiles),
		zap.Int64("chunk_size", chunkSize),
		zap.Int("threads", threads),
		zap.Duration("transfer_timeout", transferTimeout),
	)

	pitr := pitreos.New(chunkSize, threads, transferTimeout, storage)
	pitr.AppendonlyFiles = appendonlyFiles

	if viper.GetBool("enable-caching") {
		zlog.Debug("Caching enabled")
		cacheStorage, _ := pitreos.NewDStoreStorage(ctx, viper.GetString("cache-dir"))
		pitr.SetCacheStorage(cacheStorage)
	}

	return pitr
}

func resolveBackupName(pitr *pitreos.PITR, backupName string) string {
	// We assume it's a full backup name
	if strings.Contains(backupName, "--") {
		return backupName
	}

	fmt.Println("Fetching latest backup")
	lastBackup, err := pitr.GetLatestBackup(backupName)
	errorCheck("Getting last available backup", err)

	if lastBackup == "" {
		errorCheck("getting last backups", errors.New("last available backup found empty"))
	}

	fmt.Printf("Found latest backup %q\n", lastBackup)
	return lastBackup
}
