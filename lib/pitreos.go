package pitreos

import (
	"time"

	"cloud.google.com/go/storage"
)

type PITR struct {
	ChunkSize              int64
	Threads                int
	TransferTimeout        time.Duration
	CacheDir               string
	Caching                bool
	AppendonlyOptimization bool
	AppendonlyFiles        []string
	storageClient          *storage.Client
	cachingEngine          *LocalCache
}

func New() *PITR {
	return &PITR{
		ChunkSize:       50 * 1024 * 1024,
		Threads:         24,
		TransferTimeout: 300 * time.Second,
	}
}
