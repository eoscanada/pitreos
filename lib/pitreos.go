package pitreos

import (
	"cloud.google.com/go/storage"
)

type PITR struct {
	chunkSize              int64
	threads                int
	CacheDir               string
	Caching                bool
	AppendonlyOptimization bool
	AppendonlyFiles        []string
	storageClient          *storage.Client
	cachingEngine          *LocalCache
}

func New() *PITR {
	return &PITR{
		chunkSize: 50 * 1024 * 1024,
		threads:   24,
	}
}
