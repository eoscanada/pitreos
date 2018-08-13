package pitreos

import (
	"cloud.google.com/go/storage"
)

type PITR struct {
	chunkSize int64
	threads   int
	Options   *PitreosOptions

	storageBucket *storage.BucketHandle
	cachingEngine *LocalCache
}

func New(opts *PitreosOptions) *PITR {
	return &PITR{
		chunkSize: 50 * 1024 * 1024,
		threads:   24,
		Options:   opts,
	}
}
