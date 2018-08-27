package pitreos

import (
	"time"
)

type PITR struct {
	chunkSize       int64
	threads         int
	transferTimeout time.Duration
	AppendonlyFiles []string

	cacheStorage Storage
	storage      Storage
}

func NewDefaultPITR(storage Storage) *PITR {
	return New(50, 24, 300*time.Second, storage)
}
func New(chunkSizeMiB int64, threads int, transferTimeout time.Duration, storage Storage) *PITR {
	return &PITR{
		chunkSize:       chunkSizeMiB * 1024 * 1024,
		threads:         threads,
		transferTimeout: transferTimeout,
		storage:         storage,
	}
}

// SetCachingStorage enables caching through the provided Storage object.
func (p *PITR) SetCacheStorage(storage Storage) {
	p.cacheStorage = storage
}
