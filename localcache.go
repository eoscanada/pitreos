package pitreos

import (
	"fmt"
	"os"
	"path/filepath"
)

type LocalCache struct {
	enabled   bool
	directory string
}

func (p *PITR) setupCaching() error {
	if p.Options.CacheFolder == "" {
		p.cachingEngine = NewLocalCache(false, "")
		return nil
	}

	//prepare caching folder
	err := os.MkdirAll(p.Options.CacheFolder, 0755)
	if err != nil {
		return err
	}

	p.cachingEngine = NewLocalCache(true, p.Options.CacheFolder)
	return nil
}

func NewLocalCache(enabled bool, directory string) *LocalCache {
	return &LocalCache{
		enabled:   enabled,
		directory: directory,
	}
}

func (c *LocalCache) getChunkFromCache(sha1sum string) (data []byte, err error) {
	if !c.enabled {
		return data, fmt.Errorf("Cache disabled")
	}

	filePath := filepath.Join(c.directory, fmt.Sprintf("%s.blob", sha1sum))

	info, err := os.Stat(filePath)
	if err != nil {
		return data, err
	}

	data = make([]byte, info.Size())

	f, err := os.Open(filePath)
	if err != nil {
		return data, fmt.Errorf("Open file error: %s", err)
	}
	_, err = f.Read(data)

	return

}

func (c *LocalCache) putChunkToCache(sha1sum string, data []byte) error {
	if !c.enabled {
		return fmt.Errorf("Cache disabled")
	}

	filePath := filepath.Join(c.directory, fmt.Sprintf("%s.blob", sha1sum))

	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	return err
}
