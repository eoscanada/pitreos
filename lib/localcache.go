package pitreos

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type LocalCache struct {
	enabled   bool
	directory string
}

func (p *PITR) setupCaching() error {
	if !p.Caching || p.CacheDir == "" {
		p.cachingEngine = NewLocalCache(false, "")
		return nil
	}

	//prepare caching folder
	err := os.MkdirAll(p.CacheDir, 0755)
	if err != nil {
		return err
	}

	log.Printf("Setting up local caching in folder %s\n", p.CacheDir)
	p.cachingEngine = NewLocalCache(true, p.CacheDir)
	return nil
}

func NewLocalCache(enabled bool, directory string) *LocalCache {
	return &LocalCache{
		enabled:   enabled,
		directory: directory,
	}
}

func (c *LocalCache) getChunkFromCache(sha1sum string) (data []byte, filename string, err error) {
	if !c.enabled {
		return data, "", fmt.Errorf("Cache disabled")
	}

	filename = filepath.Join(c.directory, fmt.Sprintf("%s.blob", sha1sum))

	info, err := os.Stat(filename)
	if err != nil {
		return data, filename, err
	}

	data = make([]byte, info.Size())

	f, err := os.Open(filename)
	if err != nil {
		return data, filename, fmt.Errorf("Open file error: %s", err)
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
