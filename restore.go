package pitreos

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/abourget/llerrgroup"
	humanize "github.com/dustin/go-humanize"
	"github.com/ghodss/yaml"
)

func (p *PITR) RestoreFromBackup() error {
	if err := p.setupStorage(); err != nil {
		return err
	}
	if err := p.setupCaching(); err != nil {
		return err
	}

	if p.Options.BeforeTimestamp == 0 {
		p.Options.BeforeTimestamp = time.Now().Unix()
	}

	wantedBackupYAML, err := p.findAvailableBackup()
	if err != nil {
		return err
	}

	var bm *BackupMeta
	if err := p.downloadYaml(wantedBackupYAML, &bm); err != nil {
		return err
	}

	for _, y := range bm.MetadataFiles {
		var fm *FileMeta
		if err := p.downloadYaml(p.getStorageFilePath(y), &fm); err != nil {
			return err
		}

		err := p.downloadFileFromChunks(fm, p.Options.LocalFolder)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PITR) downloadFileFromChunks(fm *FileMeta, localFolder string) error {
	log.Println("")
	log.Printf("Restoring file %q with size %s from snapshot dated %s\n", fm.FileName, humanize.Bytes(uint64(fm.TotalSize)), fm.Date)
	log.Println("")
	filePath := filepath.Join(localFolder, fm.FileName)

	err := os.MkdirAll(path.Dir(filePath), 0755)
	if err != nil {
		return err
	}

	f := NewFileOps(filePath, true)
	if err := f.Open(); err != nil {
		return err
	}
	defer f.Close()

	if err = f.Truncate(fm.TotalSize); err != nil {
		return err
	}

	eg := llerrgroup.New(p.threads)
	numChunks := len(fm.Chunks)
	for n, chunkMeta := range fm.Chunks {

		if eg.Stop() {
			return fmt.Errorf("Got an error in thread management. Stopping.")
		}

		n := n
		chunkMeta := chunkMeta
		eg.Go(func() error {

			partBuffer, localChunkEmpty, err := f.getLocalChunk(int64(chunkMeta.Start), int64(chunkMeta.End-chunkMeta.Start+1))
			if err != nil {
				return err
			}

			if localChunkEmpty && chunkMeta.IsEmpty {
				log.Printf("Chunk %d/%d has empty contents already", n+1, numChunks)
				return nil
			}

			if !localChunkEmpty && chunkMeta.IsEmpty {
				log.Printf("Chunk %d/%d punching a hole (empty chunk)", n+1, numChunks)
				err := f.wipeChunk(chunkMeta.Start, chunkMeta.End-chunkMeta.Start+1)
				if err != nil {
					return err
				}
				return nil
			}

			numBytes := humanize.Bytes(uint64(chunkMeta.End - chunkMeta.Start - 1))
			if localChunkEmpty && !chunkMeta.IsEmpty {
				log.Printf("Chunk %d/%d empty, downloading sha1 %q (%s)", n+1, numChunks, chunkMeta.ContentSHA1, numBytes)
			} else {
				readSHA1Sum := sha1.Sum(partBuffer)
				shasum := fmt.Sprintf("%x", readSHA1Sum)
				if shasum == chunkMeta.ContentSHA1 {
					log.Printf("Chunk %d/%d has correct contents already", n+1, numChunks)
					return nil
				}
				log.Printf("Chunk %d/%d has sha1 %q, downloading sha1 %q (%s)", n+1, numChunks, shasum, chunkMeta.ContentSHA1, numBytes)
			}

			blobPath := p.getStorageFilePath(chunkMeta.URL)

			blobStart := chunkMeta.Start
			expectedSum := chunkMeta.ContentSHA1

			//try from cache first
			newData, err := p.cachingEngine.getChunkFromCache(chunkMeta.ContentSHA1)
			if err == nil {
				log.Printf("Got it from local cache. Great!")
			} else {
				newData, err = p.readFromGoogleStorage(blobPath)
				if err != nil {
					log.Printf("Something went wrong reading, error = %s\n", err)
					return err
				}
				//save to cache
				err := p.cachingEngine.putChunkToCache(chunkMeta.ContentSHA1, newData)
				if err != nil {
					return fmt.Errorf("Error in writing cache file: %s", err.Error())
				}

			}

			newSHA1Sum := fmt.Sprintf("%x", sha1.Sum(newData))
			log.Printf("- Chunk %d/%d download finished, new sha1: %s\n", n+1, numChunks, newSHA1Sum)
			if expectedSum != newSHA1Sum {
				return fmt.Errorf("Invalid sha1sum from downloaded blob. Got %s, expected %s\n", newSHA1Sum, expectedSum)
			}
			return f.writeChunkToFile(int64(blobStart), newData)
		})

	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (p *PITR) downloadYaml(filePath string, obj interface{}) error {
	y, err := p.readFromGoogleStorage(filePath)
	if err != nil {
		return err
	}

	if err = yaml.Unmarshal(y, obj); err != nil {
		return fmt.Errorf("yaml unmarshal: %s", err)
	}

	return nil
}
