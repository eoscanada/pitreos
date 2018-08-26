package pitreos

import (
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	humanize "github.com/dustin/go-humanize"
	"github.com/ghodss/yaml"
)

var counterLock sync.Mutex

func (p *PITR) RestoreFromBackup(source, dest string, targetTimestamp time.Time) error {
	if !isGSURL(source) {
		return fmt.Errorf("Backup to/Restore from local file not implemented.")
	}
	if err := p.setupStorageClient(); err != nil {
		return err
	}
	if err := p.setupCaching(); err != nil {
		return err
	}

	wantedBackupYAML, err := p.findAvailableBackup(source, targetTimestamp)
	if err != nil {
		return err
	}
	log.Printf("Found valid backup definition at %s\n", wantedBackupYAML)

	var bm *BackupMeta
	if err := p.downloadYaml(wantedBackupYAML, &bm); err != nil {
		return err
	}

	for _, y := range bm.MetadataFiles {
		var fm *FileMeta
		if err := p.downloadYaml(y, &fm); err != nil {
			return err
		}

		err := p.downloadFileFromChunks(fm, dest)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PITR) downloadFileFromChunks(fm *FileMeta, localFolder string) error {
	log.Printf("Restoring file %q with size %s from snapshot dated %s\n", fm.FileName, humanize.Bytes(uint64(fm.TotalSize)), fm.Date)
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

	if p.AppendonlyOptimization && stringarrayContains(p.AppendonlyFiles, fm.FileName) {
		f.isAppendOnly = true
	}
	fstats, err := f.file.Stat()
	if err != nil {
		return err
	}
	f.originalSize = fstats.Size()

	if err = f.Truncate(fm.TotalSize); err != nil {
		return err
	}
	if f.isAppendOnly && f.originalSize >= fm.TotalSize {
		log.Printf("- File %s treated as 'appendonly'. Got truncated to %s\n", fm.FileName, humanize.Bytes(uint64(fm.TotalSize)))
		return nil
	}

	skippedChunks := 0
	emptyChunks := 0
	correctChunks := 0
	eg := llerrgroup.New(p.Threads)
	numChunks := len(fm.Chunks)
	for n, chunkMeta := range fm.Chunks {

		if f.isAppendOnly && f.originalSize > chunkMeta.End {
			counterLock.Lock()
			skippedChunks += 1
			counterLock.Unlock()
			continue
		}
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
				counterLock.Lock()
				emptyChunks += 1
				counterLock.Unlock()
				return nil
			}

			if !localChunkEmpty && chunkMeta.IsEmpty {
				log.Printf("- Chunk %d/%d punching a hole (empty chunk)", n+1, numChunks)
				err := f.wipeChunk(chunkMeta.Start, chunkMeta.End-chunkMeta.Start+1)
				if err != nil {
					return err
				}
				return nil
			}

			numBytes := humanize.Bytes(uint64(chunkMeta.End - chunkMeta.Start - 1))
			if localChunkEmpty && !chunkMeta.IsEmpty {
				log.Printf("- Chunk %d/%d invalid, downloading sha1 %q (%s)", n+1, numChunks, chunkMeta.ContentSHA1, numBytes)
			} else {
				readSHA1Sum := sha1.Sum(partBuffer)
				shasum := fmt.Sprintf("%x", readSHA1Sum)
				if shasum == chunkMeta.ContentSHA1 {
					counterLock.Lock()
					correctChunks += 1
					counterLock.Unlock()
					return nil
				}
				log.Printf("- Chunk %d/%d has sha1 %q, downloading sha1 %q (%s)", n+1, numChunks, shasum, chunkMeta.ContentSHA1, numBytes)
			}

			blobStart := chunkMeta.Start
			expectedSum := chunkMeta.ContentSHA1

			//try from cache first
			newData, filename, err := p.cachingEngine.getChunkFromCache(chunkMeta.ContentSHA1)
			if err == nil {
				log.Printf("- Got it from local cache file: %s", filename)
			} else {
				newData, err = p.readFromGoogleStorage(chunkMeta.URL)
				if err != nil {
					log.Printf("Something went wrong reading, error = %s\n", err)
					return err
				}
				//save to cache if possible
				_ = p.cachingEngine.putChunkToCache(chunkMeta.ContentSHA1, newData)

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

	if skippedChunks > 0 {
		log.Printf("- %d/%d chunks were not verified on appendonly file %s. use --enable-appendonly-optimization=false to force verification\n", skippedChunks, numChunks, fm.FileName)
	}
	if correctChunks > 0 {
		log.Printf("- %d/%d chunks were already correct. on file %s\n", correctChunks, numChunks, fm.FileName)
	}
	if emptyChunks > 0 {
		log.Printf("- %d/%d chunks were left empty. on file %s\n", emptyChunks, numChunks, fm.FileName)
	}
	return nil
}

func (p *PITR) downloadYaml(URL string, obj interface{}) error {
	y, err := p.readFromGoogleStorage(URL)
	if err != nil {
		return err
	}

	if err = yaml.Unmarshal(y, obj); err != nil {
		return fmt.Errorf("yaml unmarshal: %s", err)
	}

	return nil
}
