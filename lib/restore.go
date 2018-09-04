package pitreos

import (
	"fmt"
	"golang.org/x/crypto/sha3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/abourget/llerrgroup"
	humanize "github.com/dustin/go-humanize"
	"github.com/ghodss/yaml"
)

var counterLock sync.Mutex

func (p *PITR) RestoreFromBackup(dest string, backupName string) error {
	bm, err := p.downloadBackupIndex(backupName)
	if err != nil {
		return err
	}

	for _, file := range bm.Files {
		err := p.downloadFileFromChunks(file, dest)
		if err != nil {
			return fmt.Errorf("retrieve chunk %q: %s", file.FileName, err)
		}
	}

	return nil
}

func (p *PITR) downloadFileFromChunks(fm *FileIndex, localFolder string) error {
	log.Printf("Restoring file %q with size %s from snapshot dated %s\n", fm.FileName, humanize.Bytes(uint64(fm.TotalSize)), fm.Date)
	filePath := filepath.Join(localFolder, fm.FileName)

	err := os.MkdirAll(path.Dir(filePath), 0755)
	if err != nil {
		return fmt.Errorf("mkdirall: %s", err)
	}

	f := NewFileOps(filePath, true)
	if err := f.Open(); err != nil {
		return fmt.Errorf("new fileops: %s", err)
	}
	defer f.Close()

	if stringarrayContains(p.AppendonlyFiles, fm.FileName) {
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
	eg := llerrgroup.New(p.threads)
	numChunks := len(fm.Chunks)
	for n, chunkMeta := range fm.Chunks {

		if f.isAppendOnly && f.originalSize > chunkMeta.End {
			counterLock.Lock()
			skippedChunks++
			counterLock.Unlock()
			continue
		}

		if eg.Stop() {
			break
		}
		n := n
		chunkMeta := chunkMeta
		eg.Go(func() error {

			partBuffer, localChunkEmpty, err := f.getLocalChunk(int64(chunkMeta.Start), int64(chunkMeta.End-chunkMeta.Start+1))
			if err != nil {
				return fmt.Errorf("getting local chunk: %s", err)
			}

			if localChunkEmpty && chunkMeta.IsEmpty {
				counterLock.Lock()
				emptyChunks++
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
				log.Printf("- Chunk %d/%d invalid, downloading sha3 %q (%s)", n+1, numChunks, chunkMeta.ContentSHA, numBytes)
			} else {
				readSHASum := sha3.Sum256(partBuffer)
				shasum := fmt.Sprintf("%x", readSHASum)
				if shasum == chunkMeta.ContentSHA {
					counterLock.Lock()
					correctChunks++
					counterLock.Unlock()
					return nil
				}
				log.Printf("- Chunk %d/%d has sha3 %q, downloading sha3 %q (%s)", n+1, numChunks, shasum, chunkMeta.ContentSHA, numBytes)
			}

			//try from cache first
			var openChunk io.ReadCloser
			var inCache bool
			if p.cacheStorage != nil {
				// Try this first
				found, err := p.cacheStorage.ChunkExists(chunkMeta.ContentSHA)
				if err != nil {
					return err
				}
				if found {
					openChunk, err = p.cacheStorage.OpenChunk(chunkMeta.ContentSHA)
					inCache = true
				}
			}
			if openChunk == nil {
				openChunk, err = p.storage.OpenChunk(chunkMeta.ContentSHA)
			}
			if err != nil {
				return fmt.Errorf("open chunk: %s", err)
			}
			defer openChunk.Close()

			newData, err := ioutil.ReadAll(openChunk)
			if err != nil {
				return err
			}

			if p.cacheStorage != nil && !inCache {
				err := p.cacheStorage.WriteChunk(chunkMeta.ContentSHA, newData)
				if err != nil {
					return err
				}
			}

			newSHASum := fmt.Sprintf("%x", sha3.Sum256(newData))
			if chunkMeta.ContentSHA != newSHASum {
				return fmt.Errorf("Invalid sha3sum from downloaded blob. Got %s, expected %s\n", newSHASum, chunkMeta.ContentSHA)
			}

			log.Printf("- Chunk %d/%d download finished, new sha3: %s\n", n+1, numChunks, newSHASum)
			return f.writeChunkToFile(int64(chunkMeta.Start), newData)
		})

	}
	if err := eg.Wait(); err != nil {
		return err
	}

	if skippedChunks > 0 {
		log.Printf("- %d/%d chunks were not verified on this appendonly file %s.\n", skippedChunks, numChunks, fm.FileName)
	}
	if correctChunks > 0 {
		log.Printf("- %d/%d chunks were already correct. on file %s\n", correctChunks, numChunks, fm.FileName)
	}
	if emptyChunks > 0 {
		log.Printf("- %d/%d chunks were left empty. on file %s\n", emptyChunks, numChunks, fm.FileName)
	}
	return nil
}

func (p *PITR) downloadBackupIndex(name string) (out *BackupIndex, err error) {
	y, err := p.storage.OpenBackupIndex(name)
	if err != nil {
		return
	}
	defer y.Close()

	cnt, err := ioutil.ReadAll(y)
	if err != nil {
		return
	}

	if err = yaml.Unmarshal(cnt, &out); err != nil {
		return nil, fmt.Errorf("yaml unmarshal: %s", err)
	}

	return
}
