package pitreos

import (
	"crypto/sha1"
	"fmt"
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
		if eg.Stop() {
			break
		}

		if f.isAppendOnly && f.originalSize > chunkMeta.End {
			counterLock.Lock()
			skippedChunks++
			counterLock.Unlock()
			continue
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
				log.Printf("- Chunk %d/%d invalid, downloading sha1 %q (%s)", n+1, numChunks, chunkMeta.ContentSHA1, numBytes)
			} else {
				readSHA1Sum := sha1.Sum(partBuffer)
				shasum := fmt.Sprintf("%x", readSHA1Sum)
				if shasum == chunkMeta.ContentSHA1 {
					counterLock.Lock()
					correctChunks++
					counterLock.Unlock()
					return nil
				}
				log.Printf("- Chunk %d/%d has sha1 %q, downloading sha1 %q (%s)", n+1, numChunks, shasum, chunkMeta.ContentSHA1, numBytes)
			}

			//try from cache first
			var openChunk io.ReadCloser
			var inCache bool
			if p.cacheStorage != nil {
				// Try this first
				found, err := p.cacheStorage.ChunkExists(chunkMeta.ContentSHA1)
				if err != nil {
					return err
				}
				if found {
					openChunk, err = p.cacheStorage.OpenChunk(chunkMeta.ContentSHA1)
					inCache = true
				}
			}
			if openChunk == nil {
				openChunk, err = p.storage.OpenChunk(chunkMeta.ContentSHA1)
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
				err := p.cacheStorage.WriteChunk(chunkMeta.ContentSHA1, newData)
				if err != nil {
					return err
				}
			}

			newSHA1Sum := fmt.Sprintf("%x", sha1.Sum(newData))
			if chunkMeta.ContentSHA1 != newSHA1Sum {
				return fmt.Errorf("Invalid sha1sum from downloaded blob. Got %s, expected %s\n", newSHA1Sum, chunkMeta.ContentSHA1)
			}

			log.Printf("- Chunk %d/%d download finished, new sha1: %s\n", n+1, numChunks, newSHA1Sum)
			return f.writeChunkToFile(int64(chunkMeta.Start), newData)
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
