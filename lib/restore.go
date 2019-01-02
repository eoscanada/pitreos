package pitreos

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/crypto/sha3"

	"github.com/abourget/llerrgroup"
	humanize "github.com/dustin/go-humanize"
	"github.com/ghodss/yaml"
)

var counterLock sync.Mutex

func (p *PITR) RestoreFromBackup(dest string, backupName string, filter string) error {
	bm, err := p.downloadBackupIndex(backupName)
	if err != nil {
		return err
	}

	if bm.Version != p.filemetaVersion {
		return fmt.Errorf("Incompatible version of backupIndex. Expected: %s, found: %s.", p.filemetaVersion, bm.Version)
	}

	matchingFiles, err := bm.FindFilesMatching(filter)
	if err != nil {
		return err
	}

	for _, file := range matchingFiles {
		err := p.downloadFileFromChunks(file, dest)
		if err != nil {
			return fmt.Errorf("retrieve chunk %q: %s", file.FileName, err)
		}
	}

	return nil
}

func (p *PITR) downloadFileFromChunks(fm *FileIndex, localFolder string) error {
	zlog.Info("restoring file with size from snapshot",
		zap.String("file_name", fm.FileName),
		zap.String("bytes", humanize.Bytes(uint64(fm.TotalSize))),
		zap.Time("date", fm.Date),
	)

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
		zlog.Info("file treated as 'appendonly'",
			zap.String("file_name", fm.FileName),
			zap.String("truncated_to", humanize.Bytes(uint64(fm.TotalSize))),
		)

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
				zlog.Debug("punching a hole (empty chunk)", zap.Int("chunk_index", n+1), zap.Int("num_chunks", numChunks))
				err := f.wipeChunk(chunkMeta.Start, chunkMeta.End-chunkMeta.Start+1)
				if err != nil {
					return err
				}
				return nil
			}

			numBytes := humanize.Bytes(uint64(chunkMeta.End - chunkMeta.Start - 1))
			if localChunkEmpty && !chunkMeta.IsEmpty {
				zlog.Info("chunk invalid",
					zap.Int("chunk_index", n+1),
					zap.Int("num_chunks", numChunks),
					zap.Any("sha3_sum", chunkMeta.ContentSHA),
					zap.Any("num_bytes", numBytes),
				)
			} else {
				readSHASum := sha3.Sum256(partBuffer)
				shasum := fmt.Sprintf("%x", readSHASum)
				if shasum == chunkMeta.ContentSHA {
					counterLock.Lock()
					correctChunks++
					counterLock.Unlock()
					return nil
				}

				zlog.Debug("chunk valid",
					zap.Int("chunk_index", n+1),
					zap.Int("num_chunks", numChunks),
					zap.Any("sha3_sum", shasum),
					zap.Any("chunk_meta_content_sha", chunkMeta.ContentSHA),
					zap.Any("num_bytes", numBytes),
				)
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
				return fmt.Errorf("invalid sha3sum from downloaded blob, got %s, expected %s", newSHASum, chunkMeta.ContentSHA)
			}

			zlog.Debug("chunk download finished",
				zap.Int("chunk_index", n+1),
				zap.Any("num_chunks", numChunks),
				zap.Any("new_sha3_sum", newSHASum),
			)

			return f.writeChunkToFile(int64(chunkMeta.Start), newData)
		})

	}
	if err := eg.Wait(); err != nil {
		return err
	}

	if skippedChunks > 0 {
		zlog.Debug("skipped chunks",
			zap.Any("skipped_chunk_count", skippedChunks),
			zap.Any("total_chunk_count", numChunks),
			zap.Any("file_name", fm.FileName),
		)
	}

	if correctChunks > 0 {
		zlog.Debug("correct chunks",
			zap.Any("correct_chunk_count", correctChunks),
			zap.Any("total_chunk_count", numChunks),
			zap.Any("file_name", fm.FileName),
		)
	}

	if emptyChunks > 0 {
		zlog.Debug("empty chunks",
			zap.Any("empty_chunk_count", emptyChunks),
			zap.Any("total_chunk_count", numChunks),
			zap.Any("file_name", fm.FileName),
		)
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
