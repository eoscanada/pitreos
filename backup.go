package pitreos

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/crypto/sha3"

	"github.com/abourget/llerrgroup"
	"github.com/ghodss/yaml"
)

func (p *PITR) GenerateBackup(source string, tag string, metadata map[string]interface{}, filter Filter) error {
	now := time.Now()
	backupName := makeBackupName(now, tag)
	bm := &BackupIndex{
		ChunkSize: p.chunkSize,
		Date:      now.UTC(),
		Version:   p.filemetaVersion,
		Meta:      metadata,
	}

	dirs, err := getDirFiles(source)
	for _, filePath := range dirs {
		relName, err := filepath.Rel(source, filePath)
		if err != nil {
			return err
		}

		if !filter.Match(relName) {
			continue
		}

		fileMeta, err := p.uploadFileToGSChunks(filePath, relName, now, tag)
		if err != nil {
			return fmt.Errorf("upload file to chunks: %s", err)
		}

		bm.Files = append(bm.Files, fileMeta)
	}

	err = p.uploadBackupIndexYamlFile(backupName, bm)
	if err != nil {
		return fmt.Errorf("upload backup index: %s", err)
	}

	zlog.Debug("backup index uploaded", zap.String("backup_name", backupName))

	return nil
}

func (p *PITR) uploadFileToGSChunks(localFile, relFileName string, timestamp time.Time, tag string) (*FileIndex, error) {
	f := NewFileOps(localFile, false)
	if err := f.Open(); err != nil {
		return nil, fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	fileInfo, _ := f.file.Stat()
	fileMeta := &FileIndex{
		FileName:  relFileName,
		TotalSize: fileInfo.Size(),
		Date:      timestamp,
	}
	totalPartsNum := int64(math.Ceil(float64(fileMeta.TotalSize) / float64(p.chunkSize)))

	var previousFile *FileIndex
	var previousChunksMap = make(map[int64]*ChunkDef)

	// get previousFile if we can find it perfectly in previous backup
	// with the same tag

	if stringarrayContains(p.AppendonlyFiles, fileMeta.FileName) {
		previousBackup, err := p.GetLatestBackup(tag)
		if err == nil && len(previousBackup) > 0 {
			previousBM, err := p.downloadBackupIndex(previousBackup)
			zlog.Debug("previous backup index", zap.String("version", previousBM.Version), zap.String("filemeta_version", p.filemetaVersion))
			if err == nil && previousBM != nil && previousBM.Version == p.filemetaVersion && previousBM.ChunkSize == p.chunkSize {
				for _, pf := range previousBM.Files {
					if pf.FileName == fileMeta.FileName {
						previousFile = pf
						f.isAppendOnly = true

						for _, c := range previousFile.Chunks {
							previousChunksMap[c.Start] = c
						}
					}
				}
			}
		}

	}

	zlog.Debug("splitting to pieces", zap.String("relative_file_name", relFileName), zap.Int64("total_parts_num", totalPartsNum))

	// setup chunk metadata reader to populate fileMeta
	done := make(chan bool)
	chunkCh := make(chan *ChunkDef, 1000)
	cleanup := func() {
		zlog.Debug("cleaning up")
		close(chunkCh)
		<-done
	}

	go func() {
		for chunk := range chunkCh {
			fileMeta.Chunks = append(fileMeta.Chunks, chunk)
		}
		done <- true
	}()

	alreadyBackedupChunks := 0
	skippedChunks := 0
	emptyChunks := 0
	// iterate over chunks
	eg := llerrgroup.New(p.threads)
	for i := int64(0); i < totalPartsNum; i++ {
		if eg.Stop() {
			cleanup()
			return nil, fmt.Errorf("One of the threads failed. Stopping.")
		}

		partnum := i
		eg.Go(func() error {

			partSize := int64(math.Min(float64(p.chunkSize), float64(fileMeta.TotalSize-int64(partnum*p.chunkSize))))

			chunkMeta := &ChunkDef{
				Start: partnum * p.chunkSize,
				End:   partnum*p.chunkSize + partSize - 1,
			}

			skipChunk := false
			if f.isAppendOnly && previousFile.TotalSize >= chunkMeta.End {
				skipChunk = true
				chunkMeta = previousChunksMap[chunkMeta.Start]
				chunkCh <- chunkMeta
				counterLock.Lock()
				skippedChunks++
				counterLock.Unlock()
				return nil
			}

			partBuffer, blockIsEmpty, err := f.getLocalChunk(chunkMeta.Start, partSize)
			if err != nil {
				errmsg := fmt.Errorf("get chunk contents: %s", err)
				zlog.Error("get chunk contents", zap.Error(errmsg))
				return errmsg
			}

			chunkMeta.IsEmpty = blockIsEmpty
			if blockIsEmpty {
				counterLock.Lock()
				emptyChunks++
				counterLock.Unlock()
			}

			if !blockIsEmpty && !skipChunk {
				zlog.Info("processing part", zap.Int64("part_num", partnum+1), zap.Int64("total_parts_num", totalPartsNum))
				chunkMeta.ContentSHA = fmt.Sprintf("%x", sha3.Sum256(partBuffer))

				// don't fail if caching disabled
				if p.cacheStorage != nil {
					err := p.cacheStorage.WriteChunk(chunkMeta.ContentSHA, partBuffer)
					if err != nil {
						errmsg := fmt.Errorf("cache storage writechunk: %s", err)
						zlog.Error("cache storage write chunk", zap.Error(errmsg))
						return errmsg
					}
				}

				exists, err := p.storage.ChunkExists(chunkMeta.ContentSHA)
				if err != nil {
					errmsg := fmt.Errorf("chunk exists: %s", err)
					zlog.Error("chunk exists", zap.Error(errmsg))
					return errmsg
				}
				if exists {
					counterLock.Lock()
					alreadyBackedupChunks++
					counterLock.Unlock()
				} else {
					err := p.storage.WriteChunk(chunkMeta.ContentSHA, partBuffer)
					if err != nil {
						errmsg := fmt.Errorf("write chunk: %s", err)
						zlog.Error("write chunk", zap.Error(errmsg))
						return errmsg
					}
				}
			}

			chunkCh <- chunkMeta

			return nil
		})

	}

	if err := eg.Wait(); err != nil {
		cleanup()
		zlog.Fatal("waiting for tasks completion", zap.Error(err))
	}

	if alreadyBackedupChunks > 0 {
		zlog.Debug("already backed up chunks",
			zap.Int("already_backed_up_chunk_count", alreadyBackedupChunks),
			zap.Int64("total_parts_num", totalPartsNum),
			zap.String("file_name", fileMeta.FileName),
		)
	}

	if skippedChunks > 0 {
		zlog.Debug("skipped chunks",
			zap.Int("skipped_chunk_count", skippedChunks),
			zap.Int64("total_parts_num", totalPartsNum),
			zap.String("file_name", fileMeta.FileName),
		)
	}

	if emptyChunks != 0 {
		zlog.Debug("empty chunks",
			zap.Int("empty_chunk_count", emptyChunks),
			zap.Int64("total_parts_num", totalPartsNum),
			zap.String("file_name", fileMeta.FileName),
		)
	}

	cleanup()
	return fileMeta, nil

}

func (p *PITR) uploadBackupIndexYamlFile(name string, bm *BackupIndex) error {
	d, err := yaml.Marshal(&bm)
	if err != nil {
		return fmt.Errorf("yaml marshal: %s", err)
	}
	return p.storage.WriteBackupIndex(name, d)
}

func makeBackupName(now time.Time, tag string) string {
	dt := now.UTC().Format(time.RFC3339)
	dt = strings.Replace(dt, ":", "-", -1)
	dt = strings.Replace(dt, "T", "-", -1)
	dt = strings.Replace(dt, "Z", "", -1)
	backupName := fmt.Sprintf("%s--%s", dt, tag)
	return backupName
}
