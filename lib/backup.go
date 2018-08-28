package pitreos

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/ghodss/yaml"
)

func (p *PITR) GenerateBackup(source string, tag string, metadata map[string]interface{}) error {
	now := time.Now()
	backupName := makeBackupName(now, tag)
	bm := &BackupIndex{
		Date:    now.UTC(),
		Version: "v2",
		Meta:    metadata,
	}

	dirs, err := getDirFiles(source)
	for _, filePath := range dirs {
		relName, err := filepath.Rel(source, filePath)
		if err != nil {
			return err
		}

		fileMeta, err := p.uploadFileToGSChunks(filePath, relName, now)
		if err != nil {
			return fmt.Errorf("upload file to chunks: %s", err)
		}

		bm.Files = append(bm.Files, fileMeta)
	}

	err = p.uploadBackupIndexYamlFile(backupName, bm)
	if err != nil {
		return fmt.Errorf("upload backup index: %s", err)
	}

	log.Println("Backup index uploaded:", backupName)

	return nil
}

func (p *PITR) uploadFileToGSChunks(localFile, relFileName string, timestamp time.Time) (*FileIndex, error) {
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
	log.Printf("Splitting %s to %d pieces.\n", relFileName, totalPartsNum)

	// setup chunk metadata reader to populate fileMeta
	done := make(chan bool)
	chunkCh := make(chan *ChunkDef, 1000)
	go func() {
		for chunk := range chunkCh {
			fileMeta.Chunks = append(fileMeta.Chunks, chunk)
		}
		done <- true
	}()

	emptyChunks := 0
	// iterate over chunks
	eg := llerrgroup.New(p.threads)
	for i := int64(0); i < totalPartsNum; i++ {
		if eg.Stop() {
			return nil, fmt.Errorf("One of the threads failed. Stopping.")
		}

		partnum := i
		eg.Go(func() error {

			partSize := int64(math.Min(float64(p.chunkSize), float64(fileMeta.TotalSize-int64(partnum*p.chunkSize))))

			chunkMeta := &ChunkDef{
				Start: partnum * p.chunkSize,
				End:   partnum*p.chunkSize + partSize - 1,
			}

			partBuffer, blockIsEmpty, err := f.getLocalChunk(chunkMeta.Start, partSize)
			if err != nil {
				return fmt.Errorf("get chunk contents: %s", err)
			}

			chunkMeta.IsEmpty = blockIsEmpty
			if blockIsEmpty {
				counterLock.Lock()
				emptyChunks += 1
				counterLock.Unlock()
			}

			if !blockIsEmpty {
				log.Printf("Processing part %d of %d ###\n", partnum+1, totalPartsNum)
				chunkMeta.ContentSHA1 = fmt.Sprintf("%x", sha1.Sum(partBuffer))

				// don't fail if caching disabled
				if p.cacheStorage != nil {
					err := p.cacheStorage.WriteChunk(chunkMeta.ContentSHA1, partBuffer)
					if err != nil {
						return err
					}
				}

				exists, err := p.storage.ChunkExists(chunkMeta.ContentSHA1)
				if err != nil {
					return err
				}
				if exists {
					log.Printf("File already exists for sha1: %s", chunkMeta.ContentSHA1)
				} else {
					log.Printf("Sending file to Storage: %s", chunkMeta.ContentSHA1)
					writeChan := make(chan error, 1)
					go func() {
						writeChan <- p.storage.WriteChunk(chunkMeta.ContentSHA1, partBuffer)
					}()
					select {
					case err := <-writeChan:
						if err != nil {
							return err
						}

					case <-time.After(p.transferTimeout):
						return fmt.Errorf("Upload of chunk %q to storage timed out", chunkMeta.ContentSHA1)
					}

					if err != nil {
						return err
					}
				}
			}

			chunkCh <- chunkMeta

			return nil
		})

	}

	//json.NewEncoder(os.Stdout).Encode(fileMeta)

	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}

	if emptyChunks != 0 {
		log.Printf("- %d of %d chunks were empty and ignored", emptyChunks, totalPartsNum)
	}
	close(chunkCh)
	<-done

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
