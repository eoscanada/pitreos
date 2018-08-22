package pitreos

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"path"
	"path/filepath"
	"time"

	"github.com/abourget/llerrgroup"
	yaml "gopkg.in/yaml.v2"
)

func (p *PITR) GenerateBackup(source string, dest string, metadata map[string]interface{}) error {
	if !isGSURL(dest) {
		return fmt.Errorf("Backup to local file not implemented.")
	}
	bucketName, gsPath, err := splitGSURL(dest)
	if err != nil {
		return err
	}
	if err := p.setupStorageClient(); err != nil {
		return err
	}
	if err := p.setupCaching(); err != nil {
		return err
	}

	now := time.Now()
	nowString := now.UTC().Format(time.RFC3339)
	bm := BackupMeta{
		Date:        now,
		Kind:        "filesMap",
		MetaVersion: "v2",
		Details:     metadata,
	}

	dirs, err := getDirFiles(source)
	for _, filePath := range dirs {
		relName, err := filepath.Rel(source, filePath)
		if err != nil {
			return err
		}

		fileMeta, err := p.uploadFileToGSChunks(filePath, relName, bucketName, gsPath, now)
		if err != nil {
			return fmt.Errorf("upload file to chunks: %s", err)
		}

		yamlURL := getGSURL(bucketName, path.Join(gsPath, nowString, "yaml", fileMeta.FileName+".yaml"))
		err = p.uploadYamlFile(fileMeta, yamlURL)
		if err != nil {
			return fmt.Errorf("upload yaml: %s", err)
		}

		bm.MetadataFiles = append(bm.MetadataFiles, yamlURL)
	}

	yamlURL := getGSURL(bucketName, path.Join(gsPath, nowString, "index.yaml"))
	err = p.uploadBackupMetaYamlFile(bm, yamlURL)
	if err != nil {
		return fmt.Errorf("upload backup meta: %s", err)
	}

	log.Println("Backup meta uploaded:", yamlURL)

	return nil
}

func (p *PITR) uploadFileToGSChunks(localFile, relFileName, bucketName, gsPath string, timestamp time.Time) (*FileMeta, error) {
	f := NewFileOps(localFile, false)
	if err := f.Open(); err != nil {
		return nil, fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	fileInfo, _ := f.file.Stat()
	fileMeta := &FileMeta{
		Kind:        "blobMap",
		MetaVersion: "v1",
		FileName:    relFileName,
		TotalSize:   fileInfo.Size(),
		Date:        timestamp,
	}
	totalPartsNum := int64(math.Ceil(float64(fileMeta.TotalSize) / float64(p.chunkSize)))
	log.Printf("Splitting %s to %d pieces.\n", relFileName, totalPartsNum)

	// setup chunk metadata reader to populate fileMeta
	done := make(chan bool)
	chunkCh := make(chan *ChunkMeta, 1000)
	go func() {
		for chunk := range chunkCh {
			fileMeta.Chunks = append(fileMeta.Chunks, chunk)
		}
		done <- true
	}()

	// iterate over chunks
	eg := llerrgroup.New(p.threads)
	for i := int64(0); i < totalPartsNum; i++ {
		if eg.Stop() {
			return nil, fmt.Errorf("One of the threads failed. Stopping.")
		}

		partnum := i
		eg.Go(func() error {
			log.Printf("Processing part %d of %d ###\n", partnum+1, totalPartsNum)

			partSize := int64(math.Min(float64(p.chunkSize), float64(fileMeta.TotalSize-int64(partnum*p.chunkSize))))

			chunkMeta := &ChunkMeta{
				Start: partnum * p.chunkSize,
				End:   partnum*p.chunkSize + partSize - 1,
			}

			partBuffer, blockIsEmpty, err := f.getLocalChunk(chunkMeta.Start, partSize)
			if err != nil {
				return fmt.Errorf("get chunk contents: %s", err)
			}

			chunkMeta.IsEmpty = blockIsEmpty

			if !blockIsEmpty {
				chunkMeta.ContentSHA1 = fmt.Sprintf("%x", sha1.Sum(partBuffer))

				// don't fail if caching disabled
				_ = p.cachingEngine.putChunkToCache(chunkMeta.ContentSHA1, partBuffer)

				fileName := path.Join(gsPath, "blobs", chunkMeta.ContentSHA1+".blob")
				chunkMeta.URL = getGSURL(bucketName, fileName)
				exists := p.checkFileExistsOnGoogleStorage(fileName)
				if exists {
					log.Printf("File already exists: %s", fileName)
				} else {
					log.Printf("Sending file to google storage: %s", fileName)
					writeChan := make(chan error, 1)
					go func() {
						err := p.writeToGoogleStorage(chunkMeta.URL, partBuffer, true)
						writeChan <- err
					}()
					select {
					case err := <-writeChan:
						if err != nil {
							return err
						}

					case <-time.After(300 * time.Second):
						return fmt.Errorf("Upload of %s to storage timed out", fileName)
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

	close(chunkCh)
	<-done

	return fileMeta, nil

}

func (p *PITR) uploadBackupMetaYamlFile(bm BackupMeta, fileURL string) error {
	d, err := yaml.Marshal(&bm)
	if err != nil {
		return fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(fileURL, d, false)
}

func (p *PITR) uploadYamlFile(fileMeta *FileMeta, fileURL string) error {
	d, err := yaml.Marshal(&fileMeta)
	if err != nil {
		return fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(fileURL, d, false)
}
