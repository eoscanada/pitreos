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

func (p *PITR) GenerateBackup(details map[string]interface{}) error {
	if err := p.setupStorage(); err != nil {
		return err
	}

	if err := p.setupCaching(); err != nil {
		return err
	}

	now := time.Now()
	bm := BackupMeta{
		Date:        now,
		Tag:         p.Options.BackupTag,
		Kind:        "filesMap",
		MetaVersion: "v1",
		Details:     details,
	}
	nowString := fmt.Sprintf(time.Now().UTC().Format(time.RFC3339))

	dirs, err := getDirFiles(p.Options.LocalFolder)
	for _, filePath := range dirs {
		relName, err := filepath.Rel(p.Options.LocalFolder, filePath)
		if err != nil {
			return err
		}

		fileMeta, err := p.uploadFileToChunks(filePath, relName, path.Join(p.Options.BucketFolder, "blobs"), now)
		if err != nil {
			return fmt.Errorf("upload file to chunks: %s", err)
		}

		yamlURL, err := p.uploadYamlFile(fileMeta, path.Join(p.Options.BucketFolder, p.Options.BackupTag, nowString, "yaml", fileMeta.FileName+".yaml"))
		if err != nil {
			return fmt.Errorf("upload yaml: %s", err)
		}

		bm.MetadataFiles = append(bm.MetadataFiles, yamlURL)
	}

	yamlURL, err := p.uploadBackupMetaYamlFile(bm, path.Join(p.Options.BucketFolder, p.Options.BackupTag, nowString, "index.yaml"))
	if err != nil {
		return fmt.Errorf("upload backup meta: %s", err)
	}

	log.Println("Backup meta uploaded:", yamlURL)

	return nil
}

func (p *PITR) uploadFileToChunks(filePath, fileName, bucketFolder string, timestamp time.Time) (*FileMeta, error) {

	f := NewFileOps(filePath, false)
	if err := f.Open(); err != nil {
		return nil, fmt.Errorf("open file: %s", err)
	}
	defer f.Close()

	fileInfo, _ := f.file.Stat()
	fileMeta := &FileMeta{
		Kind:        "blobMap",
		MetaVersion: "v1",
		FileName:    fileName,
		TotalSize:   fileInfo.Size(),
		Date:        timestamp,
	}

	// calculate total number of parts the file will be chunked into
	totalPartsNum := int64(math.Ceil(float64(fileMeta.TotalSize) / float64(p.chunkSize)))

	log.Printf("Splitting %s to %d pieces.\n", fileName, totalPartsNum)

	done := make(chan bool)
	chunkCh := make(chan *ChunkMeta, 1000)
	go func() {
		for chunk := range chunkCh {
			fileMeta.Chunks = append(fileMeta.Chunks, chunk)
		}
		done <- true
	}()

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

				fileName := path.Join(bucketFolder, chunkMeta.ContentSHA1+".blob")
				chunkMeta.URL = p.getStorageFileURL(fileName)
				exists := p.checkFileExistsOnGoogleStorage(fileName)
				if exists {
					log.Printf("File already exists: %s", fileName)
				} else {
					log.Printf("Sending file to google storage: %s", fileName)
					writeChan := make(chan error, 1)
					go func() {
						_, err := p.writeToGoogleStorage(fileName, partBuffer, true)
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

func (p *PITR) uploadBackupMetaYamlFile(bm BackupMeta, filePath string) (url string, err error) {
	d, err := yaml.Marshal(&bm)
	if err != nil {
		return "", fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(filePath, d, false)
}

func (p *PITR) uploadYamlFile(fileMeta *FileMeta, filePath string) (url string, err error) {
	d, err := yaml.Marshal(&fileMeta)
	if err != nil {
		return "", fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(filePath, d, false)
}
