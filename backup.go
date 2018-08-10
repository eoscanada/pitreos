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

func (p *PITR) GenerateBackup() error {
	err := p.setupStorage()
	if err != nil {
		return err
	}

	now := time.Now()
	bm := BackupMeta{
		Date:        now,
		Tag:         p.Options.BackupTag,
		Kind:        "filesMap",
		MetaVersion: "v1",
	}
	nowString := fmt.Sprintf(time.Now().UTC().Format(time.RFC3339))

	dirs, err := getDirFiles(p.Options.LocalFolder)
	for _, filePath := range dirs {
		relName, err := filepath.Rel(p.Options.LocalFolder, filePath)
		if err != nil {
			return err
		}

		fm, err := p.uploadFileToChunks(filePath, relName, path.Join(p.Options.BucketFolder, "blobs"), now)
		if err != nil {
			return fmt.Errorf("upload file to chunks: %s", err)
		}

		yamlURL, err := p.uploadYamlFile(fm, path.Join(p.Options.BucketFolder, p.Options.BackupTag, nowString, "yaml", fm.FileName+".yaml"))
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
	fm := &FileMeta{
		Kind:        "blobMap",
		MetaVersion: "v1",
		FileName:    fileName,
		TotalSize:   fileInfo.Size(),
		Date:        timestamp,
	}

	// calculate total number of parts the file will be chunked into
	totalPartsNum := int64(math.Ceil(float64(fm.TotalSize) / float64(p.chunkSize)))

	log.Printf("Splitting to %d pieces.\n", totalPartsNum)

	done := make(chan bool)
	chunkCh := make(chan *ChunkMeta, 1000)
	go func() {
		for chunk := range chunkCh {
			fm.Chunks = append(fm.Chunks, chunk)
		}
		done <- true
	}()

	eg := llerrgroup.New(p.threads)
	for i := int64(0); i < totalPartsNum; i++ {
		if eg.Stop() {
			break // short-circuit the loop if we got an error
		}

		eg.Go(func() error {
			log.Printf("Processing part %d of %d ###\n", i, totalPartsNum)

			partSize := int64(math.Min(float64(p.chunkSize), float64(fm.TotalSize-int64(i*p.chunkSize))))

			cm := &ChunkMeta{
				Start: i * p.chunkSize,
				End:   i*p.chunkSize + partSize - 1,
			}

			partBuffer, blockIsEmpty, err := f.getLocalChunk(cm.Start, partSize)
			if err != nil {
				return fmt.Errorf("get chunk contents: %s", err)
			}

			cm.IsEmpty = blockIsEmpty

			if !blockIsEmpty {
				cm.Content = fmt.Sprintf("%x", sha1.Sum(partBuffer))
				fileName := path.Join(bucketFolder, cm.Content+".blob")

				cm.URL = p.getStorageFileURL(fileName)
				exists := p.checkFileExistsOnGoogleStorage(fileName)
				if exists {
					log.Printf("File already exists: %s\n", fileName)
				} else {
					_, err := p.writeToGoogleStorage(fileName, partBuffer, true)
					if err != nil {
						return err
					}
				}
			}

			chunkCh <- cm

			return nil
		})

	}

	//json.NewEncoder(os.Stdout).Encode(fm)

	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}

	close(chunkCh)
	<-done

	return fm, nil

}

func (p *PITR) uploadBackupMetaYamlFile(bm BackupMeta, filePath string) (url string, err error) {
	d, err := yaml.Marshal(&bm)
	if err != nil {
		return "", fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(filePath, d, false)
}

func (p *PITR) uploadYamlFile(fm *FileMeta, filePath string) (url string, err error) {
	d, err := yaml.Marshal(&fm)
	if err != nil {
		return "", fmt.Errorf("yaml marshal: %s", err)
	}
	return p.writeToGoogleStorage(filePath, d, false)
}
