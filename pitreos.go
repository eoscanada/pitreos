package pitreos

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/abourget/llerrgroup"
	humanize "github.com/dustin/go-humanize"

	yaml "gopkg.in/yaml.v2"
)

var (
	ChunkSize      = int64(250 * 1024 * 1024)
	NetworkThreads = 10
)

func downloadFileFromChunks(fm Filemeta, localFolder string) error {
	log.Printf("Restoring file %s with size %s from snapshot dated %s\n", fm.FileName, humanize.Bytes(uint64(fm.TotalSize)), fm.Date)

	filePath := filepath.Join(localFolder, fm.FileName)

	err := os.MkdirAll(path.Dir(filePath), 0755)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err = f.Truncate(fm.TotalSize); err != nil {
		return err
	}
	defer f.Close()
	if err != nil {
		return err
	}

	eg := llerrgroup.New(NetworkThreads)
	numChunks := len(fm.Chunks)
	for n, i := range fm.Chunks {

		if eg.Stop() {
			return fmt.Errorf("Got an error in thread management. Stopping.")
		}
		eg.Go(func() error {

			partBuffer, blockIsEmpty := getChunkContentUnlessEmpty(f, int64(i.Start), int64(i.End-i.Start+1))

			if blockIsEmpty {
				if i.IsEmpty {
					log.Printf("Skipping empty chunk #%d / %d", n, numChunks)
					return nil
				}
			} else {
				readSHA1Sum := sha1.Sum(partBuffer)
				shasum := fmt.Sprintf("%x", readSHA1Sum)
				if shasum == i.Content {
					log.Printf("Skipping correct chunk #%d / %d", n, numChunks)
					return nil
				}
			}

			if i.IsEmpty {
				log.Printf("Punching a hole through chunk #%d / %d", n, numChunks)
				err := wipeChunk(f, i.Start, i.End-i.Start+1)
				if err != nil {
					return err
				}
				return nil
			}

			fmt.Printf("Downloading... chunk #%d / %d with SHA1 %s\n (%s)", n, numChunks, i.Content, humanize.Bytes(uint64(i.End-i.Start-1)))
			blobPath := getStorageFilePath(i.URL)
			blobStart := i.Start
			thischunk := n
			expectedSum := i.Content
			newData, err := readFromGoogleStorage(blobPath)
			if err != nil {
				log.Printf("Something went wrong reading, error = %s\n", err)
				return err
			}
			newSHA1Sum := fmt.Sprintf("%x", sha1.Sum(newData))
			log.Printf("Finished downloading chunk #%d / %d with new sum: %s\n", thischunk, numChunks, newSHA1Sum)
			if expectedSum != newSHA1Sum {
				return fmt.Errorf("Invalid sha1sum from downloaded blob. Got %s, expected %s\n", newSHA1Sum, expectedSum)
			}
			return writeChunkToFile(f, int64(blobStart), newData)
		})

	}
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func uploadFileToChunks(
	filePath string,
	fileName string,
	bucketFolder string,
	timestamp time.Time,
) Filemeta {

	f, err := os.Open(filePath)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	fileInfo, _ := f.Stat()
	fm := Filemeta{
		Kind:        "blobMap",
		Metaversion: "v1",
		FileName:    fileName,
		TotalSize:   fileInfo.Size(),
		Date:        timestamp,
	}

	// calculate total number of parts the file will be chunked into
	totalPartsNum := int64(math.Ceil(float64(fm.TotalSize) / float64(ChunkSize)))

	log.Printf("Splitting to %d pieces.\n", totalPartsNum)

	eg := llerrgroup.New(10)
	for i := int64(0); i < totalPartsNum; i++ {

		fmt.Printf("### Processing part %d of %d ###\n", i, totalPartsNum)
		partSize := int64(math.Min(float64(ChunkSize), float64(fm.TotalSize-int64(i*ChunkSize))))
		var cm Chunkmeta
		cm.Start = (i * ChunkSize)
		cm.End = cm.Start + partSize - 1

		partBuffer, blockIsEmpty := getChunkContentUnlessEmpty(f, int64(cm.Start), int64(partSize))
		cm.IsEmpty = blockIsEmpty

		if !blockIsEmpty {
			cm.Content = fmt.Sprintf("%x", sha1.Sum(partBuffer))
			fileName := path.Join(bucketFolder, cm.Content+".blob")

			if eg.Stop() {
				continue // short-circuit the loop if we got an error
			}
			cm.URL = getStorageFileURL(fileName)
			eg.Go(func() error {
				itExists := checkFileExistsOnGoogleStorage(fileName)
				if itExists {
					log.Printf("File already exists: %s\n", fileName)
					return nil
				} else {
					_, err := writeToGoogleStorage(fileName, partBuffer, true)
					if err != nil {
						return err
					}
					return nil
				}
			})
		}

		fm.Chunks = append(fm.Chunks, cm)
	}
	fmt.Printf("%+v\n", fm)
	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}
	return fm

}

func RestoreFromBackup(opts *PitreosOptions) error {
	err := configureStorage(opts.BucketName)
	if err != nil {
		return err
	}
	if opts.BeforeTimestamp == 0 {
		opts.BeforeTimestamp = time.Now().Unix()
	}
	wantedBackupYAML, err := findAvailableBackup(opts.BeforeTimestamp, opts.BucketFolder, opts.BackupTag)
	if err != nil {
		return err
	}

	bm := downloadBackupMetaYamlFile(wantedBackupYAML)
	fmt.Printf("%+v\n", bm)
	for _, y := range bm.MetadataFiles {
		fm := downloadYamlFile(getStorageFilePath(y))
		err := downloadFileFromChunks(*fm, opts.LocalFolder)
		if err != nil {
			return err
		}
	}

	return nil
}

func GenerateBackup(opts *PitreosOptions) error {
	err := configureStorage(opts.BucketName)
	if err != nil {
		return err
	}

	now := time.Now()
	bm := Backupmeta{
		Date:        now,
		Tag:         opts.BackupTag,
		Kind:        "filesMap",
		Metaversion: "v1",
	}
	nowString := fmt.Sprintf(time.Now().UTC().Format(time.RFC3339))

	dirs, err := getDirFiles(opts.LocalFolder)
	for _, file := range dirs {
		fileName, err := filepath.Rel(opts.LocalFolder, file)
		if err != nil {
			return err
		}

		fm := uploadFileToChunks(file, fileName, path.Join(opts.BucketFolder, "blobs"), now)

		yamlURL := uploadYamlFile(fm, path.Join(opts.BucketFolder, opts.BackupTag, nowString, "yaml", fm.FileName+".yaml"))
		bm.MetadataFiles = append(bm.MetadataFiles, yamlURL)
	}
	yamlURL := uploadBackupMetaYamlFile(bm, path.Join(opts.BucketFolder, opts.BackupTag, nowString, "index.yaml"))
	fmt.Printf("whole files meta: %s\n", yamlURL)
	return nil
}

func uploadBackupMetaYamlFile(bm Backupmeta, filePath string) (url string) {
	d, err := yaml.Marshal(&bm)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	url, err = writeToGoogleStorage(filePath, d, false)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	return
}

func downloadBackupMetaYamlFile(filePath string) *Backupmeta {
	y, err := readFromGoogleStorage(filePath)
	if err != nil {
		log.Fatal(err)
	}

	var bm Backupmeta
	if err = yaml.Unmarshal(y, &bm); err != nil {
		log.Fatalf("error: %v", err)
	}
	return &bm
}

func downloadYamlFile(filePath string) *Filemeta {
	y, err := readFromGoogleStorage(filePath)
	if err != nil {
		log.Fatal(err)
	}

	var fm Filemeta
	if err = yaml.Unmarshal(y, &fm); err != nil {
		log.Fatalf("error: %v", err)
	}
	return &fm
}

func uploadYamlFile(fm Filemeta, filePath string) (url string) {

	d, err := yaml.Marshal(&fm)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	url, err = writeToGoogleStorage(filePath, d, false)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	return
}
