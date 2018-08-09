package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	flags "github.com/jessevdk/go-flags"

	"cloud.google.com/go/storage"
	"github.com/abourget/llerrgroup"

	yaml "gopkg.in/yaml.v2"
)

var (
	ChunkSize = int64(250 * 1024 * 1024)
	opts      struct {
		BucketName string `long:"bucket-name" description:"GS bucket name where backups are stored" default:"eoscanada-playground-pitr"`

		BucketDir string `long:"bucket-dir" description:"Prefixed directory in GS bucket where backups are stored" default:"backups"`

		StateFolder string `short:"s" long:"state-folder" description:"Folder relative to cwd where state files can be found" default:"state"`

		BlocksFolder string `short:"b" long:"blocks-folder" description:"Folder relative to cwd where blocks files can be found" default:"blocks"`

		StateBackupType string `long:"state-backup-type" description:"The type of state files under which the backup is classified, because the state differs when certain plugins are present" default:"standard"`

		BlocksBackupType string `long:"blocks-backup-type" description:"The type of blocks files under which the backup is classified, currently only 'standard' seems to be useful" default:"standard"`

		Timestamp int64 `long:"timestamp" description:"unix timestamp before which we want the latest restorable backup" default:"0"`
		Args      struct {
			Command string
		} `positional-args:"yes" required:"yes"`
	}
)

func downloadFileFromChunks(fm Filemeta, bucket *storage.BucketHandle) {
	log.Printf("Restoring file %s with size %d from snapshot dated %s\n", fm.FileName, fm.TotalSize, fm.Date)

	f, err := os.OpenFile(fm.FileName, os.O_RDWR|os.O_CREATE, 0644)
	if err = f.Truncate(fm.TotalSize); err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	eg := llerrgroup.New(10)
	for n, i := range fm.Chunks {

		partBuffer, blockIsEmpty := getChunkContentUnlessEmpty(f, int64(i.Start), int64(i.End-i.Start+1))

		if blockIsEmpty {
			fmt.Printf("block #%d is empty: ", n)
			if i.IsEmpty {
				fmt.Println("...Excellent")
				continue
			}
		} else {
			readSHA1Sum := sha1.Sum(partBuffer)
			shasum := fmt.Sprintf("%x", readSHA1Sum)
			fmt.Printf("block #%d has this sha1: %s...", n, shasum)
			if shasum == i.Content {
				fmt.Println("...Excellent")
				continue
			}
		}

		if i.IsEmpty {
			fmt.Println("...Punching a hole through")
			err := wipeChunk(f, i.Start, i.End-i.Start+1)
			if err != nil {
				log.Fatalln(err)
			}
			continue
		}

		fmt.Printf("...instead of sha1sum %s. Downloading...\n", i.Content)
		blobPath := fmt.Sprintf("%s.blob", i.Content)
		blobStart := i.Start
		thischunk := n
		expectedSum := i.Content
		if eg.Stop() {
			fmt.Println("got stop signal")
			continue
		}
		eg.Go(func() error {
			newData, err := readFromGoogleStorage(blobPath, bucket)
			if err != nil {
				fmt.Printf("something went wrong reading, error = %s\n", err)
				return err
			}
			newSHA1Sum := fmt.Sprintf("%x", sha1.Sum(newData))
			fmt.Printf("Got chunk #%d with new sum: %s\n", thischunk, newSHA1Sum)
			if expectedSum != newSHA1Sum {
				return fmt.Errorf("Invalid sha1sum from downloaded blob. Got %s, expected %s\n", newSHA1Sum, expectedSum)
			}
			return writeChunkToFile(f, int64(blobStart), newData)
		})

	}
	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}
}

func uploadFileToChunks(filePath string, fileName string, chunkSize int64, bucket *storage.BucketHandle) {

	f, err := os.Open(filePath)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	fileInfo, _ := f.Stat()
	fm := Filemeta{
		FileName:      fileName,
		TotalSize:     fileInfo.Size(),
		BlobsLocation: "",
	}

	// calculate total number of parts the file will be chunked into
	totalPartsNum := int64(math.Ceil(float64(fm.TotalSize) / float64(chunkSize)))

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
			fileName := cm.Content + ".blob"

			if eg.Stop() {
				continue // short-circuit the loop if we got an error
			}
			eg.Go(func() error {
				if checkFileExistsOnGoogleStorage(fileName, bucket) {
					log.Printf("File already exists: %s\n", fileName)
					return nil
				} else {
					url, err := writeToGoogleStorage(fileName, partBuffer, bucket)
					fmt.Printf("Wrote file: %s\n", url)
					return err
				}
			})
		}

		fm.Chunks = append(fm.Chunks, cm)
	}
	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}
	d, err := yaml.Marshal(&fm)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("file meta is: \n---\n%s\n", string(d))

}

func initializeBucket(bucketName string) (buck *storage.BucketHandle, err error) {
	buck, err = configureStorage(bucketName)
	return
}

func generateBackup() error {
	bucket, err := initializeBucket(opts.BucketName)
	if err != nil {
		return err
	}

	_ = bucket
	dirs, err := getDirFiles(opts.StateFolder)
	for _, file := range dirs {
		fileName, err := filepath.Rel(opts.StateFolder, file)
		if err != nil {
			return err
		}

		// add errorhandling, get yaml content...
		// prepare metayaml
		uploadFileToChunks(file, fileName, ChunkSize, bucket)
	}
	dirs, err = getDirFiles(opts.BlocksFolder)
	for _, file := range dirs {
		fileName, err := filepath.Rel(opts.BlocksFolder, file)
		if err != nil {
			return err
		}
		// add errorhandling, get yaml content...
		// prepare metayaml
		uploadFileToChunks(file, fileName, ChunkSize, bucket)
	}
	return nil
}

func main() {

	_, err := flags.Parse(&opts)
	if err != nil {
		log.Fatalln(err)
	}

	switch opts.Args.Command {
	case "backup":
		err := generateBackup()
		if err != nil {
			log.Fatalln(err)
		}

	case "restore":
		fmt.Println("trying to find what to download")
		//err := restoreFromBackup()
		//if err != nil {
	//		log.Fatalln(err)
	//	}
	// downloadFileFromChunks(fm, bucket)
	default:
		log.Fatalln("Unknown command", opts.Args.Command)
	}

}
