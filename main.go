package main

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"os"

	"cloud.google.com/go/storage"
	"github.com/abourget/llerrgroup"

	yaml "gopkg.in/yaml.v2"
)

var (
	StorageBucketName = "eoscanada-playground-pitr"
	ChunkSize         = uint64(50 * (1 << 20))
	StorageBucket     *storage.BucketHandle
)

func isEmptyChunk(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func uploadFileToChunks(fileName string, chunkSize uint64) {

	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
	}

	var fm Filemeta
	fm.FileName = fileName

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()
	fm.TotalSize = fileSize
	fm.BlobsLocation = "/here"

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	log.Printf("Splitting to %d pieces.\n", totalPartsNum)

	eg := llerrgroup.New(25)
	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := uint64(math.Min(float64(ChunkSize), float64(fileSize-int64(i*ChunkSize))))
		var cm Chunkmeta
		cm.Start = (i * ChunkSize)
		cm.End = cm.Start + partSize - 1
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		if isEmptyChunk(partBuffer) {
			cm.IsEmpty = true
		} else {
			cm.Content = fmt.Sprintf("%x", sha1.Sum(partBuffer))
			fileName := cm.Content + ".blob"

			if eg.Stop() {
				continue // short-circuit the loop if we got an error
			}
			eg.Go(func() error {
				if checkFileExistsOnGoogleStorage(fileName) {
					log.Printf("File already exists: %s\n", fileName)
					return nil
				} else {

					url, err := writeToGoogleStorage(fileName, partBuffer)
					fmt.Printf("Wrote file: %s\n", url)
					return err
				}

			})

			//newData, err := readFromGoogleStorage(fileName)
			//if err != nil {
			//	fmt.Printf("something went wrong reading")
			//	os.Exit(1)
			//}
			//newSHA1Sum := sha1.Sum(newData)
			//fmt.Printf("THIS IS THE NEW SUM %x\n", newSHA1Sum)

		}

		fm.Chunks = append(fm.Chunks, cm)
	}
	if err := eg.Wait(); err != nil {
		// eg.Wait() will block until everything is done, and return the first error.
		os.Exit(1)
	}
	d, err := yaml.Marshal(&fm)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("file meta is: \n---\n%s\n", string(d))

}

func main() {

	// parse args
	command := "backup"
	fileName := "file.img"
	if len(os.Args) > 2 {
		fileName = os.Args[2]
	}
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	// initialize google storage
	var err error
	StorageBucket, err = configureStorage(StorageBucketName)
	if err != nil {
		log.Fatal(err)
	}

	// backup file
	if command == "backup" {
		uploadFileToChunks(fileName, ChunkSize)
		os.Exit(0)
	}
	if command == "restore" {
		//downloadFileFromChunks(fileName)
		os.Exit(0)
	}

}
