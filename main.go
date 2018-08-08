package main

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"

	"cloud.google.com/go/storage"
	"github.com/abourget/llerrgroup"

	yaml "gopkg.in/yaml.v2"
)

var (
	StorageBucketName = "eoscanada-playground-pitr"
	ChunkSize         = uint64(250 * (1 << 20))
	StorageBucket     *storage.BucketHandle
)

func downloadFileFromChunks(fm Filemeta) {
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

		partBuffer, blockIsEmpty := getChunkContentUnlessEmpty(f, i.Start, i.End-i.Start+1)

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
			err := WipeChunk(f, i.Start, i.End-i.Start+1)
			if err != nil {
				log.Fatalln(err)
			}
			continue
		}

		fmt.Printf("...instead of sha1sum %s. Downloading...\n", i.Content)
		blobPath := fmt.Sprintf("%s.blob", i.Content)
		blobStart := int64(i.Start)
		thischunk := n
		expectedSum := i.Content
		if eg.Stop() {
			fmt.Println("got stop signal")
			continue
		}
		eg.Go(func() error {
			newData, err := readFromGoogleStorage(blobPath)
			if err != nil {
				fmt.Printf("something went wrong reading, error = %s\n", err)
				return err
			}
			newSHA1Sum := fmt.Sprintf("%x", sha1.Sum(newData))
			fmt.Printf("Got chunk #%d with new sum: %s\n", thischunk, newSHA1Sum)
			if expectedSum != newSHA1Sum {
				return fmt.Errorf("Invalid sha1sum from downloaded blob. Got %s, expected %s\n", newSHA1Sum, expectedSum)
			}
			mutex.Lock()
			f.Seek(blobStart, 0)
			f.Write(newData)
			mutex.Unlock()
			return nil
		})

	}
	if err := eg.Wait(); err != nil {
		log.Fatalln(err)
	}
}

func uploadFileToChunks(fileName string, chunkSize uint64) {

	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	fileInfo, _ := f.Stat()
	fm := Filemeta{FileName: fileName, TotalSize: fileInfo.Size(), BlobsLocation: ""}

	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fm.TotalSize) / float64(chunkSize)))

	log.Printf("Splitting to %d pieces.\n", totalPartsNum)

	eg := llerrgroup.New(60)
	for i := uint64(0); i < totalPartsNum; i++ {

		fmt.Printf("### Processing part %d of %d ###\n", i, totalPartsNum)
		partSize := uint64(math.Min(float64(ChunkSize), float64(fm.TotalSize-int64(i*ChunkSize))))
		var cm Chunkmeta
		cm.Start = (i * ChunkSize)
		cm.End = cm.Start + partSize - 1

		partBuffer, blockIsEmpty := getChunkContentUnlessEmpty(f, cm.Start, partSize)
		cm.IsEmpty = blockIsEmpty

		if !blockIsEmpty {
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

func main() {

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

	fmt.Println(command)
	// backup file
	if command == "backup" {
		uploadFileToChunks(fileName, ChunkSize)
		os.Exit(0)
	}

	if command == "restore" {
		var fm Filemeta

		y, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Fatal(err)
		}

		err = yaml.Unmarshal(y, &fm)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		downloadFileFromChunks(fm)
		os.Exit(0)
	}

}
