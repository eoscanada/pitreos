package main

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"time"

	yaml "gopkg.in/yaml.v2"
)

type Chunkmeta struct {
	Start   uint64
	End     uint64
	Content string
	IsEmpty bool
}

type Filemeta struct {
	FileName      string
	BlobsLocation string
	Date          time.Time
	TotalSize     int64
	Chunks        []Chunkmeta
}

func main() {

	fileToBeChunked := "file.img"

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var fm Filemeta
	fm.FileName = fileToBeChunked

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()
	fm.TotalSize = fileSize
	fm.BlobsLocation = "/here"

	const fileChunk = 50 * (1 << 20) // 50 MB, change this to your requirement
	emptyBytes := make([]byte, fileChunk)
	holeSHA1Sum := sha1.Sum(emptyBytes)

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := uint64(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		var cm Chunkmeta
		cm.Start = (i * fileChunk)
		cm.End = cm.Start + partSize - 1
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// write to disk
		partSHA1Sum := sha1.Sum(partBuffer)
		// write/save buffer to disk
		if partSHA1Sum == holeSHA1Sum {
			cm.Content = fmt.Sprintf("%x", partSHA1Sum)
			cm.IsEmpty = true
		} else {
			fileName := fmt.Sprintf("%x", partSHA1Sum) + ".blob"

			_, err := os.Create(fileName)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
			cm.Content = fmt.Sprintf("%x", partSHA1Sum)
			fmt.Printf("Wrote file: %s\n", fileName)
		}

		fm.Chunks = append(fm.Chunks, cm)
	}
	d, err := yaml.Marshal(&fm)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("file meta is: \n---\n%s\n", string(d))
}
