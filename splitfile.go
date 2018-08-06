package main

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
)

func main() {

	fileToBeChunked := "file.img"

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 50 * (1 << 20) // 50 MB, change this to your requirement
	emptyBytes := make([]byte, fileChunk)
	holeSHA1Sum := sha1.Sum(emptyBytes)

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// write to disk
		fileName := "somebigfile_" + strconv.FormatUint(i, 10)
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// write/save buffer to disk
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)
		partSHA1Sum := sha1.Sum(partBuffer)
		if partSHA1Sum == holeSHA1Sum {
			fmt.Println("This is a hole in sparsefile")
		} else {
			fmt.Printf("Got some sum: %x\n", partSHA1Sum)
		}

		fmt.Println("Split to : ", fileName)
	}
}
