package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"

	"cloud.google.com/go/storage"
	"github.com/abourget/llerrgroup"
	"golang.org/x/net/context"

	yaml "gopkg.in/yaml.v2"
)

var (
	StorageBucketName = "eoscanada-playground-pitr"
	fileChunk         = uint64(50 * (1 << 20))
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

func configureStorage(bucketID string) (*storage.BucketHandle, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.Bucket(bucketID), nil
}

func writeToGoogleStorage(filename string, data []byte) (string, error) {
	if StorageBucket == nil {
		return "", errors.New("storage bucket is missing")
	}

	ctx := context.Background()
	w := StorageBucket.Object(filename).NewWriter(ctx)
	w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
	w.ContentType = "application/octet-stream"

	// Entries are immutable, be aggressive about caching (1 day).
	w.CacheControl = "public, max-age=86400"

	w.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)

	//f, err := os.Open("README.md")
	//if err != nil {
	//	return "", err
	//}
	f := bytes.NewReader(data)

	if _, err := io.Copy(gw, f); err != nil {
		return "", err
	}
	if err := gw.Close(); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}

	const publicURL = "https://storage.googleapis.com/%s/%s"
	return fmt.Sprintf(publicURL, StorageBucketName, filename), nil

}

func readFromGoogleStorage(filename string) (data []byte, err error) {
	if StorageBucket == nil {
		return nil, errors.New("storage bucket is missing")
	}

	ctx := context.Background()

	r, err := StorageBucket.Object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	data, err = ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return data, nil

}

func checkFileExistsOnGoogleStorage(fileName string) bool {
	ctx := context.Background()
	o := StorageBucket.Object(fileName)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return false
	}
	if attrs.Name != fileName {
		return false
	}

	return true

}

func main() {

	var err error
	StorageBucket, err = configureStorage(StorageBucketName)
	if err != nil {
		log.Fatal(err)
	}

	fileToBeChunked := "file.img"

	file, err := os.Open(fileToBeChunked)

	if err != nil {
		log.Fatal(err)
	}

	var fm Filemeta
	fm.FileName = fileToBeChunked

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()
	fm.TotalSize = fileSize
	fm.BlobsLocation = "/here"

	// calculate total number of parts the file will be chunked into

	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	log.Printf("Splitting to %d pieces.\n", totalPartsNum)

	eg := llerrgroup.New(25)
	for i := uint64(0); i < totalPartsNum; i++ {

		partSize := uint64(math.Min(float64(fileChunk), float64(fileSize-int64(i*fileChunk))))
		var cm Chunkmeta
		cm.Start = (i * fileChunk)
		cm.End = cm.Start + partSize - 1
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		if isEmptyChunk(partBuffer) {
			cm.IsEmpty = true
		} else {
			cm.Content = fmt.Sprintf("%x", sha1.Sum(partBuffer))
			fileName := cm.Content + ".blob"

			if checkFileExistsOnGoogleStorage(fileName) {
				log.Printf("File already exists: %s\n", fileName)
			} else {
				if eg.Stop() {
					continue // short-circuit the loop if we got an error
				}
				eg.Go(func() error {
					url, err := writeToGoogleStorage(fileName, partBuffer)
					fmt.Printf("Wrote file: %s\n", url)
					return err
				})
				if err != nil {
					fmt.Printf("something went wrong, %s", err)
					os.Exit(1)
				}
			}

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
