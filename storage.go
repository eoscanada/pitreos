package main

import (
	"bytes"
	"cloud.google.com/go/storage"
	"compress/gzip"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
)

var (
	storageBucket     *storage.BucketHandle
	storageBucketName string
)

func configureStorage(bucketID string) (err error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return
	}
	storageBucket = client.Bucket(bucketID)
	storageBucketName = bucketID
	return
}

func writeToGoogleStorage(filename string, data []byte, encrypt bool) (string, error) {
	if storageBucket == nil {
		return "", fmt.Errorf("storage bucket not initialized")
	}

	ctx := context.Background()
	w := storageBucket.Object(filename).NewWriter(ctx)
	defer w.Close()
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	f := bytes.NewReader(data)

	if encrypt {
		w.ContentEncoding = "gzip"
		gw := gzip.NewWriter(w)
		defer gw.Close()
		if _, err := io.Copy(gw, f); err != nil {
			return "", err
		}
	} else {
		if _, err := io.Copy(w, f); err != nil {
			return "", err
		}
	}

	const publicURL = "gs://%s/%s"
	return fmt.Sprintf(publicURL, storageBucketName, filename), nil

}

func readFromGoogleStorage(filename string) (data []byte, err error) {

	if storageBucket == nil {
		return nil, fmt.Errorf("storage bucket not initialized")
	}
	ctx := context.Background()

	r, err := storageBucket.Object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func getStorageFileURL(fileName string) string {
	return fmt.Sprintf("gs://%s/%s", storageBucketName, fileName)
}

func checkFileExistsOnGoogleStorage(fileName string) bool {
	// we don't return errors because non-existing usually returns an error.
	// error means false
	if storageBucket == nil {
		return false
	}
	ctx := context.Background()
	o := storageBucket.Object(fileName)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return false
	}
	if attrs.Name != fileName {
		return false
	}

	return true

}
