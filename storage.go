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

func configureStorage(bucketID string) (*storage.BucketHandle, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return client.Bucket(bucketID), nil
}

func writeToGoogleStorage(filename string, data []byte, bucket *storage.BucketHandle) (string, error) {

	ctx := context.Background()
	w := bucket.Object(filename).NewWriter(ctx)
	// would make readable publicly
	//w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	w.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)
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
	return fmt.Sprintf(publicURL, opts.BucketName, filename), nil

}

func readFromGoogleStorage(filename string, bucket *storage.BucketHandle) (data []byte, err error) {

	ctx := context.Background()

	r, err := bucket.Object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func checkFileExistsOnGoogleStorage(fileName string, bucket *storage.BucketHandle) bool {
	ctx := context.Background()
	o := bucket.Object(fileName)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return false
	}
	if attrs.Name != fileName {
		return false
	}

	return true

}
