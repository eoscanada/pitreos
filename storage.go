package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
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

func findAvailableBackup(
	beforeTimestamp int64,
	bucketFolder string,
	backupTag string,
) (latestValidFilepath string, err error) {
	if storageBucket == nil {
		return "", fmt.Errorf("storage bucket not initialized")
	}

	timeString := fmt.Sprintf(time.Unix(beforeTimestamp, 0).UTC().Format(time.RFC3339))
	prefix := fmt.Sprintf(path.Join(bucketFolder, backupTag))

	latestValidTimestamp := ""
	ctx := context.Background()
	iter := storageBucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		objAttrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}

		if objAttrs == nil {
			return "", fmt.Errorf("Error, probably missing permissions...")
		}
		if strings.HasSuffix(objAttrs.Name, "index.yaml") {
			thisTimestamp := strings.TrimSuffix(strings.TrimPrefix(objAttrs.Name, prefix+"/"), "/index.yaml")

			if thisTimestamp < timeString { //valid timestamp
				if thisTimestamp > latestValidTimestamp { //newer
					latestValidTimestamp = thisTimestamp
					latestValidFilepath = objAttrs.Name
				}
			}
		}
	}

	if latestValidFilepath == "" {
		err = fmt.Errorf("cannot find any")
	}

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

	return getStorageFileURL(filename), nil

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
