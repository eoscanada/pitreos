package pitreos

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type Storage interface {
	ListBackups(limit int, prefix string) ([]string, error)
	OpenBackup(name string) (io.ReadCloser, error)
	WriteBackup(name string, content []byte) error

	OpenChunk(hash string) (io.ReadCloser, error)
	WriteChunk(hash string, content []byte) error
	ChunkExists(hash string) (bool, error)
}

func (p *PITR) setupStorageClient() (err error) {
	ctx := context.Background()
	p.storageClient, err = storage.NewClient(ctx)
	return err
}

func (p *PITR) findAvailableBackup(sourceURL string, targetTimestamp time.Time) (string, error) {
	bucketName, sourcePath, err := splitGSURL(sourceURL)
	if err != nil {
		return "", err
	}
	if p.storageClient == nil {
		return "", fmt.Errorf("storage bucket not initialized")
	}

	timeString := fmt.Sprintf(targetTimestamp.UTC().Format(time.RFC3339))

	latestValidTimestamp := ""
	latestValidFilepath := ""
	ctx := context.Background()
	iter := p.storageClient.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: sourcePath})
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
		if strings.HasSuffix(objAttrs.Name, "index.yaml") && filepath.Dir(filepath.Dir(objAttrs.Name)) == sourcePath {
			thisTimestamp := strings.TrimSuffix(strings.TrimPrefix(objAttrs.Name, sourcePath+"/"), "/index.yaml")

			if thisTimestamp < timeString { //valid timestamp
				if thisTimestamp > latestValidTimestamp { //newer
					latestValidTimestamp = thisTimestamp
					latestValidFilepath = objAttrs.Name
				}
			}
		}
	}

	if latestValidFilepath == "" {
		return "", fmt.Errorf("cannot find any available backup in %s before %s", sourceURL, timeString)
	}

	return getGSURL(bucketName, latestValidFilepath), nil
}

func (p *PITR) writeToGoogleStorage(URL string, data []byte, compress bool) error {
	if p.storageClient == nil {
		return fmt.Errorf("storage client not initialized")
	}

	bucketName, fileLocation, err := splitGSURL(URL)
	if err != nil {
		return err
	}

	ctx := context.Background()
	w := p.storageClient.Bucket(bucketName).Object(fileLocation).NewWriter(ctx)
	defer w.Close()
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	f := bytes.NewReader(data)

	if compress {
		w.ContentEncoding = "gzip"
		gw := gzip.NewWriter(w)
		defer gw.Close()
		if _, err := io.Copy(gw, f); err != nil {
			return err
		}
	} else {
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
	}

	return nil
}

func (p *PITR) readFromGoogleStorage(URL string) (data []byte, err error) {

	if p.storageClient == nil {
		return nil, fmt.Errorf("storage client not initialized")
	}

	bucketName, fileLocation, err := splitGSURL(URL)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	r, err := p.storageClient.Bucket(bucketName).Object(fileLocation).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (p *PITR) checkFileExistsOnGoogleStorage(URL string) bool {
	// any error (ex: 404, 403...) will return false
	if p.storageClient == nil {
		return false
	}

	bucketName, fileLocation, err := splitGSURL(URL)
	if err != nil {
		return false
	}

	ctx := context.Background()
	o := p.storageClient.Bucket(bucketName).Object(fileLocation)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return false
	}
	if attrs.Name != fileLocation {
		return false
	}

	return true

}
