package pitreos

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func (p *PITR) setupStorage() (err error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return
	}
	p.storageBucket = client.Bucket(p.Options.BucketName)
	return
}

func (p *PITR) findAvailableBackup() (latestValidFilepath string, err error) {
	if p.storageBucket == nil {
		return "", fmt.Errorf("storage bucket not initialized")
	}

	timeString := fmt.Sprintf(time.Unix(p.Options.BeforeTimestamp, 0).UTC().Format(time.RFC3339))
	prefix := fmt.Sprintf(path.Join(p.Options.BucketFolder, p.Options.BackupTag))

	latestValidTimestamp := ""
	ctx := context.Background()
	iter := p.storageBucket.Objects(ctx, &storage.Query{Prefix: prefix})
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
		if strings.HasSuffix(objAttrs.Name, "index.yaml") && filepath.Dir(filepath.Dir(objAttrs.Name)) == prefix {
			//log.Println("Found name is " + objAttrs.Name)
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

	log.Println("Restoring from this backup file: " + p.getStorageFileURL(latestValidFilepath))
	return
}

func (p *PITR) writeToGoogleStorage(filename string, data []byte, compress bool) (string, error) {
	if p.storageBucket == nil {
		return "", fmt.Errorf("storage bucket not initialized")
	}

	ctx := context.Background()
	w := p.storageBucket.Object(filename).NewWriter(ctx)
	defer w.Close()
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	f := bytes.NewReader(data)

	if compress {
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

	return p.getStorageFileURL(filename), nil
}

func (p *PITR) readFromGoogleStorage(filename string) (data []byte, err error) {

	if p.storageBucket == nil {
		return nil, fmt.Errorf("storage bucket not initialized")
	}
	ctx := context.Background()

	r, err := p.storageBucket.Object(filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (p *PITR) getStorageFileURL(fileName string) string {
	return fmt.Sprintf("gs://%s/%s", p.Options.BucketName, fileName)
}

func (p *PITR) getStorageFilePath(URL string) string {
	return strings.TrimPrefix(URL, fmt.Sprintf("gs://%s/", p.Options.BucketName))
}

func (p *PITR) checkFileExistsOnGoogleStorage(fileName string) bool {
	// we don't return errors because non-existing usually returns an error.
	// error means false
	if p.storageBucket == nil {
		return false
	}
	ctx := context.Background()
	o := p.storageBucket.Object(fileName)
	attrs, err := o.Attrs(ctx)
	if err != nil {
		return false
	}
	if attrs.Name != fileName {
		return false
	}

	return true

}
