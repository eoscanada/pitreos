package pitreos

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type Storage interface {
	ListBackups(limit int, prefix string) ([]string, error)
	OpenBackupIndex(name string) (io.ReadCloser, error)
	WriteBackupIndex(name string, content []byte) error

	OpenChunk(hash string) (io.ReadCloser, error)
	WriteChunk(hash string, content []byte) error
	ChunkExists(hash string) (bool, error)
}

func SetupStorage(baseURL string) (Storage, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	switch base.Scheme {
	case "file", "":
		fmt.Println("MAMAA", base)
		return NewFSStorage(base)
	case "gs":
		return NewGSStorage(base)
	}
	return nil, fmt.Errorf("unsupported storage scheme %q", base.Scheme)
}

//
// Google Storage Storage
//

type GSStorage struct {
	baseURL *url.URL
	client  *storage.Client
	context context.Context
}

func NewGSStorage(baseURL *url.URL) (*GSStorage, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GSStorage{
		baseURL: baseURL,
		client:  client,
		context: ctx,
	}, nil
}

func (s *GSStorage) ListBackups(limit int, prefix string) (out []string, err error) {
	location := s.indexPath(prefix)
	basePrefix := strings.Replace(s.indexPath(""), ".yaml", "", 1)

	ctx := context.Background()
	iter := s.client.Bucket(s.baseURL.Host).Objects(ctx, &storage.Query{Prefix: location})
	for {
		objAttrs, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}

		// if objAttrs == nil {
		// 	return "", fmt.Errorf("Error, probably missing permissions...")
		// }
		name := objAttrs.Name
		if !strings.HasPrefix(name, basePrefix) {
			return nil, fmt.Errorf("returned object attrs is not based at %q: %q", basePrefix, name)
		}

		out = append(out, strings.Replace(objAttrs.Name[len(basePrefix):], ".yaml", "", 1))

		if len(out) >= limit {
			break
		}
	}

	return
}

func (s *GSStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.getObject(s.indexPath(name))
}

func (s *GSStorage) indexPath(name string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), "indexes", fmt.Sprintf("%s.yaml", name))
}

func (s *GSStorage) chunkPath(hash string) string {
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), "chunks", fmt.Sprintf("%s", hash))
}

func (s *GSStorage) WriteBackupIndex(name string, content []byte) (err error) {
	return s.putObject(s.indexPath(name), content)
}

func (s *GSStorage) putObject(location string, content []byte) (err error) {
	ctx := context.Background()
	w := s.client.Bucket(s.baseURL.Host).Object(location).NewWriter(ctx)
	defer w.Close()
	w.ContentType = "application/octet-stream"
	w.CacheControl = "public, max-age=86400"

	f := bytes.NewReader(content)

	gw := gzip.NewWriter(w)
	defer gw.Close()
	if _, err := io.Copy(gw, f); err != nil {
		return err
	}
	return nil
}

func (s *GSStorage) getObject(location string) (out io.ReadCloser, err error) {
	ctx := context.Background()
	r, err := s.client.Bucket(s.baseURL.Host).Object(location).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	return NewGZipReadCloser(r)
}

func (s *GSStorage) WriteChunk(hash string, content []byte) (err error) {
	return s.putObject(s.chunkPath(hash), content)
}

func (s *GSStorage) OpenChunk(hash string) (out io.ReadCloser, err error) {
	return s.getObject(s.chunkPath(hash))
}

func (s *GSStorage) ChunkExists(hash string) (bool, error) {
	location := s.chunkPath(hash)
	ctx := context.Background()
	_, err := s.client.Bucket(s.baseURL.Host).Object(location).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

//
// File system Storage
//

type FSStorage struct {
	baseURL *url.URL
}

func NewFSStorage(baseURL *url.URL) (out *FSStorage, err error) {
	if baseURL.Scheme != "file" && baseURL.Scheme != "" {
		return nil, fmt.Errorf("invalid filesystem storage scheme %q", baseURL.Scheme)
	}

	if !strings.HasPrefix(baseURL.Path, "/") {
		baseURL.Path, err = filepath.Abs(baseURL.Path)
		if err != nil {
			return
		}
	}

	if err := os.MkdirAll(filepath.Join(baseURL.Path, "chunks"), 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Join(baseURL.Path, "indexes"), 0755); err != nil {
		return nil, err
	}

	return &FSStorage{
		baseURL: baseURL,
	}, nil
}

func (s *FSStorage) ListBackups(limit int, prefix string) (out []string, err error) {
	matches, err := filepath.Glob(fmt.Sprintf("%s*", prefix))
	if err != nil {
		return
	}

	for _, m := range matches {
		out = append(out, strings.Replace(m, ".yaml", "", 1))
	}

	return
}

func (s *FSStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.getObject(s.indexPath(name))
}

func (s *FSStorage) indexPath(name string) string {
	return path.Join(s.baseURL.Path, "indexes", fmt.Sprintf("%s.yaml", name))
}

func (s *FSStorage) chunkPath(hash string) string {
	return path.Join(s.baseURL.Path, "chunks", hash)
}

func (s *FSStorage) WriteBackupIndex(name string, content []byte) (err error) {
	return s.putObject(s.indexPath(name), content)
}

func (s *FSStorage) putObject(location string, content []byte) (err error) {
	w, err := os.Create(location)
	if err != nil {
		return
	}
	defer w.Close()

	f := bytes.NewReader(content)

	gw := gzip.NewWriter(w)
	defer gw.Close()
	if _, err := io.Copy(gw, f); err != nil {
		return err
	}
	return nil
}

func (s *FSStorage) getObject(location string) (out io.ReadCloser, err error) {
	fl, err := os.Open(location)
	if err != nil {
		return nil, fmt.Errorf("getObject: %s", err)
	}

	return NewGZipReadCloser(fl)
}

func (s *FSStorage) WriteChunk(hash string, content []byte) (err error) {
	return s.putObject(s.chunkPath(hash), content)
}

func (s *FSStorage) OpenChunk(hash string) (out io.ReadCloser, err error) {
	return s.getObject(s.chunkPath(hash))
}

func (s *FSStorage) ChunkExists(hash string) (bool, error) {
	location := s.chunkPath(hash)
	_, err := os.Stat(location)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
