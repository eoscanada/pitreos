package pitreos

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
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
	ListBackups(limit int, offset int, prefix string) ([]string, error)
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

func (s *GSStorage) ListBackups(limit int, offset int, prefix string) (out []string, err error) {
	slashLocation := strings.TrimSuffix(s.indexPath(prefix), ".yaml.gz") // ex: /myapp/v1/indexes/2018-*
	location := strings.TrimPrefix(slashLocation, "/")                   // GS does not want "/" in prefix filter

	ctx := context.Background()
	iter := s.client.Bucket(s.baseURL.Host).Objects(ctx, &storage.Query{Prefix: location})

	var chronoOut []string
	for {
		objAttrs, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}

		name := objAttrs.Name
		if !strings.HasSuffix(name, ".yaml.gz") {
			log.Printf("ignoring file: %s with wrong suffix", name)
			break // ignore any non-yaml.gz file
		}

		chronoOut = append(chronoOut, objAttrs.Name)

	}

	for i := len(chronoOut) - offset - 1; i >= int(math.Max(0, float64(len(chronoOut)-offset-limit))); i-- {
		out = append(out, strings.TrimSuffix(path.Base(chronoOut[i]), ".yaml.gz"))
	}

	return
}

func (s *GSStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.getObject(s.indexPath(name))
}

func (s *GSStorage) indexPath(name string) string {
	if name == "" {
		return path.Join(s.baseURL.Path, "indexes")
	}
	return path.Join(strings.TrimLeft(s.baseURL.Path, "/"), "indexes", fmt.Sprintf("%s.yaml.gz", name))
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

func (s *FSStorage) ListBackups(limit int, offset int, prefix string) (out []string, err error) {

	matches, err := filepath.Glob(fmt.Sprintf("%s/%s*.yaml.gz", s.indexPath(""), prefix))
	if err != nil {
		return
	}

	// reverse and keep 'limit' entries max
	for i := len(matches) - offset - 1; i >= int(math.Max(0, float64(len(matches)-offset-limit))); i-- {
		out = append(out, strings.TrimSuffix(path.Base(matches[i]), ".yaml.gz"))
	}

	return
}

func (s *FSStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.getObject(s.indexPath(name))
}

func (s *FSStorage) indexPath(name string) string {
	if name == "" {
		return path.Join(s.baseURL.Path, "indexes")
	}
	return path.Join(s.baseURL.Path, "indexes", fmt.Sprintf("%s.yaml.gz", name))
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

//
// HTTP Storage
//

type HTTPStorage struct {
	baseURL *url.URL
	Client  *http.Client
}

func NewHTTPStorage(baseURL *url.URL) (out *HTTPStorage, err error) {
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("invalid http storage scheme %q", baseURL.Scheme)
	}

	return &HTTPStorage{
		baseURL: baseURL,
		Client:  http.DefaultClient,
	}, nil
}

func (s *HTTPStorage) ListBackups(limit int, offset int, prefix string) (out []string, err error) {
	q := url.Values{}
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", offset))
	q.Set("prefix", prefix)
	url := fmt.Sprintf("%s/list?%s", s.baseURL.String(), q.Encode())
	resp, err := s.Client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)

	if err = dec.Decode(&out); err != nil {
		return nil, err
	}

	return
}

func (s *HTTPStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.getObject(s.indexPath(name))
}

func (s *HTTPStorage) indexPath(name string) string {
	if name == "" {
		return "indexes"
	}
	return path.Join("indexes", fmt.Sprintf("%s.yaml.gz", name))
}

func (s *HTTPStorage) chunkPath(hash string) string {
	return path.Join("chunks", hash)
}

func (s *HTTPStorage) WriteBackupIndex(name string, content []byte) (err error) {
	return s.putObject(s.indexPath(name), content)
}

func (s *HTTPStorage) putObject(location string, content []byte) (err error) {
	return fmt.Errorf("http storage doesn't implement backup, only restores")
}

func (s *HTTPStorage) getObject(location string) (out io.ReadCloser, err error) {
	url := fmt.Sprintf("%s/%s", s.baseURL.String(), location)
	resp, err := s.Client.Get(url)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("request to %s failed with status code %d", url, resp.StatusCode)
	}

	return NewGZipReadCloser(resp.Body)
}

func (s *HTTPStorage) WriteChunk(hash string, content []byte) (err error) {
	return s.putObject(s.chunkPath(hash), content)
}

func (s *HTTPStorage) OpenChunk(hash string) (out io.ReadCloser, err error) {
	return s.getObject(s.chunkPath(hash))
}

func (s *HTTPStorage) ChunkExists(hash string) (bool, error) {
	location := s.chunkPath(hash)

	url := fmt.Sprintf("%s/%s", s.baseURL.String(), location)
	resp, err := s.Client.Head(url)
	if err != nil {
		return false, err
	}

	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("request to %s failed with status code %d", url, resp.StatusCode)
	}

	return true, nil
}
