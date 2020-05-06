package pitreos

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/dfuse-io/dstore"
	"golang.org/x/net/context"
)

type Storage interface {
	ListBackups(limit int, prefix string) ([]string, error)
	OpenBackupIndex(name string) (io.ReadCloser, error)
	WriteBackupIndex(name string, content []byte) error

	OpenChunk(hash string) (io.ReadCloser, error)
	WriteChunk(hash string, content []byte) error
	ChunkExists(hash string) (bool, error)
	SetTimeout(timeout time.Duration)
}

type DStoreStorage struct {
	store   dstore.Store
	ctx     context.Context
	timeout time.Duration
}

func NewDStoreStorage(ctx context.Context, baseURL string) (*DStoreStorage, error) {
	store, err := dstore.NewSimpleStore(baseURL)
	if err != nil {
		return nil, err
	}

	return &DStoreStorage{
		timeout: time.Minute * 30,
		store:   store,
		ctx:     ctx,
	}, nil
}

func (s *DStoreStorage) SetTimeout(timeout time.Duration) {
	s.timeout = timeout
}

func (s *DStoreStorage) ListBackups(limit int, prefix string) (out []string, err error) {
	ctx, _ := context.WithTimeout(s.ctx, s.timeout)
	withoutExtension := strings.TrimSuffix(s.indexPath(prefix), ".yaml.gz")

	backups, err := s.store.ListFiles(ctx, withoutExtension, "", limit)
	for _, b := range backups {
		out = append(out, strings.Trim(b, ".yaml.gz"))
	}
	return
}

func (s *DStoreStorage) OpenBackupIndex(name string) (out io.ReadCloser, err error) {
	return s.store.OpenObject(s.ctx, s.indexPath(name))
}

func (s *DStoreStorage) indexPath(name string) string {
	if name == "" {
		return "indexes"
	}
	return path.Join("indexes", fmt.Sprintf("%s.yaml.gz", name))
}

func (s *DStoreStorage) chunkPath(hash string) string {
	return path.Join("chunks", hash)
}

func (s *DStoreStorage) WriteBackupIndex(name string, content []byte) (err error) {
	br := bytes.NewBuffer(content)
	return s.store.WriteObject(s.ctx, s.indexPath(name), br)
}

func (s *DStoreStorage) WriteChunk(hash string, content []byte) (err error) {
	br := bytes.NewBuffer(content)
	return s.store.WriteObject(s.ctx, s.chunkPath(hash), br)
}

func (s *DStoreStorage) OpenChunk(hash string) (out io.ReadCloser, err error) {

	return s.store.OpenObject(s.ctx, s.chunkPath(hash))
}

func (s *DStoreStorage) ChunkExists(hash string) (bool, error) {
	return s.store.FileExists(s.ctx, s.chunkPath(hash))
}
