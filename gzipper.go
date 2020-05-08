package pitreos

import (
	"compress/gzip"
	"io"
)

type GZipReadCloser struct {
	src io.ReadCloser
	*gzip.Reader
}

func NewGZipReadCloser(src io.ReadCloser) (*GZipReadCloser, error) {
	reader, err := gzip.NewReader(src)
	if err != nil {
		src.Close()
		return nil, err
	}
	return &GZipReadCloser{
		src:    src,
		Reader: reader,
	}, nil
}

func (g *GZipReadCloser) Close() error {
	err1 := g.Reader.Close()
	err2 := g.src.Close()

	if err1 == nil && err2 == nil {
		return nil
	}
	if err2 != nil {
		return err2
	}
	return err1
}
