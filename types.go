package main

import (
	"time"

	fibmap "github.com/frostschutz/go-fibmap"
)

type Chunkmeta struct {
	Start   uint64
	End     uint64
	Content string
	IsEmpty bool
}

type Filemeta struct {
	FileName      string
	BlobsLocation string
	Date          time.Time
	TotalSize     int64
	Chunks        []Chunkmeta
}

type ExtendedFile struct {
	SparseSupported bool
	Extents         []fibmap.Extent
}
