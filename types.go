package main

import (
	"time"

	fibmap "github.com/frostschutz/go-fibmap"
)

type Chunkmeta struct {
	Start   int64
	End     int64
	Content string
	IsEmpty bool
	URL     string
}

type Filemeta struct {
	Kind        string
	Metaversion string
	FileName    string
	Date        time.Time
	TotalSize   int64
	Chunks      []Chunkmeta
}

type ExtendedFile struct {
	SparseSupported bool
	Extents         []fibmap.Extent
}

type Backupmeta struct {
	Kind          string
	Metaversion   string
	Tag           string
	Date          time.Time
	MetadataFiles []string
}
