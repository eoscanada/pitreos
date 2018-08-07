package main

import "time"

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
