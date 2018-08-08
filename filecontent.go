package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	fibmap "github.com/frostschutz/go-fibmap"
)

var (
	mutex      = &sync.Mutex{}
	knownFiles = map[*os.File]ExtendedFile{}
)

func getSparseFileExtents(f *os.File) ([]fibmap.Extent, error) {
	ff := fibmap.NewFibmapFile(f)

	x, errno := (ff.Fiemap(999999))
	if errno != 0 {
		return nil, errno
	}
	return x, nil
}

func WipeChunk(f *os.File, offset uint64, length uint64) error {
	ff := fibmap.NewFibmapFile(f)
	return ff.PunchHole(int64(offset), int64(length))
}

func rangeIsInSparseFileExtent(start uint64, end uint64, extents []fibmap.Extent) bool {
	for _, ex := range extents {
		if start <= ex.Logical+ex.Length && ex.Logical <= end {
			return true
		}
	}
	return false
}

func getChunkContentUnlessEmpty(file *os.File, offset uint64, size uint64) (data []byte, empty bool) {
	_, ok := knownFiles[file]
	if !ok {
		exts, err := getSparseFileExtents(file)
		sparseSupported := true
		if err != nil {
			fmt.Println("cannot optimize based on sparse file readout")
			sparseSupported = false
		}
		knownFiles[file] = ExtendedFile{SparseSupported: sparseSupported, Extents: exts}
	}

	if knownFiles[file].SparseSupported && !rangeIsInSparseFileExtent(offset, offset+size-1, knownFiles[file].Extents) {
		empty = true
	} else {
		data = make([]byte, size)
		mutex.Lock()
		file.Seek(int64(offset), 0)
		_, err := file.Read(data)
		mutex.Unlock()
		if err != nil {
			log.Fatalf("error wtf: %s\n", err)
		}
		if isEmptyChunk(data) {
			empty = true
		}
	}
	return data, empty
}

func isEmptyChunk(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}
