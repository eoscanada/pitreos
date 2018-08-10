package pitreos

import (
	"log"
	"os"
	"path/filepath"
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

func wipeChunk(f *os.File, offset int64, length int64) error {
	ff := fibmap.NewFibmapFile(f)
	return ff.PunchHole(offset, length)
}

func rangeIsInSparseFileExtent(start int64, end int64, extents []fibmap.Extent) bool {
	for _, ex := range extents {
		if uint64(start) <= ex.Logical+ex.Length && ex.Logical <= uint64(end) {
			return true
		}
	}
	return false
}

func writeChunkToFile(f *os.File, offset int64, s []byte) error {
	mutex.Lock()
	f.Seek(offset, 0)
	_, err := f.Write(s)
	mutex.Unlock()
	return err
}

func getChunkContentUnlessEmpty(file *os.File, offset int64, size int64) (data []byte, empty bool) {
	_, ok := knownFiles[file]
	if !ok {
		exts, err := getSparseFileExtents(file)
		sparseSupported := true
		if err != nil {
			log.Printf("cannot optimize based on sparse file readout")
			sparseSupported = false
		}
		knownFiles[file] = ExtendedFile{SparseSupported: sparseSupported, Extents: exts}
	}

	if knownFiles[file].SparseSupported && !rangeIsInSparseFileExtent(offset, offset+size-1, knownFiles[file].Extents) {
		empty = true
	} else {
		data = make([]byte, size)
		mutex.Lock()
		file.Seek(offset, 0)
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

func getDirFiles(directory string) (fileNames []string, err error) {
	err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return err
		}
		fileNames = append(fileNames, path)
		return nil
	})
	return
}
