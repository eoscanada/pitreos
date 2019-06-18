package pitreos

import (
	"fmt"
	"time"
)

type BackupIndex struct {
	Version   string                 `json:"version"`
	Date      time.Time              `json:"date"`
	Tag       string                 `json:"tag"`
	Meta      map[string]interface{} `json:"meta"`
	Files     []*FileIndex           `json:"files"`
	ChunkSize int64                  `json:"chunk_size"`
}

type FileIndex struct {
	FileName  string      `json:"filename"`
	Date      time.Time   `json:"date"`
	TotalSize int64       `json:"size"`
	Chunks    []*ChunkDef `json:"chunks"`
}

type ChunkDef struct {
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	IsEmpty    bool   `json:"empty,omitempty"`
	ContentSHA string `json:"contentSHA,omitempty"`
}

type ListableBackup struct {
	Name string
	Meta map[string]interface{}
}

func (backup *BackupIndex) ComputeFileEstimatedDiskSize(filename string) (uint64, error) {
	fileIndex, err := backup.findFileIndex(filename)
	if err != nil {
		return 0, err
	}

	estimatedDiskSize := uint64(0)
	for _, chunk := range fileIndex.Chunks {
		if chunk.IsEmpty {
			continue
		}

		estimatedDiskSize += uint64(chunk.End - chunk.Start)
	}

	return estimatedDiskSize, nil
}

func (backupIndex *BackupIndex) FindFilesMatching(filter Filter) ([]*FileIndex, error) {
	var matchingFiles []*FileIndex

	for _, file := range backupIndex.Files {
		if filter.Match(file.FileName) {
			matchingFiles = append(matchingFiles, file)
		}
	}

	return matchingFiles, nil
}

func (backup *BackupIndex) findFileIndex(filename string) (*FileIndex, error) {
	for _, file := range backup.Files {
		if file.FileName == filename {
			return file, nil
		}
	}

	return nil, fmt.Errorf("file %q not found in backup index", filename)
}
