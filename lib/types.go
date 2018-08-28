package pitreos

import (
	"time"
)

type BackupIndex struct {
	Version string                 `json:"version"`
	Date    time.Time              `json:"date"`
	Tag     string                 `json:"tag"`
	Meta    map[string]interface{} `json:"meta"`
	Files   []*FileIndex           `json:"files"`
}

type FileIndex struct {
	FileName  string      `json:"filename"`
	Date      time.Time   `json:"date"`
	TotalSize int64       `json:"size"`
	Chunks    []*ChunkDef `json:"chunks"`
}

type ChunkDef struct {
	Start       int64  `json:"start"`
	End         int64  `json:"end"`
	IsEmpty     bool   `json:"empty,omitempty"`
	ContentSHA1 string `json:"contentSHA1,omitempty"`
}

type ListableBackup struct {
	Name string
	Meta map[string]interface{}
	Date time.Time
}
