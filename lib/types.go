package pitreos

import (
	"time"
)

type BackupIndex struct {
	Version string                 `yaml:"version"`
	Date    time.Time              `yaml:"date"`
	Tag     string                 `yaml:"tag"`
	Meta    map[string]interface{} `yaml:"meta"`
	Files   []*FileIndex           `yaml:"files"`
}

type FileIndex struct {
	FileName  string      `yaml:"filename"`
	Date      time.Time   `yaml:"date"`
	TotalSize int64       `yaml:"size"`
	Chunks    []*ChunkDef `yaml:"chunks"`
}

type ChunkDef struct {
	Start       int64  `yaml:"start"`
	End         int64  `yaml:"end"`
	IsEmpty     bool   `yaml:"empty,omitempty"`
	ContentSHA1 string `yaml:"contentSHA1,omitempty"` // content ?
}
