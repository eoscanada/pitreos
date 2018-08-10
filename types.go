package pitreos

import (
	"time"

	fibmap "github.com/frostschutz/go-fibmap"
)

type ChunkMeta struct {
	Start   int64
	End     int64
	Content string
	IsEmpty bool
	URL     string
}

type FileMeta struct {
	Kind        string
	MetaVersion string
	FileName    string
	Date        time.Time
	TotalSize   int64
	Chunks      []*ChunkMeta
}

type ExtendedFile struct {
	SparseSupported bool
	Extents         []fibmap.Extent
}

type BackupMeta struct {
	Kind          string
	MetaVersion   string
	Tag           string
	Date          time.Time
	MetadataFiles []string
}

type PitreosOptions struct {
	BucketName string `short:"n" long:"bucket-name" description:"GS bucket name where backups are stored" default:"eoscanada-playground-pitr"`

	BucketFolder string `short:"f" long:"bucket-folder" description:"Prefixed folder in GS bucket where backups are stored" default:"backups"`

	LocalFolder string `short:"l" long:"local-folder" description:"Folder relative to cwd where files will be backed up or restored" default:"."`

	BackupTag string `short:"t" long:"backup-tag" description:"Tag for the backup, depending on activated plugins like 'history'" default:"linux_ubuntu1604_gcc4_nohistory"`

	BeforeTimestamp int64 `short:"b" long:"before-timestamp" description:"closest timestamp (unix format) before which we want the latest restorable backup" default:"now"`
	Args            struct {
		Command string
	} `positional-args:"yes" required:"yes"`
}
