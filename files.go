package pitreos

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
)

func (p *PITR) ListBackupFiles(backupName string, filter Filter) error {
	bm, err := p.downloadBackupIndex(backupName)
	if err != nil {
		return err
	}

	if bm.Version != p.filemetaVersion {
		return fmt.Errorf("Incompatible version of backupIndex. Expected: %s, found: %s.", p.filemetaVersion, bm.Version)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 23, 0, 3, ' ', 0)

	fmt.Fprintln(w, "size\testimated disk size\tname")

	matchingFiles, err := bm.FindFilesMatching(filter)
	if err != nil {
		return err
	}

	for _, file := range matchingFiles {
		size := uint64(file.TotalSize)
		estimatedDiskSize, err := bm.ComputeFileEstimatedDiskSize(file.FileName)
		if err != nil {
			return err
		}

		fmt.Fprintf(w, "%s\t%s\t%s\n", humanize.Bytes(size), humanize.Bytes(estimatedDiskSize), file.FileName)
		w.Flush()
	}

	return nil
}
