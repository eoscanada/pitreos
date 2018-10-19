package pitreos

import (
	"fmt"
	"os"
	"regexp"
	"text/tabwriter"

	"github.com/dustin/go-humanize"
)

func (p *PITR) ListBackupFiles(backupName string, filter string) error {
	bm, err := p.downloadBackupIndex(backupName)
	if err != nil {
		return err
	}

	if bm.Version != p.filemetaVersion {
		return fmt.Errorf("Incompatible version of backupIndex. Expected: %s, found: %s.", p.filemetaVersion, bm.Version)
	}

	filterRegex, err := regexp.Compile(filter)
	if err != nil {
		return err
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 5, 5, 1, '\t', tabwriter.AlignRight)

	for _, file := range bm.Files {
		if !filterRegex.MatchString(file.FileName) {
			continue
		}

		fmt.Fprintf(w, "%s\t%s\n", humanize.Bytes(uint64(file.TotalSize)), file.FileName)
		w.Flush()
	}

	return nil
}
