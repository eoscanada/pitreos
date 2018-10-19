package pitreos

import (
	"fmt"
)

func (p *PITR) ListBackupFiles(backupName string) error {
	bm, err := p.downloadBackupIndex(backupName)
	if err != nil {
		return err
	}

	if bm.Version != p.filemetaVersion {
		return fmt.Errorf("Incompatible version of backupIndex. Expected: %s, found: %s.", p.filemetaVersion, bm.Version)
	}

	for _, file := range bm.Files {
		fmt.Println(file.FileName)
	}

	return nil
}
