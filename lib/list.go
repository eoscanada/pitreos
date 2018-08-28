package pitreos

import (
	"encoding/json"
	"fmt"
	"strings"

	humanize "github.com/dustin/go-humanize"
)

func (p *PITR) GetLatestBackup(tag string) (string, error) {
	limit := 20
	for offset := 0; ; offset += limit {
		list, err := p.storage.ListBackups(limit, offset, "")
		if err != nil {
			return "", err
		}
		if len(list) == 0 {
			break
		}
		for _, b := range list {
			if strings.HasSuffix(b, tag) {
				return b, nil

			}
		}
	}
	return "", fmt.Errorf("No backup found")
}

func (p *PITR) ListBackups(limit int, detailed bool) error {
	list, err := p.storage.ListBackups(limit, 0, "")
	if err != nil {
		return err
	}
	for _, b := range list {
		if detailed {
			bi, err := p.downloadBackupIndex(b)
			if err != nil {
				return err
			}
			biMetaBytes, err := json.Marshal(bi.Meta)
			if err != nil {
				return err
			}
			fmt.Printf("%s\t%s\t%+v\n", b, humanize.Time(bi.Date), string(biMetaBytes))
		} else {
			fmt.Println(b)
		}
	}
	return nil
}
