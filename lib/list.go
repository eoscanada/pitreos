package pitreos

import (
	"fmt"
	"strings"
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

func (p *PITR) ListBackups(limit, offset int, prefix string, withMeta bool) (out []*ListableBackup, err error) {
	list, err := p.storage.ListBackups(limit, offset, prefix)
	if err != nil {
		return nil, err
	}

	for _, el := range list {
		newBackup := &ListableBackup{Name: el}
		if withMeta {
			bi, err := p.downloadBackupIndex(el)
			if err != nil {
				return nil, err
			}
			newBackup.Meta = bi.Meta
		}
		out = append(out, newBackup)
	}

	return
}
