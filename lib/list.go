package pitreos

import (
	"fmt"
	"strings"
)

func (p *PITR) GetLatestBackup(tag string) (string, error) {
	list, err := p.storage.ListBackups(1, "")
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("no backup found")
	}

	for _, b := range list {
		if strings.HasSuffix(b, tag) {
			return b, nil

		}
	}

	return "", fmt.Errorf("no backup found")
}

func (p *PITR) ListBackups(limit, offset int, prefix string, withMeta bool) (out []*ListableBackup, err error) {
	list, err := p.storage.ListBackups(offset+limit, prefix)
	if err != nil {
		return nil, err
	}

	for _, el := range list[offset:] {
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
