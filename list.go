package pitreos

import (
	"fmt"
	"math"
	"strings"

	"go.uber.org/zap"
)

func (p *PITR) GetLatestBackup(tag string) (string, error) {
	list, err := p.storage.ListBackups(math.MaxInt32, "")
	if err != nil {
		return "", err
	}

	for i := len(list) - 1; i >= 0; i-- {
		candidate := list[i]
		zlog.Debug("Found candidate backup", zap.String("name", candidate))
		if strings.HasSuffix(candidate, tag) {
			zlog.Debug("Found matching backup", zap.String("name", candidate))
			return candidate, nil
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
