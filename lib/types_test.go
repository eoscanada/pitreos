package pitreos

import (
	"testing"

	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
)

func TestComputeFileEstimatedDiskSize(t *testing.T) {
	backupIndex := createBackupIndex(t)
	size, err := backupIndex.ComputeFileEstimatedDiskSize("bigfile")

	assert.NoError(t, err)
	assert.Equal(t, uint64(100), size, "Size should be 100")
}

func TestComputeFileEstimatedDiskSize_FileNotPresent(t *testing.T) {
	backupIndex := createBackupIndex(t)
	_, err := backupIndex.ComputeFileEstimatedDiskSize("no_such_file")

	assert.EqualError(t, err, `file "no_such_file" not found in backup index`)
}

func createBackupIndex(t *testing.T) *BackupIndex {
	cnt := []byte(backupIndexContent)
	var bi *BackupIndex
	if err := yaml.Unmarshal(cnt, &bi); err != nil {
		t.Errorf("Unmarshal failed with %s\n", err)
	}

	return bi
}

var backupIndexContent = `version: v2
chunk_size: 10000
files:
- filename: bigfile
  size: 73400320
  chunks:
  - start: 0
    end: 50
    empty: true
  - start: 0
    end: 50
    empty: false
  - start: 51
    end: 101
    empty: false
`
