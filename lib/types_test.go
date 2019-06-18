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

func TestFindFilesMatching_EmptyStringMatchAll(t *testing.T) {
	backupIndex := createBackupIndex(t)
	files, err := backupIndex.FindFilesMatching(AllFileFilter)

	assert.NoError(t, err)
	assertMatchAllFiles(t, files)
}

func TestFindFilesMatching_SingleMatch(t *testing.T) {
	backupIndex := createBackupIndex(t)
	files, err := backupIndex.FindFilesMatching(MustNewIncludeThanExcludeFilter("big", ""))

	assert.NoError(t, err)
	assert.Len(t, files, 1, "There should be 1 matching files only")
	assert.Equal(t, "bigfile", files[0].FileName, "Matching file should be 'bigfile'")
}

func TestFindFilesMatching_IncludeWithRegex_Works(t *testing.T) {
	backupIndex := createBackupIndex(t)
	files, err := backupIndex.FindFilesMatching(MustNewIncludeThanExcludeFilter("big|small", ""))

	assert.NoError(t, err)
	assertMatchAllFiles(t, files)
}

func TestFindFilesMatching_FilterWithExclude_Works(t *testing.T) {
	backupIndex := createBackupIndex(t)
	files, err := backupIndex.FindFilesMatching(MustNewIncludeThanExcludeFilter("file", "small"))

	assert.NoError(t, err)
	assert.Len(t, files, 1, "There should be 1 matching files only")
	assert.Equal(t, "bigfile", files[0].FileName, "Matching file should be 'bigfile'")
}

func TestFindFilesMatching_RegexProblem(t *testing.T) {
	_, err := NewIncludeThanExcludeFilter("(", "")

	assert.EqualError(t, err, "error parsing regexp: missing closing ): `(`")
}

func assertMatchAllFiles(t *testing.T, actualFiles []*FileIndex) {
	assert.Len(t, actualFiles, 2, "There should be 2 matching files only")
	assert.Equal(t, "bigfile", actualFiles[0].FileName, "First matching file should be 'bigfile'")
	assert.Equal(t, "smallfile", actualFiles[1].FileName, "Second matching file should be 'smallfile'")
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
- filename: smallfile
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
