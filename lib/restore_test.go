package pitreos

import (
	"testing"

	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/assert"
)

func TestUnmarshal(t *testing.T) {

	restoreYAML := `version: v2
date: 2018-08-28T12:55:39.12702167Z
tag: ""
meta: {}
files:
- filename: bigfile
  date: 2018-08-28T08:55:39.12702167-04:00
  size: 73400320
  chunks:
  - start: 52428800
    end: 73400319
    empty: true
  - start: 0
    end: 52428799
    contentSHA1: 21fd1fa9da7bda488ea1b3f62e1eae2224c0da73
- filename: data/test.index
  date: 2018-08-28T08:55:39.12702167-04:00
  size: 62914560
  chunks:
  - start: 52428800
    end: 62914559
    contentSHA1: 6c592575b37b6d81949b914412bbda1c798b017f
  - start: 0
    end: 52428799
    contentSHA1: acd9c9ac1c23aafa19151e354bb23e2e3a01c007
- filename: sparse.file
  date: 2018-08-28T08:55:39.12702167-04:00
  size: 5009
  chunks:
  - start: 0
    end: 5008
    contentSHA1: 73886020c8cdb0e7210f54a8d988387961171f90
`
	cnt := []byte(restoreYAML)
	var bi *BackupIndex
	if err := yaml.Unmarshal(cnt, &bi); err != nil {
		t.Errorf("Unmarshal failed with %s\n", err)
	}

	assert.Equal(t, bi.Files[0].Chunks[0].IsEmpty, true, "Chunk 0 should be empty")
	assert.Equal(t, bi.Files[0].Chunks[1].IsEmpty, false, "Chunk 1 should be non-empty")
	assert.Equal(t, bi.Files[0].Chunks[0].Start, int64(52428800), "Chunk 0 should start at 52428800")
	assert.Equal(t, bi.Files[0].Chunks[1].End, int64(52428799), "Chunk 1 should end at 52428799")
	assert.Equal(t, bi.Files[0].Chunks[1].ContentSHA1, "21fd1fa9da7bda488ea1b3f62e1eae2224c0da73", "Chunk 1 should sha1 incorrectly decoded")

}
