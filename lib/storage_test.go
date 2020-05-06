package pitreos

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDStoreStorage_ListBackups(t *testing.T) {

	path := "/tmp/test"
	_ = os.RemoveAll(path)

	ctx := context.Background()
	storage, err := NewDStoreStorage(ctx, fmt.Sprintf("file://%s", path))
	require.NoError(t, err)

	err = storage.WriteBackupIndex("b1", []byte{1, 2, 3})
	require.NoError(t, err)
	err = storage.WriteBackupIndex("b2", []byte{1, 2, 3})
	require.NoError(t, err)
	err = storage.WriteBackupIndex("b3", []byte{1, 2, 3})
	require.NoError(t, err)

	out, err := storage.ListBackups(2, "")
	require.NoError(t, err)
	require.Equal(t, []string{"b1", "b2"}, out)

	out, err = storage.ListBackups(3, "")
	require.NoError(t, err)
	require.Equal(t, []string{"b1", "b2", "b3"}, out)

}

func TestDStoreStorage_WriteBackupIndex_OpenBackupIndex(t *testing.T) {
	path := "/tmp/test"
	_ = os.RemoveAll(path)

	ctx := context.Background()
	storage, err := NewDStoreStorage(ctx, fmt.Sprintf("file://%s", path))
	require.NoError(t, err)

	storage.WriteBackupIndex("b1", []byte{1, 2, 3})

	rc, err := storage.OpenBackupIndex("b1")
	require.NoError(t, err)

	b := make([]byte, 8)
	l, err := rc.Read(b)
	require.NoError(t, err)
	require.Equal(t, 3, l)
	require.Equal(t, []byte{1, 2, 3, 0, 0, 0, 0, 0}, b)

}

func TestNewDStoreStorage_WriteChunk_ChunkExists_OpenChunk(t *testing.T) {
	path := "/tmp/test"
	_ = os.RemoveAll(path)

	ctx := context.Background()
	storage, err := NewDStoreStorage(ctx, fmt.Sprintf("file://%s", path))
	require.NoError(t, err)

	err = storage.WriteChunk("hash.1", []byte{1, 2, 3})
	require.NoError(t, err)

	exist, err := storage.ChunkExists("hash.1")
	require.NoError(t, err)
	require.True(t, exist)

	rc, err := storage.OpenChunk("hash.1")
	b := make([]byte, 8)
	l, err := rc.Read(b)
	require.NoError(t, err)
	require.Equal(t, 3, l)
	require.Equal(t, []byte{1, 2, 3, 0, 0, 0, 0, 0}, b)
}
