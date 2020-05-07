package pitreos

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPITR_ListBackups(t *testing.T) {

	path := "/tmp/test"
	_ = os.RemoveAll(path)

	ctx := context.Background()
	storage, err := NewDStoreStorage(ctx, fmt.Sprintf("file://%s", path))
	require.NoError(t, err)

	for i := 1; i < 5; i++ {
		err = storage.WriteBackupIndex(fmt.Sprintf("b-%d", i), nil)
		require.NoError(t, err)
	}

	pitr := NewDefaultPITR(storage)

	bk, err := pitr.ListBackups(2, 0, "b", false)
	require.NoError(t, err)

	expected := []*ListableBackup{
		{Name: "b-1"},
		{Name: "b-2"},
	}
	require.Equal(t, expected, bk)

}
