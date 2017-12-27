package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestBackup(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestBackup")
	require.NoError(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	wo := NewDefaultWriteOptions()
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)
	err = db.Put(wo, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	var be *BackupEngine
	be, err = OpenBackupEngine(opts, dir)
	require.NoError(t, err)
	err = be.CreateNewBackup(db)
	require.NoError(t, err)
	defer be.Close()
	bei := be.GetInfo()
	_ = bei.GetBackupId(0)
	_ = bei.GetCount()
	_ = bei.GetNumFiles(0)
	_ = bei.GetSize(0)
	_ = bei.GetTimestamp(0)

	_ = be.UnsafeGetBackupEngine()

	rso := NewRestoreOptions()
	rso.SetKeepLogFiles(1)

	err = be.RestoreDBFromLatestBackup(dir, dir, rso)
	require.NoError(t, err)

	defer rso.Destroy()
}
