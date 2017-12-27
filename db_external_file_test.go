package gorocksdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExternalFile(t *testing.T) {
	db := newTestDB(t, "TestDBExternalFile", nil)
	defer db.Close()

	envOpts := NewDefaultEnvOptions()
	opts := NewDefaultOptions()
	w := NewSSTFileWriter(envOpts, opts)
	defer w.Destroy()

	filePath, err := ioutil.TempFile("", "sst-file-test")
	require.NoError(t, err)
	defer os.Remove(filePath.Name())

	err = w.Open(filePath.Name())
	require.NoError(t, err)

	err = w.Add([]byte("aaa"), []byte("aaaValue"))
	require.NoError(t, err)
	err = w.Add([]byte("bbb"), []byte("bbbValue"))
	require.NoError(t, err)
	err = w.Add([]byte("ccc"), []byte("cccValue"))
	require.NoError(t, err)
	err = w.Add([]byte("ddd"), []byte("dddValue"))
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	ingestOpts := NewDefaultIngestExternalFileOptions()
	ingestOpts.SetAllowBlockingFlush(true)
	ingestOpts.SetAllowGlobalSeqNo(true)
	ingestOpts.SetSnapshotConsistency(true)
	ingestOpts.SetMoveFiles(true)
	defer ingestOpts.Destroy()

	err = db.IngestExternalFile([]string{filePath.Name()}, ingestOpts)
	require.NoError(t, err)

	readOpts := NewDefaultReadOptions()

	v1, err := db.Get(readOpts, []byte("aaa"))
	require.NoError(t, err)
	require.Equal(t, v1, []byte("aaaValue"))
	v2, err := db.Get(readOpts, []byte("bbb"))
	require.NoError(t, err)
	require.Equal(t, v2, []byte("bbbValue"))
	v3, err := db.Get(readOpts, []byte("ccc"))
	require.NoError(t, err)
	require.Equal(t, v3, []byte("cccValue"))
	v4, err := db.Get(readOpts, []byte("ddd"))
	require.NoError(t, err)
	require.Equal(t, v4, []byte("dddValue"))
}

func TestExternalFileCFs(t *testing.T) {
	db, cfs := newTestDBCFs(t, "TestDBExternalFileCF", []string{"default", "cf1"}, nil)
	defer db.Close()

	envOpts := NewDefaultEnvOptions()
	opts := NewDefaultOptions()
	w := NewSSTFileWriter(envOpts, opts)
	defer w.Destroy()

	filePath, err := ioutil.TempFile("", "sst-file-test")
	require.NoError(t, err)
	defer os.Remove(filePath.Name())

	err = w.Open(filePath.Name())
	require.NoError(t, err)

	err = w.Add([]byte("aaa"), []byte("aaaValue"))
	require.NoError(t, err)
	err = w.Add([]byte("bbb"), []byte("bbbValue"))
	require.NoError(t, err)
	err = w.Add([]byte("ccc"), []byte("cccValue"))
	require.NoError(t, err)
	err = w.Add([]byte("ddd"), []byte("dddValue"))
	require.NoError(t, err)

	err = w.Finish()
	require.NoError(t, err)

	ingestOpts := NewDefaultIngestExternalFileOptions()
	ingestOpts.SetAllowBlockingFlush(true)
	ingestOpts.SetAllowGlobalSeqNo(true)
	ingestOpts.SetSnapshotConsistency(true)
	ingestOpts.SetMoveFiles(true)
	defer ingestOpts.Destroy()

	err = db.IngestExternalFileCF(cfs[0], []string{filePath.Name()}, ingestOpts)
	require.NoError(t, err)

	readOpts := NewDefaultReadOptions()

	v1, err := db.Get(readOpts, []byte("aaa"))
	require.NoError(t, err)
	require.Equal(t, v1, []byte("aaaValue"))
	v2, err := db.Get(readOpts, []byte("bbb"))
	require.NoError(t, err)
	require.Equal(t, v2, []byte("bbbValue"))
	v3, err := db.Get(readOpts, []byte("ccc"))
	require.NoError(t, err)
	require.Equal(t, v3, []byte("cccValue"))
	v4, err := db.Get(readOpts, []byte("ddd"))
	require.NoError(t, err)
	require.Equal(t, v4, []byte("dddValue"))
}
