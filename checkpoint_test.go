package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestCheckpoint(t *testing.T) {

	suffix := "checkpoint"
	dir, err := ioutil.TempDir("", "gorocksdb-"+suffix)
	require.NoError(t, err)
	err = os.RemoveAll(dir)
	require.NoError(t, err)

	db := newTestDB(t, "TestCheckpoint", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	givenVal := []byte("val")
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		require.Nil(t, db.Put(wo, k, givenVal))
	}

	var dbCheck *DB
	var checkpoint *Checkpoint

	checkpoint, err = db.NewCheckpoint()
	defer checkpoint.Destroy()
	require.NotNil(t, checkpoint)
	require.NoError(t, err)

	err = checkpoint.CreateCheckpoint(dir, 0)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	dbCheck, err = OpenDb(opts, dir)
	defer dbCheck.Close()
	require.NoError(t, err)

	// test keys
	var value []byte
	ro := NewDefaultReadOptions()
	for _, k := range givenKeys {
		value, err = dbCheck.Get(ro, k)
		defer CfreeByteSlice(value)
		require.NoError(t, err)
		require.Equal(t, value, givenVal)
	}

}
