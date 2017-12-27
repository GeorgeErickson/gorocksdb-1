package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestColumnFamilyOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyOpen")
	require.NoError(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, len(cfh), 2)
	cfh[0].Destroy()
	cfh[1].Destroy()

	actualNames, err := ListColumnFamilies(opts, dir)
	require.NoError(t, err)
	require.EqualValues(t, actualNames, givenNames)
}

func TestColumnFamilyCreateDrop(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyCreate")
	require.NoError(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)
	defer db.Close()
	cf, err := db.CreateColumnFamily(opts, "guide")
	require.NoError(t, err)
	defer cf.Destroy()
	_ = cf.UnsafeGetCFHandler()

	actualNames, err := ListColumnFamilies(opts, dir)
	require.NoError(t, err)
	require.EqualValues(t, actualNames, []string{"default", "guide"})

	require.Nil(t, db.DropColumnFamily(cf))

	actualNames, err = ListColumnFamilies(opts, dir)
	require.NoError(t, err)
	require.EqualValues(t, actualNames, []string{"default"})
}

func TestColumnFamilyBatchPutGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyPutGet")
	require.NoError(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, len(cfh), 2)
	defer cfh[0].Destroy()
	defer cfh[1].Destroy()

	wo := NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := NewDefaultReadOptions()
	defer ro.Destroy()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	b0 := NewWriteBatch()
	defer b0.Destroy()
	b0.PutCF(cfh[0], givenKey0, givenVal0)
	require.Nil(t, db.Write(wo, b0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer CfreeByteSlice(actualVal0)
	require.NoError(t, err)
	require.Equal(t, actualVal0, givenVal0)

	b1 := NewWriteBatch()
	defer b1.Destroy()
	b1.PutCF(cfh[1], givenKey1, givenVal1)
	require.Nil(t, db.Write(wo, b1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer CfreeByteSlice(actualVal1)
	require.NoError(t, err)
	require.Equal(t, actualVal1, givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	require.NoError(t, err)
	require.Equal(t, len(actualVal), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	require.NoError(t, err)
	require.Equal(t, len(actualVal), 0)
}

func TestColumnFamilyPutGetDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "gorocksdb-TestColumnFamilyPutGet")
	require.NoError(t, err)

	givenNames := []string{"default", "guide"}
	opts := NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetCreateIfMissing(true)
	db, cfh, err := OpenDbColumnFamilies(opts, dir, givenNames, []*Options{opts, opts})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, len(cfh), 2)
	defer cfh[0].Destroy()
	defer cfh[1].Destroy()

	wo := NewDefaultWriteOptions()
	defer wo.Destroy()
	ro := NewDefaultReadOptions()
	defer ro.Destroy()

	givenKey0 := []byte("hello0")
	givenVal0 := []byte("world0")
	givenKey1 := []byte("hello1")
	givenVal1 := []byte("world1")

	require.Nil(t, db.PutCF(wo, cfh[0], givenKey0, givenVal0))
	actualVal0, err := db.GetCF(ro, cfh[0], givenKey0)
	defer CfreeByteSlice(actualVal0)
	require.NoError(t, err)
	require.Equal(t, actualVal0, givenVal0)

	require.Nil(t, db.PutCF(wo, cfh[1], givenKey1, givenVal1))
	actualVal1, err := db.GetCF(ro, cfh[1], givenKey1)
	defer CfreeByteSlice(actualVal1)
	require.NoError(t, err)
	require.Equal(t, actualVal1, givenVal1)

	actualVal, err := db.GetCF(ro, cfh[0], givenKey1)
	require.NoError(t, err)
	require.Equal(t, len(actualVal), 0)
	actualVal, err = db.GetCF(ro, cfh[1], givenKey0)
	require.NoError(t, err)
	require.Equal(t, len(actualVal), 0)

	require.Nil(t, db.DeleteCF(wo, cfh[0], givenKey0))
	actualVal, err = db.GetCF(ro, cfh[0], givenKey0)
	require.NoError(t, err)
	require.Equal(t, len(actualVal), 0)
}
