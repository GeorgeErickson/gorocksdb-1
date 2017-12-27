package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestWriteBatchVs(t *testing.T) {
	db, cfs := newTestDBCFs(t, "TestWriteBatch", []string{"default"}, nil)
	defer db.Close()
	cf := cfs[0]

	var keys, values [][]byte
	for i := 0; i < 3; i++ {
		keys = append(keys, []byte("key"+strconv.FormatInt(int64(i), 10)))
		values = append(values, []byte("value"+strconv.FormatInt(int64(i), 10)))
	}

	wo := NewDefaultWriteOptions()

	// create and fill the write batch
	wb := NewWriteBatch()
	wb.PutVCFs(
		[]*ColumnFamilyHandle{cf, cf, cf},
		keys,
		values,
	)

	err := db.Write(wo, wb)
	require.NoError(t, err)
	wb.Destroy()

	ro := NewDefaultReadOptions()
	for i, key := range keys {
		v1, err := db.Get(ro, key)
		defer CfreeByteSlice(v1)
		require.NoError(t, err)
		require.Equal(t, values[i], v1)
	}

	wb = NewWriteBatch()
	wb.DeleteVCFs(
		[]*ColumnFamilyHandle{cf, cf, cf},
		[][]byte{keys[0], keys[2]},
	)

	err = db.Write(wo, wb)
	require.NoError(t, err)
	wb.Destroy()

	// check changes
	v1, err := db.Get(ro, keys[1])
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.Equal(t, v1, values[1])

	v2, err := db.Get(ro, keys[0])
	require.Nil(t, err)
	require.True(t, v2 == nil)
	CfreeByteSlice(v2)

	v2, err = db.Get(ro, keys[2])
	defer CfreeByteSlice(v2)
	require.Nil(t, err)
	require.True(t, v2 == nil)
}

func TestWriteBatchV(t *testing.T) {
	db, cfs := newTestDBCFs(t, "TestWriteBatch", []string{"default"}, nil)
	defer db.Close()
	cf := cfs[0]

	var keys, values [][]byte
	for i := 0; i < 3; i++ {
		keys = append(keys, []byte("key"+strconv.FormatInt(int64(i), 10)))
		values = append(values, []byte("value"+strconv.FormatInt(int64(i), 10)))
	}

	wo := NewDefaultWriteOptions()

	// create and fill the write batch
	wb := NewWriteBatch()
	wb.PutVCF(
		cf,
		keys,
		values,
	)

	err := db.Write(wo, wb)
	require.NoError(t, err)
	wb.Destroy()

	ro := NewDefaultReadOptions()
	for i, key := range keys {
		v1, err := db.Get(ro, key)
		defer CfreeByteSlice(v1)
		require.NoError(t, err)
		require.Equal(t, values[i], v1)
	}

	wb = NewWriteBatch()
	wb.DeleteVCF(
		cf,
		[][]byte{keys[0], keys[2]},
	)

	err = db.Write(wo, wb)
	require.NoError(t, err)
	wb.Destroy()

	// check changes
	v1, err := db.Get(ro, keys[1])
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.Equal(t, v1, values[1])

	v2, err := db.Get(ro, keys[0])
	require.Nil(t, err)
	require.True(t, v2 == nil)
	CfreeByteSlice(v2)

	v2, err = db.Get(ro, keys[2])
	defer CfreeByteSlice(v2)
	require.Nil(t, err)
	require.True(t, v2 == nil)
}
