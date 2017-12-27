package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIterator(t *testing.T) {
	db := newTestDBPrefixTransform(t, "TestIterator", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		require.Nil(t, db.Put(wo, k, []byte("val")))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()
	var actualKeys [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, 4)
		copy(key, iter.Key())
		actualKeys = append(actualKeys, key)
	}

	require.Nil(t, iter.Err())
	require.EqualValues(t, givenKeys, actualKeys)

	actualKeys = [][]byte{}
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		key := make([]byte, 4)
		copy(key, iter.Key())
		bactualKeys := [][]byte{key}
		actualKeys = append(bactualKeys, actualKeys...)
	}

	require.Nil(t, iter.Err())
	require.EqualValues(t, givenKeys, actualKeys)

	actualKeys = [][]byte{}
	iter.SeekToFirst()
	for valid, skey, _ := iter.ValidKeyValue(); valid; valid, skey, _ = iter.NextValidKeyValue() {
		xkey, _ := iter.KeyValue()
		require.EqualValues(t, skey, xkey)
		key := make([]byte, 4)
		copy(key, skey)
		actualKeys = append(actualKeys, key)
	}

	require.Nil(t, iter.Err())
	require.Equal(t, givenKeys, actualKeys)

	actualKeys = [][]byte{}
	iter.SeekToLast()
	for valid, skey, _ := iter.ValidKeyValue(); valid; valid, skey, _ = iter.PrevValidKeyValue() {
		xkey, _ := iter.KeyValue()
		require.EqualValues(t, skey, xkey)
		key := make([]byte, 4)
		copy(key, skey)
		bactualKeys := [][]byte{key}
		actualKeys = append(bactualKeys, actualKeys...)
	}

	require.Nil(t, iter.Err())
	require.Equal(t, givenKeys, actualKeys)

}

func TestIterators(t *testing.T) {
	db := newTestDB(t, "TestIterators", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	wo := NewDefaultWriteOptions()

	var err error
	var cf1, cf2, cf3 *ColumnFamilyHandle

	o := NewDefaultOptions()
	ro := NewDefaultReadOptions()
	cf1, err = db.CreateColumnFamily(o, "A")
	require.NoError(t, err)
	cf2, err = db.CreateColumnFamily(o, "B")
	require.NoError(t, err)
	cf3, err = db.CreateColumnFamily(o, "C")
	require.NoError(t, err)
	cfs := []*ColumnFamilyHandle{cf1, cf2, cf3}

	for _, cf := range cfs {
		for _, k := range givenKeys {
			require.Nil(t, db.PutCF(wo, cf, k, []byte("val")))
		}
	}

	var iters []*Iterator
	iters, err = db.NewIteratorsCF(ro, cfs)
	require.NoError(t, err)

	for _, iter := range iters {
		defer iter.Close()
		var actualKeys [][]byte
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := make([]byte, 4)
			copy(key, iter.Key())
			actualKeys = append(actualKeys, key)
		}
		require.Nil(t, iter.Err())
		require.EqualValues(t, givenKeys, actualKeys)
	}

}
