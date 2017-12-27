package gorocksdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fatalAsError is used as a wrapper to make it possible to use ensure
// also if C calls Go otherwise it will throw a internal lockOSThread error.
type fatalAsError struct {
	t *testing.T
}

func (f *fatalAsError) Fatal(a ...interface{}) {
	f.t.Error(a...)
}

func TestFilterPolicy(t *testing.T) {
	var (
		givenKeys          = [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
		givenFilter        = []byte("key")
		createFilterCalled = false
		keyMayMatchCalled  = false
	)
	policy := &mockFilterPolicy{
		createFilter: func(keys [][]byte) []byte {
			createFilterCalled = true
			require.Equal(t, keys, givenKeys)
			return givenFilter
		},
		keyMayMatch: func(key, filter []byte) bool {
			keyMayMatchCalled = true
			require.Equal(t, key, givenKeys[0])
			require.Equal(t, filter, givenFilter)
			return true
		},
	}

	fp := NewBloomFilter(111)
	_ = fp.CreateFilter([][]byte{})
	_ = fp.KeyMayMatch([]byte{}, []byte{})
	_ = fp.Name()

	cache := NewLRUCache(1024 * 1024)

	db := newTestDB(t, "TestFilterPolicy", func(opts *Options) {
		blockOpts := NewDefaultBlockBasedTableOptions()
		blockOpts.SetFilterPolicy(policy)
		blockOpts.SetBlockSize(1024 * 1024)
		blockOpts.SetWholeKeyFiltering(true)
		blockOpts.SetNoBlockCache(false)
		blockOpts.SetBlockRestartInterval(10)
		blockOpts.SetCacheIndexAndFilterBlocks(false)
		blockOpts.SetPinL0FilterAndIndexBlocksInCache(false)
		blockOpts.SetBlockSizeDeviation(10)
		blockOpts.SetBlockCacheCompressed(cache)
		opts.SetBlockBasedTableFactory(blockOpts)
		blockOpts.Destroy()
	})
	defer db.Close()

	// insert keys
	wo := NewDefaultWriteOptions()
	for _, k := range givenKeys {
		require.Nil(t, db.Put(wo, k, []byte("val")))
	}

	// flush to trigger the filter creation
	fo := NewDefaultFlushOptions()
	fo.SetWait(true)
	require.Nil(t, db.Flush(fo))
	require.True(t, createFilterCalled)
	fo.Destroy()

	// test key may match call
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKeys[0])
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.True(t, keyMayMatchCalled)
}

type mockFilterPolicy struct {
	createFilter func(keys [][]byte) []byte
	keyMayMatch  func(key, filter []byte) bool
}

func (m *mockFilterPolicy) Name() string { return "gorocksdb.test" }
func (m *mockFilterPolicy) CreateFilter(keys [][]byte) []byte {
	return m.createFilter(keys)
}
func (m *mockFilterPolicy) KeyMayMatch(key, filter []byte) bool {
	return m.keyMayMatch(key, filter)
}
