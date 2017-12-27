package gorocksdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactionFilter(t *testing.T) {
	var (
		changeKey    = []byte("change")
		changeValOld = []byte("old")
		changeValNew = []byte("new")
		deleteKey    = []byte("delete")
	)
	db := newTestDB(t, "TestCompactionFilter", func(opts *Options) {
		opts.SetCompactionFilter(&mockCompactionFilter{
			filter: func(level int, key, val []byte) (remove bool, newVal []byte) {
				if bytes.Equal(key, changeKey) {
					return false, changeValNew
				}
				if bytes.Equal(key, deleteKey) {
					return true, val
				}
				t.Errorf("key %q not expected during compaction", key)
				return false, nil
			},
		})
	})
	defer db.Close()

	// insert the test keys
	wo := NewDefaultWriteOptions()
	require.Nil(t, db.Put(wo, changeKey, changeValOld))
	require.Nil(t, db.Put(wo, deleteKey, changeValNew))

	// trigger a compaction
	db.CompactRange(Range{nil, nil})

	// ensure that the value is changed after compaction
	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, changeKey)
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.Equal(t, v1, changeValNew)

	// ensure that the key is deleted after compaction
	v2, err := db.Get(ro, deleteKey)
	require.NoError(t, err)
	require.True(t, v2 == nil)
}

type mockCompactionFilter struct {
	filter func(level int, key, val []byte) (remove bool, newVal []byte)
}

func (m *mockCompactionFilter) Name() string { return "gorocksdb.test" }
func (m *mockCompactionFilter) Filter(level int, key, val []byte) (bool, []byte) {
	return m.filter(level, key, val)
}
