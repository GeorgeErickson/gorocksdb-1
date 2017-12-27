package gorocksdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceTransform(t *testing.T) {
	db := newTestDB(t, "TestSliceTransform", func(opts *Options) {
		opts.SetPrefixExtractor(&testSliceTransform{})
	})
	defer db.Close()

	wo := NewDefaultWriteOptions()
	require.Nil(t, db.Put(wo, []byte("foo1"), []byte("foo")))
	require.Nil(t, db.Put(wo, []byte("foo2"), []byte("foo")))
	require.Nil(t, db.Put(wo, []byte("bar1"), []byte("bar")))

	iter := db.NewIterator(NewDefaultReadOptions())
	defer iter.Close()
	prefix := []byte("foo")
	numFound := 0
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		numFound++
	}
	require.Nil(t, iter.Err())
	require.Equal(t, numFound, 2)

}

func TestFixedPrefixTransformOpen(t *testing.T) {
	db := newTestDB(t, "TestFixedPrefixTransformOpen", func(opts *Options) {
		nst := NewFixedPrefixTransform(3)
		_ = nst.Transform(nil)
		_ = nst.InDomain(nil)
		_ = nst.InRange(nil)
		_ = nst.Name()
		opts.SetPrefixExtractor(nst)
	})
	defer db.Close()
}

type testSliceTransform struct {
	initiated bool
}

func (st *testSliceTransform) Name() string                { return "gorocksdb.test" }
func (st *testSliceTransform) Transform(src []byte) []byte { return src[0:3] }
func (st *testSliceTransform) InDomain(src []byte) bool    { return len(src) >= 3 }
func (st *testSliceTransform) InRange(src []byte) bool     { return len(src) == 3 }
