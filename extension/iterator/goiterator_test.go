package iterator

import (
	"github.com/kapitan-k/goiterator"
	. "github.com/kapitan-k/gorocksdb"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"strconv"
	"testing"
)

func givenKeysValues(cnt int64) (givenKeys, givenValues [][]byte) {
	givenKeys = make([][]byte, cnt)
	givenValues = make([][]byte, cnt)
	beg := cnt * cnt
	for i := int64(0); i < cnt; i++ {
		n := beg + i
		givenKeys[i] = []byte("sometypeofkey" + strconv.FormatInt(n, 10))
		givenValues[i] = []byte("sometypeofvalue" + strconv.FormatInt(n, 10))
	}

	return
}

func TestGoIterator(t *testing.T) {
	db := newTestDB(t, "TestIterator", nil)
	defer db.Close()

	wo := NewDefaultWriteOptions()

	givenKeys, givenValues := givenKeysValues(155)
	for i, k := range givenKeys {
		require.NoError(t, db.Put(wo, k, givenValues[i]))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)

	goitr := NewGoBufferIteratorFromIterator(iter, 256, 1, false, goiterator.IteratorSortOrder_Asc)
	_ = goitr.UnderlyingItr()

	i := 0
	for goitr.SeekToFirst(); goitr.Valid(); goitr.Next() {
		require.NoError(t, goitr.Err())
		k, v := goitr.KeyValue()
		require.Equal(t, k, givenKeys[i])
		require.Equal(t, v, givenValues[i])
		i++
	}
	require.NoError(t, goitr.Err())
	require.Equal(t, i, len(givenKeys))

	goitr = NewGoBufferIteratorFromIterator(iter, 7, 18, false, goiterator.IteratorSortOrder_Natural)
	goitr.SeekToFirst()
	require.False(t, goitr.Valid())
	require.Equal(t, goitr.Err(), ErrReadaheadBufferTooSmall)

	goitr = NewGoBufferIteratorFromIteratorWithBuffer(iter, 7, 18, true, make([]byte, 1024), goiterator.IteratorSortOrder_Desc)
	// test the wrong order
	goitr.Next()
	require.Equal(t, goitr.Err(), ErrInvalidIteratorDirection)
	goitr.Reset()
	require.NoError(t, goitr.Err())

	i = 1
	for goitr.SeekToLast(); goitr.Valid(); goitr.Prev() {
		require.NoError(t, goitr.Err())
		k, v := goitr.KeyValue()
		require.Equal(t, k, givenKeys[len(givenKeys)-i])
		require.Equal(t, v, givenValues[len(givenKeys)-i])
		i++
	}
	require.NoError(t, goitr.Err())
	require.Equal(t, i, len(givenKeys)+1)

	i = 0
	goitr.SetIteratorSortOrder(goiterator.IteratorSortOrder_Asc)
	for goitr.SeekToFirst(); goitr.Valid(); goitr.Next() {
		require.NoError(t, goitr.Err())
		require.Equal(t, goitr.Key(), givenKeys[i])
		require.Equal(t, goitr.Value(), givenValues[i])
		i++
	}
	require.NoError(t, goitr.Err())
	require.Equal(t, i, len(givenKeys))

	goitr = NewGoBufferIteratorFromIteratorWithBuffer(iter, 7, 18, true, make([]byte, 1024), goiterator.IteratorSortOrder_Natural)
	i = 0
	for goitr.Seek([]byte{}); goitr.Valid(); goitr.Next() {
		require.NoError(t, goitr.Err())
		require.Equal(t, goitr.Key(), givenKeys[i])
		require.Equal(t, goitr.Value(), givenValues[i])
		i++
	}

	require.NoError(t, goitr.Err())
	require.Equal(t, i, len(givenKeys))

	goitr.SeekToFirst()
	pdb := goiterator.BasePositionedDataBuffer{}
	goitr.SetIteratorSortOrder(goiterator.IteratorSortOrder_Asc)
	goitr.NextTo(&pdb, 1024, 55, goiterator.IteratorSortOrder_Asc)
	require.NoError(t, goitr.Err())

	fitr, err := pdb.ToForwardKVLengthBufferIterator()
	require.NoError(t, err)
	i = 0
	for ; fitr.Valid(); fitr.Next() {
		require.NoError(t, goitr.Err())
		require.Equal(t, fitr.Key(), givenKeys[i])
		require.Equal(t, fitr.Value(), givenValues[i])
		i++
	}

	goitr.SeekToLast()
	pdb = goiterator.BasePositionedDataBuffer{}
	goitr.SetIteratorSortOrder(goiterator.IteratorSortOrder_Desc)
	goitr.PrevTo(&pdb, 1024, 55, goiterator.IteratorSortOrder_Desc)
	require.NoError(t, goitr.Err())

	fitr, err = pdb.ToForwardKVLengthBufferIterator()
	require.NoError(t, err)
	i = 1
	for ; fitr.Valid(); fitr.Next() {
		require.NoError(t, goitr.Err())
		require.Equal(t, fitr.Key(), givenKeys[len(givenKeys)-i])
		require.Equal(t, fitr.Value(), givenValues[len(givenKeys)-i])
		i++
	}

	goitr.SeekForPrev([]byte{})
	require.NoError(t, goitr.Err())

	goitr.Close()
}

func BenchmarkGoIterator_Get(b *testing.B) {
	db, err := newBenchDB("TestBenchIteratorXX", nil)
	defer db.Close()
	require.NoError(b, err)

	wo := NewDefaultWriteOptions()

	givenKeys, givenValues := givenKeysValues(155555)
	for i, k := range givenKeys {
		require.NoError(b, db.Put(wo, k, givenValues[i]))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		goitr := NewGoBufferIteratorFromIterator(iter, 4096, 1024, false, goiterator.IteratorSortOrder_Asc)
		for goitr.SeekToFirst(); goitr.Valid(); goitr.Next() {
			_, _ = goitr.KeyValue()
		}
	}
}

func newTestDB(t *testing.T, name string, applyOpts func(opts *Options)) *DB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)

	return db
}

func newBenchDB(name string, applyOpts func(opts *Options)) (*DB, error) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	if err != nil {
		return nil, err
	}

	opts := NewDefaultOptions()
	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	return db, nil
}
