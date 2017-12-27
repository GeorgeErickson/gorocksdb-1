package iterator

import (
	"github.com/kapitan-k/goiterator"
	. "github.com/kapitan-k/gorocksdb"
	"github.com/stretchr/testify/require"
	"sort"
	"strconv"
	"testing"
)

var db *DB
var multiitr *MultiIterator

func TestMultiIteratorFixedSuffixInsertFunc(t *testing.T) {

	db = newTestDB(t, "TestMultiIteratorFixedSuffix", nil)

	wo := NewDefaultWriteOptions()

	for i, keys := range suffixKeys {
		values := suffixValues[i]
		for j, key := range keys {
			require.NoError(t, db.Put(wo, key, values[j]))
		}
	}

	itrs := createItrsSuffix()

	for i, keys := range suffixKeys {
		values := suffixValues[i]
		it := itrs[i]
		j := 0
		begKeyStr := "topicID" + strconv.FormatInt(int64(i+1), 10)
		for it.Seek([]byte(begKeyStr)); it.Valid(); it.Next() {
			k, v := it.KeyValue()
			require.EqualValues(t, keys[j], k)
			require.EqualValues(t, values[j], v)
			j++
		}

		require.Equal(t, len(keys), j)
	}

	for _, itr := range itrs {
		itr.Close()
	}

}

func TestMultiIteratorClose(t *testing.T) {
	readaheadSize := uint64(1024 * 1024)
	readaheadCnt := uint64(3)
	multiitr = NewMultiIteratorFromIteratorsNativeUnsafe(readaheadSize, readaheadCnt, true, createItrsSuffix(), nil, nil)
	multiitr.Close()
}

func TestMultiIteratorFixedSuffixAsc(t *testing.T) {
	readaheadSize := uint64(1024 * 1024)
	readaheadCnt := uint64(3)
	suffixLen := uint64(8)
	itrs := createItrsSuffix()
	multiitr = NewFixedSuffixMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, itrs, suffixLen)
	defer multiitr.Close()

	totalKeys := [][]byte{}
	totalValues := [][]byte{}

	{
		for i, keys := range suffixKeys {
			totalKeys = append(totalKeys, keys...)
			totalValues = append(totalValues, suffixValues[i]...)
		}
		kvs := MultiIteratorBySuffixPlainAsc{
			totalKeys,
			totalValues,
			suffixLen,
		}

		sort.Sort(kvs)
	}

	seekKeys := [][]byte{}
	for i := range suffixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(i+1), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.Seek(seekKeys)
	require.NoError(t, multiitr.Err())

	// afte SeekToFirst alls must be at first
	for i, keys := range suffixKeys {
		values := suffixValues[i]
		it := itrs[i]
		j := 0

		for ; it.Valid(); it.Next() {
			k, v := it.KeyValue()
			require.EqualValues(t, keys[j], k)
			require.EqualValues(t, values[j], v)
			j++
		}

		require.Equal(t, len(keys), j)
	}

	// reset to zero
	multiitr.Seek(seekKeys)
	require.NoError(t, multiitr.Err())

	j := 0
	indexes := []uint64{}
	for ; multiitr.Valid(); multiitr.Next() {
		k, v := multiitr.KeyValue()
		require.EqualValues(t, totalKeys[j], k)
		require.EqualValues(t, totalValues[j], v)
		indexes = append(indexes, uint64(multiitr.IteratorIndex()))
		j++
	}
	require.EqualValues(t, []uint64{1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2, 0}, indexes)
	require.NoError(t, multiitr.Err())

}

func TestMultiIteratorResetClose1(t *testing.T) {
	// reset and close
	multiitr.Reset()
	db.Close()
}

func TestMultiIteratorFixedPrefixInsertFunc(t *testing.T) {

	db = newTestDB(t, "TestMultiIteratorFixedPrefix", nil)

	wo := NewDefaultWriteOptions()

	for i, keys := range prefixKeys {
		values := prefixValues[i]
		for j, key := range keys {
			require.NoError(t, db.Put(wo, key, values[j]))
		}
	}

	itrs := createItrsPrefix()
	for i, keys := range prefixKeys {
		it := itrs[i]
		j := 0
		begKeyStr := "topicID" + strconv.FormatInt(int64(i+1), 10)
		it.Seek([]byte(begKeyStr))
		for ; it.Valid(); it.Next() {
			j++
		}
		require.Equal(t, len(keys), j)
	}

	for _, itr := range itrs {
		itr.Close()
	}
}

func TestMultiIteratorFixedPrefixAsc(t *testing.T) {
	readaheadSize := uint64(1024 * 1024)
	readaheadCnt := uint64(111)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, createItrsPrefix(), prefixLen)
	defer multiitr.Close()
	multiitr.SeekToFirst()
	require.NoError(t, multiitr.Err())

	seekKeys := [][]byte{}
	for i := range prefixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(i+1), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.Seek(seekKeys)
	require.NoError(t, multiitr.Err())
	j := 0
	indexes := []uint64{}
	for ; multiitr.Valid(); multiitr.Next() {
		indexes = append(indexes, uint64(multiitr.IteratorIndex()))
		j++
	}
	require.Equal(t, 12, j)
	require.NoError(t, multiitr.Err())

}

func TestMultiIteratorLast(t *testing.T) {
	readaheadSize := uint64(4)
	readaheadCnt := uint64(1)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixMultiIteratorFromIterators(readaheadSize, readaheadCnt, false, createItrsPrefix(), prefixLen)

	seekKeys := [][]byte{}
	for range prefixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(8), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.Seek(seekKeys)
	multiitr.Valid()
}

func TestMultiIteratorForceAutoadjust(t *testing.T) {
	readaheadSize := uint64(1)
	readaheadCnt := uint64(1)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, createItrsPrefix(), prefixLen)

	seekKeys := [][]byte{}
	for range prefixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(0), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.SeekToLast()
	multiitr.Seek(seekKeys)
	multiitr.Valid()
}

func TestMultiIteratorForceAutoadjustSeekForPrev(t *testing.T) {
	readaheadSize := uint64(1)
	readaheadCnt := uint64(1)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, createItrsPrefix(), prefixLen)

	seekKeys := [][]byte{}
	for range prefixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(0), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.SeekForPrev(seekKeys)
	multiitr.Valid()
}

func TestMultiIteratorNextTo(t *testing.T) {
	readaheadSize := uint64(4)
	readaheadCnt := uint64(1)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixMultiIteratorFromIterators(readaheadSize, readaheadCnt, false, createItrsPrefix(), prefixLen)
	seekKeys := [][]byte{}
	for range prefixKeys {
		begKeyStr := "topicID" + strconv.FormatInt(int64(8), 10)
		seekKeys = append(seekKeys, []byte(begKeyStr))
	}

	multiitr.Seek(seekKeys)

	pdb := goiterator.BasePositionedDataBuffer{}
	multiitr.NextTo(&pdb, readaheadSize, readaheadCnt, 0)
	require.NoError(t, multiitr.Err())

	pdb = goiterator.BasePositionedDataBuffer{}
	multiitr.PrevTo(&pdb, readaheadSize, readaheadCnt, 0)
	require.NoError(t, multiitr.Err())
}

func TestMultiIteratorResetClose2(t *testing.T) {
	// reset and close
	multiitr.Reset()
	multiitr.Close()
	db.Close()
}

func createItrsPrefix() []*Iterator {
	ro1 := NewDefaultReadOptions()
	ro1.SetIterateUpperBound([]byte("topicID2"))

	ro2 := NewDefaultReadOptions()
	ro2.SetIterateUpperBound([]byte("topicID3"))

	ro3 := NewDefaultReadOptions()
	ro3.SetIterateUpperBound([]byte("topicID4"))

	it1 := db.NewIterator(ro1)
	it2 := db.NewIterator(ro2)
	it3 := db.NewIterator(ro3)

	return []*Iterator{
		it1, it2, it3,
	}
}

func createItrsSuffix() []*Iterator {
	ro1 := NewDefaultReadOptions()
	ro1.SetIterateUpperBound([]byte("topicID1eventID9"))

	ro2 := NewDefaultReadOptions()
	ro2.SetIterateUpperBound([]byte("topicID2eventID9"))

	ro3 := NewDefaultReadOptions()
	ro3.SetIterateUpperBound([]byte("topicID3eventID9"))

	it1 := db.NewIterator(ro1)
	it2 := db.NewIterator(ro2)
	it3 := db.NewIterator(ro3)

	return []*Iterator{
		it1, it2, it3,
	}
}

var suffixKeys = [][][]byte{
	{
		[]byte("topicID1eventID2"),
		[]byte("topicID1eventID3"),
		[]byte("topicID1eventID4"),
		[]byte("topicID1eventID5"),
	},

	{
		[]byte("topicID2eventID1"),
		[]byte("topicID2eventID2"),
		[]byte("topicID2eventID3"),
		[]byte("topicID2eventID4"),
	},

	{
		[]byte("topicID3eventID1"),
		[]byte("topicID3eventID2"),
		[]byte("topicID3eventID3"),
		[]byte("topicID3eventID4"),
	},
}

var suffixValues = [][][]byte{
	{
		[]byte("topicID1eventID2"),
		[]byte("topicID1eventID3"),
		[]byte("topicID1eventID4"),
		[]byte("topicID1eventID5"),
	},

	{
		[]byte("topicID2eventID1"),
		[]byte("topicID2eventID2"),
		[]byte("topicID2eventID3"),
		[]byte("topicID2eventID4"),
	},

	{
		[]byte("topicID3eventID1"),
		[]byte("topicID3eventID2"),
		[]byte("topicID3eventID3"),
		[]byte("topicID3eventID4"),
	},
}

var prefixKeys = [][][]byte{
	{
		[]byte("topicID1eventID2"),
		[]byte("topicID1eventID3"),
		[]byte("topicID1eventID4"),
		[]byte("topicID1eventID5"),
	},

	{
		[]byte("topicID2eventID1ffh"),
		[]byte("topicID2eventID2"),
		[]byte("topicID2eventID3"),
		[]byte("topicID2eventID4"),
	},

	{
		[]byte("topicID3eventID8ffg"),
		[]byte("topicID3eventID2"),
		[]byte("topicID3eventID3"),
		[]byte("topicID3eventID4"),
	},
}

var prefixValues = [][][]byte{
	{
		[]byte("topicID1eventID2"),
		[]byte("topicID1eventID3"),
		[]byte("topicID1eventID4"),
		[]byte("topicID1eventID5"),
	},

	{
		[]byte("topicID2eventID1ffh"),
		[]byte("topicID2eventID2"),
		[]byte("topicID2eventID3"),
		[]byte("topicID2eventID4"),
	},

	{
		[]byte("topicID3eventID1ffg"),
		[]byte("topicID3eventID2"),
		[]byte("topicID3eventID3"),
		[]byte("topicID3eventID4"),
	},
}
