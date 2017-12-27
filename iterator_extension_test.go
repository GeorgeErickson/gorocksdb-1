package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"testing"
)

var benchDb *DB
var benchIter *Iterator
var benchReadOptions *ReadOptions

var key []byte
var val []byte

func TestMain(m *testing.M) {
	var err error
	benchDb, err = newBenchDB("TestBenchIterator", nil)
	if err != nil {
		log.Println("Couldnt setup bench db", err)
		os.Exit(1)
	}
	givenKeys, givenValues := givenKeysValues(150000)
	wo := NewDefaultWriteOptions()
	for i, key := range givenKeys {
		err = benchDb.Put(wo, key, givenValues[i])
		if err != nil {
			log.Println("Couldnt setup bench db", err)
			os.Exit(1)
		}
	}
	runtime.LockOSThread()

	benchReadOptions = NewDefaultReadOptions()
	benchReadOptions.SetPinData(true)
	benchIter = benchDb.NewIterator(benchReadOptions)
	code := m.Run()

	benchReadOptions.Destroy()
	benchIter.Close()

	os.Exit(code)
}

func TestIteratorExtensions(t *testing.T) {
	db := newTestDB(t, "TestIteratorExtension", nil)
	defer db.Close()

	// insert keys
	givenKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	givenValues := [][]byte{[]byte("val1"), []byte("val2"), []byte("val3")}
	wo := NewDefaultWriteOptions()
	for i, k := range givenKeys {
		require.Nil(t, db.Put(wo, k, givenValues[i]))
	}

	ro := NewDefaultReadOptions()
	iter := db.NewIterator(ro)
	defer iter.Close()

	var actualKeys [][]byte
	var actualValues [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key = make([]byte, 4)
		val = make([]byte, 4)
		copy(key, iter.Key())
		copy(val, iter.Value())
		actualKeys = append(actualKeys, key)
		actualValues = append(actualValues, val)
	}
	require.Nil(t, iter.Err())
	require.Equal(t, actualKeys, givenKeys)
	require.Equal(t, actualValues, givenValues)

	var actualExtKeys [][]byte
	var actualExtValues [][]byte

	iter.SeekToFirst()
	for valid, skey, sval := iter.ValidKeyValue(); valid; valid, skey, sval = iter.NextValidKeyValue() {
		key = make([]byte, 4)
		val = make([]byte, 4)
		copy(key, skey)
		copy(val, sval)
		actualExtKeys = append(actualExtKeys, key)
		actualExtValues = append(actualExtValues, val)
	}
	require.Nil(t, iter.Err())
	require.Equal(t, actualExtKeys, givenKeys)
	require.Equal(t, actualExtValues, givenValues)

}

func BenchmarkIteratorExtension_Get(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchIter.SeekToFirst()
		for valid, skey, sval := benchIter.ValidKeyValue(); valid; valid, skey, sval = benchIter.NextValidKeyValue() {
			key = skey
			val = sval
		}
	}
}

func BenchmarkIteratorExtensionSlices_Get(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchIter.SeekToFirst()
		for valid, xkey, xval := benchIter.ValidKeyValue(); valid; valid, xkey, xval = benchIter.NextValidKeyValue() {
			key = xkey
			val = xval
		}
	}
}

func BenchmarkIterator_Get(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for benchIter.SeekToFirst(); benchIter.Valid(); benchIter.Next() {
			key = benchIter.Key()
			val = benchIter.Value()
		}
	}
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
