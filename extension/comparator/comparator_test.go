package comparator

import (
	. "github.com/kapitan-k/gorocksdb"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
	"unsafe"
)

func TestExtension(t *testing.T) {
	var db *DB
	var cfsDb []*ColumnFamilyHandle
	var err error
	var v []byte

	var wd string
	wd, err = os.Getwd()
	require.NoError(t, err)

	path := wd + "/DB"
	os.RemoveAll(path)

	prefixOpts := NewDefaultOptions()
	prefixOpts.SetPrefixExtractor(NewFixedPrefixTransform(int(10)))
	to := NewDefaultBlockBasedTableOptions()
	to.SetBlockSize(4 * 1024)

	to.SetBlockCache(NewLRUCache(16 * 1024 * 1024))

	prefixOpts.SetBlockBasedTableFactory(to)

	// test all set functions
	OptionsSetSingleUint64Comparator(2, prefixOpts)
	OptionsSetReverseSingleUint64Comparator(2, prefixOpts)

	OptionsSetDoubleUint64Comparator(2, prefixOpts)

	cfNames := []string{"default", "CF1", "CF2"}
	cfOpts := []*Options{
		prefixOpts,
		NewDefaultOptions(),
		NewDefaultOptions(),
	}

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	wopts := NewDefaultWriteOptions()
	ropts := NewDefaultReadOptions()

	db, cfsDb, err = OpenDbColumnFamilies(opts, path, cfNames, cfOpts)
	require.NoError(t, err)
	defer db.Close()

	cfTest := cfsDb[0]

	ks := make([]DoubleUintPrefixKey, 3)
	k := DoubleUintPrefixKey{}
	k.SetKeyInfo(0, 0, 45412985267117248, 5555)
	ks[0] = k
	k2 := DoubleUintPrefixKey{}
	k2.SetKeyInfo(0, 0, 45412985267117248, 5557)
	ks[1] = k2
	k3 := DoubleUintPrefixKey{}
	k3.SetKeyInfo(0, 0, 45412985267117248, 5559)
	ks[2] = k3

	log.Println("", wopts, ropts, cfsDb)

	k4 := DoubleUintPrefixKey{}
	k4.SetKeyInfo(0, 0, 45412985267117247, 5559)
	err = db.PutCF(wopts, cfTest, k4[:], []byte("blabl4"))
	require.NoError(t, err)

	err = db.PutCF(wopts, cfTest, k[:], []byte("blabla"))
	require.NoError(t, err)

	err = db.PutCF(wopts, cfTest, k2[:], []byte("blablu"))
	require.NoError(t, err)

	err = db.PutCF(wopts, cfTest, k3[:], []byte("blablx"))
	require.NoError(t, err)

	v, err = db.GetCF(ropts, cfTest, k[:])
	require.NoError(t, err)
	require.Equal(t, []byte("blabla"), v)

	v, err = db.GetCF(ropts, cfTest, k2[:])
	require.NoError(t, err)
	require.Equal(t, []byte("blablu"), v)

	itr := db.NewIteratorCF(ropts, cfTest)
	cnt := 0
	for itr.Seek(k[:]); itr.Valid(); itr.Next() {
		log.Println("val", string(itr.Value()))
		require.Equal(t, ks[cnt][:], itr.Key())
		cnt++
	}
	itr.Close()

	ropts.SetIterateUpperBound(k2[:])
	itr = db.NewIteratorCF(ropts, cfTest)
	cnt = 0
	for itr.Seek(k[:]); itr.Valid(); itr.Next() {
		log.Println("val", string(itr.Value()))
		require.Equal(t, ks[cnt][:], itr.Key())
		cnt++
	}
	itr.Close()
	require.Equal(t, 1, cnt)
	os.RemoveAll(path)
}

type DoubleUintPrefixKey [18]byte

func (self *DoubleUintPrefixKey) KeyInfo() (prefix1, prefix2 byte, id1, id2 uint64) {
	return KeyUInt64DoublePrefixedIDs(self[:])
}

func (self *DoubleUintPrefixKey) SetKeyInfo(prefix1, prefix2 byte, id1, id2 uint64) {
	KeyUInt64DoublePrefixedToBuf(self[:], prefix1, prefix2, id1, id2)
}

func KeyUInt64DoublePrefixedIDs(key []byte) (prefix1, prefix2 byte, id1, id2 uint64) {
	return KeyUInt64DoublePrefixedIDsFromPtr(uintptr(unsafe.Pointer(&key[0])))
}

func KeyUInt64DoublePrefixedIDsFromPtr(ptr uintptr) (prefix1, prefix2 byte, id1, id2 uint64) {
	prefix1 = *(*byte)(unsafe.Pointer(ptr))
	prefix2 = *(*byte)(unsafe.Pointer(ptr + 1))
	id1 = *(*uint64)(unsafe.Pointer(ptr + 2))
	id2 = *(*uint64)(unsafe.Pointer(ptr + 10))
	return
}

func KeyUInt64DoublePrefixedToBuf(buf []byte, prefix1, prefix2 byte, id1, id2 uint64) {
	KeyUInt64DoublePrefixedToPtr(uintptr(unsafe.Pointer(&buf[0])), prefix1, prefix2, id1, id2)
}

func KeyUInt64DoublePrefixedToPtr(ptr uintptr, prefix1, prefix2 byte, id1, id2 uint64) {
	*(*byte)(unsafe.Pointer(ptr)) = prefix1
	*(*byte)(unsafe.Pointer(ptr + 1)) = prefix2
	*(*uint64)(unsafe.Pointer(ptr + 2)) = id1
	*(*uint64)(unsafe.Pointer(ptr + 10)) = id2
}
