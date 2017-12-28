# A Go wrapper for RocksDB with utilities for better performance and extensions.

[![Build Status](https://travis-ci.org/kapitan-k/gorocksdb.png)](https://travis-ci.org/kapitan-k/gorocksdb) [![GoDoc](https://godoc.org/github.com/kapitan-k/gorocksdb?status.png)](http://godoc.org/github.com/kapitan-k/gorocksdb)
[![codecov](https://codecov.io/gh/kapitan-k/gorocksdb/branch/master/graph/badge.svg)](https://codecov.io/gh/kapitan-k/gorocksdb)

## Install

You'll need to build [RocksDB](https://github.com/facebook/rocksdb) v5.8+ on your machine.
Future RocksDB versions will have separate branches.



# Additional features

## GoBufferIterator
GoBufferIterator is an iterator that that uses the original iterator but does less cgo calls
due to prefetching data in a go byte slice.
This is useful if you range over a large number of KVs and can give you a large performance boost.

```go

	readaheadByteSize := uint64(1024 * 1024)
	readaheadCnt := uint64(1024)
	iter := db.NewIterator(readOptions)
	goitr := NewGoBufferIteratorFromIterator(iter, readaheadByteSize, readaheadCnt, false, IteratorSortOrder_Asc)

```

##  Extensions 
To extend functionality and mainly to reduce cgo calls.

ẀriteBatch
```go

	// PutVCF queues multiple key-value pairs in a column family.
	PutVCF(cf *ColumnFamilyHandle, keys, values [][]byte)

	// PutVCFs queues multiple key-value pairs in column families.
	PutVCFs(cfs []*ColumnFamilyHandle, keys, values [][]byte)

```

## MultiIterator
Its purpose is to iterate with multiple iterators at once.

Package /extension/events does provide a MultiIterator for "topics with events" -> [TopicEventMultiIterator](https://github.com/kapitan-k/gorocksdb/blob/master/extension/event/multiiterator.go).
An example can be found [here](https://github.com/kapitan-k/gorocksdb/blob/master/extension/example/event.go).


The standard MultiIterator can be used for example:
You have "topics" in your RocksDB which do store "events" and share a common suffix length. 
i.e. 8bytes Topic ID, 8bytes Event ID (16 byte is the key).
You want to "combine" some topics and iterate through those by ascending event id up to eventID9.

```go

	ro1 := NewDefaultReadOptions()
	ro1.SetIterateUpperBound([]byte("topicID1eventID9"))

	ro2 := NewDefaultReadOptions()
	ro2.SetIterateUpperBound([]byte("topicID2eventID9"))

	ro3 := NewDefaultReadOptions()
	ro3.SetIterateUpperBound([]byte("topicID3eventID9"))

	it1 := db.NewIterator(ro1)
	it2 := db.NewIterator(ro2)
	it3 := db.NewIterator(ro3)

	itrs = []*Iterator{
		it1, it2, it3,
	}

	readaheadSize := uint64(1024 * 1024)
	readaheadCnt := uint64(3)
	prefixLen := uint64(8)
	multiitr = NewFixedPrefixedMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, itrs, prefixLen)

	for multiitr.Seek(seekKeys); multiitr.Valid(); multiitr.Next() {
		k, v := multiitr.KeyValue()
		itrIdx := multiitr.IteratorIndex() // describes which of the iterators in "itrs" provides k, v
	}

```
Or:
You have "topics" in your RocksDB which do store "events" and share a common suffix length.
i.e. 8bytes Topic ID, 8bytes Event ID (16 byte is the key).
You want to "combine" some topics and iterate through those by ascending event id up to eventID9.
For the suffix type of MultiIterator you need an appropriate comparator.

```go
	
	suffixLen := uint64(8) // our Event ID key part which always has to be 8 bytes here.
	multiitr = NewFixedSuffixMultiIteratorFromIterators(readaheadSize, readaheadCnt, true, itrs, suffixLen)

```

Please be aware that your DB / column family needs the appropriate comparator!
If you use the standard comparator (BytewiseComparator) all keys MUST have same length.


## Examples
[TopicEventMultiIterator](https://github.com/kapitan-k/gorocksdb/blob/master/extension/example/event.go).

## Thanks
Thanks to [gorocksdb](https://github.com/tecbot/gorocksdb) where this is forked from originally.
