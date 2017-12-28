package iterator

// #include <stdlib.h>
// #include "rocksdb/c.h"
// #include "multiiterator.h"
import "C"
import (
	"errors"
	. "github.com/kapitan-k/goiterator"
	. "github.com/kapitan-k/gorocksdb"
	"unsafe"
)

type ComarisonType int

const (
	ComparisonType_FixedPrefixLen = 1
	ComparisonType_FixedSuffixLen = 2
)

// MultiIterator is used to iterate with multiple iterators at once.
// It currently only supports forward iteration.
type MultiIterator struct {
	c    *C.multiiterator_t
	bbi  IndexedBaseBufferIterator
	itrs []*Iterator
}

// NewFixedSuffixMultiIteratorFromIterators returns a newly allocated MultiIterator.
// The iterators itrs should not be used elsewhere from now.
// The iterators must have appropriate upper bounds set.
// The comparator of the iterators column families must be appropriate.
// (for example the standard BytewiseComparator would ONLY need same key sizes here to work).
func NewFixedSuffixMultiIteratorFromIterators(
	readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool,
	itrs []*Iterator, fixedSuffixLen uint64,
) (self *MultiIterator) {
	citrs := make([]*C.rocksdb_iterator_t, len(itrs))
	for i, itr := range itrs {
		citrs[i] = (*C.rocksdb_iterator_t)(itr.UnsafeGetUnsafeIterator())
	}
	c := C.create_multiiterator_by_rocksdb_iterators(
		(**C.rocksdb_iterator_t)(unsafe.Pointer(&citrs[0])),
		C.size_t(len(citrs)),
		C.size_t(fixedSuffixLen),
	)
	C.multiiterator_set_cmp_fn_cmp_all_fixed_suffix(c)
	C.multiiterator_set_keycmp_fn_key_memcmp(c)

	bbi := CreateIndexedBaseBufferIterator(readaheadSize, readaheadCnt, autoAdjustBufferSize)
	bbi.Order = IteratorSortOrder_Asc

	return &MultiIterator{
		c:    c,
		bbi:  bbi,
		itrs: itrs,
	}
}

// NewFixedPrefixMultiIteratorFromIterators returns a newly allocated MultiIterator.
// The iterators itrs should not be used elsewhere from now.
// The iterators must have appropriate upper bounds set.
// The comparator of the iterators column families must be appropriate.
// (for example the standard BytewiseComparator would ONLY need same key sizes here to work).
func NewFixedPrefixMultiIteratorFromIterators(
	readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool,
	itrs []*Iterator, fixedSuffixLen uint64,
) (self *MultiIterator) {
	citrs := make([]*C.rocksdb_iterator_t, len(itrs))
	for i, itr := range itrs {
		citrs[i] = (*C.rocksdb_iterator_t)(itr.UnsafeGetUnsafeIterator())
	}
	c := C.create_multiiterator_by_rocksdb_iterators(
		(**C.rocksdb_iterator_t)(unsafe.Pointer(&citrs[0])),
		C.size_t(len(citrs)),
		C.size_t(fixedSuffixLen),
	)
	C.multiiterator_set_cmp_fn_cmp_all_fixed_prefix(c)
	C.multiiterator_set_keycmp_fn_key_memcmp(c)

	bbi := CreateIndexedBaseBufferIterator(readaheadSize, readaheadCnt, autoAdjustBufferSize)
	bbi.Order = IteratorSortOrder_Asc

	return &MultiIterator{
		c:    c,
		bbi:  bbi,
		itrs: itrs,
	}
}

// NewMultiIteratorFromIteratorsNative returns a newly allocated MultiIterator.
// The iterators itrs should not be used elsewhere from now.
// The iterators must have appropriate upper bounds set.
// The comparator of the iterators column families must be appropriate.
// (for example the standard BytewiseComparator would ONLY need same key sizes here to work).
func NewMultiIteratorFromIteratorsNativeUnsafe(
	readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool,
	itrs []*Iterator,
	cIteratorCmpFn unsafe.Pointer,
	cKeyCmpFn unsafe.Pointer,
	configs ...func(self *MultiIterator),
) (self *MultiIterator) {
	citrs := make([]*C.rocksdb_iterator_t, len(itrs))
	for i, itr := range itrs {
		citrs[i] = (*C.rocksdb_iterator_t)(itr.UnsafeGetUnsafeIterator())
	}
	c := C.create_multiiterator_by_rocksdb_iterators(
		(**C.rocksdb_iterator_t)(unsafe.Pointer(&citrs[0])),
		C.size_t(len(citrs)),
		C.size_t(0),
	)

	if cIteratorCmpFn != nil {
		C.multiiterator_set_cmp_fn(c, (C.iters_cmp_fn)(cIteratorCmpFn))
	} else {
		C.multiiterator_set_cmp_fn_cmp_all_fixed_prefix(c)
	}

	if cKeyCmpFn != nil {
		C.multiiterator_set_keycmp_fn(c, (C.key_cmp_fn)(cKeyCmpFn))
	} else {
		C.multiiterator_set_keycmp_fn_key_memcmp(c)
	}

	bbi := CreateIndexedBaseBufferIterator(readaheadSize, readaheadCnt, autoAdjustBufferSize)
	bbi.Order = IteratorSortOrder_Asc

	self = &MultiIterator{
		c:    c,
		bbi:  bbi,
		itrs: itrs,
	}

	for _, cfg := range configs {
		cfg(self)
	}

	return
}

// fillReadahead tries to get new data from the underlying iterator in the current direction.
func (self *MultiIterator) fillReadahead() {
	pbbi := &self.bbi
	pbbi.Reset()

	var cErr *C.char
	var csize, ccnt, cneeded, cvalid C.size_t

	C.multiiterator_valid_next_to_buffer(
		self.c,
		C.int64_t(pbbi.Order),
		(*C.char)(unsafe.Pointer(&pbbi.Buffer[0])),
		C.size_t(pbbi.ReadaheadSize),
		(*C.size_t)(unsafe.Pointer(&pbbi.Lengths[0])),
		(*C.uint32_t)(unsafe.Pointer(&pbbi.Indexes[0])),
		C.size_t(pbbi.ReadaheadCnt),
		&csize,
		&ccnt,
		&cneeded,
		&cvalid,
		&cErr,
	)

	pbbi.Cnt = uint64(ccnt)
	pbbi.Size = uint64(csize)

	if ccnt == 0 && cneeded > 0 {
		if pbbi.AutoAdjustBufferSize {
			pbbi.SetReadaheadSize(uint64(cneeded))
			self.fillReadahead()
		} else {
			pbbi.SetErr(ErrReadaheadBufferTooSmall)
			return
		}
	}

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		pbbi.SetErr(errors.New(C.GoString(cErr)))
	}

}

// SeekToFirst moves the iterator to the first key in the database.
func (self *MultiIterator) SeekToFirst() {
	pbbi := &self.bbi
	var cErr *C.char
	pbbi.Reset()
	C.multiiterator_seek_to_first(self.c, &cErr)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		pbbi.SetErr(errors.New(C.GoString(cErr)))
	}
}

// SeekToKast moves the iterator to the last key in the database.
func (self *MultiIterator) SeekToLast() {
	pbbi := &self.bbi
	var cErr *C.char
	pbbi.Reset()
	C.multiiterator_seek_to_last(self.c, &cErr)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		pbbi.SetErr(errors.New(C.GoString(cErr)))
	}
}

// seekBaseKeys defines the basic seek with keys functionality.
func (self *MultiIterator) seekBaseKeys(
	keys [][]byte,
	fnCSeek func(
		*C.multiiterator_t,
		**C.char,
		*C.size_t,
		**C.char,
	),
) {
	pbbi := &self.bbi
	var cErr *C.char
	pbbi.Reset()
	keysPtrs, keysSizes := ByteSlicesToUintptrsAndSizeTSlices(keys)
	fnCSeek(
		self.c,
		(**C.char)(unsafe.Pointer(&keysPtrs[0])),
		(*C.size_t)(unsafe.Pointer(&keysSizes[0])),
		&cErr,
	)

	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		pbbi.SetErr(errors.New(C.GoString(cErr)))
	}
}

// Seek moves the underlying iterators to the position greater than or equal to the keys.
func (self *MultiIterator) Seek(keys [][]byte) {
	self.seekBaseKeys(keys, func(
		citr *C.multiiterator_t,
		ckeys **C.char,
		ckeysSizes *C.size_t,
		cerr **C.char,
	) {
		C.multiiterator_seek(
			citr, ckeys, ckeysSizes, cerr,
		)
	},
	)
}

// SeekForPrev moves the underlying iterators to the position smaller than the keys.
func (self *MultiIterator) SeekForPrev(keys [][]byte) {
	self.seekBaseKeys(keys, func(
		citr *C.multiiterator_t,
		ckeys **C.char,
		ckeysSizes *C.size_t,
		cerr **C.char,
	) {
		C.multiiterator_seek_for_prev(
			citr, ckeys, ckeysSizes, cerr,
		)
	},
	)
}

// KeyValue returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *MultiIterator) KeyValue() (k, v []byte) {
	return self.bbi.KeyValue()
}

// Key returns the key the iterator currently holds.
// Must not be called if Valid is false.
func (self *MultiIterator) Key() (k []byte) {
	return self.bbi.Key()
}

// Value returns the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *MultiIterator) Value() (v []byte) {
	return self.bbi.Value()
}

// IteratorIndex returns the index of MultiIterator itrs (given at initialization)
// which currently provides KeyValue().
// Must not be called if Valid is false.
func (self *MultiIterator) IteratorIndex() uint32 {
	return self.bbi.IteratorIndex()
}

// Next moves the iterator to the next sequential key in the database.
// Must not be called if Valid is false.
func (self *MultiIterator) Next() {
	pbbi := &self.bbi
	if pbbi.Order != IteratorSortOrder_Asc {
		pbbi.SetErr(ErrInvalidIteratorDirection)
		return
	}
	pbbi.InnerNext()
}

// Prev is not implemented with this iterator.
func (self *MultiIterator) Prev() {
	self.bbi.SetErr(ErrInvalidIteratorDirection)
}

func (self *MultiIterator) innerValid() bool {
	return self.bbi.InnerValid()
}

// Valid returns false only when ALL underlying iterators
// of the MultiIterator have iterated past either the
// first or the last key in the database.
func (self *MultiIterator) Valid() bool {
	pbbi := &self.bbi
	if pbbi.Err() != nil {
		return false
	}

	if pbbi.Cnt == 0 || pbbi.ReadPos == pbbi.Cnt {
		self.fillReadahead()
	}
	return self.innerValid()
}

// Reset resets the iterator to its defaults.
func (self *MultiIterator) Reset() {
	self.bbi.Reset()
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (self *MultiIterator) Err() (err error) {
	err = self.bbi.Err()
	if err != nil {
		return
	}

	return self.bbi.Err()
}

// Close closes the iterator and ALL underlying iterators.
// The MultiIterator is no longer usable.
func (self *MultiIterator) Close() {
	C.multiiterator_close(self.c)
}

// NextTo iterates with Next() up to readaheadSize or readaheadCnt and fills data into buf.
func (self *MultiIterator) NextTo(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
	self.to(buf, readaheadSize, readaheadCnt, order)
}

// PrevTo iterates with Prev() up to readaheadSize or readaheadCnt and fills data into buf.
func (self *MultiIterator) PrevTo(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
	self.to(buf, readaheadSize, readaheadCnt, order)
}

func (self *MultiIterator) to(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
	self.bbi.SetReadahead(readaheadSize, readaheadCnt)
	// Valid() if returns true has filled the readahead buffer
	isValid := self.Valid()
	if isValid {
		err := self.bbi.ToPositionedDataBuffer(buf)
		if err != nil {
			self.bbi.SetErr(err)
			return
		}
		self.bbi.Reset()
	}
}

func (self *MultiIterator) IndexedBaseBufferIterator() *IndexedBaseBufferIterator {
	return &self.bbi
}

// MultiIteratorBySuffixPlainAsc defines how suffix sort via
// standard string comparison (in C memcmp) is done in MultiIterator.
type MultiIteratorBySuffixPlainAsc struct {
	Keys      [][]byte
	Values    [][]byte
	SuffixLen uint64
}

func (s MultiIteratorBySuffixPlainAsc) Len() int {
	return len(s.Values)
}
func (s MultiIteratorBySuffixPlainAsc) Swap(i, j int) {
	s.Keys[i], s.Keys[j] = s.Keys[j], s.Keys[i]
	s.Values[i], s.Values[j] = s.Values[j], s.Values[i]
}
func (s MultiIteratorBySuffixPlainAsc) Less(i, j int) bool {
	suffixLen := int(s.SuffixLen)
	k1 := s.Keys[i]
	k2 := s.Keys[j]
	if len(k1) != len(k2) {
		return len(k1) < len(k2)
	}

	if len(k1) < suffixLen || len(k2) < suffixLen {
		return false
	}

	sk1 := string(k1[len(k1)-suffixLen:])
	sk2 := string(k2[len(k2)-suffixLen:])
	if sk1 == sk2 {
		bsk1 := string(k1[:len(k1)-suffixLen])
		bsk2 := string(k2[:len(k2)-suffixLen])
		return bsk1 < bsk2
	}

	return sk1 < sk2
}
