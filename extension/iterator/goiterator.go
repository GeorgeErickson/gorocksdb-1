package iterator

// #include "rocksdb/c.h"
// #include "goiterator.h"
import "C"
import (
	"errors"
	. "github.com/kapitan-k/goiterator"
	. "github.com/kapitan-k/gorocksdb"
	"unsafe"
)

var ErrReadaheadBufferTooSmall = errors.New("Readahead buffer too small")
var ErrInvalidIteratorDirection = errors.New("Invalid iterator direction")

// GoBufferIterator is a wrapper to the RocksDB Iterator which is optimized for
// sequential access on a given count of keys and/or a given total lengths of keys needed.
// So if you know for example that you need >= readaheadCnt keys this iterator
// is definitely faster than the normal iterator because the decreased count of cgo calls.
type GoBufferIterator struct {
	bbi BaseBufferIterator
	itr *Iterator
}

// NewGoBufferIteratorFromIterator allocates a new GoBufferIterator
// which is based on the RocksDB iterator itr.
// autoAdjustBuffer defines whether the readahead buffer len might be
// increased if a single key/value pair is too large for the current readahead buffer.
// It now should keep ownership of the itr provided.
func NewGoBufferIteratorFromIterator(
	itr *Iterator, readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool,
	order IteratorSortOrder,
) (gitr *GoBufferIterator) {
	gitr = &GoBufferIterator{
		bbi: CreateBaseBufferIterator(readaheadSize, readaheadCnt, autoAdjustBufferSize),
		itr: itr,
	}

	if order == IteratorSortOrder_Natural {
		order = IteratorSortOrder_Asc
	}

	gitr.bbi.Order = order

	return
}

// NewGoBufferIteratorFromIteratorWithBuffer allocates a new GoBufferIterator
// which is based on the RocksDB iterator itr.
// autoAdjustBuffer defines whether the readahead buffer len might be
// increased if a single key/value pair is too large for the current readahead buffer.
// The iterator itr should not be used elsewhere from now.
func NewGoBufferIteratorFromIteratorWithBuffer(
	itr *Iterator, readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool, buf []byte,
	order IteratorSortOrder,
) (gitr *GoBufferIterator) {
	gitr = &GoBufferIterator{
		bbi: CreateBaseBufferIteratorWithBuffer(readaheadSize, readaheadCnt, autoAdjustBufferSize, buf),
		itr: itr,
	}

	if order == IteratorSortOrder_Natural {
		order = IteratorSortOrder_Asc
	}

	gitr.bbi.Order = order

	return
}

func (self *GoBufferIterator) UnderlyingItr() *Iterator {
	return self.itr
}

func (self *GoBufferIterator) Close() {
	self.itr.Close()
}

// fillReadahead tries to get new data from the underlying iterator in the current direction.
func (self *GoBufferIterator) fillReadahead() {
	pbbi := &self.bbi
	pbbi.Reset()

	var cErr *C.char
	var csize, ccnt, cneeded, cvalid C.size_t

	C.iter_valid_next_to_buffer(
		(*C.rocksdb_iterator_t)(self.itr.UnsafeGetUnsafeIterator()),
		C.int64_t(pbbi.Order),
		(*C.char)(unsafe.Pointer(&pbbi.Buffer[0])),
		C.size_t(pbbi.ReadaheadSize),
		(*C.size_t)(unsafe.Pointer(&pbbi.Lengths[0])),
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

// Next moves the iterator to the next sequential key in the database.
// Must not be called if Valid is false.
func (self *GoBufferIterator) Next() {
	pbbi := &self.bbi
	if pbbi.Order != IteratorSortOrder_Asc {
		pbbi.SetErr(ErrInvalidIteratorDirection)
		return
	}
	pbbi.InnerNext()
}

// Prev moves the iterator to the previous sequential key in the database.
// Must not be called if Valid is false.
func (self *GoBufferIterator) Prev() {
	pbbi := &self.bbi
	if pbbi.Order != IteratorSortOrder_Desc {
		pbbi.SetErr(ErrInvalidIteratorDirection)
		return
	}
	pbbi.InnerNext()
}

// SeekToFirst moves the iterator to the first key in the database.
func (self *GoBufferIterator) SeekToFirst() {
	self.bbi.Reset()
	self.itr.SeekToFirst()
}

// SeekToLast moves the iterator to the last key in the database.
func (self *GoBufferIterator) SeekToLast() {
	self.bbi.Reset()
	self.itr.SeekToLast()
}

// Seek moves the iterator to the position greater than or equal to the key.
func (self *GoBufferIterator) Seek(k []byte) {
	self.bbi.Reset()
	self.itr.Seek(k)
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (self *GoBufferIterator) SeekForPrev(k []byte) {
	self.bbi.Reset()
	self.itr.SeekForPrev(k)
}

// Key returns the key the iterator currently holds.
// Must not be called if Valid is false.
func (self *GoBufferIterator) Key() (k []byte) {
	k, _ = self.bbi.KeyValue()
	return
}

// Value returns the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *GoBufferIterator) Value() (v []byte) {
	_, v = self.bbi.KeyValue()
	return
}

// KeyValue returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *GoBufferIterator) KeyValue() (k, v []byte) {
	return self.bbi.KeyValue()
}

// IteratorIndex returns the index of the iterator "element"
// that currently provides Key(), Value(), KeyValue().
// As the iterator has only one "element" it returns 0.
func (self *GoBufferIterator) IteratorIndex() (idx uint32) {
	return 0
}

func (self *GoBufferIterator) innerValid() bool {
	return self.bbi.InnerValid()
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (self *GoBufferIterator) Valid() bool {
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
func (self *GoBufferIterator) Reset() {
	self.bbi.Reset()
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (self *GoBufferIterator) Err() (err error) {
	err = self.bbi.Err()
	if err != nil {
		return
	}
	self.bbi.SetErr(self.itr.Err())
	return self.bbi.Err()
}

func (self *GoBufferIterator) SetIteratorSortOrder(order IteratorSortOrder) {
	pbbi := &self.bbi
	if pbbi.Order != IteratorSortOrder_Natural && pbbi.Order != order {
		pbbi.Reset()
		pbbi.Order = order
		self.fillReadahead()
		return
	}
}

// NextTo iterates with Next() up to readaheadSize or readaheadCnt and fills data into buf.
func (self *GoBufferIterator) NextTo(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
	self.to(buf, readaheadSize, readaheadCnt, order)
}

// PrevTo iterates with Prev() up to readaheadSize or readaheadCnt and fills data into buf.
func (self *GoBufferIterator) PrevTo(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
	self.to(buf, readaheadSize, readaheadCnt, order)
}

func (self *GoBufferIterator) to(buf PositionedDataBuffer, readaheadSize, readaheadCnt uint64, order IteratorSortOrder) {
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
