package iterator

// #include "rocksdb/c.h"
// #include "goiterator.h"
import "C"
import (
	"errors"
	"github.com/kapitan-k/goiterator"
	"github.com/kapitan-k/gorocksdb"
	"unsafe"
)

// ErrReadaheadBufferTooSmall is returned if the readahead buffer
// can not or is not allowed to be enlarged.
var ErrReadaheadBufferTooSmall = errors.New("Readahead buffer too small")

// ErrInvalidIteratorDirection is returned if the seek type or Next()/Prev()
// does not suit the used direction.
var ErrInvalidIteratorDirection = errors.New("Invalid iterator direction")

// GoBufferIterator is a wrapper to the RocksDB Iterator which is optimized for
// sequential access on a given count of keys and/or a given total lengths of keys needed.
// So if you know for example that you need >= readaheadCnt keys this iterator
// is definitely faster than the normal iterator because the decreased count of cgo calls.
// Values in RocksDB must be smaller that uint32_t max.
type GoBufferIterator struct {
	bbi goiterator.BaseBufferIterator
	itr *gorocksdb.Iterator
}

// NewGoBufferIteratorFromIterator allocates a new GoBufferIterator
// which is based on the RocksDB iterator itr.
// autoAdjustBuffer defines whether the readahead buffer len might be
// increased if a single key/value pair is too large for the current readahead buffer.
// It now should keep ownership of the itr provided.
func NewGoBufferIteratorFromIterator(
	itr *gorocksdb.Iterator, readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool,
	order goiterator.IteratorSortOrder,
) (gitr *GoBufferIterator) {
	gitr = &GoBufferIterator{
		bbi: goiterator.CreateBaseBufferIterator(
			readaheadSize, readaheadCnt, autoAdjustBufferSize),
		itr: itr,
	}

	if order == goiterator.IteratorSortOrder_Natural {
		order = goiterator.IteratorSortOrder_Asc
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
	itr *gorocksdb.Iterator, readaheadSize, readaheadCnt uint64, autoAdjustBufferSize bool, buf []byte,
	order goiterator.IteratorSortOrder,
) (gitr *GoBufferIterator) {
	gitr = &GoBufferIterator{
		bbi: goiterator.CreateBaseBufferIteratorWithBuffer(
			readaheadSize, readaheadCnt, autoAdjustBufferSize, buf),
		itr: itr,
	}

	if order == goiterator.IteratorSortOrder_Natural {
		order = goiterator.IteratorSortOrder_Asc
	}

	gitr.bbi.Order = order

	return
}

// UnderlyingItr returns the underlying Iterator.
func (gbi *GoBufferIterator) UnderlyingItr() *gorocksdb.Iterator {
	return gbi.itr
}

// Close closes the underlying Iterator.
func (gbi *GoBufferIterator) Close() {
	gbi.itr.Close()
}

// fillReadahead tries to get new data from the underlying iterator in the current direction.
func (gbi *GoBufferIterator) fillReadahead() {
	pbbi := &gbi.bbi
	pbbi.Reset()

	var cErr *C.char
	var csize, ccnt, cneeded, cvalid C.size_t

	C.iter_valid_next_to_buffer(
		(*C.rocksdb_iterator_t)(gbi.itr.UnsafeGetUnsafeIterator()),
		C.int64_t(pbbi.Order),
		(*C.char)(unsafe.Pointer(&pbbi.Buffer[0])),
		C.size_t(pbbi.ReadaheadSize),
		(*C.uint32_t)(unsafe.Pointer(&pbbi.Lengths[0])),
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
			gbi.fillReadahead()
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
func (gbi *GoBufferIterator) Next() {
	pbbi := &gbi.bbi
	if pbbi.Order != goiterator.IteratorSortOrder_Asc {
		pbbi.SetErr(ErrInvalidIteratorDirection)
		return
	}
	pbbi.InnerNext()
}

// Prev moves the iterator to the previous sequential key in the database.
// Must not be called if Valid is false.
func (gbi *GoBufferIterator) Prev() {
	pbbi := &gbi.bbi
	if pbbi.Order != goiterator.IteratorSortOrder_Desc {
		pbbi.SetErr(ErrInvalidIteratorDirection)
		return
	}
	pbbi.InnerNext()
}

// SeekToFirst moves the iterator to the first key in the database.
func (gbi *GoBufferIterator) SeekToFirst() {
	gbi.bbi.Reset()
	gbi.itr.SeekToFirst()
}

// SeekToLast moves the iterator to the last key in the database.
func (gbi *GoBufferIterator) SeekToLast() {
	gbi.bbi.Reset()
	gbi.itr.SeekToLast()
}

// Seek moves the iterator to the position greater than or equal to the key.
func (gbi *GoBufferIterator) Seek(k []byte) {
	gbi.bbi.Reset()
	gbi.itr.Seek(k)
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (gbi *GoBufferIterator) SeekForPrev(k []byte) {
	gbi.bbi.Reset()
	gbi.itr.SeekForPrev(k)
}

// Key returns the key the iterator currently holds.
// Must not be called if Valid is false.
func (gbi *GoBufferIterator) Key() (k []byte) {
	k, _ = gbi.bbi.KeyValue()
	return
}

// Value returns the value the iterator currently holds.
// Must not be called if Valid is false.
func (gbi *GoBufferIterator) Value() (v []byte) {
	_, v = gbi.bbi.KeyValue()
	return
}

// KeyValue returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (gbi *GoBufferIterator) KeyValue() (k, v []byte) {
	return gbi.bbi.KeyValue()
}

// IteratorIndex returns the index of the iterator "element"
// that currently provides Key(), Value(), KeyValue().
// As the iterator has only one "element" it returns 0.
func (gbi *GoBufferIterator) IteratorIndex() (idx uint32) {
	return 0
}

func (gbi *GoBufferIterator) innerValid() bool {
	return gbi.bbi.InnerValid()
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (gbi *GoBufferIterator) Valid() bool {
	pbbi := &gbi.bbi
	if pbbi.Err() != nil {
		return false
	}

	if pbbi.Cnt == 0 || pbbi.ReadPos == pbbi.Cnt {
		gbi.fillReadahead()
	}
	return gbi.innerValid()
}

// Reset resets the iterator to its defaults.
func (gbi *GoBufferIterator) Reset() {
	gbi.bbi.Reset()
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (gbi *GoBufferIterator) Err() (err error) {
	err = gbi.bbi.Err()
	if err != nil {
		return
	}
	gbi.bbi.SetErr(gbi.itr.Err())
	return gbi.bbi.Err()
}

// SetIteratorSortOrder sets the sort order and may reset
// and updates the readahead buffer. Err() should be checked afterwards.
func (gbi *GoBufferIterator) SetIteratorSortOrder(order goiterator.IteratorSortOrder) {
	pbbi := &gbi.bbi
	if pbbi.Order != goiterator.IteratorSortOrder_Natural &&
		pbbi.Order != order {
		pbbi.Reset()
		pbbi.Order = order
		gbi.fillReadahead()
		return
	}
}

// NextTo iterates with Next() up to readaheadSize or readaheadCnt and fills data into buf.
func (gbi *GoBufferIterator) NextTo(
	buf goiterator.PositionedDataBuffer,
	readaheadSize, readaheadCnt uint64,
	order goiterator.IteratorSortOrder) {
	gbi.to(buf, readaheadSize, readaheadCnt, order)
}

// PrevTo iterates with Prev() up to readaheadSize or readaheadCnt and fills data into buf.
func (gbi *GoBufferIterator) PrevTo(
	buf goiterator.PositionedDataBuffer,
	readaheadSize, readaheadCnt uint64,
	order goiterator.IteratorSortOrder) {
	gbi.to(buf, readaheadSize, readaheadCnt, order)
}

func (gbi *GoBufferIterator) to(
	buf goiterator.PositionedDataBuffer,
	readaheadSize, readaheadCnt uint64,
	order goiterator.IteratorSortOrder) {
	gbi.bbi.SetReadahead(readaheadSize, readaheadCnt)
	// Valid() if returns true has filled the readahead buffer
	isValid := gbi.Valid()
	if isValid {
		err := gbi.bbi.ToPositionedDataBuffer(buf)
		if err != nil {
			gbi.bbi.SetErr(err)
			return
		}
		gbi.bbi.Reset()
	}
}
