package comparator

// #cgo CXXFLAGS: -std=c++11
// #include <stdlib.h>
// #include "rocksdb/c.h"
// #include "uint.h"
import "C"

import (
	"github.com/kapitan-k/gorocksdb"
	"unsafe"
)

// OptionsSetSingleUint64Comparator sets a
// singleuint64comparator_t as comparator for the options.
// thus the comparison is based on a uint64_t after
// cmpPrefixByteOffset (in bytes).
func OptionsSetSingleUint64Comparator(
	cmpPrefixByteOffset uint64,
	opts *gorocksdb.Options,
) {
	opts.SetComparatorUnsafe(
		unsafe.Pointer(C.singleuint64comparator_new(
			C.size_t(cmpPrefixByteOffset))),
	)
}

// OptionsSetDoubleUint64Comparator sets a
// doubleuint64comparator_t as comparator for the options.
// thus the comparison is based on two following uint64_t after
// cmpPrefixByteOffset (in bytes).
func OptionsSetDoubleUint64Comparator(
	cmpPrefixByteOffset uint64,
	opts *gorocksdb.Options,
) {
	opts.SetComparatorUnsafe(
		unsafe.Pointer(C.doubleuint64comparator_new(
			C.size_t(cmpPrefixByteOffset))),
	)
}

// OptionsSetReverseSingleUint64Comparator sets a
// reversesingleuint64comparator_t as comparator for the options.
// thus the comparison is based on a uint64_t after
// cmpPrefixByteOffset (in bytes) in reverse order
func OptionsSetReverseSingleUint64Comparator(
	cmpPrefixByteOffset uint64,
	opts *gorocksdb.Options,
) {
	opts.SetComparatorUnsafe(
		unsafe.Pointer(C.reversesingleuint64comparator_new(
			C.size_t(cmpPrefixByteOffset))),
	)
}
