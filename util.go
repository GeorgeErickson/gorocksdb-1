package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"reflect"
	"unsafe"
)

// btoi converts a bool value to int.
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// CfreeByteSlice frees the underlying C memory.
func CfreeByteSlice(b []byte) {
	if b != nil {
		C.free(unsafe.Pointer(&b[0]))
	}
}

// boolToChar converts a bool value to C.uchar.
func boolToChar(b bool) C.uchar {
	if b {
		return 1
	}
	return 0
}

// charToByte converts a *C.char to a byte slice.
func charToByte(data *C.char, len C.size_t) []byte {
	var value []byte
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// byteToChar returns *C.char from byte slice.
func byteToChar(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		c = (*C.char)(unsafe.Pointer(&b[0]))
	}
	return c
}

// Go []byte to C string
// The C string is allocated in the C heap using malloc.
func cByteSlice(b []byte) *C.char {
	var c *C.char
	if len(b) > 0 {
		cData := C.malloc(C.size_t(len(b)))
		copy((*[1 << 24]byte)(cData)[0:len(b)], b)
		c = (*C.char)(cData)
	}
	return c
}

// stringToChar returns *C.char from string.
func stringToChar(s string) *C.char {
	ptrStr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(ptrStr.Data))
}

// charSlice converts a C array of *char to a []*C.char.
func charSlice(data **C.char, len C.int) []*C.char {
	var value []*C.char
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// sizeSlice converts a C array of size_t to a []C.size_t.
func sizeSlice(data *C.size_t, len C.int) []C.size_t {
	var value []C.size_t
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&value))
	sH.Cap, sH.Len, sH.Data = int(len), int(len), uintptr(unsafe.Pointer(data))
	return value
}

// ByteSlicesToUintptrsAndSizeTSlices uses the values data as pointers and lengths as sizes.
func ByteSlicesToUintptrsAndSizeTSlices(values [][]byte) (ptrs []uintptr, sizes []C.size_t) {
	ptrs = make([]uintptr, len(values))
	sizes = make([]C.size_t, len(values))
	for i, val := range values {
		if len(val) > 0 {
			ptrs[i] = uintptr(unsafe.Pointer(&val[0]))
		}

		sizes[i] = C.size_t(len(val))
	}
	return
}

// CFsToCCFs returns a slice of cfss *C.rocksdb_column_family_handle_t types.
func CFsToCCFs(cfs []*ColumnFamilyHandle) (ccfs []*C.rocksdb_column_family_handle_t) {
	ccfs = make([]*C.rocksdb_column_family_handle_t, len(cfs))
	for i, cf := range cfs {
		ccfs[i] = cf.c
	}
	return
}
