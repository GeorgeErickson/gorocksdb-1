package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import "unsafe"

// DBPath represents options for a dbpath.
type DBPath struct {
	c *C.rocksdb_dbpath_t
}

// NewDBPath creates a DBPath object
// with the given path and target_size.
func NewDBPath(path string, targetSize uint64) *DBPath {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	return NewNativeDBPath(C.rocksdb_dbpath_create(cpath, C.uint64_t(targetSize)))
}

// NewNativeDBPath creates a DBPath object.
func NewNativeDBPath(c *C.rocksdb_dbpath_t) *DBPath {
	return &DBPath{c}
}

// Destroy deallocates the DBPath object.
func (dbpath *DBPath) Destroy() {
	C.rocksdb_dbpath_destroy(dbpath.c)
}

// NewDBPathsFromData creates a slice with allocated DBPath objects
// from paths and targetSizes.
func NewDBPathsFromData(paths []string, targetSizes []uint64) []*DBPath {
	dbpaths := make([]*DBPath, len(paths))
	for i, path := range paths {
		targetSize := targetSizes[i]
		dbpaths[i] = NewDBPath(path, targetSize)
	}

	return dbpaths
}

// DestroyDBPaths deallocates all DBPath objects in dbpaths.
func DestroyDBPaths(dbpaths []*DBPath) {
	for _, dbpath := range dbpaths {
		dbpath.Destroy()
	}
}
