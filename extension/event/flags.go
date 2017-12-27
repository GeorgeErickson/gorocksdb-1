package event

// #cgo LDFLAGS: -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
// #cgo CXXFLAGS: -std=c++11
// #cgo CFLAGS: -Irocksdb
import "C"
