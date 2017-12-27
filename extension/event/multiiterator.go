package event

// #include "rocksdb/c.h"
// #include "event.h"
// #include "../../extension/iterator/multiiterator.h"
import "C"

import (
	"github.com/kapitan-k/goerror"
	. "github.com/kapitan-k/gorocksdb"
	rocksitr "github.com/kapitan-k/gorocksdb/extension/iterator"
	"github.com/kapitan-k/goutilities/event"
	"unsafe"
)

// TopicEventMultiIterator can be used to aggregate events from different topics.
// We use the TopicEventEventKey from github.com/kapitan-k/goutilities/event
// as keys which have a fixed length/size of 16 byte
// 8 byte uint64 for the topic ID and
// 8 byte uint64 for the event ID, which includes time micros.
type TopicEventMultiIterator struct {
	*rocksitr.MultiIterator
	topicIDs    []event.TopicID
	readOptions []*ReadOptions
}

// NewTopicEventMultiIteratorInit.
// readOpts will be used for the underlying iterators but upper bound
// is overwritten.
func NewTopicEventMultiIteratorInit(
	db *DB,
	cfEvents *ColumnFamilyHandle,
	readaheadSize uint64,
	readaheadCnt uint64,
	upperBoundEventID event.EventID,
	topicIDs []event.TopicID,
) (self *TopicEventMultiIterator, err error) {
	l := len(topicIDs)

	itrs := make([]*Iterator, l)
	readOptions := make([]*ReadOptions, l)

	for i, topicID := range topicIDs {
		ro := NewDefaultReadOptions()
		tek := event.TopicEventEventKey{}
		tek.TopicID = topicID
		tek.EventID = upperBoundEventID
		ro.SetIterateUpperBound(tek.AsSlice())
		itr := db.NewIteratorCF(ro, cfEvents)
		itrs[i] = itr
		readOptions[i] = ro
	}

	mitr := rocksitr.NewMultiIteratorFromIteratorsNativeUnsafe(
		readaheadSize, readaheadCnt, true, itrs, unsafe.Pointer(nil), C.key_uint64_single_uint64_offs_cmp)

	return &TopicEventMultiIterator{
		MultiIterator: mitr,
		topicIDs:      topicIDs,
		readOptions:   readOptions,
	}, nil
}

func (self *TopicEventMultiIterator) seekEventIDBase(
	eventID event.EventID, fnSeek func(keys [][]byte)) {
	keys := make([][]byte, len(self.topicIDs))
	tek := event.TopicEventEventKey{}
	tek.EventID = eventID
	for i, topicID := range self.topicIDs {
		tek.TopicID = topicID
		keys[i] = tek.Slice()
	}

	fnSeek(keys)
}

func (self *TopicEventMultiIterator) SeekEventID(eventID event.EventID) {
	self.seekEventIDBase(eventID, self.MultiIterator.Seek)
}

func (self *TopicEventMultiIterator) SeekForPrevEventID(eventID event.EventID) {
	self.seekEventIDBase(eventID, self.MultiIterator.SeekForPrev)
}

func (self *TopicEventMultiIterator) Seek(k []byte) {
	if len(k) != event.TopicEventEventKeyByteSz {
		self.IndexedBaseBufferIterator().SetErr(goerror.ErrInvalidLength)
		return
	}
	var eventID event.EventID
	eventID.FromSlice(k)
	self.SeekEventID(eventID)
}

func (self *TopicEventMultiIterator) SeekForPrev(k []byte) {
	if len(k) != event.TopicEventEventKeyByteSz {
		self.IndexedBaseBufferIterator().SetErr(goerror.ErrInvalidLength)
		return
	}

	var eventID event.EventID
	eventID.FromSlice(k)
	self.SeekForPrevEventID(eventID)
}

// SeekToFirst moves the iterator to the first key in the database.
func (self *TopicEventMultiIterator) SeekToFirst() {
	var eventID event.EventID
	self.SeekEventID(eventID)
}

// SeekToKast moves the iterator to the last key in the database.
func (self *TopicEventMultiIterator) SeekToLast() {
	self.SeekEventID(event.EventIDMax)
}

// EventKeyValue returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *TopicEventMultiIterator) EventKeyValue() (k event.TopicEventEventKey, v []byte) {
	var sk []byte
	sk, v = self.MultiIterator.KeyValue()
	k.FromSlice(sk)
	return
}

// EventKey returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (self *TopicEventMultiIterator) EventKey() (k event.TopicEventEventKey) {
	sk := self.MultiIterator.Key()
	k.FromSlice(sk)
	return
}

// Close closes the iterator and ALL underlying iterators.
// The MultiIterator is no longer usable.
func (self *TopicEventMultiIterator) Close() {
	self.MultiIterator.Close()
	for _, ro := range self.readOptions {
		ro.Destroy()
	}
}
