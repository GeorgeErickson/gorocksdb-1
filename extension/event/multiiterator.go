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

// NewTopicEventMultiIteratorInit allocates and initializes a
// new TopicEventMultiIterator.
// readOpts will be used for the underlying iterators but upper bound
// is overwritten.
func NewTopicEventMultiIteratorInit(
	db *DB,
	cfEvents *ColumnFamilyHandle,
	readaheadSize uint64,
	readaheadCnt uint64,
	upperBoundEventID event.EventID,
	topicIDs []event.TopicID,
) (tmitr *TopicEventMultiIterator, err error) {
	l := len(topicIDs)

	itrs := make([]*Iterator, l)
	readOptions := make([]*ReadOptions, l)

	for i, topicID := range topicIDs {
		ro := NewDefaultReadOptions()
		tek := event.TopicEventEventKey{}
		tek.TopicID = topicID
		tek.EventID = upperBoundEventID
		ro.SetIterateUpperBound(tek.AsSlice())
		ro.SetTailing(true)
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

func (tmitr *TopicEventMultiIterator) seekEventIDBase(
	eventID event.EventID, fnSeek func(keys [][]byte)) {
	keys := make([][]byte, len(tmitr.topicIDs))
	tek := event.TopicEventEventKey{}
	tek.EventID = eventID
	for i, topicID := range tmitr.topicIDs {
		tek.TopicID = topicID
		keys[i] = tek.Slice()
	}

	fnSeek(keys)
}

// SeekEventID moves the iterator to the position greater than or equal to the eventID.
func (tmitr *TopicEventMultiIterator) SeekEventID(eventID event.EventID) {
	tmitr.seekEventIDBase(eventID, tmitr.MultiIterator.Seek)
}

// SeekForPrevEventID moves the iterator to the last key that less than or equal
// to the eventID, in contrast with Seek.
func (tmitr *TopicEventMultiIterator) SeekForPrevEventID(eventID event.EventID) {
	tmitr.seekEventIDBase(eventID, tmitr.MultiIterator.SeekForPrev)
}

// Seek moves the iterator to the position greater than or equal to the key.
func (tmitr *TopicEventMultiIterator) Seek(k []byte) {
	if len(k) != event.TopicEventEventKeyByteSz {
		tmitr.IndexedBaseBufferIterator().SetErr(goerror.ErrInvalidLength)
		return
	}
	var eventID event.EventID
	eventID.FromSlice(k)
	tmitr.SeekEventID(eventID)
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (tmitr *TopicEventMultiIterator) SeekForPrev(k []byte) {
	if len(k) != event.TopicEventEventKeyByteSz {
		tmitr.IndexedBaseBufferIterator().SetErr(goerror.ErrInvalidLength)
		return
	}

	var eventID event.EventID
	eventID.FromSlice(k)
	tmitr.SeekForPrevEventID(eventID)
}

// SeekToFirst moves the iterator to the first key in the database.
func (tmitr *TopicEventMultiIterator) SeekToFirst() {
	var eventID event.EventID
	tmitr.SeekEventID(eventID)
}

// SeekToLast moves the iterator to the last key in the database.
func (tmitr *TopicEventMultiIterator) SeekToLast() {
	tmitr.SeekEventID(event.EventIDMax)
}

// EventKeyValue returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (tmitr *TopicEventMultiIterator) EventKeyValue() (k event.TopicEventEventKey, v []byte) {
	var sk []byte
	sk, v = tmitr.MultiIterator.KeyValue()
	k.FromSlice(sk)
	return
}

// EventKey returns the key and the value the iterator currently holds.
// Must not be called if Valid is false.
func (tmitr *TopicEventMultiIterator) EventKey() (k event.TopicEventEventKey) {
	sk := tmitr.MultiIterator.Key()
	k.FromSlice(sk)
	return
}

// Close closes the iterator and ALL underlying iterators.
// The MultiIterator is no longer usable.
func (tmitr *TopicEventMultiIterator) Close() {
	tmitr.MultiIterator.Close()
	for _, ro := range tmitr.readOptions {
		ro.Destroy()
	}
}
