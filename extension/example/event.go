package main

import (
	"github.com/google/gofuzz"
	"github.com/kapitan-k/goerror"
	"github.com/kapitan-k/gorocksdb"
	"github.com/kapitan-k/gorocksdb/extension/comparator"
	eventExt "github.com/kapitan-k/gorocksdb/extension/event"
	"github.com/kapitan-k/goutilities/event"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)

/*

How to use MultiIterator to aggregate events from different topics?
Lets use the TopicEventEventKey from github.com/kapitan-k/goutilities/event
as keys which have a fixed length/size of 16 byte
8 byte uint64 for the topic ID and
8 byte uint64 for the event ID, which includes time micros.

This example adresses the following scenario:
We want to efficiently store and retrieve all the chat messages of our customers
in RocksDB with go.
Here the chat message is thus the event and customer is our topic.
Each topic has a unique topic ID.
The event ID is ascending and unique per topic.

This scenario is common for events in different use cases.

How can we efficiently store retrieve all the chat messages of our customers?
There are plenty of tools and ideas of how to accomplish that,
but one can use RocksDB on a single node and a single writer service here.

My version of this case is:
2 column families:
cf_messages_total which is just a log of all total ordered keys without values.
Both use my TopicEventEventKey as key.
cf_messages_per_topic which is prefixed with a comparator for the topic ID so
RocksDB can keep these values together.


Ok, we could now iterator through cf_messages_total to get our events, no problem.
But, what if we have 1M customers and want to get the messages of 10 in ascending order?
Iterating through cf_messages_total would be not very efficient.

This is where the MultiIterator can be used.


*/

var db *gorocksdb.DB
var cfMessagesTotal, cfMessagesPerTopic *gorocksdb.ColumnFamilyHandle

var topicCnt = 300
var eventPerTopicCnt = 30
var f = fuzz.New()

var tkeys [][]event.TopicEventEventKey
var tvalues [][][]byte

func main() {
	var dir string
	var err error
	{
		dir, err = ioutil.TempDir("", "EventMultiIterator")
		goerror.PanicOnError(err)
	}
	db, cfMessagesTotal, cfMessagesPerTopic, err = createDB(dir)
	goerror.PanicOnError(err)
	defer db.Close()

	// inserts for each topic from 1 - topicCnt, eventPerTopicCnt events.
	insert()

	// lets use topicIDs 5, 6 and 244, just for testing.
	topicIDs := []event.TopicID{
		5, 6, 244,
	}

	mitr, err := eventExt.NewTopicEventMultiIteratorInit(
		db, cfMessagesPerTopic, 1024, 10, event.EventIDMax, topicIDs)
	goerror.PanicOnError(err)
	defer mitr.Close()

	// lets seek to beginning with 0.
	// but one could seek to any other EventID.
	mitr.SeekEventID(0)

	i := 0
	for ; mitr.Valid(); mitr.Next() {
		k, v := mitr.EventKeyValue()
		log.Println("having key", k.TopicID, k.EventID.String(), "with value len", len(v))
		i++
	}
	log.Println("got total keys", i)
	mitr.Reset()

}

func insert() {
	tvalues = make([][][]byte, topicCnt)
	for i := range tvalues {
		tvalues[i] = make([][]byte, eventPerTopicCnt)
	}

	tkeys = make([][]event.TopicEventEventKey, topicCnt)
	for i := range tkeys {
		tkeys[i] = make([]event.TopicEventEventKey, eventPerTopicCnt)
	}

	var err error
	f.NumElements(15, 300)

	writeOptions := gorocksdb.NewDefaultWriteOptions()

	// just a time
	timeSecs := time.Now().UTC().Unix() - 100000

	// single routine insert takes a while
	batch := gorocksdb.NewWriteBatch()
	// lets insert 1000 topics with 10000 events each reverse
	for i := topicCnt; i > 0; i-- {
		topicID := event.TopicID(i)
		for j := 0; j < eventPerTopicCnt; j++ {
			var eid event.EventID
			eid.SetTimeUsec(uint64((timeSecs + int64(j)*100) * 1000000))
			ekey := event.TopicEventEventKey{}
			ekey.TopicID = topicID
			ekey.EventID = eid
			value := []byte(strconv.FormatUint(eid.TimeUsec(), 10))
			f.Fuzz(&value)
			if len(value) == 0 {
				value = []byte{}
			}

			tvalues[i-1][j] = value
			tkeys[i-1][j] = ekey

			key := ekey.Slice()

			batch.PutVCFs(
				[]*gorocksdb.ColumnFamilyHandle{cfMessagesTotal, cfMessagesPerTopic},
				[][]byte{key, key}, [][]byte{{}, value})

			time.Sleep(time.Microsecond)
		}

		err = db.Write(writeOptions, batch)
		goerror.PanicOnError(err)
		batch.Clear()
	}

	batch.Destroy()
}

func createDB(path string) (
	db *gorocksdb.DB,
	cfMessagesTotal, cfMessagesPerTopic *gorocksdb.ColumnFamilyHandle,
	err error,
) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	var cfs []*gorocksdb.ColumnFamilyHandle
	db, cfs, err = gorocksdb.OpenDbColumnFamilies(
		opts, path+"RocksEventExample",
		[]string{"default", "cf_messages_total", "cf_messages_per_topic"},
		createCFOpts())
	if err != nil {
		return
	}

	return db, cfs[1], cfs[2], nil
}

func createCFOpts() []*gorocksdb.Options {

	optsTotal := gorocksdb.NewDefaultOptions()
	comparator.OptionsSetDoubleUint64Comparator(0, optsTotal)
	//optsTotal.SetPrefixExtractor(NewFixedPrefixTransform(int(8)))

	optsPerTopic := gorocksdb.NewDefaultOptions()
	comparator.OptionsSetDoubleUint64Comparator(0, optsPerTopic)
	//optsPerTopic.SetPrefixExtractor(NewFixedPrefixTransform(int(8)))

	return []*gorocksdb.Options{
		gorocksdb.NewDefaultOptions(), optsTotal, optsPerTopic}
}
