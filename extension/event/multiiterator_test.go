package event

import (
	"github.com/google/gofuzz"
	"github.com/kapitan-k/goerror"
	. "github.com/kapitan-k/gorocksdb"
	"github.com/kapitan-k/gorocksdb/extension/comparator"
	"github.com/kapitan-k/goutilities/event"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"strconv"
	"testing"
	"time"
)

var db *DB
var cfMessagesTotal, cfMessagesPerTopic *ColumnFamilyHandle

var topicCnt = 300
var eventPerTopicCnt = 30
var f = fuzz.New()

var tkeys [][]event.TopicEventEventKey
var tvalues [][][]byte

func TestMain(m *testing.M) {
	var dir string
	var err error
	{
		dir, err = ioutil.TempDir("", "EventMultiIterator")
		goerror.PanicOnError(err)
	}
	db, cfMessagesTotal, cfMessagesPerTopic, err = createDB(dir)
	goerror.PanicOnError(err)
	m.Run()

	db.Close()
}

func TestInsert(t *testing.T) {
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

	writeOptions := NewDefaultWriteOptions()

	// just a time
	timeSecs := time.Now().UTC().Unix() - 100000
	timeBegin := time.Now()

	// single routine insert takes a while
	batch := NewWriteBatch()
	// lets insert 1000 topics with 10000 events each reverse
	for i := topicCnt; i > 0; i-- {
		topicID := event.TopicID(i)
		//log.Println("inserting topicid ", topicID)
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
				[]*ColumnFamilyHandle{cfMessagesTotal, cfMessagesPerTopic},
				[][]byte{key, key}, [][]byte{{}, value})

			time.Sleep(time.Microsecond)
		}

		err = db.Write(writeOptions, batch)
		require.NoError(t, err)
		batch.Clear()
	}

	batch.Destroy()
	timeEnd := time.Now()
	log.Println("inserted in", timeEnd.Sub(timeBegin))
}

func TestGet(t *testing.T) {
	ro := NewDefaultReadOptions()
	for i, lkeys := range tkeys {
		for j, k := range lkeys {
			v, err := db.GetCF(ro, cfMessagesPerTopic, k.Slice())
			if len(v) == 0 {
				v = []byte{}
			}
			require.NoError(t, err)
			require.EqualValues(t, tvalues[i][j], v)
		}
	}
}

func TestTotalOrderIterator(t *testing.T) {
	ro := NewDefaultReadOptions()

	tek := event.TopicEventEventKey{}
	tek.TopicID = 6
	tek.EventID = event.EventIDMax
	ro.SetIterateUpperBound(tek.Slice())
	itr := db.NewIteratorCF(ro, cfMessagesPerTopic)
	defer itr.Close()
	tek.EventID = 0
	itr.Seek(tek.Slice())
	i := 0
	for ; itr.Valid(); itr.Next() {
		tek.FromSlice(itr.Key())
		require.EqualValues(t, tvalues[6-1][i], itr.Value())
		i++
	}
	require.Equal(t, int(eventPerTopicCnt), i)
}

func TestIterator(t *testing.T) {

	topicIDs := []event.TopicID{
		5, 6,
	}

	mitr, err := NewTopicEventMultiIteratorInit(
		db, cfMessagesPerTopic, 1024, 10, event.EventIDMax, topicIDs)
	require.NoError(t, err)
	defer mitr.Close()

	// test seeks
	mitr.Seek([]byte{})
	mitr.SeekToFirst()
	mitr.SeekToLast()

	mitr.SeekEventID(0)
	i := 0
	for ; mitr.Valid(); mitr.Next() {
		k, v := mitr.KeyValue()
		ek, _ := mitr.EventKeyValue()
		kek := mitr.EventKey()
		kk := mitr.Key()
		kv := mitr.Value()

		require.Equal(t, ek, kek)
		require.EqualValues(t, k, kk)
		require.EqualValues(t, v, kv)
		require.EqualValues(t, k, ek.Slice())

		p := i % 2
		tkey := tkeys[4+p][(i-p)/2]
		tvalue := tvalues[4+p][(i-p)/2]
		require.Equal(t, uint32(p), mitr.IteratorIndex())
		require.EqualValues(t, tkey, ek)
		require.EqualValues(t, tvalue, v)

		i++
	}
	mitr.Reset()
}

func createDB(path string) (
	db *DB,
	cfMessagesTotal, cfMessagesPerTopic *ColumnFamilyHandle,
	err error,
) {
	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	var cfs []*ColumnFamilyHandle
	db, cfs, err = OpenDbColumnFamilies(
		opts, path+"RocksEventExample",
		[]string{"default", "cf_messages_total", "cf_messages_per_topic"},
		createCFOpts())
	if err != nil {
		return
	}

	return db, cfs[1], cfs[2], nil
}

func createCFOpts() []*Options {

	optsTotal := NewDefaultOptions()
	comparator.OptionsSetDoubleUint64Comparator(0, optsTotal)
	optsTotal.SetPrefixExtractor(NewFixedPrefixTransform(int(8)))

	optsPerTopic := NewDefaultOptions()
	comparator.OptionsSetDoubleUint64Comparator(0, optsPerTopic)
	optsPerTopic.SetPrefixExtractor(NewFixedPrefixTransform(int(8)))

	return []*Options{NewDefaultOptions(), optsTotal, optsPerTopic}
}
