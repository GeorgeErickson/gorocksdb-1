package gorocksdb

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"strconv"
	"testing"
)

func TestOpenDb(t *testing.T) {
	db := newTestDB(t, "TestOpenDb", nil)
	defer db.Close()
	_ = db.UnsafeGetDB()
	_ = db.GetProperty("a")
	_ = db.GetApproximateSizes([]Range{{[]byte{}, []byte{}}})
	_ = db.GetLiveFilesMetaData()
}

func TestOpenDbError(t *testing.T) {
	newTestDBError(t, "TestOpenDb", nil)
}

func TestOpenDbDestroy(t *testing.T) {
	newTestDBDestroy(t, "TestOpenDbDestroy", nil)
}

func TestOpenDbReadOnly(t *testing.T) {
	db := newTestDBReadOnly(t, "TestOpenDb", nil)
	defer db.Close()
}

func TestOpenDbCfsReadOnly(t *testing.T) {
	db, cfs := newTestDBCFsReadOnly(t, "TestOpenDb", []string{"default", "bla"}, nil)
	defer db.Close()
	_ = db.GetPropertyCF("", cfs[0])
	_ = db.GetApproximateSizesCF(cfs[0], []Range{{[]byte{}, []byte{}}})
	db.CompactRangeCF(cfs[0], Range{[]byte{}, []byte{}})
	_ = db.NewIteratorCF(NewDefaultReadOptions(), cfs[0])
}

func TestOpenDbFail(t *testing.T) {
	newTestDBCFsWrongOptsCnt(t, "TestOpenDbFail", []string{"default", "x"}, nil)
	newTestDBCFsNoDefault(t, "TestOpenDbFail")
	newTestDBCFsReadOnlyWrongOptsCnt(t, "TestOpenDbFailCnt", []string{"default", "x"}, nil)
}

func TestDBCRUD(t *testing.T) {
	db := newTestDB(t, "TestDBGet", nil)
	defer db.Close()

	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		wo        = NewDefaultWriteOptions()
		ro        = NewDefaultReadOptions()
	)

	wo.DisableWAL(false)
	wo.SetSync(true)

	// set the read options
	ro.SetFillCache(true)
	ro.SetTailing(true)
	ro.SetPinData(true)
	ro.SetReadaheadSize(1024)
	ro.SetVerifyChecksums(true)
	ro.SetIterateUpperBound([]byte("zzzzzzzzzzzzzzzzzzzzzzzzzzzz"))
	ro.SetReadTier(ReadAllTier)
	ro.UnsafeGetReadOptions()

	// create
	require.Nil(t, db.Put(wo, givenKey, givenVal1))

	// retrieve
	v1, err := db.Get(ro, givenKey)
	require.NoError(t, err)
	require.Equal(t, v1, givenVal1)
	CfreeByteSlice(v1)

	v1, err = db.GetBytes(ro, givenKey)
	require.NoError(t, err)
	require.Equal(t, v1, givenVal1)

	// update
	require.Nil(t, db.Put(wo, givenKey, givenVal2))
	v2, err := db.Get(ro, givenKey)
	defer CfreeByteSlice(v2)
	require.NoError(t, err)
	require.Equal(t, v2, givenVal2)

	// delete
	require.Nil(t, db.Delete(wo, givenKey))
	v3, err := db.Get(ro, givenKey)
	require.NoError(t, err)
	require.True(t, v3 == nil)

}

func TestOpts(t *testing.T) {
	cache := NewLRUCache(1024)
	_ = cache.GetPinnedUsage()
	_ = cache.GetUsage()
	cache.Destroy()

	db := newTestDB(t, "TestOpts", func(opts *Options) {
		uco := NewDefaultUniversalCompactionOptions()
		uco.SetCompressionSizePercent(80)
		uco.SetMaxMergeWidth(1024)
		uco.SetMaxSizeAmplificationPercent(400)
		uco.SetMinMergeWidth(3)
		uco.SetSizeRatio(3)
		uco.SetStopStyle(CompactionStopStyleTotalSize)
		defer uco.Destroy()

		fco := NewDefaultFIFOCompactionOptions()
		fco.SetMaxTableFilesSize(1024 * 1024)
		defer fco.Destroy()

		envOpts := NewDefaultEnvOptions()
		defer envOpts.Destroy()

		env := NewDefaultEnv()
		env.SetBackgroundThreads(1)
		env.SetHighPriorityBackgroundThreads(1)

		co := NewDefaultFIFOCompactionOptions()
		co.SetMaxTableFilesSize(1024 * 1024)

		defer env.Destroy()
		opts.SetEnv(env)

		opts.SetUniversalCompactionOptions(uco)
		opts.SetFIFOCompactionOptions(fco)

		opts.EnableStatistics()
		opts.IncreaseParallelism(5)
		opts.OptimizeForPointLookup(4)
		opts.OptimizeLevelStyleCompaction(1024 * 1024)
		opts.OptimizeUniversalStyleCompaction(1024 * 1024)
		opts.PrepareForBulkLoad()
		opts.SetAccessHintOnCompactionStart(NoneCompactionAccessPattern)
		opts.SetAdviseRandomOnOpen(true)
		opts.SetAllowConcurrentMemtableWrites(true)
		opts.SetAllowMmapReads(true)
		opts.SetAllowMmapWrites(true)
		opts.SetArenaBlockSize(1024 * 1024)
		opts.SetBloomLocality(0)
		opts.SetBytesPerSync(1024)
		opts.SetCreateIfMissing(true)
		opts.SetCreateIfMissingColumnFamilies(true)
		opts.SetCompression(Bz2Compression)
		opts.SetDbWriteBufferSize(1024 * 1024)
		opts.SetDisableAutoCompactions(true)
		opts.SetErrorIfExists(true)
		opts.SetFIFOCompactionOptions(co)
		opts.SetHardRateLimit(256)
		opts.SetInfoLogLevel(WarnInfoLogLevel)
		opts.SetInplaceUpdateNumLocks(8)
		opts.SetInplaceUpdateSupport(false)
		opts.SetIsFdCloseOnExec(true)
		opts.SetLevel0FileNumCompactionTrigger(8)
		opts.SetLevel0SlowdownWritesTrigger(4)
		opts.SetLevel0FileNumCompactionTrigger(512)
		opts.SetLogFileTimeToRoll(10)
		opts.SetManifestPreallocationSize(1024)
		opts.SetKeepLogFileNum(6)
		opts.SetMaxBackgroundCompactions(6)
		opts.SetMaxBackgroundFlushes(4)
		opts.SetMaxCompactionBytes(1024 * 1024 * 1024)
		opts.SetMaxOpenFiles(1024)
		opts.SetMaxSuccessiveMerges(1024)
		opts.SetMaxFileOpeningThreads(1024)
		opts.SetMaxMemCompactionLevel(1024)
		opts.SetMaxTotalWalSize(1024 * 1024 * 1024)
		opts.SetMaxWriteBufferNumber(1024)
		opts.SetMaxBytesForLevelBase(1024 * 1024 * 256)
		opts.SetMaxBytesForLevelMultiplier(4)
		opts.SetMaxSequentialSkipInIterations(1024)
		opts.SetMaxBytesForLevelMultiplierAdditional([]int{11, 12, 13})
		opts.SetRateLimitDelayMaxMilliseconds(1024)
		opts.SetUseAdaptiveMutex(true)
		opts.SetCompressionOptions(NewDefaultCompressionOptions())
		opts.SetCompressionOptions(NewCompressionOptions(10, 1, 0, 1))

		opts.SetMinLevelToCompress(3)
		opts.SetMinWriteBufferNumberToMerge(100)
		opts.SetSoftRateLimit(1024)
		opts.SetStatsDumpPeriodSec(1024)
		opts.SetUseDirectReads(false)
		opts.SetPurgeRedundantKvsWhileFlush(true)

		opts.SetParanoidChecks(true)
		opts.SetTableCacheRemoveScanCountLimit(1024)
	})
	defer db.Close()

}

func TestDBCRUDDBPaths(t *testing.T) {
	names := make([]string, 4)
	targetSizes := make([]uint64, len(names))

	for i := range names {
		names[i] = "TestDBGet_" + strconv.FormatInt(int64(i), 10)
		targetSizes[i] = uint64(1024 * 1024 * (i + 1))
	}

	db := newTestDBPathNames(t, "TestDBGet", names, targetSizes, nil)
	defer db.Close()

	var (
		givenKey  = []byte("hello")
		givenVal1 = []byte("world1")
		givenVal2 = []byte("world2")
		wo        = NewDefaultWriteOptions()
		ro        = NewDefaultReadOptionsSetupQuick(true, true, true, nil, 1024, true)
	)

	// create
	require.Nil(t, db.Put(wo, givenKey, givenVal1))

	// retrieve
	v1, err := db.Get(ro, givenKey)
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.Equal(t, v1, givenVal1)

	// update
	require.Nil(t, db.Put(wo, givenKey, givenVal2))
	v2, err := db.Get(ro, givenKey)
	defer CfreeByteSlice(v2)
	require.NoError(t, err)
	require.Equal(t, v2, givenVal2)

	// delete
	require.Nil(t, db.Delete(wo, givenKey))
	v3, err := db.Get(ro, givenKey)
	defer CfreeByteSlice(v3)
	require.NoError(t, err)
	require.True(t, v3 == nil)
}

func newTestDB(t *testing.T, name string, applyOpts func(opts *Options)) *DB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)

	return db
}

func newTestDBPrefixTransform(t *testing.T, name string, applyOpts func(opts *Options)) *DB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	nst := NewFixedPrefixTransform(3)
	opts.SetPrefixExtractor(nst)

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)

	return db
}

func newTestDBDestroy(t *testing.T, name string, applyOpts func(opts *Options)) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)
	db.Close()
	DestroyDb(dir, opts)
}

func newTestDBError(t *testing.T, name string, applyOpts func(opts *Options)) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	if applyOpts != nil {
		applyOpts(opts)
	}
	_, err = OpenDb(opts, dir)
	require.Error(t, err)
}

func newTestDBReadOnly(t *testing.T, name string, applyOpts func(opts *Options)) *DB {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}

	db, err := OpenDb(opts, dir)
	require.NoError(t, err)
	db.Close()

	db, err = OpenDbForReadOnly(opts, dir, false)
	require.NoError(t, err)
	rateLimiter.Destroy()

	return db
}

func newTestDBCFs(t *testing.T, name string, cfNames []string, applyOpts func(opts *Options)) (*DB, []*ColumnFamilyHandle) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	if applyOpts != nil {
		applyOpts(opts)
	}

	cfOpts := make([]*Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = NewDefaultOptions()
	}

	db, cfs, err := OpenDbColumnFamilies(opts, dir, cfNames, cfOpts)
	require.NoError(t, err)

	return db, cfs
}

func newTestDBCFsWrongOptsCnt(t *testing.T, name string, cfNames []string, applyOpts func(opts *Options)) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	if applyOpts != nil {
		applyOpts(opts)
	}

	cfOpts := make([]*Options, len(cfNames)-1)
	for i := range cfOpts {
		cfOpts[i] = NewDefaultOptions()
	}

	_, _, err = OpenDbColumnFamilies(opts, dir, cfNames, cfOpts)
	require.Error(t, err)
}

func newTestDBCFsNoDefault(t *testing.T, name string) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfNames := []string{"cf1", "cf2"}
	cfOpts := make([]*Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = NewDefaultOptions()
	}

	_, _, err = OpenDbColumnFamilies(opts, dir, cfNames, cfOpts)
	require.Error(t, err)
}

func newTestDBCFsReadOnly(t *testing.T, name string, cfNames []string, applyOpts func(opts *Options)) (*DB, []*ColumnFamilyHandle) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	if applyOpts != nil {
		applyOpts(opts)
	}

	cfOpts := make([]*Options, len(cfNames))
	for i := range cfOpts {
		cfOpts[i] = NewDefaultOptions()
	}

	db, cfs, err := OpenDbColumnFamilies(opts, dir, cfNames, cfOpts)
	require.NoError(t, err)
	db.Close()

	db, cfs, err = OpenDbForReadOnlyColumnFamilies(opts, dir, cfNames, cfOpts, false)
	require.NoError(t, err)

	return db, cfs
}

func newTestDBCFsReadOnlyWrongOptsCnt(t *testing.T, name string, cfNames []string, applyOpts func(opts *Options)) {
	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()

	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	if applyOpts != nil {
		applyOpts(opts)
	}

	cfOpts := make([]*Options, len(cfNames)-1)
	for i := range cfOpts {
		cfOpts[i] = NewDefaultOptions()
	}

	_, _, err = OpenDbForReadOnlyColumnFamilies(opts, dir, cfNames, cfOpts, false)
	require.Error(t, err)
}

func newTestDBPathNames(t *testing.T, name string, names []string, target_sizes []uint64, applyOpts func(opts *Options)) *DB {
	require.Equal(t, len(target_sizes), len(names))
	require.NotEqual(t, len(names), 0)

	dir, err := ioutil.TempDir("", "gorocksdb-"+name)
	require.NoError(t, err)

	paths := make([]string, len(names))
	for i, name := range names {
		dir, err := ioutil.TempDir("", "gorocksdb-"+name)
		require.NoError(t, err)
		paths[i] = dir
	}

	dbpaths := NewDBPathsFromData(paths, target_sizes)
	defer DestroyDBPaths(dbpaths)

	opts := NewDefaultOptions()
	opts.SetDBPaths(dbpaths)
	// test the ratelimiter
	rateLimiter := NewRateLimiter(1024, 100*1000, 10)
	opts.SetRateLimiter(rateLimiter)
	opts.SetCreateIfMissing(true)
	if applyOpts != nil {
		applyOpts(opts)
	}
	db, err := OpenDb(opts, dir)
	require.NoError(t, err)

	return db
}
