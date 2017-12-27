package gorocksdb

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenTransactionDb(t *testing.T) {
	db := newTestTransactionDB(t, "TestOpenTransactionDb", nil)
	defer db.Close()
}

func TestTransactionDBCRUD(t *testing.T) {
	db := newTestTransactionDB(t, "TestTransactionDBGet", nil)
	defer db.Close()

	_ = db.NewSnapshot()
	_, _ = db.NewCheckpoint()

	var (
		givenKey     = []byte("hello")
		givenVal1    = []byte("world1")
		givenVal2    = []byte("world2")
		givenTxnKey  = []byte("hello2")
		givenTxnKey2 = []byte("hello3")
		givenTxnVal1 = []byte("whatawonderful")
		wo           = NewDefaultWriteOptions()
		ro           = NewDefaultReadOptions()
		to           = NewDefaultTransactionOptions()
	)

	to.SetDeadlockDetect(true)
	to.SetExpiration(1024 * 1024)
	to.SetLockTimeout(1024 * 1024)
	to.SetDeadlockDetectDepth(10)
	to.SetMaxWriteBatchSize(1024 * 1024)
	defer to.Destroy()

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

	// transaction
	txn := db.TransactionBegin(wo, to, nil)
	defer txn.Destroy()
	txitr := txn.NewIterator(ro)
	txitr.Close()
	// create
	require.Nil(t, txn.Put(givenTxnKey, givenTxnVal1))
	v4, err := txn.Get(ro, givenTxnKey)
	defer CfreeByteSlice(v4)
	require.NoError(t, err)
	require.Equal(t, v4, givenTxnVal1)

	require.Nil(t, txn.Commit())
	v5, err := db.Get(ro, givenTxnKey)
	defer CfreeByteSlice(v5)
	require.NoError(t, err)
	require.Equal(t, v5, givenTxnVal1)

	// transaction
	txn2 := db.TransactionBegin(wo, to, nil)
	defer txn2.Destroy()
	// create
	require.Nil(t, txn2.Put(givenTxnKey2, givenTxnVal1))
	// rollback
	require.Nil(t, txn2.Rollback())

	v6, err := txn2.Get(ro, givenTxnKey2)
	defer CfreeByteSlice(v6)
	require.NoError(t, err)
	require.True(t, v6 == nil)
	// transaction
	txn3 := db.TransactionBegin(wo, to, nil)
	defer txn3.Destroy()
	// delete
	require.Nil(t, txn3.Delete(givenTxnKey))
	require.Nil(t, txn3.Commit())

	v7, err := db.Get(ro, givenTxnKey)
	defer CfreeByteSlice(v7)
	require.NoError(t, err)
	require.True(t, v7 == nil)

}

func newTestTransactionDB(t *testing.T, name string, applyOpts func(opts *Options, transactionDBOpts *TransactionDBOptions)) *TransactionDB {
	dir, err := ioutil.TempDir("", "gorockstransactiondb-"+name)
	require.NoError(t, err)

	opts := NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	transactionDBOpts := NewDefaultTransactionDBOptions()
	transactionDBOpts.SetDefaultLockTimeout(1024 * 1024)
	transactionDBOpts.SetMaxNumLocks(10)
	transactionDBOpts.SetNumStripes(10)
	transactionDBOpts.SetTransactionLockTimeout(1024)

	if applyOpts != nil {
		applyOpts(opts, transactionDBOpts)
	}
	db, err := OpenTransactionDb(opts, transactionDBOpts, dir)
	require.NoError(t, err)

	return db
}
