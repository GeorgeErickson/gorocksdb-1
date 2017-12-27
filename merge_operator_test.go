package gorocksdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeOperator(t *testing.T) {
	var (
		givenKey    = []byte("hello")
		givenVal1   = []byte("foo")
		givenVal2   = []byte("bar")
		givenMerged = []byte("foobar")
	)
	merger := &mockMergeOperator{
		fullMerge: func(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
			require.Equal(t, key, givenKey)
			require.Equal(t, existingValue, givenVal1)
			require.Equal(t, operands, [][]byte{givenVal2})
			return givenMerged, true
		},
		partialMerge: func(key, leftOperand, rightOperand []byte) ([]byte, bool) {
			return append(leftOperand, rightOperand...), true
		},
	}
	db := newTestDB(t, "TestMergeOperator", func(opts *Options) {
		opts.SetMergeOperator(merger)
	})
	defer db.Close()

	wo := NewDefaultWriteOptions()
	require.Nil(t, db.Put(wo, givenKey, givenVal1))
	require.Nil(t, db.Merge(wo, givenKey, givenVal2))

	// trigger a compaction to ensure that a merge is performed
	db.CompactRange(Range{nil, nil})

	ro := NewDefaultReadOptions()
	v1, err := db.Get(ro, givenKey)
	defer CfreeByteSlice(v1)
	require.NoError(t, err)
	require.Equal(t, v1, givenMerged)
}

type mockMergeOperator struct {
	fullMerge    func(key, existingValue []byte, operands [][]byte) ([]byte, bool)
	partialMerge func(key, leftOperand, rightOperand []byte) ([]byte, bool)
}

func (m *mockMergeOperator) Name() string { return "gorocksdb.test" }
func (m *mockMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	return m.fullMerge(key, existingValue, operands)
}
func (m *mockMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return m.partialMerge(key, leftOperand, rightOperand)
}
