package pogreb

import (
	"testing"

	"github.com/akrylysov/pogreb/fs"
	"github.com/akrylysov/pogreb/internal/assert"
)

type syncCountingFile struct {
	fs.File
	syncCount *int
}

func (f *syncCountingFile) Sync() error {
	*f.syncCount++
	return f.File.Sync()
}

func TestDeleteDistinguishesMissingAndDeleted(t *testing.T) {
	db, err := createTestDB(&Options{FileSystem: testFS, BackgroundSyncInterval: -1})
	assert.Nil(t, err)

	deleted, err := db.Delete([]byte("missing-before-batch"))
	assert.Nil(t, err)
	assert.Equal(t, false, deleted)

	assert.Nil(t, db.Put([]byte("present"), []byte("value")))

	deleted, err = db.Delete([]byte("present"))
	assert.Nil(t, err)
	assert.Equal(t, true, deleted)

	deleted, err = db.Delete([]byte("present"))
	assert.Nil(t, err)
	assert.Equal(t, false, deleted)

	assert.Nil(t, db.Put([]byte("present"), []byte("value")))
	results, err := db.DeleteBatchResults([][]byte{[]byte("present")})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, true, results[0].Deleted)
	assert.Nil(t, results[0].Err)

	results, err = db.DeleteBatchResults([][]byte{[]byte("present")})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, false, results[0].Deleted)
	assert.Nil(t, results[0].Err)

	assert.Nil(t, db.Close())
}

func TestDeleteBatchMixedResultsOrderAndSingleSync(t *testing.T) {
	opts := &Options{FileSystem: testFS, BackgroundSyncInterval: -1}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	firstMissing, err := db.Delete([]byte("missing-first"))
	assert.Nil(t, err)
	assert.Equal(t, false, firstMissing)

	keys := [][]byte{[]byte("key-a"), []byte("key-b"), []byte("key-c")}
	for _, key := range keys {
		assert.Nil(t, db.Put(key, key))
	}

	batchKeys := [][]byte{
		keys[0],
		[]byte("missing-batch-1"),
		keys[2],
		keys[1],
		[]byte("missing-batch-2"),
	}
	expectedDeleted := []bool{true, false, true, true, false}

	syncCount := 0
	oldFile := db.datalog.curSeg.File
	db.datalog.curSeg.File = &syncCountingFile{File: oldFile, syncCount: &syncCount}

	results, err := db.DeleteBatchResults(batchKeys)
	db.datalog.curSeg.File = oldFile
	assert.Nil(t, err)
	assert.Equal(t, len(batchKeys), len(results))
	for i, expected := range expectedDeleted {
		assert.Equal(t, expected, results[i].Deleted)
		assert.Nil(t, results[i].Err)
	}
	assert.Equal(t, 1, syncCount)

	deleted, err := db.DeleteBatch(batchKeys)
	assert.Nil(t, err)
	assert.Equal(t, []bool{false, false, false, false, false}, deleted)

	assert.Nil(t, db.Close())

	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	defer func() { _ = db.Close() }()

	for _, key := range keys {
		has, err := db.Has(key)
		assert.Nil(t, err)
		assert.Equal(t, false, has)
	}
}

func TestDeleteBatchPreservesPerKeyErrors(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	assert.Nil(t, db.Put(nil, nil))

	errorFile := &errfile{}
	oldFile := db.datalog.segments[0].File
	db.datalog.segments[0].File = errorFile

	batchKeys := [][]byte{nil, []byte("missing")}
	results, err := db.DeleteBatchResults(batchKeys)
	assert.Equal(t, errfileError, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, false, results[0].Deleted)
	assert.Equal(t, errfileError, results[0].Err)
	assert.Equal(t, false, results[1].Deleted)
	assert.Nil(t, results[1].Err)

	deleted, err := db.DeleteBatch(batchKeys)
	assert.Equal(t, errfileError, err)
	assert.Equal(t, []bool{false, false}, deleted)

	singleDeleted, err := db.Delete(nil)
	assert.Equal(t, errfileError, err)
	assert.Equal(t, false, singleDeleted)

	db.datalog.segments[0].File = oldFile
	assert.Nil(t, db.Close())
}
