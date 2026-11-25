package pogreb

import (
	"fmt"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func batchTestOptions() *Options {
	return &Options{
		maxSegmentSize:             2048,
		compactionMinSegmentSize:   1,
		compactionMinFragmentation: 0.01,
	}
}

func seedBatchEquivalenceDB(t *testing.T, db *DB) {
	t.Helper()
	for i := 0; i < 600; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		assert.Nil(t, db.Put(key, []byte("v1")))
	}
	for i := 0; i < 450; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		assert.Nil(t, db.Put(key, []byte("v2-longer-value")))
	}
	for i := 0; i < 150; i += 3 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		assert.Nil(t, db.Delete(key))
	}
}

func dumpBatchItems(t *testing.T, db *DB) map[string]string {
	t.Helper()
	items := make(map[string]string)
	it := db.Items()
	for {
		key, value, err := it.Next()
		if err == ErrIterationDone {
			break
		}
		assert.Nil(t, err)
		items[string(key)] = string(value)
	}
	return items
}

func compactSequentially(t *testing.T, db *DB) {
	t.Helper()
	db.mu.RLock()
	segments := db.pickForCompaction()
	db.mu.RUnlock()
	for _, seg := range segments {
		_, err := db.compactSequential(seg)
		assert.Nil(t, err)
	}
}

func TestBatchCompactionEquivalentToSingleRecord(t *testing.T) {
	opts := batchTestOptions()
	db, err := createTestDB(opts)
	assert.Nil(t, err)
	seedBatchEquivalenceDB(t, db)
	compactSequentially(t, db)
	single := dumpBatchItems(t, db)
	singleCount := db.Count()
	assert.Nil(t, db.Close())

	opts = batchTestOptions()
	db, err = createTestDB(opts)
	assert.Nil(t, err)
	seedBatchEquivalenceDB(t, db)
	cr, err := db.Compact()
	assert.Nil(t, err)
	if cr.CompactedSegments == 0 {
		t.Fatal("expected at least one compacted segment")
	}
	batch := dumpBatchItems(t, db)
	batchCount := db.Count()

	assert.Equal(t, singleCount, batchCount)
	assert.Equal(t, len(single), int(batchCount))
	assert.Equal(t, single, batch)
	assert.Nil(t, db.Close())
}

func TestCompactionFailureCanReenter(t *testing.T) {
	opts := batchTestOptions()
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		assert.Nil(t, db.Put(key, []byte("old-value")))
	}
	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		assert.Nil(t, db.Put(key, []byte("new-value")))
	}

	db.mu.RLock()
	seg := db.datalog.segments[0]
	db.mu.RUnlock()
	if seg == nil {
		t.Fatal("expected segment 0")
	}

	oldFile := seg.File
	seg.File = &errfile{}
	_, err = db.compact(seg)
	if err == nil {
		t.Fatal("expected injected read failure")
	}
	seg.File = oldFile

	cr, err := db.Compact()
	assert.Nil(t, err)
	if cr.CompactedSegments == 0 {
		t.Fatal("retry did not compact any segment")
	}

	for i := 0; i < 400; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, "new-value", string(value))
	}
	for i := 400; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value, err := db.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, "old-value", string(value))
	}
	assert.Nil(t, db.Close())
}

func TestBatchCompactionConcurrentReadWrite(t *testing.T) {
	opts := batchTestOptions()
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := 0; i < 500; i++ {
		assert.Nil(t, db.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte("stable")))
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := []byte(fmt.Sprintf("key-%04d", i%500))
			value, err := db.Get(key)
			if err != nil {
				t.Error(err)
				return
			}
			if string(value) != "stable" {
				t.Errorf("unexpected value %q", value)
				return
			}
		}
	}()

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := []byte(fmt.Sprintf("write-%06d", i))
			if err := db.Put(key, []byte("concurrent")); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	if _, err := db.Compact(); err != nil && err != errBusy {
		t.Fatal(err)
	}
	close(stop)
	<-done
	<-writeDone
	assert.Nil(t, db.Close())
}
