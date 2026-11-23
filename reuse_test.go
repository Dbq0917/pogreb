package pogreb

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func reuseTestKey(i uint32) []byte {
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, i)
	return key
}

func reuseTestValue(tag byte) []byte {
	value := make([]byte, 64)
	for i := range value {
		value[i] = tag + byte(i)
	}
	return value
}

func TestDeletedSpaceReuse(t *testing.T) {
	const (
		numKeys  = 5000
		numDels  = 3500
		numNew   = 3500
		firstNew = numKeys
	)

	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := uint32(0); i < numKeys; i++ {
		assert.Nil(t, db.Put(reuseTestKey(i), reuseTestValue(byte(i))))
	}
	sizeBefore, err := db.FileSize()
	assert.Nil(t, err)

	for i := uint32(0); i < numDels; i++ {
		assert.Nil(t, db.Delete(reuseTestKey(i)))
	}
	for i := uint32(0); i < numNew; i++ {
		assert.Nil(t, db.Put(reuseTestKey(firstNew+i), reuseTestValue(byte(i))))
	}

	sizeAfter, err := db.FileSize()
	assert.Nil(t, err)
	if sizeAfter > sizeBefore*11/10 {
		t.Fatalf("deleted space was not reused: file size grew from %d to %d", sizeBefore, sizeAfter)
	}

	for i := uint32(0); i < numDels; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Nil(t, value)
	}
	for i := uint32(numDels); i < numKeys; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	for i := uint32(0); i < numNew; i++ {
		value, err := db.Get(reuseTestKey(firstNew + i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}

	assert.Nil(t, db.Close())
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())
	assert.Nil(t, db.Close())

	// Simulate a crash so the index is rebuilt from free and live records.
	assert.Nil(t, touchFile(testFS, filepath.Join(testDBName, lockName)))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, segmentMetaName(0, 1))))
	assert.Nil(t, testFS.Remove(filepath.Join(testDBName, indexMetaName)))

	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())
	for i := uint32(numDels); i < numKeys; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	for i := uint32(0); i < numNew; i++ {
		value, err := db.Get(reuseTestKey(firstNew + i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	assert.Nil(t, db.Close())
}

func TestDeletedSpaceReuseRepeated(t *testing.T) {
	const (
		numKeys   = 3000
		numDels   = 2100
		numNew    = 2100
		numRounds = 3
	)

	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	live := make([]uint32, 0, numKeys)
	for i := uint32(0); i < numKeys; i++ {
		live = append(live, i)
		assert.Nil(t, db.Put(reuseTestKey(i), reuseTestValue(byte(i))))
	}
	sizeBefore, err := db.FileSize()
	assert.Nil(t, err)

	nextKey := uint32(numKeys)
	for round := 0; round < numRounds; round++ {
		for i := 0; i < numDels; i++ {
			assert.Nil(t, db.Delete(reuseTestKey(live[i])))
		}
		live = append(live[:0], live[numDels:]...)
		for i := 0; i < numNew; i++ {
			key := nextKey
			nextKey++
			live = append(live, key)
			assert.Nil(t, db.Put(reuseTestKey(key), reuseTestValue(byte(key))))
		}
	}

	sizeAfter, err := db.FileSize()
	assert.Nil(t, err)
	if sizeAfter > sizeBefore*5/4 {
		t.Fatalf("deleted space was not reused across rounds: file size grew from %d to %d", sizeBefore, sizeAfter)
	}
	assert.Equal(t, uint32(numKeys), db.Count())
	for _, key := range live {
		value, err := db.Get(reuseTestKey(key))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(key)), value)
	}

	assert.Nil(t, db.Close())
}

func TestDeletedSpaceReuseCompaction(t *testing.T) {
	const (
		numKeys  = 1000
		numDels  = 700
		numNew   = 700
		firstNew = numKeys
	)

	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := uint32(0); i < numKeys; i++ {
		assert.Nil(t, db.Put(reuseTestKey(i), reuseTestValue(byte(i))))
	}
	for i := uint32(0); i < numDels; i++ {
		assert.Nil(t, db.Delete(reuseTestKey(i)))
	}
	for i := uint32(0); i < numNew; i++ {
		assert.Nil(t, db.Put(reuseTestKey(firstNew+i), reuseTestValue(byte(i))))
	}

	_, err = db.Compact()
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())

	for i := uint32(numDels); i < numKeys; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	for i := uint32(0); i < numNew; i++ {
		value, err := db.Get(reuseTestKey(firstNew + i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}

	assert.Nil(t, db.Close())
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())
	assert.Nil(t, db.Close())
}

func TestDeletedSpaceReuseMultiSegment(t *testing.T) {
	const (
		numKeys  = 2000
		numDels  = 1400
		numNew   = 1400
		firstNew = numKeys
	)

	opts := &Options{
		FileSystem:     testFS,
		maxSegmentSize: 4096,
	}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := uint32(0); i < numKeys; i++ {
		assert.Nil(t, db.Put(reuseTestKey(i), reuseTestValue(byte(i))))
	}
	sizeBefore, err := db.FileSize()
	assert.Nil(t, err)
	if countSegments(t, db) < 10 {
		t.Fatal("test requires a multi-segment database")
	}

	for i := uint32(0); i < numDels; i++ {
		assert.Nil(t, db.Delete(reuseTestKey(i)))
	}
	for i := uint32(0); i < numNew; i++ {
		assert.Nil(t, db.Put(reuseTestKey(firstNew+i), reuseTestValue(byte(i))))
	}

	sizeAfter, err := db.FileSize()
	assert.Nil(t, err)
	if sizeAfter > sizeBefore*3/2 {
		t.Fatalf("deleted space was not reused across segments: file size grew from %d to %d", sizeBefore, sizeAfter)
	}

	assert.Nil(t, db.Close())
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())
	for i := uint32(numDels); i < numKeys; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	for i := uint32(0); i < numNew; i++ {
		value, err := db.Get(reuseTestKey(firstNew + i))
		assert.Nil(t, err)
		assert.Equal(t, reuseTestValue(byte(i)), value)
	}
	assert.Nil(t, db.Close())
}

func TestDeletedSpaceReuseVariableSizes(t *testing.T) {
	const (
		numKeys  = 1500
		numDels  = 1050
		numNew   = 1050
		firstNew = numKeys
	)

	valueFor := func(i uint32, multiplier uint32) []byte {
		size := 1 + int((i*multiplier)%220)
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(i + uint32(j))
		}
		return value
	}

	opts := &Options{FileSystem: testFS}
	db, err := createTestDB(opts)
	assert.Nil(t, err)

	for i := uint32(0); i < numKeys; i++ {
		assert.Nil(t, db.Put(reuseTestKey(i), valueFor(i, 37)))
	}
	for i := uint32(0); i < numDels; i++ {
		assert.Nil(t, db.Delete(reuseTestKey(i)))
	}
	for i := uint32(0); i < numNew; i++ {
		key := firstNew + i
		assert.Nil(t, db.Put(reuseTestKey(key), valueFor(i, 53)))
	}

	for i := uint32(numDels); i < numKeys; i++ {
		value, err := db.Get(reuseTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, valueFor(i, 37), value)
	}
	for i := uint32(0); i < numNew; i++ {
		key := reuseTestKey(firstNew + i)
		value, err := db.Get(key)
		assert.Nil(t, err)
		expected := valueFor(i, 53)
		if !bytes.Equal(expected, value) {
			sl, _, _ := db.findSlot(db.hash(key), key)
			t.Fatalf("i=%d key=%d slot=%+v expected=%v got=%v", i, firstNew+i, sl, expected, value)
		}
	}

	assert.Nil(t, db.Close())
	db, err = Open(testDBName, opts)
	assert.Nil(t, err)
	assert.Equal(t, uint32(numKeys), db.Count())
	assert.Nil(t, db.Close())
}
