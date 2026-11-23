package pogreb

import (
	"bytes"
	"testing"

	"github.com/akrylysov/pogreb/internal/assert"
)

func TestIteratorEmpty(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)
	it := db.Items()
	for i := 0; i < 8; i++ {
		_, _, err := it.Next()
		if err != ErrIterationDone {
			t.Fatalf("expected %v; got %v", ErrIterationDone, err)
		}
	}
	assert.Nil(t, db.Close())
}

func TestIteratorSnapshot(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	var i byte
	for i = 0; i < 255; i++ {
		assert.Nil(t, db.Put([]byte{i}, []byte{i}))
	}

	it := db.Items()

	// Writes performed after the iterator was created don't affect the iteration.
	assert.Nil(t, db.Put([]byte{0}, []byte{255}))   // Overwrite an existing key.
	assert.Nil(t, db.Put([]byte{255}, []byte{255})) // Insert a new key.
	assert.Nil(t, db.Delete([]byte{1}))             // Delete an existing key.

	items := map[byte]bool{}
	for {
		key, value, err := it.Next()
		if err == ErrIterationDone {
			break
		}
		assert.Nil(t, err)
		if !bytes.Equal(key, value) {
			t.Fatalf("expected %v; got %v", key, value)
		}
		items[key[0]] = true
	}

	if len(items) != 255 {
		t.Fatalf("expected to iterate over 255 keys; got %v", len(items))
	}
	for i = 0; i < 255; i++ {
		if !items[i] {
			t.Fatalf("expected to iterate over key %v", i)
		}
	}

	assert.Nil(t, db.Close())
}

func TestIterator(t *testing.T) {
	db, err := createTestDB(nil)
	assert.Nil(t, err)

	items := map[byte]bool{}
	var i byte
	for i = 0; i < 255; i++ {
		items[i] = false
		err := db.Put([]byte{i}, []byte{i})
		assert.Nil(t, err)
	}

	it := db.Items()
	for {
		key, value, err := it.Next()
		if err == ErrIterationDone {
			break
		}
		assert.Nil(t, err)
		if k, ok := items[key[0]]; !ok {
			t.Fatalf("unknown key %v", k)
		}
		if !bytes.Equal(key, value) {
			t.Fatalf("expected %v; got %v", key, value)
		}
		items[key[0]] = true
	}

	for k, v := range items {
		if !v {
			t.Fatalf("expected to iterate over key %v", k)
		}
	}

	for i := 0; i < 8; i++ {
		_, _, err := it.Next()
		if err != ErrIterationDone {
			t.Fatalf("expected %v; got %v", ErrIterationDone, err)
		}
	}

	assert.Nil(t, db.Close())
}
