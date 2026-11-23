package pogreb

import (
	"errors"
	"sync"
)

// ErrIterationDone is returned by ItemIterator.Next calls when there are no more items to return.
var ErrIterationDone = errors.New("no more items in iterator")

type item struct {
	key   []byte
	value []byte
}

// ItemIterator is an iterator over DB key-value pairs.
// The iterator iterates over a snapshot of the database taken at the moment the iterator was created.
// Writes performed while iterating don't affect the items returned by the iterator.
// It iterates the items in an unspecified order.
type ItemIterator struct {
	items       []item
	nextItemIdx int
	err         error
	mu          sync.Mutex
}

// newItemIterator returns a new ItemIterator iterating over a snapshot of the database.
// The caller must hold the db.mu read lock.
func newItemIterator(db *DB) *ItemIterator {
	it := &ItemIterator{}
	for bucketIdx := uint32(0); bucketIdx < db.index.numBuckets; bucketIdx++ {
		if err := it.fetchItems(db, bucketIdx); err != nil {
			it.items = nil
			it.err = err
			return it
		}
	}
	return it
}

// fetchItems appends items from a bucket located at bucketIdx to the iterator snapshot.
func (it *ItemIterator) fetchItems(db *DB, bucketIdx uint32) error {
	bit := db.index.newBucketIterator(bucketIdx)
	for {
		b, err := bit.next()
		if err == ErrIterationDone {
			return nil
		}
		if err != nil {
			return err
		}
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				// No more items in the bucket.
				break
			}
			key, value, err := db.datalog.readKeyValue(sl)
			if err != nil {
				return err
			}
			key = cloneBytes(key)
			value = cloneBytes(value)
			it.items = append(it.items, item{key: key, value: value})
		}
	}
}

// Next returns the next key-value pair if available, otherwise it returns ErrIterationDone error.
func (it *ItemIterator) Next() ([]byte, []byte, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil {
		return nil, nil, it.err
	}

	if it.nextItemIdx < len(it.items) {
		item := it.items[it.nextItemIdx]
		it.nextItemIdx++
		return item.key, item.value, nil
	}

	return nil, nil, ErrIterationDone
}
