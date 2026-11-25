package pogreb

// DeleteResult is the result of deleting one key as part of a batch.
type DeleteResult struct {
	// Deleted is true only when the key existed and was removed successfully.
	Deleted bool

	// Err is the error for this key. A nil error and Deleted false mean the key was absent.
	Err error
}

// Delete removes the given key from the DB.
//
// It reports whether the key existed and was deleted. A false result with a nil
// error means the key was not present; a non-nil error describes a failed delete.
func (db *DB) Delete(key []byte) (bool, error) {
	if len(key) > MaxKeyLength {
		return false, errKeyTooLarge
	}

	db.metrics.Dels.Add(1)
	h := db.hash(key)
	db.mu.Lock()
	defer db.mu.Unlock()

	deleted, err := db.del(h, key, true)
	if err != nil {
		return false, err
	}
	if db.syncWrites {
		if err := db.sync(); err != nil {
			return false, err
		}
	}
	return deleted, nil
}

// DeleteBatch deletes keys in the provided order. The returned slice has the
// same length and order as keys.
//
// All index and write-ahead-log changes happen under one write lock. When
// synchronous writes are enabled, the batch is flushed once, not once per key.
// Per-key errors are available from DeleteBatchResults; the returned error is
// the first per-key or flush error.
func (db *DB) DeleteBatch(keys [][]byte) ([]bool, error) {
	results, err := db.DeleteBatchResults(keys)
	deleted := make([]bool, len(results))
	for i, result := range results {
		deleted[i] = result.Err == nil && result.Deleted
	}
	return deleted, err
}

// DeleteBatchResults is like DeleteBatch, but preserves each key's error.
func (db *DB) DeleteBatchResults(keys [][]byte) ([]DeleteResult, error) {
	db.metrics.Dels.Add(int64(len(keys)))
	db.mu.Lock()
	defer db.mu.Unlock()

	results := make([]DeleteResult, len(keys))
	var firstErr error
	recordError := func(index int, err error) {
		results[index] = DeleteResult{Err: err}
		if firstErr == nil {
			firstErr = err
		}
	}

	wrote := false
	for i, key := range keys {
		if len(key) > MaxKeyLength {
			recordError(i, errKeyTooLarge)
			continue
		}
		deleted, err := db.del(db.hash(key), key, true)
		if err != nil {
			recordError(i, err)
			continue
		}
		wrote = true
		results[i] = DeleteResult{Deleted: deleted}
	}

	if wrote && db.syncWrites {
		if err := db.sync(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			for i := range results {
				if results[i].Err == nil {
					results[i] = DeleteResult{Err: err}
				}
			}
		}
	}

	return results, firstErr
}
