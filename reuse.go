package pogreb

// reclaimObsoleteRecords replaces records that are no longer referenced by the
// index with free records. It runs once when a segment becomes sufficiently
// fragmented and after crash recovery.
func (db *DB) reclaimObsoleteRecords() error {
	live, err := db.index.liveLocations()
	if err != nil {
		return err
	}

	for _, seg := range db.datalog.segmentsBySequenceID() {
		it, err := newSegmentIterator(seg)
		if err != nil {
			return err
		}
		for {
			rec, err := it.next()
			if err == ErrIterationDone {
				break
			}
			if err != nil {
				return err
			}
			if rec.rtype == recordTypeFree {
				continue
			}
			if rec.rtype == recordTypePut {
				loc := recordLocation{segmentID: rec.segmentID, offset: rec.offset}
				if _, ok := live[loc]; ok {
					continue
				}
			}
			if err := db.datalog.markFreeRange(seg, rec.offset, uint32(len(rec.data))); err != nil {
				return err
			}
			seg.addFreeBlock(rec.offset, uint32(len(rec.data)))
		}
	}
	return nil
}

func (db *DB) maybeEnableReuse() error {
	if db.datalog.reuse || !db.datalog.shouldReuse() {
		return nil
	}
	db.datalog.reuse = true
	if err := db.reclaimObsoleteRecords(); err != nil {
		db.datalog.reuse = false
		return err
	}
	return nil
}
