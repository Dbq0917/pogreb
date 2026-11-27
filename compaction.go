package pogreb

import (
	"github.com/akrylysov/pogreb/internal/errors"
)

// compactionBatchSize is the maximum number of records processed
// under a single lock acquisition during compaction.
const compactionBatchSize = 1024

// bucketRef identifies an index bucket on disk.
type bucketRef struct {
	file   *file
	offset int64
}

// bucketBatch caches index buckets accessed while promoting a batch of records.
// A bucket is read from disk at most once per batch and modified buckets are
// written back once when the batch is flushed, no matter how many records in
// the batch map to the same bucket.
type bucketBatch struct {
	buckets map[bucketRef]*bucketHandle
	dirty   map[bucketRef]struct{}
}

func newBucketBatch() *bucketBatch {
	return &bucketBatch{
		buckets: make(map[bucketRef]*bucketHandle),
		dirty:   make(map[bucketRef]struct{}),
	}
}

// next returns the next bucket in the iterator chain.
// Buckets modified earlier in the batch are returned from the cache.
func (bb *bucketBatch) next(it *bucketIterator) (*bucketHandle, error) {
	if it.off == 0 {
		return nil, ErrIterationDone
	}
	ref := bucketRef{file: it.f, offset: it.off}
	b, ok := bb.buckets[ref]
	if !ok {
		b = &bucketHandle{file: it.f, offset: it.off}
		if err := b.read(); err != nil {
			return nil, err
		}
		bb.buckets[ref] = b
	}
	it.f = it.overflow
	it.off = b.next
	return b, nil
}

// flush writes the buckets modified during the batch to disk.
func (bb *bucketBatch) flush() error {
	for ref := range bb.dirty {
		if err := bb.buckets[ref].write(); err != nil {
			return err
		}
	}
	return nil
}

// findRecordSlot returns the bucket and the slot index pointing to the record.
// It returns false if the index doesn't point to the record and the record is safe to discard.
func (db *DB) findRecordSlot(bb *bucketBatch, rec record) (*bucketHandle, int, bool, error) {
	hash := db.hash(rec.key)
	it := db.index.newBucketIterator(db.index.bucketIndex(hash))
	for {
		b, err := bb.next(it)
		if err == ErrIterationDone {
			// Exhausted all buckets and the slot wasn't found.
			// The key was deleted or overwritten. The record is safe to discard.
			return nil, 0, false, nil
		}
		if err != nil {
			return nil, 0, false, err
		}
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]

			// No more slots in the bucket.
			if sl.offset == 0 {
				break
			}

			// Slot points to a different record.
			if hash != sl.hash || rec.offset != sl.offset || rec.segmentID != sl.segmentID {
				continue
			}

			return b, i, true, nil
		}
	}
}

// promoteBatch copies live records of the batch to the current segment and updates the index.
// Records are processed in the order they appear in the source segment.
// Records are appended to the current segment and index buckets are written back in batches.
// It returns the number of discarded records and their size in bytes.
func (db *DB) promoteBatch(recs []record) (int, int, error) {
	reclaimed := 0
	reclaimedBytes := 0
	bb := newBucketBatch()
	type pendingRecord struct {
		data    []byte
		bucket  *bucketHandle
		slotIdx int
	}
	var pending []pendingRecord
	for _, rec := range recs {
		if rec.rtype == recordTypeDelete {
			reclaimed++
			reclaimedBytes += len(rec.data)
			continue
		}
		b, slotIdx, found, err := db.findRecordSlot(bb, rec)
		if err != nil {
			return 0, 0, err
		}
		if !found {
			reclaimed++
			reclaimedBytes += len(rec.data)
			continue
		}
		// The record is in the index, move it to the current segment.
		pending = append(pending, pendingRecord{data: rec.data, bucket: b, slotIdx: slotIdx})
	}
	if len(pending) > 0 {
		datas := make([][]byte, len(pending))
		for i, p := range pending {
			datas[i] = p.data
		}
		locs, err := db.datalog.appendRecords(datas, recordTypePut)
		if err != nil {
			return 0, 0, err
		}
		// Update index.
		for i, p := range pending {
			p.bucket.slots[p.slotIdx].segmentID = locs[i].segmentID
			p.bucket.slots[p.slotIdx].offset = locs[i].offset
			bb.dirty[bucketRef{file: p.bucket.file, offset: p.bucket.offset}] = struct{}{}
		}
	}
	if err := bb.flush(); err != nil {
		return 0, 0, err
	}
	return reclaimed, reclaimedBytes, nil
}

// CompactionResult holds the compaction result.
type CompactionResult struct {
	CompactedSegments int
	ReclaimedRecords  int
	ReclaimedBytes    int
}

func (db *DB) compact(sourceSeg *segment) (CompactionResult, error) {
	cr := CompactionResult{}

	db.mu.Lock()
	sourceSeg.meta.Full = true // Prevent writes to the compacted file.
	db.mu.Unlock()

	it, err := newSegmentIterator(sourceSeg)
	if err != nil {
		return cr, err
	}
	// Copy records from sourceSeg to the current segment.
	// Records are processed in batches in order to not block
	// concurrent reads and writes for the duration of the whole segment compaction.
	for {
		batch := make([]record, 0, compactionBatchSize)
		for len(batch) < compactionBatchSize {
			rec, err := it.next()
			if err == ErrIterationDone {
				break
			}
			if err != nil {
				return cr, err
			}
			batch = append(batch, rec)
		}
		if len(batch) == 0 {
			break
		}
		db.mu.Lock()
		reclaimed, reclaimedBytes, err := db.promoteBatch(batch)
		db.mu.Unlock()
		if err != nil {
			return cr, err
		}
		cr.ReclaimedRecords += reclaimed
		cr.ReclaimedBytes += reclaimedBytes
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	err = db.datalog.removeSegment(sourceSeg)
	return cr, err
}

// pickForCompaction returns segments eligible for compaction.
func (db *DB) pickForCompaction() []*segment {
	segments := db.datalog.segmentsBySequenceID()
	var picked []*segment
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]

		if uint32(seg.size) < db.opts.compactionMinSegmentSize {
			continue
		}

		fragmentation := float32(seg.meta.DeletedBytes) / float32(seg.size)
		if fragmentation < db.opts.compactionMinFragmentation {
			continue
		}

		if seg.meta.DeleteRecords > 0 {
			// Delete records can be discarded only when older segments contain no put records
			// for the corresponding keys.
			// All segments older than the segment eligible for compaction have to be compacted.
			return append(segments[:i+1], picked...)
		}

		picked = append([]*segment{seg}, picked...)
	}
	return picked
}

// Compact compacts the DB. Deleted and overwritten items are discarded.
// Returns an error if compaction is already in progress.
func (db *DB) Compact() (CompactionResult, error) {
	cr := CompactionResult{}

	// Run only a single compaction at a time.
	if !db.maintenanceMu.TryLock() {
		return cr, errBusy
	}
	defer func() {
		db.maintenanceMu.Unlock()
	}()

	db.mu.RLock()
	segments := db.pickForCompaction()
	db.mu.RUnlock()

	for _, seg := range segments {
		segcr, err := db.compact(seg)
		if err != nil {
			return cr, errors.Wrapf(err, "compacting segment %s", seg.name)
		}
		cr.CompactedSegments++
		cr.ReclaimedRecords += segcr.ReclaimedRecords
		cr.ReclaimedBytes += segcr.ReclaimedBytes
	}

	return cr, nil
}
