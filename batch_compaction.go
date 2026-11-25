package pogreb

import (
	"os"

	"github.com/akrylysov/pogreb/internal/errors"
)

const (
	reclaimBatchMaxRecords = 32
	reclaimBatchMaxBytes   = 256 << 10
)

type slotLocation struct {
	segmentID uint16
	offset    uint32
}

type stagedReclaim struct {
	old     slotLocation
	next    slot
	dataLen int
}

func readReclaimBatch(it *segmentIterator) ([]record, error) {
	records := make([]record, 0, reclaimBatchMaxRecords)
	totalBytes := 0
	for len(records) < reclaimBatchMaxRecords && totalBytes < reclaimBatchMaxBytes {
		rec, err := it.next()
		if err != nil {
			return records, err
		}
		records = append(records, rec)
		totalBytes += len(rec.data)
	}
	return records, nil
}

// recordLocationLive reports whether the index still points to rec.
// Matching the exact segment and offset is the same predicate used by the
// single-record promotion path.
func (db *DB) recordLocationLive(rec record) (bool, error) {
	hash := db.hash(rec.key)
	it := db.index.newBucketIterator(db.index.bucketIndex(hash))
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		db.index.pinBucket(&b)
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				break
			}
			if hash != sl.hash || rec.offset != sl.offset || rec.segmentID != sl.segmentID {
				continue
			}
			return true, nil
		}
	}
}

func (db *DB) planReclaimBatch(records []record) ([]record, CompactionResult, error) {
	cr := CompactionResult{}
	live := make([]record, 0, len(records))

	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, rec := range records {
		if rec.rtype == recordTypeDelete {
			cr.ReclaimedRecords++
			cr.ReclaimedBytes += len(rec.data)
			continue
		}

		matches, err := db.recordLocationLive(rec)
		if err != nil {
			return nil, cr, err
		}
		if matches {
			live = append(live, rec)
			continue
		}

		cr.ReclaimedRecords++
		cr.ReclaimedBytes += len(rec.data)
	}

	return live, cr, nil
}

func (db *DB) stageReclaimBatch(live []record) ([]stagedReclaim, error) {
	staged := make([]stagedReclaim, 0, len(live))
	maxSegmentSize := int64(db.opts.maxSegmentSize)

	for i := 0; i < len(live); {
		db.mu.RLock()
		seg := db.datalog.curSeg
		db.mu.RUnlock()

		seg.ioMu.Lock()
		firstLen := int64(len(live[i].data))
		if seg.meta.Full || (seg.size > int64(headerSize) && seg.size+firstLen > maxSegmentSize) {
			seg.ioMu.Unlock()

			db.mu.Lock()
			var swapErr error
			if db.datalog.curSeg == seg {
				seg.meta.Full = true
				swapErr = db.datalog.swapSegment()
			}
			db.mu.Unlock()
			if swapErr != nil {
				return nil, swapErr
			}
			continue
		}

		count := 0
		totalBytes := 0
		for j := i; j < len(live); j++ {
			recordSize := len(live[j].data)
			if count > 0 && seg.size+int64(totalBytes+recordSize) > maxSegmentSize {
				break
			}
			totalBytes += recordSize
			count++
		}

		batchData := make([]byte, totalBytes)
		writeOffset := 0
		recordOffset := uint32(seg.size)
		for j := 0; j < count; j++ {
			rec := live[i+j]
			copy(batchData[writeOffset:], rec.data)
			writeOffset += len(rec.data)

			next := slot{
				hash:      db.hash(rec.key),
				segmentID: seg.id,
				keySize:   uint16(len(rec.key)),
				valueSize: uint32(len(rec.value)),
				offset:    recordOffset,
			}
			recordOffset += uint32(len(rec.data))
			staged = append(staged, stagedReclaim{
				old:     slotLocation{segmentID: rec.segmentID, offset: rec.offset},
				next:    next,
				dataLen: len(rec.data),
			})
		}

		if _, err := seg.append(batchData); err != nil {
			seg.ioMu.Unlock()
			return nil, err
		}
		seg.ioMu.Unlock()
		i += count
	}
	return staged, nil
}

// relocateSlotGroup scans one bucket chain and applies all matching moves in a
// single pass. Every modified bucket is marked dirty once.
func (idx *index) relocateSlotGroup(rootBucket uint32, candidates map[slotLocation]stagedReclaim) (int, error) {
	it := idx.newBucketIterator(idx.bucketIndex(rootBucket))
	updated := 0
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			return updated, nil
		}
		if err != nil {
			return updated, err
		}
		changed := false
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				break
			}
			location := slotLocation{segmentID: sl.segmentID, offset: sl.offset}
			item, ok := candidates[location]
			if !ok || item.next.hash != sl.hash {
				continue
			}
			b.slots[i] = item.next
			delete(candidates, location)
			updated++
			changed = true
		}
		if changed {
			idx.markBucketDirty(&b)
		}
	}
}

func (idx *index) evictCleanPages() {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	for key, page := range idx.pageCache {
		if !page.dirty {
			delete(idx.pageCache, key)
		}
	}
}

func (db *DB) commitPendingReclaims(pending []stagedReclaim) (CompactionResult, error) {
	cr := CompactionResult{}
	if len(pending) == 0 {
		return cr, nil
	}

	putRecords := make(map[uint16]uint32)
	for _, item := range pending {
		putRecords[item.next.segmentID]++
	}

	groups := make(map[uint32]map[slotLocation]stagedReclaim, len(pending))
	db.mu.Lock()
	for segmentID, count := range putRecords {
		if seg := db.datalog.segments[segmentID]; seg != nil {
			seg.meta.PutRecords += count
		}
	}

	for _, item := range pending {
		root := db.index.bucketIndex(item.next.hash)
		group := groups[root]
		if group == nil {
			group = make(map[slotLocation]stagedReclaim)
			groups[root] = group
		}
		group[item.old] = item
	}

	var pages []dirtyPage
	var commitErr error
	for _, candidates := range groups {
		currentRoots := make(map[uint32]struct{})
		for _, item := range candidates {
			currentRoots[db.index.bucketIndex(item.next.hash)] = struct{}{}
		}
		for currentRoot := range currentRoots {
			if _, err := db.index.relocateSlotGroup(currentRoot, candidates); err != nil {
				commitErr = err
				break
			}
		}
		if commitErr != nil {
			break
		}
	}
	if commitErr == nil {
		pages, commitErr = db.index.snapshotDirtyPages()
	}
	db.mu.Unlock()
	if commitErr != nil {
		return cr, commitErr
	}
	if err := db.index.flushDirtyPages(pages); err != nil {
		return cr, err
	}
	db.index.evictCleanPages()

	// Candidates left in the maps no longer match the index. A concurrent writer
	// moved or deleted them after planning; account them as single-record
	// compaction does when its exact-location predicate misses.
	for _, group := range groups {
		for _, item := range group {
			cr.ReclaimedRecords++
			cr.ReclaimedBytes += item.dataLen
		}
	}
	return cr, nil
}

func (db *DB) removeCompactedSegment(seg *segment) error {
	db.mu.Lock()
	if db.datalog.segments[seg.id] == seg {
		db.datalog.segments[seg.id] = nil
	}
	db.mu.Unlock()

	if err := seg.Close(); err != nil {
		return err
	}

	metaName := segmentMetaName(seg.id, seg.sequenceID)
	if err := db.opts.FileSystem.Remove(metaName); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := db.opts.FileSystem.Remove(seg.name); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (db *DB) compact(sourceSeg *segment) (CompactionResult, error) {
	cr := CompactionResult{}

	db.index.beginBatchCache()
	defer db.index.endBatchCache()

	db.mu.Lock()
	sourceSeg.meta.Full = true // Prevent writes to the compacted file.
	db.mu.Unlock()

	it, err := newSegmentIterator(sourceSeg)
	if err != nil {
		return cr, err
	}

	for {
		records, readErr := readReclaimBatch(it)
		if len(records) > 0 {
			live, planCR, err := db.planReclaimBatch(records)
			cr.ReclaimedRecords += planCR.ReclaimedRecords
			cr.ReclaimedBytes += planCR.ReclaimedBytes
			if err != nil {
				return cr, err
			}

			staged, err := db.stageReclaimBatch(live)
			if err != nil {
				return cr, err
			}
			commitCR, err := db.commitPendingReclaims(staged)
			cr.ReclaimedRecords += commitCR.ReclaimedRecords
			cr.ReclaimedBytes += commitCR.ReclaimedBytes
			if err != nil {
				return cr, err
			}
		}

		if readErr == ErrIterationDone {
			break
		}
		if readErr != nil {
			return cr, readErr
		}
	}

	if err := db.index.flushAllDirtyPages(); err != nil {
		return cr, err
	}
	if err := db.removeCompactedSegment(sourceSeg); err != nil {
		return cr, errors.Wrapf(err, "removing segment %s", sourceSeg.name)
	}
	return cr, nil
}
