package pogreb

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/akrylysov/pogreb/internal/errors"
)

const (
	maxSegments = math.MaxInt16

	freeListExt = ".pfl"
)

// datalog is a write-ahead log.
type datalog struct {
	opts          *Options
	curSeg        *segment
	segments      [maxSegments]*segment
	maxSequenceID uint64
	reuse         bool
}

func openDatalog(opts *Options) (*datalog, error) {
	files, err := opts.FileSystem.ReadDir(".")
	if err != nil {
		return nil, err
	}

	dl := &datalog{
		opts: opts,
	}

	// Open existing segments.
	for _, file := range files {
		name := file.Name()
		ext := filepath.Ext(name)
		if ext != segmentExt {
			continue
		}
		id, seqID, err := parseSegmentName(name)
		if err != nil {
			return nil, err
		}
		seg, err := dl.openSegment(name, id, seqID)
		if err != nil {
			return nil, errors.Wrapf(err, "opening segment %s", name)
		}
		if seg.sequenceID > dl.maxSequenceID {
			dl.maxSequenceID = seg.sequenceID
		}
		dl.segments[seg.id] = seg
	}

	if err := dl.swapSegment(); err != nil {
		return nil, err
	}

	return dl, nil
}

func parseSegmentName(name string) (uint16, uint64, error) {
	parts := strings.SplitN(strings.TrimSuffix(name, segmentExt), "-", 2)
	id, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return 0, 0, err
	}
	var seqID uint64
	if len(parts) == 2 {
		seqID, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	return uint16(id), seqID, nil
}

func (dl *datalog) openSegment(name string, id uint16, seqID uint64) (*segment, error) {
	f, err := openFile(dl.opts.FileSystem, name, openFileFlags{})
	if err != nil {
		return nil, err
	}

	meta := &segmentMeta{}
	if !f.empty() {
		metaName := name + metaExt
		if err := readGobFile(dl.opts.FileSystem, metaName, &meta); err != nil {
			logger.Printf("error reading segment meta %d: %v", id, err)
			// TODO: rebuild meta?
		}
	}

	seg := &segment{
		file:       f,
		id:         id,
		sequenceID: seqID,
		name:       name,
		meta:       meta,
	}
	var blocks []freeBlock
	if err := readGobFile(dl.opts.FileSystem, freeListName(name), &blocks); err == nil {
		seg.setFreeBlocks(blocks)
		if len(blocks) > 0 {
			dl.reuse = true
		}
	} else if !os.IsNotExist(err) {
		logger.Printf("error reading free list for segment %d: %v", id, err)
	}

	return seg, nil
}

func (dl *datalog) nextWritableSegmentID() (uint16, uint64, error) {
	for id, seg := range dl.segments {
		// Pick empty segment.
		if seg == nil {
			dl.maxSequenceID++
			return uint16(id), dl.maxSequenceID, nil
		}
	}
	return 0, 0, fmt.Errorf("number of segments exceeds %d", maxSegments)
}

func (dl *datalog) swapSegment() error {
	// Pick unfilled segment.
	for _, seg := range dl.segments {
		if seg != nil && !seg.meta.Full {
			dl.curSeg = seg
			return nil
		}
	}

	// Create new segment.
	id, seqID, err := dl.nextWritableSegmentID()
	if err != nil {
		return err
	}

	name := segmentName(id, seqID)
	seg, err := dl.openSegment(name, id, seqID)
	if err != nil {
		return err
	}

	dl.segments[id] = seg
	dl.curSeg = seg

	return nil
}

func (dl *datalog) removeSegment(seg *segment) error {
	dl.segments[seg.id] = nil

	if err := seg.Close(); err != nil {
		return err
	}

	// Remove segment meta from FS.
	metaName := segmentMetaName(seg.id, seg.sequenceID)
	if err := dl.opts.FileSystem.Remove(metaName); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := dl.opts.FileSystem.Remove(freeListName(seg.name)); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove segment from FS.
	if err := dl.opts.FileSystem.Remove(seg.name); err != nil {
		return err
	}

	return nil
}

func (dl *datalog) readKeyValue(sl slot) ([]byte, []byte, error) {
	off := int64(sl.offset) + 6 // Skip key size and value size.
	seg := dl.segments[sl.segmentID]
	keyValue, err := seg.Slice(off, off+int64(sl.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	return keyValue[:sl.keySize], keyValue[sl.keySize:], nil
}

func (dl *datalog) readKey(sl slot) ([]byte, error) {
	off := int64(sl.offset) + 6
	seg := dl.segments[sl.segmentID]
	return seg.Slice(off, off+int64(sl.keySize))
}

// trackDel updates segment's metadata for deleted or overwritten items.
func (dl *datalog) trackDel(sl slot) {
	meta := dl.segments[sl.segmentID].meta
	meta.DeletedKeys++
	meta.DeletedBytes += encodedRecordSize(sl.kvSize())
}

func (dl *datalog) del(key []byte, after ...*slot) (uint16, uint32, error) {
	rec := encodeDeleteRecord(key)
	var afterSlot *slot
	if len(after) > 0 {
		afterSlot = after[0]
	}
	segID, offset, err := dl.writeRecordReusing(rec, recordTypeDelete, afterSlot)
	if err != nil {
		return 0, 0, err
	}
	// Compaction removes delete records, increment DeletedBytes.
	dl.segments[segID].meta.DeletedBytes += uint32(len(rec))
	return segID, offset, nil
}

func freeListName(segmentName string) string {
	return segmentName + freeListExt
}

func (dl *datalog) writeRecord(data []byte, rt recordType) (uint16, uint32, error) {
	if dl.curSeg.meta.Full || dl.curSeg.size+int64(len(data)) > int64(dl.opts.maxSegmentSize) {
		// Current segment is full, create a new one.
		dl.curSeg.meta.Full = true
		if err := dl.swapSegment(); err != nil {
			return 0, 0, err
		}
	}
	off, err := dl.curSeg.append(data)
	if err != nil {
		return 0, 0, err
	}
	switch rt {
	case recordTypePut:
		dl.curSeg.meta.PutRecords++
	case recordTypeDelete:
		dl.curSeg.meta.DeleteRecords++
	}
	return dl.curSeg.id, uint32(off), nil
}

func (dl *datalog) positionAfter(seg *segment, offset uint32, after *slot) bool {
	if after == nil {
		return true
	}
	afterSeg := dl.segments[after.segmentID]
	if afterSeg == nil {
		return true
	}
	if seg.sequenceID != afterSeg.sequenceID {
		return seg.sequenceID > afterSeg.sequenceID
	}
	return offset > after.offset
}

func (dl *datalog) allocateFree(dataLen uint32, after *slot) (*segment, freeBlock, bool, error) {
	var bestSeg *segment
	var bestBlock freeBlock
	found := false

	for _, seg := range dl.segmentsBySequenceID() {
		if seg.compacting {
			continue
		}
		block, ok := seg.findFreeBlock(dataLen, func(offset uint32) bool {
			if !dl.positionAfter(seg, offset, after) {
				return false
			}
			remaining := seg.freeBlocks[offset] - dataLen
			return remaining == 0 || remaining >= minFreeRecordSize
		})
		if !ok {
			continue
		}
		if !found || block.Size < bestBlock.Size {
			bestSeg = seg
			bestBlock = block
			found = true
		}
	}
	return bestSeg, bestBlock, found, nil
}

func (dl *datalog) markFreeRange(seg *segment, offset, size uint32) error {
	for size > 0 {
		chunkSize := size
		if chunkSize > maxFreeRecordSize {
			chunkSize = maxFreeRecordSize
		}
		if remaining := size - chunkSize; remaining > 0 && remaining < minFreeRecordSize {
			chunkSize -= minFreeRecordSize - remaining
		}
		buf := make([]byte, chunkSize)
		encodeFreeRecord(buf)
		if _, err := seg.WriteAt(buf, int64(offset)); err != nil {
			return err
		}
		offset += chunkSize
		size -= chunkSize
	}
	return nil
}

func (dl *datalog) writeReusedRecord(seg *segment, block freeBlock, data []byte, rt recordType) (uint32, error) {
	remaining := block.Size - uint32(len(data))
	if remaining > 0 {
		// Write the new free marker before overwriting the range's first
		// record. If a crash happens in between, the old free record still
		// covers the whole range.
		if err := dl.markFreeRange(seg, block.Offset+uint32(len(data)), remaining); err != nil {
			return 0, err
		}
	}
	if _, err := seg.WriteAt(data, int64(block.Offset)); err != nil {
		return 0, err
	}

	seg.removeFreeBlock(block.Offset)
	if remaining > 0 {
		seg.addFreeBlock(block.Offset+uint32(len(data)), remaining)
	}
	switch rt {
	case recordTypePut:
		seg.meta.PutRecords++
	case recordTypeDelete:
		seg.meta.DeleteRecords++
	}
	return block.Offset, nil
}

func (dl *datalog) writeRecordReusing(data []byte, rt recordType, after *slot) (uint16, uint32, error) {
	if dl.reuse {
		seg, block, ok, err := dl.allocateFree(uint32(len(data)), after)
		if err != nil {
			return 0, 0, err
		}
		if ok {
			offset, err := dl.writeReusedRecord(seg, block, data, rt)
			return seg.id, offset, err
		}
	}
	return dl.writeRecord(data, rt)
}

func (dl *datalog) put(key []byte, value []byte, after ...*slot) (uint16, uint32, error) {
	var afterSlot *slot
	if len(after) > 0 {
		afterSlot = after[0]
	}
	return dl.writeRecordReusing(encodePutRecord(key, value), recordTypePut, afterSlot)
}

func (dl *datalog) freeRange(segID uint16, offset, size uint32) error {
	if size == 0 {
		return nil
	}
	seg := dl.segments[segID]
	if seg == nil {
		return errors.New("freeing a record in a missing segment")
	}
	if err := dl.markFreeRange(seg, offset, size); err != nil {
		return err
	}
	seg.addFreeBlock(offset, size)
	return nil
}

func (dl *datalog) freeSlot(sl slot) error {
	return dl.freeRange(sl.segmentID, sl.offset, encodedRecordSize(sl.kvSize()))
}

func (dl *datalog) shouldReuse() bool {
	var dataSize uint64
	var deletedBytes uint64
	for _, seg := range dl.segments {
		if seg == nil {
			continue
		}
		if seg.size > headerSize {
			dataSize += uint64(seg.size - headerSize)
		}
		deletedBytes += uint64(seg.meta.DeletedBytes)
	}
	if dataSize == 0 {
		return false
	}
	return float64(deletedBytes)/float64(dataSize) >= 0.7
}

func (dl *datalog) hasFreeBlocks() bool {
	for _, seg := range dl.segments {
		if seg != nil && len(seg.freeBlocks) > 0 {
			return true
		}
	}
	return false
}

func (dl *datalog) writeFreeLists() error {
	for _, seg := range dl.segments {
		if seg == nil {
			continue
		}
		name := freeListName(seg.name)
		blocks := seg.freeBlockList()
		if len(blocks) == 0 {
			if err := dl.opts.FileSystem.Remove(name); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if err := writeGobFile(dl.opts.FileSystem, name, blocks); err != nil {
			return err
		}
	}
	return nil
}

func (dl *datalog) sync() error {
	return dl.curSeg.Sync()
}

func (dl *datalog) close() error {
	for _, seg := range dl.segments {
		if seg == nil {
			continue
		}
		if err := seg.Close(); err != nil {
			return err
		}
		metaName := seg.name + metaExt
		if err := writeGobFile(dl.opts.FileSystem, metaName, seg.meta); err != nil {
			return err
		}
	}
	return dl.writeFreeLists()
}

// segmentsBySequenceID returns segments ordered from oldest to newest.
func (dl *datalog) segmentsBySequenceID() []*segment {
	var segments []*segment

	for _, seg := range dl.segments {
		if seg == nil {
			continue
		}
		segments = append(segments, seg)
	}

	sort.SliceStable(segments, func(i, j int) bool {
		return segments[i].sequenceID < segments[j].sequenceID
	})

	return segments
}
