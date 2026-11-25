package pogreb

import (
	"sync"
	"sync/atomic"

	"github.com/akrylysov/pogreb/internal/errors"
)

const (
	indexExt          = ".pix"
	indexMainName     = "main" + indexExt
	indexOverflowName = "overflow" + indexExt
	indexMetaName     = "index" + metaExt
	loadFactor        = 0.7
)

// index is an on-disk linear hashing hash table.
// It uses two files to store the hash table on disk - "main" and "overflow" index files.
// Each index file holds an array of buckets.
type index struct {
	opts           *Options
	main           *file   // Main index file.
	overflow       *file   // Overflow index file.
	freeBucketOffs []int64 // Offsets of freed buckets.
	level          uint8   // Maximum number of buckets on a logarithmic scale.
	numKeys        uint32  // Number of keys.
	numBuckets     uint32  // Number of buckets.
	splitBucketIdx uint32  // Index of the bucket to split on next split.
	pageCache      map[pageKey]*cachedPage
	cacheMu        sync.RWMutex
	pageIO         sync.RWMutex
	cachePages     bool
}

type pageKey struct {
	file   *file
	offset int64
}

type cachedPage struct {
	bucket   bucket
	file     *file
	revision uint64
	dirty    bool
}

type indexMeta struct {
	Level               uint8
	NumKeys             uint32
	NumBuckets          uint32
	SplitBucketIndex    uint32
	FreeOverflowBuckets []int64
}

// matchKeyFunc returns whether the slot matches the key sought.
type matchKeyFunc func(slot) (bool, error)

func openIndex(opts *Options) (*index, error) {
	main, err := openFile(opts.FileSystem, indexMainName, openFileFlags{})
	if err != nil {
		return nil, errors.Wrap(err, "opening main index")
	}
	overflow, err := openFile(opts.FileSystem, indexOverflowName, openFileFlags{})
	if err != nil {
		_ = main.Close()
		return nil, errors.Wrap(err, "opening overflow index")
	}
	idx := &index{
		opts:       opts,
		main:       main,
		overflow:   overflow,
		numBuckets: 1,
	}
	if main.empty() {
		// Add an empty bucket.
		if _, err = idx.main.extend(bucketSize); err != nil {
			_ = main.Close()
			_ = overflow.Close()
			return nil, err
		}
	} else if err := idx.readMeta(); err != nil {
		_ = main.Close()
		_ = overflow.Close()
		return nil, errors.Wrap(err, "opening index meta")
	}
	return idx, nil
}

func (idx *index) writeMeta() error {
	m := indexMeta{
		Level:               idx.level,
		NumKeys:             idx.numKeys,
		NumBuckets:          idx.numBuckets,
		SplitBucketIndex:    idx.splitBucketIdx,
		FreeOverflowBuckets: idx.freeBucketOffs,
	}
	return writeGobFile(idx.opts.FileSystem, indexMetaName, m)
}

func (idx *index) readMeta() error {
	m := indexMeta{}
	if err := readGobFile(idx.opts.FileSystem, indexMetaName, &m); err != nil {
		return err
	}
	idx.level = m.Level
	idx.numKeys = m.NumKeys
	idx.numBuckets = m.NumBuckets
	idx.splitBucketIdx = m.SplitBucketIndex
	idx.freeBucketOffs = m.FreeOverflowBuckets
	return nil
}

func (idx *index) bucketIndex(hash uint32) uint32 {
	bidx := hash & ((1 << idx.level) - 1)
	if bidx < idx.splitBucketIdx {
		return hash & ((1 << (idx.level + 1)) - 1)
	}
	return bidx
}

type bucketIterator struct {
	idx      *index
	off      int64 // Offset of the next bucket.
	f        *file // Current index file.
	overflow *file // Overflow index file.
}

func (idx *index) readBucket(b *bucketHandle) error {
	if idx != nil {
		key := pageKey{file: b.file, offset: b.offset}
		idx.cacheMu.RLock()
		cp := idx.pageCache[key]
		if cp != nil {
			b.bucket = cp.bucket
			idx.cacheMu.RUnlock()
			return nil
		}
		idx.cacheMu.RUnlock()

		idx.pageIO.RLock()
		err := b.read()
		idx.pageIO.RUnlock()
		return err
	}
	return b.read()
}

func (idx *index) pinBucket(b *bucketHandle) {
	if idx == nil {
		return
	}
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	if idx.pageCache == nil {
		idx.pageCache = make(map[pageKey]*cachedPage)
	}
	key := pageKey{file: b.file, offset: b.offset}
	if _, ok := idx.pageCache[key]; !ok {
		idx.pageCache[key] = &cachedPage{bucket: b.bucket, file: b.file}
	}
}

func (idx *index) writeBucket(b *bucketHandle) error {
	var cp *cachedPage
	var revision uint64
	if idx != nil {
		idx.cacheMu.Lock()
		key := pageKey{file: b.file, offset: b.offset}
		cp = idx.pageCache[key]
		if cp == nil && idx.cachePages {
			cp = &cachedPage{}
			idx.pageCache[key] = cp
		}
		if cp != nil {
			cp.bucket = b.bucket
			cp.file = b.file
			revision = atomic.AddUint64(&cp.revision, 1)
			cp.dirty = true
		}
		idx.cacheMu.Unlock()

		idx.pageIO.Lock()
		err := b.write()
		idx.pageIO.Unlock()
		if err != nil {
			return err
		}

		if cp != nil {
			idx.cacheMu.Lock()
			if atomic.LoadUint64(&cp.revision) == revision {
				cp.dirty = false
			}
			idx.cacheMu.Unlock()
		}
		return nil
	}
	return b.write()
}

func (idx *index) markBucketDirty(b *bucketHandle) {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	if idx.pageCache == nil {
		idx.pageCache = make(map[pageKey]*cachedPage)
	}
	key := pageKey{file: b.file, offset: b.offset}
	cp := idx.pageCache[key]
	if cp == nil {
		cp = &cachedPage{file: b.file}
		idx.pageCache[key] = cp
	}
	cp.bucket = b.bucket
	cp.file = b.file
	atomic.AddUint64(&cp.revision, 1)
	cp.dirty = true
}

type dirtyPage struct {
	offset   int64
	file     *file
	data     []byte
	page     *cachedPage
	revision uint64
}

func (idx *index) snapshotDirtyPages() ([]dirtyPage, error) {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	pages := make([]dirtyPage, 0)
	for key, cp := range idx.pageCache {
		if !cp.dirty {
			continue
		}
		data, err := cp.bucket.MarshalBinary()
		if err != nil {
			return nil, err
		}
		pages = append(pages, dirtyPage{
			offset:   key.offset,
			file:     cp.file,
			data:     data,
			page:     cp,
			revision: atomic.LoadUint64(&cp.revision),
		})
	}
	return pages, nil
}

func (idx *index) flushDirtyPages(pages []dirtyPage) error {
	for _, page := range pages {
		idx.pageIO.Lock()
		current := atomic.LoadUint64(&page.page.revision)
		var err error
		if current == page.revision {
			_, err = page.file.WriteAt(page.data, page.offset)
		}
		idx.pageIO.Unlock()
		if err != nil {
			return err
		}

		idx.cacheMu.Lock()
		if atomic.LoadUint64(&page.page.revision) == page.revision {
			page.page.dirty = false
		}
		idx.cacheMu.Unlock()
	}
	return nil
}

func (idx *index) flushAllDirtyPages() error {
	pages, err := idx.snapshotDirtyPages()
	if err != nil {
		return err
	}
	return idx.flushDirtyPages(pages)
}

func (idx *index) beginBatchCache() {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	if idx.pageCache == nil {
		idx.pageCache = make(map[pageKey]*cachedPage)
	}
	idx.cachePages = true
}

func (idx *index) endBatchCache() {
	idx.cacheMu.Lock()
	defer idx.cacheMu.Unlock()
	idx.cachePages = false
	for key, cp := range idx.pageCache {
		if !cp.dirty {
			delete(idx.pageCache, key)
		}
	}
}

// bucketOffset returns on-disk bucket offset by the bucket index.
func bucketOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(bucketSize) * int64(idx))
}

func (idx *index) newBucketIterator(startBucketIdx uint32) *bucketIterator {
	return &bucketIterator{
		idx:      idx,
		off:      bucketOffset(startBucketIdx),
		f:        idx.main,
		overflow: idx.overflow,
	}
}

func (it *bucketIterator) next() (bucketHandle, error) {
	if it.off == 0 {
		return bucketHandle{}, ErrIterationDone
	}
	b := bucketHandle{file: it.f, offset: it.off}
	if err := it.idx.readBucket(&b); err != nil {
		return bucketHandle{}, err
	}
	it.f = it.overflow
	it.off = b.next
	return b, nil
}

func (idx *index) get(hash uint32, matchKey matchKeyFunc) error {
	it := idx.newBucketIterator(idx.bucketIndex(hash))
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			return nil
		}
		if err != nil {
			return err
		}
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			// No more slots in the bucket.
			if sl.offset == 0 {
				break
			}
			if hash != sl.hash {
				continue
			}
			if match, err := matchKey(sl); match || err != nil {
				return err
			}
		}
	}
}

func (idx *index) findInsertionBucket(newSlot slot, matchKey matchKeyFunc) (*slotWriter, bool, error) {
	sw := &slotWriter{idx: idx}
	it := idx.newBucketIterator(idx.bucketIndex(newSlot.hash))
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			return nil, false, errors.New("failed to insert a new slot")
		}
		if err != nil {
			return nil, false, err
		}
		sw.bucket = &b
		var i int
		for i = 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				// Found an empty slot.
				sw.slotIdx = i
				return sw, false, nil
			}
			if newSlot.hash != sl.hash {
				continue
			}
			match, err := matchKey(sl)
			if err != nil {
				return nil, false, err
			}
			if match {
				// Key already in the index.
				// The slot writer will overwrite the existing slot.
				sw.slotIdx = i
				return sw, true, nil
			}
		}
		if b.next == 0 {
			// No more buckets in the chain.
			sw.slotIdx = i
			return sw, false, nil
		}
	}
}

func (idx *index) put(newSlot slot, matchKey matchKeyFunc) error {
	if idx.numKeys == MaxKeys {
		return errFull
	}
	sw, overwritingExisting, err := idx.findInsertionBucket(newSlot, matchKey)
	if err != nil {
		return err
	}
	if err := sw.insert(newSlot, idx); err != nil {
		return err
	}
	if err := sw.write(); err != nil {
		return err
	}
	if overwritingExisting {
		return nil
	}
	idx.numKeys++
	if float64(idx.numKeys)/float64(idx.numBuckets*slotsPerBucket) > loadFactor {
		if err := idx.split(); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) delete(hash uint32, matchKey matchKeyFunc) error {
	it := idx.newBucketIterator(idx.bucketIndex(hash))
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			return nil
		}
		if err != nil {
			return err
		}
		for i := 0; i < slotsPerBucket; i++ {
			sl := b.slots[i]
			if sl.offset == 0 {
				break
			}
			if hash != sl.hash {
				continue
			}
			match, err := matchKey(sl)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
			b.del(i)
			if err := idx.writeBucket(&b); err != nil {
				return err
			}
			idx.numKeys--
			return nil
		}
	}
}

func (idx *index) createOverflowBucket() (*bucketHandle, error) {
	var off int64
	if len(idx.freeBucketOffs) > 0 {
		off = idx.freeBucketOffs[0]
		idx.freeBucketOffs = idx.freeBucketOffs[1:]
	} else {
		var err error
		idx.pageIO.Lock()
		off, err = idx.overflow.extend(bucketSize)
		idx.pageIO.Unlock()
		if err != nil {
			return nil, err
		}
	}
	return &bucketHandle{file: idx.overflow, offset: off}, nil
}

func (idx *index) freeOverflowBucket(offsets ...int64) {
	idx.freeBucketOffs = append(idx.freeBucketOffs, offsets...)
}

func (idx *index) split() error {
	updatedBucketIdx := idx.splitBucketIdx
	updatedBucketOff := bucketOffset(updatedBucketIdx)
	updatedBucket := slotWriter{
		idx:    idx,
		bucket: &bucketHandle{file: idx.main, offset: updatedBucketOff},
	}

	idx.pageIO.Lock()
	newBucketOff, err := idx.main.extend(bucketSize)
	idx.pageIO.Unlock()
	if err != nil {
		return err
	}

	sw := slotWriter{
		idx:    idx,
		bucket: &bucketHandle{file: idx.main, offset: newBucketOff},
	}

	idx.splitBucketIdx++
	if idx.splitBucketIdx == 1<<idx.level {
		idx.level++
		idx.splitBucketIdx = 0
	}

	var overflowBuckets []int64
	it := idx.newBucketIterator(updatedBucketIdx)
	for {
		b, err := it.next()
		if err == ErrIterationDone {
			break
		}
		if err != nil {
			return err
		}
		for j := 0; j < slotsPerBucket; j++ {
			sl := b.slots[j]
			if sl.offset == 0 {
				break
			}
			if idx.bucketIndex(sl.hash) == updatedBucketIdx {
				if err := updatedBucket.insert(sl, idx); err != nil {
					return err
				}
			} else {
				if err := sw.insert(sl, idx); err != nil {
					return err
				}
			}
		}
		if b.next != 0 {
			overflowBuckets = append(overflowBuckets, b.next)
		}
	}

	idx.freeOverflowBucket(overflowBuckets...)

	if err := sw.write(); err != nil {
		return err
	}
	if err := updatedBucket.write(); err != nil {
		return err
	}

	idx.numBuckets++
	return nil
}

func (idx *index) close() error {
	if err := idx.flushAllDirtyPages(); err != nil {
		return err
	}
	if err := idx.writeMeta(); err != nil {
		return err
	}
	if err := idx.main.Close(); err != nil {
		return err
	}
	if err := idx.overflow.Close(); err != nil {
		return err
	}
	return nil
}

func (idx *index) count() uint32 {
	return idx.numKeys
}
