package pogreb

import "sort"

const minFreeRecordSize = 10 // Empty free record header plus checksum.

type freeBlock struct {
	Offset uint32
	Size   uint32
}

func (seg *segment) initFreeBlocks() {
	if seg.freeBlocks == nil {
		seg.freeBlocks = make(map[uint32]uint32)
		seg.freeSizes = make(map[uint32]map[uint32]struct{})
		seg.freeEnds = make(map[uint32]uint32)
	}
}

func (seg *segment) addFreeBlock(offset, size uint32) {
	if size == 0 {
		return
	}
	seg.initFreeBlocks()

	// Merge with the previous range, if any.
	if prevOffset, ok := seg.freeEnds[offset]; ok {
		prevSize := seg.freeBlocks[prevOffset]
		seg.removeFreeBlock(prevOffset)
		offset = prevOffset
		size += prevSize
	}

	// Merge with the next range, if any.
	if nextSize, ok := seg.freeBlocks[offset+size]; ok && nextSize != 0 {
		seg.removeFreeBlock(offset + size)
		size += nextSize
	}

	if oldSize, ok := seg.freeBlocks[offset]; ok {
		if size <= oldSize {
			return
		}
		seg.removeFreeBlock(offset)
	}

	seg.freeBlocks[offset] = size
	seg.freeEnds[offset+size] = offset
	offsets := seg.freeSizes[size]
	if offsets == nil {
		offsets = make(map[uint32]struct{})
		seg.freeSizes[size] = offsets
	}
	offsets[offset] = struct{}{}
}

func (seg *segment) removeFreeBlock(offset uint32) {
	size, ok := seg.freeBlocks[offset]
	if !ok {
		return
	}
	delete(seg.freeBlocks, offset)
	delete(seg.freeEnds, offset+size)
	if offsets := seg.freeSizes[size]; offsets != nil {
		delete(offsets, offset)
		if len(offsets) == 0 {
			delete(seg.freeSizes, size)
		}
	}
}

func (seg *segment) setFreeBlocks(blocks []freeBlock) {
	seg.freeBlocks = make(map[uint32]uint32, len(blocks))
	seg.freeSizes = make(map[uint32]map[uint32]struct{})
	seg.freeEnds = make(map[uint32]uint32)
	for _, b := range blocks {
		seg.addFreeBlock(b.Offset, b.Size)
	}
}

func (seg *segment) freeBlockList() []freeBlock {
	if len(seg.freeBlocks) == 0 {
		return nil
	}
	blocks := make([]freeBlock, 0, len(seg.freeBlocks))
	for offset, size := range seg.freeBlocks {
		blocks = append(blocks, freeBlock{Offset: offset, Size: size})
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})
	return blocks
}

// findFreeBlock returns the smallest free range that fits size and satisfies
// accept.
func (seg *segment) findFreeBlock(size uint32, accept func(offset uint32) bool) (freeBlock, bool) {
	var bestSize uint32
	found := false
	for blockSize, offsets := range seg.freeSizes {
		if blockSize < size || (found && blockSize >= bestSize) {
			continue
		}
		for offset := range offsets {
			if seg.freeBlocks[offset] != blockSize {
				continue
			}
			if accept != nil && !accept(offset) {
				continue
			}
			bestSize = blockSize
			found = true
			break
		}
	}
	if !found {
		return freeBlock{}, false
	}

	var bestOffset uint32
	first := true
	for offset := range seg.freeSizes[bestSize] {
		if seg.freeBlocks[offset] != bestSize {
			continue
		}
		if accept != nil && !accept(offset) {
			continue
		}
		if first || offset < bestOffset {
			bestOffset = offset
			first = false
		}
	}
	return freeBlock{Offset: bestOffset, Size: bestSize}, first == false
}
