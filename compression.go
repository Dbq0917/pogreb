package pogreb

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"sort"
)

const (
	blockHeaderSize = 24
	// defaultBlockUncompressedSize bounds the amount of data a random read has to
	// decompress. A larger record is stored as its own block.
	defaultBlockUncompressedSize = 32 << 10

	blockCompressionNone  byte = 0
	blockCompressionFlate byte = 1
)

var blockMagic = [8]byte{'P', 'O', 'G', 'B', 'L', 'K', '0', '1'}

type blockInfo struct {
	physicalOffset int64
	physicalEnd    int64
	logicalStart   int64
	logicalEnd     int64
	compression    byte
	uncompressed   uint32
	compressed     uint32
	checksum       uint32
}

type blockCompressor struct {
	writer         *flate.Writer
	checksum       hash.Hash32
	raw            *bytes.Buffer
	physicalOffset int64
	payloadStart   int64
	logicalStart   int64
	uncompressed   int
	dirty          bool
}

type segmentAppendWriter struct {
	segment *segment
}

func (w segmentAppendWriter) Write(data []byte) (int, error) {
	if _, err := w.segment.file.append(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func maxBlockSize() uint32 {
	return encodedRecordSize(uint32(MaxKeyLength) + MaxValueLength)
}

func (s *segment) initBlockFormat() error {
	s.logicalSize = int64(headerSize)
	s.comp = nil

	if s.file.size == int64(headerSize) {
		s.compressed = true
		return nil
	}

	magicEnd := int64(headerSize) + int64(len(blockMagic))
	magic, err := s.Slice(int64(headerSize), magicEnd)
	if err != nil {
		if err == io.EOF {
			s.compressed = false
			return nil
		}
		return err
	}

	if len(magic) != len(blockMagic) || !bytes.Equal(magic, blockMagic[:]) {
		// Segments created by older versions are a continuous stream of records.
		s.compressed = false
		s.logicalSize = s.file.size
		return nil
	}

	s.compressed = true
	return s.loadBlocks()
}

func (s *segment) loadBlocks() error {
	physicalOffset := int64(headerSize)
	logicalOffset := int64(headerSize)

	for physicalOffset < s.file.size {
		headerEnd := physicalOffset + blockHeaderSize
		header, err := s.Slice(physicalOffset, headerEnd)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(header) < blockHeaderSize || !bytes.Equal(header[:8], blockMagic[:]) || header[8] != 1 {
			break
		}

		compressionType := header[9]
		uncompressedSize := binary.LittleEndian.Uint32(header[12:16])
		compressedSize := binary.LittleEndian.Uint32(header[16:20])
		checksum := binary.LittleEndian.Uint32(header[20:24])
		physicalEnd := headerEnd + int64(compressedSize)

		if compressionType > blockCompressionFlate ||
			uncompressedSize == 0 ||
			uncompressedSize > maxBlockSize() ||
			compressedSize == 0 ||
			physicalEnd > s.file.size {
			break
		}

		s.blocks = append(s.blocks, blockInfo{
			physicalOffset: physicalOffset,
			physicalEnd:    physicalEnd,
			logicalStart:   logicalOffset,
			logicalEnd:     logicalOffset + int64(uncompressedSize),
			compression:    compressionType,
			uncompressed:   uncompressedSize,
			compressed:     compressedSize,
			checksum:       checksum,
		})

		physicalOffset = physicalEnd
		logicalOffset += int64(uncompressedSize)
	}

	s.logicalSize = logicalOffset
	return nil
}

func (s *segment) exceedsMaxSegmentSize(recordSize int, maxSegmentSize uint32) bool {
	size := s.file.size
	if s.compressed {
		size = s.logicalSize
	}
	return size+int64(recordSize) > int64(maxSegmentSize)
}

func (s *segment) appendRecord(data []byte) (uint32, error) {
	if !s.compressed {
		offset, err := s.file.append(data)
		return uint32(offset), err
	}

	if s.comp != nil && s.comp.uncompressed > 0 &&
		s.comp.uncompressed+len(data) > defaultBlockUncompressedSize {
		if err := s.finishBlock(); err != nil {
			return 0, err
		}
	}

	if s.comp == nil {
		if err := s.startBlock(len(data)); err != nil {
			return 0, err
		}
	}

	offset := s.logicalSize
	if _, err := s.comp.writer.Write(data); err != nil {
		s.abortActiveBlock()
		return 0, err
	}
	s.comp.checksum.Write(data)
	if s.comp.raw != nil {
		_, _ = s.comp.raw.Write(data)
	}
	s.comp.uncompressed += len(data)
	s.logicalSize += int64(len(data))
	s.comp.dirty = true

	if err := s.flushBlock(); err != nil {
		return 0, err
	}

	if s.comp != nil && s.comp.uncompressed >= defaultBlockUncompressedSize {
		if err := s.finishBlock(); err != nil {
			return 0, err
		}
	}

	return uint32(offset), nil
}

func (s *segment) startBlock(firstRecordSize int) error {
	header := make([]byte, blockHeaderSize)
	copy(header[:8], blockMagic[:])
	header[8] = 1 // Block format version.
	header[9] = blockCompressionFlate

	physicalOffset, err := s.file.append(header)
	if err != nil {
		return err
	}

	compressor := &blockCompressor{
		physicalOffset: physicalOffset,
		payloadStart:   physicalOffset + blockHeaderSize,
		logicalStart:   s.logicalSize,
		checksum:       crc32.NewIEEE(),
	}
	if firstRecordSize <= defaultBlockUncompressedSize {
		compressor.raw = &bytes.Buffer{}
	}
	writer, err := flate.NewWriter(segmentAppendWriter{segment: s}, flate.BestCompression)
	if err != nil {
		return err
	}
	compressor.writer = writer
	s.comp = compressor
	s.blocks = append(s.blocks, blockInfo{
		physicalOffset: physicalOffset,
		physicalEnd:    compressor.payloadStart,
		logicalStart:   s.logicalSize,
		logicalEnd:     s.logicalSize,
		compression:    blockCompressionFlate,
	})
	return nil
}

func blockHeaderBytes(compressionType byte, uncompressedSize, compressedSize uint32, checksum uint32) []byte {
	header := make([]byte, blockHeaderSize)
	copy(header[:8], blockMagic[:])
	header[8] = 1
	header[9] = compressionType
	binary.LittleEndian.PutUint16(header[10:12], 0)
	binary.LittleEndian.PutUint32(header[12:16], uncompressedSize)
	binary.LittleEndian.PutUint32(header[16:20], compressedSize)
	binary.LittleEndian.PutUint32(header[20:24], checksum)
	return header
}

func (s *segment) persistBlockHeader() error {
	if s.comp == nil {
		return nil
	}
	header := blockHeaderBytes(
		blockCompressionFlate,
		uint32(s.comp.uncompressed),
		uint32(s.file.size-s.comp.payloadStart),
		s.comp.checksum.Sum32(),
	)
	_, err := s.WriteAt(header, s.comp.physicalOffset)
	return err
}

func (s *segment) updateActiveBlockInfo() {
	if s.comp == nil || len(s.blocks) == 0 {
		return
	}
	s.blocks[len(s.blocks)-1] = blockInfo{
		physicalOffset: s.comp.physicalOffset,
		physicalEnd:    s.file.size,
		logicalStart:   s.comp.logicalStart,
		logicalEnd:     s.logicalSize,
		compression:    blockCompressionFlate,
		uncompressed:   uint32(s.comp.uncompressed),
		compressed:     uint32(s.file.size - s.comp.payloadStart),
		checksum:       s.comp.checksum.Sum32(),
	}
}

// abortActiveBlock drops an in-memory compressor after a failed write. The last
// persisted block header still describes the last durable record boundary.
func (s *segment) abortActiveBlock() {
	if s.comp == nil {
		return
	}
	if len(s.blocks) > 0 {
		s.logicalSize = s.blocks[len(s.blocks)-1].logicalEnd
	}
	s.comp = nil
}

func (s *segment) flushBlock() error {
	if !s.compressed || s.comp == nil {
		return nil
	}
	if !s.comp.dirty {
		return nil
	}
	if err := s.comp.writer.Flush(); err != nil {
		s.abortActiveBlock()
		return err
	}
	s.comp.dirty = false
	if err := s.persistBlockHeader(); err != nil {
		s.abortActiveBlock()
		return err
	}
	s.updateActiveBlockInfo()
	return nil
}

func (s *segment) finishBlock() error {
	if !s.compressed || s.comp == nil {
		return nil
	}
	compressor := s.comp
	if err := compressor.writer.Close(); err != nil {
		s.abortActiveBlock()
		return err
	}

	if compressor.raw != nil && compressor.raw.Len() > 0 {
		if err := s.rewriteFinishedBlock(compressor); err != nil {
			s.abortActiveBlock()
			return err
		}
		s.comp = nil
		return nil
	}

	if err := s.persistBlockHeader(); err != nil {
		s.abortActiveBlock()
		return err
	}
	s.updateActiveBlockInfo()
	s.comp = nil
	return nil
}

func encodeBlockPayload(data []byte) (byte, []byte, error) {
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		return 0, nil, err
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return 0, nil, err
	}
	if err := writer.Close(); err != nil {
		return 0, nil, err
	}
	if compressed.Len() < len(data) {
		return blockCompressionFlate, compressed.Bytes(), nil
	}
	return blockCompressionNone, data, nil
}

func (s *segment) rewriteFinishedBlock(compressor *blockCompressor) error {
	raw := compressor.raw.Bytes()
	compressionType, payload, err := encodeBlockPayload(raw)
	if err != nil {
		return err
	}

	header := blockHeaderBytes(
		compressionType,
		uint32(len(raw)),
		uint32(len(payload)),
		compressor.checksum.Sum32(),
	)
	frame := make([]byte, blockHeaderSize+len(payload))
	copy(frame, header)
	copy(frame[blockHeaderSize:], payload)

	physicalEnd := compressor.physicalOffset + int64(len(frame))
	// The streaming representation is the fallback. Replace it only when the
	// independently compressed block is not larger.
	if physicalEnd > s.file.size {
		if err := s.persistBlockHeader(); err != nil {
			return err
		}
		s.updateActiveBlockInfo()
		return nil
	}
	if _, err := s.WriteAt(frame, compressor.physicalOffset); err != nil {
		return err
	}
	if err := s.file.File.Truncate(physicalEnd); err != nil {
		return err
	}
	s.file.size = physicalEnd
	if len(s.blocks) > 0 {
		s.blocks[len(s.blocks)-1] = blockInfo{
			physicalOffset: compressor.physicalOffset,
			physicalEnd:    physicalEnd,
			logicalStart:   compressor.logicalStart,
			logicalEnd:     s.logicalSize,
			compression:    compressionType,
			uncompressed:   uint32(len(raw)),
			compressed:     uint32(len(payload)),
			checksum:       compressor.checksum.Sum32(),
		}
	}
	return nil
}

func (s *segment) loadBlockData(block blockInfo) ([]byte, error) {
	frame, err := s.Slice(block.physicalOffset, block.physicalEnd)
	if err != nil {
		return nil, err
	}
	if len(frame) < blockHeaderSize {
		return nil, errCorrupted
	}
	return decodeBlockPayload(block, frame[blockHeaderSize:])
}

func decodeBlockPayload(block blockInfo, payload []byte) ([]byte, error) {
	if uint32(len(payload)) != block.compressed {
		return nil, errCorrupted
	}

	var data []byte
	switch block.compression {
	case blockCompressionNone:
		data = make([]byte, block.uncompressed)
		copy(data, payload)
	case blockCompressionFlate:
		reader := flate.NewReader(bytes.NewReader(payload))
		defer reader.Close()
		data = make([]byte, block.uncompressed)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, errCorrupted
		}
	default:
		return nil, errCorrupted
	}

	if block.checksum != 0 && crc32.ChecksumIEEE(data) != block.checksum {
		return nil, errCorrupted
	}
	return data, nil
}

func (s *segment) readLogicalRange(offset int64, length int64) ([]byte, error) {
	if !s.compressed {
		return s.Slice(offset, offset+length)
	}

	blockIndex := sort.Search(len(s.blocks), func(i int) bool {
		return offset < s.blocks[i].logicalEnd
	})
	if blockIndex == len(s.blocks) || offset < s.blocks[blockIndex].logicalStart {
		return nil, io.EOF
	}

	block := s.blocks[blockIndex]
	data, err := s.loadBlockData(block)
	if err != nil {
		return nil, err
	}

	localOffset := offset - block.logicalStart
	end := localOffset + length
	if localOffset < 0 || end > int64(len(data)) {
		return nil, io.ErrUnexpectedEOF
	}
	return data[localOffset:end], nil
}

func (s *segment) truncateTo(size int64) error {
	if err := s.file.File.Truncate(size); err != nil {
		return err
	}
	s.file.size = size
	s.comp = nil

	if !s.compressed {
		s.logicalSize = size
		return nil
	}

	validBlocks := s.blocks[:0]
	var logicalSize int64 = headerSize
	for _, block := range s.blocks {
		if block.physicalEnd <= size {
			validBlocks = append(validBlocks, block)
			logicalSize = block.logicalEnd
		}
	}
	s.blocks = validBlocks
	s.logicalSize = logicalSize
	return nil
}

func (it *segmentIterator) loadNextCompressedBlock() error {
	if it.nextBlock >= len(it.blocks) {
		it.blockData = nil
		return nil
	}

	block := it.blocks[it.nextBlock]
	it.nextBlock++
	data, err := it.f.loadBlockData(block)
	if err != nil {
		return err
	}

	it.currentBlock = block
	it.blockData = data
	it.blockPos = 0
	it.offset = uint32(block.logicalStart)
	return nil
}

func (it *segmentIterator) nextCompressed() (record, error) {
	for {
		if it.blockData == nil {
			if err := it.loadNextCompressedBlock(); err != nil {
				return record{}, err
			}
			if it.blockData == nil {
				if it.truncateOffset < it.f.file.size {
					return record{}, errCorrupted
				}
				return record{}, ErrIterationDone
			}
		}

		if it.blockPos == len(it.blockData) {
			// The complete block passed record and record checksum validation.
			it.truncateOffset = it.currentBlock.physicalEnd
			it.blockData = nil
			continue
		}

		remaining := it.blockData[it.blockPos:]
		if len(remaining) < 6 {
			return record{}, errCorrupted
		}

		keySize := uint32(binary.LittleEndian.Uint16(remaining[:2]))
		valueSize := binary.LittleEndian.Uint32(remaining[2:6])
		recordType := recordTypePut
		if valueSize&(1<<31) != 0 {
			recordType = recordTypeDelete
			valueSize &^= 1 << 31
		}

		recordSize := encodedRecordSize(keySize + valueSize)
		if uint32(len(remaining)) < recordSize {
			return record{}, errCorrupted
		}

		data := make([]byte, recordSize)
		copy(data, remaining[:recordSize])

		checksum := binary.LittleEndian.Uint32(data[len(data)-4:])
		if checksum != crc32.ChecksumIEEE(data[:len(data)-4]) {
			return record{}, errCorrupted
		}

		offset := it.offset
		it.offset += recordSize
		it.blockPos += int(recordSize)

		return record{
			rtype:     recordType,
			segmentID: it.f.id,
			offset:    offset,
			data:      data,
			key:       data[6 : 6+keySize],
			value:     data[6+keySize : 6+keySize+valueSize],
		}, nil
	}
}
