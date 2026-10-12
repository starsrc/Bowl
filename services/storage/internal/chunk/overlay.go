package chunk

import (
	"fmt"
)

// SliceReader abstracts reading raw slice payload bytes by slice ID.
// StagingManager satisfies this interface.
type SliceReader interface {
	ReadSlice(diskID string, sliceID uint64) ([]byte, error)
}

// RangeSliceReader abstracts reading sub-ranges of slice payload bytes by slice ID.
// chunkReaderWrapper and StagingManager satisfy this interface.
type RangeSliceReader interface {
	ReadSliceRange(diskID string, sliceID uint64, off uint32, length uint32) ([]byte, error)
}

// ReconstituteRange reconstructs byte data for a specific chunk interval [start, start+size)
// by layering slices in chronological order over an initial sparse hole of zeros (0x00).
func ReconstituteRange(diskID string, slices []Slice, start uint32, size uint32, reader SliceReader) ([]byte, error) {
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if err := ReconstituteRangeTo(buf, diskID, slices, start, size, reader); err != nil {
		return nil, err
	}
	return buf, nil
}

// ReconstituteRangeTo reconstructs byte data for [start, start+size) directly into dst.
// dst must have length of at least size.
func ReconstituteRangeTo(dst []byte, diskID string, slices []Slice, start uint32, size uint32, reader SliceReader) error {
	if size == 0 {
		return nil
	}
	if uint64(start)+uint64(size) > ChunkSize {
		return fmt.Errorf("%w: range [%d, %d) exceeds chunk size %d", ErrOutOfBounds, start, start+size, ChunkSize)
	}
	if uint32(len(dst)) < size {
		return fmt.Errorf("%w: destination buffer length %d is less than requested size %d", ErrTruncatedBuffer, len(dst), size)
	}

	reqEnd := start + size

	rangeReader, hasRangeReader := reader.(RangeSliceReader)

	var cache map[uint64][]byte
	getSliceData := func(id uint64) ([]byte, error) {
		if cache != nil {
			if data, ok := cache[id]; ok {
				return data, nil
			}
		}
		data, err := reader.ReadSlice(diskID, id)
		if err != nil {
			return nil, err
		}
		if cache == nil {
			cache = make(map[uint64][]byte)
		}
		cache[id] = data
		return data, nil
	}

	for _, s := range slices {
		sliceEnd := s.Pos + s.Len
		// Check overlap between [s.Pos, sliceEnd) and [start, reqEnd)
		overlapStart := start
		if s.Pos > overlapStart {
			overlapStart = s.Pos
		}
		overlapEnd := reqEnd
		if sliceEnd < overlapEnd {
			overlapEnd = sliceEnd
		}

		if overlapStart >= overlapEnd {
			// No intersection
			continue
		}

		overlapLen := overlapEnd - overlapStart
		dstStart := overlapStart - start
		dstEnd := dstStart + overlapLen

		sliceOff := s.Off + (overlapStart - s.Pos)

		if hasRangeReader {
			subPayload, err := rangeReader.ReadSliceRange(diskID, s.ID, sliceOff, overlapLen)
			if err != nil {
				return fmt.Errorf("chunk: failed to read slice %d range: %w", s.ID, err)
			}
			if uint32(len(subPayload)) < overlapLen {
				return fmt.Errorf("chunk: corrupted slice payload %d: range length %d is less than required %d", s.ID, len(subPayload), overlapLen)
			}
			copy(dst[dstStart:dstEnd], subPayload[:overlapLen])
		} else {
			srcEnd := sliceOff + overlapLen
			payload, err := getSliceData(s.ID)
			if err != nil {
				return fmt.Errorf("chunk: failed to read slice %d: %w", s.ID, err)
			}

			if uint64(len(payload)) < uint64(srcEnd) {
				return fmt.Errorf("chunk: corrupted slice payload %d: length %d is less than required %d", s.ID, len(payload), srcEnd)
			}

			copy(dst[dstStart:dstEnd], payload[sliceOff:srcEnd])
		}
	}

	return nil
}
