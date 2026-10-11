package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// SliceBytes is the fixed byte size of a 24-byte serialized Slice in big-endian format.
	SliceBytes = 24
	// ChunkSize defines the maximum logical size of a chunk (64 MiB).
	ChunkSize = 64 * 1024 * 1024
	// BlockSize defines the physical object block size (4 MiB).
	BlockSize = 4 * 1024 * 1024
)

var (
	// ErrTruncatedBuffer indicates the binary buffer is incomplete or not an exact multiple of 24 bytes.
	ErrTruncatedBuffer = errors.New("chunk: buffer is truncated or not a multiple of 24 bytes")

	// ErrOutOfBounds indicates slice fields violate boundary constraints.
	ErrOutOfBounds = errors.New("chunk: slice offset or length exceeds bounds")
)

// Slice represents an immutable byte interval within a 64 MiB Chunk.
// Binary format (24 bytes, big-endian):
//
//	Pos:  uint32 (bytes 0..3)   - Byte offset within the 64 MiB Chunk [0, ChunkSize)
//	ID:   uint64 (bytes 4..11)  - Globally unique slice identifier
//	Size: uint32 (bytes 12..15) - Total payload size of the slice
//	Off:  uint32 (bytes 16..19) - Offset within the slice payload [0, Size)
//	Len:  uint32 (bytes 20..23) - Length of valid slice data [0, Size - Off]
type Slice struct {
	Pos  uint32
	ID   uint64
	Size uint32
	Off  uint32
	Len  uint32
}

// Validate checks whether the slice invariants and boundaries are satisfied.
func (s Slice) Validate() error {
	// Guard against integer overflow and ensure slice reference does not exceed payload size
	if uint64(s.Off)+uint64(s.Len) > uint64(s.Size) {
		return fmt.Errorf("%w: off (%d) + len (%d) > size (%d)", ErrOutOfBounds, s.Off, s.Len, s.Size)
	}
	// Guard against integer overflow and ensure slice does not exceed 64 MiB Chunk boundary
	if uint64(s.Pos)+uint64(s.Len) > ChunkSize {
		return fmt.Errorf("%w: pos (%d) + len (%d) > chunk size (%d)", ErrOutOfBounds, s.Pos, s.Len, ChunkSize)
	}
	return nil
}

// EncodeSlice serializes a single Slice into a 24-byte big-endian buffer.
func EncodeSlice(s Slice) []byte {
	return AppendSlice(make([]byte, 0, SliceBytes), s)
}

// AppendSlice appends the 24-byte big-endian binary encoding of s to dst and returns the extended slice.
func AppendSlice(dst []byte, s Slice) []byte {
	dst = binary.BigEndian.AppendUint32(dst, s.Pos)
	dst = binary.BigEndian.AppendUint64(dst, s.ID)
	dst = binary.BigEndian.AppendUint32(dst, s.Size)
	dst = binary.BigEndian.AppendUint32(dst, s.Off)
	dst = binary.BigEndian.AppendUint32(dst, s.Len)
	return dst
}

// DecodeSlice decodes a single 24-byte buffer into a Slice and validates boundary invariants.
func DecodeSlice(data []byte) (Slice, error) {
	if len(data) < SliceBytes {
		return Slice{}, fmt.Errorf("%w: expected %d bytes, got %d", ErrTruncatedBuffer, SliceBytes, len(data))
	}
	s := Slice{
		Pos:  binary.BigEndian.Uint32(data[0:4]),
		ID:   binary.BigEndian.Uint64(data[4:12]),
		Size: binary.BigEndian.Uint32(data[12:16]),
		Off:  binary.BigEndian.Uint32(data[16:20]),
		Len:  binary.BigEndian.Uint32(data[20:24]),
	}
	if err := s.Validate(); err != nil {
		return Slice{}, err
	}
	return s, nil
}

// DecodeSlices deserializes raw bytes into a slice of Slice structs, enforcing boundary checks on each slice.
func DecodeSlices(data []byte) ([]Slice, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data)%SliceBytes != 0 {
		return nil, fmt.Errorf("%w: buffer length %d is not a multiple of %d", ErrTruncatedBuffer, len(data), SliceBytes)
	}
	count := len(data) / SliceBytes
	slices := make([]Slice, 0, count)
	for i := 0; i < len(data); i += SliceBytes {
		s, err := DecodeSlice(data[i : i+SliceBytes])
		if err != nil {
			return nil, fmt.Errorf("corrupted slice at index %d: %w", i/SliceBytes, err)
		}
		slices = append(slices, s)
	}
	return slices, nil
}
