package chunk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidObjectKey is returned when an S3 object key doesn't match the chunk naming convention.
	ErrInvalidObjectKey = errors.New("chunk: invalid object key format")
)

// FormatObjectKey formats an S3 object key according to the convention:
// chunks/<disk_id>/<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx>
// where chunk_prefix = fmt.Sprintf("%03d", chunk_indx/1000).
func FormatObjectKey(diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32) string {
	chunkPrefix := fmt.Sprintf("%03d", chunkIndx/1000)
	return fmt.Sprintf("chunks/%s/%s/%d/%d_%d", diskID, chunkPrefix, chunkIndx, sliceID, blockIndx)
}

// ParseObjectKey parses an S3 object key formatted as:
// chunks/<disk_id>/<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx>
func ParseObjectKey(key string) (diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "chunks" {
		return "", 0, 0, 0, fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	diskID = parts[1]
	if diskID == "" {
		return "", 0, 0, 0, fmt.Errorf("%w: empty disk id", ErrInvalidObjectKey)
	}
	chunkIdxParsed, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: invalid chunk index %q", ErrInvalidObjectKey, parts[3])
	}
	chunkIndx = uint32(chunkIdxParsed)

	sliceBlockParts := strings.Split(parts[4], "_")
	if len(sliceBlockParts) != 2 {
		return "", 0, 0, 0, fmt.Errorf("%w: invalid slice_block segment %q", ErrInvalidObjectKey, parts[4])
	}

	sliceIDParsed, err := strconv.ParseUint(sliceBlockParts[0], 10, 64)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: invalid slice id %q", ErrInvalidObjectKey, sliceBlockParts[0])
	}
	sliceID = sliceIDParsed

	blockIndxParsed, err := strconv.ParseUint(sliceBlockParts[1], 10, 32)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: invalid block index %q", ErrInvalidObjectKey, sliceBlockParts[1])
	}
	blockIndx = uint32(blockIndxParsed)

	return diskID, chunkIndx, sliceID, blockIndx, nil
}

// Block represents a 4MB physical block of a slice ready for S3 upload.
type Block struct {
	DiskID    string
	ChunkIndx uint32
	SliceID   uint64
	BlockIndx uint32
	Offset    uint32 // offset within slice payload
	Data      []byte
	Key       string
}

// BlockBatcher partitions slice data into 4MB physical blocks.
type BlockBatcher struct {
	blockSize int
}

// NewBlockBatcher creates a new BlockBatcher with default 4MB BlockSize (or optional custom block size).
func NewBlockBatcher(blockSize ...int) *BlockBatcher {
	bs := BlockSize
	if len(blockSize) > 0 && blockSize[0] > 0 {
		bs = blockSize[0]
	}
	return &BlockBatcher{blockSize: bs}
}

// BlockSize returns the configured block size in bytes.
func (b *BlockBatcher) BlockSize() int {
	return b.blockSize
}

// SplitSlice partitions a slice payload into 4MB blocks with valid S3 object keys.
func (b *BlockBatcher) SplitSlice(diskID string, chunkIndx uint32, sliceID uint64, data []byte) []Block {
	if len(data) == 0 {
		return nil
	}

	numBlocks := (len(data) + b.blockSize - 1) / b.blockSize
	blocks := make([]Block, 0, numBlocks)

	for i := 0; i < numBlocks; i++ {
		start := i * b.blockSize
		end := start + b.blockSize
		if end > len(data) {
			end = len(data)
		}
		blockIndx := uint32(i)
		key := FormatObjectKey(diskID, chunkIndx, sliceID, blockIndx)
		blocks = append(blocks, Block{
			DiskID:    diskID,
			ChunkIndx: chunkIndx,
			SliceID:   sliceID,
			BlockIndx: blockIndx,
			Offset:    uint32(start),
			Data:      data[start:end],
			Key:       key,
		})
	}

	return blocks
}

// SplitSliceFromStaging reads slice data from staging and splits it into blocks.
func (b *BlockBatcher) SplitSliceFromStaging(staging *StagingManager, diskID string, chunkIndx uint32, sliceID uint64) ([]Block, error) {
	data, err := staging.ReadSlice(diskID, sliceID)
	if err != nil {
		return nil, fmt.Errorf("read staging slice: %w", err)
	}
	return b.SplitSlice(diskID, chunkIndx, sliceID, data), nil
}
