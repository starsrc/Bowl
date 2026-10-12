package object

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrInvalidObjectKey 当对象键不符合规范时返回该错误
	ErrInvalidObjectKey = errors.New("无效的对象键格式")
)

// FormatChunkKey 根据多租户命名规范格式化 S3 对象键：
// chunks/<disk_id>/<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx>
// 其中 chunk_prefix 计算规则为 fmt.Sprintf("%03d", chunk_indx/1000)
func FormatChunkKey(diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32) string {
	chunkPrefix := fmt.Sprintf("%03d", chunkIndx/1000)
	return fmt.Sprintf("chunks/%s/%s/%d/%d_%d", diskID, chunkPrefix, chunkIndx, sliceID, blockIndx)
}

// FormatObjectKey 是 FormatChunkKey 的同义别名，便于与现有代码保持命名兼容
func FormatObjectKey(diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32) string {
	return FormatChunkKey(diskID, chunkIndx, sliceID, blockIndx)
}

// ParseChunkKey 解析格式为 chunks/<disk_id>/<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx> 的对象键
func ParseChunkKey(key string) (diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "chunks" {
		return "", 0, 0, 0, fmt.Errorf("%w: %q", ErrInvalidObjectKey, key)
	}
	diskID = parts[1]
	if diskID == "" {
		return "", 0, 0, 0, fmt.Errorf("%w: 虚拟磁盘 ID 不能为空", ErrInvalidObjectKey)
	}

	chunkIdxParsed, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: 无效的 chunk 索引 %q", ErrInvalidObjectKey, parts[3])
	}
	chunkIndx = uint32(chunkIdxParsed)

	// 校验 chunk_prefix 是否与 chunkIndx 严格匹配
	expectedPrefix := fmt.Sprintf("%03d", chunkIndx/1000)
	if parts[2] != expectedPrefix {
		return "", 0, 0, 0, fmt.Errorf("%w: chunk 前缀 %q 与索引 %d 不匹配 (期望 %q)", ErrInvalidObjectKey, parts[2], chunkIndx, expectedPrefix)
	}

	sliceBlockParts := strings.Split(parts[4], "_")
	if len(sliceBlockParts) != 2 {
		return "", 0, 0, 0, fmt.Errorf("%w: 无效的 slice_block 段 %q", ErrInvalidObjectKey, parts[4])
	}

	sliceIDParsed, err := strconv.ParseUint(sliceBlockParts[0], 10, 64)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: 无效的 slice ID %q", ErrInvalidObjectKey, sliceBlockParts[0])
	}
	sliceID = sliceIDParsed

	blockIndxParsed, err := strconv.ParseUint(sliceBlockParts[1], 10, 32)
	if err != nil {
		return "", 0, 0, 0, fmt.Errorf("%w: 无效的 block 索引 %q", ErrInvalidObjectKey, sliceBlockParts[1])
	}
	blockIndx = uint32(blockIndxParsed)

	return diskID, chunkIndx, sliceID, blockIndx, nil
}

// ParseObjectKey 是 ParseChunkKey 的同义别名
func ParseObjectKey(key string) (diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32, err error) {
	return ParseChunkKey(key)
}

// DiskPrefix 返回指定磁盘在对象存储中的根前缀：chunks/<disk_id>/
func DiskPrefix(diskID string) string {
	return fmt.Sprintf("chunks/%s/", diskID)
}

// ChunkPrefix 返回指定磁盘及 chunk 所属分桶目录前缀：chunks/<disk_id>/<chunk_prefix>/
func ChunkPrefix(diskID string, chunkIndx uint32) string {
	return fmt.Sprintf("chunks/%s/%03d/", diskID, chunkIndx/1000)
}

// FormatRelativeChunkKey 格式化磁盘作用域内的相对对象键：<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx>
func FormatRelativeChunkKey(chunkIndx uint32, sliceID uint64, blockIndx uint32) string {
	chunkPrefix := fmt.Sprintf("%03d", chunkIndx/1000)
	return fmt.Sprintf("%s/%d/%d_%d", chunkPrefix, chunkIndx, sliceID, blockIndx)
}

// ParseRelativeChunkKey 解析磁盘作用域内的相对对象键
func ParseRelativeChunkKey(key string) (chunkIndx uint32, sliceID uint64, blockIndx uint32, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("%w: 相对键格式不正确 %q", ErrInvalidObjectKey, key)
	}

	chunkIdxParsed, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: 无效的 chunk 索引 %q", ErrInvalidObjectKey, parts[1])
	}
	chunkIndx = uint32(chunkIdxParsed)

	expectedPrefix := fmt.Sprintf("%03d", chunkIndx/1000)
	if parts[0] != expectedPrefix {
		return 0, 0, 0, fmt.Errorf("%w: 前缀 %q 与索引 %d 不匹配 (期望 %q)", ErrInvalidObjectKey, parts[0], chunkIndx, expectedPrefix)
	}

	sliceBlockParts := strings.Split(parts[2], "_")
	if len(sliceBlockParts) != 2 {
		return 0, 0, 0, fmt.Errorf("%w: 无效的 slice_block 段 %q", ErrInvalidObjectKey, parts[2])
	}

	sliceIDParsed, err := strconv.ParseUint(sliceBlockParts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: 无效的 slice ID %q", ErrInvalidObjectKey, sliceBlockParts[0])
	}
	sliceID = sliceIDParsed

	blockIndxParsed, err := strconv.ParseUint(sliceBlockParts[1], 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: 无效的 block 索引 %q", ErrInvalidObjectKey, sliceBlockParts[1])
	}
	blockIndx = uint32(blockIndxParsed)

	return chunkIndx, sliceID, blockIndx, nil
}

// WithDiskPrefix 返回以特定磁盘根路径 chunks/<disk_id>/ 为作用域的前缀存储适配器
func WithDiskPrefix(os ObjectStorage, diskID string) ObjectStorage {
	return WithPrefix(os, DiskPrefix(diskID))
}
