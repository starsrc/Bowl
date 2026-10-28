package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// BlockStore 定义不可变数据块物理持久化接口。
type BlockStore interface {
	// PutBlock 将切片子块数据原子写入存储介质。
	PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
	// GetBlock 从物理块文件的指定偏移处精确读取 len(dst) 字节。
	GetBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error
	// DeleteBlock 物理删除对应的块文件，文件不存在时保持幂等成功。
	DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error
}

// SliceBlockInfo 表示一个 Slice 包含的物理 Block 索引与大小信息。
type SliceBlockInfo struct {
	BlockIdx  uint32
	BlockSize uint32
}

// SliceBlocks 根据 sliceSize 计算其切分出的所有物理 Block 列表。
// 若 sliceSize 为 0 则返回空切片。
func SliceBlocks(sliceSize uint32) []SliceBlockInfo {
	return SliceBlocksWithBlockSize(sliceSize, BlockSize)
}

// SliceBlocksWithBlockSize 根据给定的单块大小 defaultBlockSize 计算 sliceSize 切分出的所有物理 Block 列表。
func SliceBlocksWithBlockSize(sliceSize uint32, defaultBlockSize uint32) []SliceBlockInfo {
	if sliceSize == 0 || defaultBlockSize == 0 {
		return nil
	}

	fullBlocks := sliceSize / defaultBlockSize
	remainder := sliceSize % defaultBlockSize

	totalBlocks := fullBlocks
	if remainder > 0 {
		totalBlocks++
	}

	blocks := make([]SliceBlockInfo, 0, totalBlocks)
	for i := uint32(0); i < fullBlocks; i++ {
		blocks = append(blocks, SliceBlockInfo{
			BlockIdx:  i,
			BlockSize: defaultBlockSize,
		})
	}
	if remainder > 0 {
		blocks = append(blocks, SliceBlockInfo{
			BlockIdx:  fullBlocks,
			BlockSize: remainder,
		})
	}

	return blocks
}

// BlockLocation 表示对 Slice 进行区间读写时所定位到的具体物理 Block 及其块内偏移范围。
type BlockLocation struct {
	BlockIdx  uint32 // 物理块索引（从 0 开始）
	BlockSize uint32 // 该物理块的总大小（<= 4 MiB）
	BlockOff  uint32 // 请求在当前物理块内的起始偏移
	Length    uint32 // 请求在当前物理块内的读取/操作字节数
}

// LocateBlocks 根据 sliceSize、sliceOff 与 reqLen 计算所涉物理 Block 的范围列表。
// 若 reqLen == 0 则返回空切片。
// 若 sliceOff + reqLen > sliceSize 或参数越界，返回 ErrInvalidArgument。
func LocateBlocks(sliceSize uint32, sliceOff uint32, reqLen uint32) ([]BlockLocation, error) {
	return LocateBlocksWithBlockSize(sliceSize, sliceOff, reqLen, BlockSize)
}

// LocateBlocksWithBlockSize 根据指定物理块大小 defaultBlockSize 计算所涉 Block 范围。
func LocateBlocksWithBlockSize(sliceSize uint32, sliceOff uint32, reqLen uint32, defaultBlockSize uint32) ([]BlockLocation, error) {
	if defaultBlockSize == 0 {
		return nil, fmt.Errorf("%w: defaultBlockSize must be greater than 0", ErrInvalidArgument)
	}
	if sliceOff > sliceSize {
		return nil, fmt.Errorf("%w: sliceOff %d exceeds sliceSize %d", ErrInvalidArgument, sliceOff, sliceSize)
	}
	if reqLen == 0 {
		return nil, nil
	}
	if uint64(sliceOff)+uint64(reqLen) > uint64(sliceSize) {
		return nil, fmt.Errorf("%w: slice offset %d + req len %d exceeds slice size %d", ErrInvalidArgument, sliceOff, reqLen, sliceSize)
	}

	startIdx := sliceOff / defaultBlockSize
	startOff := sliceOff % defaultBlockSize
	remaining := reqLen
	idx := startIdx
	curOff := startOff

	lastBlockIdx := (sliceSize - 1) / defaultBlockSize
	var locs []BlockLocation

	for remaining > 0 {
		var curBlockSize uint32
		if idx == lastBlockIdx {
			curBlockSize = sliceSize - idx*defaultBlockSize
		} else {
			curBlockSize = defaultBlockSize
		}

		availableInBlock := curBlockSize - curOff
		readLen := remaining
		if readLen > availableInBlock {
			readLen = availableInBlock
		}

		locs = append(locs, BlockLocation{
			BlockIdx:  idx,
			BlockSize: curBlockSize,
			BlockOff:  curOff,
			Length:    readLen,
		})

		remaining -= readLen
		idx++
		curOff = 0
	}

	return locs, nil
}

// LocalFSBlockStore 实现基于本地 NVMe SSD 文件系统的两级散列目录存储引擎。
type LocalFSBlockStore struct {
	dataDir    string
	dirMu      sync.Mutex
	syncedDirs map[string]struct{}
}

var _ BlockStore = (*LocalFSBlockStore)(nil)

// NewLocalFSBlockStore 创建基于本地文件系统的 BlockStore 实例。
func NewLocalFSBlockStore(dataDir string) *LocalFSBlockStore {
	return &LocalFSBlockStore{
		dataDir:    dataDir,
		syncedDirs: make(map[string]struct{}),
	}
}

// blockDir 计算指定 sliceID 对应的两级十六进制散列父目录路径。
// 第一级目录：%02X = sliceID % 256（256 个十六进制桶）
// 第二级目录：%v   = sliceID / 1000 / 1000（每百万 ID 一组）
func (s *LocalFSBlockStore) blockDir(sliceID uint64) string {
	return filepath.Join(
		s.dataDir,
		"chunks",
		fmt.Sprintf("%02X", sliceID%256),
		fmt.Sprintf("%d", sliceID/1000/1000),
	)
}

// blockPath 计算指定块的完整目标物理文件路径。
// 文件名格式：<slice_id>_<block_idx>_<block_size>
func (s *LocalFSBlockStore) blockPath(sliceID uint64, blockIdx uint32, blockSize uint32) string {
	return filepath.Join(
		s.blockDir(sliceID),
		fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize),
	)
}

// syncDir 打开指定目录并调用 Sync 将其目录项变更持久化至介质。
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("%w: failed to open directory %s for sync: %v", ErrIO, dir, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("%w: failed to sync directory %s: %v", ErrIO, dir, err)
	}
	return nil
}

// ensureDirSynced 保证 targetDir 及其所有祖先目录项均已持久化刷盘。
func (s *LocalFSBlockStore) ensureDirSynced(targetDir string) error {
	s.dirMu.Lock()
	defer s.dirMu.Unlock()

	if s.syncedDirs == nil {
		s.syncedDirs = make(map[string]struct{})
	}

	cleanBase := filepath.Clean(s.dataDir)
	cleanTarget := filepath.Clean(targetDir)

	if _, ok := s.syncedDirs[cleanTarget]; ok {
		fi, err := os.Stat(cleanTarget)
		if err == nil {
			if fi.IsDir() {
				return nil
			}
			return fmt.Errorf("%w: path %s exists but is not a directory", ErrIO, cleanTarget)
		}
		delete(s.syncedDirs, cleanTarget)
	}

	var toCreate []string
	curr := cleanTarget
	for {
		fi, err := os.Stat(curr)
		if err == nil {
			if !fi.IsDir() {
				return fmt.Errorf("%w: path %s exists but is not a directory", ErrIO, curr)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: failed to stat %s: %v", ErrIO, curr, err)
		}
		toCreate = append(toCreate, curr)
		parent := filepath.Dir(curr)
		if parent == curr {
			return fmt.Errorf("%w: reached filesystem root without existing directory: %s", ErrIO, cleanTarget)
		}
		curr = parent
	}

	for i, j := 0, len(toCreate)-1; i < j; i, j = i+1, j-1 {
		toCreate[i], toCreate[j] = toCreate[j], toCreate[i]
	}

	for _, d := range toCreate {
		if err := os.Mkdir(d, 0o755); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("%w: failed to create directory %s: %v", ErrIO, d, err)
			}
			fi, statErr := os.Stat(d)
			if statErr != nil || !fi.IsDir() {
				return fmt.Errorf("%w: path %s already exists and is not a directory: %v", ErrIO, d, statErr)
			}
		}
	}

	leafParent := filepath.Dir(cleanTarget)
	var ancestorsToSync []string
	for a := leafParent; ; a = filepath.Dir(a) {
		ancestorsToSync = append(ancestorsToSync, a)
		if a == cleanBase || a == filepath.Dir(a) {
			break
		}
	}

	for i, j := 0, len(ancestorsToSync)-1; i < j; i, j = i+1, j-1 {
		ancestorsToSync[i], ancestorsToSync[j] = ancestorsToSync[j], ancestorsToSync[i]
	}

	for _, a := range ancestorsToSync {
		if _, synced := s.syncedDirs[a]; !synced {
			if err := syncDir(a); err != nil {
				return err
			}
			s.syncedDirs[a] = struct{}{}
		}
	}

	s.syncedDirs[cleanTarget] = struct{}{}
	return nil
}

// PutBlock 采用两级目录散列，通过写入同目录唯一临时文件、Sync 刷盘并原子重命名发布到目标路径。
func (s *LocalFSBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	blockSize := uint32(len(data))
	finalPath := s.blockPath(sliceID, blockIdx, blockSize)
	dir := s.blockDir(sliceID)

	if err := s.ensureDirSynced(dir); err != nil {
		return fmt.Errorf("%w: failed to create chunk directory %s: %v", ErrIO, dir, err)
	}

	tmpFile, err := os.CreateTemp(dir, fmt.Sprintf(".tmp_%d_%d_*", sliceID, blockIdx))
	if err != nil {
		return fmt.Errorf("%w: failed to create temp block file: %v", ErrIO, err)
	}
	tmpPath := tmpFile.Name()

	var success bool
	defer func() {
		if !success {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if len(data) > 0 {
		if _, err := tmpFile.Write(data); err != nil {
			return fmt.Errorf("%w: failed to write block data: %v", ErrIO, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("%w: failed to sync block file: %v", ErrIO, err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("%w: failed to close temp block file: %v", ErrIO, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("%w: failed to rename temp block to %s: %v", ErrIO, finalPath, err)
	}

	success = true

	if err := syncDir(dir); err != nil {
		_ = os.Remove(finalPath)
		return fmt.Errorf("%w: failed to sync chunk directory %s after rename: %v", ErrIO, dir, err)
	}

	return nil
}

// GetBlock 从目标物理块文件的 offset 处精确读取 len(dst) 字节。
// 文件不存在或读取字节不足时返回包含 ErrIO 的错误。
func (s *LocalFSBlockStore) GetBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dst) == 0 {
		return nil
	}

	reqEnd := uint64(offset) + uint64(len(dst))
	if reqEnd > uint64(blockSize) {
		return fmt.Errorf("%w: read range [%d, %d) exceeds block size %d", ErrIO, offset, reqEnd, blockSize)
	}

	finalPath := s.blockPath(sliceID, blockIdx, blockSize)
	f, err := os.Open(finalPath)
	if err != nil {
		return fmt.Errorf("%w: failed to open block file %s: %v", ErrIO, finalPath, err)
	}
	defer f.Close()

	if err := ctx.Err(); err != nil {
		return err
	}

	n, err := f.ReadAt(dst, int64(offset))
	if n < len(dst) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return fmt.Errorf("%w: failed to read requested bytes (read %d/%d): %v", ErrIO, n, len(dst), err)
	}

	return nil
}

// DeleteBlock 物理删除对应 Block 文件，对文件不存在保持幂等成功。
func (s *LocalFSBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	finalPath := s.blockPath(sliceID, blockIdx, blockSize)
	if err := os.Remove(finalPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("%w: failed to remove block file %s: %v", ErrIO, finalPath, err)
	}

	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("%w: failed to sync directory after removing block file %s: %v", ErrIO, finalPath, err)
	}

	return nil
}
