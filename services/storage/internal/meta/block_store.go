package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrBlockNotFound 表示目标物理块文件不存在（如已被并发 Compaction + GC 物理回收）。
// 包装 ErrIO 以保持对上层 EIO 状态码映射与现有断言的向后兼容。
var ErrBlockNotFound = fmt.Errorf("%w: block not found", ErrIO)

// PhysicalBlockFile 描述本地物理块存储介质上的单个物理块文件元数据。
type PhysicalBlockFile struct {
	SliceID   uint64
	BlockIdx  uint32
	BlockSize uint32
	ModTime   time.Time
}

// BlockLister 定义物理块存储引擎可选实现的存量块扫描接口，用于后台 GC 孤儿块对账回收。
type BlockLister interface {
	// ListBlocksBefore 扫描并返回修改时间严格早于 cutoff 的物理块文件列表（当 limit > 0 时最多返回 limit 条）。
	ListBlocksBefore(ctx context.Context, cutoff time.Time, limit int) ([]PhysicalBlockFile, error)
}

// BlockStore 定义不可变数据块物理持久化接口。
type BlockStore interface {
	// PutBlock 将切片子块数据原子写入存储介质。
	PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
	// GetBlock 从物理块文件的指定偏移处精确读取 len(dst) 字节。
	GetBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error
	// DeleteBlock 物理删除对应的块文件，文件不存在时保持幂等成功。
	DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error
}

// StagedBlockStore 定义支持阶段化原子发布（先写入重命名至页缓存、按需批量同步落盘）的块存储扩展接口。
type StagedBlockStore interface {
	BlockStore
	// StageBlock 将切片子块数据写入同目录唯一临时文件并原子 Rename 至目标路径，返回待同步的父目录路径。
	StageBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) (parentDir string, err error)
	// SyncDirs 对给定的父目录路径集合按首次出现顺序去重后逐一执行目录级持久化 Sync。
	SyncDirs(ctx context.Context, dirs []string) error
	// SyncPending 将自上次成功刷盘以来累积的未刷盘物理块文件与目录变更刷入存储介质（并发调用合并等待）。
	SyncPending(ctx context.Context) error
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

// LocalFSBlockStoreOption 定义 LocalFSBlockStore 的可选配置项。
type LocalFSBlockStoreOption func(*LocalFSBlockStore)

// DirHandle 定义目录句柄持久化与关闭契约。
type DirHandle interface {
	Sync() error
	Close() error
}

// WithOpenDirHook 注入打开目录句柄的钩子（默认调用 os.Open）。
func WithOpenDirHook(fn func(dir string) (DirHandle, error)) LocalFSBlockStoreOption {
	return func(s *LocalFSBlockStore) {
		if fn != nil {
			s.openDirFn = fn
			s.hasCustomOpenDir = true
		}
	}
}

// WithStatHook 注入查询文件/目录状态的钩子（默认调用 os.Stat）。
func WithStatHook(fn func(name string) (os.FileInfo, error)) LocalFSBlockStoreOption {
	return func(s *LocalFSBlockStore) {
		if fn != nil {
			s.statFn = fn
		}
	}
}

type blockScanCursor struct {
	lvl1 string
	lvl2 string
	file string
}

// LocalFSBlockStore 实现基于本地 NVMe SSD 文件系统的两级散列目录存储引擎。
type LocalFSBlockStore struct {
	dataDir          string
	dirMu            sync.RWMutex
	syncedDirs       map[string]struct{}
	pendingSyncDirs  map[string]struct{}
	openDirFn        func(dir string) (DirHandle, error)
	hasCustomOpenDir bool
	statFn           func(name string) (os.FileInfo, error)
	scanMu           sync.Mutex
	scanCursor       blockScanCursor

	syncMu       sync.Mutex
	syncing      bool
	syncWaitCh   chan struct{}
	writeGen     uint64
	flushedGen   uint64
	pendingFiles map[string]uint64
	pendingDirs  map[string]uint64
}

var (
	_ BlockStore       = (*LocalFSBlockStore)(nil)
	_ StagedBlockStore = (*LocalFSBlockStore)(nil)
	_ BlockLister      = (*LocalFSBlockStore)(nil)
)

// NewLocalFSBlockStore 创建基于本地文件系统的 BlockStore 实例。
func NewLocalFSBlockStore(dataDir string, opts ...LocalFSBlockStoreOption) *LocalFSBlockStore {
	s := &LocalFSBlockStore{
		dataDir:         dataDir,
		syncedDirs:      make(map[string]struct{}),
		pendingSyncDirs: make(map[string]struct{}),
		openDirFn: func(dir string) (DirHandle, error) {
			return os.Open(dir)
		},
		statFn:       os.Stat,
		pendingFiles: make(map[string]uint64),
		pendingDirs:  make(map[string]uint64),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *LocalFSBlockStore) statDir(name string) (os.FileInfo, error) {
	if s.statFn != nil {
		return s.statFn(name)
	}
	return os.Stat(name)
}

func (s *LocalFSBlockStore) invalidateSyncedDir(targetDir string) {
	cleanTarget := filepath.Clean(targetDir)
	s.dirMu.Lock()
	delete(s.syncedDirs, cleanTarget)
	s.dirMu.Unlock()
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
func (s *LocalFSBlockStore) syncDir(dir string) error {
	openFn := s.openDirFn
	if openFn == nil {
		openFn = func(d string) (DirHandle, error) {
			return os.Open(d)
		}
	}
	f, err := openFn(dir)
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
	cleanTarget := filepath.Clean(targetDir)

	s.dirMu.RLock()
	_, fastSynced := s.syncedDirs[cleanTarget]
	noPending := len(s.pendingSyncDirs) == 0
	s.dirMu.RUnlock()
	if fastSynced && noPending {
		return nil
	}

	s.dirMu.Lock()
	defer s.dirMu.Unlock()

	if s.syncedDirs == nil {
		s.syncedDirs = make(map[string]struct{})
	}
	if s.pendingSyncDirs == nil {
		s.pendingSyncDirs = make(map[string]struct{})
	}

	if _, ok := s.syncedDirs[cleanTarget]; ok && len(s.pendingSyncDirs) == 0 {
		return nil
	}

	cleanBase := filepath.Clean(s.dataDir)

	var toCreate []string
	curr := cleanTarget
	for {
		fi, err := s.statDir(curr)
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
			fi, statErr := s.statDir(d)
			if statErr != nil || !fi.IsDir() {
				return fmt.Errorf("%w: path %s already exists and is not a directory: %v", ErrIO, d, statErr)
			}
		} else {
			parentDir := filepath.Dir(d)
			if parentDir != d {
				delete(s.syncedDirs, parentDir)
				s.pendingSyncDirs[parentDir] = struct{}{}
			}
			if d != cleanTarget {
				delete(s.syncedDirs, d)
				s.pendingSyncDirs[d] = struct{}{}
			}
		}
	}

	leafParent := filepath.Dir(cleanTarget)
	var ancestorsToSync []string
	reachedBase := false
	for a := leafParent; ; a = filepath.Dir(a) {
		if a == cleanBase {
			reachedBase = true
		}
		_, pending := s.pendingSyncDirs[a]
		if !reachedBase || a == cleanBase || pending {
			ancestorsToSync = append(ancestorsToSync, a)
		}
		if a == filepath.Dir(a) {
			break
		}
	}

	for i, j := 0, len(ancestorsToSync)-1; i < j; i, j = i+1, j-1 {
		ancestorsToSync[i], ancestorsToSync[j] = ancestorsToSync[j], ancestorsToSync[i]
	}

	for _, a := range ancestorsToSync {
		_, synced := s.syncedDirs[a]
		_, pending := s.pendingSyncDirs[a]
		if !synced || pending {
			if err := s.syncDir(a); err != nil {
				return err
			}
			delete(s.pendingSyncDirs, a)
			s.syncedDirs[a] = struct{}{}
		}
	}

	delete(s.pendingSyncDirs, cleanTarget)
	s.syncedDirs[cleanTarget] = struct{}{}
	return nil
}

// StageBlock 采用两级目录散列，将子块写入同目录唯一临时文件并原子重命名至目标路径（发布至 OS Page Cache），返回待同步的父目录路径。
func (s *LocalFSBlockStore) StageBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	blockSize := uint32(len(data))
	finalPath := s.blockPath(sliceID, blockIdx, blockSize)
	dir := s.blockDir(sliceID)

	if err := s.ensureDirSynced(dir); err != nil {
		return "", fmt.Errorf("%w: failed to create chunk directory %s: %v", ErrIO, dir, err)
	}

	tmpPattern := fmt.Sprintf(".tmp_%d_%d_*", sliceID, blockIdx)
	tmpFile, err := os.CreateTemp(dir, tmpPattern)
	if err != nil {
		// 防御性自愈：若已缓存的父目录被外部移除，剔除该目录缓存后重新创建同步并重试一次
		s.invalidateSyncedDir(dir)
		if syncErr := s.ensureDirSynced(dir); syncErr != nil {
			return "", fmt.Errorf("%w: failed to create chunk directory %s: %v", ErrIO, dir, syncErr)
		}
		tmpFile, err = os.CreateTemp(dir, tmpPattern)
		if err != nil {
			return "", fmt.Errorf("%w: failed to create temp block file: %v", ErrIO, err)
		}
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
			return "", fmt.Errorf("%w: failed to write block data: %v", ErrIO, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return "", err
	}

	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("%w: failed to close temp block file: %v", ErrIO, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", fmt.Errorf("%w: failed to rename temp block to %s: %v", ErrIO, finalPath, err)
	}

	s.recordPendingWrite(finalPath, dir)
	success = true
	return dir, nil
}

func (s *LocalFSBlockStore) recordPendingWrite(finalPath string, dir string) {
	cleanFile := filepath.Clean(finalPath)
	cleanDir := filepath.Clean(dir)
	s.syncMu.Lock()
	if s.pendingFiles == nil {
		s.pendingFiles = make(map[string]uint64)
	}
	if s.pendingDirs == nil {
		s.pendingDirs = make(map[string]uint64)
	}
	s.writeGen++
	gen := s.writeGen
	s.pendingFiles[cleanFile] = gen
	s.pendingDirs[cleanDir] = gen
	s.syncMu.Unlock()
}

func (s *LocalFSBlockStore) syncBlockFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("%w: failed to open block file %s for sync: %v", ErrIO, filePath, err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("%w: failed to sync block file %s: %v", ErrIO, filePath, err)
	}
	return nil
}

// SyncPending 将自上次刷盘以来累积的未刷盘物理块文件与目录变更刷入物理介质。
// 并发调用合并等待；若期间无新写入则直接返回 nil。
func (s *LocalFSBlockStore) SyncPending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.syncMu.Lock()
	targetGen := s.writeGen
	if s.flushedGen >= targetGen {
		s.syncMu.Unlock()
		return nil
	}

	for {
		if s.flushedGen >= targetGen {
			s.syncMu.Unlock()
			return nil
		}
		if !s.syncing {
			break
		}
		waitCh := s.syncWaitCh
		s.syncMu.Unlock()

		select {
		case <-waitCh:
		case <-ctx.Done():
			return ctx.Err()
		}

		s.syncMu.Lock()
	}

	for s.flushedGen < targetGen {
		if err := ctx.Err(); err != nil {
			s.syncMu.Unlock()
			return err
		}

		roundTargetGen := s.writeGen
		waitCh := make(chan struct{})
		s.syncing = true
		s.syncWaitCh = waitCh

		var filesToSync []string
		var dirsToSync []string
		useFallback := s.hasCustomOpenDir
		if useFallback {
			filesToSync = make([]string, 0, len(s.pendingFiles))
			for f := range s.pendingFiles {
				filesToSync = append(filesToSync, f)
			}
			slices.Sort(filesToSync)

			dirsToSync = make([]string, 0, len(s.pendingDirs))
			for d := range s.pendingDirs {
				dirsToSync = append(dirsToSync, d)
			}
			slices.Sort(dirsToSync)
		}
		s.syncMu.Unlock()

		syncErr := s.flushPendingRound(ctx, useFallback, filesToSync, dirsToSync)

		s.syncMu.Lock()
		if syncErr == nil {
			if roundTargetGen > s.flushedGen {
				s.flushedGen = roundTargetGen
			}
			for f, gen := range s.pendingFiles {
				if gen <= roundTargetGen {
					delete(s.pendingFiles, f)
				}
			}
			for d, gen := range s.pendingDirs {
				if gen <= roundTargetGen {
					delete(s.pendingDirs, d)
				}
			}
		}
		s.syncing = false
		close(waitCh)

		if syncErr != nil {
			s.syncMu.Unlock()
			return syncErr
		}
	}

	s.syncMu.Unlock()
	return nil
}

func (s *LocalFSBlockStore) flushPendingRound(ctx context.Context, forcePerFile bool, filesToSync []string, dirsToSync []string) error {
	if !forcePerFile {
		usedSyncFS, err := syncFSDataDir(s.dataDir)
		if usedSyncFS {
			return err
		}
		s.syncMu.Lock()
		filesToSync = make([]string, 0, len(s.pendingFiles))
		for f := range s.pendingFiles {
			filesToSync = append(filesToSync, f)
		}
		slices.Sort(filesToSync)

		dirsToSync = make([]string, 0, len(s.pendingDirs))
		for d := range s.pendingDirs {
			dirsToSync = append(dirsToSync, d)
		}
		slices.Sort(dirsToSync)
		s.syncMu.Unlock()
	}

	for _, f := range filesToSync {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.syncBlockFile(f); err != nil {
			return err
		}
	}

	for _, d := range dirsToSync {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.syncDir(d); err != nil {
			return err
		}
	}

	return nil
}

// SyncDirs 对给定的父目录路径集合按首次出现顺序去重（忽略空字符串）后逐一执行目录级持久化 Sync。
func (s *LocalFSBlockStore) SyncDirs(ctx context.Context, dirs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		cleanDir := filepath.Clean(dir)
		if _, ok := seen[cleanDir]; ok {
			continue
		}
		seen[cleanDir] = struct{}{}
		if err := s.syncDir(cleanDir); err != nil {
			return fmt.Errorf("%w: failed to sync chunk directory %s after rename: %v", ErrIO, dir, err)
		}
	}
	return nil
}

// PutBlock 采用两级目录散列，通过写入同目录唯一临时文件、原子重命名发布到目标路径、Sync 块文件并同步父目录。
func (s *LocalFSBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	dir, err := s.StageBlock(ctx, sliceID, blockIdx, data)
	if err != nil {
		return err
	}

	finalPath := s.blockPath(sliceID, blockIdx, uint32(len(data)))
	if err := s.syncBlockFile(finalPath); err != nil {
		_ = os.Remove(finalPath)
		return err
	}

	if err := s.SyncDirs(ctx, []string{dir}); err != nil {
		_ = os.Remove(finalPath)
		return err
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
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: failed to open block file %s: %v", ErrBlockNotFound, finalPath, err)
		}
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

	cleanFile := filepath.Clean(finalPath)
	s.syncMu.Lock()
	delete(s.pendingFiles, cleanFile)
	s.syncMu.Unlock()

	return nil
}

func compareBlockScanPos(l1, l2, f string, cur blockScanCursor) int {
	if l1 != cur.lvl1 {
		if l1 < cur.lvl1 {
			return -1
		}
		return 1
	}
	if l2 != cur.lvl2 {
		if l2 < cur.lvl2 {
			return -1
		}
		return 1
	}
	if f != cur.file {
		if f < cur.file {
			return -1
		}
		return 1
	}
	return 0
}

// ListBlocksBefore 遍历 chunks 下的两级散列目录，收集修改时间早于 cutoff 的物理块文件。
// 维护并发安全的跨批次扫描游标，有界扫描从上次位置之后继续推进并在到达目录树末尾时回绕，确保已引用前缀块不会饿死后续孤儿块。
// 遇到早于 cutoff 的陈旧 .tmp_* 临时文件时一并清理；若 chunks 根目录不存在则直接返回 nil, nil。
func (s *LocalFSBlockStore) ListBlocksBefore(ctx context.Context, cutoff time.Time, limit int) ([]PhysicalBlockFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	chunksRoot := filepath.Join(s.dataDir, "chunks")
	lvl1Entries, err := os.ReadDir(chunksRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: failed to read chunks root directory %s: %v", ErrIO, chunksRoot, err)
	}

	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	startCursor := s.scanCursor
	hasCursor := limit > 0 && startCursor != (blockScanCursor{})

	var result []PhysicalBlockFile
	var lastVisited blockScanCursor

	scanRange := func(afterStart bool, upToStart bool) error {
		for _, e1 := range lvl1Entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !e1.IsDir() {
				continue
			}
			l1Name := e1.Name()
			if afterStart && l1Name < startCursor.lvl1 {
				continue
			}
			if upToStart && l1Name > startCursor.lvl1 {
				return nil
			}

			dir1 := filepath.Join(chunksRoot, l1Name)
			lvl2Entries, err := os.ReadDir(dir1)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return fmt.Errorf("%w: failed to read hash bucket directory %s: %v", ErrIO, dir1, err)
			}

			for _, e2 := range lvl2Entries {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !e2.IsDir() {
					continue
				}
				l2Name := e2.Name()
				if afterStart && l1Name == startCursor.lvl1 && l2Name < startCursor.lvl2 {
					continue
				}
				if upToStart && l1Name == startCursor.lvl1 && l2Name > startCursor.lvl2 {
					return nil
				}

				dir2 := filepath.Join(dir1, l2Name)
				files, err := os.ReadDir(dir2)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						continue
					}
					return fmt.Errorf("%w: failed to read block group directory %s: %v", ErrIO, dir2, err)
				}

				for _, f := range files {
					if err := ctx.Err(); err != nil {
						return err
					}
					if f.IsDir() {
						continue
					}
					name := f.Name()
					cmp := compareBlockScanPos(l1Name, l2Name, name, startCursor)
					if afterStart && cmp <= 0 {
						continue
					}
					if upToStart && cmp > 0 {
						return nil
					}

					info, err := f.Info()
					if err != nil {
						if errors.Is(err, os.ErrNotExist) {
							continue
						}
						return fmt.Errorf("%w: failed to stat block file %s: %v", ErrIO, filepath.Join(dir2, name), err)
					}
					if !info.ModTime().Before(cutoff) {
						continue
					}

					if strings.HasPrefix(name, ".tmp") {
						_ = os.Remove(filepath.Join(dir2, name))
						continue
					}

					parts := strings.Split(name, "_")
					if len(parts) != 3 {
						continue
					}
					sliceID, err1 := strconv.ParseUint(parts[0], 10, 64)
					blockIdx64, err2 := strconv.ParseUint(parts[1], 10, 32)
					blockSize64, err3 := strconv.ParseUint(parts[2], 10, 32)
					if err1 != nil || err2 != nil || err3 != nil || sliceID == 0 {
						continue
					}

					result = append(result, PhysicalBlockFile{
						SliceID:   sliceID,
						BlockIdx:  uint32(blockIdx64),
						BlockSize: uint32(blockSize64),
						ModTime:   info.ModTime(),
					})
					lastVisited = blockScanCursor{lvl1: l1Name, lvl2: l2Name, file: name}
					if limit > 0 && len(result) >= limit {
						s.scanCursor = lastVisited
						return nil
					}
				}
			}
		}
		return nil
	}

	if err := scanRange(hasCursor, false); err != nil {
		return nil, err
	}
	if limit > 0 && len(result) >= limit {
		return result, nil
	}

	// 已到达目录树末尾但尚未凑满 limit：游标回绕至起点，若本轮曾从中间游标启动则补齐前缀区间以保证单轮完整扫描
	s.scanCursor = blockScanCursor{}
	if hasCursor {
		if err := scanRange(false, true); err != nil {
			return nil, err
		}
	}

	return result, nil
}
