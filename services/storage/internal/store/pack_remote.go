package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// 按需取块：一个数据包可以暂时不在本地，只在 packs/ 里留一个占位文件
// （<pack>.pack.remote，记录内容哈希与大小）。读到这种包时：
//
//  1. 先向远端按范围取这一次要读的那一段，立刻满足本次读；
//  2. 同时在后台把整个包下回来，校验后改名就位并删掉占位文件。
//
// 占位文件和真实包放在同一个目录、用同一个编号，所以 Checkpoint / Fork 这些靠硬链接
// packs/ 目录工作的机制可以原样带着它走；包补齐之后引擎看到的就是一个普通的包。

const remotePackSuffix = ".remote"

// PackSource 按内容哈希从远端取数据包。Options.PackSource 为 nil 的盘遇到占位包时读取失败。
type PackSource interface {
	// FetchPack 把整个包下载到 dst（dst 不存在），并保证内容与 sha256 一致。
	FetchPack(ctx context.Context, sha256 string, size int64, dst string) error
	// ReadPackRange 读取包内 [off, off+len(dst)) 这一段。
	ReadPackRange(ctx context.Context, sha256 string, off int64, dst []byte) error
}

// RemotePackStub 是占位文件的内容。
type RemotePackStub struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// RemotePackStubName 返回数据包 packName（形如 0000000000000001.pack）对应的占位文件名。
func RemotePackStubName(packName string) string { return packName + remotePackSuffix }

// PackNameOfStub 判断 name 是否为占位文件名，并返回它代表的数据包文件名。
func PackNameOfStub(name string) (string, bool) {
	packName, ok := strings.CutSuffix(name, remotePackSuffix)
	return packName, ok && strings.HasSuffix(packName, ".pack")
}

// WriteRemotePackStub 在 packsDir 下为 packName 写入占位文件。
func WriteRemotePackStub(packsDir, packName string, stub RemotePackStub) error {
	raw, err := json.Marshal(stub)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(packsDir, RemotePackStubName(packName)), raw, 0o644)
}

// ReadRemotePackStub 读取占位文件。
func ReadRemotePackStub(path string) (RemotePackStub, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return RemotePackStub{}, err
	}
	var stub RemotePackStub
	if err := json.Unmarshal(raw, &stub); err != nil || stub.SHA256 == "" || stub.Size <= 0 {
		return RemotePackStub{}, fmt.Errorf("%w: malformed remote pack stub %s", ErrInvalidValue, path)
	}
	return stub, nil
}

func (s *blobStore) stubPath(packID uint64) string {
	return packFilePath(s.dir, packID) + remotePackSuffix
}

// isRemotePack 判断该包是否仍只有占位文件。
func (s *blobStore) isRemotePack(packID uint64) bool {
	if _, err := os.Stat(packFilePath(s.dir, packID)); err == nil {
		return false
	}
	_, err := os.Stat(s.stubPath(packID))
	return err == nil
}

// remotePackIDs 返回当前仍未下载的包编号（升序）。
func (s *blobStore) remotePackIDs() []uint64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	var ids []uint64
	for _, entry := range entries {
		packName, ok := PackNameOfStub(entry.Name())
		if !ok {
			continue
		}
		if id, err := parsePackID(packName); err == nil && id > 0 && s.isRemotePack(id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// readRemoteRange 处理"包文件不在本地"的读：handled 为 false 表示这个包根本没有占位文件，
// 调用方应按原来的文件不存在错误处理。
func (s *blobStore) readRemoteRange(ctx context.Context, packID uint64, packOffset int64, dst []byte) (handled bool, err error) {
	stub, err := ReadRemotePackStub(s.stubPath(packID))
	if errors.Is(err, os.ErrNotExist) {
		// 占位文件刚被后台补齐流程删掉：包已经就位，按本地文件再读一次。
		if file, openErr := s.getOrOpenPackFile(packID); openErr == nil {
			n, readErr := file.ReadAt(dst, packOffset)
			if n == len(dst) {
				return true, nil
			}
			return true, readErr
		}
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if s.source == nil {
		return true, fmt.Errorf("%w: pack %s is not downloaded and no remote source is configured", ErrIO, packFileName(packID))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.hydrateAsync(packID)
	if packOffset+int64(len(dst)) > stub.Size {
		return true, fmt.Errorf("%w: read past end of remote pack %s", ErrIO, packFileName(packID))
	}
	if err := s.source.ReadPackRange(ctx, stub.SHA256, packOffset, dst); err != nil {
		return true, fmt.Errorf("%w: fetch block from remote pack %s: %w", ErrIO, packFileName(packID), err)
	}
	s.metrics.incRemoteBlockReads()
	return true, nil
}

// hydrateAsync 在后台补齐一个包；同一个包同时只有一个下载。
func (s *blobStore) hydrateAsync(packID uint64) {
	s.hydrMu.Lock()
	if s.hydrCtx == nil || s.hydrCtx.Err() != nil || s.hydrFlights[packID] != nil {
		s.hydrMu.Unlock()
		return
	}
	s.hydrWG.Add(1)
	s.hydrMu.Unlock()
	go func() {
		defer s.hydrWG.Done()
		if err := s.hydrate(packID); err != nil && s.logger != nil && s.hydrCtx.Err() == nil {
			s.logger.Warn("remote pack hydrate failed", "pack_id", packID, "err", err)
		}
	}()
}

// hydrate 把一个占位包下载到本地并就位；已有同包的下载在跑时等它结束。
func (s *blobStore) hydrate(packID uint64) error {
	s.hydrMu.Lock()
	if s.hydrCtx == nil {
		s.hydrMu.Unlock()
		return ErrClosed
	}
	if flight := s.hydrFlights[packID]; flight != nil {
		s.hydrMu.Unlock()
		<-flight
		return nil
	}
	flight := make(chan struct{})
	s.hydrFlights[packID] = flight
	ctx := s.hydrCtx
	s.hydrMu.Unlock()
	defer func() {
		s.hydrMu.Lock()
		delete(s.hydrFlights, packID)
		s.hydrMu.Unlock()
		close(flight)
	}()

	stubPath := s.stubPath(packID)
	stub, err := ReadRemotePackStub(stubPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil // 已经就位，或这个包已被回收
	}
	if err != nil {
		return err
	}
	if s.source == nil {
		return fmt.Errorf("%w: no remote source", ErrIO)
	}
	tmp := filepath.Join(s.dir, ".fetch-"+packFileName(packID))
	_ = os.Remove(tmp)
	if err := s.source.FetchPack(ctx, stub.SHA256, stub.Size, tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	// 就位与"这个包被回收"互斥：占位文件已不在说明包在下载期间被删了，丢弃下载结果。
	s.hydrMu.Lock()
	defer s.hydrMu.Unlock()
	if _, statErr := os.Stat(stubPath); statErr != nil {
		_ = os.Remove(tmp)
		return nil
	}
	if err := os.Rename(tmp, packFilePath(s.dir, packID)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	_ = os.Remove(stubPath)
	s.mu.Lock()
	s.markDirDirtyLocked()
	s.mu.Unlock()
	s.metrics.incRemotePacksHydrated()
	return nil
}

// hydrateAll 依次补齐所有占位包，直到全部就位、出错或被取消。
func (s *blobStore) hydrateAll(ctx context.Context) error {
	for _, packID := range s.remotePackIDs() {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := s.hydrate(packID); err != nil {
			return err
		}
	}
	if left := len(s.remotePackIDs()); left > 0 {
		return fmt.Errorf("%w: %d remote packs still not downloaded", ErrIO, left)
	}
	return nil
}

// stopHydration 取消在途下载并等待它们退出。
func (s *blobStore) stopHydration() {
	s.hydrMu.Lock()
	cancel := s.hydrCancel
	s.hydrMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.hydrWG.Wait()
}

// checkpointPackNames 返回切面里应当硬链接的文件名：包在本地就链包，否则链它的占位文件。
// 调用方需持有 hydrMu，保证判断与链接之间包不会就位。
func (s *blobStore) checkpointPackNamesLocked(packIDs []uint64) []string {
	names := make([]string, 0, len(packIDs))
	for _, id := range packIDs {
		if id == 0 {
			continue
		}
		name := packFileName(id)
		if _, err := os.Stat(filepath.Join(s.dir, name)); err != nil {
			if _, stubErr := os.Stat(filepath.Join(s.dir, RemotePackStubName(name))); stubErr == nil {
				name = RemotePackStubName(name)
			}
		}
		names = append(names, name)
	}
	return names
}

// RemotePackCount 返回这块盘还有多少个数据包没有下载到本地。
func (s *StoreActor) RemotePackCount() int {
	if s.blobs == nil {
		return 0
	}
	return len(s.blobs.remotePackIDs())
}

// HydrateRemotePacks 把所有尚未下载的数据包补齐到本地，全部就位后返回。
func (s *StoreActor) HydrateRemotePacks(ctx context.Context) error {
	if s.blobs == nil {
		return nil
	}
	return s.blobs.hydrateAll(ctx)
}

// HydrateDir 把一个没有被任何实例打开的目录（例如不可变的提交切面）里的占位包全部补齐。
func HydrateDir(ctx context.Context, dir string, source PackSource) error {
	packsDir := filepath.Join(dir, "packs")
	entries, err := os.ReadDir(packsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		packName, ok := PackNameOfStub(entry.Name())
		if !ok {
			continue
		}
		stubPath := filepath.Join(packsDir, entry.Name())
		packPath := filepath.Join(packsDir, packName)
		if _, statErr := os.Stat(packPath); statErr == nil {
			_ = os.Remove(stubPath)
			continue
		}
		if source == nil {
			return fmt.Errorf("%w: %s is not downloaded and no remote source is configured", ErrIO, packName)
		}
		stub, err := ReadRemotePackStub(stubPath)
		if err != nil {
			return err
		}
		tmp := filepath.Join(packsDir, ".fetch-"+packName)
		_ = os.Remove(tmp)
		if err := source.FetchPack(ctx, stub.SHA256, stub.Size, tmp); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, packPath); err != nil {
			return err
		}
		_ = os.Remove(stubPath)
	}
	return syncDirPath(packsDir)
}
