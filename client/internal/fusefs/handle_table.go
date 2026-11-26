package fusefs

import (
	"context"
	"sync"
	"syscall"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type FileHandle struct {
	Ino   uint64
	Flags uint32
	IsDir bool
}

func (h FileHandle) CanRead() bool {
	return !h.IsDir && h.Flags&uint32(syscall.O_ACCMODE) != syscall.O_WRONLY
}

func (h FileHandle) CanWrite() bool {
	return !h.IsDir && h.Flags&uint32(syscall.O_ACCMODE) != syscall.O_RDONLY
}

type HandleTable struct {
	mu          sync.RWMutex
	next        uint64
	handles     map[uint64]FileHandle
	counts      map[uint64]int
	openFiles   int
	openDirs    int
	orphans     map[uint64]struct{}
	removedDirs map[uint64]*storagev1.Inode
	onOrphan    func(context.Context, uint64)
}

func NewHandleTable(onOrphan func(uint64)) *HandleTable {
	var fn func(context.Context, uint64)
	if onOrphan != nil {
		fn = func(_ context.Context, ino uint64) {
			onOrphan(ino)
		}
	}
	return NewHandleTableWithContext(fn)
}

func NewHandleTableWithContext(onOrphan func(context.Context, uint64)) *HandleTable {
	return &HandleTable{
		handles:     make(map[uint64]FileHandle),
		counts:      make(map[uint64]int),
		orphans:     make(map[uint64]struct{}),
		removedDirs: make(map[uint64]*storagev1.Inode),
		onOrphan:    onOrphan,
	}
}

func (t *HandleTable) stats() (openFiles, openDirs int) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.openFiles, t.openDirs
}

func (t *HandleTable) Open(ino uint64, flags uint32, isDir bool) (uint64, error) {
	mode := flags & uint32(syscall.O_ACCMODE)
	if ino == 0 || mode == syscall.O_ACCMODE {
		return 0, syscall.EINVAL
	}
	if isDir && (mode != syscall.O_RDONLY || flags&uint32(syscall.O_TRUNC) != 0) {
		return 0, syscall.EISDIR
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		t.next++
		if t.next == 0 {
			continue
		}
		if _, exists := t.handles[t.next]; !exists {
			break
		}
	}
	t.handles[t.next] = FileHandle{Ino: ino, Flags: flags, IsDir: isDir}
	t.counts[ino]++
	if isDir {
		t.openDirs++
	} else {
		t.openFiles++
	}
	return t.next, nil
}

func (t *HandleTable) Get(fh uint64) (FileHandle, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	handle, ok := t.handles[fh]
	return handle, ok
}

func (t *HandleTable) Release(fh uint64) (FileHandle, bool) {
	return t.ReleaseContext(context.Background(), fh)
}

func (t *HandleTable) ReleaseContext(ctx context.Context, fh uint64) (FileHandle, bool) {
	t.mu.Lock()
	handle, ok := t.handles[fh]
	if !ok {
		t.mu.Unlock()
		return FileHandle{}, false
	}
	delete(t.handles, fh)
	if handle.IsDir {
		t.openDirs--
	} else {
		t.openFiles--
	}
	t.counts[handle.Ino]--
	reclaim := false
	if t.counts[handle.Ino] == 0 {
		delete(t.counts, handle.Ino)
		delete(t.removedDirs, handle.Ino)
		_, reclaim = t.orphans[handle.Ino]
		delete(t.orphans, handle.Ino)
	}
	t.mu.Unlock()
	if reclaim && t.onOrphan != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		t.onOrphan(ctx, handle.Ino)
	}
	return handle, true
}

func (t *HandleTable) OpenCount(ino uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.counts[ino]
}

func (t *HandleTable) IsOpen(ino uint64) bool {
	return t.OpenCount(ino) != 0
}

func (t *HandleTable) MarkOrphan(ino uint64) {
	t.MarkOrphanContext(context.Background(), ino)
}

func (t *HandleTable) MarkOrphanContext(ctx context.Context, ino uint64) {
	t.mu.Lock()
	if t.counts[ino] != 0 {
		t.orphans[ino] = struct{}{}
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	if t.onOrphan != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		t.onOrphan(ctx, ino)
	}
}

func (t *HandleTable) AnyOpen() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.handles) != 0
}

func (t *HandleTable) AnyOpenFiles() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.openFiles != 0
}

func (t *HandleTable) AnyOpenDirs() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.openDirs != 0
}

func (t *HandleTable) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.handles)
	clear(t.counts)
	t.openFiles = 0
	t.openDirs = 0
	clear(t.orphans)
	clear(t.removedDirs)
}

func (t *HandleTable) ClearOrphan(ino uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.orphans, ino)
}

func (t *HandleTable) IsOrphan(ino uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.orphans[ino]
	return ok
}

func (t *HandleTable) MarkRemovedDir(ino uint64, inode *storagev1.Inode) {
	if inode == nil || inode.Attr == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts[ino] == 0 {
		return
	}
	removed := proto.CloneOf(inode)
	removed.Attr.Nlink = 0
	removed.Attr.Ctime = timestamppb.Now()
	t.removedDirs[ino] = removed
}

func (t *HandleTable) RemovedDir(ino uint64) (*storagev1.Inode, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	inode, ok := t.removedDirs[ino]
	return inode, ok
}
