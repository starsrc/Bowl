package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/starsrc/pkg/serverx"
)

const (
	storageErrorDomain = "bowl.storage"
	staleEpochReason   = "EBUSY"
)

func isStaleEpochError(err error) bool {
	if err == nil || connect.CodeOf(err) != connect.CodeFailedPrecondition {
		return false
	}
	info := serverx.ExtractErrorInfo(err)
	return info != nil && info.GetDomain() == storageErrorDomain && info.GetReason() == staleEpochReason
}

func (fs *RawFS) Fence(status fuse.Status, reason string) {
	if fs == nil {
		return
	}
	if status == fuse.OK {
		status = fuse.EBUSY
	}
	if fs.fencedStatus.Load() != 0 {
		return
	}

	fs.fenceMu.Lock()
	if fs.fencedStatus.Load() != 0 {
		fs.fenceMu.Unlock()
		return
	}
	fs.fenceReason = reason
	fs.fencedStatus.Store(uint32(status))
	fenceCh := fs.fenceCh
	cb := fs.onFence
	fs.fenceMu.Unlock()

	fs.stage.AbortAll(syscall.Errno(status))
	fs.cache.clear()
	fs.readAhead.InvalidateAll()
	fs.caps.reset()
	fs.specialMu.Lock()
	clear(fs.specialInodes)
	fs.specialMu.Unlock()

	fs.releaseMu.Lock()
	fs.releaseClosed = true
	fs.releaseQueue = nil
	fs.releaseMu.Unlock()
	if fs.releaseCancel != nil {
		fs.releaseCancel()
	}

	if cb != nil {
		cb(status, reason)
	}
	if fenceCh != nil {
		close(fenceCh)
	}
}

func (fs *RawFS) IsFenced() bool {
	return fs != nil && fs.fencedStatus.Load() != 0
}

func (fs *RawFS) FenceStatus() fuse.Status {
	if fs == nil {
		return fuse.OK
	}
	return fuse.Status(fs.fencedStatus.Load())
}

func (fs *RawFS) FenceReason() string {
	if fs == nil {
		return ""
	}
	fs.fenceMu.Lock()
	reason := fs.fenceReason
	fs.fenceMu.Unlock()
	return reason
}

func (fs *RawFS) FencedCh() <-chan struct{} {
	if fs == nil {
		return nil
	}
	return fs.fenceCh
}

func (fs *RawFS) SetOnFence(fn func(status fuse.Status, reason string)) {
	if fs == nil {
		return
	}
	fs.fenceMu.Lock()
	fs.onFence = fn
	st := fuse.Status(fs.fencedStatus.Load())
	reason := fs.fenceReason
	fs.fenceMu.Unlock()
	if st != fuse.OK && fn != nil {
		fn(st, reason)
	}
}

func (fs *RawFS) fenceIfStaleEpoch(err error) {
	if fs == nil || !isStaleEpochError(err) {
		return
	}
	fs.Fence(fuse.EBUSY, err.Error())
}

func (fs *RawFS) fenceErrToErrno() syscall.Errno {
	st := fs.FenceStatus()
	if st == fuse.OK {
		return syscall.EBUSY
	}
	return syscall.Errno(st)
}
