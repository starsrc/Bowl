package fusefs

import (
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

const VirtualStatsIno uint64 = (uint64(1) << 63) - 1

const (
	virtualStatsName     = ".stats"
	virtualStatsAttrSize = 64 * 1024
)

func (fs *RawFS) isVirtualStatsName(parent uint64, name string) bool {
	return fs.cfg.EnableStatsFile &&
		parent == fuse.FUSE_ROOT_ID &&
		name == virtualStatsName
}

func (fs *RawFS) isVirtualStatsIno(ino uint64) bool {
	return fs.cfg.EnableStatsFile && ino == VirtualStatsIno
}

func (fs *RawFS) fillVirtualStatsAttr(out *fuse.Attr) {
	if out == nil {
		return
	}
	now := time.Now()
	sec := uint64(max(int64(0), now.Unix()))
	nsec := uint32(max(0, now.Nanosecond()))
	*out = fuse.Attr{
		Ino:       VirtualStatsIno,
		Size:      virtualStatsAttrSize,
		Mode:      syscall.S_IFREG | 0o444,
		Nlink:     1,
		Blksize:   4096,
		Atime:     sec,
		Atimensec: nsec,
		Mtime:     sec,
		Mtimensec: nsec,
		Ctime:     sec,
		Ctimensec: nsec,
	}
}

func (fs *RawFS) fillVirtualStatsEntryOut(out *fuse.EntryOut) {
	if out == nil {
		return
	}
	*out = fuse.EntryOut{
		NodeId:     VirtualStatsIno,
		Generation: 1,
	}
	out.SetEntryTimeout(0)
	out.SetAttrTimeout(0)
	fs.fillVirtualStatsAttr(&out.Attr)
}

func (fs *RawFS) openVirtualStats(input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	accMode := input.Flags & uint32(syscall.O_ACCMODE)
	if accMode == syscall.O_ACCMODE {
		return fuse.EINVAL
	}
	if accMode == syscall.O_WRONLY || accMode == syscall.O_RDWR ||
		input.Flags&uint32(syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return fuse.EACCES
	}
	fh, status := fs.openHandle(VirtualStatsIno, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	out.Fh = fh
	out.OpenFlags = fuse.FOPEN_DIRECT_IO
	return fuse.OK
}

func (fs *RawFS) readVirtualStats(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	handle, status := fs.checkedHandle(VirtualStatsIno, input.Fh, false)
	if status != fuse.OK {
		return nil, status
	}
	if !handle.CanRead() {
		return nil, fuse.EBADF
	}

	fs.stateMu.Lock()
	snap := fs.statsSnapshots[input.Fh]
	fs.stateMu.Unlock()
	if snap == nil {
		snap = fs.metrics.FormatStatsFile()
		fs.stateMu.Lock()
		if existing := fs.statsSnapshots[input.Fh]; existing != nil {
			snap = existing
		} else if _, stillOpen := fs.handles.Get(input.Fh); stillOpen {
			fs.statsSnapshots[input.Fh] = snap
		}
		fs.stateMu.Unlock()
	}

	want := int(input.Size)
	if len(buf) > 0 && len(buf) < want {
		want = len(buf)
	}
	if want <= 0 || input.Offset >= uint64(len(snap)) {
		return fuse.ReadResultData(nil), fuse.OK
	}
	rem := snap[input.Offset:]
	n := min(want, len(rem))
	if len(buf) >= n {
		copy(buf[:n], rem[:n])
		return fuse.ReadResultData(buf[:n]), fuse.OK
	}
	out := make([]byte, n)
	copy(out, rem[:n])
	return fuse.ReadResultData(out), fuse.OK
}
