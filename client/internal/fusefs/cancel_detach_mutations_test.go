package fusefs_test

import (
	"context"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func requireDetachedRPCContext(t *testing.T, label string, ctx context.Context, cancelCh chan struct{}) error {
	t.Helper()
	deadline, hasDeadline := ctx.Deadline()
	require.True(t, hasDeadline, "scene=%s want_deadline=true", label)
	remaining := time.Until(deadline)
	require.Greater(t, remaining, 20*time.Second, "scene=%s want_timeout>20s got=%v", label, remaining)
	require.LessOrEqual(t, remaining, fusefs.DefaultRPCTimeout, "scene=%s want_timeout<=30s got=%v", label, remaining)
	return awaitBeyondInterruptGrace(ctx, cancelCh)
}

func TestStep70_NonIdempotentMutatingRPCs_DetachFromFuseInterruptCancel(t *testing.T) {
	t.Run("1_OpenOTrunc_SetAttr", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "open-trunc.txt", 0o644, []byte("old-bytes"))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		mustGetAttr(t, fs, ino)

		cancelCh := make(chan struct{})
		srv.setAttrHook = func(ctx context.Context, _ *storagev1.SetAttrRequest) error {
			return requireDetachedRPCContext(t, "open_otrunc", ctx, cancelCh)
		}

		var out fuse.OpenOut
		st := fs.Open(cancelCh, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Flags:    syscall.O_RDWR | syscall.O_TRUNC,
		}, &out)

		requireStatus(t, "Open(O_TRUNC) detached from cancel", fuse.OK, st)
		got, ok := srv.fileBytes(ino)
		require.True(t, ok)
		require.Empty(t, got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out.Fh})
	})

	t.Run("2_SetAttr", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "setattr-detach.txt", 0o644, []byte("abcdef"))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		cancelCh := make(chan struct{})
		srv.setAttrHook = func(ctx context.Context, _ *storagev1.SetAttrRequest) error {
			return requireDetachedRPCContext(t, "setattr", ctx, cancelCh)
		}

		var out fuse.AttrOut
		st := fs.SetAttr(cancelCh, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_SIZE,
				Size:     2,
			},
		}, &out)

		requireStatus(t, "SetAttr detached from cancel", fuse.OK, st)
		require.Equal(t, uint64(2), out.Size)
	})

	t.Run("3_Fallocate", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "falloc-detach.bin", syscall.O_RDWR, 0o644)

		cancelCh := make(chan struct{})
		srv.fallocateHook = func(ctx context.Context, _ *storagev1.FallocateRequest) error {
			return requireDetachedRPCContext(t, "fallocate", ctx, cancelCh)
		}

		st := fs.Fallocate(cancelCh, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Fh:       f.Fh,
			Offset:   0,
			Length:   32,
			Mode:     0,
		})

		requireStatus(t, "Fallocate detached from cancel", fuse.OK, st)
		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		require.Len(t, got, 32)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("4_SetXAttr", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "setxattr-detach.txt", 0o644, nil)
		fs := newTestRawFS(t, srv, withDirectIO(false))

		cancelCh := make(chan struct{})
		srv.setXattrHook = func(ctx context.Context, _ *storagev1.SetXattrRequest) error {
			return requireDetachedRPCContext(t, "setxattr", ctx, cancelCh)
		}

		st := fs.SetXAttr(cancelCh, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino},
		}, "user.detach", []byte("val"))

		requireStatus(t, "SetXAttr detached from cancel", fuse.OK, st)
		buf := make([]byte, 16)
		sz, getSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.detach", buf)
		requireStatus(t, "GetXAttr after detached SetXAttr", fuse.OK, getSt)
		require.Equal(t, "val", string(buf[:sz]))
	})

	t.Run("5_RemoveXAttr", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "rmxattr-detach.txt", 0o644, nil)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		requireStatus(t, "seed xattr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: ino},
		}, "user.rm", []byte("bye")))

		cancelCh := make(chan struct{})
		srv.removeXattrHook = func(ctx context.Context, _ *storagev1.RemoveXattrRequest) error {
			return requireDetachedRPCContext(t, "removexattr", ctx, cancelCh)
		}

		st := fs.RemoveXAttr(cancelCh, &fuse.InHeader{NodeId: ino}, "user.rm")

		requireStatus(t, "RemoveXAttr detached from cancel", fuse.OK, st)
		_, getSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.rm", make([]byte, 8))
		requireStatus(t, "GetXAttr after detached RemoveXAttr", fuse.Status(syscall.ENODATA), getSt)
	})

	t.Run("6_IoctlSetFlagsAndFSSetXAttr", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "ioctl-detach.txt", 0o644, nil)
		fs := newTestRawFS(t, srv, withDirectIO(false))

		cancel1 := make(chan struct{})
		srv.setAttrHook = func(ctx context.Context, _ *storagev1.SetAttrRequest) error {
			return requireDetachedRPCContext(t, "ioctl_setflags", ctx, cancel1)
		}
		in64 := make([]byte, 8)
		binary.LittleEndian.PutUint64(in64, uint64(fusefs.FS_APPEND_FL))
		var out fuse.IoctlOut
		st1 := fs.Ioctl(cancel1, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
		}, in64, &out, nil)
		requireStatus(t, "Ioctl SETFLAGS detached from cancel", fuse.OK, st1)

		cancel2 := make(chan struct{})
		srv.setAttrHook = func(ctx context.Context, _ *storagev1.SetAttrRequest) error {
			return requireDetachedRPCContext(t, "ioctl_fssetxattr", ctx, cancel2)
		}
		fsx := make([]byte, fsxattrStructLen)
		binary.LittleEndian.PutUint32(fsx[:4], 0)
		st2 := fs.Ioctl(cancel2, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Cmd:      fusefs.FS_IOC_FSSETXATTR,
		}, fsx, &out, nil)
		requireStatus(t, "Ioctl FSSETXATTR detached from cancel", fuse.OK, st2)
	})

	t.Run("7_Unlink", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "unlink-detach.txt", 0o644, []byte("x"))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		mustLookup(t, fs, fuse.FUSE_ROOT_ID, "unlink-detach.txt")

		cancelCh := make(chan struct{})
		srv.unlinkHook = func(ctx context.Context, _ *storagev1.UnlinkRequest) error {
			return requireDetachedRPCContext(t, "unlink", ctx, cancelCh)
		}

		st := fs.Unlink(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "unlink-detach.txt")

		requireStatus(t, "Unlink detached from cancel", fuse.OK, st)
		require.False(t, srv.hasNode(ino))
	})

	t.Run("8_Rename", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "rename-src.txt", 0o644, []byte("r"))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		mustLookup(t, fs, fuse.FUSE_ROOT_ID, "rename-src.txt")

		cancelCh := make(chan struct{})
		srv.renameHook = func(ctx context.Context, _ *storagev1.RenameRequest) error {
			return requireDetachedRPCContext(t, "rename", ctx, cancelCh)
		}

		st := fs.Rename(cancelCh, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "rename-src.txt", "rename-dst.txt")

		requireStatus(t, "Rename detached from cancel", fuse.OK, st)
		entry := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "rename-dst.txt")
		require.Equal(t, ino, entry.NodeId)
	})

	t.Run("9_Create", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var preOut fuse.CreateOut
		stPre := fs.Create(newClosedCancelChan(), &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "create-preclosed.txt", &preOut)
		requireStatus(t, "Create with pre-closed cancel", fuse.OK, stPre)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: preOut.NodeId}, Fh: preOut.Fh})

		cancelCh := make(chan struct{})
		srv.createHook = func(ctx context.Context, _ *storagev1.CreateRequest) error {
			return requireDetachedRPCContext(t, "create", ctx, cancelCh)
		}
		var midOut fuse.CreateOut
		stMid := fs.Create(cancelCh, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "create-midcancel.txt", &midOut)
		requireStatus(t, "Create with mid-RPC cancel", fuse.OK, stMid)
		require.True(t, srv.hasNode(midOut.NodeId))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: midOut.NodeId}, Fh: midOut.Fh})
	})

	t.Run("10_Mkdir", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var preOut fuse.EntryOut
		stPre := fs.Mkdir(newClosedCancelChan(), &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0o755,
		}, "mkdir-preclosed", &preOut)
		requireStatus(t, "Mkdir with pre-closed cancel", fuse.OK, stPre)

		cancelCh := make(chan struct{})
		srv.mkdirHook = func(ctx context.Context, _ *storagev1.MkdirRequest) error {
			return requireDetachedRPCContext(t, "mkdir", ctx, cancelCh)
		}
		var midOut fuse.EntryOut
		stMid := fs.Mkdir(cancelCh, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0o755,
		}, "mkdir-midcancel", &midOut)
		requireStatus(t, "Mkdir with mid-RPC cancel", fuse.OK, stMid)
		require.True(t, srv.hasNode(midOut.NodeId))
	})

	t.Run("11_Mknod", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var preOut fuse.EntryOut
		stPre := fs.Mknod(newClosedCancelChan(), &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     syscall.S_IFREG | 0o644,
		}, "mknod-preclosed", &preOut)
		requireStatus(t, "Mknod with pre-closed cancel", fuse.OK, stPre)

		cancelCh := make(chan struct{})
		srv.mknodHook = func(ctx context.Context, _ *storagev1.MknodRequest) error {
			return requireDetachedRPCContext(t, "mknod", ctx, cancelCh)
		}
		var midOut fuse.EntryOut
		stMid := fs.Mknod(cancelCh, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     syscall.S_IFREG | 0o644,
		}, "mknod-midcancel", &midOut)
		requireStatus(t, "Mknod with mid-RPC cancel", fuse.OK, stMid)
		require.True(t, srv.hasNode(midOut.NodeId))
	})

	t.Run("12_Symlink", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var preOut fuse.EntryOut
		stPre := fs.Symlink(newClosedCancelChan(), &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target-pre", "sym-preclosed", &preOut)
		requireStatus(t, "Symlink with pre-closed cancel", fuse.OK, stPre)

		cancelCh := make(chan struct{})
		srv.symlinkHook = func(ctx context.Context, _ *storagev1.SymlinkRequest) error {
			return requireDetachedRPCContext(t, "symlink", ctx, cancelCh)
		}
		var midOut fuse.EntryOut
		stMid := fs.Symlink(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target-mid", "sym-midcancel", &midOut)
		requireStatus(t, "Symlink with mid-RPC cancel", fuse.OK, stMid)
		require.True(t, srv.hasNode(midOut.NodeId))
	})

	t.Run("13_Rmdir", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		dirIno := srv.seedDir(1, "rmdir-detach", 0o755)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		mustLookup(t, fs, fuse.FUSE_ROOT_ID, "rmdir-detach")

		cancelCh := make(chan struct{})
		srv.rmdirHook = func(ctx context.Context, _ *storagev1.RmdirRequest) error {
			return requireDetachedRPCContext(t, "rmdir", ctx, cancelCh)
		}

		st := fs.Rmdir(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "rmdir-detach")

		requireStatus(t, "Rmdir detached from cancel", fuse.OK, st)
		require.False(t, srv.hasNode(dirIno))
	})
}

func TestStep70_PreRPCLookupsAndIdempotentOps_RemainInterruptibleByCancel(t *testing.T) {
	t.Run("Unlink_PreRPCLookup_InterruptibleWithEINTR", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		keepIno := srv.seedFile(1, "keep-open.txt", 0o644, nil)
		srv.seedFile(1, "target-unlink.txt", 0o644, nil)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, keepIno, syscall.O_RDONLY)

		cancelCh := make(chan struct{})
		srv.lookupHook = func(ctx context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return nil, awaitBeyondInterruptGrace(ctx, cancelCh)
		}

		st := fs.Unlink(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "target-unlink.txt")

		requireStatus(t, "Unlink pre-RPC Lookup canceled", fuse.EINTR, st)
		require.Zero(t, srv.unlinkCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: keepIno}, Fh: opened.Fh})
	})

	t.Run("Rename_PreRPCLookup_InterruptibleWithEINTR", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		keepIno := srv.seedFile(1, "keep-open-ren.txt", 0o644, nil)
		srv.seedFile(1, "ren-src.txt", 0o644, nil)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, keepIno, syscall.O_RDONLY)

		cancelCh := make(chan struct{})
		srv.lookupHook = func(ctx context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return nil, awaitBeyondInterruptGrace(ctx, cancelCh)
		}

		st := fs.Rename(cancelCh, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "ren-src.txt", "ren-dst.txt")

		requireStatus(t, "Rename pre-RPC Lookup canceled", fuse.EINTR, st)
		require.Zero(t, srv.renameCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: keepIno}, Fh: opened.Fh})
	})

	t.Run("Rmdir_PreRPCLookup_InterruptibleWithEINTR", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		keepIno := srv.seedFile(1, "keep-open-rmd.txt", 0o644, nil)
		srv.seedDir(1, "rmd-target", 0o755)
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, keepIno, syscall.O_RDONLY)

		cancelCh := make(chan struct{})
		srv.lookupHook = func(ctx context.Context, _ *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return nil, awaitBeyondInterruptGrace(ctx, cancelCh)
		}

		st := fs.Rmdir(cancelCh, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "rmd-target")

		requireStatus(t, "Rmdir pre-RPC Lookup canceled", fuse.EINTR, st)
		require.Zero(t, srv.rmdirCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: keepIno}, Fh: opened.Fh})
	})

	t.Run("Fsync_IdempotentRPC_RemainsInterruptibleWithEINTR", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "fsync-cancel.txt", syscall.O_RDWR, 0o644)

		cancelCh := make(chan struct{})
		srv.fsyncHook = func(ctx context.Context, _ *storagev1.FsyncRequest) error {
			return awaitBeyondInterruptGrace(ctx, cancelCh)
		}

		st := fs.Fsync(cancelCh, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Fh:       f.Fh,
		})

		requireStatus(t, "Fsync mid-RPC cancel", fuse.EINTR, st)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})
}
