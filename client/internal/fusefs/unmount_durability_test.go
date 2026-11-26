package fusefs_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := string(buf[:n])
	rest, ok := strings.CutPrefix(line, "goroutine ")
	if !ok {
		return 0
	}
	idStr, _, ok := strings.Cut(rest, " ")
	if !ok {
		return 0
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return 0
	}
	return id
}

func createDirtySyncFiles(t *testing.T, fs *fusefs.RawFS, count int) []uint64 {
	t.Helper()
	inos := make([]uint64, 0, count)
	for i := range count {
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, fmt.Sprintf("dirty-%03d.bin", i), syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		inos = append(inos, f.NodeId)
	}
	return inos
}

func TestRawFS_OnUnmount_MultiFileWrites_IssuesSingleFsyncDirRootInoInline(t *testing.T) {
	t.Run("SyncMode200FilesIssuesSingleFsyncDirOnRootIno", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(42), withTTL(1.0, 1.0))
		entered := make(chan uint64, 2)
		gate := make(chan struct{})
		srv.fsyncDirHook = func(ctx context.Context, req *storagev1.FsyncDirRequest) error {
			entered <- req.GetIno()
			select {
			case <-gate:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		t.Cleanup(func() {
			select {
			case <-gate:
			default:
				close(gate)
			}
		})
		createDirtySyncFiles(t, fs, 200)

		unmountDone := make(chan struct{})
		go func() {
			fs.OnUnmount()
			close(unmountDone)
		}()

		var gotIno uint64
		select {
		case gotIno = <-entered:
		case <-unmountDone:
			t.Fatal("OnUnmount returned without calling FsyncDir")
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for single FsyncDir RPC")
		}

		require.Equal(t, uint64(42), gotIno)
		require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 0, Total: 1}, fs.UnmountProgress())

		close(gate)
		<-unmountDone

		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		snap := fs.Snapshot()
		require.Equal(t, uint64(1), snap.Queues.DirtyFlushOK)
		require.Zero(t, snap.Queues.DirtyFlushErrors)

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("SingleFileWriteRunsFsyncDirInlineOnCallerGoroutine", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(19), withTTL(1.0, 1.0))
		var hookGoroutineID atomic.Uint64
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			hookGoroutineID.Store(currentGoroutineID())
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		createDirtySyncFiles(t, fs, 1)

		callerID := currentGoroutineID()
		fs.OnUnmount()

		require.NotZero(t, callerID)
		require.Equal(t, callerID, hookGoroutineID.Load())
		require.Equal(t, uint64(19), gotIno.Load())
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		snap := fs.Snapshot()
		require.Equal(t, uint64(1), snap.Queues.DirtyFlushOK)
		require.Zero(t, snap.Queues.DirtyFlushErrors)
	})

	t.Run("StagedModeMultiFileWritesIssueSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(33), withTTL(1.0, 1.0))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
		for i := range 5 {
			f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, fmt.Sprintf("staged-%d.bin", i), syscall.O_RDWR, 0o644)
			mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("staged-payload"))
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		}

		fs.OnUnmount()

		require.Equal(t, uint64(33), gotIno.Load())
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		snap := fs.Snapshot()
		require.Equal(t, uint64(1), snap.Queues.DirtyFlushOK)
		require.Zero(t, snap.Queues.DirtyFlushErrors)
	})

	t.Run("StagedTmpfileWithDataLinkPromotionIssuesSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(34), withTTL(1.0, 1.0))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("promoted-tmp-data"))
		var linkOut fuse.EntryOut
		requireStatus(t, "Link", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "promoted-data.bin", &linkOut))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})

		fs.OnUnmount()

		require.Equal(t, uint64(34), gotIno.Load())
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, uint64(1), fs.Snapshot().Queues.DirtyFlushOK)
	})
}

func collectUnmountFsyncLogInodes(t *testing.T, raw string) []uint64 {
	t.Helper()
	records := parseJSONLogMessages(t, raw)
	var inos []uint64
	for _, rec := range records {
		if rec["msg"] != "unmount_fsync_failed" {
			continue
		}
		inoVal, ok := rec["ino"].(float64)
		require.True(t, ok, "unmount_fsync_failed log record missing numeric ino: %v", rec)
		inos = append(inos, uint64(inoVal))
	}
	return inos
}

func TestRawFS_OnUnmount_OpenTruncateSetAttrSizeAndFallocate_IssuesSingleFsyncDirRootIno(t *testing.T) {
	t.Run("OpenWithOTruncIssuesSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(61), withTTL(1.0, 1.0))
		ino := srv.seedFile(61, "trunc-open.bin", 0o644, []byte("existing-payload"))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))

		opened := mustOpenFile(t, fs, ino, uint32(syscall.O_RDWR|syscall.O_TRUNC))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, uint64(61), gotIno.Load())
	})

	t.Run("SetAttrFATTRSIZEIssuesSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(62), withTTL(1.0, 1.0))
		ino := srv.seedFile(62, "trunc-setattr.bin", 0o644, []byte("0123456789"))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var out fuse.AttrOut
		st := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_SIZE,
				Size:     4,
			},
		}, &out)
		requireStatus(t, "SetAttr truncate", fuse.OK, st)

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, uint64(62), gotIno.Load())
	})

	t.Run("FallocateIssuesSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(63), withTTL(1.0, 1.0))
		ino := srv.seedFile(63, "alloc.bin", 0o644, []byte("0123456789"))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		opened := mustOpenFile(t, fs, ino, syscall.O_RDWR)

		st := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       opened.Fh,
			Offset:   2,
			Length:   4,
			Mode:     0x02 | 0x01,
		})
		requireStatus(t, "Fallocate punch hole", fuse.OK, st)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, uint64(63), gotIno.Load())
	})

	t.Run("DirectoryAndNonSizeSetAttrIssueSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(64), withTTL(1.0, 1.0))
		regIno := srv.seedFile(64, "chmod-only.bin", 0o644, []byte("data"))
		var gotIno atomic.Uint64
		srv.fsyncDirHook = func(_ context.Context, req *storagev1.FsyncDirRequest) error {
			gotIno.Store(req.GetIno())
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "sub", 0o755)

		var out fuse.AttrOut
		requireStatus(t, "dir chmod", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: dir.NodeId},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o700,
			},
		}, &out))
		requireStatus(t, "file chmod", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: regIno},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_MTIME_NOW,
				Mode:     0o600,
			},
		}, &out))

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, uint64(64), gotIno.Load())
	})
}

func TestRawFS_OnUnmount_MetadataOnlyMutations_IssuesSingleFsyncDirRootIno(t *testing.T) {
	cases := []struct {
		name    string
		rootIno uint64
		mode    string
		mutate  func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64)
	}{
		{
			name:    "Mkdir",
			rootIno: 42,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "dir-a", 0o755)
			},
		},
		{
			name:    "CreateEmptyFile",
			rootIno: 1,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "empty.txt", syscall.O_RDWR, 0o644)
				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
			},
		},
		{
			name:    "Mknod",
			rootIno: 42,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				var out fuse.EntryOut
				requireStatus(t, "Mknod", fuse.OK, fs.Mknod(nil, &fuse.MknodIn{
					InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Mode:     syscall.S_IFIFO | 0o644,
				}, "pipe.fifo", &out))
			},
		},
		{
			name:    "Symlink",
			rootIno: 42,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				var out fuse.EntryOut
				requireStatus(t, "Symlink", fuse.OK, fs.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "/target", "sym", &out))
			},
		},
		{
			name:    "Link",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "src.txt", 0o644, []byte("data"))
				var out fuse.EntryOut
				requireStatus(t, "Link", fuse.OK, fs.Link(nil, &fuse.LinkIn{
					InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Oldnodeid: ino,
				}, "dst.txt", &out))
			},
		},
		{
			name:    "Unlink",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				srv.seedFile(rootIno, "gone.txt", 0o644, nil)
				requireStatus(t, "Unlink", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "gone.txt"))
			},
		},
		{
			name:    "Rmdir",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				srv.seedDir(rootIno, "gone-dir", 0o755)
				requireStatus(t, "Rmdir", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "gone-dir"))
			},
		},
		{
			name:    "Rename",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				srv.seedFile(rootIno, "old.txt", 0o644, nil)
				requireStatus(t, "Rename", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
					InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Newdir:   fuse.FUSE_ROOT_ID,
				}, "old.txt", "new.txt"))
			},
		},
		{
			name:    "SetXAttr",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "x.txt", 0o644, nil)
				requireStatus(t, "SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
					InHeader: fuse.InHeader{NodeId: ino},
				}, "user.k", []byte("v")))
			},
		},
		{
			name:    "RemoveXAttr",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "rx.txt", 0o644, nil)
				srv.mu.Lock()
				srv.nodes[ino].xattrs["user.k"] = []byte("v")
				srv.mu.Unlock()
				requireStatus(t, "RemoveXAttr", fuse.OK, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.k"))
			},
		},
		{
			name:    "IoctlSetFlags",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "flags.txt", 0o644, nil)
				in64 := make([]byte, 8)
				binary.LittleEndian.PutUint64(in64, uint64(fusefs.FS_APPEND_FL))
				var ioOut fuse.IoctlOut
				requireStatus(t, "Ioctl SETFLAGS", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
					InHeader: fuse.InHeader{NodeId: ino},
					Cmd:      fusefs.FS_IOC_SETFLAGS,
				}, in64, &ioOut, nil))
			},
		},
		{
			name:    "IoctlSetFlags32",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "flags32.txt", 0o644, nil)
				in32 := make([]byte, 4)
				binary.LittleEndian.PutUint32(in32, fusefs.FS_IMMUTABLE_FL)
				var ioOut fuse.IoctlOut
				requireStatus(t, "Ioctl SETFLAGS32", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
					InHeader: fuse.InHeader{NodeId: ino},
					Cmd:      fusefs.FS_IOC32_SETFLAGS,
				}, in32, &ioOut, nil))
			},
		},
		{
			name:    "IoctlFsSetXattr",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "xflags.txt", 0o644, nil)
				fsxIn := make([]byte, 28)
				binary.LittleEndian.PutUint32(fsxIn[:4], fusefs.FS_XFLAG_IMMUTABLE)
				var ioOut fuse.IoctlOut
				requireStatus(t, "Ioctl FSSETXATTR", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
					InHeader: fuse.InHeader{NodeId: ino},
					Cmd:      fusefs.FS_IOC_FSSETXATTR,
				}, fsxIn, &ioOut, nil))
			},
		},
		{
			name:    "SetAttrChmodAndUtimes",
			rootIno: 42,
			mutate: func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, rootIno uint64) {
				ino := srv.seedFile(rootIno, "chmod.txt", 0o644, nil)
				var out fuse.AttrOut
				requireStatus(t, "SetAttr", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
					SetAttrInCommon: fuse.SetAttrInCommon{
						InHeader: fuse.InHeader{NodeId: ino},
						Valid:    fuse.FATTR_MODE | fuse.FATTR_MTIME_NOW,
						Mode:     0o600,
					},
				}, &out))
			},
		},
		{
			name:    "SyncTmpfileReclaimOrphan",
			rootIno: 42,
			mode:    fusefs.MountModeSync,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				var tmpOut fuse.CreateOut
				requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
					InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Flags:    uint32(syscall.O_RDWR),
					Mode:     0o600,
				}, &tmpOut))
				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
			},
		},
		{
			name:    "StagedTmpfileLinkPromotion",
			rootIno: 42,
			mutate: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, _ uint64) {
				var tmpOut fuse.CreateOut
				requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
					InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Flags:    uint32(syscall.O_RDWR),
					Mode:     0o600,
				}, &tmpOut))
				var linkOut fuse.EntryOut
				requireStatus(t, "Link", fuse.OK, fs.Link(nil, &fuse.LinkIn{
					InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Oldnodeid: tmpOut.NodeId,
				}, "promoted-tmp.bin", &linkOut))
				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			srv := newFakeStorage(t, withRootIno(tc.rootIno), withTTL(1.0, 1.0))
			entered := make(chan uint64, 2)
			gate := make(chan struct{})
			srv.fsyncDirHook = func(ctx context.Context, req *storagev1.FsyncDirRequest) error {
				entered <- req.GetIno()
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			var opts []rawFSOption
			if tc.mode != "" {
				opts = append(opts, withMode(tc.mode))
			}
			fs := newTestRawFS(t, srv, opts...)
			t.Cleanup(func() {
				select {
				case <-gate:
				default:
					close(gate)
				}
			})

			tc.mutate(t, srv, fs, tc.rootIno)
			require.Zero(t, srv.fsyncCalls.Load())
			require.Zero(t, srv.fsyncDirCalls.Load())

			done := make(chan struct{})
			go func() {
				fs.OnUnmount()
				close(done)
			}()

			var gotIno uint64
			select {
			case gotIno = <-entered:
			case <-done:
				t.Fatal("OnUnmount returned without calling FsyncDir")
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for FsyncDir on metadata-only unmount")
			}

			require.Equal(t, tc.rootIno, gotIno)
			require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 0, Total: 1}, fs.UnmountProgress())
			close(gate)
			<-done

			require.Zero(t, srv.fsyncCalls.Load())
			require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
			require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
			snap := fs.Snapshot()
			require.Equal(t, uint64(1), snap.Queues.DirtyFlushOK)
			require.Zero(t, snap.Queues.DirtyFlushErrors)

			fs.OnUnmount()
			require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
			require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		})
	}
}

func TestRawFS_OnUnmount_CleanMountAndLocalTmpfile_IssuesZeroSyncRPCs(t *testing.T) {
	t.Run("ReadOnlyOpsAndFailedMutationIssueZeroSyncRPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ino := srv.seedFile(1, "ro-check.txt", 0o644, []byte("hello"))
		srv.seedSymlink(1, "sym-check", "ro-check.txt")
		fs := newTestRawFS(t, srv)

		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "ro-check.txt")
		var miss fuse.EntryOut
		requireStatus(t, "Lookup miss", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "absent", &miss))
		_ = mustGetAttr(t, fs, ino)
		rd := mustOpenFile(t, fs, ino, syscall.O_RDONLY)
		_ = mustReadAt(t, fs, ino, rd.Fh, 0, 5)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: rd.Fh})

		var dh fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &dh))
		dirBuf := make([]byte, 4096)
		requireStatus(t, "ReadDirPlus", fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh, Size: uint32(len(dirBuf)),
		}, fuse.NewDirEntryList(dirBuf, 0)))
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh})

		var stfs fuse.StatfsOut
		requireStatus(t, "StatFs", fuse.OK, fs.StatFs(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, &stfs))
		sym := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "sym-check")
		_, rlSt := fs.Readlink(nil, &fuse.InHeader{NodeId: sym.NodeId})
		requireStatus(t, "Readlink", fuse.OK, rlSt)
		_, gxSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "user.missing", make([]byte, 8))
		requireStatus(t, "GetXAttr miss", fuse.Status(syscall.ENODATA), gxSt)
		_, lxSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: ino}, make([]byte, 64))
		requireStatus(t, "ListXAttr", fuse.OK, lxSt)

		var ioOut fuse.IoctlOut
		requireStatus(t, "Ioctl GETFLAGS", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
		}, nil, &ioOut, make([]byte, 8)))

		var dupOut fuse.EntryOut
		requireStatus(t, "failed Mkdir", fuse.Status(syscall.EEXIST), fs.Mkdir(nil, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0o755,
		}, "ro-check.txt", &dupOut))

		fs.OnUnmount()

		require.Zero(t, srv.fsyncCalls.Load())
		require.Zero(t, srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})

	t.Run("UnpromotedLocalTmpfileLifecycleIssuesZeroSyncRPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("local-only-data"))
		var attrOut fuse.AttrOut
		requireStatus(t, "local SetAttr", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
				Valid:    fuse.FATTR_SIZE,
				Size:     8,
			},
		}, &attrOut))
		requireStatus(t, "local Fallocate", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh, Offset: 0, Length: 16, Mode: 0,
		}))
		requireStatus(t, "local Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh,
		}))
		requireStatus(t, "local Flush", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh,
		}))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)

		fs.OnUnmount()

		require.Zero(t, srv.fsyncCalls.Load())
		require.Zero(t, srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})
}

func TestRawFS_OnUnmount_AlreadySyncedOrFileFsyncCovered_SkipsRedundantFsyncDir(t *testing.T) {
	t.Run("ExplicitFsyncDirSyncsLatestMetadata", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d1", 0o755)
		var dh fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &dh))
		requireStatus(t, "FsyncDir", fuse.OK, fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh}))
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh})

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})

	t.Run("ExplicitFileFsyncSyncsPriorMetadata", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d2", 0o755)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "f2.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))
		requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncCalls.Load())
		require.Zero(t, srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})

	t.Run("MetadataMutationAfterExplicitFsyncTriggersFsyncDir", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "f3.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))
		requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "after-fsync", 0o755)

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("ConcurrentMutationDuringExplicitFsyncStillTriggersFsyncDir", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "f-race.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))
		srv.fsyncHook = func(_ context.Context, _ *storagev1.FsyncRequest) error {
			mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "concurrent-dir", 0o755)
			return nil
		}
		requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
		srv.fsyncHook = nil
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
	})

	t.Run("MixedDataAndMetadataMutationsIssueSingleFsyncDir", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(44), withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d4", 0o755)
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "f4.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		fs.OnUnmount()

		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("MultiFileUnmountFsyncDirFailureDoesNotHangOrRetry", func(t *testing.T) {
		srv := newFakeStorage(t, withRootIno(45), withTTL(1.0, 1.0))
		srv.fsyncDirHook = func(context.Context, *storagev1.FsyncDirRequest) error {
			return errnoError(connect.CodeInternal, "EIO")
		}
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "d5", 0o755)
		createDirtySyncFiles(t, fs, 3)

		fs.OnUnmount()

		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		require.Equal(t, uint64(1), fs.Snapshot().Queues.DirtyFlushErrors)

		fs.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		require.Equal(t, uint64(1), fs.Snapshot().Queues.DirtyFlushErrors)
	})
}

func TestRawFS_OnUnmount_ReleaseQueueDrainAndFailedStageFallback_TriggersFsyncDir(t *testing.T) {
	t.Run("RetriedReclaimOrphanDrainedBeforePhase2TriggersFsyncDir", func(t *testing.T) {

		srv := newFakeStorage(t, withRootIno(77), withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orphan-retry.bin", syscall.O_RDWR, 0o644)
		requireStatus(t, "Unlink", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orphan-retry.bin"))
		requireStatus(t, "Fsync", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))

		var failReclaim atomic.Bool
		failReclaim.Store(true)
		srv.reclaimOrphanHook = func(context.Context, *storagev1.ReclaimOrphanRequest) error {
			if failReclaim.Load() {
				return errnoError(connect.CodeUnavailable, "EAGAIN")
			}
			return nil
		}
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		require.Equal(t, 1, fs.Snapshot().Queues.ReleaseQueueDepth)
		failReclaim.Store(false)

		fs.OnUnmount()

		require.GreaterOrEqual(t, srv.reclaimOrphanCalls.Load(), int64(2))
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("FailedStageUploadAfterCreateFallsBackToFsyncDir", func(t *testing.T) {

		srv := newFakeStorage(t, withRootIno(88), withTTL(1.0, 1.0))
		writeGate := newStepGate()
		writeEntered := make(chan struct{}, 2)
		srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			writeEntered <- struct{}{}
			if err := writeGate.wait(ctx); err != nil {
				return 0, err
			}
			return 0, errnoError(connect.CodeResourceExhausted, "ENOSPC")
		}
		fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
		t.Cleanup(writeGate.allowAll)

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "failed-stage.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("will-fail-upload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		<-writeEntered

		done := make(chan struct{})
		go func() {
			fs.OnUnmount()
			close(done)
		}()
		require.Eventually(t, func() bool {
			return fs.UnmountProgress().Active
		}, 2*time.Second, 5*time.Millisecond)
		writeGate.allowAll()
		<-done

		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 2, Total: 2}, fs.UnmountProgress())
		require.Equal(t, uint64(1), fs.Snapshot().Queues.DirtyFlushOK)
	})
}

func TestRawFS_OnUnmount_FsyncDirErrorAndUnimplementedHandling(t *testing.T) {
	t.Run("FsyncDirErrorLogsAndCountsDirtyFlushError", func(t *testing.T) {

		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		srv := newFakeStorage(t, withRootIno(55), withTTL(1.0, 1.0))
		srv.fsyncDirHook = func(context.Context, *storagev1.FsyncDirRequest) error {
			return errnoError(connect.CodeInternal, "EIO")
		}
		fs := newTestRawFS(t, srv, withLogger(logger))
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "err-dir", 0o755)

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		snap := fs.Snapshot()
		require.Zero(t, snap.Queues.DirtyFlushOK)
		require.Equal(t, uint64(1), snap.Queues.DirtyFlushErrors)
		require.Equal(t, []uint64{55}, collectUnmountFsyncLogInodes(t, logBuf.String()))

		fs.OnUnmount()
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		require.Equal(t, uint64(1), fs.Snapshot().Queues.DirtyFlushErrors)
		require.Equal(t, []uint64{55}, collectUnmountFsyncLogInodes(t, logBuf.String()))
	})

	t.Run("FsyncDirUnimplementedCountsOKWithoutWarning", func(t *testing.T) {

		var logBuf syncBuffer
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		srv := newFakeStorage(t, withRootIno(56), withTTL(1.0, 1.0))
		srv.fsyncDirHook = func(context.Context, *storagev1.FsyncDirRequest) error {
			return connect.NewError(connect.CodeUnimplemented, nil)
		}
		fs := newTestRawFS(t, srv, withLogger(logger))
		mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "unimpl-dir", 0o755)

		fs.OnUnmount()

		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
		snap := fs.Snapshot()
		require.Equal(t, uint64(1), snap.Queues.DirtyFlushOK)
		require.Zero(t, snap.Queues.DirtyFlushErrors)
		require.Empty(t, logBuf.String())
	})
}
