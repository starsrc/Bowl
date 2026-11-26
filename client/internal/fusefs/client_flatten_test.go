package fusefs_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestStep15_HandleTable_AnyOpenFilesAndAnyOpenDirs(t *testing.T) {
	ht := fusefs.NewHandleTable(nil)
	require.False(t, ht.AnyOpenFiles())
	require.False(t, ht.AnyOpenDirs())

	fileFH, err := ht.Open(100, syscall.O_RDONLY, false)
	require.NoError(t, err)
	require.True(t, ht.AnyOpenFiles())
	require.False(t, ht.AnyOpenDirs())

	dirFH, err := ht.Open(200, syscall.O_RDONLY, true)
	require.NoError(t, err)
	require.True(t, ht.AnyOpenFiles())
	require.True(t, ht.AnyOpenDirs())

	ht.Release(fileFH)
	require.False(t, ht.AnyOpenFiles())
	require.True(t, ht.AnyOpenDirs())

	ht.Release(dirFH)
	require.False(t, ht.AnyOpenFiles())
	require.False(t, ht.AnyOpenDirs())
}

func TestStep15_NoUserSpaceCache_LookupGetAttrReadlinkReadDirSinglePage(t *testing.T) {
	srv := newFakeStorage(t, withTTL(60.0, 60.0))
	fileIno := srv.seedFile(1, "plain.txt", 0o644, []byte("hello"))
	linkIno := srv.seedSymlink(1, "sym", "/target")
	fs := newTestRawFS(t, srv)

	lookupsBefore := srv.lookupCalls.Load()
	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "plain.txt")
	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "plain.txt")
	require.Equal(t, lookupsBefore+2, srv.lookupCalls.Load())

	getAttrsBefore := srv.getAttrCalls.Load()
	_ = mustGetAttr(t, fs, fileIno)
	_ = mustGetAttr(t, fs, fileIno)
	require.Equal(t, getAttrsBefore+2, srv.getAttrCalls.Load())

	readlinksBefore := srv.readlinkCalls.Load()
	t1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: linkIno})
	t2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: linkIno})
	requireStatus(t, "Readlink 1", fuse.OK, st1)
	requireStatus(t, "Readlink 2", fuse.OK, st2)
	require.Equal(t, "/target", string(t1))
	require.Equal(t, "/target", string(t2))
	require.Equal(t, readlinksBefore+2, srv.readlinkCalls.Load())

	seedLargeDirectory(srv, 1, 600)
	var dh fuse.OpenOut
	requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &dh))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dh.Fh})

	rdpBefore := srv.readDirPlusCalls.Load()
	buf := make([]byte, 128<<10)
	st := fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Fh:       dh.Fh,
		Offset:   0,
		Size:     uint32(len(buf)),
	}, fuse.NewDirEntryList(buf, 0))
	requireStatus(t, "ReadDirPlus single page", fuse.OK, st)
	require.Equal(t, rdpBefore+1, srv.readDirPlusCalls.Load())
}

func TestStep15_DentryIndex_SkipsPreLookupWhenUnrelatedHandlesOpen(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv)

	unrelated := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "unrelated.txt", syscall.O_RDWR, 0o644)
	defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: unrelated.NodeId}, Fh: unrelated.Fh})

	victim := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "victim.txt", syscall.O_RDWR, 0o644)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: victim.NodeId}, Fh: victim.Fh})

	lookupsBefore := srv.lookupCalls.Load()
	getAttrsBefore := srv.getAttrCalls.Load()
	requireStatus(t, "Unlink victim", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "victim.txt"))
	require.Equal(t, lookupsBefore, srv.lookupCalls.Load())
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())

	unrelatedDir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "open-dir", 0o755)
	var dirOut fuse.OpenOut
	requireStatus(t, "OpenDir unrelated", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: unrelatedDir.NodeId}}, &dirOut))
	defer fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: unrelatedDir.NodeId}, Fh: dirOut.Fh})

	_ = mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "victim-dir", 0o755)
	lookupsBefore = srv.lookupCalls.Load()
	getAttrsBefore = srv.getAttrCalls.Load()
	requireStatus(t, "Rmdir victim-dir", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "victim-dir"))
	require.Equal(t, lookupsBefore, srv.lookupCalls.Load())
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())

	src := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "rename-src.txt", syscall.O_RDWR, 0o644)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
	dst := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "rename-dst.txt", syscall.O_RDWR, 0o644)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dst.NodeId}, Fh: dst.Fh})

	lookupsBefore = srv.lookupCalls.Load()
	getAttrsBefore = srv.getAttrCalls.Load()
	requireStatus(t, "Rename known src over known dst", fuse.OK, fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Newdir:   fuse.FUSE_ROOT_ID,
	}, "rename-src.txt", "rename-dst.txt"))
	require.Equal(t, lookupsBefore, srv.lookupCalls.Load())
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())
}

func TestStep16_KnownInodes_ZeroRPCOpenOpenDirAndFallbackGetAttr(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fileIno := srv.seedFile(1, "hello.txt", 0o644, []byte("initial-data"))
	dirIno := srv.seedDir(1, "sub", 0o755)
	fs := newTestRawFS(t, srv)

	getAttrsBefore := srv.getAttrCalls.Load()
	var rootDirOut fuse.OpenOut
	requireStatus(t, "OpenDir root zero RPC", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &rootDirOut))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: rootDirOut.Fh})

	var rootAsFile fuse.OpenOut
	requireStatus(t, "Open root returns EISDIR zero RPC", fuse.Status(syscall.EISDIR), fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    syscall.O_RDONLY,
	}, &rootAsFile))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())

	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "hello.txt")
	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "sub")

	getAttrsBefore = srv.getAttrCalls.Load()
	var fileOut fuse.OpenOut
	requireStatus(t, "Open known file zero RPC", fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    syscall.O_RDWR,
	}, &fileOut))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fileOut.Fh})

	var fileAsDir fuse.OpenOut
	requireStatus(t, "OpenDir known file ENOTDIR zero RPC", fuse.Status(syscall.ENOTDIR), fs.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
	}, &fileAsDir))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())

	var subOut fuse.OpenOut
	requireStatus(t, "OpenDir known dir zero RPC", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: dirIno},
	}, &subOut))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirIno}, Fh: subOut.Fh})

	var truncOut fuse.OpenOut
	getAttrsBefore = srv.getAttrCalls.Load()
	setAttrsBefore := srv.setAttrCalls.Load()
	requireStatus(t, "Open O_TRUNC skips GetAttr", fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    syscall.O_RDWR | syscall.O_TRUNC,
	}, &truncOut))
	require.Equal(t, getAttrsBefore, srv.getAttrCalls.Load())
	require.Equal(t, setAttrsBefore+1, srv.setAttrCalls.Load())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: truncOut.Fh})

	fs.Forget(fileIno, 1)
	getAttrsBefore = srv.getAttrCalls.Load()
	var afterForget fuse.OpenOut
	requireStatus(t, "Open after Forget falls back to GetAttr", fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    syscall.O_RDONLY,
	}, &afterForget))
	require.Equal(t, getAttrsBefore+1, srv.getAttrCalls.Load())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: afterForget.Fh})

	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "hello.txt")
	requireStatus(t, "Unlink deletes knownInodes", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "hello.txt"))
	getAttrsBefore = srv.getAttrCalls.Load()
	var afterUnlink fuse.OpenOut
	requireStatus(t, "Open after Unlink falls back to GetAttr ENOENT", fuse.ENOENT, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    syscall.O_RDONLY,
	}, &afterUnlink))
	require.Equal(t, getAttrsBefore+1, srv.getAttrCalls.Load())
	require.Zero(t, fs.Snapshot().Handles.OpenFiles)
}

func TestStep16_UnlinkSerializesKnownInodesInvalidationWithConcurrentOpen(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fileIno := srv.seedFile(1, "race.txt", 0o644, []byte("data"))
	fs := newTestRawFS(t, srv)

	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "race.txt")

	unlinkEntered := make(chan struct{})
	unlinkRelease := make(chan struct{})
	srv.unlinkHook = func(_ context.Context, req *storagev1.UnlinkRequest) error {
		if string(req.GetName()) == "race.txt" {
			close(unlinkEntered)
			<-unlinkRelease
		}
		return nil
	}

	unlinkDone := make(chan fuse.Status, 1)
	go func() {
		unlinkDone <- fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "race.txt")
	}()
	<-unlinkEntered

	openDone := make(chan fuse.Status, 1)
	go func() {
		var out fuse.OpenOut
		openDone <- fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    syscall.O_RDONLY,
		}, &out)
	}()
	waitForOpInflightAtLeast(t, fs, fusefs.OpOpen, 1)

	close(unlinkRelease)
	requireStatus(t, "Unlink", fuse.OK, <-unlinkDone)
	requireStatus(t, "Concurrent Open after Unlink serialized by inodeLocks", fuse.ENOENT, <-openDone)
	require.Zero(t, fs.Snapshot().Handles.OpenFiles)
}
