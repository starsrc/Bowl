package fusefs_test

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestRawFS_GivenCustomBranch_PropagatesBranchAcrossAllPOSIXRPCs(t *testing.T) {
	const customBranch = "feature/slice-5"

	s := newFakeStorage(t, withStorageBranch(customBranch), withTTL(0, 0))
	fs := newTestRawFS(t, s, withMode(fusefs.MountModeSync))

	mountOpts := fusefs.BuildMountOptions(fusefs.Config{
		DiskID: "disk-branch-test",
		Branch: customBranch,
		Mode:   fusefs.MountModeSync,
	})
	require.Equal(t, "bowl:disk-branch-test@"+customBranch, mountOpts.FsName)

	seededIno := s.seedFile(1, "seeded.txt", 0o644, []byte("seed"))
	_ = mustGetAttr(t, fs, seededIno)
	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "seeded.txt")
	seededLinkIno := s.seedSymlink(1, "seeded.sym", "seeded.txt")
	seededTarget, st := fs.Readlink(nil, &fuse.InHeader{NodeId: seededLinkIno})
	requireStatus(t, "Readlink seeded", fuse.OK, st)
	require.Equal(t, []byte("seeded.txt"), seededTarget)

	dirOut := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "dir", 0o755)

	var mknodOut fuse.EntryOut
	st = fs.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
		Mode:     syscall.S_IFREG | 0o644,
	}, "node.bin", &mknodOut)
	requireStatus(t, "Mknod", fuse.OK, st)

	var symOut fuse.EntryOut
	st = fs.Symlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "node.bin", "link.sym", &symOut)
	requireStatus(t, "Symlink", fuse.OK, st)

	target, st := fs.Readlink(nil, &fuse.InHeader{NodeId: symOut.NodeId})
	requireStatus(t, "Readlink", fuse.OK, st)
	require.Equal(t, []byte("node.bin"), target)

	var linkOut fuse.EntryOut
	st = fs.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: dirOut.NodeId},
		Oldnodeid: mknodOut.NodeId,
	}, "node-hard.bin", &linkOut)
	requireStatus(t, "Link", fuse.OK, st)

	st = fs.Rename(nil, &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
		Newdir:   dirOut.NodeId,
	}, "node-hard.bin", "node-renamed.bin")
	requireStatus(t, "Rename", fuse.OK, st)

	createOut := mustCreateFile(t, fs, dirOut.NodeId, "data.txt", uint32(syscall.O_RDWR), 0o644)
	mustWriteAt(t, fs, createOut.NodeId, createOut.Fh, 0, []byte("branch-data"))
	require.Equal(t, []byte("branch-data"), mustReadAt(t, fs, createOut.NodeId, createOut.Fh, 0, 11))

	var attrOut fuse.AttrOut
	setAttrIn := &fuse.SetAttrIn{}
	setAttrIn.NodeId = createOut.NodeId
	setAttrIn.Valid = fuse.FATTR_MODE
	setAttrIn.Mode = 0o600
	st = fs.SetAttr(nil, setAttrIn, &attrOut)
	requireStatus(t, "SetAttr", fuse.OK, st)

	st = fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
		Length:   64,
	})
	requireStatus(t, "Fallocate", fuse.OK, st)

	st = fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}}, "user.k", []byte("v"))
	requireStatus(t, "SetXAttr", fuse.OK, st)
	xbuf := make([]byte, 16)
	xn, st := fs.GetXAttr(nil, &fuse.InHeader{NodeId: createOut.NodeId}, "user.k", xbuf)
	requireStatus(t, "GetXAttr", fuse.OK, st)
	require.Equal(t, []byte("v"), xbuf[:xn])
	_, st = fs.ListXAttr(nil, &fuse.InHeader{NodeId: createOut.NodeId}, make([]byte, 64))
	requireStatus(t, "ListXAttr", fuse.OK, st)
	st = fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: createOut.NodeId}, "user.k")
	requireStatus(t, "RemoveXAttr", fuse.OK, st)

	st = fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}, Fh: createOut.Fh})
	requireStatus(t, "Fsync", fuse.OK, st)

	var openDirOut fuse.OpenOut
	st = fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}}, &openDirOut)
	requireStatus(t, "OpenDir", fuse.OK, st)
	dirBuf := make([]byte, 4096)
	st = fs.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: openDirOut.Fh}, fuse.NewDirEntryList(dirBuf, 0))
	requireStatus(t, "ReadDirPlus", fuse.OK, st)
	st = fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: openDirOut.Fh})
	requireStatus(t, "FsyncDir", fuse.OK, st)
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: openDirOut.Fh})

	var statOut fuse.StatfsOut
	st = fs.StatFs(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, &statOut)
	requireStatus(t, "StatFs", fuse.OK, st)

	var tmpOut fuse.CreateOut
	st = fs.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &tmpOut)
	requireStatus(t, "Tmpfile", fuse.OK, st)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}, Fh: createOut.Fh})
	requireStatus(t, "Unlink data.txt", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "data.txt"))
	requireStatus(t, "Unlink node.bin", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "node.bin"))
	requireStatus(t, "Unlink node-renamed.bin", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "node-renamed.bin"))
	requireStatus(t, "Unlink link.sym", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "link.sym"))
	requireStatus(t, "Rmdir", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"))

	require.Positive(t, s.lookupCalls.Load())
	require.Positive(t, s.getAttrCalls.Load())
	require.Positive(t, s.setAttrCalls.Load())
	require.Positive(t, s.mknodCalls.Load())
	require.Positive(t, s.mkdirCalls.Load())
	require.Positive(t, s.symlinkCalls.Load())
	require.Positive(t, s.readlinkCalls.Load())
	require.Positive(t, s.linkCalls.Load())
	require.Positive(t, s.unlinkCalls.Load())
	require.Positive(t, s.rmdirCalls.Load())
	require.Positive(t, s.renameCalls.Load())
	require.Positive(t, s.createCalls.Load())
	require.Positive(t, s.createTmpfileCalls.Load())
	require.Positive(t, s.readCalls.Load())
	require.Positive(t, s.writeCalls.Load())
	require.Positive(t, s.fsyncCalls.Load())
	require.Positive(t, s.fsyncDirCalls.Load())
	require.Positive(t, s.readDirPlusCalls.Load())
	require.Positive(t, s.fallocateCalls.Load())
	require.Positive(t, s.setXattrCalls.Load())
	require.Positive(t, s.getXattrCalls.Load())
	require.Positive(t, s.listXattrCalls.Load())
	require.Positive(t, s.removeXattrCalls.Load())
	require.Positive(t, s.statFSCalls.Load())
	require.Positive(t, s.reclaimOrphanCalls.Load())
}
