package fusefs_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestStagedTmpfile_ParentValidationAndSGIDInheritance(t *testing.T) {
	t.Run("RemovedOpenDirectoryReturnsENOENTWithZeroRPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		dir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "removed-dir", 0o755)
		var dh fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: dir.NodeId}}, &dh))
		requireStatus(t, "Rmdir", fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "removed-dir"))
		getAttrBefore := srv.getAttrCalls.Load()

		var out fuse.CreateOut
		st := fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: dir.NodeId},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &out)

		requireStatus(t, "Tmpfile on removed dir", fuse.ENOENT, st)
		require.Equal(t, getAttrBefore, srv.getAttrCalls.Load(), "RemovedDir must short-circuit without GetAttr RPC")
		fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dir.NodeId}, Fh: dh.Fh})
	})

	t.Run("NonDirectoryParentReturnsENOTDIR", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		regIno := srv.seedFile(1, "not-a-dir.txt", 0o644, []byte("x"))
		fs := newTestRawFS(t, srv)

		var tmpOut fuse.CreateOut
		requireStatus(t, "create unpromoted tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))

		var out1, out2 fuse.CreateOut
		stUnpromoted := fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &out1)
		stServerReg := fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: regIno},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &out2)

		requireStatus(t, "Tmpfile on unpromoted file", fuse.Status(syscall.ENOTDIR), stUnpromoted)
		requireStatus(t, "Tmpfile on server regular file", fuse.Status(syscall.ENOTDIR), stServerReg)

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)
	})

	t.Run("CachedSGIDParentInheritsGidWithZeroRPCsAndCacheMissFallsBackToOneGetAttr", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)

		cachedDir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "sgid-cached", 0o755)
		var dirAttr fuse.AttrOut
		requireStatus(t, "SetAttr S_ISGID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: cachedDir.NodeId},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o2755,
				Owner:    fuse.Owner{Gid: 2000},
			},
		}, &dirAttr))

		uncachedDirIno := srv.allocInoLockedForTest()
		srv.mu.Lock()
		now := timestamppb.Now()
		srv.nodes[uncachedDirIno] = &fakeNode{
			inode: &storagev1.Inode{
				Ino:        uncachedDirIno,
				Generation: 1,
				Attr: &storagev1.InodeAttribute{
					Mode:    syscall.S_IFDIR | syscall.S_ISGID | 0o755,
					Nlink:   2,
					Uid:     1000,
					Gid:     3000,
					Blksize: 4096,
					Atime:   now,
					Mtime:   now,
					Ctime:   now,
				},
			},
			xattrs: make(map[string][]byte),
		}
		srv.entries[uncachedDirIno] = make(map[string]uint64)
		srv.entries[srv.rootIno]["sgid-uncached"] = uncachedDirIno
		srv.mu.Unlock()

		getAttrBefore := srv.getAttrCalls.Load()

		var cachedOut, uncachedOut fuse.CreateOut
		requireStatus(t, "Tmpfile cached SGID", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{
				NodeId: cachedDir.NodeId,
				Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}},
			},
			Flags: uint32(syscall.O_RDWR),
			Mode:  0o640,
		}, &cachedOut))
		getAttrAfterCached := srv.getAttrCalls.Load()

		requireStatus(t, "Tmpfile uncached SGID", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{
				NodeId: uncachedDirIno,
				Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}},
			},
			Flags: uint32(syscall.O_RDWR),
			Mode:  0o640,
		}, &uncachedOut))
		getAttrAfterUncached := srv.getAttrCalls.Load()

		require.Equal(t, getAttrBefore, getAttrAfterCached, "cached SGID parent must issue zero GetAttr RPCs")
		require.Equal(t, getAttrBefore+1, getAttrAfterUncached, "cache-miss SGID parent must issue one GetAttr RPC")
		require.Equal(t, uint32(2000), cachedOut.Gid, "cached SGID parent must inherit gid=2000")
		require.Equal(t, uint32(3000), uncachedOut.Gid, "uncached SGID parent must inherit gid=3000")
		require.Zero(t, cachedOut.Mode&syscall.S_ISGID, "regular O_TMPFILE must not inherit S_ISGID mode bit")

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cachedOut.NodeId}, Fh: cachedOut.Fh})
		fs.Forget(cachedOut.NodeId, 1)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: uncachedOut.NodeId}, Fh: uncachedOut.Fh})
		fs.Forget(uncachedOut.NodeId, 1)
	})

	t.Run("CacheMissParentRespectsCancelChannel", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		dirIno := srv.seedDir(1, "cancel-dir", 0o755)
		var sawCanceled atomic.Bool
		cancel := make(chan struct{})
		srv.getAttrHook = func(ctx context.Context, _ *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			close(cancel)
			<-ctx.Done()
			sawCanceled.Store(true)
			return nil, ctx.Err()
		}
		fs := newTestRawFS(t, srv)

		var out fuse.CreateOut
		st := fs.Tmpfile(cancel, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: dirIno},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &out)

		requireStatus(t, "Tmpfile canceled parent GetAttr", fuse.EINTR, st)
		require.Equal(t, int64(1), srv.getAttrCalls.Load())
		require.True(t, sawCanceled.Load(), "cancel channel must propagate to parent GetAttr RPC")
	})
}

func (s *fakeStorage) allocInoLockedForTest() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocInoLocked()
}

func fillTmpfileXattrQuota(t *testing.T, fs *fusefs.RawFS, nodeID uint64) string {
	t.Helper()
	var firstKey string
	for i := range 256 {
		key := fmt.Sprintf("user.%0250d", i)
		if i == 0 {
			firstKey = key
		}
		requireStatus(t, "fill quota SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, key, []byte("v")))
	}
	return firstKey
}

func TestStagedTmpfile_LocalXAttr_ValidationBoundsQuotaSortingAndCapsCoherence(t *testing.T) {
	t.Run("ValidationFlagsBoundsSortingAndCapsCoherenceWithZeroRPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		nodeID := tmpOut.NodeId

		emptyListSz, emptyListSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, nil)
		requireStatus(t, "empty ListXAttr", fuse.OK, emptyListSt)
		require.Zero(t, emptyListSz)
		_, missGetSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.missing", make([]byte, 8))
		requireStatus(t, "missing GetXAttr", fuse.Status(syscall.ENODATA), missGetSt)
		requireStatus(t, "missing RemoveXAttr", fuse.Status(syscall.ENODATA), fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.missing"))

		requireStatus(t, "invalid SetXAttr flags", fuse.EINVAL, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: 0x4,
		}, "user.a", []byte("1")))
		requireStatus(t, "empty xattr name", fuse.EINVAL, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "", []byte("1")))
		requireStatus(t, "NUL byte in xattr name", fuse.EINVAL, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.a\x00b", []byte("1")))
		requireStatus(t, "name > 255 bytes", fuse.Status(syscall.ERANGE), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, strings.Repeat("k", 256), []byte("1")))
		requireStatus(t, "value > 65536 bytes", fuse.Status(syscall.ERANGE), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.big", make([]byte, 65537)))
		_, getInvalidNameSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "bad\x00name", make([]byte, 8))
		requireStatus(t, "GetXAttr NUL name", fuse.EINVAL, getInvalidNameSt)
		_, getLongNameSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, strings.Repeat("k", 256), make([]byte, 8))
		requireStatus(t, "GetXAttr name > 255", fuse.Status(syscall.ERANGE), getLongNameSt)
		requireStatus(t, "RemoveXAttr empty name", fuse.EINVAL, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: nodeID}, ""))
		requireStatus(t, "RemoveXAttr name > 255", fuse.Status(syscall.ERANGE), fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: nodeID}, strings.Repeat("k", 256)))

		requireStatus(t, "REPLACE on missing", fuse.Status(syscall.ENODATA), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrReplaceFlag,
		}, "user.z", []byte("v")))
		requireStatus(t, "flags==3 on missing", fuse.Status(syscall.ENODATA), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrCreateFlag | xattrReplaceFlag,
		}, "user.z", []byte("v")))
		requireStatus(t, "CREATE user.z", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrCreateFlag,
		}, "user.z", []byte("z\x00val")))
		requireStatus(t, "CREATE duplicate user.z", fuse.Status(syscall.EEXIST), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrCreateFlag,
		}, "user.z", []byte("dup")))
		requireStatus(t, "flags==3 on existing", fuse.Status(syscall.EEXIST), fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrCreateFlag | xattrReplaceFlag,
		}, "user.z", []byte("dup")))
		requireStatus(t, "REPLACE existing user.z", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: xattrReplaceFlag,
		}, "user.z", []byte("z-new")))

		maxName := strings.Repeat("m", 255)
		maxVal := buildSeededBytes(65536, 7)
		requireStatus(t, "max 255 name and 65536 value", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, maxName, maxVal))
		gotMax := make([]byte, 65536)
		gotMaxSz, gotMaxSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, maxName, gotMax)
		requireStatus(t, "GetXAttr max value", fuse.OK, gotMaxSt)
		require.Equal(t, uint32(65536), gotMaxSz)
		requireBytesMatch(t, "max xattr value", maxVal, gotMax)
		requireStatus(t, "RemoveXAttr maxName", fuse.OK, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: nodeID}, maxName))

		requireStatus(t, "empty value SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.empty", nil))
		emptySz, emptySt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.empty", make([]byte, 4))
		requireStatus(t, "GetXAttr empty value", fuse.OK, emptySt)
		require.Zero(t, emptySz)

		probeSz, probeSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.z", nil)
		requireStatus(t, "GetXAttr probe", fuse.OK, probeSt)
		require.Equal(t, uint32(5), probeSz)
		rangeSz, rangeSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.z", make([]byte, 2))
		requireStatus(t, "GetXAttr ERANGE", fuse.Status(syscall.ERANGE), rangeSt)
		require.Equal(t, uint32(5), rangeSz)
		buf := make([]byte, 16)
		gotSz, gotSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.z", buf)
		requireStatus(t, "GetXAttr read", fuse.OK, gotSt)
		require.Equal(t, "z-new", string(buf[:gotSz]))

		_, initialCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", make([]byte, 16))
		requireStatus(t, "initial capability absent", fuse.Status(syscall.ENODATA), initialCapSt)
		requireStatus(t, "set security.capability", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap-local")))
		capOut := make([]byte, 16)
		capSz, capSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capOut)
		requireStatus(t, "get security.capability after set", fuse.OK, capSt)
		require.Equal(t, "cap-local", string(capOut[:capSz]))

		wantList := "security.capability\x00user.empty\x00user.z\x00"
		listProbeSz, listProbeSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, nil)
		requireStatus(t, "ListXAttr probe", fuse.OK, listProbeSt)
		require.Equal(t, uint32(len(wantList)), listProbeSz)
		listRangeSz, listRangeSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, make([]byte, len(wantList)-1))
		requireStatus(t, "ListXAttr ERANGE", fuse.Status(syscall.ERANGE), listRangeSt)
		require.Equal(t, uint32(len(wantList)), listRangeSz)
		listBuf := make([]byte, len(wantList))
		gotListSz, gotListSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, listBuf)
		requireStatus(t, "ListXAttr read", fuse.OK, gotListSt)
		require.Equal(t, wantList, string(listBuf[:gotListSz]))

		requireStatus(t, "remove security.capability", fuse.OK, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability"))
		_, afterRmCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", make([]byte, 16))
		requireStatus(t, "get security.capability after remove", fuse.Status(syscall.ENODATA), afterRmCapSt)

		require.Zero(t, srv.setXattrCalls.Load()+srv.getXattrCalls.Load()+srv.listXattrCalls.Load()+srv.removeXattrCalls.Load()+srv.getAttrCalls.Load()+srv.setAttrCalls.Load())

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
		fs.Forget(nodeID, 1)
	})

	t.Run("ListQuotaEnforces65536ByteLimitOnNewKeysAndAllowsUpdate", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))

		existingKey := fillTmpfileXattrQuota(t, fs, tmpOut.NodeId)

		overflowSt := fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		}, "a", []byte("1"))
		updateSt := fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Flags: xattrReplaceFlag,
		}, existingKey, []byte("updated"))

		requireStatus(t, "new xattr over quota", fuse.Status(syscall.ENOSPC), overflowSt)
		requireStatus(t, "update existing xattr at quota", fuse.OK, updateSt)
		require.Zero(t, srv.setXattrCalls.Load())

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)
	})
}

func TestStagedTmpfile_LocalIoctlFlags_RoundTripAndImmutableAppendEnforcement(t *testing.T) {
	t.Run("IoctlFlagsRoundTripAndValidationWithZeroRPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		nodeID := tmpOut.NodeId

		var ioOut fuse.IoctlOut
		requireStatus(t, "unsupported ioctl", fuse.Status(syscall.ENOTTY), fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: 0x12345678,
		}, nil, &ioOut, nil))

		badFlags := make([]byte, 8)
		binary.LittleEndian.PutUint64(badFlags, 0x40000000)
		requireStatus(t, "unsupported SETFLAGS mask", fuse.Status(syscall.EOPNOTSUPP), fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, badFlags, &ioOut, nil))

		badXFlags := make([]byte, fsxattrStructLen)
		binary.LittleEndian.PutUint32(badXFlags[:4], 0x80000000)
		requireStatus(t, "unsupported FSSETXATTR mask", fuse.Status(syscall.EOPNOTSUPP), fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_FSSETXATTR,
		}, badXFlags, &ioOut, nil))

		requireStatus(t, "short outbuf GETFLAGS", fuse.EINVAL, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_GETFLAGS,
		}, nil, &ioOut, make([]byte, 2)))
		requireStatus(t, "short outbuf FSGETXATTR", fuse.EINVAL, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_FSGETXATTR,
		}, nil, &ioOut, make([]byte, 12)))

		in64 := make([]byte, 8)
		binary.LittleEndian.PutUint64(in64, uint64(fusefs.FS_APPEND_FL))
		requireStatus(t, "RO mount SETFLAGS", fuse.Status(syscall.EROFS), roFS.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, in64, &ioOut, nil))

		requireStatus(t, "SETFLAGS 64-bit APPEND", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, in64, &ioOut, nil))
		out64 := make([]byte, 8)
		requireStatus(t, "GETFLAGS 64-bit APPEND", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_GETFLAGS,
		}, nil, &ioOut, out64))
		require.Equal(t, uint64(fusefs.FS_APPEND_FL), binary.LittleEndian.Uint64(out64))

		in32 := make([]byte, 4)
		binary.LittleEndian.PutUint32(in32, fusefs.FS_IMMUTABLE_FL)
		requireStatus(t, "SETFLAGS 32-bit IMMUTABLE", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC32_SETFLAGS,
		}, in32, &ioOut, nil))
		out32 := make([]byte, 4)
		requireStatus(t, "GETFLAGS 32-bit IMMUTABLE", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC32_GETFLAGS,
		}, nil, &ioOut, out32))
		require.Equal(t, fusefs.FS_IMMUTABLE_FL, binary.LittleEndian.Uint32(out32))

		requireStatus(t, "re-set IMMUTABLE while immutable", fuse.EPERM, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC32_SETFLAGS,
		}, in32, &ioOut, nil))

		fsxIn := make([]byte, fsxattrStructLen)
		binary.LittleEndian.PutUint32(fsxIn[:4], fusefs.FS_XFLAG_APPEND)
		requireStatus(t, "FSSETXATTR clear IMMUTABLE to APPEND", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_FSSETXATTR,
		}, fsxIn, &ioOut, nil))
		fsxOut := make([]byte, fsxattrStructLen)
		requireStatus(t, "FSGETXATTR APPEND", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_FSGETXATTR,
		}, nil, &ioOut, fsxOut))
		require.Equal(t, fusefs.FS_XFLAG_APPEND, binary.LittleEndian.Uint32(fsxOut[:4]))

		require.Zero(t, srv.getAttrCalls.Load()+srv.setAttrCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
		fs.Forget(nodeID, 1)
	})

	t.Run("AppendAndImmutableEnforceEPERMOnWriteSetAttrAndAllocateLocally", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		nodeID := tmpOut.NodeId
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 0, []byte("hello"))
		appHandle := mustOpenFile(t, fs, nodeID, syscall.O_RDWR|syscall.O_APPEND)

		var ioOut fuse.IoctlOut
		appBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(appBuf, uint64(fusefs.FS_APPEND_FL))
		requireStatus(t, "set FS_APPEND_FL", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, appBuf, &ioOut, nil))

		_, nonEOFSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh, Offset: 1, Size: 1,
		}, []byte("x"))
		requireStatus(t, "FlagAppend non-EOF write", fuse.EPERM, nonEOFSt)

		var attrOut fuse.AttrOut
		requireStatus(t, "FlagAppend SetAttr FATTR_SIZE", fuse.EPERM, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_SIZE,
				Size:     0,
			},
		}, &attrOut))
		var truncOut fuse.OpenOut
		requireStatus(t, "FlagAppend Open O_TRUNC", fuse.EPERM, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Flags: syscall.O_RDWR | syscall.O_TRUNC,
		}, &truncOut))
		requireStatus(t, "FlagAppend Fallocate", fuse.EPERM, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh, Offset: 0, Length: 4, Mode: 0,
		}))

		_, zeroWrSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh, Offset: 0, Size: 0,
		}, nil)
		requireStatus(t, "FlagAppend 0-byte write", fuse.OK, zeroWrSt)
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 5, []byte("-eof"))
		mustWriteAt(t, fs, nodeID, appHandle.Fh, 0, []byte("-app"))
		requireStatus(t, "FlagAppend SetAttr FATTR_MODE", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o640,
			},
		}, &attrOut))
		requireBytesMatch(t, "append content", []byte("hello-eof-app"), mustReadAt(t, fs, nodeID, tmpOut.Fh, 0, 13))

		immBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(immBuf, uint64(fusefs.FS_IMMUTABLE_FL))
		requireStatus(t, "set FS_IMMUTABLE_FL", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, immBuf, &ioOut, nil))

		_, immZeroWrSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh, Offset: 13, Size: 0,
		}, nil)
		requireStatus(t, "FlagImmutable 0-byte write", fuse.EPERM, immZeroWrSt)
		_, immAppWrSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: appHandle.Fh, Offset: 0, Size: 1,
		}, []byte("z"))
		requireStatus(t, "FlagImmutable O_APPEND write", fuse.EPERM, immAppWrSt)
		requireStatus(t, "FlagImmutable SetAttr MODE", fuse.EPERM, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o600,
			},
		}, &attrOut))
		requireStatus(t, "FlagImmutable Fallocate", fuse.EPERM, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh, Offset: 0, Length: 4, Mode: 0,
		}))
		requireStatus(t, "FlagImmutable zero-valid SetAttr read", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: nodeID}},
		}, &attrOut))

		clearBuf := make([]byte, 8)
		requireStatus(t, "clear FS_IMMUTABLE_FL", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, clearBuf, &ioOut, nil))
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 13, []byte("!"))
		requireBytesMatch(t, "content after unfreeze", []byte("hello-eof-app!"), mustReadAt(t, fs, nodeID, tmpOut.Fh, 0, 14))

		require.Zero(t, srv.getAttrCalls.Load()+srv.setAttrCalls.Load()+srv.writeCalls.Load()+srv.fallocateCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: appHandle.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
		fs.Forget(nodeID, 1)
	})
}

func TestStagedTmpfile_LocalKillprivAndUnlinkedUnmountCleanliness(t *testing.T) {
	t.Run("WriteSetAttrFallocateOpenTruncStripKillprivLocally", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile SUID|SGID", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o6755,
		}, &tmpOut))
		nodeID := tmpOut.NodeId
		fh := tmpOut.Fh

		requireStatus(t, "set user.keep", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.keep", []byte("stay")))
		requireStatus(t, "set cap", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap1")))

		_, zeroSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh, Offset: 0, Size: 0,
		}, nil)
		requireStatus(t, "0-byte write", fuse.OK, zeroSt)
		require.Equal(t, uint32(syscall.S_IFREG|0o6755), mustGetAttr(t, fs, nodeID).Mode)
		capBuf := make([]byte, 16)
		_, zeroCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "0-byte write keeps cap", fuse.OK, zeroCapSt)

		mustWriteAt(t, fs, nodeID, fh, 0, []byte("payload"))
		require.Equal(t, uint32(syscall.S_IFREG|0o755), mustGetAttr(t, fs, nodeID).Mode)
		_, afterWriteCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "Write strips security.capability", fuse.Status(syscall.ENODATA), afterWriteCapSt)
		listAfterWrite := make([]byte, 64)
		listSz, listSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, listAfterWrite)
		requireStatus(t, "ListXAttr after Write", fuse.OK, listSt)
		require.Equal(t, "user.keep\x00", string(listAfterWrite[:listSz]))

		var attrOut fuse.AttrOut
		requireStatus(t, "restore 06755", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o6755,
			},
		}, &attrOut))
		requireStatus(t, "restore cap for UID", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap2")))
		requireStatus(t, "SetAttr ATIME_NOW preserves killpriv", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_ATIME_NOW,
			},
		}, &attrOut))
		require.Equal(t, uint32(syscall.S_IFREG|0o6755), attrOut.Mode)
		_, atimeCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "ATIME_NOW keeps cap", fuse.OK, atimeCapSt)

		requireStatus(t, "SetAttr UID strips SUID|SGID and cap", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_UID,
				Owner:    fuse.Owner{Uid: 1001},
			},
		}, &attrOut))
		require.Equal(t, uint32(syscall.S_IFREG|0o755), attrOut.Mode)
		_, uidCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "UID strips cap", fuse.Status(syscall.ENODATA), uidCapSt)

		requireStatus(t, "restore cap for MODE", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap3")))
		requireStatus(t, "SetAttr MODE 06755", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o6755,
			},
		}, &attrOut))
		require.Equal(t, uint32(syscall.S_IFREG|0o6755), attrOut.Mode)
		_, modeCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "SetAttr MODE strips cap", fuse.Status(syscall.ENODATA), modeCapSt)
		listAfterMode := make([]byte, 64)
		listModeSz, listModeSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, listAfterMode)
		requireStatus(t, "ListXAttr after SetAttr MODE", fuse.OK, listModeSt)
		require.Equal(t, "user.keep\x00", string(listAfterMode[:listModeSz]))

		requireStatus(t, "restore cap for falloc0", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap4")))
		requireStatus(t, "Fallocate mode=0", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh, Offset: 0, Length: 16, Mode: 0,
		}))
		require.Equal(t, uint32(syscall.S_IFREG|0o6755), mustGetAttr(t, fs, nodeID).Mode)
		_, falloc0CapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "Fallocate mode=0 clears cap", fuse.Status(syscall.ENODATA), falloc0CapSt)

		requireStatus(t, "restore cap for punch", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap5")))
		requireStatus(t, "Fallocate PUNCH_HOLE", fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh, Offset: 0, Length: 4, Mode: 0x03,
		}))
		require.Equal(t, uint32(syscall.S_IFREG|0o755), mustGetAttr(t, fs, nodeID).Mode)
		_, punchCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "Fallocate PUNCH_HOLE strips cap", fuse.Status(syscall.ENODATA), punchCapSt)

		requireStatus(t, "restore 06755 for O_TRUNC", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o6755,
			},
		}, &attrOut))
		requireStatus(t, "restore cap for O_TRUNC", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap6")))
		truncHandle := mustOpenFile(t, fs, nodeID, syscall.O_RDWR|syscall.O_TRUNC)
		require.Equal(t, uint32(syscall.S_IFREG|0o755), mustGetAttr(t, fs, nodeID).Mode)
		_, truncCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capBuf)
		requireStatus(t, "Open O_TRUNC strips cap", fuse.Status(syscall.ENODATA), truncCapSt)

		require.Zero(t, srv.writeCalls.Load()+srv.setAttrCalls.Load()+srv.setXattrCalls.Load()+srv.getXattrCalls.Load()+srv.listXattrCalls.Load()+srv.fallocateCalls.Load())
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: truncHandle.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh})
		fs.Forget(nodeID, 1)
	})

	t.Run("UnlinkedCloseAfterXattrAndIoctlLeavesZeroServerRPCsAndCleanUnmount", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv)
		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o600,
		}, &tmpOut))
		mustWriteAt(t, fs, tmpOut.NodeId, tmpOut.Fh, 0, []byte("unlinked-secret"))
		requireStatus(t, "SetXAttr", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		}, "user.secret", []byte("val")))
		immBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(immBuf, uint64(fusefs.FS_IMMUTABLE_FL))
		var ioOut fuse.IoctlOut
		requireStatus(t, "Ioctl SETFLAGS", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, immBuf, &ioOut, nil))

		requireStatus(t, "Flush", fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh,
		}))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		fs.Forget(tmpOut.NodeId, 1)
		fs.OnUnmount()

		totalRPCs := srv.mknodCalls.Load() + srv.writeCalls.Load() + srv.setAttrCalls.Load() +
			srv.setXattrCalls.Load() + srv.getXattrCalls.Load() + srv.listXattrCalls.Load() +
			srv.removeXattrCalls.Load() + srv.fsyncCalls.Load() + srv.fsyncDirCalls.Load() +
			srv.unlinkCalls.Load() + srv.reclaimOrphanCalls.Load()
		require.Zero(t, totalRPCs, "unlinked O_TMPFILE must issue zero server RPCs")
		require.Zero(t, fs.Snapshot().TmpFile.ActiveFiles)
		require.Zero(t, fs.Snapshot().TmpFile.ResidentBytes)
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})
}

func TestStagedTmpfile_LinkPromotion_ReplaysModeGidSortedXattrsFlagsInOrderAndUnlinkBarriersImmutable(t *testing.T) {
	t.Run("NonBlockingLinkAndOrderedCommitAndUnlinkBarrierOnImmutable", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		var (
			traceMu sync.Mutex
			events  []string
		)
		record := func(ev string) {
			traceMu.Lock()
			events = append(events, ev)
			traceMu.Unlock()
		}
		uploadStarted := make(chan struct{})
		var startedOnce sync.Once
		uploadGate, releaseUpload := newTestGate()
		defer releaseUpload()

		srv.mknodHook = func(_ context.Context, req *storagev1.MknodRequest) error {
			record(fmt.Sprintf("Mknod(mode=%o,gid=%d)", req.GetMode(), req.GetGid()))
			return nil
		}
		srv.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
			record(fmt.Sprintf("Write(off=%d,len=%d)", req.GetOffset(), len(req.GetData())))
			startedOnce.Do(func() { close(uploadStarted) })
			<-uploadGate
			return 0, nil
		}
		srv.setAttrHook = func(_ context.Context, req *storagev1.SetAttrRequest) error {
			switch {
			case req.Flags != nil:
				record(fmt.Sprintf("SetAttr(flags=%d)", req.GetFlags()))
			default:
				record(fmt.Sprintf("SetAttr(size=%d,mode=%o,gid=%d)", req.GetSize(), req.GetMode(), req.GetGid()))
			}
			return nil
		}
		srv.setXattrHook = func(_ context.Context, req *storagev1.SetXattrRequest) error {
			record(fmt.Sprintf("SetXattr(%s)", string(req.GetName())))
			return nil
		}

		fs := newTestRawFS(t, srv, withDirectIO(false))
		srcDir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "src-sgid", 0o755)
		requireStatus(t, "src SetAttr SGID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: srcDir.NodeId},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o2755,
				Owner:    fuse.Owner{Gid: 2000},
			},
		}, &fuse.AttrOut{}))
		dstDir := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "dst-sgid", 0o755)
		requireStatus(t, "dst SetAttr SGID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: dstDir.NodeId},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o2755,
				Owner:    fuse.Owner{Gid: 3000},
			},
		}, &fuse.AttrOut{}))

		traceMu.Lock()
		events = nil
		traceMu.Unlock()

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{
				NodeId: srcDir.NodeId,
				Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}},
			},
			Flags: uint32(syscall.O_RDWR),
			Mode:  0o600,
		}, &tmpOut))
		require.Equal(t, uint32(2000), tmpOut.Gid)
		nodeID := tmpOut.NodeId
		fh := tmpOut.Fh

		mustWriteAt(t, fs, nodeID, fh, 0, []byte("HEAD"))
		mustWriteAt(t, fs, nodeID, fh, 8192, []byte("TAIL"))
		requireStatus(t, "SetAttr mode+gid", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o4755,
				Owner:    fuse.Owner{Gid: 2500},
			},
		}, &fuse.AttrOut{}))
		requireStatus(t, "SetXAttr user.z", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.z", []byte("vz")))
		requireStatus(t, "SetXAttr user.a", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.a", []byte("va")))
		requireStatus(t, "SetXAttr security.capability", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap-prom")))
		immBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(immBuf, uint64(fusefs.FS_IMMUTABLE_FL))
		var ioOut fuse.IoctlOut
		requireStatus(t, "Ioctl FS_IMMUTABLE_FL", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, immBuf, &ioOut, nil))

		var linkOut fuse.EntryOut
		requireStatus(t, "Link promote", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: dstDir.NodeId}, Oldnodeid: nodeID,
		}, "promoted.bin", &linkOut))
		<-uploadStarted

		require.Equal(t, int64(1), srv.mknodCalls.Load())
		require.Equal(t, uint32(syscall.S_IFREG|0o4755), linkOut.Mode)
		require.Equal(t, uint32(2500), linkOut.Gid)
		require.Equal(t, uint64(8196), linkOut.Size)
		duringAttr := mustGetAttr(t, fs, nodeID)
		require.Equal(t, uint32(syscall.S_IFREG|0o4755), duringAttr.Mode)
		require.Equal(t, uint32(2500), duringAttr.Gid)
		duringLookup := mustLookup(t, fs, dstDir.NodeId, "promoted.bin")
		require.Equal(t, uint32(syscall.S_IFREG|0o4755), duringLookup.Mode)
		require.Equal(t, uint32(2500), duringLookup.Gid)

		setAttrBefore := srv.setAttrCalls.Load()
		unlinkDone := make(chan fuse.Status, 1)
		go func() {
			unlinkDone <- fs.Unlink(nil, &fuse.InHeader{NodeId: dstDir.NodeId}, "promoted.bin")
		}()
		truncDone := make(chan fuse.Status, 1)
		go func() {
			var openOut fuse.OpenOut
			truncDone <- fs.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: nodeID}, Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			}, &openOut)
		}()
		time.Sleep(30 * time.Millisecond)
		require.Zero(t, srv.unlinkCalls.Load(), "Unlink of promoted immutable file must wait for stageItemPromotedCommit")
		require.Equal(t, setAttrBefore, srv.setAttrCalls.Load(), "Open(O_TRUNC) of promoted immutable file must wait for stageItemPromotedCommit")

		releaseUpload()
		requireStatus(t, "Unlink immutable after commit", fuse.EPERM, waitUntilStatusChan(t, unlinkDone, "Unlink immutable"))
		requireStatus(t, "Open(O_TRUNC) immutable after commit", fuse.EPERM, waitUntilStatusChan(t, truncDone, "Open(O_TRUNC) immutable"))
		requireStatus(t, "Fsync promoted", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh,
		}))

		traceMu.Lock()
		gotEvents := slices.Clone(events)
		traceMu.Unlock()
		require.Len(t, gotEvents, 9)
		require.Equal(t, "Mknod(mode=100755,gid=2500)", gotEvents[0])
		require.ElementsMatch(t, []string{"Write(off=0,len=4096)", "Write(off=8192,len=4)"}, gotEvents[1:3])
		require.Equal(t, []string{
			"SetAttr(size=8196,mode=104755,gid=2500)",
			"SetXattr(security.capability)",
			"SetXattr(user.a)",
			"SetXattr(user.z)",
			"SetAttr(flags=1)",
			"SetAttr(size=0,mode=0,gid=0)",
		}, gotEvents[3:])

		capOut := make([]byte, 16)
		capSz, capSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capOut)
		requireStatus(t, "GetXAttr security.capability after commit", fuse.OK, capSt)
		require.Equal(t, "cap-prom", string(capOut[:capSz]))
		listOut := make([]byte, 64)
		listSz, listSt := fs.ListXAttr(nil, &fuse.InHeader{NodeId: nodeID}, listOut)
		requireStatus(t, "ListXAttr after commit", fuse.OK, listSt)
		require.Equal(t, "security.capability\x00user.a\x00user.z\x00", string(listOut[:listSz]))

		_, postWrSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh, Offset: 0, Size: 1,
		}, []byte("x"))
		requireStatus(t, "post-commit Write on immutable", fuse.EPERM, postWrSt)

		clearBuf := make([]byte, 8)
		requireStatus(t, "clear immutable", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, clearBuf, &ioOut, nil))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh})
		fs.Forget(nodeID, 3)
		requireStatus(t, "Unlink after clearing immutable", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: dstDir.NodeId}, "promoted.bin"))
	})

	t.Run("PromotedAppendAndZeroByteTmpfilePreserveCommitSemantics", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var appOut fuse.CreateOut
		requireStatus(t, "Tmpfile append", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR | syscall.O_APPEND),
			Mode:     0o4755,
		}, &appOut))
		mustWriteAt(t, fs, appOut.NodeId, appOut.Fh, 0, []byte("base"))
		requireStatus(t, "restore S_ISUID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: appOut.NodeId},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o4755,
			},
		}, &fuse.AttrOut{}))
		requireStatus(t, "set cap", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: appOut.NodeId},
		}, "security.capability", []byte("cap-app")))
		appFlags := make([]byte, 8)
		binary.LittleEndian.PutUint64(appFlags, uint64(fusefs.FS_APPEND_FL))
		var ioOut fuse.IoctlOut
		requireStatus(t, "set FS_APPEND_FL", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: appOut.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, appFlags, &ioOut, nil))

		var linkOut fuse.EntryOut
		requireStatus(t, "Link append", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: appOut.NodeId,
		}, "app.bin", &linkOut))

		rwHandle := mustOpenFile(t, fs, appOut.NodeId, syscall.O_RDWR)
		_, nonEOFSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: appOut.NodeId}, Fh: rwHandle.Fh, Offset: 0, Size: 1,
		}, []byte("x"))
		requireStatus(t, "promoted FlagAppend non-EOF write", fuse.EPERM, nonEOFSt)
		mustWriteAt(t, fs, appOut.NodeId, appOut.Fh, 0, []byte("-tail"))
		require.Zero(t, mustGetAttr(t, fs, appOut.NodeId).Mode&syscall.S_ISUID, "post-Link append write strips S_ISUID")
		_, afterAppCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: appOut.NodeId}, "security.capability", make([]byte, 16))
		requireStatus(t, "post-Link append write strips security.capability", fuse.Status(syscall.ENODATA), afterAppCapSt)
		requireBytesMatch(t, "promoted append bytes", []byte("base-tail"), mustReadAt(t, fs, appOut.NodeId, rwHandle.Fh, 0, 9))

		var emptyOut fuse.CreateOut
		requireStatus(t, "Tmpfile empty", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o4600,
		}, &emptyOut))
		requireStatus(t, "set user.zero", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: emptyOut.NodeId},
		}, "user.zero", []byte("z")))
		immFlags := make([]byte, 8)
		binary.LittleEndian.PutUint64(immFlags, uint64(fusefs.FS_IMMUTABLE_FL))
		requireStatus(t, "set immutable on 0-byte tmpfile", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: emptyOut.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, immFlags, &ioOut, nil))
		var emptyLink fuse.EntryOut
		requireStatus(t, "Link 0-byte immutable", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: emptyOut.NodeId,
		}, "empty-imm.bin", &emptyLink))
		requireStatus(t, "Fsync 0-byte immutable", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: emptyOut.NodeId}, Fh: emptyOut.Fh,
		}))
		requireStatus(t, "Unlink 0-byte immutable", fuse.EPERM, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "empty-imm.bin"))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: appOut.NodeId}, Fh: rwHandle.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: appOut.NodeId}, Fh: appOut.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: emptyOut.NodeId}, Fh: emptyOut.Fh})
		fs.Forget(appOut.NodeId, 2)
		fs.Forget(emptyOut.NodeId, 2)
	})

	t.Run("FailedMknodPreservesLocalStateAndMidFlightExtentPreservesSpecialAndCapsAcrossCacheMiss", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		srv.seedFile(1, "taken.bin", 0o644, []byte("x"))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o4755,
		}, &tmpOut))
		nodeID := tmpOut.NodeId
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 0, []byte("ext1"))
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 8192, []byte("ext2"))
		requireStatus(t, "set SUID and GID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o4755,
				Owner:    fuse.Owner{Gid: 2222},
			},
		}, &fuse.AttrOut{}))
		requireStatus(t, "set cap", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "security.capability", []byte("cap-edge")))
		immBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(immBuf, uint64(fusefs.FS_IMMUTABLE_FL))
		var ioOut fuse.IoctlOut
		requireStatus(t, "set immutable", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, immBuf, &ioOut, nil))

		var failedEntry fuse.EntryOut
		requireStatus(t, "Link EEXIST", fuse.Status(syscall.EEXIST), fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: nodeID,
		}, "taken.bin", &failedEntry))
		require.Equal(t, uint32(syscall.S_IFREG|0o4755), mustGetAttr(t, fs, nodeID).Mode)
		require.Equal(t, uint32(2222), mustGetAttr(t, fs, nodeID).Gid)
		capCheck := make([]byte, 16)
		capSz, capSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capCheck)
		requireStatus(t, "local cap preserved after failed Link", fuse.OK, capSt)
		require.Equal(t, "cap-edge", string(capCheck[:capSz]))

		ext1Done := make(chan struct{})
		ext2Started := make(chan struct{})
		ext2Gate, releaseExt2 := newTestGate()
		defer releaseExt2()
		var writeIdx atomic.Int32
		srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			idx := writeIdx.Add(1)
			if idx == 1 {
				close(ext1Done)
				return 0, nil
			}
			close(ext2Started)
			<-ext2Gate
			return 0, nil
		}

		var linkOut fuse.EntryOut
		requireStatus(t, "Link retry succeeds", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: nodeID,
		}, "final.bin", &linkOut))
		<-ext1Done
		<-ext2Started

		unlinkDone := make(chan fuse.Status, 1)
		go func() {
			unlinkDone <- fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "final.bin")
		}()
		time.Sleep(30 * time.Millisecond)
		require.Zero(t, srv.unlinkCalls.Load(), "Unlink must barrier even after first extent finished")

		releaseExt2()
		requireStatus(t, "Unlink after commit", fuse.EPERM, waitUntilStatusChan(t, unlinkDone, "Unlink immutable zero-ttl"))
		gotCapSz, gotCapSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "security.capability", capCheck)
		requireStatus(t, "cap preserved after commit", fuse.OK, gotCapSt)
		require.Equal(t, "cap-edge", string(capCheck[:gotCapSz]))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
		fs.Forget(nodeID, 2)
	})

	t.Run("OpenTruncateWhilePromotedCommitPendingPreservesXattrsAndExplicitGid", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		uploadGate, releaseUpload := newTestGate()
		defer releaseUpload()
		uploadStarted := make(chan struct{})
		var startedOnce sync.Once
		srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			startedOnce.Do(func() { close(uploadStarted) })
			<-uploadGate
			return 0, nil
		}

		fs := newTestRawFS(t, srv, withDirectIO(false))
		sgidDst := mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "sgid-dst-trunc", 0o755)
		requireStatus(t, "dst SetAttr SGID", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: sgidDst.NodeId},
				Valid:    fuse.FATTR_MODE | fuse.FATTR_GID,
				Mode:     0o2755,
				Owner:    fuse.Owner{Gid: 3000},
			},
		}, &fuse.AttrOut{}))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		nodeID := tmpOut.NodeId
		mustWriteAt(t, fs, nodeID, tmpOut.Fh, 0, []byte("data"))
		requireStatus(t, "set explicit Gid 2500", fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_GID,
				Owner:    fuse.Owner{Gid: 2500},
			},
		}, &fuse.AttrOut{}))
		requireStatus(t, "set user.keep", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, "user.keep", []byte("stay")))

		var linkOut fuse.EntryOut
		requireStatus(t, "Link into sgidDst", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: sgidDst.NodeId}, Oldnodeid: nodeID,
		}, "trunc-pending.bin", &linkOut))
		<-uploadStarted

		setAttrBefore := srv.setAttrCalls.Load()
		truncDone := make(chan fuse.Status, 1)
		var openOut fuse.OpenOut
		go func() {
			truncDone <- fs.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: nodeID}, Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			}, &openOut)
		}()
		time.Sleep(30 * time.Millisecond)

		require.Equal(t, setAttrBefore, srv.setAttrCalls.Load(), "Open with O_TRUNC must barrier on pending stageItemPromotedCommit")

		releaseUpload()
		requireStatus(t, "Open with O_TRUNC after commit", fuse.OK, waitUntilStatusChan(t, truncDone, "Open O_TRUNC"))
		afterAttr := mustGetAttr(t, fs, nodeID)
		require.Zero(t, afterAttr.Size)
		require.Equal(t, uint32(2500), afterAttr.Gid, "Open with O_TRUNC must not revert Gid to S_ISGID parent directory")
		xattrBuf := make([]byte, 16)
		xattrSz, xattrSt := fs.GetXAttr(nil, &fuse.InHeader{NodeId: nodeID}, "user.keep", xattrBuf)
		requireStatus(t, "user.keep preserved after O_TRUNC", fuse.OK, xattrSt)
		require.Equal(t, "stay", string(xattrBuf[:xattrSz]))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: openOut.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
		fs.Forget(nodeID, 2)
	})

	t.Run("CacheMissAfterUnlinkOrZeroTTLLookupWhilePromotedCommitPendingPreservesSpecialFlags", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		uploadGate, releaseUpload := newTestGate()
		defer releaseUpload()
		var startedCount atomic.Int32
		allStarted := make(chan struct{})
		srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			if startedCount.Add(1) == 2 {
				close(allStarted)
			}
			<-uploadGate
			return 0, nil
		}

		fs := newTestRawFS(t, srv, withDirectIO(false))
		var ioOut fuse.IoctlOut
		appFlags := make([]byte, 8)
		binary.LittleEndian.PutUint64(appFlags, uint64(fusefs.FS_APPEND_FL))

		var tmpUnlink fuse.CreateOut
		requireStatus(t, "Tmpfile unlink case", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpUnlink))
		mustWriteAt(t, fs, tmpUnlink.NodeId, tmpUnlink.Fh, 0, []byte("base1"))
		requireStatus(t, "set FS_APPEND_FL on unlink case", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: tmpUnlink.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, appFlags, &ioOut, nil))

		var tmpLookup fuse.CreateOut
		requireStatus(t, "Tmpfile lookup case", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpLookup))
		mustWriteAt(t, fs, tmpLookup.NodeId, tmpLookup.Fh, 0, []byte("base2"))
		requireStatus(t, "set FS_APPEND_FL on lookup case", fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: tmpLookup.NodeId}, Cmd: fusefs.FS_IOC_SETFLAGS,
		}, appFlags, &ioOut, nil))

		var linkUnlink, linkLookup fuse.EntryOut
		requireStatus(t, "Link unlink case", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: tmpUnlink.NodeId,
		}, "app-unlink.bin", &linkUnlink))
		requireStatus(t, "Link lookup case", fuse.OK, fs.Link(nil, &fuse.LinkIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: tmpLookup.NodeId,
		}, "app-lookup.bin", &linkLookup))
		<-allStarted

		requireStatus(t, "Unlink append file", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "app-unlink.bin"))
		gotUnlinkAttr := mustGetAttr(t, fs, tmpUnlink.NodeId)
		require.Equal(t, uint32(syscall.S_IFREG|0o644), gotUnlinkAttr.Mode)
		gotLookupAttr := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "app-lookup.bin")
		require.Equal(t, uint32(syscall.S_IFREG|0o644), gotLookupAttr.Mode)

		wr1Done := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: tmpUnlink.NodeId}, Fh: tmpUnlink.Fh, Offset: 0, Size: 3,
			}, []byte("bad"))
			wr1Done <- st
		}()
		wr2Done := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: tmpLookup.NodeId}, Fh: tmpLookup.Fh, Offset: 0, Size: 3,
			}, []byte("bad"))
			wr2Done <- st
		}()

		time.Sleep(30 * time.Millisecond)
		select {
		case st := <-wr1Done:
			t.Fatalf("getInodeMissWithContext lost specialInodes flag and allowed early staged Write with status %v", st)
		case st := <-wr2Done:
			t.Fatalf("lookupInodeAfterCache lost specialInodes flag and allowed early staged Write with status %v", st)
		default:
		}

		releaseUpload()
		requireStatus(t, "post-unlink cache-miss non-EOF Write on append file", fuse.EPERM, waitUntilStatusChan(t, wr1Done, "wr1Done"))
		requireStatus(t, "lookup cache-miss non-EOF Write on append file", fuse.EPERM, waitUntilStatusChan(t, wr2Done, "wr2Done"))

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpUnlink.NodeId}, Fh: tmpUnlink.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpLookup.NodeId}, Fh: tmpLookup.Fh})
		fs.Forget(tmpUnlink.NodeId, 2)
		fs.Forget(tmpLookup.NodeId, 3)
	})
}

func TestStagedTmpfile_LinkPromotion_WhenFencedBeforePromote_ReturnsEBUSYAndSkipsDirtyAndCache(t *testing.T) {
	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv, withDirectIO(false))

	var tmpOut fuse.CreateOut
	requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpOut))
	nodeID := tmpOut.NodeId
	mustWriteAt(t, fs, nodeID, tmpOut.Fh, 0, []byte("fence-before-promote-payload"))

	srv.mknodHook = func(_ context.Context, _ *storagev1.MknodRequest) error {
		fs.Fence(fuse.EBUSY, "fenced between Mknod and PromoteTmpfile")
		return nil
	}

	var linkOut fuse.EntryOut
	st := fs.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: nodeID,
	}, "fenced-promote.bin", &linkOut)

	requireStatus(t, "fenced linkTmp", fuse.EBUSY, st)
	require.Zero(t, linkOut.NodeId, "scene=fenced_linktmp_entryout want=0 got=%d", linkOut.NodeId)
	snap := fs.Snapshot()
	require.Zero(t, snap.TmpFile.Promotes, "scene=fenced_linktmp_promotes want=0 got=%d", snap.TmpFile.Promotes)
	require.Zero(t, snap.TmpFile.ActiveFiles, "scene=fenced_linktmp_active_files want=0 got=%d", snap.TmpFile.ActiveFiles)
	require.Zero(t, snap.TmpFile.ResidentBytes, "scene=fenced_linktmp_resident_bytes want=0 got=%d", snap.TmpFile.ResidentBytes)
	require.Zero(t, snap.NamespaceCache.Entries, "scene=fenced_linktmp_cache_entries want=0 got=%d", snap.NamespaceCache.Entries)

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: tmpOut.Fh})
	fs.Forget(nodeID, 1)
}
