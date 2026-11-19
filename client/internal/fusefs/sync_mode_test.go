package fusefs_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestSyncMode_RegularFileWrites_ExecutesSynchronouslyWithZeroStagingAndSurfacesQuotaAndEpochErrors(t *testing.T) {
	// Given sync mount mode with an explicit StageDir and an isolated TMPDIR.
	emptyTmpDir := t.TempDir()
	t.Setenv("TMPDIR", emptyTmpDir)

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	unusedStageDir := filepath.Join(t.TempDir(), "sync-no-stage")
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withStageDir(unusedStageDir))
	ephemeralCheckFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	_ = ephemeralCheckFS

	mountOpts := fusefs.BuildMountOptions(fusefs.Config{DiskID: "disk-sync", Mode: fusefs.MountModeSync})
	require.Equal(t, []string{"default_permissions"}, mountOpts.Options, "scene=sync_mount_options want=[default_permissions] got=%v", mountOpts.Options)
	requireStatus(t, "sync CheckWritable", fuse.OK, syncFS.CheckWritable())

	// When the caller creates a regular file and calls Write and multi-segment Writev without Flush or Fsync.
	created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "sync.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, []byte("hello-"))
	writtenV, writevSt := syncFS.Writev(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   6,
		Size:     9,
	}, [][]byte{[]byte("sync"), []byte("-mode")})
	requireStatus(t, "sync Writev", fuse.OK, writevSt)
	require.Equal(t, uint32(9), writtenV, "scene=sync_writev want=9 got=%d", writtenV)

	// Then zero host stage directories exist and writes have reached StorageService synchronously.
	_, stageStatErr := os.Stat(unusedStageDir)
	require.ErrorIs(t, stageStatErr, os.ErrNotExist, "scene=sync_unused_stage_dir want=NotExist got=%v", stageStatErr)
	tmpEntries, tmpReadErr := os.ReadDir(emptyTmpDir)
	require.NoError(t, tmpReadErr)
	require.Empty(t, tmpEntries, "scene=sync_ephemeral_tmpdir want=0_entries got=%d", len(tmpEntries))

	snap := syncFS.Snapshot()
	require.Zero(t, snap.Stage.CapacityBytes, "scene=sync_stage_capacity want=0 got=%d", snap.Stage.CapacityBytes)
	require.Zero(t, snap.Stage.StagedCalls, "scene=sync_staged_calls want=0 got=%d", snap.Stage.StagedCalls)
	require.Zero(t, snap.Stage.StagedBytes, "scene=sync_staged_bytes want=0 got=%d", snap.Stage.StagedBytes)
	require.Equal(t, uint64(2), snap.Stage.PassthroughCalls, "scene=sync_passthrough_calls want=2 got=%d", snap.Stage.PassthroughCalls)
	require.Equal(t, uint64(15), snap.Stage.PassthroughBytes, "scene=sync_passthrough_bytes want=15 got=%d", snap.Stage.PassthroughBytes)
	require.Equal(t, int64(2), srv.writeCalls.Load(), "scene=sync_server_write_calls want=2 got=%d", srv.writeCalls.Load())
	stored, ok := srv.fileBytes(created.NodeId)
	require.True(t, ok)
	requireBytesMatch(t, "sync immediate server bytes", []byte("hello-sync-mode"), stored)

	// Server quota ENOSPC and stale Epoch EBUSY fail synchronously on the Write call.
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		return 0, errnoError(connect.CodeResourceExhausted, "ENOSPC")
	}
	nQuota, stQuota := syncFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   15,
		Size:     4,
	}, []byte("full"))
	requireStatus(t, "sync Write ENOSPC", fuse.Status(syscall.ENOSPC), stQuota)
	require.Zero(t, nQuota)

	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		return 0, errnoError(connect.CodeFailedPrecondition, "EBUSY")
	}
	nFence, stFence := syncFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   15,
		Size:     4,
	}, []byte("old!"))
	requireStatus(t, "sync Write EBUSY", fuse.Status(syscall.EBUSY), stFence)
	require.Zero(t, nFence)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestTmpfile_SyncMode_Validation_RejectsInvalidInputsWithoutRPC(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withRootIno(99), withTTL(1.0, 1.0))
	roFS := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withStatsFile(true))
	var out fuse.CreateOut

	// When / Then
	requireStatus(t, "RO Tmpfile", fuse.Status(syscall.EROFS), roFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &out))
	requireStatus(t, "stats Tmpfile", fuse.Status(syscall.ENOTDIR), syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fusefs.VirtualStatsIno},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &out))
	requireStatus(t, "unmapped RootIno Tmpfile", fuse.ENOENT, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 99},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &out))
	requireStatus(t, "O_RDONLY Tmpfile", fuse.EINVAL, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDONLY),
		Mode:     0o600,
	}, &out))
	requireStatus(t, "O_ACCMODE Tmpfile", fuse.EINVAL, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_ACCMODE),
		Mode:     0o600,
	}, &out))
	require.Zero(t, srv.createTmpfileCalls.Load(), "scene=tmpfile_validation want=0_rpcs got=%d", srv.createTmpfileCalls.Load())
}

func TestTmpfile_SyncMode_UnlinkedLifecycle_RoutesAllFileOpsToServerAndReclaimsOnLastRelease(t *testing.T) {
	// Given sync RawFS instances with DirectIO disabled and enabled, RootIno 42, and isolated TMPDIR.
	emptyTmpDir := t.TempDir()
	t.Setenv("TMPDIR", emptyTmpDir)

	srv := newFakeStorage(t, withRootIno(42), withTTL(1.0, 1.0))
	var capturedReqs []*storagev1.CreateTmpfileRequest
	srv.createTmpfileHook = func(_ context.Context, req *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		capturedReqs = append(capturedReqs, req)
		return nil, nil
	}

	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
	dioFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(true))

	// When the caller creates a linkable tmpfile on syncFS and an O_EXCL tmpfile on dioFS.
	var tmpOut fuse.CreateOut
	st := syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1001, Gid: 1002}},
		},
		Flags: uint32(syscall.O_RDWR),
		Mode:  0o640,
	}, &tmpOut)
	requireStatus(t, "sync Tmpfile linkable", fuse.OK, st)

	var dioOut fuse.CreateOut
	stDIO := dioFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 2001, Gid: 2002}},
		},
		Flags: uint32(syscall.O_WRONLY | syscall.O_EXCL),
		Mode:  syscall.S_IFREG | 0o600,
	}, &dioOut)
	requireStatus(t, "sync Tmpfile O_EXCL DirectIO", fuse.OK, stDIO)

	// Then CreateTmpfile receives the mapped ParentIno, S_IFREG mode, and Exclusive flag with zero host files.
	require.Len(t, capturedReqs, 2)
	require.Equal(t, uint64(42), capturedReqs[0].GetParentIno())
	require.Equal(t, uint32(syscall.S_IFREG|0o640), capturedReqs[0].GetMode())
	require.Equal(t, uint32(1001), capturedReqs[0].GetUid())
	require.Equal(t, uint32(1002), capturedReqs[0].GetGid())
	require.False(t, capturedReqs[0].GetExclusive())
	require.Equal(t, uint32(fuse.FOPEN_KEEP_CACHE), tmpOut.OpenFlags)
	require.Zero(t, tmpOut.Nlink)

	require.True(t, capturedReqs[1].GetExclusive())
	require.Equal(t, uint32(fuse.FOPEN_DIRECT_IO), dioOut.OpenFlags)
	dioFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dioOut.NodeId}, Fh: dioOut.Fh})
	require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=dio_excl_release want=1_reclaim got=%d", srv.reclaimOrphanCalls.Load())
	require.False(t, srv.hasNode(dioOut.NodeId))

	tmpEntries, err := os.ReadDir(emptyTmpDir)
	require.NoError(t, err)
	require.Empty(t, tmpEntries, "scene=sync_tmpfile_host_disk want=0_files got=%d", len(tmpEntries))
	require.Zero(t, syncFS.Snapshot().TmpFile.ActiveFiles)
	require.Zero(t, syncFS.Snapshot().TmpFile.ResidentBytes)

	// GetAttr hits the cached inode populated by Tmpfile without issuing an extra RPC.
	attr0 := mustGetAttr(t, syncFS, tmpOut.NodeId)
	require.Zero(t, srv.getAttrCalls.Load(), "scene=tmpfile_cached_getattr want=0_rpcs got=%d", srv.getAttrCalls.Load())
	require.Zero(t, attr0.Nlink)

	// Write, Read, SetAttr, Fallocate preallocation and hole punch, and Fsync execute on serverIno.
	mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, []byte("helloWORLD!"))
	requireBytesMatch(t, "sync tmpfile Read", []byte("helloWORLD!"), mustReadAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, 11))

	var setOut fuse.AttrOut
	requireStatus(t, "sync tmpfile SetAttr", fuse.OK, syncFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
			Valid:    fuse.FATTR_MODE,
			Mode:     0o600,
		},
	}, &setOut))
	require.Equal(t, uint32(syscall.S_IFREG|0o600), setOut.Mode)

	requireStatus(t, "sync tmpfile Fallocate extend", fuse.OK, syncFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		Fh:       tmpOut.Fh,
		Offset:   11,
		Length:   5,
		Mode:     0,
	}))
	requireStatus(t, "sync tmpfile Fallocate punch hole", fuse.OK, syncFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		Fh:       tmpOut.Fh,
		Offset:   5,
		Length:   5,
		Mode:     0x02 | 0x01,
	}))
	requireBytesMatch(t, "sync tmpfile post-fallocate Read", []byte("hello\x00\x00\x00\x00\x00!\x00\x00\x00\x00\x00"), mustReadAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, 16))

	requireStatus(t, "sync tmpfile Fsync", fuse.OK, syncFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
		Fh:       tmpOut.Fh,
	}))
	require.Equal(t, int64(1), srv.fsyncCalls.Load())

	// With a dirty write and a second open handle, ReclaimOrphan waits until the last handle closes.
	mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 16, []byte("tail"))
	secondOpen := mustOpenFile(t, syncFS, tmpOut.NodeId, syscall.O_RDONLY)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=first_handle_release want=still_1 got=%d", srv.reclaimOrphanCalls.Load())
	require.True(t, srv.hasNode(tmpOut.NodeId))

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: secondOpen.Fh})
	require.Equal(t, int64(2), srv.reclaimOrphanCalls.Load(), "scene=last_handle_release want=2 got=%d", srv.reclaimOrphanCalls.Load())
	require.False(t, srv.hasNode(tmpOut.NodeId))
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes)
	require.Zero(t, syncFS.Snapshot().Queues.DirtyInodes)

	fsyncBeforeUnmount := srv.fsyncCalls.Load()
	syncFS.OnUnmount()
	require.Equal(t, fsyncBeforeUnmount, srv.fsyncCalls.Load(), "scene=unmount_after_reclaim want=no_extra_fsync")
}

func TestTmpfile_SyncMode_OpenHandleFailure_ImmediatelyReclaimsServerOrphan(t *testing.T) {
	// Given a faulty server returning an S_IFDIR inode on CreateTmpfile so openHandle fails with EISDIR.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var leakedIno uint64
	srv.createTmpfileHook = func(_ context.Context, _ *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		srv.mu.Lock()
		leakedIno = srv.allocInoLocked()
		now := timestamppb.Now()
		inode := &storagev1.Inode{
			Ino:        leakedIno,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFDIR | 0o700,
				Nlink:   0,
				Blksize: 4096,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		}
		srv.nodes[leakedIno] = &fakeNode{
			inode:      inode,
			xattrs:     make(map[string][]byte),
			orphanKind: fakeOrphanTmpfileLinkable,
		}
		srv.mu.Unlock()
		return &storagev1.CreateTmpfileResponse{
			Inode:               cloneProtoInode(inode),
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		}, nil
	}
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

	// When
	var out fuse.CreateOut
	st := syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &out)

	// Then openHandle fails with EISDIR and reclaims the server orphan without leaking.
	requireStatus(t, "Tmpfile openHandle EISDIR", fuse.Status(syscall.EISDIR), st)
	require.NotZero(t, leakedIno)
	require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=open_handle_fail_reclaim want=1 got=%d", srv.reclaimOrphanCalls.Load())
	require.False(t, srv.hasNode(leakedIno), "scene=open_handle_fail_reclaim want=server_orphan_deleted")
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes)
}

func TestTmpfile_SyncMode_LinkPromotion_ClearsOrphanPreservesDirtyAndCountsSinglePromote(t *testing.T) {
	// Given a linkable sync tmpfile with dirty written data and no explicit Fsync.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

	var tmpOut fuse.CreateOut
	requireStatus(t, "sync Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpOut))
	mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, []byte("promoted-payload"))
	require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes)
	require.Equal(t, 1, syncFS.Snapshot().Queues.DirtyInodes)

	// When the caller promotes via Link, releases the original handle, and creates a second hard link.
	var linkOut fuse.EntryOut
	requireStatus(t, "promote Link", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpOut.NodeId,
	}, "promoted.bin", &linkOut))
	require.Equal(t, uint32(1), linkOut.Nlink)
	require.Equal(t, uint64(1), syncFS.Snapshot().TmpFile.Promotes, "scene=first_promote want=1")
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes, "scene=post_link_orphan_cleared want=0")

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})

	var linkOut2 fuse.EntryOut
	requireStatus(t, "second hard Link", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpOut.NodeId,
	}, "promoted-2.bin", &linkOut2))

	// Then Release does not call ReclaimOrphan or clear dirtyInodes, and the second Link does not double-count Promotes.
	require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=promoted_release want=0_reclaim got=%d", srv.reclaimOrphanCalls.Load())
	require.True(t, srv.hasNode(tmpOut.NodeId))
	require.Equal(t, uint32(2), linkOut2.Nlink)
	require.Equal(t, uint64(1), syncFS.Snapshot().TmpFile.Promotes, "scene=second_hard_link want=promotes_still_1")
	require.Equal(t, 1, syncFS.Snapshot().Queues.DirtyInodes, "scene=promoted_dirty_preserved want=1")

	looked := mustLookup(t, syncFS, fuse.FUSE_ROOT_ID, "promoted.bin")
	require.Equal(t, tmpOut.NodeId, looked.NodeId)
	require.Equal(t, uint64(16), looked.Size)

	syncFS.OnUnmount()
	require.Equal(t, int64(1), srv.fsyncCalls.Load(), "scene=unmount_flushes_promoted_dirty want=1 got=%d", srv.fsyncCalls.Load())
}

func TestTmpfile_SyncMode_LinkFailure_PreservesOrphanStateAndReclaimsOnRelease(t *testing.T) {
	// Given an existing named file plus an O_EXCL tmpfile and a linkable tmpfile.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.seedFile(1, "taken.bin", 0o644, []byte("existing"))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

	var exclOut fuse.CreateOut
	requireStatus(t, "O_EXCL Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR | syscall.O_EXCL),
		Mode:     0o600,
	}, &exclOut))
	mustWriteAt(t, syncFS, exclOut.NodeId, exclOut.Fh, 0, []byte("secret"))

	var collideOut fuse.CreateOut
	requireStatus(t, "linkable Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &collideOut))
	mustWriteAt(t, syncFS, collideOut.NodeId, collideOut.Fh, 0, []byte("collide"))

	// When Link fails on O_EXCL with ENOENT and on target collision with EEXIST.
	var entryOut fuse.EntryOut
	exclLinkSt := syncFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: exclOut.NodeId,
	}, "excl-denied.bin", &entryOut)
	collideLinkSt := syncFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: collideOut.NodeId,
	}, "taken.bin", &entryOut)

	// Then both files remain tracked as orphans with zero Promotes, and Release reclaims both.
	requireStatus(t, "O_EXCL Link ENOENT", fuse.ENOENT, exclLinkSt)
	requireStatus(t, "collision Link EEXIST", fuse.Status(syscall.EEXIST), collideLinkSt)
	require.Zero(t, syncFS.Snapshot().TmpFile.Promotes)
	require.Equal(t, 2, syncFS.Snapshot().Queues.OrphanInodes)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: exclOut.NodeId}, Fh: exclOut.Fh})
	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: collideOut.NodeId}, Fh: collideOut.Fh})
	require.Equal(t, int64(2), srv.reclaimOrphanCalls.Load(), "scene=failed_links_reclaimed want=2 got=%d", srv.reclaimOrphanCalls.Load())
	require.False(t, srv.hasNode(exclOut.NodeId))
	require.False(t, srv.hasNode(collideOut.NodeId))
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes)
}

func TestTmpfile_SyncMode_ConcurrentLinkAndRelease_SerializesUnderInodeLock(t *testing.T) {
	// Given an in-flight Link RPC gated inside srv.linkHook while concurrent Release arrives.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	linkEntered := make(chan struct{})
	linkGate, unblockLink := newTestGate()
	defer unblockLink()
	srv.linkHook = func(_ context.Context, _ *storagev1.LinkRequest) error {
		close(linkEntered)
		<-linkGate
		return nil
	}

	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	var tmpOut fuse.CreateOut
	requireStatus(t, "sync Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpOut))
	mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, []byte("race-payload"))

	// When Link holds inodeLocks while Release concurrently attempts to close the last handle.
	linkStatusCh := make(chan fuse.Status, 1)
	go func() {
		var out fuse.EntryOut
		linkStatusCh <- syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "race-promoted.bin", &out)
	}()
	<-linkEntered

	var releaseWG sync.WaitGroup
	releaseWG.Go(func() {
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	})

	require.Eventually(t, func() bool {
		return syncFS.Snapshot().FUSE.Ops[fusefs.OpRelease].Inflight == 1
	}, 2*time.Second, time.Millisecond, "scene=release_waiting_on_inode_lock want=OpRelease_inflight_1")
	require.Zero(t, syncFS.Snapshot().FUSE.Ops[fusefs.OpRelease].Calls, "scene=release_blocked_during_link want=0_completed_releases")
	require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=release_blocked_during_link want=0_reclaim")

	unblockLink()
	requireStatus(t, "concurrent Link", fuse.OK, <-linkStatusCh)
	releaseWG.Wait()

	// Then Release observes the cleared orphan state after Link unlocks and leaves the promoted file intact.
	snap := syncFS.Snapshot()
	require.GreaterOrEqual(t, snap.InodeLocks.ContendedAcquisitions, uint64(1), "scene=inode_lock_contention want>=1")
	require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=post_concurrent_link_release want=0_reclaim got=%d", srv.reclaimOrphanCalls.Load())
	require.True(t, srv.hasNode(tmpOut.NodeId))
	require.Equal(t, uint32(1), srv.nodeNlink(tmpOut.NodeId))
	require.Equal(t, uint64(1), snap.TmpFile.Promotes)
	require.Zero(t, snap.Queues.OrphanInodes)
}

func newClosedCancelChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func awaitBeyondInterruptGrace(ctx context.Context, cancelCh chan struct{}) error {
	close(cancelCh)
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return connect.NewError(connect.CodeCanceled, ctx.Err())
	case <-timer.C:
		return nil
	}
}

func TestSyncMode_Write_GivenPreClosedCancel_CompletesRegularPassthrough(t *testing.T) {
	// Given a sync RawFS regular file and an already-closed FUSE cancel channel.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "cancel-sync.bin", syscall.O_RDWR, 0o644)
	preClosed := newClosedCancelChan()

	// When Write and multi-segment Writev execute with the closed cancel channel.
	nWrite, stWrite := syncFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     6,
	}, []byte("hello-"))
	nWritev, stWritev := syncFS.Writev(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   6,
		Size:     9,
	}, [][]byte{[]byte("sync"), []byte("-mode")})

	// Then both synchronous RPCs complete without returning EINTR.
	requireStatus(t, "sync Write with pre-closed cancel", fuse.OK, stWrite)
	require.Equal(t, uint32(6), nWrite, "scene=sync_write_preclosed_cancel want=6 got=%d", nWrite)
	requireStatus(t, "sync Writev with pre-closed cancel", fuse.OK, stWritev)
	require.Equal(t, uint32(9), nWritev, "scene=sync_writev_preclosed_cancel want=9 got=%d", nWritev)
	require.Equal(t, int64(2), srv.writeCalls.Load(), "scene=sync_write_rpc_calls want=2 got=%d", srv.writeCalls.Load())
	stored, ok := srv.fileBytes(created.NodeId)
	require.True(t, ok)
	requireBytesMatch(t, "sync pre-closed cancel stored bytes", []byte("hello-sync-mode"), stored)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestStagedMode_Write_GivenCancelSignal_StagesWithFreeCapacityThenReturnsEINTROnBackpressure(t *testing.T) {
	// Given a staged RawFS with a 4 MiB stage limit and gated background uploads.
	notifier := newSpanStartNotifier()
	bpEntered := notifier.watch("stage.BackpressureWait")
	tp, _ := newTestTracerProvider(t, notifier)

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	bpGate, releaseBPGate := newTestGate()
	defer releaseBPGate()
	uploadStarted := make(chan struct{})
	var startOnce sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		startOnce.Do(func() { close(uploadStarted) })
		<-bpGate
		return 0, nil
	}

	stagedFS := newTestRawFS(t, srv, withMode(fusefs.MountModeStaged), withStageSize(4194304), withTracerProvider(tp))
	created := mustCreateFile(t, stagedFS, fuse.FUSE_ROOT_ID, "staged-bp.bin", syscall.O_RDWR, 0o644)
	preClosed := newClosedCancelChan()

	// When stage capacity is available, a pre-closed cancel still stages locally. Once 4 MiB fills the stage, backpressure wait returns EINTR on cancel.
	nFree, stFree := stagedFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   0,
		Size:     4096,
	}, buildRepeatingBytes(4096, 'A'))
	requireStatus(t, "staged Write with free capacity and pre-closed cancel", fuse.OK, stFree)
	require.Equal(t, uint32(4096), nFree, "scene=staged_free_capacity_write want=4096 got=%d", nFree)

	mustWriteAt(t, stagedFS, created.NodeId, created.Fh, 4096, buildRepeatingBytes(4190208, 'B'))
	<-uploadStarted

	nPreClosedBP, stPreClosedBP := stagedFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
		Offset:   4194304,
		Size:     4096,
	}, buildRepeatingBytes(4096, 'C'))
	requireStatus(t, "staged backpressure with pre-closed cancel", fuse.EINTR, stPreClosedBP)
	require.Zero(t, nPreClosedBP, "scene=staged_bp_preclosed_written want=0 got=%d", nPreClosedBP)

	inflightCancel := make(chan struct{})
	inflightStCh := make(chan fuse.Status, 1)
	go func() {
		_, st := stagedFS.Write(inflightCancel, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
			Offset:   4194304,
			Size:     4096,
		}, buildRepeatingBytes(4096, 'D'))
		inflightStCh <- st
	}()
	<-bpEntered
	close(inflightCancel)

	// Then backpressure wait honors cancel with EINTR without leaking stage quota.
	requireStatus(t, "staged backpressure with in-flight cancel", fuse.EINTR, waitUntilStatusChan(t, inflightStCh, "staged backpressure EINTR"))
	require.Equal(t, uint64(4194304), stagedFS.Snapshot().Stage.UsedBytes, "scene=staged_bp_used_bytes want=4194304 got=%d", stagedFS.Snapshot().Stage.UsedBytes)

	releaseBPGate()
	requireStatus(t, "staged Fsync after release", fuse.OK, stagedFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: created.NodeId},
		Fh:       created.Fh,
	}))
	stagedFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
}

func TestSyncMode_AppendWrite_GivenInvalidatedCacheUnderClosedCancel_ResolvesTailViaGetAttr(t *testing.T) {
	// Given open O_APPEND handles after SetXAttr invalidates their cached inode entries.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	ino := srv.seedFile(1, "append-cold.log", 0o644, []byte("head-"))
	fencedIno := srv.seedFile(1, "append-fenced.log", 0o644, []byte("old"))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

	opened := mustOpenFile(t, syncFS, ino, syscall.O_WRONLY|syscall.O_APPEND)
	fencedOpen := mustOpenFile(t, syncFS, fencedIno, syscall.O_WRONLY|syscall.O_APPEND)
	requireStatus(t, "SetXAttr invalidate ino", fuse.OK, syncFS.SetXAttr(nil, &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Size:     1,
	}, "user.k", []byte("v")))
	requireStatus(t, "SetXAttr invalidate fencedIno", fuse.OK, syncFS.SetXAttr(nil, &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: fencedIno},
		Size:     1,
	}, "user.k", []byte("v")))

	getAttrBefore := srv.getAttrCalls.Load()
	preClosed := newClosedCancelChan()

	var deadlinePresent bool
	var remainingTimeout time.Duration
	srv.getAttrHook = func(ctx context.Context, _ *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
		deadline, ok := ctx.Deadline()
		deadlinePresent = ok
		remainingTimeout = time.Until(deadline)
		return nil, nil
	}

	// When O_APPEND Write executes with an already-closed cancel channel and an invalidated inode cache.
	written, st := syncFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Fh:       opened.Fh,
		Offset:   0,
		Size:     4,
	}, []byte("tail"))

	// Then getInodeWithContext completes GetAttr with DefaultRPCTimeout and appends at offset 5 without EINTR.
	requireStatus(t, "sync O_APPEND Write with invalidated cache and closed cancel", fuse.OK, st)
	require.Equal(t, uint32(4), written, "scene=sync_append_written want=4 got=%d", written)
	require.Equal(t, getAttrBefore+1, srv.getAttrCalls.Load(), "scene=sync_append_cold_getattr want=%d got=%d", getAttrBefore+1, srv.getAttrCalls.Load())
	require.True(t, deadlinePresent, "scene=sync_append_getattr_deadline want=true")
	require.Greater(t, remainingTimeout, 20*time.Second, "scene=sync_append_getattr_timeout want>20s got=%v", remainingTimeout)
	stored, ok := srv.fileBytes(ino)
	require.True(t, ok)
	requireBytesMatch(t, "sync O_APPEND stored bytes", []byte("head-tail"), stored)

	srv.getAttrHook = func(_ context.Context, _ *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
		return nil, errnoError(connect.CodeFailedPrecondition, "EBUSY")
	}
	nFenced, stFenced := syncFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fencedIno},
		Fh:       fencedOpen.Fh,
		Offset:   0,
		Size:     4,
	}, []byte("deny"))
	requireStatus(t, "sync O_APPEND GetAttr EBUSY not masked as EINTR", fuse.Status(syscall.EBUSY), stFenced)
	require.Zero(t, nFenced, "scene=sync_append_fenced_written want=0 got=%d", nFenced)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: opened.Fh})
	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fencedIno}, Fh: fencedOpen.Fh})
}

func TestSyncMode_LargeWrite_GivenCancelSignal_CommitsFirstChunkThenReturnsShortWrite(t *testing.T) {
	// Given two sync regular files and a 4195328-byte payload spanning two 4 MiB chunks.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	firstFile := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "large-preclosed.bin", syscall.O_RDWR, 0o644)
	secondFile := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "large-midcancel.bin", syscall.O_RDWR, 0o644)
	payload := buildSeededBytes(4195328, 0x3C)
	preClosed := newClosedCancelChan()

	// When Write starts with an already-closed cancel channel at totalWritten == 0.
	nPre, stPre := syncFS.Write(preClosed, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: firstFile.NodeId},
		Fh:       firstFile.Fh,
		Offset:   0,
		Size:     4195328,
	}, payload)

	// Then chunk 1 commits and the loop stops before chunk 2 with a 4 MiB POSIX short write instead of EINTR.
	requireStatus(t, "sync >4MiB Write with pre-closed cancel", fuse.OK, stPre)
	require.Equal(t, uint32(4194304), nPre, "scene=sync_large_preclosed_short_write want=4194304 got=%d", nPre)
	require.Equal(t, int64(1), srv.writeCalls.Load(), "scene=sync_large_preclosed_rpcs want=1 got=%d", srv.writeCalls.Load())
	storedFirst, okFirst := srv.fileBytes(firstFile.NodeId)
	require.True(t, okFirst)
	requireBytesMatch(t, "sync >4MiB pre-closed first chunk bytes", payload[:4194304], storedFirst)

	midCancel := make(chan struct{})
	var closeOnce sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		closeOnce.Do(func() { close(midCancel) })
		return 0, nil
	}
	nMid, stMid := syncFS.Write(midCancel, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: secondFile.NodeId},
		Fh:       secondFile.Fh,
		Offset:   0,
		Size:     4195328,
	}, payload)

	requireStatus(t, "sync >4MiB Write with cancel closed before chunk 2", fuse.OK, stMid)
	require.Equal(t, uint32(4194304), nMid, "scene=sync_large_midcancel_short_write want=4194304 got=%d", nMid)
	require.Equal(t, int64(2), srv.writeCalls.Load(), "scene=sync_large_midcancel_rpcs want=2 got=%d", srv.writeCalls.Load())
	storedSecond, okSecond := srv.fileBytes(secondFile.NodeId)
	require.True(t, okSecond)
	requireBytesMatch(t, "sync >4MiB mid-cancel first chunk bytes", payload[:4194304], storedSecond)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: firstFile.NodeId}, Fh: firstFile.Fh})
	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: secondFile.NodeId}, Fh: secondFile.Fh})
}

func TestTmpfile_SyncMode_GivenClosedOrMidFlightCancel_CompletesCreateTmpfileWithServerErrorMapping(t *testing.T) {
	// Given a sync RawFS and both pre-closed and mid-flight cancel channels.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	preClosed := newClosedCancelChan()

	var deadlinePresent bool
	var remainingTimeout time.Duration
	srv.createTmpfileHook = func(ctx context.Context, _ *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		deadline, ok := ctx.Deadline()
		deadlinePresent = ok
		remainingTimeout = time.Until(deadline)
		return nil, nil
	}

	// When Tmpfile runs with a pre-closed cancel channel and with a cancel channel closed for 25ms mid-flight.
	var preOut fuse.CreateOut
	stPre := syncFS.Tmpfile(preClosed, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &preOut)

	midCancel := make(chan struct{})
	srv.createTmpfileHook = func(ctx context.Context, _ *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		return nil, awaitBeyondInterruptGrace(ctx, midCancel)
	}
	var midOut fuse.CreateOut
	stMid := syncFS.Tmpfile(midCancel, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &midOut)

	// Then both CreateTmpfile RPCs succeed with DefaultRPCTimeout. ToStatus maps ENOSPC and EBUSY directly instead of returning EINTR.
	requireStatus(t, "sync Tmpfile with pre-closed cancel", fuse.OK, stPre)
	requireStatus(t, "sync Tmpfile with >15ms mid-flight cancel", fuse.OK, stMid)
	require.True(t, deadlinePresent, "scene=tmpfile_rpc_deadline want=true")
	require.Greater(t, remainingTimeout, 20*time.Second, "scene=tmpfile_rpc_timeout_low want>20s got=%v", remainingTimeout)
	require.LessOrEqual(t, remainingTimeout, fusefs.DefaultRPCTimeout, "scene=tmpfile_rpc_timeout_high want<=30s got=%v", remainingTimeout)
	require.Equal(t, 2, syncFS.Snapshot().Queues.OrphanInodes, "scene=tmpfile_orphans want=2 got=%d", syncFS.Snapshot().Queues.OrphanInodes)

	srv.createTmpfileHook = func(_ context.Context, _ *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		return nil, errnoError(connect.CodeResourceExhausted, "ENOSPC")
	}
	var quotaOut fuse.CreateOut
	stQuota := syncFS.Tmpfile(preClosed, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &quotaOut)
	requireStatus(t, "sync Tmpfile ENOSPC under closed cancel", fuse.Status(syscall.ENOSPC), stQuota)

	srv.createTmpfileHook = func(_ context.Context, _ *storagev1.CreateTmpfileRequest) (*storagev1.CreateTmpfileResponse, error) {
		return nil, errnoError(connect.CodeFailedPrecondition, "EBUSY")
	}
	var fenceOut fuse.CreateOut
	stFence := syncFS.Tmpfile(preClosed, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o600,
	}, &fenceOut)
	requireStatus(t, "sync Tmpfile EBUSY under closed cancel", fuse.Status(syscall.EBUSY), stFence)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: preOut.NodeId}, Fh: preOut.Fh})
	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: midOut.NodeId}, Fh: midOut.Fh})
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes, "scene=tmpfile_orphans_reclaimed want=0")
}

func TestTmpfile_SyncMode_Link_GivenClosedOrMidFlightCancel_PromotesOrphanWithServerErrorMapping(t *testing.T) {
	// Given four linkable sync tmpfiles with pre-closed and mid-flight cancel channels.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
	preClosed := newClosedCancelChan()

	var tmpPre, tmpMid, tmpErr, tmpFence fuse.CreateOut
	requireStatus(t, "Tmpfile 1", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpPre))
	requireStatus(t, "Tmpfile 2", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpMid))
	requireStatus(t, "Tmpfile 3", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpErr))
	requireStatus(t, "Tmpfile 4", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, &tmpFence))
	require.Equal(t, 4, syncFS.Snapshot().Queues.OrphanInodes, "scene=initial_orphans want=4")

	var deadlinePresent bool
	var remainingTimeout time.Duration
	srv.linkHook = func(ctx context.Context, _ *storagev1.LinkRequest) error {
		deadline, ok := ctx.Deadline()
		deadlinePresent = ok
		remainingTimeout = time.Until(deadline)
		return nil
	}

	// When Link promotes tmpPre with a pre-closed cancel and tmpMid with a cancel closed for 25ms mid-flight.
	var linkPreOut fuse.EntryOut
	stPre := syncFS.Link(preClosed, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpPre.NodeId,
	}, "promoted-pre.bin", &linkPreOut)

	midCancel := make(chan struct{})
	srv.linkHook = func(ctx context.Context, _ *storagev1.LinkRequest) error {
		return awaitBeyondInterruptGrace(ctx, midCancel)
	}
	var linkMidOut fuse.EntryOut
	stMid := syncFS.Link(midCancel, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpMid.NodeId,
	}, "promoted-mid.bin", &linkMidOut)

	// Then both Link RPCs succeed with DefaultRPCTimeout, clear orphan tracking, and increment Promotes. ToStatus preserves ENOSPC and EBUSY error mapping.
	requireStatus(t, "sync Link with pre-closed cancel", fuse.OK, stPre)
	requireStatus(t, "sync Link with >15ms mid-flight cancel", fuse.OK, stMid)
	require.True(t, deadlinePresent, "scene=link_rpc_deadline want=true")
	require.Greater(t, remainingTimeout, 20*time.Second, "scene=link_rpc_timeout_low want>20s got=%v", remainingTimeout)
	require.LessOrEqual(t, remainingTimeout, fusefs.DefaultRPCTimeout, "scene=link_rpc_timeout_high want<=30s got=%v", remainingTimeout)
	require.Equal(t, uint64(2), syncFS.Snapshot().TmpFile.Promotes, "scene=link_promotes want=2 got=%d", syncFS.Snapshot().TmpFile.Promotes)
	require.Equal(t, 2, syncFS.Snapshot().Queues.OrphanInodes, "scene=remaining_orphans want=2 got=%d", syncFS.Snapshot().Queues.OrphanInodes)

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpPre.NodeId}, Fh: tmpPre.Fh})
	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpMid.NodeId}, Fh: tmpMid.Fh})
	require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=promoted_releases_no_reclaim want=0 got=%d", srv.reclaimOrphanCalls.Load())
	require.True(t, srv.hasNode(tmpPre.NodeId))
	require.True(t, srv.hasNode(tmpMid.NodeId))

	// Unfenced ENOSPC failure preserves orphan tracking and Release reclaims it via ReclaimOrphan.
	srv.linkHook = func(_ context.Context, _ *storagev1.LinkRequest) error {
		return errnoError(connect.CodeResourceExhausted, "ENOSPC")
	}
	var quotaOut fuse.EntryOut
	stQuota := syncFS.Link(preClosed, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpErr.NodeId,
	}, "quota-fail.bin", &quotaOut)
	requireStatus(t, "sync Link ENOSPC under closed cancel", fuse.Status(syscall.ENOSPC), stQuota)
	require.Equal(t, uint64(2), syncFS.Snapshot().TmpFile.Promotes, "scene=failed_link_promotes_unchanged want=2")
	require.Equal(t, 2, syncFS.Snapshot().Queues.OrphanInodes, "scene=failed_link_orphan_preserved want=2")

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpErr.NodeId}, Fh: tmpErr.Fh})
	require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=unpromoted_tmpfile_reclaimed want=1 got=%d", srv.reclaimOrphanCalls.Load())
	require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes, "scene=one_orphan_remaining want=1")

	// Stale-epoch EBUSY failure maps to EBUSY, fences syncFS, and fenced Release clears local orphan without sending ReclaimOrphan.
	srv.linkHook = func(_ context.Context, _ *storagev1.LinkRequest) error {
		return errnoError(connect.CodeFailedPrecondition, "EBUSY")
	}
	var fenceOut fuse.EntryOut
	stFence := syncFS.Link(preClosed, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: tmpFence.NodeId,
	}, "fence-fail.bin", &fenceOut)
	requireStatus(t, "sync Link EBUSY under closed cancel", fuse.Status(syscall.EBUSY), stFence)
	require.True(t, syncFS.IsFenced(), "scene=stale_epoch_link_fences_fs want=true")
	require.Equal(t, uint64(2), syncFS.Snapshot().TmpFile.Promotes, "scene=fenced_link_promotes_unchanged want=2")
	require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes, "scene=fenced_link_orphan_preserved want=1")

	syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpFence.NodeId}, Fh: tmpFence.Fh})
	require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=fenced_release_skips_reclaim want=1 got=%d", srv.reclaimOrphanCalls.Load())
	require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes, "scene=all_orphans_cleared want=0")
}
