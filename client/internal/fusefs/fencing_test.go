package fusefs_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestRawFS_FenceState_IsStickyIdempotentAndNotifiesObservers(t *testing.T) {

	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	var cbCalls atomic.Int32
	var cbMu sync.Mutex
	var cbStatus fuse.Status
	var cbReason string

	fs := newTestRawFS(t, srv, func(cfg *fusefs.Config) {
		cfg.OnFence = func(status fuse.Status, reason string) {
			cbCalls.Add(1)
			cbMu.Lock()
			cbStatus = status
			cbReason = reason
			cbMu.Unlock()
		}
	})

	require.False(t, fs.IsFenced())
	require.Equal(t, fuse.OK, fs.FenceStatus())
	require.Empty(t, fs.FenceReason())
	select {
	case <-fs.FencedCh():
		t.Fatal("FencedCh must remain open before Fence")
	default:
	}

	fs.Fence(fuse.EBUSY, "writer epoch 1 superseded by 2")
	fs.Fence(fuse.ENOENT, "disk deleted later")

	require.True(t, fs.IsFenced())
	require.Equal(t, fuse.EBUSY, fs.FenceStatus())
	require.Equal(t, "writer epoch 1 superseded by 2", fs.FenceReason())
	select {
	case <-fs.FencedCh():
	default:
		t.Fatal("FencedCh must be closed after Fence")
	}
	require.Equal(t, int32(1), cbCalls.Load(), "OnFence must fire exactly once")
	cbMu.Lock()
	require.Equal(t, fuse.EBUSY, cbStatus)
	require.Equal(t, "writer epoch 1 superseded by 2", cbReason)
	cbMu.Unlock()

	var lateStatus fuse.Status
	var lateReason string
	fs.SetOnFence(func(status fuse.Status, reason string) {
		lateStatus = status
		lateReason = reason
	})
	require.Equal(t, fuse.EBUSY, lateStatus)
	require.Equal(t, "writer epoch 1 superseded by 2", lateReason)
}

func TestRawFS_CheckWritableAndOpen_FailClosedWithZeroRPCsWhenFenced(t *testing.T) {
	t.Run("RW mount fenced with EBUSY rejects mutations and writable Open locally", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "doc.txt", 0o644, []byte("hello"))
		fs := newTestRawFS(t, srv)

		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "doc.txt")
		var rwOpen fuse.OpenOut
		requireStatus(t, "Open RW pre-fence", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Flags:    syscall.O_RDWR,
		}, &rwOpen))

		fs.Fence(fuse.EBUSY, "preempted by newer writer")

		beforeGetAttr := srv.getAttrCalls.Load()
		beforeSetAttr := srv.setAttrCalls.Load()
		beforeWrite := srv.writeCalls.Load()
		beforeCreate := srv.createCalls.Load()
		beforeMkdir := srv.mkdirCalls.Load()
		beforeFsync := srv.fsyncCalls.Load()
		beforeFsyncDir := srv.fsyncDirCalls.Load()

		require.Equal(t, fuse.EBUSY, fs.CheckWritable())

		for _, flags := range []uint32{
			syscall.O_WRONLY,
			syscall.O_RDWR,
			syscall.O_RDONLY | syscall.O_TRUNC,
			syscall.O_WRONLY | syscall.O_TRUNC,
		} {
			var out fuse.OpenOut
			st := fs.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: ino},
				Flags:    flags,
			}, &out)
			requireStatus(t, "fenced writable/trunc Open", fuse.EBUSY, st)
		}

		var createOut fuse.CreateOut
		requireStatus(t, "Create", fuse.EBUSY, fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR | syscall.O_CREAT,
			Mode:     0o644,
		}, "new.txt", &createOut))

		var mkdirOut fuse.EntryOut
		requireStatus(t, "Mkdir", fuse.EBUSY, fs.Mkdir(nil, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0o755,
		}, "sub", &mkdirOut))

		_, writeSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       rwOpen.Fh,
			Offset:   0,
		}, []byte("blocked"))
		requireStatus(t, "Write", fuse.EBUSY, writeSt)

		var attrOut fuse.AttrOut
		requireStatus(t, "SetAttr", fuse.EBUSY, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_SIZE,
				Size:     0,
			},
		}, &attrOut))

		requireStatus(t, "Fsync", fuse.EBUSY, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       rwOpen.Fh,
		}))

		var dh fuse.OpenOut
		requireStatus(t, "OpenDir", fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		}, &dh))
		requireStatus(t, "FsyncDir", fuse.EBUSY, fs.FsyncDir(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Fh:       dh.Fh,
		}))

		require.Equal(t, beforeGetAttr+1, srv.getAttrCalls.Load(), "only OpenDir should touch GetAttr; fenced Open/mutations must send 0 GetAttr")
		require.Equal(t, beforeSetAttr, srv.setAttrCalls.Load())
		require.Equal(t, beforeWrite, srv.writeCalls.Load())
		require.Equal(t, beforeCreate, srv.createCalls.Load())
		require.Equal(t, beforeMkdir, srv.mkdirCalls.Load())
		require.Equal(t, beforeFsync, srv.fsyncCalls.Load())
		require.Equal(t, beforeFsyncDir, srv.fsyncDirCalls.Load())
	})

	t.Run("RO mount fenced with ENOENT on disk delete returns ENOENT instead of EROFS", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "ro.txt", 0o644, []byte("ro"))
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeRO))

		require.Equal(t, fuse.Status(syscall.EROFS), fs.CheckWritable())

		fs.Fence(fuse.ENOENT, "disk not found")

		require.Equal(t, fuse.ENOENT, fs.CheckWritable())
		var openOut fuse.OpenOut
		requireStatus(t, "RO fenced Open(O_WRONLY)", fuse.ENOENT, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Flags:    syscall.O_WRONLY,
		}, &openOut))
		require.Zero(t, srv.getAttrCalls.Load())
	})
}

func staleEpochRPCError() error {
	return errnoError(connect.CodeFailedPrecondition, "EBUSY")
}

func customErrorInfoRPCError(code connect.Code, domain, reason string) error {
	ce := connect.NewError(code, errors.New(reason))
	if detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	}); err == nil {
		ce.AddDetail(detail)
	}
	return ce
}

func TestRawFS_StaleEpochRPC_TriggersImmediateFenceOnSyncAndAsyncPaths(t *testing.T) {
	t.Run("synchronous RPC ErrStaleEpoch immediately fences RawFS and blocks follow-up mutations", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		ino := srv.seedFile(1, "sync.txt", 0o644, []byte("abc"))
		var fenced atomic.Int32
		fs := newTestRawFS(t, srv, func(cfg *fusefs.Config) {
			cfg.OnFence = func(status fuse.Status, _ string) {
				if status == fuse.EBUSY {
					fenced.Add(1)
				}
			}
		})
		srv.setAttrHook = func(context.Context, *storagev1.SetAttrRequest) error {
			return staleEpochRPCError()
		}

		var out fuse.AttrOut
		st := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_SIZE,
				Size:     0,
			},
		}, &out)

		requireStatus(t, "SetAttr ErrStaleEpoch", fuse.EBUSY, st)
		require.True(t, fs.IsFenced())
		require.Equal(t, fuse.EBUSY, fs.FenceStatus())
		require.Equal(t, int32(1), fenced.Load())
		select {
		case <-fs.FencedCh():
		default:
			t.Fatal("FencedCh must be closed on ErrStaleEpoch")
		}

		before := srv.setAttrCalls.Load()
		st2 := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: ino},
				Valid:    fuse.FATTR_SIZE,
				Size:     0,
			},
		}, &out)
		requireStatus(t, "follow-up SetAttr after fence", fuse.EBUSY, st2)
		require.Equal(t, before, srv.setAttrCalls.Load())
	})

	t.Run("non-stale-epoch errors do not trigger fencing", func(t *testing.T) {
		cases := []struct {
			name string
			err  error
		}{
			{
				name: "Unavailable with bowl.storage EBUSY",
				err:  customErrorInfoRPCError(connect.CodeUnavailable, "bowl.storage", "EBUSY"),
			},
			{
				name: "FailedPrecondition with wrong domain",
				err:  customErrorInfoRPCError(connect.CodeFailedPrecondition, "other.domain", "EBUSY"),
			},
			{
				name: "FailedPrecondition without ErrorInfo",
				err:  connect.NewError(connect.CodeFailedPrecondition, errors.New("precondition failed")),
			},
			{
				name: "NotFound ENOENT",
				err:  errnoError(connect.CodeNotFound, "ENOENT"),
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				srv := newFakeStorage(t, withTTL(2.0, 2.0))
				ino := srv.seedFile(1, "keep.txt", 0o644, []byte("abc"))
				fs := newTestRawFS(t, srv)
				srv.setAttrHook = func(context.Context, *storagev1.SetAttrRequest) error {
					return tc.err
				}

				var out fuse.AttrOut
				_ = fs.SetAttr(nil, &fuse.SetAttrIn{
					SetAttrInCommon: fuse.SetAttrInCommon{
						InHeader: fuse.InHeader{NodeId: ino},
						Valid:    fuse.FATTR_SIZE,
						Size:     0,
					},
				}, &out)

				require.False(t, fs.IsFenced(), "error %q must not falsely fence RawFS", tc.name)
			})
		}
	})

	t.Run("StageCoordinator background Write RPC ErrStaleEpoch immediately fences RawFS", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		var fenced atomic.Int32
		fs := newTestRawFS(t, srv, withStageSize(8*1024*1024), func(cfg *fusefs.Config) {
			cfg.OnFence = func(status fuse.Status, _ string) {
				if status == fuse.EBUSY {
					fenced.Add(1)
				}
			}
		})
		created := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "async.bin", syscall.O_RDWR, 0o644)

		srv.writeHook = func(context.Context, *storagev1.WriteRequest) (uint32, error) {
			return 0, staleEpochRPCError()
		}

		mustWriteAt(t, fs, created.NodeId, created.Fh, 0, []byte("async-data"))
		_ = fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: created.NodeId},
			Fh:       created.Fh,
		})

		select {
		case <-fs.FencedCh():
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for StageCoordinator Write ErrStaleEpoch to fence RawFS")
		}
		require.True(t, fs.IsFenced())
		require.Equal(t, fuse.EBUSY, fs.FenceStatus())
		require.Equal(t, int32(1), fenced.Load())
	})
}

func TestStageCoordinator_AbortAll_DrainsQueueUnlinksFilesRecordsStickyErrAndWakesWaiters(t *testing.T) {

	stageDir := t.TempDir()
	const stageSize = 2 * fusefs.StageChunkSize

	firstEntered := make(chan struct{}, 1)
	cleanupDone := make(chan struct{})
	var writeAttempts atomic.Int32
	writer := func(ctx context.Context, _, _ uint64, _ []byte) (uint32, *storagev1.Inode, error) {
		attempt := writeAttempts.Add(1)
		if attempt == 1 {
			select {
			case firstEntered <- struct{}{}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-cleanupDone:
			return 0, nil, syscall.ECANCELED
		}
	}

	coord := fusefs.NewStageCoordinator(stageDir, stageSize, writer, fusefs.StageHooks{})
	t.Cleanup(func() {
		close(cleanupDone)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		coord.Clear(ctx, 0)
	})

	const (
		fhA     = uint64(10)
		inoA    = uint64(100)
		inoB    = uint64(200)
		fhB     = uint64(20)
		chunkSz = fusefs.StageChunkSize
	)
	coord.RegisterHandle(fhA)
	coord.RegisterHandle(fhB)

	w1, handled1, err1 := coord.WriteForHandle(context.Background(), nil, fhA, inoA, inoA, 0, buildRepeatingBytes(chunkSz, '1'))
	require.NoError(t, err1)
	require.True(t, handled1)
	require.Equal(t, uint32(chunkSz), w1)

	select {
	case <-firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first chunk did not enter upload worker")
	}

	w2, handled2, err2 := coord.WriteForHandle(context.Background(), nil, fhA, inoA, inoA, chunkSz, buildRepeatingBytes(2<<20, '2'))
	require.NoError(t, err2)
	require.True(t, handled2)
	require.Equal(t, uint32(2<<20), w2)
	coord.SealInode(inoA)

	w3, handled3, err3 := coord.WriteForHandle(context.Background(), nil, fhA, inoA, inoA, 8<<20, buildRepeatingBytes(2<<20, '3'))
	require.NoError(t, err3)
	require.True(t, handled3)
	require.Equal(t, uint32(2<<20), w3)

	require.GreaterOrEqual(t, len(listStageChunkFiles(t, stageDir)), 3)

	barrierInodeDone := make(chan struct{})
	go func() {
		_ = coord.BarrierInode(context.Background(), inoA)
		close(barrierInodeDone)
	}()

	barrierAllDone := make(chan struct{})
	go func() {
		_ = coord.BarrierAll(context.Background())
		close(barrierAllDone)
	}()

	bpErrCh := make(chan error, 1)
	go func() {
		_, _, bpErr := coord.WriteForHandle(context.Background(), nil, fhB, inoB, inoB, 0, buildRepeatingBytes(1<<20, '4'))
		bpErrCh <- bpErr
	}()

	time.Sleep(30 * time.Millisecond)

	coord.AbortAll(syscall.EBUSY)

	select {
	case <-barrierInodeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("BarrierInode did not wake after AbortAll")
	}
	select {
	case <-barrierAllDone:
	case <-time.After(2 * time.Second):
		t.Fatal("BarrierAll did not wake after AbortAll")
	}
	select {
	case bpErr := <-bpErrCh:
		require.ErrorIs(t, bpErr, syscall.EBUSY)
	case <-time.After(2 * time.Second):
		t.Fatal("backpressure WriteForHandle did not wake after AbortAll")
	}

	_, newHandled, newErr := coord.WriteForHandle(context.Background(), nil, fhA, inoA, inoA, 0, []byte("rejected"))
	require.True(t, newHandled)
	require.ErrorIs(t, newErr, syscall.EBUSY)

	require.Equal(t, int32(1), writeAttempts.Load())

	require.Eventually(t, func() bool {
		return len(listStageChunkFiles(t, stageDir)) == 0
	}, 2*time.Second, 10*time.Millisecond, "all local stage chunk files must be unlinked after AbortAll")

	require.ErrorIs(t, coord.FlushHandle(fhA, inoA), syscall.EBUSY)
	require.ErrorIs(t, coord.FlushInodeForHandle(context.Background(), fhA, inoA), syscall.EBUSY)
	require.ErrorIs(t, coord.FlushAndResetInodeForHandle(context.Background(), fhB, inoB), syscall.EBUSY)

	require.False(t, coord.HasPending(inoA))
	require.False(t, coord.HasPending(inoB))

	attr := fuse.Attr{Size: 12}
	coord.OverlayAttr(inoA, &attr)
	require.Equal(t, uint64(12), attr.Size, "OverlayAttr must not overlay aborted staged size")

	protoInode := &storagev1.Inode{Ino: inoA, Attr: &storagev1.InodeAttribute{Size: 12}}
	coord.OverlayInode(inoA, protoInode)
	require.Equal(t, uint64(12), protoInode.GetAttr().GetSize(), "OverlayInode must not overlay aborted staged size")

	buf := make([]byte, 64)
	_, readHandled, readErr := coord.Read(context.Background(), inoA, 0, buf, func(context.Context, uint64, uint32) (rpc.ReadPayload, error) {
		return rpc.ReadPayload{}, nil
	})
	require.NoError(t, readErr)
	require.False(t, readHandled, "StageCoordinator.Read must not serve aborted staged bytes")
}

func TestRawFS_FenceAbortsStagedWritesAndPreventsStaleReadsAndUploads(t *testing.T) {

	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	ino := srv.seedFile(1, "staged.txt", 0o644, []byte("server-v1"))

	stageRoot := t.TempDir()
	session, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)

	fs := newTestRawFS(t, srv, withStageSession(session), withStageSize(8*1024*1024))
	_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "staged.txt")

	_, _ = fs.GetXAttr(nil, &fuse.InHeader{NodeId: ino}, "security.capability", nil)

	var openOut fuse.OpenOut
	requireStatus(t, "Open RW", fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Flags:    syscall.O_RDWR,
	}, &openOut))

	mustWriteAt(t, fs, ino, openOut.Fh, 0, []byte("local-dirty-uncommitted-payload"))
	require.Zero(t, srv.writeCalls.Load(), "write should be buffered in local stage")
	require.NotEmpty(t, listStageChunkFiles(t, session.InstanceDir()))

	srv.mu.Lock()
	srv.nodes[ino].data = []byte("server-v2-from-new-writer")
	srv.nodes[ino].inode.Attr.Size = uint64(len(srv.nodes[ino].data))
	srv.mu.Unlock()

	fs.Fence(fuse.EBUSY, "writer epoch preempted")

	require.Zero(t, srv.writeCalls.Load(), "aborted staged chunk must not be uploaded")
	require.Empty(t, listStageChunkFiles(t, session.InstanceDir()))

	requireStatus(t, "Flush after fence", fuse.EBUSY, fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
	}))
	requireStatus(t, "Fsync after fence", fuse.EBUSY, fs.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: ino},
		Fh:       openOut.Fh,
	}))

	got := mustReadAt(t, fs, ino, openOut.Fh, 0, 64)
	require.Equal(t, "server-v2-from-new-writer", string(got))

	attr := mustGetAttr(t, fs, ino)
	require.Equal(t, uint64(len("server-v2-from-new-writer")), attr.Size)
}

func TestRawFS_FenceClearsAndBypassesReadAheadAndKnownInodes(t *testing.T) {
	t.Run("preempted mount clears and bypasses knownInodes and ReadAheadCoordinator on all read ops", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(60.0, 60.0))
		initialData := buildRepeatingBytes(512*1024, 'A')
		fileIno := srv.seedFile(1, "data.bin", 0o644, initialData)
		linkIno := srv.seedSymlink(1, "sym", "old-target")
		fs := newTestRawFS(t, srv)

		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "data.bin")
		_ = mustGetAttr(t, fs, fileIno)
		_, _ = readAllDirEntries(t, fs, fuse.FUSE_ROOT_ID, true, 64*1024)
		targetBefore, rlSt := fs.Readlink(nil, &fuse.InHeader{NodeId: linkIno})
		requireStatus(t, "Readlink warm", fuse.OK, rlSt)
		require.Equal(t, "old-target", string(targetBefore))

		var roOpen fuse.OpenOut
		requireStatus(t, "Open RO warm", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    syscall.O_RDONLY,
		}, &roOpen))
		_ = mustReadAt(t, fs, fileIno, roOpen.Fh, 0, 128*1024)
		_ = mustReadAt(t, fs, fileIno, roOpen.Fh, 128*1024, 128*1024)

		readsBeforeHit := srv.readCalls.Load()
		gotWarm := mustReadAt(t, fs, fileIno, roOpen.Fh, 256*1024, 64*1024)
		require.Equal(t, byte('A'), gotWarm[0])
		require.Equal(t, readsBeforeHit, srv.readCalls.Load(), "pre-fence sequential read must hit ReadAhead cache")

		updatedData := buildRepeatingBytes(512*1024, 'Z')
		srv.mu.Lock()
		srv.nodes[fileIno].data = updatedData
		srv.nodes[fileIno].inode.Attr.Size = uint64(len(updatedData))
		srv.nodes[linkIno].link = []byte("new-target")
		srv.mu.Unlock()
		_ = srv.seedFile(1, "created-by-new-writer.txt", 0o644, []byte("fresh"))

		fs.Fence(fuse.EBUSY, "writer epoch superseded")

		lookupsBefore := srv.lookupCalls.Load()
		e1 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "created-by-new-writer.txt")
		e2 := mustLookup(t, fs, fuse.FUSE_ROOT_ID, "created-by-new-writer.txt")
		require.Equal(t, lookupsBefore+2, srv.lookupCalls.Load(), "fenced Lookup must bypass cache on every call")
		require.Equal(t, e1.NodeId, e2.NodeId)
		require.Zero(t, e1.EntryValid)
		require.Zero(t, e1.EntryValidNsec)
		require.Zero(t, e1.AttrValid)
		require.Zero(t, e1.AttrValidNsec)

		getAttrsBefore := srv.getAttrCalls.Load()
		a1 := mustGetAttr(t, fs, fileIno)
		a2 := mustGetAttr(t, fs, fileIno)
		require.Equal(t, getAttrsBefore+2, srv.getAttrCalls.Load(), "fenced GetAttr must bypass cache on every call")
		require.Equal(t, uint64(len(updatedData)), a1.Size)
		require.Equal(t, uint64(len(updatedData)), a2.Size)
		require.Zero(t, a1.AttrValid)
		require.Zero(t, a1.AttrValidNsec)

		var roOpen2 fuse.OpenOut
		getAttrsBefore = srv.getAttrCalls.Load()
		requireStatus(t, "Open RO fenced", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    syscall.O_RDONLY,
		}, &roOpen2))
		require.Equal(t, getAttrsBefore+1, srv.getAttrCalls.Load())
		require.Zero(t, roOpen2.OpenFlags&fuse.FOPEN_KEEP_CACHE, "fenced Open(O_RDONLY) must not set FOPEN_KEEP_CACHE")

		readsBefore := srv.readCalls.Load()
		got1 := mustReadAt(t, fs, fileIno, roOpen.Fh, 320*1024, 64*1024)
		got2 := mustReadAt(t, fs, fileIno, roOpen.Fh, 384*1024, 64*1024)
		require.Equal(t, byte('Z'), got1[0], "fenced Read must serve fresh server bytes, not stale ReadAhead window")
		require.Equal(t, byte('Z'), got2[0])
		require.Equal(t, readsBefore+2, srv.readCalls.Load(), "fenced Read must pass through directly to server on every call")

		readDirsBefore := srv.readDirPlusCalls.Load()
		_, _ = readAllDirEntries(t, fs, fuse.FUSE_ROOT_ID, true, 64*1024)
		_, _ = readAllDirEntries(t, fs, fuse.FUSE_ROOT_ID, true, 64*1024)
		require.GreaterOrEqual(t, srv.readDirPlusCalls.Load(), readDirsBefore+2, "fenced ReadDirPlus must bypass cache on every call")

		readlinksBefore := srv.readlinkCalls.Load()
		rl1, st1 := fs.Readlink(nil, &fuse.InHeader{NodeId: linkIno})
		rl2, st2 := fs.Readlink(nil, &fuse.InHeader{NodeId: linkIno})
		requireStatus(t, "fenced Readlink 1", fuse.OK, st1)
		requireStatus(t, "fenced Readlink 2", fuse.OK, st2)
		require.Equal(t, "new-target", string(rl1))
		require.Equal(t, "new-target", string(rl2))
		require.Equal(t, readlinksBefore+2, srv.readlinkCalls.Load(), "fenced Readlink must bypass cache on every call")
	})

	t.Run("deleted disk fenced with ENOENT passes read ops through to server ENOENT", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(60.0, 60.0))
		fileIno := srv.seedFile(1, "gone.txt", 0o644, []byte("before-delete"))
		fs := newTestRawFS(t, srv)
		_ = mustLookup(t, fs, fuse.FUSE_ROOT_ID, "gone.txt")
		var roOpen fuse.OpenOut
		requireStatus(t, "Open RO pre-delete", fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    syscall.O_RDONLY,
		}, &roOpen))

		notFoundErr := errnoError(connect.CodeNotFound, "ENOENT")
		srv.lookupHook = func(context.Context, *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return nil, notFoundErr
		}
		srv.getAttrHook = func(context.Context, *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			return nil, notFoundErr
		}
		srv.readHook = func(context.Context, *storagev1.ReadRequest) ([]byte, error) {
			return nil, notFoundErr
		}

		fs.Fence(fuse.ENOENT, "disk deleted")

		var entryOut fuse.EntryOut
		requireStatus(t, "Lookup on deleted disk", fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "gone.txt", &entryOut))
		var attrOut fuse.AttrOut
		requireStatus(t, "GetAttr on deleted disk", fuse.ENOENT, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileIno}}, &attrOut))
		_, readSt := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: roOpen.Fh, Size: 16}, make([]byte, 16))
		requireStatus(t, "Read on deleted disk", fuse.ENOENT, readSt)
	})
}

func TestReadAheadCoordinator_InvalidateAll_DropsCachedWindows(t *testing.T) {

	const fileSize = 512 * 1024
	ra := fusefs.NewReadAheadCoordinator()
	var fetchCalls atomic.Int32
	payloadByte := atomic.Uint32{}
	payloadByte.Store('X')
	fetch := func(_ context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		fetchCalls.Add(1)
		if offset >= fileSize {
			return rpc.ReadPayload{}, nil
		}
		n := min(int(length), int(fileSize-offset))
		b := byte(payloadByte.Load())
		return rpc.ReadPayload{Data: buildRepeatingBytes(n, b)}, nil
	}

	buf := make([]byte, 128*1024)
	_, err := ra.Read(context.Background(), 1, 100, 0, buf, fetch)
	require.NoError(t, err)
	_, err = ra.Read(context.Background(), 1, 100, 128*1024, buf, fetch)
	require.NoError(t, err)
	callsBeforeHit := fetchCalls.Load()
	_, err = ra.Read(context.Background(), 1, 100, 256*1024, buf, fetch)
	require.NoError(t, err)
	require.Equal(t, callsBeforeHit, fetchCalls.Load(), "window should be cached before InvalidateAll")

	payloadByte.Store('Y')
	ra.InvalidateAll()

	n, err := ra.Read(context.Background(), 1, 100, 384*1024, buf, fetch)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('Y'), buf[0])
	require.Greater(t, fetchCalls.Load(), callsBeforeHit)
}

func TestRawFS_OnUnmount_WhenFenced_SkipsAllServerRPCsAndCleansLocalState(t *testing.T) {
	t.Run("skips stage upload, dirty Fsync, FsyncDir, and releaseQueue ReclaimOrphan RPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		srv.reclaimOrphanHook = func(context.Context, *storagev1.ReclaimOrphanRequest) error {
			return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
		}

		stageRoot := t.TempDir()
		session, err := fusefs.OpenStageDirSession(stageRoot)
		require.NoError(t, err)
		instDir := session.InstanceDir()

		fs := newTestRawFS(t, srv, withStageSession(session), withStageSize(8*1024*1024))

		dirtyFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, dirtyFile.NodeId, dirtyFile.Fh, 0, []byte("committed-dirty"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirtyFile.NodeId}, Fh: dirtyFile.Fh})
		require.Eventually(t, func() bool {
			return srv.writeCalls.Load() >= 1 && fs.Snapshot().Stage.PendingChunks == 0 && fs.Snapshot().Stage.UsedBytes == 0
		}, 2*time.Second, 5*time.Millisecond)

		orphanFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orphan.bin", syscall.O_RDWR, 0o644)
		requireStatus(t, "Unlink open orphan", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orphan.bin"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: orphanFile.NodeId}, Fh: orphanFile.Fh})
		require.Eventually(t, func() bool {
			return fs.Snapshot().Queues.ReleaseQueueDepth >= 1
		}, 2*time.Second, 5*time.Millisecond)

		stagedFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "staged.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, stagedFile.NodeId, stagedFile.Fh, 0, []byte("never-upload-me"))

		var tmpOut fuse.CreateOut
		requireStatus(t, "Tmpfile", fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR,
			Mode:     0o600,
		}, &tmpOut))

		fs.Fence(fuse.EBUSY, "lease preempted")
		writesBefore := srv.writeCalls.Load()
		fsyncsBefore := srv.fsyncCalls.Load()
		fsyncDirsBefore := srv.fsyncDirCalls.Load()
		reclaimsBefore := srv.reclaimOrphanCalls.Load()

		fs.OnUnmount()

		require.Equal(t, writesBefore, srv.writeCalls.Load(), "fenced OnUnmount must skip stage Write RPCs")
		require.Equal(t, fsyncsBefore, srv.fsyncCalls.Load(), "fenced OnUnmount must skip dirty Fsync RPCs")
		require.Equal(t, fsyncDirsBefore, srv.fsyncDirCalls.Load(), "fenced OnUnmount must skip root FsyncDir RPC")
		require.Equal(t, reclaimsBefore, srv.reclaimOrphanCalls.Load(), "fenced OnUnmount must skip releaseQueue ReclaimOrphan RPCs")

		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())

		snap := fs.Snapshot()
		require.Zero(t, snap.Handles.OpenFiles)
		require.Zero(t, snap.TmpFile.ActiveFiles)
		require.Zero(t, snap.Queues.OrphanInodes)
		require.Zero(t, snap.Queues.ReleaseQueueDepth)
		_, statErr := os.Stat(instDir)
		require.True(t, os.IsNotExist(statErr), "StageSession instance dir must be removed on fenced OnUnmount")
	})

	t.Run("skips root FsyncDir RPC when only metadata is dirty", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(2.0, 2.0))
		fs := newTestRawFS(t, srv)
		_ = mustMkdir(t, fs, fuse.FUSE_ROOT_ID, "meta-only", 0o755)
		fs.Fence(fuse.ENOENT, "disk deleted")
		fsyncDirsBefore := srv.fsyncDirCalls.Load()

		fs.OnUnmount()

		require.Equal(t, fsyncDirsBefore, srv.fsyncDirCalls.Load(), "fenced OnUnmount must skip root FsyncDir RPC")
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})
}

func TestRawFS_FencedReleaseOfUnlinkedOrphan_SkipsReclaimOrphanRPC(t *testing.T) {

	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	fs := newTestRawFS(t, srv)
	orphanFile := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "open-orphan.bin", syscall.O_RDWR, 0o644)
	requireStatus(t, "Unlink open orphan", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "open-orphan.bin"))

	fs.Fence(fuse.EBUSY, "lease preempted")
	reclaimsBefore := srv.reclaimOrphanCalls.Load()

	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: orphanFile.NodeId},
		Fh:       orphanFile.Fh,
	})

	require.Equal(t, reclaimsBefore, srv.reclaimOrphanCalls.Load(), "fenced Release must skip ReclaimOrphan RPC")
}

func TestStageCoordinator_DiscardInodeAfterAbortAll_DoesNotRaceWithInflightChunkUpload(t *testing.T) {

	payload := buildRepeatingBytes(fusefs.StageChunkSize, 'P')

	srv := newFakeStorage(t, withTTL(2.0, 2.0))
	releaseWrites := make(chan struct{})
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		<-releaseWrites
		return 0, connect.NewError(connect.CodeUnavailable, errors.New("aborted"))
	}

	session, err := fusefs.OpenStageDirSession(t.TempDir())
	require.NoError(t, err)
	fs := newTestRawFS(t, srv, withStageSession(session), withStageSize(16*1024*1024))

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "staged-orphan", syscall.O_RDWR, 0o600)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, payload)
	requireStatus(t, "Unlink staged open orphan", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "staged-orphan"))

	require.Eventually(t, func() bool {
		return fs.Snapshot().Stage.Inflight >= 1
	}, 2*time.Second, time.Millisecond, "staged chunk upload did not become inflight")

	fs.Fence(fuse.EBUSY, "preempted during staged chunk upload")
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: f.NodeId},
		Fh:       f.Fh,
	})
	close(releaseWrites)

	fs.OnUnmount()
}

func TestStep20_RawFS_MountLeaseReadinessAndRefresh(t *testing.T) {
	var (
		mu  sync.Mutex
		now = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	fs := fusefs.NewRawFS(nil, fusefs.Config{
		DiskID:          "disk-step20-lease",
		RootIno:         1,
		MountID:         "01950000-0000-7000-9000-000000000020",
		LeaseTTLSeconds: 30,
		NowFn:           clock,
	})
	t.Cleanup(fs.OnUnmount)

	require.NoError(t, fs.CheckReady())

	advance(29 * time.Second)
	require.NoError(t, fs.CheckReady())

	advance(time.Second)
	err := fs.CheckReady()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mount lease expired")

	fs.RefreshMountLease(30)
	require.NoError(t, fs.CheckReady())

	fs.Fence(fuse.EBUSY, "writer epoch superseded")
	err = fs.CheckReady()
	require.Error(t, err)
	require.Contains(t, err.Error(), "writer epoch superseded")

	fs.RefreshMountLease(60)
	err = fs.CheckReady()
	require.Error(t, err)
	require.Contains(t, err.Error(), "writer epoch superseded")

	leaseFree := fusefs.NewRawFS(nil, fusefs.Config{
		DiskID:  "disk-step20-lease-free",
		RootIno: 1,
		NowFn:   clock,
	})
	t.Cleanup(leaseFree.OnUnmount)
	advance(time.Hour)
	require.NoError(t, leaseFree.CheckReady())

	var nilFS *fusefs.RawFS
	require.NoError(t, nilFS.CheckReady())
	nilFS.RefreshMountLease(30)
}
