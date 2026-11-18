package fusefs_test

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type stepGate struct {
	mu    sync.Mutex
	allow bool
	ch    chan struct{}
}

func newStepGate() *stepGate {
	return &stepGate{ch: make(chan struct{})}
}

func (g *stepGate) allowOne() {
	g.mu.Lock()
	ch := g.ch
	g.ch = make(chan struct{})
	g.mu.Unlock()
	close(ch)
}

func (g *stepGate) allowAll() {
	g.mu.Lock()
	if !g.allow {
		g.allow = true
		close(g.ch)
	}
	g.mu.Unlock()
}

func (g *stepGate) wait(ctx context.Context) error {
	g.mu.Lock()
	if g.allow {
		g.mu.Unlock()
		return nil
	}
	ch := g.ch
	g.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRawFS_UnmountProgress_GivenStagedChunksAndDirtyInodes_AdvancesMonotonicallyToTotal(t *testing.T) {
	// Given
	srv := newFakeStorage(t, withTTL(1.0, 1.0))

	var gateWrites atomic.Bool
	writeEnteredCh := make(chan uint64, 4)
	writeGate := newStepGate()

	var gateFsyncs atomic.Bool
	gateFsyncs.Store(true)
	fsyncEnteredCh := make(chan uint64, 4)
	fsyncGate := newStepGate()

	srv.writeHook = func(ctx context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if !gateWrites.Load() {
			return 0, nil
		}
		writeEnteredCh <- req.GetOffset()
		return 0, writeGate.wait(ctx)
	}
	srv.fsyncHook = func(ctx context.Context, req *storagev1.FsyncRequest) error {
		if !gateFsyncs.Load() {
			return nil
		}
		fsyncEnteredCh <- req.GetIno()
		return fsyncGate.wait(ctx)
	}

	fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
	t.Cleanup(func() {
		writeGate.allowAll()
		fsyncGate.allowAll()
	})

	// File A uploads immediately so it enters dirtyInodes with 0 pending chunks.
	fileA := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty-only.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, fileA.NodeId, fileA.Fh, 0, []byte("file-a-initial"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileA.NodeId}, Fh: fileA.Fh})
	require.Eventually(t, func() bool {
		snap := fs.Snapshot()
		return srv.writeCalls.Load() >= 1 && snap.Stage.PendingChunks == 0 && snap.Stage.UsedBytes == 0 && snap.Queues.DirtyInodes == 1
	}, 2*time.Second, 5*time.Millisecond)

	// Block uploads before writing File B so 2 chunks stay pending during OnUnmount.
	gateWrites.Store(true)
	fileB := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "staged-two.bin", syscall.O_RDWR, 0o644)
	fullChunk := buildRepeatingBytes(fusefs.StageChunkSize, 'B')
	mustWriteAt(t, fs, fileB.NodeId, fileB.Fh, 0, fullChunk)
	mustWriteAt(t, fs, fileB.NodeId, fileB.Fh, fusefs.StageChunkSize, []byte("tail"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileB.NodeId}, Fh: fileB.Fh})
	<-writeEnteredCh

	before := fs.UnmountProgress()
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, before)

	// When
	unmountDone := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	// Then: OnUnmount freezes Total = 2 staged chunks + 2 union inodes = 4.
	var pStart fusefs.UnmountProgress
	require.Eventually(t, func() bool {
		pStart = fs.UnmountProgress()
		return pStart.Active
	}, 2*time.Second, 5*time.Millisecond, "OnUnmount must enter active state")
	require.Equal(t, uint64(4), pStart.Total)
	require.Equal(t, uint64(0), pStart.Completed)

	// Release first staged chunk and hold second chunk in writeHook.
	writeGate.allowOne()
	<-writeEnteredCh
	require.Eventually(t, func() bool {
		return fs.UnmountProgress().Completed == 1
	}, 2*time.Second, 5*time.Millisecond, "Completed must advance to 1 after first chunk upload")
	require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 1, Total: 4}, fs.UnmountProgress())

	writeGate.allowAll()

	// Wait for first Fsync RPC: Phase 1 stage uploads are done so Completed == 2.
	select {
	case <-fsyncEnteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first Fsync RPC")
	}
	require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 2, Total: 4}, fs.UnmountProgress())

	fsyncGate.allowOne()

	// Wait for second Fsync RPC: Completed advances to 3.
	select {
	case <-fsyncEnteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second Fsync RPC")
	}
	require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 3, Total: 4}, fs.UnmountProgress())

	fsyncGate.allowAll()
	select {
	case <-unmountDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OnUnmount to complete")
	}

	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 4, Total: 4}, fs.UnmountProgress())

	// Repeat OnUnmount must preserve completed progress instead of resetting to 0/0.
	fs.OnUnmount()
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 4, Total: 4}, fs.UnmountProgress())
}

func TestRawFS_UnmountProgress_GivenOverlappingDirtyAndStagedInode_DeduplicatesUnion(t *testing.T) {
	// Given: File X uploads an initial write into dirtyInodes, then stages 1 blocked chunk on the same inode.
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var gateWrites atomic.Bool
	writeEnteredCh := make(chan struct{}, 2)
	writeGate := newStepGate()
	srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		if !gateWrites.Load() {
			return 0, nil
		}
		writeEnteredCh <- struct{}{}
		return 0, writeGate.wait(ctx)
	}

	fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
	t.Cleanup(writeGate.allowAll)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "overlap.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("first"))
	fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	require.Eventually(t, func() bool {
		snap := fs.Snapshot()
		return snap.Stage.PendingChunks == 0 && snap.Queues.DirtyInodes == 1
	}, 2*time.Second, 5*time.Millisecond)

	gateWrites.Store(true)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 4096, []byte("second"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	<-writeEnteredCh

	// When
	doneCh := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(doneCh)
	}()

	// Then: Total = 1 staged chunk + 1 deduplicated inode = 2.
	var p fusefs.UnmountProgress
	require.Eventually(t, func() bool {
		p = fs.UnmountProgress()
		return p.Active
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, uint64(2), p.Total)

	writeGate.allowAll()
	<-doneCh
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 2, Total: 2}, fs.UnmountProgress())
}

func TestRawFS_UnmountProgress_GivenSyncAndCleanMounts_ReportsExpectedCounts(t *testing.T) {
	t.Run("SyncModeTracksDirtyFsyncs", func(t *testing.T) {
		// Given
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fsyncEntered := make(chan struct{}, 2)
		fsyncGate := newStepGate()
		srv.fsyncHook = func(ctx context.Context, _ *storagev1.FsyncRequest) error {
			fsyncEntered <- struct{}{}
			return fsyncGate.wait(ctx)
		}
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		t.Cleanup(fsyncGate.allowAll)

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "sync.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("sync-payload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		// When
		doneCh := make(chan struct{})
		go func() {
			fs.OnUnmount()
			close(doneCh)
		}()

		// Then
		<-fsyncEntered
		require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 0, Total: 1}, fs.UnmountProgress())
		fsyncGate.allowAll()
		<-doneCh
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("CleanMountAndNilReceiverReportZero", func(t *testing.T) {
		// Given
		var nilFS *fusefs.RawFS
		require.Equal(t, fusefs.UnmountProgress{}, nilFS.UnmountProgress())

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		// When
		fs.OnUnmount()
		fs.OnUnmount()

		// Then
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})
}
