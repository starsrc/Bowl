package fusefs_test

import (
	"context"
	"slices"
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
	mu      sync.Mutex
	allow   bool
	tokens  int
	waiters []chan struct{}
}

func newStepGate() *stepGate {
	return &stepGate{}
}

func (g *stepGate) allowOne() {
	g.mu.Lock()
	if g.allow {
		g.mu.Unlock()
		return
	}
	if len(g.waiters) > 0 {
		w := g.waiters[0]
		g.waiters[0] = nil
		g.waiters = g.waiters[1:]
		g.mu.Unlock()
		close(w)
		return
	}
	g.tokens++
	g.mu.Unlock()
}

func (g *stepGate) allowAll() {
	g.mu.Lock()
	if g.allow {
		g.mu.Unlock()
		return
	}
	g.allow = true
	waiters := g.waiters
	g.waiters = nil
	g.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

func (g *stepGate) wait(ctx context.Context) error {
	g.mu.Lock()
	if g.allow {
		g.mu.Unlock()
		return nil
	}
	if g.tokens > 0 {
		g.tokens--
		g.mu.Unlock()
		return nil
	}
	w := make(chan struct{})
	g.waiters = append(g.waiters, w)
	g.mu.Unlock()
	select {
	case <-w:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		if idx := slices.Index(g.waiters, w); idx >= 0 {
			g.waiters = slices.Delete(g.waiters, idx, idx+1)
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}

func TestRawFS_UnmountProgress_GivenStagedChunksAndSingleRootFsyncDir_AdvancesMonotonicallyToTotal(t *testing.T) {

	srv := newFakeStorage(t, withRootIno(17), withTTL(1.0, 1.0))

	var gateWrites atomic.Bool
	writeEnteredCh := make(chan uint64, 4)
	writeGate := newStepGate()

	var gateFsyncDir atomic.Bool
	gateFsyncDir.Store(true)
	fsyncDirEnteredCh := make(chan uint64, 2)
	fsyncDirGate := newStepGate()

	srv.writeHook = func(ctx context.Context, req *storagev1.WriteRequest) (uint32, error) {
		if !gateWrites.Load() {
			return 0, nil
		}
		writeEnteredCh <- req.GetOffset()
		return 0, writeGate.wait(ctx)
	}
	srv.fsyncDirHook = func(ctx context.Context, req *storagev1.FsyncDirRequest) error {
		if !gateFsyncDir.Load() {
			return nil
		}
		fsyncDirEnteredCh <- req.GetIno()
		return fsyncDirGate.wait(ctx)
	}

	fs := newTestRawFS(t, srv, withDirectIO(false), withStageSize(16*1024*1024))
	t.Cleanup(func() {
		writeGate.allowAll()
		fsyncDirGate.allowAll()
	})

	fileA := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty-only.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, fileA.NodeId, fileA.Fh, 0, []byte("file-a-initial"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileA.NodeId}, Fh: fileA.Fh})
	require.Eventually(t, func() bool {
		snap := fs.Snapshot()
		return srv.writeCalls.Load() >= 1 && snap.Stage.PendingChunks == 0 && snap.Stage.UsedBytes == 0
	}, 2*time.Second, 5*time.Millisecond)

	gateWrites.Store(true)
	fileB := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "staged-two.bin", syscall.O_RDWR, 0o644)
	fullChunk := buildRepeatingBytes(fusefs.StageChunkSize, 'B')
	mustWriteAt(t, fs, fileB.NodeId, fileB.Fh, 0, fullChunk)
	mustWriteAt(t, fs, fileB.NodeId, fileB.Fh, fusefs.StageChunkSize, []byte("tail"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileB.NodeId}, Fh: fileB.Fh})
	<-writeEnteredCh

	before := fs.UnmountProgress()
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, before)

	unmountDone := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	var pStart fusefs.UnmountProgress
	require.Eventually(t, func() bool {
		pStart = fs.UnmountProgress()
		return pStart.Active
	}, 2*time.Second, 5*time.Millisecond, "OnUnmount must enter active state")
	require.Equal(t, uint64(3), pStart.Total)
	require.Equal(t, uint64(0), pStart.Completed)

	writeGate.allowOne()
	<-writeEnteredCh
	require.Eventually(t, func() bool {
		return fs.UnmountProgress().Completed == 1
	}, 2*time.Second, 5*time.Millisecond, "Completed must advance to 1 after first chunk upload")
	require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 1, Total: 3}, fs.UnmountProgress())

	writeGate.allowAll()

	var gotRootIno uint64
	select {
	case gotRootIno = <-fsyncDirEnteredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for single root FsyncDir RPC")
	}
	require.Equal(t, uint64(17), gotRootIno)
	require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 2, Total: 3}, fs.UnmountProgress())

	fsyncDirGate.allowAll()
	select {
	case <-unmountDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OnUnmount to complete")
	}

	require.Zero(t, srv.fsyncCalls.Load())
	require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 3, Total: 3}, fs.UnmountProgress())

	fs.OnUnmount()
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 3, Total: 3}, fs.UnmountProgress())
}

func TestRawFS_UnmountProgress_GivenPendingStageChunk_CountsSinglePhase2Barrier(t *testing.T) {

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
		return srv.writeCalls.Load() >= 1 && snap.Stage.PendingChunks == 0 && snap.Stage.UsedBytes == 0
	}, 2*time.Second, 5*time.Millisecond)

	gateWrites.Store(true)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 4096, []byte("second"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	<-writeEnteredCh

	doneCh := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(doneCh)
	}()

	var p fusefs.UnmountProgress
	require.Eventually(t, func() bool {
		p = fs.UnmountProgress()
		return p.Active
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, uint64(2), p.Total)

	writeGate.allowAll()
	<-doneCh
	require.Zero(t, srv.fsyncCalls.Load())
	require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 2, Total: 2}, fs.UnmountProgress())
}

func TestRawFS_UnmountProgress_GivenSyncAndCleanMounts_ReportsExpectedCounts(t *testing.T) {
	t.Run("SyncModeTracksSingleRootFsyncDir", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fsyncDirEntered := make(chan struct{}, 2)
		fsyncDirGate := newStepGate()
		srv.fsyncDirHook = func(ctx context.Context, _ *storagev1.FsyncDirRequest) error {
			fsyncDirEntered <- struct{}{}
			return fsyncDirGate.wait(ctx)
		}
		fs := newTestRawFS(t, srv, withMode(fusefs.MountModeSync), withDirectIO(false))
		t.Cleanup(fsyncDirGate.allowAll)

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "sync.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("sync-payload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

		doneCh := make(chan struct{})
		go func() {
			fs.OnUnmount()
			close(doneCh)
		}()

		<-fsyncDirEntered
		require.Equal(t, fusefs.UnmountProgress{Active: true, Completed: 0, Total: 1}, fs.UnmountProgress())
		fsyncDirGate.allowAll()
		<-doneCh
		require.Zero(t, srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load())
		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 1, Total: 1}, fs.UnmountProgress())
	})

	t.Run("CleanMountAndNilReceiverReportZero", func(t *testing.T) {

		var nilFS *fusefs.RawFS
		require.Equal(t, fusefs.UnmountProgress{}, nilFS.UnmountProgress())

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		fs.OnUnmount()
		fs.OnUnmount()

		require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 0, Total: 0}, fs.UnmountProgress())
	})
}
