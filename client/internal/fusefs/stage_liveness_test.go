package fusefs_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestOpenStageDirSession_CleanupPreservesLiveInitDirAndRemovesDeadInitDir(t *testing.T) {
	stageRoot := filepath.Join(t.TempDir(), "stage-init-cleanup")
	require.NoError(t, os.MkdirAll(stageRoot, 0o755))

	liveInitDir := filepath.Join(stageRoot, fmt.Sprintf(".init-%d-live", os.Getpid()))
	require.NoError(t, os.MkdirAll(liveInitDir, 0o700))

	deadInitDir := filepath.Join(stageRoot, ".init-999996-dead")
	require.NoError(t, os.MkdirAll(deadInitDir, 0o700))

	malformedInitDir := filepath.Join(stageRoot, ".init-badpid-xxxx")
	require.NoError(t, os.MkdirAll(malformedInitDir, 0o700))

	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	require.Equal(t, 2, sess.CleanedCount(), "dead and malformed .init-* dirs must be cleaned: want=2, got=%d", sess.CleanedCount())
	require.DirExists(t, liveInitDir, "live .init-* dir must not be removed")
	require.NoDirExists(t, deadInitDir, "dead PID .init-* dir must be removed")
	require.NoDirExists(t, malformedInitDir, "malformed .init-* dir must be removed")
	require.DirExists(t, sess.InstanceDir(), "opened session instance dir must exist")
}

func TestOpenStageDirSession_ConcurrentStartup_NeverDeletesSiblingInstanceDir(t *testing.T) {
	stageRoot := filepath.Join(t.TempDir(), "stage-concurrent-init")
	const workers = 16

	start := make(chan struct{})
	sessions := make([]*fusefs.StageDirSession, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			sessions[idx], errs[idx] = fusefs.OpenStageDirSession(stageRoot)
		}(i)
	}
	close(start)
	wg.Wait()

	t.Cleanup(func() {
		for _, s := range sessions {
			if s != nil {
				_ = s.Close()
			}
		}
	})

	for i := range workers {
		require.NoError(t, errs[i], "worker %d OpenStageDirSession failed", i)
		require.NotNil(t, sessions[i])
		require.DirExists(t, sessions[i].InstanceDir(), "worker %d instance dir missing after concurrent startup", i)
		require.FileExists(t, filepath.Join(sessions[i].InstanceDir(), ".lock"), "worker %d lock file missing", i)
	}

	entries, err := os.ReadDir(stageRoot)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, strings.HasPrefix(entry.Name(), ".init-"), "leftover .init-* dir found: %s", entry.Name())
	}
}

func TestStageCoordinator_PrependAndOverwriteSealPriorActiveChunk(t *testing.T) {
	t.Run("prepend seals prior active chunk instead of rewriting in place", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		uploadEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		srv.writeHook = makeGateOffsetWriteHook(3, uploadEntered, gate)

		stageRoot := filepath.Join(t.TempDir(), "prepend-seal-stage")
		sess, err := fusefs.OpenStageDirSession(stageRoot)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sess.Close() })

		fs := newTestRawFS(t, srv, withDirectIO(false), withStageSession(sess))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "prepend.bin", syscall.O_RDWR, 0o644)

		mustWriteAt(t, fs, f.NodeId, f.Fh, 3, []byte("lo-go"))
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("hel"))
		select {
		case <-uploadEntered:
		case <-time.After(300 * time.Millisecond):
			t.Fatal("prepend did not seal prior active chunk for upload")
		}

		chunkFiles := listStageChunkFiles(t, sess.InstanceDir())
		require.Len(t, chunkFiles, 2, "prepend must seal prior active chunk and create a new chunk file: want=2, got=%d", len(chunkFiles))

		releaseGate()
		requireStatus(t, "Fsync prepend", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
		stored, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "prepend final server bytes", []byte("hello-go"), stored)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("overlapping overwrite seals prior active chunk instead of mutating in place", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		uploadEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		srv.writeHook = makeGateOffsetWriteHook(0, uploadEntered, gate)

		stageRoot := filepath.Join(t.TempDir(), "overwrite-seal-stage")
		sess, err := fusefs.OpenStageDirSession(stageRoot)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sess.Close() })

		fs := newTestRawFS(t, srv, withDirectIO(false), withStageSession(sess))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "overwrite.bin", syscall.O_RDWR, 0o644)

		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("hello-go"))
		mustWriteAt(t, fs, f.NodeId, f.Fh, 2, []byte("WORLD"))
		select {
		case <-uploadEntered:
		case <-time.After(300 * time.Millisecond):
			t.Fatal("overlapping overwrite did not seal prior active chunk for upload")
		}

		chunkFiles := listStageChunkFiles(t, sess.InstanceDir())
		require.Len(t, chunkFiles, 2, "overlapping overwrite must seal prior active chunk: want=2, got=%d", len(chunkFiles))

		releaseGate()
		requireStatus(t, "Fsync overwrite", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh}))
		stored, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "overwrite final server bytes", []byte("heWORLDo"), stored)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})
}

func TestRawFS_StageBarriers_CancelInterruptsWithEINTRWithoutIssuingServerRPCAndPreservesBackgroundRetry(t *testing.T) {
	runBarrierCase := func(t *testing.T, name string, invoke func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, fh uint64) fuse.Status, serverCalls func(srv *fakeStorage) int64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			srv := newFakeStorage(t, withTTL(1.0, 1.0))
			var outage atomic.Bool
			outage.Store(true)
			uploadAttempted := make(chan struct{})
			var attemptOnce sync.Once
			srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
				attemptOnce.Do(func() { close(uploadAttempted) })
				if outage.Load() {
					return 0, connect.NewError(connect.CodeUnavailable, errors.New("transient outage"))
				}
				return 0, nil
			}
			fs := newTestRawFS(t, srv, withDirectIO(false))
			t.Cleanup(func() {
				outage.Store(false)
				fs.OnUnmount()
			})

			f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "outage-"+name+".bin", syscall.O_RDWR, 0o644)
			mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("staged-payload"))

			cancelCh := make(chan struct{})
			statusCh := make(chan fuse.Status, 1)
			go func() {
				statusCh <- invoke(fs, cancelCh, f.NodeId, f.Fh)
			}()

			select {
			case <-uploadAttempted:
			case <-time.After(2 * time.Second):
				t.Fatal("background upload did not attempt Write RPC")
			}

			close(cancelCh)

			select {
			case gotSt := <-statusCh:
				requireStatus(t, name+" canceled during stage barrier", fuse.EINTR, gotSt)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("%s hung in stage barrier after cancel closed", name)
			}

			require.Zero(t, serverCalls(srv), "%s must not issue dependent server RPC after barrier cancel", name)

			outage.Store(false)
			require.Eventually(t, func() bool {
				data, ok := srv.fileBytes(f.NodeId)
				return ok && string(data) == "staged-payload"
			}, 3*time.Second, 10*time.Millisecond, "background upload retry must self-heal after foreground cancel")

			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		})
	}

	runBarrierCase(t, "Fsync", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, fh uint64) fuse.Status {
		return fs.Fsync(cancel, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh})
	}, func(srv *fakeStorage) int64 { return srv.fsyncCalls.Load() })

	runBarrierCase(t, "StatFs", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		var out fuse.StatfsOut
		return fs.StatFs(cancel, &fuse.InHeader{NodeId: nodeID}, &out)
	}, func(srv *fakeStorage) int64 { return srv.statFSCalls.Load() })

	runBarrierCase(t, "SetAttr", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		var out fuse.AttrOut
		return fs.SetAttr(cancel, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     0o600,
			},
		}, &out)
	}, func(srv *fakeStorage) int64 { return srv.setAttrCalls.Load() })

	runBarrierCase(t, "Fallocate", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, fh uint64) fuse.Status {
		return fs.Fallocate(cancel, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
	}, func(srv *fakeStorage) int64 { return srv.fallocateCalls.Load() })

	runBarrierCase(t, "Link", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		var out fuse.EntryOut
		return fs.Link(cancel, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: nodeID,
		}, "linked.bin", &out)
	}, func(srv *fakeStorage) int64 { return srv.linkCalls.Load() })

	runBarrierCase(t, "Rename", func(fs *fusefs.RawFS, cancel <-chan struct{}, _, _ uint64) fuse.Status {
		return fs.Rename(cancel, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "outage-Rename.bin", "renamed.bin")
	}, func(srv *fakeStorage) int64 { return srv.renameCalls.Load() })

	runBarrierCase(t, "SetXAttr", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		return fs.SetXAttr(cancel, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: nodeID}}, "user.k", []byte("v"))
	}, func(srv *fakeStorage) int64 { return srv.setXattrCalls.Load() })

	runBarrierCase(t, "GetXAttr", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		_, st := fs.GetXAttr(cancel, &fuse.InHeader{NodeId: nodeID}, "user.k", make([]byte, 16))
		return st
	}, func(srv *fakeStorage) int64 { return srv.getXattrCalls.Load() })

	runBarrierCase(t, "ListXAttr", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		_, st := fs.ListXAttr(cancel, &fuse.InHeader{NodeId: nodeID}, make([]byte, 64))
		return st
	}, func(srv *fakeStorage) int64 { return srv.listXattrCalls.Load() })

	runBarrierCase(t, "RemoveXAttr", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		return fs.RemoveXAttr(cancel, &fuse.InHeader{NodeId: nodeID}, "user.k")
	}, func(srv *fakeStorage) int64 { return srv.removeXattrCalls.Load() })

	runBarrierCase(t, "IoctlGetFlags", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		var out fuse.IoctlOut
		return fs.Ioctl(cancel, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Cmd:      fusefs.FS_IOC_GETFLAGS,
		}, nil, &out, make([]byte, 8))
	}, func(srv *fakeStorage) int64 { return srv.getAttrCalls.Load() })

	runBarrierCase(t, "IoctlSetFlags", func(fs *fusefs.RawFS, cancel <-chan struct{}, nodeID, _ uint64) fuse.Status {
		var out fuse.IoctlOut
		inbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(inbuf, uint64(fusefs.FS_APPEND_FL))
		return fs.Ioctl(cancel, &fuse.IoctlIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Cmd:      fusefs.FS_IOC_SETFLAGS,
		}, inbuf, &out, nil)
	}, func(srv *fakeStorage) int64 { return srv.setAttrCalls.Load() })

	t.Run("WriteSyncFallback", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		var outage atomic.Bool
		outage.Store(true)
		uploadAttempted := make(chan struct{})
		var attemptOnce sync.Once
		var syncWriteSeen atomic.Bool
		srv.writeHook = func(_ context.Context, req *storagev1.WriteRequest) (uint32, error) {
			if string(req.GetData()) == "sync" {
				syncWriteSeen.Store(true)
			}
			attemptOnce.Do(func() { close(uploadAttempted) })
			if outage.Load() {
				return 0, connect.NewError(connect.CodeUnavailable, errors.New("transient outage"))
			}
			return 0, nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		t.Cleanup(func() {
			outage.Store(false)
			fs.OnUnmount()
		})

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "outage-WriteSyncFallback.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("staged-payload"))

		syncHandle := mustOpenFile(t, fs, f.NodeId, syscall.O_RDWR|syscall.O_SYNC)
		defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: syncHandle.Fh})

		cancelCh := make(chan struct{})
		statusCh := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(cancelCh, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       syncHandle.Fh,
				Offset:   14,
				Size:     4,
			}, []byte("sync"))
			statusCh <- st
		}()

		select {
		case <-uploadAttempted:
		case <-time.After(2 * time.Second):
			t.Fatal("background upload did not attempt Write RPC")
		}

		close(cancelCh)

		select {
		case gotSt := <-statusCh:
			requireStatus(t, "WriteSyncFallback canceled during stage barrier", fuse.EINTR, gotSt)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("WriteSyncFallback hung in stage barrier after cancel closed")
		}

		require.False(t, syncWriteSeen.Load(), "WriteSyncFallback must not issue dependent direct Write RPC after barrier cancel")

		outage.Store(false)
		require.Eventually(t, func() bool {
			data, ok := srv.fileBytes(f.NodeId)
			return ok && string(data) == "staged-payload"
		}, 3*time.Second, 10*time.Millisecond, "background upload retry must self-heal after foreground cancel")

		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})
}

func TestRawFS_StageBarrier_CancelWithin15msGrace_PreservesOK(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	cancelCh := make(chan struct{})
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		close(cancelCh)
		return 0, nil
	}
	fs := newTestRawFS(t, srv, withDirectIO(false))
	t.Cleanup(fs.OnUnmount)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "grace-barrier.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("fast-upload"))

	st := fs.Fsync(cancelCh, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	requireStatus(t, "Fsync with cancel closing within 15ms grace", fuse.OK, st)
	require.Equal(t, int64(1), srv.fsyncCalls.Load())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
}

func TestRawFS_StageBarrier_CancelReleasesLifecycleRLockForOnUnmount(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var outage atomic.Bool
	outage.Store(true)
	uploadAttempted := make(chan struct{})
	var attemptOnce sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		attemptOnce.Do(func() { close(uploadAttempted) })
		if outage.Load() {
			return 0, connect.NewError(connect.CodeUnavailable, errors.New("transient outage"))
		}
		return 0, nil
	}
	fs := newTestRawFS(t, srv, withDirectIO(false))
	t.Cleanup(func() { outage.Store(false) })

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "unmount-rlock.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("payload"))

	cancelCh := make(chan struct{})
	fsyncCh := make(chan fuse.Status, 1)
	go func() {
		fsyncCh <- fs.Fsync(cancelCh, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	}()
	<-uploadAttempted

	unmountDone := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	close(cancelCh)
	requireStatus(t, "Fsync cancel", fuse.EINTR, waitUntilStatusChan(t, fsyncCh, "Fsync EINTR"))

	outage.Store(false)
	select {
	case <-unmountDone:
	case <-time.After(3 * time.Second):
		t.Fatal("OnUnmount did not complete after Fsync released lifecycleMu.RLock")
	}
}

func TestRawFS_OpenTrunc_ServerSetAttrFailure_PreservesStagedData(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
		return errnoError(connect.CodePermissionDenied, "EACCES")
	}
	fs := newTestRawFS(t, srv, withDirectIO(false))
	t.Cleanup(fs.OnUnmount)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "keep-on-err.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("important-staged-data"))

	var out fuse.OpenOut
	st := fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: f.NodeId},
		Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
	}, &out)

	requireStatus(t, "Open(O_TRUNC) rejected by server", fuse.EACCES, st)
	stored, ok := srv.fileBytes(f.NodeId)
	require.True(t, ok)
	requireBytesMatch(t, "staged data must survive rejected Open(O_TRUNC)", []byte("important-staged-data"), stored)
	got := mustReadAt(t, fs, f.NodeId, f.Fh, 0, 32)
	requireBytesMatch(t, "read after rejected Open(O_TRUNC)", []byte("important-staged-data"), got)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
}

func TestRawFS_OpenTrunc_CancelDuringBarrier_ReturnsEINTRWithoutCallingSetAttr(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var outage atomic.Bool
	outage.Store(true)
	uploadAttempted := make(chan struct{})
	var attemptOnce sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		attemptOnce.Do(func() { close(uploadAttempted) })
		if outage.Load() {
			return 0, connect.NewError(connect.CodeUnavailable, errors.New("transient outage"))
		}
		return 0, nil
	}
	fs := newTestRawFS(t, srv, withDirectIO(false))
	t.Cleanup(func() {
		outage.Store(false)
		fs.OnUnmount()
	})

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "trunc-cancel.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("must-not-discard"))

	cancelCh := make(chan struct{})
	stCh := make(chan fuse.Status, 1)
	go func() {
		var out fuse.OpenOut
		stCh <- fs.Open(cancelCh, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
		}, &out)
	}()

	select {
	case <-uploadAttempted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Open(O_TRUNC) did not trigger stage barrier upload")
	}

	close(cancelCh)
	requireStatus(t, "Open(O_TRUNC) canceled during barrier", fuse.EINTR, waitUntilStatusChan(t, stCh, "Open(O_TRUNC) EINTR"))
	require.Zero(t, srv.setAttrCalls.Load(), "canceled Open(O_TRUNC) must not issue SetAttr(size=0)")

	outage.Store(false)
	require.Eventually(t, func() bool {
		data, ok := srv.fileBytes(f.NodeId)
		return ok && string(data) == "must-not-discard"
	}, 3*time.Second, 10*time.Millisecond, "staged data must recover after canceled Open(O_TRUNC)")
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
}

func withUnmountTimeouts(total, stage, stall, fsync time.Duration) rawFSOption {
	return func(cfg *fusefs.Config) {
		cfg.UnmountTimeout = total
		cfg.UnmountStageTimeout = stage
		cfg.UnmountStageStallTimeout = stall
		cfg.UnmountFsyncTimeout = fsync
	}
}

func TestRawFS_Config_NormalizesDefaultAndClampedUnmountTimeouts(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))

	fsDefault := newTestRawFS(t, srv)
	t.Cleanup(fsDefault.OnUnmount)
	cfgDefault := fsDefault.Config()
	require.Equal(t, fusefs.DefaultUnmountTimeout, cfgDefault.UnmountTimeout)
	require.Equal(t, fusefs.DefaultUnmountStageTimeout, cfgDefault.UnmountStageTimeout)
	require.Equal(t, fusefs.DefaultUnmountStageStallTimeout, cfgDefault.UnmountStageStallTimeout)
	require.Equal(t, fusefs.DefaultUnmountFsyncTimeout, cfgDefault.UnmountFsyncTimeout)

	fsLowerTotal := newTestRawFS(t, srv, withUnmountTimeouts(20*time.Second, 0, 0, 0))
	t.Cleanup(fsLowerTotal.OnUnmount)
	cfgLowerTotal := fsLowerTotal.Config()
	require.Equal(t, 20*time.Second, cfgLowerTotal.UnmountTimeout)
	require.Equal(t, 20*time.Second, cfgLowerTotal.UnmountStageTimeout)
	require.Equal(t, 20*time.Second, cfgLowerTotal.UnmountStageStallTimeout)
	require.Equal(t, 20*time.Second, cfgLowerTotal.UnmountFsyncTimeout)

	fsLowerStage := newTestRawFS(t, srv, withUnmountTimeouts(time.Minute, 10*time.Second, 0, 0))
	t.Cleanup(fsLowerStage.OnUnmount)
	cfgLowerStage := fsLowerStage.Config()
	require.Equal(t, time.Minute, cfgLowerStage.UnmountTimeout)
	require.Equal(t, 10*time.Second, cfgLowerStage.UnmountStageTimeout)
	require.Equal(t, 10*time.Second, cfgLowerStage.UnmountStageStallTimeout)
	require.Equal(t, 45*time.Second, cfgLowerStage.UnmountFsyncTimeout)
}

func TestRawFS_OnUnmount_StageStallTimeout_CancelsInflightUploadLogsDroppedAndCleansStageFiles(t *testing.T) {
	var logBuf syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var canceledInflight atomic.Bool
	srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		<-ctx.Done()
		canceledInflight.Store(true)
		return 0, ctx.Err()
	}

	stageRoot := filepath.Join(t.TempDir(), "stall-unmount-stage")
	sess, err := fusefs.OpenStageDirSession(stageRoot)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sess.Close() })

	fs := newTestRawFS(t, srv,
		withDirectIO(false),
		withStageSession(sess),
		withLogger(logger),
		withUnmountTimeouts(2*time.Second, time.Second, 80*time.Millisecond, 200*time.Millisecond),
	)

	f1 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "stall-1.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f1.NodeId, f1.Fh, 0, []byte("chunk-one"))
	f2 := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "stall-2.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f2.NodeId, f2.Fh, 0, []byte("chunk-two"))

	unmountDone := make(chan struct{})
	start := time.Now()
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	select {
	case <-unmountDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnUnmount did not abort stalled stage drain within stall timeout")
	}

	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.True(t, canceledInflight.Load(), "in-flight upload RPC must be canceled on stall timeout")
	require.Greater(t, fs.UnmountRemainingBudget(), time.Duration(0))
	require.NoDirExists(t, sess.InstanceDir(), "stage instance dir must be cleaned up on unmount")

	records := parseJSONLogMessages(t, logBuf.String())
	foundDropped := false
	for _, rec := range records {
		if rec["msg"] == "stage_unmount_drain_aborted" {
			if dropped, ok := rec["dropped_chunks"].(float64); ok && int(dropped) == 2 {
				foundDropped = true
			}
		}
	}
	require.True(t, foundDropped, "expected stage_unmount_drain_aborted log with dropped_chunks=2, got logs=%s", logBuf.String())
}

func TestRawFS_OnUnmount_HealthySlowUploads_ResetStallTimerUpToStageTimeout(t *testing.T) {
	t.Run("steady commits reset stall timer and complete all chunks", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			timer := time.NewTimer(55 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return 0, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		fs := newTestRawFS(t, srv,
			withDirectIO(false),
			withUnmountTimeouts(2*time.Second, 800*time.Millisecond, 120*time.Millisecond, 200*time.Millisecond),
		)

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "slow-healthy.bin", syscall.O_RDWR, 0o644)
		for i := range 4 {
			mustWriteAt(t, fs, f.NodeId, f.Fh, uint64(i*4096), []byte("data"))
		}

		fs.OnUnmount()

		require.Equal(t, int64(4), srv.writeCalls.Load(), "all 4 slow healthy chunks must commit when resetting stall timer")
	})

	t.Run("steady commits still stop at stage timeout ceiling", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			timer := time.NewTimer(60 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				return 0, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		fs := newTestRawFS(t, srv,
			withDirectIO(false),
			withUnmountTimeouts(2*time.Second, 200*time.Millisecond, 150*time.Millisecond, 200*time.Millisecond),
		)

		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "slow-ceiling.bin", syscall.O_RDWR, 0o644)
		for i := range 10 {
			mustWriteAt(t, fs, f.NodeId, f.Fh, uint64(i*4096), []byte("data"))
		}

		start := time.Now()
		unmountDone := make(chan struct{})
		go func() {
			fs.OnUnmount()
			close(unmountDone)
		}()

		select {
		case <-unmountDone:
		case <-time.After(600 * time.Millisecond):
			t.Fatal("OnUnmount exceeded stage timeout ceiling")
		}
		require.Less(t, time.Since(start), 500*time.Millisecond)
		require.Less(t, srv.writeCalls.Load(), int64(10))
	})
}

func TestRawFS_OnUnmount_FsyncTimeout_BoundsPhase2AndStopsDispatchingNewDirtyInodes(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.fsyncHook = func(ctx context.Context, _ *storagev1.FsyncRequest) error {
		<-ctx.Done()
		return ctx.Err()
	}
	fs := newTestRawFS(t, srv,
		withMode(fusefs.MountModeSync),
		withDirectIO(false),
		withUnmountTimeouts(time.Second, 0, 0, 80*time.Millisecond),
	)
	createDirtySyncFiles(t, fs, 100)

	unmountDone := make(chan struct{})
	start := time.Now()
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	select {
	case <-unmountDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnUnmount did not honor UnmountFsyncTimeout")
	}

	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.LessOrEqual(t, srv.fsyncCalls.Load(), int64(fusefs.DefaultUnmountFsyncConcurrency),
		"workers must stop dispatching new dirty inodes after fsync timeout: got=%d", srv.fsyncCalls.Load())
	require.Equal(t, fusefs.UnmountProgress{Active: false, Completed: 100, Total: 100}, fs.UnmountProgress())
}

func TestRawFS_OnUnmount_TotalBudgetExhausted_SkipsPhase2AndReportsZeroRemainingBudget(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	srv.writeHook = func(ctx context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	fs := newTestRawFS(t, srv,
		withDirectIO(false),
		withUnmountTimeouts(80*time.Millisecond, 80*time.Millisecond, 80*time.Millisecond, 80*time.Millisecond),
	)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "budget-exhausted.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("will-timeout"))

	unmountDone := make(chan struct{})
	go func() {
		fs.OnUnmount()
		close(unmountDone)
	}()

	select {
	case <-unmountDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnUnmount did not honor total UnmountTimeout")
	}

	require.Zero(t, fs.UnmountRemainingBudget(), "remaining budget must be 0 after total timeout exhaustion")
	require.Zero(t, srv.fsyncCalls.Load(), "Phase 2 Fsync must not run after total unmount budget is exhausted")
	require.Zero(t, srv.fsyncDirCalls.Load(), "Phase 2 FsyncDir must not run after total unmount budget is exhausted")
}

func TestRawFS_OnUnmount_RepeatedCallWithFailedOrphanInReleaseQueue_DoesNotBlockOrReissueReclaim(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	var attempt atomic.Int32
	srv.reclaimOrphanHook = func(ctx context.Context, _ *storagev1.ReclaimOrphanRequest) error {
		if attempt.Add(1) == 1 {
			return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
		}
		<-ctx.Done()
		return ctx.Err()
	}

	fs := newTestRawFS(t, srv,
		withMode(fusefs.MountModeSync),
		withDirectIO(false),
		withUnmountTimeouts(80*time.Millisecond, 0, 0, 80*time.Millisecond),
	)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orphan-repeat.bin", syscall.O_RDWR, 0o644)
	requireStatus(t, "Unlink open file", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orphan-repeat.bin"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

	fs.OnUnmount()
	require.Zero(t, fs.UnmountRemainingBudget(), "first OnUnmount must exhaust total unmount budget")
	callsAfterFirst := srv.reclaimOrphanCalls.Load()
	require.GreaterOrEqual(t, callsAfterFirst, int64(2))

	startSecond := time.Now()
	fs.OnUnmount()
	elapsedSecond := time.Since(startSecond)

	require.Less(t, elapsedSecond, 30*time.Millisecond, "repeated OnUnmount must not block again after budget expired: elapsed=%v", elapsedSecond)
	require.Equal(t, callsAfterFirst, srv.reclaimOrphanCalls.Load(),
		"repeated OnUnmount must not issue extra ReclaimOrphan RPCs: want=%d got=%d", callsAfterFirst, srv.reclaimOrphanCalls.Load())
}
