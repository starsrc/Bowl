package fusefs_test

import (
	"context"
	"encoding/binary"
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

func TestStep68_StagedWrite_SerializedAgainstSetAttrFallocateAndIoctl(t *testing.T) {
	t.Run("SetAttr_Truncate_BlocksConcurrentStagedWriteUntilComplete", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		setAttrEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var hookOnce sync.Once
		srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			hookOnce.Do(func() { close(setAttrEntered) })
			<-gate
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "setattr-race.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("0123456789"))

		setAttrDone := make(chan fuse.Status, 1)
		go func() {
			var out fuse.AttrOut
			setAttrDone <- fs.SetAttr(nil, &fuse.SetAttrIn{
				SetAttrInCommon: fuse.SetAttrInCommon{
					InHeader: fuse.InHeader{NodeId: f.NodeId},
					Valid:    fuse.FATTR_SIZE,
					Size:     2,
				},
			}, &out)
		}()

		select {
		case <-setAttrEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("SetAttr did not reach server hook")
		}

		writeDone := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     3,
			}, []byte("XYZ"))
			writeDone <- st
		}()

		select {
		case st := <-writeDone:
			t.Fatalf("staged Write completed concurrently with in-flight SetAttr: status=%v", st)
		case <-time.After(40 * time.Millisecond):
		}

		releaseGate()
		requireStatus(t, "SetAttr truncate", fuse.OK, waitUntilStatusChan(t, setAttrDone, "SetAttr"))
		requireStatus(t, "staged Write after SetAttr", fuse.OK, waitUntilStatusChan(t, writeDone, "Write"))
		requireStatus(t, "Fsync after serialized SetAttr+Write", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Fh:       f.Fh,
		}))

		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "final bytes after serialized SetAttr+Write", []byte("XYZ"), got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("NoOpSetAttr_DoesNotBlockOnExclusiveMutation", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fallocEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var hookOnce sync.Once
		srv.fallocateHook = func(_ context.Context, _ *storagev1.FallocateRequest) error {
			hookOnce.Do(func() { close(fallocEntered) })
			<-gate
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "noop-setattr.bin", syscall.O_RDWR, 0o644)

		fallocDone := make(chan fuse.Status, 1)
		go func() {
			fallocDone <- fs.Fallocate(nil, &fuse.FallocateIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Length:   16,
				Mode:     0,
			})
		}()

		select {
		case <-fallocEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("Fallocate did not reach server hook")
		}

		noopDone := make(chan fuse.Status, 1)
		go func() {
			var out fuse.AttrOut
			noopDone <- fs.SetAttr(nil, &fuse.SetAttrIn{
				SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: f.NodeId}},
			}, &out)
		}()

		select {
		case st := <-noopDone:
			requireStatus(t, "no-op SetAttr while Fallocate holds lock", fuse.OK, st)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("no-op SetAttr blocked on exclusive inode lock")
		}

		releaseGate()
		requireStatus(t, "Fallocate completion", fuse.OK, waitUntilStatusChan(t, fallocDone, "Fallocate"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("Fallocate_BlocksConcurrentStagedWriteUntilComplete", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fallocEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var hookOnce sync.Once
		srv.fallocateHook = func(_ context.Context, _ *storagev1.FallocateRequest) error {
			hookOnce.Do(func() { close(fallocEntered) })
			<-gate
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "falloc-race.bin", syscall.O_RDWR, 0o644)

		fallocDone := make(chan fuse.Status, 1)
		go func() {
			fallocDone <- fs.Fallocate(nil, &fuse.FallocateIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Length:   8,
				Mode:     0,
			})
		}()

		select {
		case <-fallocEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("Fallocate did not reach server hook")
		}

		writeDone := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     4,
			}, []byte("abcd"))
			writeDone <- st
		}()

		select {
		case st := <-writeDone:
			t.Fatalf("staged Write completed concurrently with in-flight Fallocate: status=%v", st)
		case <-time.After(40 * time.Millisecond):
		}

		releaseGate()
		requireStatus(t, "Fallocate", fuse.OK, waitUntilStatusChan(t, fallocDone, "Fallocate"))
		requireStatus(t, "staged Write after Fallocate", fuse.OK, waitUntilStatusChan(t, writeDone, "Write"))
		requireStatus(t, "Fsync after Fallocate+Write", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Fh:       f.Fh,
		}))

		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "final bytes after Fallocate+Write", []byte("abcd\x00\x00\x00\x00"), got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("IoctlSetFlags_BlocksConcurrentStagedWriteAndEnforcesImmutable", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		ioctlEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var hookOnce sync.Once
		srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			hookOnce.Do(func() { close(ioctlEntered) })
			<-gate
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "ioctl-race.bin", syscall.O_RDWR, 0o644)

		inbuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(inbuf, uint64(fusefs.FS_IMMUTABLE_FL))
		ioctlDone := make(chan fuse.Status, 1)
		go func() {
			var out fuse.IoctlOut
			ioctlDone <- fs.Ioctl(nil, &fuse.IoctlIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Cmd:      fusefs.FS_IOC_SETFLAGS,
			}, inbuf, &out, nil)
		}()

		select {
		case <-ioctlEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("Ioctl SETFLAGS did not reach server SetAttr hook")
		}

		writeDone := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     5,
			}, []byte("hello"))
			writeDone <- st
		}()

		select {
		case st := <-writeDone:
			t.Fatalf("staged Write completed concurrently with in-flight Ioctl SETFLAGS: status=%v", st)
		case <-time.After(40 * time.Millisecond):
		}

		releaseGate()
		requireStatus(t, "Ioctl SETFLAGS immutable", fuse.OK, waitUntilStatusChan(t, ioctlDone, "Ioctl"))
		requireStatus(t, "Write after immutable SETFLAGS", fuse.EPERM, waitUntilStatusChan(t, writeDone, "Write"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("StagedPassthroughWrite_SerializedAgainstRegularStagedWrite", func(t *testing.T) {
		cases := []struct {
			name        string
			setupHandle func(t *testing.T, srv *fakeStorage, fs *fusefs.RawFS, nodeID uint64) uint64
			payload     []byte
		}{
			{
				name: "O_SYNC",
				setupHandle: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, nodeID uint64) uint64 {
					t.Helper()
					return mustOpenFile(t, fs, nodeID, syscall.O_RDWR|syscall.O_SYNC).Fh
				},
				payload: []byte("sync-pass"),
			},
			{
				name: "ZeroLength",
				setupHandle: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, nodeID uint64) uint64 {
					t.Helper()
					return mustOpenFile(t, fs, nodeID, syscall.O_RDWR).Fh
				},
				payload: nil,
			},
			{
				name: "OverMaxRPCChunkSize",
				setupHandle: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, nodeID uint64) uint64 {
					t.Helper()
					return mustOpenFile(t, fs, nodeID, syscall.O_RDWR).Fh
				},
				payload: make([]byte, fusefs.MaxRPCChunkSize+1),
			},
			{
				name: "CapabilityPresentUpgrade",
				setupHandle: func(t *testing.T, _ *fakeStorage, fs *fusefs.RawFS, nodeID uint64) uint64 {
					t.Helper()
					requireStatus(t, "SetXAttr cap", fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{
						InHeader: fuse.InHeader{NodeId: nodeID},
					}, "security.capability", []byte("capv1")))
					return mustOpenFile(t, fs, nodeID, syscall.O_RDWR).Fh
				},
				payload: []byte("cap-pass"),
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				srv := newFakeStorage(t, withTTL(1.0, 1.0))
				fs := newTestRawFS(t, srv, withDirectIO(false))
				f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "pass-"+tc.name+".bin", syscall.O_RDWR, 0o644)
				passFh := tc.setupHandle(t, srv, fs, f.NodeId)

				passEntered := make(chan struct{})
				gate, releaseGate := newTestGate()
				defer releaseGate()
				var writeCalls atomic.Int32
				srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
					if writeCalls.Add(1) == 1 {
						close(passEntered)
						<-gate
					}
					return 0, nil
				}

				passDone := make(chan fuse.Status, 1)
				go func() {
					_, st := fs.Write(nil, &fuse.WriteIn{
						InHeader: fuse.InHeader{NodeId: f.NodeId},
						Fh:       passFh,
						Offset:   0,
						Size:     uint32(len(tc.payload)),
					}, tc.payload)
					passDone <- st
				}()

				select {
				case <-passEntered:
				case <-time.After(2 * time.Second):
					t.Fatalf("%s passthrough Write did not reach server hook", tc.name)
				}

				stagedDone := make(chan fuse.Status, 1)
				go func() {
					_, st := fs.Write(nil, &fuse.WriteIn{
						InHeader: fuse.InHeader{NodeId: f.NodeId},
						Fh:       f.Fh,
						Offset:   0,
						Size:     4,
					}, []byte("tail"))
					stagedDone <- st
				}()

				select {
				case st := <-stagedDone:
					t.Fatalf("%s: regular staged Write completed concurrently with passthrough Write: status=%v", tc.name, st)
				case <-time.After(40 * time.Millisecond):
				}

				releaseGate()
				requireStatus(t, tc.name+" passthrough Write", fuse.OK, waitUntilStatusChan(t, passDone, tc.name+" pass"))
				requireStatus(t, tc.name+" regular staged Write", fuse.OK, waitUntilStatusChan(t, stagedDone, tc.name+" staged"))

				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: passFh})
				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
			})
		}
	})

	t.Run("ParallelRegularWrites_StayConcurrentUnderRLock", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		var inFlight atomic.Int32
		bothEntered := make(chan struct{})
		var enteredOnce sync.Once
		srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			if inFlight.Add(1) == 2 {
				enteredOnce.Do(func() { close(bothEntered) })
			}
			select {
			case <-bothEntered:
				return 0, nil
			case <-time.After(2 * time.Second):
				return 0, context.DeadlineExceeded
			}
		}
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))
		f := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "parallel.bin", syscall.O_RDWR, 0o644)

		stCh1 := make(chan fuse.Status, 1)
		stCh2 := make(chan fuse.Status, 1)
		go func() {
			_, st := syncFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     4,
			}, []byte("AAAA"))
			stCh1 <- st
		}()
		go func() {
			_, st := syncFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   4,
				Size:     4,
			}, []byte("BBBB"))
			stCh2 <- st
		}()

		requireStatus(t, "parallel Write 1", fuse.OK, waitUntilStatusChan(t, stCh1, "Write 1"))
		requireStatus(t, "parallel Write 2", fuse.OK, waitUntilStatusChan(t, stCh2, "Write 2"))
		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "parallel writes bytes", []byte("AAAABBBB"), got)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("OAppendWrites_SerializedWithoutSelfDeadlock", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		firstEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var calls atomic.Int32
		srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
			if calls.Add(1) == 1 {
				close(firstEntered)
				<-gate
			}
			return 0, nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "append.bin", syscall.O_RDWR|syscall.O_APPEND, 0o644)

		st1 := make(chan fuse.Status, 1)
		st2 := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     4,
			}, []byte("1111"))
			st1 <- st
		}()

		select {
		case <-firstEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("first O_APPEND Write did not reach server hook")
		}

		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     4,
			}, []byte("2222"))
			st2 <- st
		}()

		select {
		case st := <-st2:
			t.Fatalf("second O_APPEND Write bypassed exclusive lock: status=%v", st)
		case <-time.After(40 * time.Millisecond):
		}

		releaseGate()
		requireStatus(t, "first O_APPEND Write", fuse.OK, waitUntilStatusChan(t, st1, "append 1"))
		requireStatus(t, "second O_APPEND Write", fuse.OK, waitUntilStatusChan(t, st2, "append 2"))
		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "serialized O_APPEND bytes", []byte("11112222"), got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})

	t.Run("OpenOTrunc_BlocksConcurrentStagedWriteUntilComplete", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		truncEntered := make(chan struct{})
		gate, releaseGate := newTestGate()
		defer releaseGate()
		var hookOnce sync.Once
		srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			hookOnce.Do(func() { close(truncEntered) })
			<-gate
			return nil
		}
		fs := newTestRawFS(t, srv, withDirectIO(false))
		f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "trunc-race.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("old-payload"))

		var openOut fuse.OpenOut
		openDone := make(chan fuse.Status, 1)
		go func() {
			openDone <- fs.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Flags:    syscall.O_RDWR | syscall.O_TRUNC,
			}, &openOut)
		}()

		select {
		case <-truncEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("Open(O_TRUNC) did not reach SetAttr hook")
		}

		writeDone := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: f.NodeId},
				Fh:       f.Fh,
				Offset:   0,
				Size:     3,
			}, []byte("NEW"))
			writeDone <- st
		}()

		select {
		case st := <-writeDone:
			t.Fatalf("staged Write completed concurrently with Open(O_TRUNC): status=%v", st)
		case <-time.After(40 * time.Millisecond):
		}

		releaseGate()
		requireStatus(t, "Open(O_TRUNC)", fuse.OK, waitUntilStatusChan(t, openDone, "Open(O_TRUNC)"))
		requireStatus(t, "staged Write after Open(O_TRUNC)", fuse.OK, waitUntilStatusChan(t, writeDone, "Write"))
		requireStatus(t, "Fsync after Open(O_TRUNC)+Write", fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: f.NodeId},
			Fh:       f.Fh,
		}))

		got, ok := srv.fileBytes(f.NodeId)
		require.True(t, ok)
		requireBytesMatch(t, "final bytes after Open(O_TRUNC)+Write", []byte("NEW"), got)
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: openOut.Fh})
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
	})
}
