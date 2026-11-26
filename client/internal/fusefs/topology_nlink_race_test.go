package fusefs_test

import (
	"context"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func waitForNamespaceAttrHits(t *testing.T, fs *fusefs.RawFS, minHits uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		if fs.Snapshot().NamespaceCache.AttrHits >= minHits {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("scene=wait_attr_hits want>=%d got=%d", minHits, fs.Snapshot().NamespaceCache.AttrHits)
		}
		runtime.Gosched()
	}
}

func waitForNamespaceEntriesAtLeast(t *testing.T, fs *fusefs.RawFS, minEntries int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		if fs.Snapshot().NamespaceCache.Entries >= minEntries {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("scene=wait_ns_entries want>=%d got=%d", minEntries, fs.Snapshot().NamespaceCache.Entries)
		}
		runtime.Gosched()
	}
}

func installFirstLinkGate(srv *fakeStorage, targetName string) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.linkHook = func(_ context.Context, req *storagev1.LinkRequest) error {
		if string(req.GetNewName()) == targetName {
			once.Do(func() {
				close(enteredCh)
				<-gate
			})
		}
		return nil
	}
	return enteredCh, unblock
}

func installFirstUnlinkGate(srv *fakeStorage, targetName string) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.unlinkHook = func(_ context.Context, req *storagev1.UnlinkRequest) error {
		if string(req.GetName()) == targetName {
			once.Do(func() {
				close(enteredCh)
				<-gate
			})
		}
		return nil
	}
	return enteredCh, unblock
}

func installTwoStepUnlinkGate(srv *fakeStorage, firstName, secondName string) (firstEntered, secondEntered <-chan struct{}, releaseFirst func()) {
	firstCh := make(chan struct{})
	secondCh := make(chan struct{})
	gate, unblock := newTestGate()
	var firstOnce, secondOnce sync.Once
	srv.unlinkHook = func(_ context.Context, req *storagev1.UnlinkRequest) error {
		switch string(req.GetName()) {
		case firstName:
			firstOnce.Do(func() {
				close(firstCh)
				<-gate
			})
		case secondName:
			secondOnce.Do(func() {
				close(secondCh)
			})
		}
		return nil
	}
	return firstCh, secondCh, unblock
}

func installFirstWriteGate(srv *fakeStorage) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.writeHook = func(_ context.Context, _ *storagev1.WriteRequest) (uint32, error) {
		once.Do(func() {
			close(enteredCh)
			<-gate
		})
		return 0, nil
	}
	return enteredCh, unblock
}

func TestUnlink_PreLockSingleNlinkRacedByConcurrentLink_PreservesLiveFileAndDirtyState(t *testing.T) {
	t.Run("cached AttrTimeout>0 on promoted sync O_TMPFILE with open handle and dirty data", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		var tmpOut fuse.CreateOut
		requireStatus(t, "sync Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, []byte("promoted-dirty"))

		var promoteOut fuse.EntryOut
		requireStatus(t, "promote Link", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "link-a.bin", &promoteOut))

		linkEntered, unblockLink := installFirstLinkGate(srv, "link-b.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: tmpOut.NodeId,
			}, "link-b.bin", &out)
		}()
		<-linkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "link-a.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+1)

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)

		midSnap := syncFS.Snapshot()
		require.Zero(t, midSnap.Queues.OrphanInodes, "scene=cached_unlink_link_race_orphans want=0 got=%d", midSnap.Queues.OrphanInodes)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=cached_unlink_link_race_reclaim want=0 got=%d", srv.reclaimOrphanCalls.Load())
		require.True(t, srv.hasNode(tmpOut.NodeId), "scene=cached_unlink_link_race_node_alive want=true")
		require.Equal(t, uint32(1), srv.nodeNlink(tmpOut.NodeId), "scene=cached_unlink_link_race_nlink want=1 got=%d", srv.nodeNlink(tmpOut.NodeId))

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=cached_unlink_link_race_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=cached_unlink_link_race_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})

	t.Run("uncached AttrTimeout=0 on promoted sync O_TMPFILE with open handle and dirty data", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		var tmpOut fuse.CreateOut
		requireStatus(t, "sync Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpOut))
		mustWriteAt(t, syncFS, tmpOut.NodeId, tmpOut.Fh, 0, []byte("strict-dirty"))

		var promoteOut fuse.EntryOut
		requireStatus(t, "promote Link", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "strict-a.bin", &promoteOut))
		requireStatus(t, "invalidate cached inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}}, "user.k", []byte("v"),
		))

		linkEntered, unblockLink := installFirstLinkGate(srv, "strict-b.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: tmpOut.NodeId,
			}, "strict-b.bin", &out)
		}()
		<-linkEntered

		beforeEntries := syncFS.Snapshot().NamespaceCache.Entries
		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "strict-a.bin")
		}()
		waitForNamespaceEntriesAtLeast(t, syncFS, beforeEntries+1)

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)

		midSnap := syncFS.Snapshot()
		require.Zero(t, midSnap.Queues.OrphanInodes, "scene=zero_ttl_unlink_link_race_orphans want=0 got=%d", midSnap.Queues.OrphanInodes)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=zero_ttl_unlink_link_race_reclaim want=0 got=%d", srv.reclaimOrphanCalls.Load())
		require.True(t, srv.hasNode(tmpOut.NodeId), "scene=zero_ttl_unlink_link_race_node_alive want=true")

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=zero_ttl_unlink_link_race_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=zero_ttl_unlink_link_race_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})

	t.Run("closed written file with zero open handles flushes via single FsyncDir when raced by Link", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "closed-a.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, []byte("closed-dirty"))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		requireStatus(t, "invalidate cached inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: created.NodeId}}, "user.k", []byte("v"),
		))
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles)

		linkEntered, unblockLink := installFirstLinkGate(srv, "closed-b.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: created.NodeId,
			}, "closed-b.bin", &out)
		}()
		<-linkEntered

		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "closed-a.bin")
		}()

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=closed_dirty_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=closed_dirty_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})
}

func TestUnlink_TwoHardlinksRacedByConcurrentUnlinks_MarksOrphanAndDiscardsPendingStageOnSecondUnlink(t *testing.T) {
	t.Run("open 2-hardlink file marks orphan on second Unlink and reclaims on Release", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "u1.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, []byte("two-links"))
		var linkOut fuse.EntryOut
		requireStatus(t, "Link u2.bin", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "u2.bin", &linkOut))

		firstUnlinkEntered, unblockFirstUnlink := installFirstUnlinkGate(srv, "u1.bin")
		defer unblockFirstUnlink()

		u1StCh := make(chan fuse.Status, 1)
		go func() {
			u1StCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "u1.bin")
		}()
		<-firstUnlinkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		u2StCh := make(chan fuse.Status, 1)
		go func() {
			u2StCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "u2.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+1)

		unblockFirstUnlink()
		requireStatus(t, "Unlink u1.bin", fuse.OK, <-u1StCh)
		requireStatus(t, "Unlink u2.bin", fuse.OK, <-u2StCh)

		require.Equal(t, int64(1), srv.getAttrCalls.Load(), "scene=double_unlink_revalidation_getattr want=1 got=%d", srv.getAttrCalls.Load())
		require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes, "scene=double_unlink_orphan_marked want=1 got=%d", syncFS.Snapshot().Queues.OrphanInodes)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=double_unlink_reclaim_calls want=1 got=%d", srv.reclaimOrphanCalls.Load())
		require.False(t, srv.hasNode(created.NodeId), "scene=double_unlink_server_node_deleted want=false")
	})

	t.Run("closed 2-hardlink staged file with HasAnyPending discards queued stage chunks on second Unlink", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		writeEntered, unblockWrite := installFirstWriteGate(srv)
		defer unblockWrite()
		stagedFS := newTestRawFS(t, srv, withMode(fusefs.MountModeStaged))

		created := mustCreateFile(t, stagedFS, fuse.FUSE_ROOT_ID, "s1.bin", syscall.O_RDWR, 0o644)
		var linkOut fuse.EntryOut
		requireStatus(t, "Link s2.bin", fuse.OK, stagedFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "s2.bin", &linkOut))

		mustWriteAt(t, stagedFS, created.NodeId, created.Fh, 0, []byte("chunk-0"))
		mustWriteAt(t, stagedFS, created.NodeId, created.Fh, 8<<20, []byte("chunk-1"))
		<-writeEntered
		stagedFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		preSnap := stagedFS.Snapshot()
		require.Zero(t, preSnap.Handles.OpenFiles)
		require.Positive(t, preSnap.Stage.PendingChunks)

		firstUnlinkEntered, secondUnlinkEntered, unblockFirstUnlink := installTwoStepUnlinkGate(srv, "s1.bin", "s2.bin")
		defer unblockFirstUnlink()

		u1StCh := make(chan fuse.Status, 1)
		go func() {
			u1StCh <- stagedFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "s1.bin")
		}()
		<-firstUnlinkEntered

		beforeAttrHits := stagedFS.Snapshot().NamespaceCache.AttrHits
		u2StCh := make(chan fuse.Status, 1)
		go func() {
			u2StCh <- stagedFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "s2.bin")
		}()
		waitForNamespaceAttrHits(t, stagedFS, beforeAttrHits+1)

		unblockFirstUnlink()
		requireStatus(t, "Unlink s1.bin", fuse.OK, <-u1StCh)
		<-secondUnlinkEntered
		unblockWrite()
		requireStatus(t, "Unlink s2.bin", fuse.OK, <-u2StCh)

		require.Zero(t, stagedFS.Snapshot().Stage.PendingChunks, "scene=double_unlink_stage_discarded want=0 got=%d", stagedFS.Snapshot().Stage.PendingChunks)
		stagedFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=double_unlink_no_dirty_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=double_unlink_single_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})
}

func TestRename_PreLockSingleNlinkTargetRacedByConcurrentLink_PreservesLiveTargetAndDirtyState(t *testing.T) {
	t.Run("cached AttrTimeout>0 overwriting open dirty target raced by Link", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "target.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, target.NodeId, target.Fh, 0, []byte("target-dirty"))

		linkEntered, unblockLink := installFirstLinkGate(srv, "target-keep.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: target.NodeId,
			}, "target-keep.bin", &out)
		}()
		<-linkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "src.bin", "target.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+2)

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Rename", fuse.OK, <-renameStCh)

		midSnap := syncFS.Snapshot()
		require.Zero(t, midSnap.Queues.OrphanInodes, "scene=cached_rename_link_race_orphans want=0 got=%d", midSnap.Queues.OrphanInodes)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=cached_rename_link_race_reclaim want=0 got=%d", srv.reclaimOrphanCalls.Load())
		require.True(t, srv.hasNode(target.NodeId), "scene=cached_rename_link_race_node_alive want=true")
		require.Equal(t, uint32(1), srv.nodeNlink(target.NodeId), "scene=cached_rename_link_race_nlink want=1 got=%d", srv.nodeNlink(target.NodeId))

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=cached_rename_link_race_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=cached_rename_link_race_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})

	t.Run("uncached AttrTimeout=0 overwriting open dirty promoted sync O_TMPFILE target raced by Link", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(0, 0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "src-0.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		var tmpTarget fuse.CreateOut
		requireStatus(t, "sync Tmpfile", fuse.OK, syncFS.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    uint32(syscall.O_RDWR),
			Mode:     0o644,
		}, &tmpTarget))
		mustWriteAt(t, syncFS, tmpTarget.NodeId, tmpTarget.Fh, 0, []byte("tmp-target-dirty"))
		var promoteOut fuse.EntryOut
		requireStatus(t, "promote target Link", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpTarget.NodeId,
		}, "target-0.bin", &promoteOut))
		requireStatus(t, "invalidate target inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: tmpTarget.NodeId}}, "user.k", []byte("v"),
		))

		linkEntered, unblockLink := installFirstLinkGate(srv, "target-0-keep.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: tmpTarget.NodeId,
			}, "target-0-keep.bin", &out)
		}()
		<-linkEntered

		beforeEntries := syncFS.Snapshot().NamespaceCache.Entries
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "src-0.bin", "target-0.bin")
		}()
		waitForNamespaceEntriesAtLeast(t, syncFS, beforeEntries+1)

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Rename", fuse.OK, <-renameStCh)

		midSnap := syncFS.Snapshot()
		require.Zero(t, midSnap.Queues.OrphanInodes, "scene=zero_ttl_rename_link_race_orphans want=0 got=%d", midSnap.Queues.OrphanInodes)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpTarget.NodeId}, Fh: tmpTarget.Fh})
		require.Zero(t, srv.reclaimOrphanCalls.Load(), "scene=zero_ttl_rename_link_race_reclaim want=0 got=%d", srv.reclaimOrphanCalls.Load())
		require.True(t, srv.hasNode(tmpTarget.NodeId), "scene=zero_ttl_rename_link_race_node_alive want=true")

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=zero_ttl_rename_link_race_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=zero_ttl_rename_link_race_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})

	t.Run("closed written target with zero open handles flushes via single FsyncDir when raced by Link", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "src-closed.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "target-closed.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, target.NodeId, target.Fh, 0, []byte("closed-target-dirty"))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})
		requireStatus(t, "invalidate target inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: target.NodeId}}, "user.k", []byte("v"),
		))
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles)

		linkEntered, unblockLink := installFirstLinkGate(srv, "target-closed-keep.bin")
		defer unblockLink()

		linkStCh := make(chan fuse.Status, 1)
		go func() {
			var out fuse.EntryOut
			linkStCh <- syncFS.Link(nil, &fuse.LinkIn{
				InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Oldnodeid: target.NodeId,
			}, "target-closed-keep.bin", &out)
		}()
		<-linkEntered

		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "src-closed.bin", "target-closed.bin")
		}()

		unblockLink()
		requireStatus(t, "concurrent Link", fuse.OK, <-linkStCh)
		requireStatus(t, "concurrent Rename", fuse.OK, <-renameStCh)

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=closed_dirty_rename_unmount_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=closed_dirty_rename_unmount_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})
}

func TestRename_TwoHardlinkTargetRacedByConcurrentUnlink_MarksOrphanAndClearsClosedDirty(t *testing.T) {
	t.Run("open 2-hardlink target overwritten concurrently with Unlink of sibling link marks orphan", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "repl.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "t1.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, target.NodeId, target.Fh, 0, []byte("target-2-links"))
		var linkOut fuse.EntryOut
		requireStatus(t, "Link t2.bin", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: target.NodeId,
		}, "t2.bin", &linkOut))

		unlinkEntered, unblockUnlink := installFirstUnlinkGate(srv, "t2.bin")
		defer unblockUnlink()

		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "t2.bin")
		}()
		<-unlinkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "repl.bin", "t1.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+2)

		unblockUnlink()
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)
		requireStatus(t, "concurrent Rename", fuse.OK, <-renameStCh)

		require.Equal(t, int64(1), srv.getAttrCalls.Load(), "scene=unlink_rename_revalidation_getattr want=1 got=%d", srv.getAttrCalls.Load())
		require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes, "scene=unlink_rename_orphan_marked want=1 got=%d", syncFS.Snapshot().Queues.OrphanInodes)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load(), "scene=unlink_rename_reclaim_calls want=1 got=%d", srv.reclaimOrphanCalls.Load())
		require.False(t, srv.hasNode(target.NodeId), "scene=unlink_rename_server_node_deleted want=false")
	})

	t.Run("closed 2-hardlink target overwritten concurrently with Unlink flushes via single FsyncDir on unmount", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		targetIno := srv.seedFile(1, "dt1.bin", 0o644, nil)
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "drepl.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		targetOpen := mustOpenFile(t, syncFS, targetIno, syscall.O_RDWR)
		mustWriteAt(t, syncFS, targetIno, targetOpen.Fh, 0, []byte("closed-2-links-dirty"))
		var linkOut fuse.EntryOut
		requireStatus(t, "Link dt2.bin", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: targetIno,
		}, "dt2.bin", &linkOut))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: targetIno}, Fh: targetOpen.Fh})
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles)

		unlinkEntered, unblockUnlink := installFirstUnlinkGate(srv, "dt2.bin")
		defer unblockUnlink()

		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dt2.bin")
		}()
		<-unlinkEntered

		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "drepl.bin", "dt1.bin")
		}()

		unblockUnlink()
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)
		requireStatus(t, "concurrent Rename", fuse.OK, <-renameStCh)

		syncFS.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load(), "scene=unlink_rename_closed_no_fsync want=0 got=%d", srv.fsyncCalls.Load())
		require.Equal(t, int64(1), srv.fsyncDirCalls.Load(), "scene=unlink_rename_closed_single_fsyncdir want=1 got=%d", srv.fsyncDirCalls.Load())
	})
}

func TestUnlinkAndRename_RevalidationFastPathAndEdgeStatuses(t *testing.T) {
	t.Run("uncontended Unlink and Rename with open handles issue zero extra GetAttr RPCs", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		f1 := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "fast-unlink.bin", syscall.O_RDWR, 0o644)
		f2 := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "fast-target.bin", syscall.O_RDWR, 0o644)
		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "fast-src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		requireStatus(t, "uncontended Unlink", fuse.OK, syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "fast-unlink.bin"))
		requireStatus(t, "uncontended Rename", fuse.OK, syncFS.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}, "fast-src.bin", "fast-target.bin"))

		require.Zero(t, srv.getAttrCalls.Load(), "scene=uncontended_zero_getattr want=0 got=%d", srv.getAttrCalls.Load())
		require.Equal(t, 2, syncFS.Snapshot().Queues.OrphanInodes, "scene=uncontended_orphans want=2 got=%d", syncFS.Snapshot().Queues.OrphanInodes)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f2.NodeId}, Fh: f2.Fh})
		require.Equal(t, int64(2), srv.reclaimOrphanCalls.Load())
	})

	t.Run("Rename racing Unlink of closed 1-link target tolerates ENOENT on GetAttr revalidation", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		keepOpen := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "keep-open.bin", syscall.O_RDONLY, 0o644)
		defer syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: keepOpen.NodeId}, Fh: keepOpen.Fh})

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "enoent-src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "enoent-target.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})

		unlinkEntered, unblockUnlink := installFirstUnlinkGate(srv, "enoent-target.bin")
		defer unblockUnlink()

		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "enoent-target.bin")
		}()
		<-unlinkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "enoent-src.bin", "enoent-target.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+2)

		unblockUnlink()
		requireStatus(t, "concurrent Unlink", fuse.OK, <-unlinkStCh)
		requireStatus(t, "concurrent Rename after target ENOENT", fuse.OK, <-renameStCh)

		require.Equal(t, int64(1), srv.getAttrCalls.Load(), "scene=rename_enoent_revalidation_getattr want=1 got=%d", srv.getAttrCalls.Load())
		require.Equal(t, src.NodeId, mustLookup(t, syncFS, fuse.FUSE_ROOT_ID, "enoent-target.bin").NodeId)
		require.Zero(t, syncFS.Snapshot().Queues.OrphanInodes)
	})

	t.Run("contended Unlink and Rename propagate non-ENOENT GetAttr revalidation errors before mutating RPC", func(t *testing.T) {

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "err-a.bin", syscall.O_RDWR, 0o644)
		defer syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		var linkB, linkC fuse.EntryOut
		requireStatus(t, "Link err-b.bin", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "err-b.bin", &linkB))
		requireStatus(t, "Link err-c.bin", fuse.OK, syncFS.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: created.NodeId,
		}, "err-c.bin", &linkC))
		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "err-src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})

		srv.getAttrHook = func(_ context.Context, _ *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			return nil, errnoError(connect.CodeInternal, "EIO")
		}
		unlinkEntered, unblockUnlink := installFirstUnlinkGate(srv, "err-a.bin")
		defer unblockUnlink()

		u1StCh := make(chan fuse.Status, 1)
		go func() {
			u1StCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "err-a.bin")
		}()
		<-unlinkEntered

		beforeAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		u2StCh := make(chan fuse.Status, 1)
		go func() {
			u2StCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "err-b.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeAttrHits+1)

		beforeRenameAttrHits := syncFS.Snapshot().NamespaceCache.AttrHits
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "err-src.bin", "err-c.bin")
		}()
		waitForNamespaceAttrHits(t, syncFS, beforeRenameAttrHits+2)

		unblockUnlink()
		requireStatus(t, "first Unlink", fuse.OK, <-u1StCh)
		requireStatus(t, "second Unlink EIO", fuse.EIO, <-u2StCh)
		requireStatus(t, "contended Rename EIO", fuse.EIO, <-renameStCh)

		require.Equal(t, int64(1), srv.unlinkCalls.Load(), "scene=eio_aborts_second_unlink_rpc want=1 got=%d", srv.unlinkCalls.Load())
		require.Zero(t, srv.renameCalls.Load(), "scene=eio_aborts_rename_rpc want=0 got=%d", srv.renameCalls.Load())
	})
}

func installFirstRenameGate(srv *fakeStorage, targetName string) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.renameHook = func(_ context.Context, req *storagev1.RenameRequest) error {
		if string(req.GetNewName()) == targetName {
			once.Do(func() {
				close(enteredCh)
				<-gate
			})
		}
		return nil
	}
	return enteredCh, unblock
}

func installFirstGetAttrGate(srv *fakeStorage, targetIno uint64) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.getAttrHook = func(_ context.Context, req *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
		if req.GetIno() == targetIno {
			once.Do(func() {
				close(enteredCh)
				<-gate
			})
		}
		return nil, nil
	}
	return enteredCh, unblock
}

func TestOpen_ConcurrentWithUnlinkAndOverwriteRename_PreservesOrphanOrReturnsENOENT(t *testing.T) {
	t.Run("GivenCachedSingleLinkFile_WhenUnlinkWinsLock_ThenOpenReturnsENOENTWithZeroDanglingHandles", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "unlink-wins.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, []byte("unlink-wins-data"))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		unlinkEntered, unblockUnlink := installFirstUnlinkGate(srv, "unlink-wins.bin")
		defer unblockUnlink()

		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "unlink-wins.bin")
		}()
		<-unlinkEntered

		var openOut fuse.OpenOut
		openStCh := make(chan fuse.Status, 1)
		go func() {
			openStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: created.NodeId},
				Flags:    uint32(syscall.O_RDONLY),
			}, &openOut)
		}()
		time.Sleep(20 * time.Millisecond)

		unblockUnlink()
		requireStatus(t, "Unlink", fuse.OK, <-unlinkStCh)
		requireStatus(t, "racing Open after Unlink", fuse.ENOENT, <-openStCh)
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles, "scene=open_after_unlink_dangling_handles want=0")
		require.False(t, srv.hasNode(created.NodeId))
	})

	t.Run("GivenCachedSingleLinkFile_WhenOpenWinsLock_ThenUnlinkMarksOrphanAndReadSucceedsUntilRelease", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "open-wins-unlink.bin", syscall.O_RDWR, 0o644)
		payload := []byte("open-wins-payload")
		mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, payload)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})
		requireStatus(t, "invalidate cached inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: created.NodeId}}, "user.k", []byte("v"),
		))

		getAttrEntered, releaseGetAttr := installFirstGetAttrGate(srv, created.NodeId)
		defer releaseGetAttr()

		var sawOpenInClient bool
		srv.unlinkHook = func(_ context.Context, req *storagev1.UnlinkRequest) error {
			sawOpenInClient = req.GetOpenInClient()
			return nil
		}

		var openOut fuse.OpenOut
		openStCh := make(chan fuse.Status, 1)
		go func() {
			openStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: created.NodeId},
				Flags:    uint32(syscall.O_RDONLY),
			}, &openOut)
		}()
		<-getAttrEntered

		entryHitsBefore := syncFS.Snapshot().NamespaceCache.EntryHits
		unlinkStCh := make(chan fuse.Status, 1)
		go func() {
			unlinkStCh <- syncFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "open-wins-unlink.bin")
		}()
		require.Eventually(t, func() bool {
			return syncFS.Snapshot().NamespaceCache.EntryHits > entryHitsBefore
		}, 3*time.Second, 5*time.Millisecond, "scene=unlink_entry_hit_before_lock")
		time.Sleep(20 * time.Millisecond)

		releaseGetAttr()
		requireStatus(t, "Open wins lock", fuse.OK, <-openStCh)
		requireStatus(t, "Unlink after Open", fuse.OK, <-unlinkStCh)
		require.True(t, sawOpenInClient, "scene=open_wins_unlink_flag want=true got=false")
		require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes)
		requireBytesMatch(t, "Read orphan after Open wins Unlink", payload, mustReadAt(t, syncFS, created.NodeId, openOut.Fh, 0, len(payload)))

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: openOut.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load())
		require.False(t, srv.hasNode(created.NodeId))
	})

	t.Run("GivenCachedSingleLinkFile_WhenOverwriteRenameWinsLock_ThenOpenReturnsENOENTWithZeroDanglingHandles", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "ren-src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "ren-target.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, target.NodeId, target.Fh, 0, []byte("victim"))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})

		renameEntered, unblockRename := installFirstRenameGate(srv, "ren-target.bin")
		defer unblockRename()

		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "ren-src.bin", "ren-target.bin")
		}()
		<-renameEntered

		var openOut fuse.OpenOut
		openStCh := make(chan fuse.Status, 1)
		go func() {
			openStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: target.NodeId},
				Flags:    uint32(syscall.O_RDONLY),
			}, &openOut)
		}()
		time.Sleep(20 * time.Millisecond)

		unblockRename()
		requireStatus(t, "overwrite Rename", fuse.OK, <-renameStCh)
		requireStatus(t, "racing Open after overwrite Rename", fuse.ENOENT, <-openStCh)
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles, "scene=open_after_rename_dangling_handles want=0")
		require.False(t, srv.hasNode(target.NodeId))
	})

	t.Run("GivenCachedSingleLinkFile_WhenOpenWinsLock_ThenOverwriteRenameMarksOrphanAndReadSucceedsUntilRelease", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		src := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "ren-ow-src.bin", syscall.O_RDWR, 0o644)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: src.NodeId}, Fh: src.Fh})
		target := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "ren-ow-target.bin", syscall.O_RDWR, 0o644)
		payload := []byte("rename-open-wins")
		mustWriteAt(t, syncFS, target.NodeId, target.Fh, 0, payload)
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: target.Fh})
		requireStatus(t, "invalidate target inode via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: target.NodeId}}, "user.k", []byte("v"),
		))

		getAttrEntered, releaseGetAttr := installFirstGetAttrGate(srv, target.NodeId)
		defer releaseGetAttr()

		var sawTargetOpen bool
		srv.renameHook = func(_ context.Context, req *storagev1.RenameRequest) error {
			sawTargetOpen = req.GetTargetOpenInClient()
			return nil
		}

		var openOut fuse.OpenOut
		openStCh := make(chan fuse.Status, 1)
		go func() {
			openStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: target.NodeId},
				Flags:    uint32(syscall.O_RDONLY),
			}, &openOut)
		}()
		<-getAttrEntered

		entryHitsBefore := syncFS.Snapshot().NamespaceCache.EntryHits
		renameStCh := make(chan fuse.Status, 1)
		go func() {
			renameStCh <- syncFS.Rename(nil, &fuse.RenameIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Newdir:   fuse.FUSE_ROOT_ID,
			}, "ren-ow-src.bin", "ren-ow-target.bin")
		}()
		require.Eventually(t, func() bool {
			return syncFS.Snapshot().NamespaceCache.EntryHits >= entryHitsBefore+2
		}, 3*time.Second, 5*time.Millisecond, "scene=rename_entry_hits_before_lock")
		time.Sleep(20 * time.Millisecond)

		releaseGetAttr()
		requireStatus(t, "Open wins lock", fuse.OK, <-openStCh)
		requireStatus(t, "overwrite Rename after Open", fuse.OK, <-renameStCh)
		require.True(t, sawTargetOpen, "scene=open_wins_rename_flag want=true got=false")
		require.Equal(t, 1, syncFS.Snapshot().Queues.OrphanInodes)
		requireBytesMatch(t, "Read orphan after Open wins Rename", payload, mustReadAt(t, syncFS, target.NodeId, openOut.Fh, 0, len(payload)))

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: target.NodeId}, Fh: openOut.Fh})
		require.Equal(t, int64(1), srv.reclaimOrphanCalls.Load())
		require.False(t, srv.hasNode(target.NodeId))
	})
}

func TestOpen_ParallelNonTruncSharesRLockAndOTruncTakesExclusiveLock(t *testing.T) {
	t.Run("GivenCacheMissFile_WhenTwoNonTruncOpensRunInParallel_ThenBothEnterGetAttrConcurrently", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(0, 0))
		ino := srv.seedFile(1, "parallel-open.bin", 0o644, []byte("abc"))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		bothEntered := make(chan struct{})
		var active sync.Mutex
		inFlight := 0
		maxInFlight := 0
		srv.getAttrHook = func(ctx context.Context, req *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			if req.GetIno() != ino {
				return nil, nil
			}
			active.Lock()
			inFlight++
			if inFlight > maxInFlight {
				maxInFlight = inFlight
			}
			if inFlight == 2 {
				close(bothEntered)
			}
			active.Unlock()

			select {
			case <-bothEntered:
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
			}

			active.Lock()
			inFlight--
			active.Unlock()
			return nil, nil
		}

		var out1, out2 fuse.OpenOut
		st1Ch := make(chan fuse.Status, 1)
		st2Ch := make(chan fuse.Status, 1)
		go func() {
			st1Ch <- syncFS.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &out1)
		}()
		go func() {
			st2Ch <- syncFS.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino}, Flags: uint32(syscall.O_RDONLY)}, &out2)
		}()

		requireStatus(t, "first parallel Open", fuse.OK, <-st1Ch)
		requireStatus(t, "second parallel Open", fuse.OK, <-st2Ch)
		require.Equal(t, 2, maxInFlight, "scene=parallel_non_trunc_open_rlock want=2 got=%d", maxInFlight)

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out1.Fh})
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out2.Fh})
	})

	t.Run("GivenFile_WhenOpenWithOTruncHoldsSetAttr_ThenConcurrentOpenBlocksUntilTruncFinishes", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		created := mustCreateFile(t, syncFS, fuse.FUSE_ROOT_ID, "trunc-excl.bin", syscall.O_RDWR, 0o644)
		mustWriteAt(t, syncFS, created.NodeId, created.Fh, 0, []byte("before-trunc"))
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: created.Fh})

		setAttrEntered := make(chan struct{})
		setAttrGate, releaseSetAttr := newTestGate()
		defer releaseSetAttr()
		var once sync.Once
		srv.setAttrHook = func(_ context.Context, _ *storagev1.SetAttrRequest) error {
			once.Do(func() {
				close(setAttrEntered)
				<-setAttrGate
			})
			return nil
		}

		var truncOut, rdOut fuse.OpenOut
		truncStCh := make(chan fuse.Status, 1)
		go func() {
			truncStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: created.NodeId},
				Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			}, &truncOut)
		}()
		<-setAttrEntered

		rdStCh := make(chan fuse.Status, 1)
		go func() {
			rdStCh <- syncFS.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: created.NodeId},
				Flags:    uint32(syscall.O_RDONLY),
			}, &rdOut)
		}()

		select {
		case st := <-rdStCh:
			t.Fatalf("scene=open_otrunc_exclusive_lock want=blocked got=early_return(%v)", st)
		case <-time.After(25 * time.Millisecond):
		}

		releaseSetAttr()
		requireStatus(t, "Open O_TRUNC", fuse.OK, <-truncStCh)
		requireStatus(t, "blocked Open O_RDONLY", fuse.OK, <-rdStCh)
		require.GreaterOrEqual(t, syncFS.Snapshot().InodeLocks.ContendedAcquisitions, uint64(1))

		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: truncOut.Fh})
		syncFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: created.NodeId}, Fh: rdOut.Fh})
	})
}

func installFirstRmdirGate(srv *fakeStorage, targetName string) (entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	gate, unblock := newTestGate()
	var once sync.Once
	srv.rmdirHook = func(_ context.Context, req *storagev1.RmdirRequest) error {
		if string(req.GetName()) == targetName {
			once.Do(func() {
				close(enteredCh)
				<-gate
			})
		}
		return nil
	}
	return enteredCh, unblock
}

func TestOpenDir_ConcurrentWithRmdir_PopulatesRemovedDirOrReturnsENOENT(t *testing.T) {
	t.Run("GivenCachedDir_WhenRmdirWinsLock_ThenOpenDirWaitsAndReturnsENOENTWithZeroDanglingHandles", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		dir := mustMkdir(t, syncFS, fuse.FUSE_ROOT_ID, "rmdir-wins-dir", 0o755)
		rmdirEntered, unblockRmdir := installFirstRmdirGate(srv, "rmdir-wins-dir")
		defer unblockRmdir()

		rmdirStCh := make(chan fuse.Status, 1)
		go func() {
			rmdirStCh <- syncFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "rmdir-wins-dir")
		}()
		<-rmdirEntered

		var dirOut fuse.OpenOut
		openDirStCh := make(chan fuse.Status, 1)
		go func() {
			openDirStCh <- syncFS.OpenDir(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: dir.NodeId},
			}, &dirOut)
		}()
		time.Sleep(20 * time.Millisecond)

		unblockRmdir()
		requireStatus(t, "Rmdir", fuse.OK, <-rmdirStCh)
		requireStatus(t, "racing OpenDir after Rmdir", fuse.ENOENT, <-openDirStCh)
		require.Zero(t, syncFS.Snapshot().Handles.OpenDirs, "scene=opendir_after_rmdir_dangling_handles want=0")
	})

	t.Run("GivenDirWithCachedEntryAndNilInode_WhenOpenDirWinsLock_ThenRmdirPopulatesInodeAndMarksRemovedDir", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		syncFS := newTestRawFS(t, srv, withMode(fusefs.MountModeSync))

		dir := mustMkdir(t, syncFS, fuse.FUSE_ROOT_ID, "opendir-wins-dir", 0o755)
		requireStatus(t, "invalidate dir inode cache via SetXAttr", fuse.OK, syncFS.SetXAttr(
			nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: dir.NodeId}}, "user.k", []byte("v"),
		))
		require.Zero(t, syncFS.Snapshot().Handles.OpenDirs)
		require.Zero(t, syncFS.Snapshot().Handles.OpenFiles)

		getAttrEntered, releaseGetAttr := installFirstGetAttrGate(srv, dir.NodeId)
		defer releaseGetAttr()

		var dirOut fuse.OpenOut
		openDirStCh := make(chan fuse.Status, 1)
		go func() {
			openDirStCh <- syncFS.OpenDir(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: dir.NodeId},
			}, &dirOut)
		}()
		<-getAttrEntered

		rmdirStCh := make(chan fuse.Status, 1)
		go func() {
			rmdirStCh <- syncFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "opendir-wins-dir")
		}()
		time.Sleep(20 * time.Millisecond)

		releaseGetAttr()
		requireStatus(t, "OpenDir wins lock", fuse.OK, <-openDirStCh)
		requireStatus(t, "Rmdir after OpenDir", fuse.OK, <-rmdirStCh)

		var removedAttr fuse.AttrOut
		requireStatus(t, "GetAttr removed open dir", fuse.OK, syncFS.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: dir.NodeId},
		}, &removedAttr))
		require.Zero(t, removedAttr.Nlink, "scene=opendir_wins_rmdir_nlink want=0 got=%d", removedAttr.Nlink)

		readDirBefore := srv.readDirPlusCalls.Load()
		list := fuse.NewDirEntryList(make([]byte, 4096), 0)
		requireStatus(t, "ReadDir removed open dir", fuse.OK, syncFS.ReadDir(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dir.NodeId},
			Fh:       dirOut.Fh,
			Size:     4096,
		}, list))
		require.Empty(t, list.Bytes())
		require.Equal(t, readDirBefore, srv.readDirPlusCalls.Load())

		syncFS.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dir.NodeId}, Fh: dirOut.Fh})
		var afterRelease fuse.AttrOut
		requireStatus(t, "GetAttr after ReleaseDir", fuse.ENOENT, syncFS.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: dir.NodeId},
		}, &afterRelease))
	})
}
