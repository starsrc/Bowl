package engine

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type syncEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *syncEventLog) add(ev string) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *syncEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.events))
	copy(out, l.events)
	return out
}

func TestStep21_CreateTmpfile_LinkPromotion_ExclusiveReject_AndPurgeOrphan(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step21-tmpfile",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	sgidDir, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "sgid", ModeSetGID|0o755, 100, 200)
	require.NoError(t, err)
	require.Equal(t, uint32(200), sgidDir.GID)

	linkable, err := disk.CreateTmpfile(ctx, DefaultBranchName, sgidDir.Ino, 0o600, 100, 999, false)
	require.NoError(t, err)
	require.Equal(t, uint32(0), linkable.Nlink)
	require.Equal(t, ModeRegular|0o600, linkable.Mode)
	require.Equal(t, uint32(200), linkable.GID)

	entries, err := disk.ReadDir(ctx, DefaultBranchName, sgidDir.Ino)
	require.NoError(t, err)
	require.Empty(t, entries)

	_, err = disk.Write(ctx, DefaultBranchName, linkable.Ino, 0, []byte("tmp-payload"))
	require.NoError(t, err)

	promoted, err := disk.Link(ctx, DefaultBranchName, linkable.Ino, sgidDir.Ino, "promoted.txt")
	require.NoError(t, err)
	require.Equal(t, uint32(1), promoted.Nlink)
	require.Equal(t, sgidDir.Ino, promoted.ParentIno)
	require.Zero(t, promoted.Flags&InodeFlagEverLinked)

	data, err := disk.Read(ctx, DefaultBranchName, promoted.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("tmp-payload"), data)

	br, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	_, hasOrphan, err := readLayerOrphanKind(disk.db, br.WorkLayerID, promoted.Ino)
	require.NoError(t, err)
	require.False(t, hasOrphan)

	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, promoted.Ino))
	stillLive, err := disk.GetAttr(ctx, DefaultBranchName, promoted.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(1), stillLive.Nlink)

	excl, err := disk.CreateTmpfile(ctx, DefaultBranchName, sgidDir.Ino, 0o600, 100, 100, true)
	require.NoError(t, err)
	require.Equal(t, uint32(0), excl.Nlink)

	_, err = disk.Link(ctx, DefaultBranchName, excl.Ino, sgidDir.Ino, "blocked.txt")
	require.ErrorIs(t, err, ErrNotFound)

	_, err = disk.Write(ctx, DefaultBranchName, excl.Ino, 0, []byte("excl-secret"))
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, excl.Ino, "user.k", []byte("v"), 0))

	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, excl.Ino))
	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, excl.Ino))
	_, err = disk.GetAttr(ctx, DefaultBranchName, excl.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = disk.CreateTmpfile(ctx, DefaultBranchName, promoted.Ino, 0o600, 0, 0, false)
	require.ErrorIs(t, err, ErrNotDir)
}

func TestStep21_UnlinkOpenInClient_OverlayOrphanIsolation_CommitForkAndZeroLayerFork(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step21-isolation",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	baseFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, baseFile.Ino, 0, []byte("ancestor-data"))
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, baseFile.Ino, "user.origin", []byte("commit1"), 0))
	cmt1, err := disk.Commit(ctx, DefaultBranchName, "base commit")
	require.NoError(t, err)
	require.Positive(t, cmt1.LayerID)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "base.txt", true))

	_, err = disk.Link(ctx, DefaultBranchName, baseFile.Ino, RootIno, "revive.txt")
	require.ErrorIs(t, err, ErrNotFound)

	orphanAttr, err := disk.GetAttr(ctx, DefaultBranchName, baseFile.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(0), orphanAttr.Nlink)

	_, err = disk.Write(ctx, DefaultBranchName, baseFile.Ino, 13, []byte("-tail"))
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, baseFile.Ino, "user.live", []byte("yes"), 0))

	tmpInWork, err := disk.CreateTmpfile(ctx, DefaultBranchName, RootIno, 0o600, 0, 0, false)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, tmpInWork.Ino, 0, []byte("work-only-tmp"))
	require.NoError(t, err)

	cmt2, err := disk.Commit(ctx, DefaultBranchName, "commit with live orphans")
	require.NoError(t, err)

	_, err = disk.GetAttrAtCommit(ctx, cmt2.ID, baseFile.Ino)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetAttrAtCommit(ctx, cmt2.ID, tmpInWork.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	gotBase, err := disk.Read(ctx, DefaultBranchName, baseFile.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("ancestor-data-tail"), gotBase)
	gotOrigin, err := disk.GetXattr(ctx, DefaultBranchName, baseFile.Ino, "user.origin")
	require.NoError(t, err)
	require.Equal(t, []byte("commit1"), gotOrigin)
	gotLive, err := disk.GetXattr(ctx, DefaultBranchName, baseFile.Ino, "user.live")
	require.NoError(t, err)
	require.Equal(t, []byte("yes"), gotLive)

	gotTmp, err := disk.Read(ctx, DefaultBranchName, tmpInWork.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("work-only-tmp"), gotTmp)

	forkCmt, featBranch, err := disk.Fork(ctx, DefaultBranchName, "feat-orphan-only", "fork with only orphans in work layer")
	require.NoError(t, err)
	require.Equal(t, uint64(0), forkCmt.LayerID)
	require.Equal(t, forkCmt.ID, featBranch.HeadCommitID)

	_, err = disk.GetAttr(ctx, "feat-orphan-only", baseFile.Ino)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetAttr(ctx, "feat-orphan-only", tmpInWork.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	stillBase, err := disk.Read(ctx, DefaultBranchName, baseFile.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("ancestor-data-tail"), stillBase)
	stillTmp, err := disk.Read(ctx, DefaultBranchName, tmpInWork.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("work-only-tmp"), stillTmp)

	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, baseFile.Ino))
	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, tmpInWork.Ino))
	_, err = disk.GetAttr(ctx, DefaultBranchName, baseFile.Ino)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetAttr(ctx, DefaultBranchName, tmpInWork.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	cmt1Data, err := disk.ReadAtCommit(ctx, cmt1.ID, baseFile.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("ancestor-data"), cmt1Data)
}

func TestStep21_MountBranch_AtomicOrphanSweep_AndGCReclaimsOrphanPacks(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:         t.TempDir(),
		DiskID:      "step21-mount-gc",
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	tmp1, err := disk.CreateTmpfile(ctx, DefaultBranchName, RootIno, 0o600, 0, 0, false)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, tmp1.Ino, 0, bytes.Repeat([]byte("O"), 8192))
	require.NoError(t, err)

	liveFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "keep.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, liveFile.Ino, 0, bytes.Repeat([]byte("K"), 4096))
	require.NoError(t, err)

	cmt, err := disk.Commit(ctx, DefaultBranchName, "seal keep without orphan")
	require.NoError(t, err)
	_, err = disk.GetAttrAtCommit(ctx, cmt.ID, tmp1.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	roMountRes, err := disk.MountBranch(ctx, DefaultBranchName, true, false, time.Minute)
	require.NoError(t, err)
	roMount := roMountRes.Mount
	require.True(t, roMount.ReadOnly)
	_, err = disk.GetAttr(ctx, DefaultBranchName, tmp1.Ino)
	require.NoError(t, err)

	rwMountRes, err := disk.MountBranch(ctx, DefaultBranchName, false, false, time.Minute)
	require.NoError(t, err)
	rwMount := rwMountRes.Mount
	require.False(t, rwMount.ReadOnly)
	require.Equal(t, uint64(2), rwMount.WriterEpoch)

	_, err = disk.GetAttr(ctx, DefaultBranchName, tmp1.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, disk.packs.SealActive(ctx))
	stats, err := disk.RunGC(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, stats.PacksDeleted, 1)
	require.GreaterOrEqual(t, stats.BytesReclaimed, uint64(8192))

	keepData, err := disk.Read(ctx, DefaultBranchName, liveFile.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte("K"), 4096), keepData)
}

func TestStep22_NormalPOSIXWrites_SkipPackAndWALSync(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step22-nosync",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	var packSyncs atomic.Int32
	var dirSyncs atomic.Int32
	var walSyncs atomic.Int32
	var walNoSyncs atomic.Int32

	disk.packs.syncFileHook = func(_ uint64) error {
		packSyncs.Add(1)
		return nil
	}
	disk.packs.syncDirHook = func(_ string) error {
		dirSyncs.Add(1)
		return nil
	}
	disk.walCommitHook = func(sync bool) {
		if sync {
			walSyncs.Add(1)
		} else {
			walNoSyncs.Add(1)
		}
	}

	dir, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "d", 0o755, 0, 0)
	require.NoError(t, err)
	f, err := disk.Create(ctx, DefaultBranchName, dir.Ino, "a.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("A"), 4096))
	require.NoError(t, err)
	_, err = disk.Mknod(ctx, DefaultBranchName, dir.Ino, "fifo", ModeFIFO|0o644, 0, 0, 0)
	require.NoError(t, err)
	_, err = disk.Symlink(ctx, DefaultBranchName, dir.Ino, "sym", "/a.bin", 0, 0)
	require.NoError(t, err)
	_, err = disk.Link(ctx, DefaultBranchName, f.Ino, dir.Ino, "a-link.bin")
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "user.k", []byte("v"), 0))
	require.NoError(t, disk.RemoveXattr(ctx, DefaultBranchName, f.Ino, "user.k"))
	mode := uint32(0o600)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Mode: &mode})
	require.NoError(t, err)
	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 8192, 0))
	tmp, err := disk.CreateTmpfile(ctx, DefaultBranchName, dir.Ino, 0o600, 0, 0, false)
	require.NoError(t, err)
	require.NoError(t, disk.PurgeOrphanInode(ctx, DefaultBranchName, tmp.Ino))
	require.NoError(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: dir.Ino,
		OldName:      "a-link.bin",
		NewParentIno: dir.Ino,
		NewName:      "a-moved.bin",
	}))
	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, dir.Ino, "a-moved.bin", false))
	emptyDir, err := disk.Mkdir(ctx, DefaultBranchName, dir.Ino, "sub", 0o755, 0, 0)
	require.NoError(t, err)
	require.NoError(t, disk.Rmdir(ctx, DefaultBranchName, dir.Ino, "sub"))
	_ = emptyDir

	require.Equal(t, int32(0), packSyncs.Load())
	require.Equal(t, int32(0), dirSyncs.Load())
	require.Equal(t, int32(0), walSyncs.Load())
	require.Positive(t, walNoSyncs.Load())
	require.True(t, disk.packs.hasDirty())
}

func TestStep22_SyncBarrier_StrictDataBeforeWALOrdering_AllOps(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		run  func(t *testing.T, disk *DiskStore, fileIno uint64)
	}{
		{
			name: "Fsync",
			run: func(t *testing.T, disk *DiskStore, fileIno uint64) {
				require.NoError(t, disk.Fsync(ctx, DefaultBranchName, fileIno))
			},
		},
		{
			name: "FsyncDir",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				require.NoError(t, disk.FsyncDir(ctx, DefaultBranchName, RootIno))
			},
		},
		{
			name: "Commit",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				_, err := disk.Commit(ctx, DefaultBranchName, "barrier commit")
				require.NoError(t, err)
			},
		},
		{
			name: "Fork",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				_, _, err := disk.Fork(ctx, DefaultBranchName, "forked", "barrier fork")
				require.NoError(t, err)
			},
		},
		{
			name: "MountBranch",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				_, err := disk.MountBranch(ctx, DefaultBranchName, false, false, time.Minute)
				require.NoError(t, err)
			},
		},
		{
			name: "UpdateDiskMeta",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				newName := "updated-disk"
				_, err := disk.UpdateDiskMeta(ctx, &newName, nil)
				require.NoError(t, err)
			},
		},
		{
			name: "Unmount",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				require.NoError(t, disk.Unmount(ctx, "missing-mount-id"))
			},
		},
		{
			name: "Reset",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				_, err := disk.Reset(ctx, DefaultBranchName, 1)
				require.NoError(t, err)
			},
		},
		{
			name: "DeleteBranch",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				_, _, err := disk.Fork(ctx, DefaultBranchName, "tmp-del", "fork before delete")
				require.NoError(t, err)
				extra, err := disk.Create(ctx, DefaultBranchName, RootIno, "dirty-again.bin", 0o644, 0, 0)
				require.NoError(t, err)
				_, err = disk.Write(ctx, DefaultBranchName, extra.Ino, 0, bytes.Repeat([]byte("D"), 4096))
				require.NoError(t, err)
				require.NoError(t, disk.DeleteBranch(ctx, "tmp-del"))
			},
		},
		{
			name: "PruneCommits",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				cmt, err := disk.Commit(ctx, DefaultBranchName, "to-prune")
				require.NoError(t, err)
				_, err = disk.Reset(ctx, DefaultBranchName, 1)
				require.NoError(t, err)
				extra, err := disk.Create(ctx, DefaultBranchName, RootIno, "dirty-prune.bin", 0o644, 0, 0)
				require.NoError(t, err)
				_, err = disk.Write(ctx, DefaultBranchName, extra.Ino, 0, bytes.Repeat([]byte("P"), 4096))
				require.NoError(t, err)
				n, err := disk.PruneCommits(ctx)
				require.NoError(t, err)
				require.GreaterOrEqual(t, n, 1)
				_ = cmt
			},
		},
		{
			name: "Close",
			run: func(t *testing.T, disk *DiskStore, _ uint64) {
				require.NoError(t, disk.Close())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			disk, err := OpenDisk(DiskOptions{
				Dir:       t.TempDir(),
				DiskID:    "step22-order-" + tc.name,
				BlockSize: 4096,
			})
			require.NoError(t, err)
			defer func() {
				if tc.name != "Close" {
					require.NoError(t, disk.Close())
				}
			}()

			f, err := disk.Create(ctx, DefaultBranchName, RootIno, "data.bin", 0o644, 0, 0)
			require.NoError(t, err)
			_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("Z"), 4096))
			require.NoError(t, err)
			require.True(t, disk.packs.hasDirty())

			var rec syncEventLog
			disk.packs.syncFileHook = func(_ uint64) error {
				rec.add("pack_sync")
				return nil
			}
			disk.walCommitHook = func(sync bool) {
				if sync {
					rec.add("wal_sync")
				}
			}

			tc.run(t, disk, f.Ino)

			events := rec.snapshot()
			require.GreaterOrEqual(t, len(events), 2)
			require.Equal(t, []string{"pack_sync", "wal_sync"}, events[len(events)-2:])
			require.False(t, disk.packs.hasDirty())
		})
	}
}

func TestStep22_PackSyncFailure_AbortsTickWithoutCommittingWAL(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "step22-fail-abort",
		BlockSize: 4096,
	})
	require.NoError(t, err)

	injectedErr := errors.New("injected fdatasync I/O error")
	var failPackSync atomic.Bool
	defer func() {
		failPackSync.Store(false)
		require.NoError(t, disk.Close())
	}()

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "seed.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("S"), 4096))
	require.NoError(t, err)

	failPackSync.Store(true)
	var walSyncCalls atomic.Int32

	disk.packs.syncFileHook = func(_ uint64) error {
		if failPackSync.Load() {
			return injectedErr
		}
		return nil
	}
	disk.walCommitHook = func(sync bool) {
		if sync {
			walSyncCalls.Add(1)
		}
	}

	var gateArmed atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	disk.beforeTickHook = func() {
		if gateArmed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
	}

	gateArmed.Store(true)
	var wg sync.WaitGroup
	var gateErr, createErr, fsyncErr error

	wg.Go(func() {
		_, gateErr = disk.Create(ctx, DefaultBranchName, RootIno, "gate.txt", 0o644, 0, 0)
	})
	<-entered

	wg.Go(func() {
		_, createErr = disk.Create(ctx, DefaultBranchName, RootIno, "aborted.txt", 0o644, 0, 0)
	})
	wg.Go(func() {
		fsyncErr = disk.Fsync(ctx, DefaultBranchName, f.Ino)
	})

	actor, releaseRef, err := disk.acquireBranchActor(DefaultBranchName)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		actor.mb.mu.Lock()
		n := len(actor.mb.active)
		actor.mb.mu.Unlock()
		return n >= 2
	}, 2*time.Second, time.Millisecond)
	releaseRef()

	close(release)
	wg.Wait()

	require.NoError(t, gateErr)
	require.ErrorIs(t, createErr, injectedErr)
	require.ErrorIs(t, fsyncErr, injectedErr)
	require.Equal(t, int32(0), walSyncCalls.Load())
	require.True(t, disk.packs.hasDirty())

	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "aborted.txt")
	require.ErrorIs(t, err, ErrNotFound)

	failPackSync.Store(false)
	require.NoError(t, disk.Fsync(ctx, DefaultBranchName, f.Ino))
	require.Equal(t, int32(1), walSyncCalls.Load())
	require.False(t, disk.packs.hasDirty())
}

func TestSliceH_Mknod_Readlink_ISGIDInheritance(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-mknod-readlink",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	sgidDir, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "sgid", ModeSetGID|0o755, 10, 42)
	require.NoError(t, err)
	subDir, err := disk.Mkdir(ctx, DefaultBranchName, sgidDir.Ino, "sub", 0o755, 10, 99)
	require.NoError(t, err)
	require.Equal(t, uint32(42), subDir.GID)
	require.NotZero(t, subDir.Mode&ModeSetGID)

	reg, err := disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "reg", ModeRegular|0o640, 10, 99, 123)
	require.NoError(t, err)
	require.Equal(t, ModeRegular|0o640, reg.Mode)
	require.Equal(t, uint32(42), reg.GID)
	require.Equal(t, uint32(0), reg.Rdev)

	fifo, err := disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "pipe", ModeFIFO|0o644, 10, 99, 123)
	require.NoError(t, err)
	require.Equal(t, ModeFIFO|0o644, fifo.Mode)
	require.Equal(t, uint32(0), fifo.Rdev)

	chr, err := disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "null", ModeChar|0o666, 0, 0, 0x0103)
	require.NoError(t, err)
	require.Equal(t, ModeChar|0o666, chr.Mode)
	require.Equal(t, uint32(0x0103), chr.Rdev)

	blk, err := disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "sda", ModeBlock|0o660, 0, 0, 0x0800)
	require.NoError(t, err)
	require.Equal(t, ModeBlock|0o660, blk.Mode)
	require.Equal(t, uint32(0x0800), blk.Rdev)

	sock, err := disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "sock", ModeSocket|0o755, 0, 0, 999)
	require.NoError(t, err)
	require.Equal(t, ModeSocket|0o755, sock.Mode)
	require.Equal(t, uint32(0), sock.Rdev)

	_, err = disk.Mknod(ctx, DefaultBranchName, sgidDir.Ino, "bad", ModeDir|0o755, 0, 0, 0)
	require.ErrorIs(t, err, ErrInvalid)

	sym, err := disk.Symlink(ctx, DefaultBranchName, sgidDir.Ino, "link", "/target/path", 10, 99)
	require.NoError(t, err)
	require.Equal(t, uint32(42), sym.GID)

	target, err := disk.Readlink(ctx, DefaultBranchName, sym.Ino)
	require.NoError(t, err)
	require.Equal(t, []byte("/target/path"), target)

	_, err = disk.Readlink(ctx, DefaultBranchName, reg.Ino)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = disk.Readlink(ctx, DefaultBranchName, RootIno)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestSliceH_Fsync_FsyncDir_ValidationAndBarriers(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-fsync",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "f.txt", 0o644, 0, 0)
	require.NoError(t, err)

	require.NoError(t, disk.Fsync(ctx, DefaultBranchName, f.Ino))
	require.ErrorIs(t, disk.Fsync(ctx, DefaultBranchName, RootIno), ErrIsDir)
	require.ErrorIs(t, disk.Fsync(ctx, DefaultBranchName, 99999), ErrNotFound)

	require.NoError(t, disk.FsyncDir(ctx, DefaultBranchName, RootIno))
	require.ErrorIs(t, disk.FsyncDir(ctx, DefaultBranchName, f.Ino), ErrNotDir)
	require.ErrorIs(t, disk.FsyncDir(ctx, DefaultBranchName, 99999), ErrNotFound)
}

func TestSliceH_ReadDirPage_And_ReadDirPlus_Pagination(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-readdir",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	sub, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "dir", 0o755, 0, 0)
	require.NoError(t, err)
	fC, err := disk.Create(ctx, DefaultBranchName, sub.Ino, "c.txt", 0o644, 0, 0)
	require.NoError(t, err)
	fA, err := disk.Create(ctx, DefaultBranchName, sub.Ino, "a.txt", 0o640, 0, 0)
	require.NoError(t, err)
	fB, err := disk.Create(ctx, DefaultBranchName, sub.Ino, "b.txt", 0o600, 0, 0)
	require.NoError(t, err)

	var allNames []string
	var offset uint64
	for {
		page, pageErr := disk.ReadDirPage(ctx, DefaultBranchName, sub.Ino, offset, 2)
		require.NoError(t, pageErr)
		if len(page) == 0 {
			break
		}
		for _, ent := range page {
			allNames = append(allNames, ent.Name)
			offset = ent.NextOffset
		}
	}
	require.Equal(t, []string{".", "..", "a.txt", "b.txt", "c.txt"}, allNames)

	page0, err := disk.ReadDirPage(ctx, DefaultBranchName, sub.Ino, 0, 2)
	require.NoError(t, err)
	require.Len(t, page0, 2)
	require.Equal(t, ".", page0[0].Name)
	require.Equal(t, sub.Ino, page0[0].Ino)
	require.Equal(t, uint64(1), page0[0].NextOffset)
	require.Equal(t, "..", page0[1].Name)
	require.Equal(t, RootIno, page0[1].Ino)
	require.Equal(t, uint64(2), page0[1].NextOffset)

	plus, err := disk.ReadDirPlus(ctx, DefaultBranchName, sub.Ino, 2, 10)
	require.NoError(t, err)
	require.Len(t, plus, 3)
	require.Equal(t, "a.txt", plus[0].Name)
	require.Equal(t, fA.Ino, plus[0].Inode.Ino)
	require.Equal(t, uint64(3), plus[0].NextOffset)
	require.Equal(t, "b.txt", plus[1].Name)
	require.Equal(t, fB.Ino, plus[1].Inode.Ino)
	require.Equal(t, uint64(4), plus[1].NextOffset)
	require.Equal(t, "c.txt", plus[2].Name)
	require.Equal(t, fC.Ino, plus[2].Inode.Ino)
	require.Equal(t, uint64(5), plus[2].NextOffset)

	_, err = disk.ReadDirPage(ctx, DefaultBranchName, fA.Ino, 0, 10)
	require.ErrorIs(t, err, ErrNotDir)
	_, err = disk.ReadDirPage(ctx, DefaultBranchName, sub.Ino, 0, 0)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestSliceH_Fallocate_Modes_ZeroElision_Flags_AndQuota(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-fallocate",
		BlockSize: 4096,
		MaxBytes:  128 << 10,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "alloc.bin", ModeSetUID|ModeSetGID|0o755, 0, 0)
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "security.capability", []byte{1, 2}, 0))

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 1<<30, 0))
	attr, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(1<<30), attr.Size)
	_, err = disk.GetXattr(ctx, DefaultBranchName, f.Ino, "security.capability")
	require.ErrorIs(t, err, ErrNoData)

	shrink := uint64(0)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Size: &shrink})
	require.NoError(t, err)

	suidMode := ModeSetUID | ModeSetGID | 0o755
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Mode: &suidMode})
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "security.capability", []byte{9}, 0))

	payload := bytes.Repeat([]byte("X"), 3*4096)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, payload)
	require.NoError(t, err)

	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Mode: &suidMode})
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "security.capability", []byte{9}, 0))

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 2048, 4096+2048+1024, FallocPunchHole|FallocKeepSize))
	afterPunch, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(3*4096), afterPunch.Size)
	require.Zero(t, afterPunch.Mode&(ModeSetUID|ModeSetGID))
	_, err = disk.GetXattr(ctx, DefaultBranchName, f.Ino, "security.capability")
	require.ErrorIs(t, err, ErrNoData)

	got, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 3*4096)
	require.NoError(t, err)
	want := make([]byte, 3*4096)
	copy(want, payload)
	clear(want[2048 : 2048+4096+2048+1024])
	require.Equal(t, want, got)

	br, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	_, hasMidBlk, err := overlayGetDataRef(disk.db, []uint64{br.WorkLayerID}, f.Ino, 1)
	require.NoError(t, err)
	require.False(t, hasMidBlk)

	bytesBeforeZeroElide := disk.packs.PhysicalBytes()
	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 2048, FallocZeroRange|FallocKeepSize))
	require.Equal(t, bytesBeforeZeroElide, disk.packs.PhysicalBytes())
	_, hasFirstBlk, err := overlayGetDataRef(disk.db, []uint64{br.WorkLayerID}, f.Ino, 0)
	require.NoError(t, err)
	require.False(t, hasFirstBlk)

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 3*4096, 4096, FallocZeroRange))
	afterZeroGrow, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(4*4096), afterZeroGrow.Size)

	require.NoError(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 4*4096, 4096, FallocKeepSize))
	afterKeep, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(4*4096), afterKeep.Size)

	require.ErrorIs(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 10, FallocPunchHole), ErrInvalid)
	require.ErrorIs(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 10, 0x80), ErrNotSupported)
	require.ErrorIs(t, disk.Fallocate(ctx, DefaultBranchName, RootIno, 0, 10, 0), ErrIsDir)

	appFlag := FlagAppend
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Flags: &appFlag})
	require.NoError(t, err)
	require.ErrorIs(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 10, 0), ErrPerm)

	immFlag := FlagImmutable
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Flags: &immFlag})
	require.NoError(t, err)
	require.ErrorIs(t, disk.Fallocate(ctx, DefaultBranchName, f.Ino, 0, 10, 0), ErrPerm)
}

func TestSliceH_Rename_NoReplace_Exchange_CycleCheck_OpenTarget_AndHardlinkParents(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-rename",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	d1, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "d1", 0o755, 0, 0)
	require.NoError(t, err)
	d2, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "d2", 0o755, 0, 0)
	require.NoError(t, err)
	d1Child, err := disk.Mkdir(ctx, DefaultBranchName, d1.Ino, "child", 0o755, 0, 0)
	require.NoError(t, err)

	f1, err := disk.Create(ctx, DefaultBranchName, d1.Ino, "f1", 0o644, 0, 0)
	require.NoError(t, err)
	f2, err := disk.Create(ctx, DefaultBranchName, d2.Ino, "f2", 0o644, 0, 0)
	require.NoError(t, err)

	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "f2",
		Flags: RenameWhiteout,
	}), ErrInvalid)
	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "f2",
		Flags: RenameNoReplace | RenameExchange,
	}), ErrInvalid)
	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "f2",
		Flags: 0x10,
	}), ErrInvalid)

	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "f2",
		Flags: RenameNoReplace,
	}), ErrExist)

	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "missing",
		Flags: RenameExchange,
	}), ErrNotFound)

	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: RootIno, OldName: "d1",
		NewParentIno: d1Child.Ino, NewName: "sub",
		Flags: 0,
	}), ErrInvalid)

	fInChild, err := disk.Create(ctx, DefaultBranchName, d1Child.Ino, "leaf", 0o644, 0, 0)
	require.NoError(t, err)
	_ = fInChild
	require.ErrorIs(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1Child.Ino, OldName: "leaf",
		NewParentIno: RootIno, NewName: "d1",
		Flags: RenameExchange,
	}), ErrInvalid)

	_, err = disk.Link(ctx, DefaultBranchName, f1.Ino, RootIno, "f1-hl")
	require.NoError(t, err)
	require.NoError(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno: d1.Ino, OldName: "f1",
		NewParentIno: d2.Ino, NewName: "f2",
		Flags: RenameExchange,
	}))

	eOld, _, err := disk.Lookup(ctx, DefaultBranchName, d1.Ino, "f1")
	require.NoError(t, err)
	require.Equal(t, f2.Ino, eOld.Ino)
	eNew, f1AfterExch, err := disk.Lookup(ctx, DefaultBranchName, d2.Ino, "f2")
	require.NoError(t, err)
	require.Equal(t, f1.Ino, eNew.Ino)
	require.NotZero(t, f1AfterExch.Flags&InodeFlagEverLinked)

	br, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	_, closer, getErr := disk.db.Get(LayerParentKey(br.WorkLayerID, f1.Ino, d2.Ino))
	require.NoError(t, getErr)
	require.NoError(t, closer.Close())

	_, err = disk.Write(ctx, DefaultBranchName, f2.Ino, 0, []byte("open-victim"))
	require.NoError(t, err)
	f3, err := disk.Create(ctx, DefaultBranchName, RootIno, "replacer", 0o644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, disk.Rename(ctx, DefaultBranchName, RenameReq{
		OldParentIno:       RootIno,
		OldName:            "replacer",
		NewParentIno:       d1.Ino,
		NewName:            "f1",
		TargetOpenInClient: true,
	}))
	eReplaced, _, err := disk.Lookup(ctx, DefaultBranchName, d1.Ino, "f1")
	require.NoError(t, err)
	require.Equal(t, f3.Ino, eReplaced.Ino)

	victimAttr, err := disk.GetAttr(ctx, DefaultBranchName, f2.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(0), victimAttr.Nlink)
	victimData, err := disk.Read(ctx, DefaultBranchName, f2.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("open-victim"), victimData)
	kind, hasOrphan, err := readLayerOrphanKind(disk.db, br.WorkLayerID, f2.Ino)
	require.NoError(t, err)
	require.True(t, hasOrphan)
	require.Equal(t, OrphanUnlinked, kind)
}

func TestSliceH_SetAttr_Flags_Timestamps_ClearSUID_AndCapability(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-setattr",
		BlockSize: 4096,
		Now:       func() time.Time { return now },
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "bin", ModeSetUID|ModeSetGID|0o755, 0, 0)
	require.NoError(t, err)
	_, err = disk.Link(ctx, DefaultBranchName, f.Ino, RootIno, "bin-hl")
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "security.capability", []byte{7}, 0))

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("abcdef"))
	require.NoError(t, err)
	afterWrite, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Zero(t, afterWrite.Mode&(ModeSetUID|ModeSetGID))
	_, err = disk.GetXattr(ctx, DefaultBranchName, f.Ino, "security.capability")
	require.ErrorIs(t, err, ErrNoData)

	suidMode := ModeSetUID | ModeSetGID | 0o755
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Mode: &suidMode})
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "security.capability", []byte{8}, 0))

	shrink := uint64(3)
	afterTrunc, err := disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Size: &shrink})
	require.NoError(t, err)
	require.Zero(t, afterTrunc.Mode&(ModeSetUID|ModeSetGID))
	_, err = disk.GetXattr(ctx, DefaultBranchName, f.Ino, "security.capability")
	require.ErrorIs(t, err, ErrNoData)

	now = now.Add(time.Minute)
	tsAttr, err := disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{AtimeNow: true, MtimeNow: true, CtimeNow: true})
	require.NoError(t, err)
	require.Equal(t, now.UnixNano(), tsAttr.Atime)
	require.Equal(t, now.UnixNano(), tsAttr.Mtime)
	require.Equal(t, now.UnixNano(), tsAttr.Ctime)

	appFlag := FlagAppend
	afterApp, err := disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Flags: &appFlag})
	require.NoError(t, err)
	require.NotZero(t, afterApp.Flags&FlagAppend)
	require.NotZero(t, afterApp.Flags&InodeFlagEverLinked)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("x"))
	require.ErrorIs(t, err, ErrPerm)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Size: &shrink})
	require.ErrorIs(t, err, ErrPerm)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 3, []byte("ok"))
	require.NoError(t, err)

	immFlag := FlagImmutable
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Flags: &immFlag})
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 5, []byte("no"))
	require.ErrorIs(t, err, ErrPerm)
	mode600 := uint32(0o600)
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Mode: &mode600})
	require.ErrorIs(t, err, ErrPerm)
	require.ErrorIs(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "bin", false), ErrPerm)

	clearFlags := uint32(0)
	afterClear, err := disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Flags: &clearFlags})
	require.NoError(t, err)
	require.Zero(t, afterClear.Flags&(FlagImmutable|FlagAppend))
	require.NotZero(t, afterClear.Flags&InodeFlagEverLinked)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 5, []byte("!"))
	require.NoError(t, err)

	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{})
	require.ErrorIs(t, err, ErrInvalid)
	explicit := now.UnixNano()
	_, err = disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Atime: &explicit, AtimeNow: true})
	require.ErrorIs(t, err, ErrInvalid)
}

func TestSliceH_Xattr_UserNamespaceRules_And64KiBQuota(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-xattr",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	sym, err := disk.Symlink(ctx, DefaultBranchName, RootIno, "sym", "/dst", 0, 0)
	require.NoError(t, err)
	fifo, err := disk.Mknod(ctx, DefaultBranchName, RootIno, "fifo", ModeFIFO|0o644, 0, 0, 0)
	require.NoError(t, err)
	dev, err := disk.Mknod(ctx, DefaultBranchName, RootIno, "dev", ModeChar|0o666, 0, 0, 0x0103)
	require.NoError(t, err)
	sock, err := disk.Mknod(ctx, DefaultBranchName, RootIno, "sock", ModeSocket|0o755, 0, 0, 0)
	require.NoError(t, err)

	for _, ino := range []uint64{sym.Ino, fifo.Ino, dev.Ino} {
		require.ErrorIs(t, disk.SetXattr(ctx, DefaultBranchName, ino, "user.k", []byte("v"), 0), ErrPerm)
		require.ErrorIs(t, disk.RemoveXattr(ctx, DefaultBranchName, ino, "user.k"), ErrPerm)
		_, err = disk.GetXattr(ctx, DefaultBranchName, ino, "user.k")
		require.ErrorIs(t, err, ErrNoData)
		require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, ino, "trusted.k", []byte("tv"), 0))
	}
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, sock.Ino, "user.ok", []byte("1"), 0))

	fQuota, err := disk.Create(ctx, DefaultBranchName, RootIno, "q.txt", 0o644, 0, 0)
	require.NoError(t, err)
	for i := range 256 {
		prefix := []byte{'u', 's', 'e', 'r', '.', byte('a' + (i / 26)), byte('a' + (i % 26)), '.'}
		name := make([]byte, MaxXattrNameBytes)
		copy(name, prefix)
		copy(name[len(prefix):], bytes.Repeat([]byte{'k'}, MaxXattrNameBytes-len(prefix)))
		require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, fQuota.Ino, string(name), []byte("v"), 0))
	}
	require.ErrorIs(t, disk.SetXattr(ctx, DefaultBranchName, fQuota.Ino, "user.overflow", []byte("v"), 0), ErrNoSpace)
}

func TestSliceH_ReadInto_And_ReadCommitNode_RegularSymlinkDir(t *testing.T) {
	ctx := context.Background()
	disk, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "slice-h-read",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disk.Close()) }()

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "hello.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("hello"))
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 4096, []byte("world"))
	require.NoError(t, err)

	dst := bytes.Repeat([]byte("D"), 8192)
	got, err := disk.ReadInto(ctx, DefaultBranchName, f.Ino, 0, 4101, dst[:0])
	require.NoError(t, err)
	require.Equal(t, &dst[0], &got[0])
	want := make([]byte, 4101)
	copy(want[0:], "hello")
	copy(want[4096:], "world")
	require.Equal(t, want, got)

	sym, err := disk.Symlink(ctx, DefaultBranchName, RootIno, "s", "/hello.txt", 0, 0)
	require.NoError(t, err)
	cmt, err := disk.Commit(ctx, DefaultBranchName, "snapshot node")
	require.NoError(t, err)

	part, err := disk.ReadCommitNode(ctx, cmt.ID, f.Ino, 0, 5)
	require.NoError(t, err)
	require.Equal(t, ModeRegular|0o644, part.Mode)
	require.Equal(t, uint64(4101), part.FileSize)
	require.Equal(t, []byte("hello"), part.Data)
	require.False(t, part.EOF)
	require.Empty(t, part.SymlinkTarget)

	tail, err := disk.ReadCommitNode(ctx, cmt.ID, f.Ino, 4096, 16)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), tail.Data)
	require.True(t, tail.EOF)

	symRes, err := disk.ReadCommitNode(ctx, cmt.ID, sym.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, ModeSymlink|0o777, symRes.Mode)
	require.Equal(t, uint64(len("/hello.txt")), symRes.FileSize)
	require.True(t, symRes.EOF)
	require.Equal(t, []byte("/hello.txt"), symRes.SymlinkTarget)

	_, err = disk.ReadCommitNode(ctx, cmt.ID, RootIno, 0, 64)
	require.ErrorIs(t, err, ErrIsDir)
}
