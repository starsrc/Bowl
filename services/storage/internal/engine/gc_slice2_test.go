package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGC_BoundedRelocation_CapsPerCycleAtFourRetiresAllZeroLiveAndDrainsViaTriggerGC(t *testing.T) {
	t.Parallel()

	t.Run("GCModeDisabled retires all zero-live packs and caps relocation at 4 per cycle", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-bounded-disabled",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		for i := range 3 {
			name := fmt.Sprintf("zero-dead-%d.bin", i)
			f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, name, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{byte(0x01 + i)}, 8192))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, name, false))
		}

		type liveEntry struct {
			ino     uint64
			payload []byte
		}
		liveFiles := make([]liveEntry, 0, 6)
		for i := range 6 {
			liveName := fmt.Sprintf("partial-live-%d.bin", i)
			liveInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, liveName, 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{byte(0x10 + i)}, 4096)
			_, wErr := disk.Write(ctx, DefaultBranchName, liveInode.Ino, 0, payload)
			require.NoError(t, wErr)
			liveFiles = append(liveFiles, liveEntry{ino: liveInode.Ino, payload: payload})

			dropName := fmt.Sprintf("partial-drop-%d.bin", i)
			dropInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, dropName, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{byte(0x80 + i)}, 4096))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, dropName, false))
		}

		sealInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal-full.bin", 0o644, 0, 0)
		require.NoError(t, err)
		sealPayload := bytes.Repeat([]byte{0xee}, 8192)
		_, err = disk.Write(ctx, DefaultBranchName, sealInode.Ino, 0, sealPayload)
		require.NoError(t, err)

		var flushCalls atomic.Int32
		disk.node.flushWALForGCHook = func() error {
			flushCalls.Add(1)
			return nil
		}

		stats1, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 7, stats1.PacksDeleted)
		require.Equal(t, 4, stats1.BlocksRelocated)
		require.Zero(t, stats1.BlocksSkipped)
		require.Equal(t, int32(1), flushCalls.Load())

		for packID := uint64(1); packID <= 7; packID++ {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		}
		for packID := uint64(8); packID <= 9; packID++ {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.NoError(t, statErr)
		}

		stats2, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, stats2.PacksDeleted)
		require.Equal(t, 2, stats2.BlocksRelocated)
		require.Zero(t, stats2.BlocksSkipped)

		for packID := uint64(8); packID <= 9; packID++ {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		}

		stats3, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Zero(t, stats3.PacksDeleted)
		require.Zero(t, stats3.BlocksRelocated)

		for _, entry := range liveFiles {
			got, rErr := disk.Read(ctx, DefaultBranchName, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}
		gotSeal, err := disk.Read(ctx, DefaultBranchName, sealInode.Ino, 0, len(sealPayload))
		require.NoError(t, err)
		require.Equal(t, sealPayload, gotSeal)
	})

	t.Run("GCModeWakeOnly drains all 6 partial packs across multiple passes via single TriggerGC", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:          t.TempDir(),
			DiskID:       "gc-bounded-wake-only",
			MaxBytes:     2 << 20,
			BlockSize:    4096,
			MaxPackSize:  8192,
			EnableGCWake: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		type liveEntry struct {
			ino     uint64
			payload []byte
		}
		liveFiles := make([]liveEntry, 0, 6)
		for i := range 6 {
			liveInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("wake-live-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{byte(0x30 + i)}, 4096)
			_, wErr := disk.Write(ctx, DefaultBranchName, liveInode.Ino, 0, payload)
			require.NoError(t, wErr)
			liveFiles = append(liveFiles, liveEntry{ino: liveInode.Ino, payload: payload})

			dropName := fmt.Sprintf("wake-drop-%d.bin", i)
			dropInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, dropName, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{byte(0x90 + i)}, 4096))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, dropName, false))
		}

		sealInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "wake-seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		sealPayload := bytes.Repeat([]byte{0xdd}, 8192)
		_, err = disk.Write(ctx, DefaultBranchName, sealInode.Ino, 0, sealPayload)
		require.NoError(t, err)

		physBefore := disk.PackPhysicalBytes()
		require.Equal(t, uint64(7*8192), physBefore)

		var (
			relocMu       sync.Mutex
			relocatedPack []uint64
		)
		disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
			relocMu.Lock()
			relocatedPack = append(relocatedPack, packID)
			relocMu.Unlock()
			return nil
		}

		disk.TriggerGC()

		expectedFinalPhysical := uint64(4 * 8192)
		require.Eventually(t, func() bool {
			return disk.PackPhysicalBytes() == expectedFinalPhysical
		}, 5*time.Second, 10*time.Millisecond)

		relocMu.Lock()
		seenPacks := append([]uint64(nil), relocatedPack...)
		relocMu.Unlock()
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, seenPacks)

		for packID := uint64(1); packID <= 6; packID++ {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		}

		for _, entry := range liveFiles {
			got, rErr := disk.Read(ctx, DefaultBranchName, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}
		gotSeal, err := disk.Read(ctx, DefaultBranchName, sealInode.Ino, 0, len(sealPayload))
		require.NoError(t, err)
		require.Equal(t, sealPayload, gotSeal)
	})

	t.Run("error on capped run stops retrigger", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:          t.TempDir(),
			DiskID:       "gc-bounded-err-no-spin",
			MaxBytes:     2 << 20,
			BlockSize:    4096,
			MaxPackSize:  8192,
			EnableGCWake: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		for i := range 6 {
			liveInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("err-live-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr := disk.Write(ctx, DefaultBranchName, liveInode.Ino, 0, bytes.Repeat([]byte{byte(0x40 + i)}, 4096))
			require.NoError(t, wErr)

			dropName := fmt.Sprintf("err-drop-%d.bin", i)
			dropInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, dropName, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{byte(0xa0 + i)}, 4096))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, dropName, false))
		}

		sealInode, err := disk.Create(ctx, DefaultBranchName, RootIno, "err-seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, sealInode.Ino, 0, bytes.Repeat([]byte{0xcc}, 8192))
		require.NoError(t, err)

		injectedErr := errors.New("injected relocate hook failure")
		var pack1Attempts atomic.Int32
		disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
			if packID == 1 {
				pack1Attempts.Add(1)
				return injectedErr
			}
			return nil
		}

		stats, err := disk.RunGC(ctx)
		require.ErrorIs(t, err, injectedErr)
		require.Equal(t, 3, stats.PacksDeleted)
		require.Equal(t, int32(1), pack1Attempts.Load())

		require.Never(t, func() bool {
			return pack1Attempts.Load() > 1
		}, 150*time.Millisecond, 10*time.Millisecond)

		for _, packID := range []uint64{1, 5, 6} {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.NoError(t, statErr)
		}
	})

	t.Run("active pack relocation shares 4-pack cap and zero-live active pack uses 0 slots", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-bounded-active-cap",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		type liveEntry struct {
			ino     uint64
			payload []byte
		}
		liveFiles := make([]liveEntry, 0, 6)
		for i := range 6 {
			liveInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("act-live-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{byte(0x50 + i)}, 4096)
			_, wErr := disk.Write(ctx, DefaultBranchName, liveInode.Ino, 0, payload)
			require.NoError(t, wErr)
			liveFiles = append(liveFiles, liveEntry{ino: liveInode.Ino, payload: payload})

			dropName := fmt.Sprintf("act-drop-%d.bin", i)
			dropInode, cErr := disk.Create(ctx, DefaultBranchName, RootIno, dropName, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr = disk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{byte(0xb0 + i)}, 4096))
			require.NoError(t, wErr)
			require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, dropName, false))
		}

		stats1, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 4, stats1.PacksDeleted)
		require.Equal(t, 4, stats1.BlocksRelocated)

		for _, packID := range []uint64{6, 1, 2, 3} {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.ErrorIs(t, statErr, os.ErrNotExist)
		}
		for _, packID := range []uint64{4, 5} {
			_, statErr := os.Stat(packFilePath(disk.packs.dir, packID))
			require.NoError(t, statErr)
		}

		stats2, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, stats2.PacksDeleted)
		require.Equal(t, 2, stats2.BlocksRelocated)

		for _, entry := range liveFiles {
			got, rErr := disk.Read(ctx, DefaultBranchName, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}

		zeroDisk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-bounded-active-zero-slot",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 8192,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, zeroDisk.Close())
		})

		for i := range 4 {
			liveInode, cErr := zeroDisk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("z-live-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr := zeroDisk.Write(ctx, DefaultBranchName, liveInode.Ino, 0, bytes.Repeat([]byte{byte(0x60 + i)}, 4096))
			require.NoError(t, wErr)

			dropName := fmt.Sprintf("z-drop-%d.bin", i)
			dropInode, cErr := zeroDisk.Create(ctx, DefaultBranchName, RootIno, dropName, 0o644, 0, 0)
			require.NoError(t, cErr)
			_, wErr = zeroDisk.Write(ctx, DefaultBranchName, dropInode.Ino, 0, bytes.Repeat([]byte{byte(0xc0 + i)}, 4096))
			require.NoError(t, wErr)
			require.NoError(t, zeroDisk.Unlink(ctx, DefaultBranchName, RootIno, dropName, false))
		}

		zeroActive, err := zeroDisk.Create(ctx, DefaultBranchName, RootIno, "z-active-dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = zeroDisk.Write(ctx, DefaultBranchName, zeroActive.Ino, 0, bytes.Repeat([]byte{0xf0}, 4096))
		require.NoError(t, err)
		require.NoError(t, zeroDisk.Unlink(ctx, DefaultBranchName, RootIno, "z-active-dead.bin", false))

		zeroStats, err := zeroDisk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 5, zeroStats.PacksDeleted)
		require.Equal(t, 4, zeroStats.BlocksRelocated)
	})
}

func TestGC_CommitLayerCoalescing_SingleWALSyncPerPackAndSkippedDeletedCommit(t *testing.T) {
	t.Parallel()

	t.Run("5 commit layers plus 1 working branch coalesce into 2 sync commits", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-coalesce-5c-1w",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 12 * 4096,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		type commitEntry struct {
			commitID uint64
			ino      uint64
			payload  []byte
		}
		commits := make([]commitEntry, 0, 5)
		for i := range 5 {
			f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("cmt-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{byte(0x20 + i)}, 4096)
			_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, payload)
			require.NoError(t, wErr)
			cmt, cmtErr := disk.Commit(ctx, DefaultBranchName, fmt.Sprintf("commit-%d", i))
			require.NoError(t, cmtErr)
			commits = append(commits, commitEntry{commitID: cmt.ID, ino: f.Ino, payload: payload})
		}

		workFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "work.bin", 0o644, 0, 0)
		require.NoError(t, err)
		workPayload := bytes.Repeat([]byte{0x70}, 4096)
		_, err = disk.Write(ctx, DefaultBranchName, workFile.Ino, 0, workPayload)
		require.NoError(t, err)

		deadFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, deadFile.Ino, 0, bytes.Repeat([]byte{0x99}, 6*4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		sealFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, sealFile.Ino, 0, bytes.Repeat([]byte{0xaa}, 12*4096))
		require.NoError(t, err)

		var syncCommits atomic.Int32
		disk.walCommitHook = func(isSync bool) {
			if isSync {
				syncCommits.Add(1)
			}
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.PacksDeleted)
		require.Equal(t, 6, stats.BlocksRelocated)
		require.Zero(t, stats.BlocksSkipped)
		require.Equal(t, int32(2), syncCommits.Load())

		for _, entry := range commits {
			got, rErr := disk.ReadAtCommit(ctx, entry.commitID, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}
		gotWork, err := disk.Read(ctx, DefaultBranchName, workFile.Ino, 0, len(workPayload))
		require.NoError(t, err)
		require.Equal(t, workPayload, gotWork)
	})

	t.Run("5 commit layers with 0 working layers coalesce into 1 sync commit", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-coalesce-5c-0w",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 10 * 4096,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		type commitEntry struct {
			commitID uint64
			ino      uint64
			payload  []byte
		}
		commits := make([]commitEntry, 0, 5)
		for i := range 5 {
			f, cErr := disk.Create(ctx, DefaultBranchName, RootIno, fmt.Sprintf("only-cmt-%d.bin", i), 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{byte(0x30 + i)}, 4096)
			_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, payload)
			require.NoError(t, wErr)
			cmt, cmtErr := disk.Commit(ctx, DefaultBranchName, fmt.Sprintf("only-commit-%d", i))
			require.NoError(t, cmtErr)
			commits = append(commits, commitEntry{commitID: cmt.ID, ino: f.Ino, payload: payload})
		}

		deadFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, deadFile.Ino, 0, bytes.Repeat([]byte{0x98}, 5*4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		sealFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, sealFile.Ino, 0, bytes.Repeat([]byte{0xab}, 10*4096))
		require.NoError(t, err)

		var syncCommits atomic.Int32
		disk.walCommitHook = func(isSync bool) {
			if isSync {
				syncCommits.Add(1)
			}
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.PacksDeleted)
		require.Equal(t, 5, stats.BlocksRelocated)
		require.Zero(t, stats.BlocksSkipped)
		require.Equal(t, int32(1), syncCommits.Load())

		for _, entry := range commits {
			got, rErr := disk.ReadAtCommit(ctx, entry.commitID, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}
	})

	t.Run("deleted commit during beforeGCRelocateHook skips cleanly coalesces remaining and flushes WAL before retire", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		disk, err := openTestDisk(testDiskOptions{
			Dir:         t.TempDir(),
			DiskID:      "gc-coalesce-deleted-cmt",
			MaxBytes:    2 << 20,
			BlockSize:   4096,
			MaxPackSize: 10 * 4096,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, disk.Close())
		})

		type commitEntry struct {
			commitID uint64
			ino      uint64
			payload  []byte
		}
		keptCommits := make([]commitEntry, 0, 4)

		writeAndCommit := func(branch, name string, fill byte) commitEntry {
			f, cErr := disk.Create(ctx, branch, RootIno, name, 0o644, 0, 0)
			require.NoError(t, cErr)
			payload := bytes.Repeat([]byte{fill}, 4096)
			_, wErr := disk.Write(ctx, branch, f.Ino, 0, payload)
			require.NoError(t, wErr)
			cmt, cmtErr := disk.Commit(ctx, branch, name)
			require.NoError(t, cmtErr)
			return commitEntry{commitID: cmt.ID, ino: f.Ino, payload: payload}
		}

		keptCommits = append(keptCommits, writeAndCommit(DefaultBranchName, "k0.bin", 0x41))
		c1 := writeAndCommit(DefaultBranchName, "k1.bin", 0x42)
		keptCommits = append(keptCommits, c1)

		_, _, err = disk.Fork(ctx, DefaultBranchName, "ephemeral", "fork ephemeral")
		require.NoError(t, err)
		deletedCommit := writeAndCommit("ephemeral", "gone.bin", 0x43)

		keptCommits = append(
			keptCommits,
			writeAndCommit(DefaultBranchName, "k3.bin", 0x44),
			writeAndCommit(DefaultBranchName, "k4.bin", 0x45),
		)

		deadFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "dead.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, deadFile.Ino, 0, bytes.Repeat([]byte{0x97}, 5*4096))
		require.NoError(t, err)
		require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "dead.bin", false))

		sealFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "seal.bin", 0o644, 0, 0)
		require.NoError(t, err)
		_, err = disk.Write(ctx, DefaultBranchName, sealFile.Ino, 0, bytes.Repeat([]byte{0xac}, 10*4096))
		require.NoError(t, err)

		var (
			inHook       atomic.Bool
			gcSyncCount  atomic.Int32
			flushCalls   atomic.Int32
			eventsMu     sync.Mutex
			events       []string
			deletedOnce  sync.Once
			deleteFailed error
		)

		disk.walCommitHook = func(isSync bool) {
			if isSync && !inHook.Load() {
				gcSyncCount.Add(1)
			}
		}
		disk.node.flushWALForGCHook = func() error {
			flushCalls.Add(1)
			eventsMu.Lock()
			events = append(events, "flushWAL")
			eventsMu.Unlock()
			return nil
		}
		disk.packs.removePackHook = func(packID uint64) error {
			if packID == 1 {
				eventsMu.Lock()
				events = append(events, "removePack1")
				eventsMu.Unlock()
			}
			return nil
		}
		disk.beforeGCRelocateHook = func(packID uint64, _ BlockRefLocation) error {
			if packID != 1 {
				return nil
			}
			deletedOnce.Do(func() {
				inHook.Store(true)
				defer inHook.Store(false)
				if _, rErr := disk.Reset(ctx, "ephemeral", c1.commitID); rErr != nil {
					deleteFailed = rErr
					return
				}
				if dErr := disk.DeleteCommit(ctx, deletedCommit.commitID); dErr != nil {
					deleteFailed = dErr
				}
			})
			return deleteFailed
		}

		stats, err := disk.RunGC(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, stats.PacksDeleted)
		require.Equal(t, 4, stats.BlocksRelocated)
		require.Equal(t, 1, stats.BlocksSkipped)
		require.Equal(t, int32(1), flushCalls.Load())
		require.Equal(t, int32(1), gcSyncCount.Load()-flushCalls.Load())

		eventsMu.Lock()
		gotEvents := append([]string(nil), events...)
		eventsMu.Unlock()
		require.Equal(t, []string{"flushWAL", "removePack1"}, gotEvents)

		_, err = disk.GetCommit(deletedCommit.commitID)
		require.ErrorIs(t, err, ErrNotFound)

		for _, entry := range keptCommits {
			got, rErr := disk.ReadAtCommit(ctx, entry.commitID, entry.ino, 0, len(entry.payload))
			require.NoError(t, rErr)
			require.Equal(t, entry.payload, got)
		}
	})
}
