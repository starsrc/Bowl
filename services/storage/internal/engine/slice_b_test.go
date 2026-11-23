package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep5_BranchActor_CreatedOnDemandAndEvictedSafelyWithoutClosingDisk(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	now := time.Unix(1700000000, 0)
	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-evict",
		BlockSize: 4096,
		Now:       func() time.Time { return now },
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	require.Empty(t, disk.ActiveBranchActors())

	root, err := disk.GetAttr(ctx, DefaultBranchName, RootIno)
	require.NoError(t, err)
	require.Equal(t, RootIno, root.Ino)
	require.Empty(t, disk.ActiveBranchActors())

	fileIno, err := disk.Create(ctx, DefaultBranchName, RootIno, "a.txt", 0o644, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []string{DefaultBranchName}, disk.ActiveBranchActors())

	now = now.Add(10 * time.Second)
	evicted := disk.EvictIdleActors(5 * time.Second)
	require.Equal(t, 1, evicted)
	require.Empty(t, disk.ActiveBranchActors())

	n, err := disk.Write(ctx, DefaultBranchName, fileIno.Ino, 0, []byte("after-evict"))
	require.NoError(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, []string{DefaultBranchName}, disk.ActiveBranchActors())

	got, err := disk.Read(ctx, DefaultBranchName, fileIno.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("after-evict"), got)
}

func TestStep3_POSIXOverlay_FileDirXattrUnlinkRenameAndPersistenceAcrossReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-posix",
		BlockSize:   4096,
		MaxPackSize: 16384,
	})
	require.NoError(t, err)

	subDir, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "docs", 0o755, 1000, 1000)
	require.NoError(t, err)
	require.Equal(t, ModeDir|0o755, subDir.Mode)

	f, err := disk.Create(ctx, DefaultBranchName, subDir.Ino, "report.bin", 0o644, 1000, 1000)
	require.NoError(t, err)

	chunk := bytes.Repeat([]byte("ABCD"), 1500)
	written, err := disk.Write(ctx, DefaultBranchName, f.Ino, 100, chunk)
	require.NoError(t, err)
	require.Equal(t, len(chunk), written)

	headZero, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 100)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 100), headZero)

	gotChunk, err := disk.Read(ctx, DefaultBranchName, f.Ino, 100, len(chunk))
	require.NoError(t, err)
	require.Equal(t, chunk, gotChunk)

	patch := []byte("PATCHED!")
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 4092, patch)
	require.NoError(t, err)
	gotPatch, err := disk.Read(ctx, DefaultBranchName, f.Ino, 4092, len(patch))
	require.NoError(t, err)
	require.Equal(t, patch, gotPatch)

	newSize := uint64(4096)
	attr, err := disk.SetAttr(ctx, DefaultBranchName, f.Ino, SetAttrReq{Size: &newSize})
	require.NoError(t, err)
	require.Equal(t, uint64(4096), attr.Size)

	gotTrunc, err := disk.Read(ctx, DefaultBranchName, f.Ino, 4092, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("PATC"), gotTrunc)

	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "user.author", []byte("alice"), XattrCreate))
	require.ErrorIs(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "user.author", []byte("bob"), XattrCreate), ErrExist)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "user.author", []byte("bob"), XattrReplace))
	xval, err := disk.GetXattr(ctx, DefaultBranchName, f.Ino, "user.author")
	require.NoError(t, err)
	require.Equal(t, []byte("bob"), xval)

	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, f.Ino, "user.empty", nil, 0))
	emptyVal, err := disk.GetXattr(ctx, DefaultBranchName, f.Ino, "user.empty")
	require.NoError(t, err)
	require.Empty(t, emptyVal)

	xnames, err := disk.ListXattr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, []string{"user.author", "user.empty"}, xnames)

	require.NoError(t, disk.RemoveXattr(ctx, DefaultBranchName, f.Ino, "user.empty"))
	_, err = disk.GetXattr(ctx, DefaultBranchName, f.Ino, "user.empty")
	require.ErrorIs(t, err, ErrNoData)

	sym, err := disk.Symlink(ctx, DefaultBranchName, RootIno, "link.txt", "docs/report.bin", 0, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("docs/report.bin"), sym.SymlinkTarget)

	linked, err := disk.Link(ctx, DefaultBranchName, f.Ino, RootIno, "hard.bin")
	require.NoError(t, err)
	require.Equal(t, uint32(2), linked.Nlink)

	require.NoError(t, disk.Rename(ctx, DefaultBranchName, subDir.Ino, "report.bin", RootIno, "moved.bin"))
	_, _, err = disk.Lookup(ctx, DefaultBranchName, subDir.Ino, "report.bin")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, disk.Rmdir(ctx, DefaultBranchName, RootIno, "docs"))
	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "docs")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "hard.bin"))
	stillAlive, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(1), stillAlive.Nlink)

	require.NoError(t, disk.Close())

	reopened, err := OpenDisk(DiskOptions{
		Dir:         dir,
		MaxPackSize: 16384,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	ent, inoAfter, err := reopened.Lookup(ctx, DefaultBranchName, RootIno, "moved.bin")
	require.NoError(t, err)
	require.Equal(t, f.Ino, ent.Ino)
	require.Equal(t, uint64(4096), inoAfter.Size)

	gotAfter, err := reopened.Read(ctx, DefaultBranchName, f.Ino, 4092, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("PATC"), gotAfter)

	xAfter, err := reopened.GetXattr(ctx, DefaultBranchName, f.Ino, "user.author")
	require.NoError(t, err)
	require.Equal(t, []byte("bob"), xAfter)

	entries, err := reopened.ReadDir(ctx, DefaultBranchName, RootIno)
	require.NoError(t, err)
	require.Equal(t, []string{"link.txt", "moved.bin"}, dirEntryNames(entries))
}

func TestStep3_OverlayReads_WalkLayerChainAndMaskTombstonesInReadDirAndXattr(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-overlay-mask",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	mainMeta, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	initCommit, err := disk.GetCommit(mainMeta.HeadCommitID)
	require.NoError(t, err)

	baseIno := Inode{
		Ino:   10,
		Mode:  ModeRegular | 0o644,
		Nlink: 1,
		Size:  5,
	}
	rawBaseIno, err := EncodeInode(baseIno)
	require.NoError(t, err)
	baseEntry := Entry{Ino: 10, Mode: ModeRegular | 0o644}

	batch := disk.db.NewBatch()
	require.NoError(t, batch.Set(LayerInodeKey(initCommit.LayerID, 10), EncodeLiveEnvelope(rawBaseIno), nil))
	require.NoError(t, batch.Set(LayerEntryKey(initCommit.LayerID, RootIno, []byte("from-base.txt")), EncodeLiveEnvelope(EncodeEntry(baseEntry)), nil))
	require.NoError(t, batch.Set(LayerXattrKey(initCommit.LayerID, 10, []byte("user.base")), EncodeLiveEnvelope([]byte("v1")), nil))
	require.NoError(t, batch.Set(LayerXattrKey(initCommit.LayerID, 10, []byte("user.drop")), EncodeLiveEnvelope([]byte("old")), nil))
	require.NoError(t, batch.Commit(nil))
	require.NoError(t, batch.Close())

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "in-work.txt", 0o644, 0, 0)
	require.NoError(t, err)

	entries, err := disk.ReadDir(ctx, DefaultBranchName, RootIno)
	require.NoError(t, err)
	require.Equal(t, []string{"from-base.txt", "in-work.txt"}, dirEntryNames(entries))

	require.NoError(t, disk.RemoveXattr(ctx, DefaultBranchName, 10, "user.drop"))
	xnames, err := disk.ListXattr(ctx, DefaultBranchName, 10)
	require.NoError(t, err)
	require.Equal(t, []string{"user.base"}, xnames)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "from-base.txt"))
	entriesAfter, err := disk.ReadDir(ctx, DefaultBranchName, RootIno)
	require.NoError(t, err)
	require.Equal(t, []string{"in-work.txt"}, dirEntryNames(entriesAfter))

	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "from-base.txt")
	require.ErrorIs(t, err, ErrNotFound)
}

func dirEntryNames(entries []DirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}
