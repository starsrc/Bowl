package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStep1_OpenDisk_InitializesSinglePebbleAndSharedPacksWithMainBranch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-alpha",
		MaxBytes:  16 << 20,
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	pebbleInfo, err := os.Stat(filepath.Join(dir, "pebble"))
	require.NoError(t, err)
	require.True(t, pebbleInfo.IsDir())

	packsInfo, err := os.Stat(filepath.Join(dir, "packs"))
	require.NoError(t, err)
	require.True(t, packsInfo.IsDir())

	meta := disk.DiskMeta()
	require.Equal(t, "disk-alpha", meta.DiskID)
	require.Equal(t, DefaultBranchName, meta.DefaultBranch)
	require.Equal(t, uint64(16<<20), meta.MaxBytes)
	require.Equal(t, uint32(4096), meta.BlockSize)

	mainBranch, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, DefaultBranchName, mainBranch.Name)
	require.Positive(t, mainBranch.HeadCommitID)
	require.Positive(t, mainBranch.WorkLayerID)
	require.Equal(t, uint64(1), mainBranch.WriterEpoch)

	branches, err := disk.ListBranches()
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.Equal(t, DefaultBranchName, branches[0].Name)
}

func TestStep1_OpenDisk_ReopenPersistsDiskMetaAndRecoversPackAccounting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-persist",
		MaxBytes:    64 << 10,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)

	payload := bytes.Repeat([]byte{0x5a}, 4096)
	refs, err := disk.packs.AppendBlocks(ctx, [][]byte{payload})
	require.NoError(t, err)
	require.Len(t, refs, 1)
	disk.packs.ReleaseInFlight(refs)

	require.NoError(t, disk.Close())

	reopened, err := OpenDisk(DiskOptions{
		Dir:         dir,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	require.Equal(t, "disk-persist", reopened.DiskMeta().DiskID)
	require.Equal(t, uint64(64<<10), reopened.DiskMeta().MaxBytes)
	require.Equal(t, uint64(4096), reopened.PackPhysicalBytes())

	stat, err := reopened.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(64<<10), stat.TotalBytes)
	require.Zero(t, stat.UsedBytes)
	require.Equal(t, uint64(64<<10), stat.AvailBytes)

	got, err := reopened.packs.ReadBlock(refs[0])
	require.NoError(t, err)
	require.Equal(t, payload, got)

	wm, err := reopened.packs.SealedWatermark(ctx)
	require.NoError(t, err)
	require.Equal(t, refs[0].PackID, wm)
}

func TestStep3_KeysAndSplitKey_SupportPrefixBloomAndTombstoneEnvelope(t *testing.T) {
	t.Parallel()

	entryKey := LayerEntryKey(42, 7, []byte("hello.txt"))
	entryPrefix := LayerEntryPrefix(42, 7)
	require.Equal(t, len(entryPrefix), SplitKey(entryKey))
	require.Equal(t, entryPrefix, entryKey[:SplitKey(entryKey)])
	require.Equal(t, len(entryPrefix), SplitKey(entryPrefix))

	xattrKey := LayerXattrKey(42, 7, []byte("user.tag"))
	xattrPrefix := LayerXattrPrefix(42, 7)
	require.Equal(t, len(xattrPrefix), SplitKey(xattrKey))
	require.Equal(t, xattrPrefix, xattrKey[:SplitKey(xattrKey)])
	require.Equal(t, len(xattrPrefix), SplitKey(xattrPrefix))

	dataKey := LayerDataKey(42, 7, 99)
	dataPrefix := LayerDataPrefix(42, 7)
	require.Equal(t, len(dataPrefix), SplitKey(dataKey))
	require.Equal(t, dataPrefix, dataKey[:SplitKey(dataKey)])
	require.Equal(t, len(dataPrefix), SplitKey(dataPrefix))

	inodeKey := LayerInodeKey(42, 7)
	require.Equal(t, len(inodeKey), SplitKey(inodeKey))

	orphanKey := LayerOrphanKey(42, 7)
	require.Equal(t, len(orphanKey), SplitKey(orphanKey))

	require.Equal(t, []byte("meta:disk"), MetaDiskKey())
	require.Equal(t, []byte("meta:branch:feat-1"), MetaBranchKey("feat-1"))
	require.Equal(t, []byte("meta:mount:mnt-1"), MetaMountKey("mnt-1"))
	require.NotEmpty(t, MetaCommitKey(9))

	liveEnv := EncodeLiveEnvelope([]byte("payload"))
	tag, val, err := DecodeEnvelope(liveEnv)
	require.NoError(t, err)
	require.Equal(t, TagLive, tag)
	require.Equal(t, []byte("payload"), val)

	emptyLiveEnv := EncodeLiveEnvelope(nil)
	tag, val, err = DecodeEnvelope(emptyLiveEnv)
	require.NoError(t, err)
	require.Equal(t, TagLive, tag)
	require.Empty(t, val)

	tombstoneEnv := EncodeTombstone()
	tag, val, err = DecodeEnvelope(tombstoneEnv)
	require.NoError(t, err)
	require.Equal(t, TagTombstone, tag)
	require.Empty(t, val)
}

func TestStep4_PackActor_CoalescesConcurrentWritesAndServesLockFreeReads(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-coalesce",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	const workers = 8
	refs := make([]BlobRef, workers)
	payloads := make([][]byte, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	for i := range workers {
		payloads[i] = bytes.Repeat([]byte{byte(i + 1)}, 4096)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			gotRefs, appendErr := disk.packs.AppendBlocks(ctx, [][]byte{payloads[idx]})
			if appendErr != nil {
				errs[idx] = appendErr
				return
			}
			refs[idx] = gotRefs[0]
			disk.packs.ReleaseInFlight(gotRefs)
		}(i)
	}
	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, uint64(1), refs[i].PackID)
		got, readErr := disk.packs.ReadBlock(refs[i])
		require.NoError(t, readErr)
		require.Equal(t, payloads[i], got)
	}
}

func TestStep30_MetadataQuota_EnforcesDiskWideLimitAndReclaimsImmediatelyOnUnlink(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-quota",
		MaxBytes:    8192,
		BlockSize:   4096,
		MaxPackSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	blk := bytes.Repeat([]byte{0xab}, 4096)
	f1, err := disk.Create(ctx, DefaultBranchName, RootIno, "a.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f1.Ino, 0, blk)
	require.NoError(t, err)

	f2, err := disk.Create(ctx, DefaultBranchName, RootIno, "b.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f2.Ino, 0, blk)
	require.NoError(t, err)

	f3, err := disk.Create(ctx, DefaultBranchName, RootIno, "c.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f3.Ino, 0, blk)
	require.ErrorIs(t, err, ErrNoSpace)

	stat, err := disk.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), stat.TotalBytes)
	require.Equal(t, uint64(8192), stat.UsedBytes)
	require.Zero(t, stat.AvailBytes)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, RootIno, "a.bin", false))

	statAfter, err := disk.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), statAfter.UsedBytes)
	require.Equal(t, uint64(4096), statAfter.AvailBytes)

	_, err = disk.Write(ctx, DefaultBranchName, f3.Ino, 0, blk)
	require.NoError(t, err)
}
