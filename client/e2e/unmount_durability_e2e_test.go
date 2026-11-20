//go:build e2e && linux

package e2e_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func writeMultiFilesAndExpectedHashes(t *testing.T, dir string, count, size int) map[string][sha256.Size]byte {
	t.Helper()
	want := make(map[string][sha256.Size]byte, count)
	for i := range count {
		name := fmt.Sprintf("batch-%03d.bin", i)
		payload := deterministicPayload(name, size)
		writeFullFile(t, filepath.Join(dir, name), payload, size)
		want[name] = sha256.Sum256(payload)
	}
	return want
}

func verifyMultiFilesSHA256(t *testing.T, dir string, want map[string][sha256.Size]byte) {
	t.Helper()
	for name, expected := range want {
		require.Equal(t, expected, fileSHA256(t, filepath.Join(dir, name)), "file=%s", name)
	}
}

func openTruncateAndClose(t *testing.T, path string) {
	t.Helper()
	fd, err := openEINTR(path, unix.O_WRONLY|unix.O_TRUNC, 0o644)
	require.NoError(t, err)
	require.NoError(t, unix.Close(fd))
}

func fallocateAndClose(t *testing.T, path string, mode uint32, off, length int64) {
	t.Helper()
	fd, err := openEINTR(path, unix.O_RDWR, 0o644)
	require.NoError(t, err)
	require.NoError(t, fallocateEINTR(fd, mode, off, length))
	require.NoError(t, unix.Close(fd))
}

func TestUnmountDurability_MultiFileConcurrentFsync_TruncateFallocateAndMetaOnly_PersistsAcrossRemount(t *testing.T) {
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()

	t.Run("MultiFileConcurrentFsyncOverMetaTransport", func(t *testing.T) {

		proxy := startControllableStorageProxy(t, realEndpoint)
		diskID := createTestDisk(t, client).GetDiskId()
		mp, _, _, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "16MiB")

		wantHashes := writeMultiFilesAndExpectedHashes(t, mp, 80, 4096)
		proxy.SetFsyncDelay(25 * time.Millisecond)

		stopMount()

		require.Equal(t, int64(80), proxy.FsyncCalls())
		require.Zero(t, proxy.FsyncDirCalls())
		require.Greater(t, proxy.FsyncMaxInflight(), int64(1))
		require.LessOrEqual(t, proxy.FsyncMaxInflight(), int64(fusefs.DefaultUnmountFsyncConcurrency))

		mpRO, stopRO := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
		defer stopRO()
		verifyMultiFilesSHA256(t, mpRO, wantHashes)
	})

	t.Run("OpenTruncateSetAttrTruncateAndFallocateWithoutFsyncPersistAcrossRemount", func(t *testing.T) {

		diskID := createTestDisk(t, client).GetDiskId()
		mpSeed, _, _, stopSeed := startMountedDaemon(t, realEndpoint, diskID)

		otruncPath := "otrunc.bin"
		setattrPath := "setattr_trunc.bin"
		punchPath := "falloc_punch.bin"
		growPath := "falloc_grow.bin"

		setattrSeed := deterministicPayload("setattr-seed", 300_000)
		punchSeed := deterministicPayload("punch-seed", 32_768)
		growSeed := deterministicPayload("grow-seed", 4096)

		writeFullFile(t, filepath.Join(mpSeed, otruncPath), deterministicPayload("otrunc-seed", 8192), 8192)
		writeFullFile(t, filepath.Join(mpSeed, setattrPath), setattrSeed, 65_536)
		writeFullFile(t, filepath.Join(mpSeed, punchPath), punchSeed, 32_768)
		writeFullFile(t, filepath.Join(mpSeed, growPath), growSeed, 4096)
		stopSeed()

		proxy := startControllableStorageProxy(t, realEndpoint)
		mpMut, _, _, stopMut := startMountedDaemon(t, proxy.Endpoint, diskID)

		openTruncateAndClose(t, filepath.Join(mpMut, otruncPath))
		require.NoError(t, os.Truncate(filepath.Join(mpMut, setattrPath), 4113))
		fallocateAndClose(t, filepath.Join(mpMut, punchPath), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 4096, 8192)
		fallocateAndClose(t, filepath.Join(mpMut, growPath), 0, 4096, 16_384)
		stopMut()

		require.Equal(t, int64(4), proxy.FsyncCalls())
		require.Zero(t, proxy.FsyncDirCalls())

		mpRO, stopRO := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
		defer stopRO()

		gotOTrunc, err := os.ReadFile(filepath.Join(mpRO, otruncPath))
		require.NoError(t, err)
		require.Empty(t, gotOTrunc)

		gotSetAttr, err := os.ReadFile(filepath.Join(mpRO, setattrPath))
		require.NoError(t, err)
		require.Equal(t, setattrSeed[:4113], gotSetAttr)

		wantPunch := append([]byte(nil), punchSeed...)
		clear(wantPunch[4096:12_288])
		gotPunch, err := os.ReadFile(filepath.Join(mpRO, punchPath))
		require.NoError(t, err)
		require.Equal(t, wantPunch, gotPunch)

		wantGrow := make([]byte, 20_480)
		copy(wantGrow, growSeed)
		gotGrow, err := os.ReadFile(filepath.Join(mpRO, growPath))
		require.NoError(t, err)
		require.Equal(t, wantGrow, gotGrow)
	})

	t.Run("MetadataOnlyUnmountPersistsViaSingleFsyncDirAndCleanMountIssuesZero", func(t *testing.T) {

		diskID := createTestDisk(t, client).GetDiskId()
		mpSeed, _, _, stopSeed := startMountedDaemon(t, realEndpoint, diskID)
		require.NoError(t, os.WriteFile(filepath.Join(mpSeed, "meta-target.txt"), []byte("meta-body"), 0o644))
		require.NoError(t, os.Mkdir(filepath.Join(mpSeed, "existing-dir"), 0o755))
		stopSeed()

		proxyMeta := startControllableStorageProxy(t, realEndpoint)
		mpMeta, _, _, stopMeta := startMountedDaemon(t, proxyMeta.Endpoint, diskID)

		require.NoError(t, os.Mkdir(filepath.Join(mpMeta, "sub-meta"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(mpMeta, "empty-meta.txt"), nil, 0o644))
		require.NoError(t, os.Symlink("meta-target.txt", filepath.Join(mpMeta, "sym-meta")))
		require.NoError(t, unix.Setxattr(filepath.Join(mpMeta, "meta-target.txt"), "user.meta", []byte("durability-val"), 0))
		require.NoError(t, os.Chmod(filepath.Join(mpMeta, "meta-target.txt"), 0o600))
		require.NoError(t, os.Rename(filepath.Join(mpMeta, "existing-dir"), filepath.Join(mpMeta, "renamed-dir")))
		stopMeta()

		require.Zero(t, proxyMeta.FsyncCalls())
		require.Equal(t, int64(1), proxyMeta.FsyncDirCalls())

		proxyClean := startControllableStorageProxy(t, realEndpoint)
		mpVerify, _, _, stopVerify := startMountedDaemon(t, proxyClean.Endpoint, diskID)

		subInfo, err := os.Stat(filepath.Join(mpVerify, "sub-meta"))
		require.NoError(t, err)
		require.True(t, subInfo.IsDir())

		emptyInfo, err := os.Stat(filepath.Join(mpVerify, "empty-meta.txt"))
		require.NoError(t, err)
		require.Zero(t, emptyInfo.Size())

		symTarget, err := os.Readlink(filepath.Join(mpVerify, "sym-meta"))
		require.NoError(t, err)
		require.Equal(t, "meta-target.txt", symTarget)

		xBuf := make([]byte, 64)
		xLen, err := unix.Getxattr(filepath.Join(mpVerify, "meta-target.txt"), "user.meta", xBuf)
		require.NoError(t, err)
		require.Equal(t, []byte("durability-val"), xBuf[:xLen])

		targetInfo, err := os.Stat(filepath.Join(mpVerify, "meta-target.txt"))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), targetInfo.Mode().Perm())

		require.NoDirExists(t, filepath.Join(mpVerify, "existing-dir"))
		require.DirExists(t, filepath.Join(mpVerify, "renamed-dir"))

		stopVerify()
		require.Zero(t, proxyClean.FsyncCalls())
		require.Zero(t, proxyClean.FsyncDirCalls())
	})
}
