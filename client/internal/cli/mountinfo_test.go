package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func TestMountinfo_IsMounted(t *testing.T) {
	sampleMountInfo := `23 61 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
24 61 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw
25 61 0:5 / /dev rw,nosuid,noexec,relatime shared:2 - devtmpfs udev rw,size=4000k,nr_inodes=1000,mode=755
30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040dir rw,nosuid,nodev shared:55 - fuse.bowl bowl rw
101 30 0:51 / /mnt/foo/bar rw,nosuid,nodev shared:56 - fuse.bowl bowl rw
`
	tmpFile := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(tmpFile, []byte(sampleMountInfo), 0o644))

	cli.SetMountInfoPathForTest(t, tmpFile)

	t.Run("detects normal mountpoint", func(t *testing.T) {
		mounted, err := cli.IsMountPoint("/mnt/foo/bar")
		require.NoError(t, err)
		require.True(t, mounted)
	})

	t.Run("detects octal escaped mountpoint with space", func(t *testing.T) {
		mounted, err := cli.IsMountPoint("/mnt/my dir")
		require.NoError(t, err)
		require.True(t, mounted)
	})

	t.Run("returns false for unmounted path", func(t *testing.T) {
		mounted, err := cli.IsMountPoint("/mnt/unmounted")
		require.NoError(t, err)
		require.False(t, mounted)
	})

	t.Run("handles clean path equivalence", func(t *testing.T) {
		mounted, err := cli.IsMountPoint("/mnt/foo/bar/")
		require.NoError(t, err)
		require.True(t, mounted)
	})
}

func TestMountinfo_FindDiskMountpoint(t *testing.T) {
	diskID1 := "0192a3b4-c5d6-7e8f-9012-3456789abcd1"
	diskID2 := "0192a3b4-c5d6-7e8f-9012-3456789abcd2"
	diskID3 := "0192a3b4-c5d6-7e8f-9012-3456789abcd3"

	sampleMountInfo := `30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040disk rw,nosuid,nodev shared:55 - fuse.bowl bowl:` + diskID1 + ` rw
101 30 0:51 / /mnt/normal/mp rw,nosuid,nodev shared:56 - fuse.bowl bowl:` + diskID2 + ` rw
`
	tmpFile := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(tmpFile, []byte(sampleMountInfo), 0o644))
	cli.SetMountInfoPathForTest(t, tmpFile)

	t.Run("detects normal disk mountpoint", func(t *testing.T) {
		mp, mounted, err := cli.FindDiskMountpoint(diskID2)
		require.NoError(t, err)
		require.True(t, mounted)
		require.Equal(t, "/mnt/normal/mp", mp)
	})

	t.Run("detects octal escaped disk mountpoint with space", func(t *testing.T) {
		mp, mounted, err := cli.FindDiskMountpoint(diskID1)
		require.NoError(t, err)
		require.True(t, mounted)
		require.Equal(t, "/mnt/my disk", mp)
	})

	t.Run("returns false for unmounted diskID", func(t *testing.T) {
		mp, mounted, err := cli.FindDiskMountpoint(diskID3)
		require.NoError(t, err)
		require.False(t, mounted)
		require.Empty(t, mp)
	})

	t.Run("returns false when mountinfo does not exist", func(t *testing.T) {
		cli.SetMountInfoPathForTest(t, filepath.Join(t.TempDir(), "nonexistent"))
		mp, mounted, err := cli.FindDiskMountpoint(diskID1)
		require.NoError(t, err)
		require.False(t, mounted)
		require.Empty(t, mp)
	})
}

func TestMount_AlreadyMountedRejection(t *testing.T) {
	tmpDir := t.TempDir()
	mountedDir := filepath.Join(tmpDir, "already-mounted")
	require.NoError(t, os.Mkdir(mountedDir, 0o755))

	sampleMountInfo := "100 30 0:50 / " + mountedDir + " rw,nosuid,nodev shared:55 - fuse.bowl bowl rw\n"
	mountinfoFile := filepath.Join(tmpDir, "mountinfo")
	require.NoError(t, os.WriteFile(mountinfoFile, []byte(sampleMountInfo), 0o644))
	cli.SetMountInfoPathForTest(t, mountinfoFile)

	var stdout, stderr bytes.Buffer
	validUUID := uuid.New().String()
	code := cli.Execute([]string{"mount", "-f", validUUID, mountedDir}, &stdout, &stderr)

	require.NotEqual(t, 0, code, "已挂载目录必须被拒绝")
	require.Contains(t, stderr.String(), "已挂载")
}
