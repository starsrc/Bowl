package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const sampleMountInfo = `23 61 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
24 61 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw
30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040dir rw,nosuid,nodev shared:55 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd1 rw
101 30 0:51 / /mnt/foo/bar ro,nosuid,nodev shared:56 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd2 ro
102 30 0:52 / /mnt/remount-ro rw,errors=remount-ro shared:57 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd4 rw
`

func TestIsMountPoint_GivenMountedAndOctalEscapedPaths_ReturnsExpectedState(t *testing.T) {
	// Given
	withMountInfoContent(t, sampleMountInfo)

	// When / Then
	mountedBar, err := IsMountPoint("/mnt/foo/bar/")
	require.NoError(t, err)
	require.True(t, mountedBar)

	mountedSpace, err := IsMountPoint("/mnt/my dir")
	require.NoError(t, err)
	require.True(t, mountedSpace, "八进制转义的空格必须被还原")

	unmounted, err := IsMountPoint("/mnt/unmounted")
	require.NoError(t, err)
	require.False(t, unmounted)
}

func TestIsMountPoint_GivenMalformedAndUnescapedLines_HandlesGracefully(t *testing.T) {
	// Given
	withMountInfoContent(t, "1 2 3\n\nnot-a-mountinfo-line\n100 30 0:50 / /mnt/no-separator rw\n101 30 0:51 / /mnt/keep\\999 rw - fuse.bowl bowl:disk rw\n")

	// When / Then
	noSep, err := IsMountPoint("/mnt/no-separator")
	require.NoError(t, err)
	require.True(t, noSep)

	keepInvalidEscape, err := IsMountPoint(`/mnt/keep\999`)
	require.NoError(t, err)
	require.True(t, keepInvalidEscape, "非八进制转义必须原样保留")

	absent, err := IsMountPoint("/1/2/3")
	require.NoError(t, err)
	require.False(t, absent)
}

func TestIsMountPoint_GivenMissingMountInfoFile_ReturnsFalseWithoutError(t *testing.T) {
	// Given
	withMissingMountInfo(t)

	// When
	mounted, err := IsMountPoint("/mnt/anything")

	// Then
	require.NoError(t, err)
	require.False(t, mounted)
}

func TestIsMountPoint_GivenUnreadableMountInfo_ReturnsError(t *testing.T) {
	// Given
	withUnreadableMountInfo(t)

	// When
	_, err := IsMountPoint("/mnt/anything")

	// Then
	require.Error(t, err)
}

func TestFindDiskMountpoint_GivenActiveAndAbsentDisks_MatchesCaseInsensitivelyAndDistinguishesReadOnly(t *testing.T) {
	// Given
	withMountInfoContent(t, sampleMountInfo)

	// When / Then: rw 挂载
	info1, err := FindDiskMountpoint("0192A3B4-C5D6-7E8F-9012-3456789ABCD1")
	require.NoError(t, err)
	require.Equal(t, DiskMountInfo{
		Mountpoint: "/mnt/my dir",
		Mounted:    true,
		ReadOnly:   false,
	}, info1)

	// When / Then: ro,nosuid 只读挂载
	info2, err := FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd2")
	require.NoError(t, err)
	require.Equal(t, DiskMountInfo{
		Mountpoint: "/mnt/foo/bar",
		Mounted:    true,
		ReadOnly:   true,
	}, info2)

	// When / Then: rw,errors=remount-ro 不应误判为只读挂载
	info4, err := FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd4")
	require.NoError(t, err)
	require.Equal(t, DiskMountInfo{
		Mountpoint: "/mnt/remount-ro",
		Mounted:    true,
		ReadOnly:   false,
	}, info4)

	// When / Then: 未挂载
	info3, err := FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd3")
	require.NoError(t, err)
	require.False(t, info3.Mounted)
	require.Empty(t, info3.Mountpoint)
	require.False(t, info3.ReadOnly)
}

func TestIsMountPoint_GivenSymlinkedLeafAndParent_MatchesCanonicalMountinfo(t *testing.T) {
	// Given: real mountpoint recorded in mountinfo, accessed via leaf symlink and symlinked parent
	realParent := t.TempDir()
	realMP := filepath.Join(realParent, "real-mnt")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := canonicalMountpoint(realMP)

	symMP := filepath.Join(t.TempDir(), "sym-mnt")
	require.NoError(t, os.Symlink(realMP, symMP))

	symParent := filepath.Join(t.TempDir(), "sym-parent")
	require.NoError(t, os.Symlink(realParent, symParent))
	viaSymParent := filepath.Join(symParent, "real-mnt")

	withMountInfoContent(t, "100 30 0:50 / "+canonMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When / Then
	symMounted, err := IsMountPoint(symMP)
	require.NoError(t, err)
	require.True(t, symMounted, "leaf symlink to active mountpoint must match mountinfo")

	parentSymMounted, err := IsMountPoint(viaSymParent)
	require.NoError(t, err)
	require.True(t, parentSymMounted, "mountpoint beneath symlinked parent must match mountinfo")
}

type fakeFileInfo struct {
	name string
	mode os.FileMode
}

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeFileInfo) Sys() any           { return nil }

type fakeFSNode struct {
	mode   os.FileMode
	target string
	err    error
}

func withMountInfoFSStub(t *testing.T, nodes map[string]fakeFSNode) *[]string {
	t.Helper()
	prevLstat := mountInfoLstat
	prevReadlink := mountInfoReadlink
	var lstatCalls []string
	mountInfoLstat = func(path string) (os.FileInfo, error) {
		lstatCalls = append(lstatCalls, path)
		node, ok := nodes[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		if node.err != nil {
			return nil, node.err
		}
		return fakeFileInfo{name: filepath.Base(path), mode: node.mode}, nil
	}
	mountInfoReadlink = func(path string) (string, error) {
		node, ok := nodes[path]
		if !ok {
			return "", os.ErrNotExist
		}
		if node.err != nil {
			return "", node.err
		}
		if node.mode&os.ModeSymlink == 0 {
			return "", syscall.EINVAL
		}
		return node.target, nil
	}
	t.Cleanup(func() {
		mountInfoLstat = prevLstat
		mountInfoReadlink = prevReadlink
	})
	return &lstatCalls
}

func buildSymlinkChainNodes(hops int, target string) map[string]fakeFSNode {
	nodes := map[string]fakeFSNode{
		"/tmp": {mode: os.ModeDir | 0o755},
	}
	for i := range hops {
		next := target
		if i+1 < hops {
			next = fmt.Sprintf("/tmp/hop-%02d", i+1)
		}
		nodes[fmt.Sprintf("/tmp/hop-%02d", i)] = fakeFSNode{
			mode:   os.ModeSymlink | 0o777,
			target: next,
		}
	}
	return nodes
}

func TestResolveMountPoint_GivenDirectAndSyntheticMountedPaths_ReturnsZeroLstatCalls(t *testing.T) {
	// Given: /mnt/bowl-a is synthetic and absent on host disk; /data/mnt is mounted
	withMountInfoContent(t, "30 61 8:1 / / rw - ext4 /dev/sda1 rw\n100 30 0:50 / /mnt/bowl-a rw - fuse.bowl bowl:"+testDiskID1+" rw\n101 30 0:51 / /data/mnt rw - fuse.bowl bowl:"+testDiskID2+" rw\n")
	lstatCalls := withMountInfoFSStub(t, map[string]fakeFSNode{})

	// When
	resolvedSynthetic, mountedSynthetic, err := ResolveMountPoint("/mnt/bowl-a")
	require.NoError(t, err)
	resolvedTrailSlash, mountedTrailSlash, err := ResolveMountPoint("/data/mnt/")
	require.NoError(t, err)

	// Then
	require.True(t, mountedSynthetic)
	require.Equal(t, "/mnt/bowl-a", resolvedSynthetic)
	require.True(t, mountedTrailSlash)
	require.Equal(t, "/data/mnt", resolvedTrailSlash)
	require.Empty(t, *lstatCalls, "direct mountinfo matches must invoke zero Lstat calls")
}

func TestResolveMountPoint_GivenLeafSymlinkToMountpoint_StopsBeforeLstatOnMountRoot(t *testing.T) {
	// Given: /tmp/sym points to /data/mnt, where /data/mnt is an active mountpoint
	withMountInfoContent(t, "30 61 8:1 / / rw - ext4 /dev/sda1 rw\n100 30 0:50 / /data/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	lstatCalls := withMountInfoFSStub(t, map[string]fakeFSNode{
		"/tmp":     {mode: os.ModeDir | 0o755},
		"/tmp/sym": {mode: os.ModeSymlink | 0o777, target: "/data/mnt"},
	})

	// When
	resolved, mounted, err := ResolveMountPoint("/tmp/sym")

	// Then
	require.NoError(t, err)
	require.True(t, mounted)
	require.Equal(t, "/data/mnt", resolved)
	require.Equal(t, []string{"/tmp", "/tmp/sym"}, *lstatCalls, "must never call Lstat on /data or mounted root /data/mnt")
}

func TestResolveMountPoint_GivenSymlinkedParent_ResolvesCanonicalMountWithoutLstatOnMountRoot(t *testing.T) {
	// Given: /var/run points to /run while /run/mnt is active, and /var/rel points to ../run
	withMountInfoContent(t, "30 61 8:1 / / rw - ext4 /dev/sda1 rw\n35 30 0:25 / /run rw - tmpfs tmpfs rw\n100 35 0:50 / /run/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	lstatCalls := withMountInfoFSStub(t, map[string]fakeFSNode{
		"/var":     {mode: os.ModeDir | 0o755},
		"/var/run": {mode: os.ModeSymlink | 0o777, target: "/run"},
		"/var/rel": {mode: os.ModeSymlink | 0o777, target: "../run"},
	})

	// When
	resolvedAbs, mountedAbs, err := ResolveMountPoint("/var/run/mnt")
	require.NoError(t, err)
	resolvedRel, mountedRel, err := ResolveMountPoint("/var/rel/mnt")
	require.NoError(t, err)

	// Then
	require.True(t, mountedAbs)
	require.Equal(t, "/run/mnt", resolvedAbs)
	require.True(t, mountedRel)
	require.Equal(t, "/run/mnt", resolvedRel)
	require.Equal(t, []string{"/var", "/var/run", "/var", "/var/rel"}, *lstatCalls, "must stop before Lstat on /run or /run/mnt")
}

func TestResolveMountPoint_GivenHopLimitErrorsAndMissingMountInfo_HandlesCleanly(t *testing.T) {
	t.Run("40SymlinkHopsSucceed", func(t *testing.T) {
		withMountInfoContent(t, "100 30 0:50 / /data/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
		withMountInfoFSStub(t, buildSymlinkChainNodes(40, "/data/mnt"))

		resolved, mounted, err := ResolveMountPoint("/tmp/hop-00")
		require.NoError(t, err)
		require.True(t, mounted)
		require.Equal(t, "/data/mnt", resolved)
	})

	t.Run("41SymlinkHopsExceedLimit", func(t *testing.T) {
		withMountInfoContent(t, "100 30 0:50 / /data/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
		withMountInfoFSStub(t, buildSymlinkChainNodes(41, "/data/mnt"))

		resolved, mounted, err := ResolveMountPoint("/tmp/hop-00")
		require.NoError(t, err)
		require.False(t, mounted)
		require.Equal(t, "/tmp/hop-40", resolved)
	})

	t.Run("SymlinkLoopAndLstatErrors", func(t *testing.T) {
		withMountInfoContent(t, "100 30 0:50 / /data/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
		withMountInfoFSStub(t, map[string]fakeFSNode{
			"/tmp":        {mode: os.ModeDir | 0o755},
			"/tmp/loop-a": {mode: os.ModeSymlink | 0o777, target: "/tmp/loop-b"},
			"/tmp/loop-b": {mode: os.ModeSymlink | 0o777, target: "/tmp/loop-a"},
			"/tmp/notdir": {err: syscall.ENOTDIR},
			"/tmp/eloop":  {err: syscall.ELOOP},
		})

		for _, path := range []string{"/tmp/loop-a", "/tmp/notdir", "/tmp/eloop", "/tmp/missing"} {
			_, mounted, err := ResolveMountPoint(path)
			require.NoError(t, err, "path=%s", path)
			require.False(t, mounted, "path=%s", path)
		}
	})

	t.Run("MissingMountInfoSkipsLstat", func(t *testing.T) {
		withMissingMountInfo(t)
		lstatCalls := withMountInfoFSStub(t, map[string]fakeFSNode{})

		resolved, mounted, err := ResolveMountPoint("/mnt/anything")
		require.NoError(t, err)
		require.False(t, mounted)
		require.Equal(t, "/mnt/anything", resolved)
		require.Empty(t, *lstatCalls)
	})
}
