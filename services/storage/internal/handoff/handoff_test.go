package handoff

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writePack(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "packs"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "packs", name), []byte(content), 0o644))
}

func TestPullSnapshot_同名不同内容的本地包不会被复用(t *testing.T) {
	ctx := context.Background()
	remote, err := NewDirRemote(t.TempDir())
	require.NoError(t, err)

	// 远端快照：包 1 是 "remote-AAAA"，包 2 是 "shared-data"
	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "CURRENT"), []byte("MANIFEST-1\n"), 0o644))
	writePack(t, src, "0000000000000001.pack", "remote-AAAA")
	writePack(t, src, "0000000000000002.pack", "shared-data")
	_, err = PushSnapshot(ctx, remote, "disk", "s1", src, nil)
	require.NoError(t, err)

	// 本地旧副本：包 1 同名、同大小、内容不同（另一个节点分配出了同一个编号）；包 2 内容相同
	local := t.TempDir()
	writePack(t, local, "0000000000000001.pack", "local--BBBB")
	writePack(t, local, "0000000000000002.pack", "shared-data")

	dst := filepath.Join(t.TempDir(), "pulled")
	stats, err := PullSnapshot(ctx, remote, "disk", "s1", dst, local, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksReused, "只有内容一致的包 2 可以复用")
	require.Equal(t, 1, stats.PacksDownloaded)

	got, err := os.ReadFile(filepath.Join(dst, "packs", "0000000000000001.pack"))
	require.NoError(t, err)
	require.Equal(t, "remote-AAAA", string(got), "同名同大小但内容不同的包必须以远端为准")
}

func TestPushSnapshot_同名包内容变了不会覆盖旧快照(t *testing.T) {
	ctx := context.Background()
	remote, err := NewDirRemote(t.TempDir())
	require.NoError(t, err)

	first := t.TempDir()
	writePack(t, first, "0000000000000001.pack", "version-one")
	_, err = PushSnapshot(ctx, remote, "disk", "s1", first, nil)
	require.NoError(t, err)

	second := t.TempDir()
	writePack(t, second, "0000000000000001.pack", "version-two")
	stats, err := PushSnapshot(ctx, remote, "disk", "s2", second, nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksUploaded)

	// 再推一次同样的内容：什么都不用传
	again, err := PushSnapshot(ctx, remote, "disk", "s3", second, nil)
	require.NoError(t, err)
	require.Zero(t, again.PacksUploaded)

	// 旧快照仍然读得到它当时的内容
	old := filepath.Join(t.TempDir(), "old")
	_, err = PullSnapshot(ctx, remote, "disk", "s1", old, "", nil)
	require.NoError(t, err)
	got, err := os.ReadFile(filepath.Join(old, "packs", "0000000000000001.pack"))
	require.NoError(t, err)
	require.Equal(t, "version-one", string(got))
}

func TestPullSnapshot_远端对象被篡改时拒绝安装(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	remote, err := NewDirRemote(root)
	require.NoError(t, err)
	src := t.TempDir()
	writePack(t, src, "0000000000000001.pack", "genuine-data")
	_, err = PushSnapshot(ctx, remote, "disk", "s1", src, nil)
	require.NoError(t, err)

	// 把远端对象原地改成同样长度的别的内容
	var object string
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "objects"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			object = p
		}
		return err
	}))
	require.NoError(t, os.WriteFile(object, []byte("tampered-dat"), 0o644))

	dst := filepath.Join(t.TempDir(), "pulled")
	_, err = PullSnapshot(ctx, remote, "disk", "s1", dst, "", nil)
	require.ErrorIs(t, err, ErrIncomplete)
	_, statErr := os.Stat(dst)
	require.True(t, os.IsNotExist(statErr), "校验失败后不能留下半成品目录")
}

func TestGC_只删没人引用的旧快照与对象(t *testing.T) {
	ctx := context.Background()
	remote, err := NewDirRemote(t.TempDir())
	require.NoError(t, err)

	// 五代快照：每一代新增一个包，并保留上一代的包（模拟只追加的磁盘）；第 3 代起丢掉包 1（被回收）
	dir := t.TempDir()
	writePack(t, dir, "0000000000000001.pack", "pack-one")
	for gen := 1; gen <= 5; gen++ {
		writePack(t, dir, SnapshotName(uint64(gen))[12:]+".pack", "data-of-gen-"+SnapshotName(uint64(gen)))
		if gen == 3 {
			require.NoError(t, os.Remove(filepath.Join(dir, "packs", "0000000000000001.pack")))
		}
		_, err = PushSnapshot(ctx, remote, "disk", SnapshotName(uint64(gen)), dir, nil)
		require.NoError(t, err)
	}
	require.NoError(t, WriteHead(ctx, remote, nil, &Head{DiskID: "disk", Generation: 5, Owner: "n", Snapshot: SnapshotName(5)}))
	// 一个没被任何清单引用的孤儿对象（推送中途失败留下的）
	require.NoError(t, remote.Put(ctx, objectKey("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), strings.NewReader("orphan")))

	later := func() time.Time { return time.Now().Add(48 * time.Hour) }

	// MinAge 之内什么都不删
	stats, err := GC(ctx, remote, GCOptions{KeepSnapshots: 1, MinAge: 24 * time.Hour})
	require.NoError(t, err)
	require.Zero(t, stats.SnapshotsDeleted)
	require.Zero(t, stats.ObjectsDeleted)

	// 过了保留期：留 HEAD（第 5 代）+ 额外 1 个（第 4 代），删第 1~3 代；
	// 包 1 只被第 1、2 代引用，随之被清除；孤儿对象也被清除
	stats, err = GC(ctx, remote, GCOptions{KeepSnapshots: 1, MinAge: 24 * time.Hour, Now: later})
	require.NoError(t, err)
	require.Equal(t, 2, stats.SnapshotsKept)
	require.Equal(t, 3, stats.SnapshotsDeleted)
	require.Equal(t, 2, stats.ObjectsDeleted, "包 1 与孤儿对象")

	// 保留下来的两代快照仍然完整可拉取，被删的那几代拉不到
	for _, gen := range []uint64{4, 5} {
		_, err = PullSnapshot(ctx, remote, "disk", SnapshotName(gen), filepath.Join(t.TempDir(), "p"), "", nil)
		require.NoError(t, err, "第 %d 代应当完好", gen)
	}
	_, err = PullSnapshot(ctx, remote, "disk", SnapshotName(2), filepath.Join(t.TempDir(), "p"), "", nil)
	require.ErrorIs(t, err, ErrIncomplete)

	// 再跑一轮是空操作
	stats, err = GC(ctx, remote, GCOptions{KeepSnapshots: 1, MinAge: 24 * time.Hour, Now: later})
	require.NoError(t, err)
	require.Zero(t, stats.SnapshotsDeleted+stats.ObjectsDeleted)

	// 删盘后它的对象在下一轮被清除干净
	require.NoError(t, DeleteDisk(ctx, remote, "disk"))
	_, _, err = ReadHead(ctx, remote, "disk")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = GC(ctx, remote, GCOptions{KeepSnapshots: 1, MinAge: 24 * time.Hour, Now: later})
	require.NoError(t, err)
	left, err := remote.List(ctx, "")
	require.NoError(t, err)
	require.Empty(t, left)
}
