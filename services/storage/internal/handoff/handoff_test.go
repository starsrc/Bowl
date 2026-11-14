package handoff

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
