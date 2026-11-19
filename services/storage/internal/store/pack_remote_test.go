package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

// fakePackSource 用一个目录顶替远端：对象按内容哈希命名。gate 非 nil 时整包下载会卡在上面，
// 用来证明读路径不依赖整包下载。
type fakePackSource struct {
	dir     string
	gate    chan struct{}
	ranges  atomic.Int64
	fetches atomic.Int64
}

func (f *fakePackSource) FetchPack(ctx context.Context, sha string, _ int64, dst string) error {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	raw, err := os.ReadFile(filepath.Join(f.dir, sha))
	if err != nil {
		return err
	}
	f.fetches.Add(1)
	return os.WriteFile(dst, raw, 0o644)
}

func (f *fakePackSource) ReadPackRange(_ context.Context, sha string, off int64, dst []byte) error {
	file, err := os.Open(filepath.Join(f.dir, sha))
	if err != nil {
		return err
	}
	defer file.Close()
	f.ranges.Add(1)
	_, err = file.ReadAt(dst, off)
	return err
}

// evictPacksToRemote 把盘目录里所有数据包挪到"远端"，原地只留占位文件，返回被挪走的包数。
func evictPacksToRemote(t *testing.T, dir, remoteDir string) int {
	t.Helper()
	packsDir := filepath.Join(dir, "packs")
	entries, err := os.ReadDir(packsDir)
	require.NoError(t, err)
	moved := 0
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".pack") {
			continue
		}
		p := filepath.Join(packsDir, e.Name())
		raw, err := os.ReadFile(p)
		require.NoError(t, err)
		sum := sha256.Sum256(raw)
		sha := hex.EncodeToString(sum[:])
		require.NoError(t, os.WriteFile(filepath.Join(remoteDir, sha), raw, 0o644))
		require.NoError(t, store.WriteRemotePackStub(packsDir, e.Name(), store.RemotePackStub{SHA256: sha, Size: int64(len(raw))}))
		require.NoError(t, os.Remove(p))
		moved++
	}
	return moved
}

// seedThreeFiles 写入三个各占一个数据包的文件并关闭，返回它们的内容。
func seedThreeFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lazy", MaxPackSize: store.BlobSize})
	want := map[string][]byte{}
	for i, name := range []string{"a.bin", "b.bin", "c.bin"} {
		f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte(name), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		data := makePatternBytes(store.BlobSize, byte(0x30+i))
		_, _, err = actor.Write(ctx, f.Ino, 0, data)
		require.NoError(t, err)
		want[name] = data
	}
	require.NoError(t, actor.Close())
	return want
}

func readWholeFile(t *testing.T, actor *store.StoreActor, name string) ([]byte, error) {
	t.Helper()
	ctx := context.Background()
	_, attr, err := actor.Lookup(ctx, store.RootIno, []byte(name))
	require.NoError(t, err)
	return actor.Read(ctx, attr.Ino, 0, store.BlobSize)
}

func TestRemotePacks_整包还没下回来时按范围读就能满足读取(t *testing.T) {
	dir, remoteDir := t.TempDir(), t.TempDir()
	want := seedThreeFiles(t, dir)
	require.Equal(t, 3, evictPacksToRemote(t, dir, remoteDir))

	src := &fakePackSource{dir: remoteDir, gate: make(chan struct{})}
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lazy", MaxPackSize: store.BlobSize, PackSource: src})
	require.Equal(t, 3, actor.RemotePackCount())

	// When: 整包下载被卡住
	got, err := readWholeFile(t, actor, "b.bin")

	// Then: 读取仍然成功，走的是按范围读
	require.NoError(t, err)
	require.Equal(t, want["b.bin"], got)
	require.Positive(t, src.ranges.Load())
	require.Zero(t, src.fetches.Load())

	// 放开下载并补齐：之后全部是本地包，占位文件消失
	close(src.gate)
	require.NoError(t, actor.HydrateRemotePacks(context.Background()))
	require.Zero(t, actor.RemotePackCount())
	requirePackFileCount(t, dir, 3)
	before := src.ranges.Load()
	for name, data := range want {
		got, err := readWholeFile(t, actor, name)
		require.NoError(t, err)
		require.Equal(t, data, got, name)
	}
	require.LessOrEqual(t, src.ranges.Load()-before, int64(0), "补齐之后不应再访问远端")
}

func TestRemotePacks_新写入的包编号避开未下载的包且快照带着占位文件(t *testing.T) {
	ctx := context.Background()
	dir, remoteDir := t.TempDir(), t.TempDir()
	want := seedThreeFiles(t, dir)
	evictPacksToRemote(t, dir, remoteDir)

	src := &fakePackSource{dir: remoteDir, gate: make(chan struct{})} // 下载一直卡住：全程保持未下载状态
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lazy", MaxPackSize: store.BlobSize, PackSource: src})

	// When: 在三个包都没下回来的情况下写入新文件
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("new.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	fresh := makePatternBytes(store.BlobSize, 0x77)
	_, _, err = actor.Write(ctx, f.Ino, 0, fresh)
	require.NoError(t, err)

	// Then: 新包没有占用任何未下载包的编号，旧文件内容不受影响
	packs := findPackFiles(t, dir)
	require.Len(t, packs, 1)
	for _, stubbed := range []string{"0000000000000001.pack", "0000000000000002.pack", "0000000000000003.pack"} {
		require.NotEqual(t, stubbed, filepath.Base(packs[0]))
	}
	got, err := readWholeFile(t, actor, "a.bin")
	require.NoError(t, err)
	require.Equal(t, want["a.bin"], got)

	// When: 对这块"半本地"的盘做快照
	snap := filepath.Join(t.TempDir(), "snap")
	_, err = actor.Checkpoint(ctx, snap)
	require.NoError(t, err)

	// Then: 快照里未下载的包仍是占位文件，用同一个远端打开后新旧内容都读得到
	entries, err := os.ReadDir(filepath.Join(snap, "packs"))
	require.NoError(t, err)
	stubs := 0
	for _, e := range entries {
		if _, ok := store.PackNameOfStub(e.Name()); ok {
			stubs++
		}
	}
	require.Equal(t, 3, stubs)
	snapActor := openTestActor(t, &store.Options{Dir: snap, DiskID: "snap", ReadOnly: true, PackSource: src})
	got, err = readWholeFile(t, snapActor, "c.bin")
	require.NoError(t, err)
	require.Equal(t, want["c.bin"], got)
	got, err = readWholeFile(t, snapActor, "new.bin")
	require.NoError(t, err)
	require.Equal(t, fresh, got)
}

func TestRemotePacks_没有配置远端时读未下载的包报错而不是返回脏数据(t *testing.T) {
	dir, remoteDir := t.TempDir(), t.TempDir()
	seedThreeFiles(t, dir)
	evictPacksToRemote(t, dir, remoteDir)

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lazy", MaxPackSize: store.BlobSize})
	_, err := readWholeFile(t, actor, "a.bin")
	require.ErrorIs(t, err, store.ErrIO)
}

func TestRemotePacks_PackPrefetch打开后自动补齐(t *testing.T) {
	dir, remoteDir := t.TempDir(), t.TempDir()
	seedThreeFiles(t, dir)
	evictPacksToRemote(t, dir, remoteDir)

	src := &fakePackSource{dir: remoteDir}
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lazy", MaxPackSize: store.BlobSize, PackSource: src, PackPrefetch: true})
	require.Eventually(t, func() bool { return actor.RemotePackCount() == 0 }, 5e9, 1e7)
	require.EqualValues(t, 3, src.fetches.Load())
}
