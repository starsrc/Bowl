package handoff

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testRemoteContract 是所有 Remote 实现都必须满足的行为。
func testRemoteContract(t *testing.T, r Remote) {
	t.Helper()
	ctx := context.Background()
	base := fmt.Sprintf("contract-%d/", time.Now().UnixNano())

	// 不存在的对象
	_, err := r.Get(ctx, base+"missing")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = r.Size(ctx, base+"missing")
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorIs(t, r.ReadAt(ctx, base+"missing", 0, make([]byte, 1)), ErrNotFound)
	require.NoError(t, r.Delete(ctx, base+"missing"), "删除不存在的对象不算错误")

	// 写入、整读、范围读、大小
	payload := bytes.Repeat([]byte("0123456789"), 100_000) // 1 MB
	require.NoError(t, r.Put(ctx, base+"objects/a", bytes.NewReader(payload)))
	rc, err := r.Get(ctx, base+"objects/a")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
	size, err := r.Size(ctx, base+"objects/a")
	require.NoError(t, err)
	require.EqualValues(t, len(payload), size)
	part := make([]byte, 25)
	require.NoError(t, r.ReadAt(ctx, base+"objects/a", 123_457, part))
	require.Equal(t, payload[123_457:123_482], part)

	// 从文件上传（数据包走的路径）
	f, err := os.CreateTemp(t.TempDir(), "pack")
	require.NoError(t, err)
	_, err = f.Write(payload[:4096])
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.NoError(t, r.Put(ctx, base+"objects/b", f))
	require.NoError(t, f.Close())

	// 列举只返回前缀下的对象
	require.NoError(t, r.Put(ctx, base+"other/c", bytes.NewReader([]byte("c"))))
	listed, err := r.List(ctx, base+"objects/")
	require.NoError(t, err)
	keys := make([]string, 0, len(listed))
	for _, o := range listed {
		keys = append(keys, o.Key)
		require.False(t, o.ModTime.IsZero())
	}
	require.ElementsMatch(t, []string{base + "objects/a", base + "objects/b"}, keys)

	// 条件写：首次创建、按旧值改写、旧值不符被拒
	head := base + "HEAD"
	require.NoError(t, r.CompareAndSwap(ctx, head, nil, []byte("v1")))
	require.ErrorIs(t, r.CompareAndSwap(ctx, head, nil, []byte("again")), ErrConflict, "对象已存在时不能再按\"不存在\"创建")
	require.ErrorIs(t, r.CompareAndSwap(ctx, head, []byte("stale"), []byte("v2")), ErrConflict)
	require.NoError(t, r.CompareAndSwap(ctx, head, []byte("v1"), []byte("v2")))
	require.ErrorIs(t, r.CompareAndSwap(ctx, base+"absent", []byte("x"), []byte("y")), ErrConflict)

	// 并发改写同一个值只有一个成功
	var wg sync.WaitGroup
	results := make([]error, 8)
	for i := range results {
		wg.Go(func() {
			results[i] = r.CompareAndSwap(ctx, head, []byte("v2"), fmt.Appendf(nil, "winner-%d", i))
		})
	}
	wg.Wait()
	wins := 0
	for _, casErr := range results {
		if casErr == nil {
			wins++
		} else {
			require.ErrorIs(t, casErr, ErrConflict)
		}
	}
	require.Equal(t, 1, wins)

	// 清理
	all, err := r.List(ctx, base)
	require.NoError(t, err)
	for _, o := range all {
		require.NoError(t, r.Delete(ctx, o.Key))
	}
	left, err := r.List(ctx, base)
	require.NoError(t, err)
	require.Empty(t, left)
}

func TestDirRemote_Contract(t *testing.T) {
	r, err := NewDirRemote(t.TempDir())
	require.NoError(t, err)
	testRemoteContract(t, r)
}

// TestS3Remote_Contract 需要一个真实的 S3 兼容服务，未配置时跳过：
//
//	HANDOFF_TEST_S3_URL='s3://bucket/prefix?endpoint=127.0.0.1:9900&insecure=true'
//	HANDOFF_TEST_S3_ACCESS_KEY / HANDOFF_TEST_S3_SECRET_KEY
func TestS3Remote_Contract(t *testing.T) {
	raw := os.Getenv("HANDOFF_TEST_S3_URL")
	if raw == "" {
		t.Skip("HANDOFF_TEST_S3_URL not set")
	}
	cfg, err := ParseS3URL(raw)
	require.NoError(t, err)
	cfg.AccessKey = os.Getenv("HANDOFF_TEST_S3_ACCESS_KEY")
	cfg.SecretKey = os.Getenv("HANDOFF_TEST_S3_SECRET_KEY")
	r, err := NewS3Remote(context.Background(), &cfg)
	require.NoError(t, err)
	testRemoteContract(t, r)

	// 整套快照协议在对象存储上同样成立
	ctx := context.Background()
	disk := fmt.Sprintf("disk-%d", time.Now().UnixNano())
	src := t.TempDir()
	require.NoError(t, os.WriteFile(src+"/CURRENT", []byte("MANIFEST-1\n"), 0o644))
	writePack(t, src, "0000000000000001.pack", "pack-payload-on-s3")
	_, err = PushSnapshot(ctx, r, disk, "s1", src, nil)
	require.NoError(t, err)
	dst := t.TempDir() + "/pulled"
	stats, err := PullSnapshot(ctx, r, disk, "s1", dst, "", nil)
	require.NoError(t, err)
	require.Equal(t, 1, stats.PacksDownloaded)
	got, err := os.ReadFile(dst + "/packs/0000000000000001.pack")
	require.NoError(t, err)
	require.Equal(t, "pack-payload-on-s3", string(got))
	require.NoError(t, DeleteDisk(ctx, r, disk))
}

func TestParseS3URL(t *testing.T) {
	cfg, err := ParseS3URL("s3://my-bucket/env/dev?endpoint=127.0.0.1:9900&insecure=true&region=us-west-2")
	require.NoError(t, err)
	require.Equal(t, S3Config{Endpoint: "127.0.0.1:9900", Bucket: "my-bucket", Prefix: "env/dev", Region: "us-west-2", Insecure: true}, cfg)

	cfg, err = ParseS3URL("s3://prod-bucket")
	require.NoError(t, err)
	require.Equal(t, "s3.amazonaws.com", cfg.Endpoint)
	require.False(t, cfg.Insecure)

	_, err = ParseS3URL("/some/dir")
	require.Error(t, err)
}
