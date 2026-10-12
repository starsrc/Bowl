package object

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func testObjectStorage(t *testing.T, s ObjectStorage) {
	ctx := context.Background()

	// 1. Put
	key := "test/data.bin"
	payload := []byte("hello juicefs pure-go object storage")
	if err := s.Put(ctx, key, bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put 写入失败: %s", err)
	}

	// 2. Head
	obj, err := s.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head 查询元数据失败: %s", err)
	}
	if obj.Size() != int64(len(payload)) {
		t.Fatalf("对象大小不匹配: 期望 %d, 实际 %d", len(payload), obj.Size())
	}

	// 3. Get 完整读取
	rc, err := s.Get(ctx, key, 0, -1)
	if err != nil {
		t.Fatalf("Get 读取失败: %s", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll 读取失败: %s", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("读取内容不一致: 期望 %s, 实际 %s", string(payload), string(data))
	}

	// 4. Get 局部区间读取 (Range Read)
	rcRange, err := s.Get(ctx, key, 6, 7) // "juicefs"
	if err != nil {
		t.Fatalf("Get 范围读取失败: %s", err)
	}
	rangeData, err := io.ReadAll(rcRange)
	rcRange.Close()
	if err != nil {
		t.Fatalf("ReadAll 局部区间读取失败: %s", err)
	}
	if string(rangeData) != "juicefs" {
		t.Fatalf("区间读取不符合预期: 期望 'juicefs', 实际 '%s'", string(rangeData))
	}

	// 5. Delete
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("Delete 失败: %s", err)
	}
	if _, err := s.Head(ctx, key); err == nil {
		t.Fatal("删除后 Head 仍能查询到对象，应当报错")
	}
}

func TestMemStorage(t *testing.T) {
	s, err := CreateStorage("mem", "test-bucket", "", "", "")
	if err != nil {
		t.Fatalf("创建内存存储失败: %s", err)
	}
	testObjectStorage(t, s)
}

func TestFileStorage(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "juicefs_test_filestore")
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	s, err := CreateStorage("file", dir+"/", "", "", "")
	if err != nil {
		t.Fatalf("创建文件存储失败: %s", err)
	}
	testObjectStorage(t, s)
}

func TestPrefixStorage(t *testing.T) {
	mem, err := CreateStorage("mem", "test-bucket", "", "", "")
	if err != nil {
		t.Fatalf("创建内存存储失败: %s", err)
	}
	prefixed := WithPrefix(mem, "scoped/prefix/")
	testObjectStorage(t, prefixed)
}
