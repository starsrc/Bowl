package object

import (
	"bytes"
	"context"
	"fmt"
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

func TestBulkDelete(t *testing.T) {
	ctx := context.Background()
	s, err := CreateStorage("mem", "test-bucket", "", "", "")
	if err != nil {
		t.Fatalf("创建内存存储失败: %s", err)
	}

	// 准备 10 个测试对象
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("batch/obj_%d.bin", i)
		if err := s.Put(ctx, keys[i], bytes.NewReader([]byte(fmt.Sprintf("data-%d", i)))); err != nil {
			t.Fatalf("写入测试对象失败: %v", err)
		}
	}

	// 1. 批量删除前 5 个对象
	if err := s.DeleteBatch(ctx, keys[:5]); err != nil {
		t.Fatalf("DeleteBatch 批量删除失败: %v", err)
	}

	// 校验前 5 个已被删除，后 5 个仍存在
	for i := 0; i < 5; i++ {
		if _, err := s.Head(ctx, keys[i]); err == nil {
			t.Fatalf("对象 %s 应已被批量删除，但仍存在", keys[i])
		}
	}
	for i := 5; i < 10; i++ {
		if _, err := s.Head(ctx, keys[i]); err != nil {
			t.Fatalf("对象 %s 应存在，但查询报错: %v", keys[i], err)
		}
	}

	// 2. 按前缀批量删除剩余对象
	if err := s.DeletePrefix(ctx, "batch/"); err != nil {
		t.Fatalf("DeletePrefix 失败: %v", err)
	}

	for i := 5; i < 10; i++ {
		if _, err := s.Head(ctx, keys[i]); err == nil {
			t.Fatalf("对象 %s 应在 DeletePrefix 后被删除", keys[i])
		}
	}
}

func TestWithDiskPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	rawMem, err := CreateStorage("mem", "test-bucket", "", "", "")
	if err != nil {
		t.Fatalf("创建底层存储失败: %s", err)
	}

	diskA := "disk-tenant-alpha"
	diskB := "disk-tenant-beta"

	storeA := WithDiskPrefix(rawMem, diskA)
	storeB := WithDiskPrefix(rawMem, diskB)

	relKey := "000/0/1_0"
	payloadA := []byte("payload for tenant A")
	payloadB := []byte("payload for tenant B")

	if err := storeA.Put(ctx, relKey, bytes.NewReader(payloadA)); err != nil {
		t.Fatalf("Tenant A 写入失败: %v", err)
	}
	if err := storeB.Put(ctx, relKey, bytes.NewReader(payloadB)); err != nil {
		t.Fatalf("Tenant B 写入失败: %v", err)
	}

	// 验证底层键包含 chunks/<disk_id>/ 前缀且互不干扰
	expectedKeyA := FormatChunkKey(diskA, 0, 1, 0)
	expectedKeyB := FormatChunkKey(diskB, 0, 1, 0)

	objA, err := rawMem.Head(ctx, expectedKeyA)
	if err != nil {
		t.Fatalf("底座存储查询 Tenant A 格式化键失败: %v", err)
	}
	if objA.Size() != int64(len(payloadA)) {
		t.Fatalf("Tenant A 底座对象大小不符合: %d", objA.Size())
	}

	objB, err := rawMem.Head(ctx, expectedKeyB)
	if err != nil {
		t.Fatalf("底座存储查询 Tenant B 格式化键失败: %v", err)
	}
	if objB.Size() != int64(len(payloadB)) {
		t.Fatalf("Tenant B 底座对象大小不符合: %d", objB.Size())
	}

	// 租户 A 读取自己的数据
	rcA, err := storeA.Get(ctx, relKey, 0, -1)
	if err != nil {
		t.Fatalf("storeA 读取失败: %v", err)
	}
	dataA, _ := io.ReadAll(rcA)
	rcA.Close()
	if !bytes.Equal(dataA, payloadA) {
		t.Fatalf("storeA 读取数据不匹配")
	}

	// 租户 A 执行批量删除不会影响租户 B
	if err := storeA.DeleteBatch(ctx, []string{relKey}); err != nil {
		t.Fatalf("storeA 批量删除失败: %v", err)
	}

	if _, err := storeA.Head(ctx, relKey); err == nil {
		t.Fatal("storeA 删除后应不存在")
	}

	// 租户 B 依然存在
	if _, err := storeB.Head(ctx, relKey); err != nil {
		t.Fatalf("storeB 应当不受影响，但 Head 报错: %v", err)
	}
}
