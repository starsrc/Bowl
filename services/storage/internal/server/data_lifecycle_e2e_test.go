package server_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

const (
	posixO_RDONLY uint32 = 0o0
	posixO_WRONLY uint32 = 0o1
	posixO_RDWR   uint32 = 0o2
	posixO_APPEND uint32 = 0o2000

	fallocPunchHole uint32 = 0x2
	fallocKeepSize  uint32 = 0x1
)

func getLifecycleE2EDSN(ctx context.Context) string {
	if dsn := os.Getenv("STORAGE_TEST_DSN"); dsn != "" {
		return dsn
	}
	for _, port := range []string{"5432", "9432"} {
		dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%s/bowl?sslmode=disable", port)
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err := pool.Ping(ctx); err == nil {
				pool.Close()
				return dsn
			}
			pool.Close()
		}
	}
	return "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
}

func setupLifecycleDataServer(t *testing.T) (storagev1connect.StorageServiceClient, meta.Manager, db.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dsn := getLifecycleE2EDSN(ctx)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过数据生命周期集成测试 (连接初始化失败): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("跳过数据生命周期集成测试 (ping 失败): %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

	cfg := defaultTestConfig()
	// 设置为 10 MiB 以容纳单次 4 MiB 的满载 WriteRequest 载荷
	cfg.ServerReadMaxBytes = 10 * 1024 * 1024

	srv, err := server.New(server.Params{
		Config:      cfg,
		MetaManager: mgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	client := storagev1connect.NewStorageServiceClient(
		http.DefaultClient,
		ts.URL,
		connect.WithReadMaxBytes(10*1024*1024),
	)
	return client, mgr, store, dataDir
}

func localBlockPath(dataDir string, sliceID uint64, blockIdx uint32, blockSize uint32) string {
	return filepath.Join(
		dataDir,
		"chunks",
		fmt.Sprintf("%02X", sliceID%256),
		fmt.Sprintf("%d", sliceID/1000/1000),
		fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize),
	)
}

func requireE2EConnectError(t testing.TB, err error, wantCode connect.Code, wantReason string) {
	t.Helper()
	require.Error(t, err, "预期产生 ConnectRPC 错误，实际为 nil")
	assert.Equal(t, wantCode, connect.CodeOf(err), "状态码不匹配: want %v, got %v (%v)", wantCode, connect.CodeOf(err), err)
	var connectErr *connect.Error
	require.True(t, errors.As(err, &connectErr), "应为 *connect.Error")
	for _, detail := range connectErr.Details() {
		val, valErr := detail.Value()
		require.NoError(t, valErr)
		if info, ok := val.(*errdetails.ErrorInfo); ok {
			assert.Equal(t, "bowl.storage", info.Domain)
			assert.Equal(t, wantReason, info.Reason)
			return
		}
	}
	t.Fatalf("错误未包含 domain=bowl.storage, reason=%s 的 ErrorInfo: %v", wantReason, err)
}

// TestServer_DataLifecycle_ErrorContracts 验证 ConnectRPC 全栈客户端驱动的错误码与 ErrorInfo 契约。
func TestServer_DataLifecycle_ErrorContracts(t *testing.T) {
	client, _, _, dataDir := setupLifecycleDataServer(t)
	ctx := context.Background()

	// 1. 创建测试磁盘与目录
	diskName := fmt.Sprintf("err_contract_disk_%d", time.Now().UnixNano())
	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     diskName,
		MaxBytes: 10 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := createDiskResp.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	t.Cleanup(func() {
		_, _ = client.DeleteDisk(context.Background(), connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: disk.GetDiskId(),
		}))
	})

	// 打开目录句柄
	openDirResp, err := client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
		Ino:   rootIno,
		Flags: posixO_RDONLY,
	}))
	require.NoError(t, err)
	dirFh := openDirResp.Msg.GetFh()
	t.Cleanup(func() {
		_, _ = client.ReleaseDir(context.Background(), connect.NewRequest(&storagev1.ReleaseDirRequest{
			Ino: rootIno,
			Fh:  dirFh,
		}))
	})

	t.Run("对目录调用 Read/Write 返回 FailedPrecondition (EISDIR)", func(t *testing.T) {
		// Read 目录
		_, readErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    rootIno,
			Fh:     dirFh,
			Offset: 0,
			Size:   1024,
		}))
		requireE2EConnectError(t, readErr, connect.CodeFailedPrecondition, "EISDIR")

		// Write 目录
		_, writeErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    rootIno,
			Fh:     dirFh,
			Offset: 0,
			Data:   []byte("cannot write to directory"),
		}))
		requireE2EConnectError(t, writeErr, connect.CodeFailedPrecondition, "EISDIR")
	})

	t.Run("使用无效/已关闭句柄或只读句柄 Write / 只写句柄 Read 返回 FailedPrecondition (EBADF)", func(t *testing.T) {
		// 创建常规文件
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("ebadf_test.txt"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		fileIno := createResp.Msg.GetInode().GetIno()

		// 1. 无效句柄 (fh = 99999999) 调用 Read / Write
		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     99999999,
			Offset: 0,
			Size:   10,
		}))
		requireE2EConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    fileIno,
			Fh:     99999999,
			Offset: 0,
			Data:   []byte("invalid fh"),
		}))
		requireE2EConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 2. 以只读模式 (O_RDONLY) 打开文件，对其执行 Write
		roResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   fileIno,
			Flags: posixO_RDONLY,
		}))
		require.NoError(t, err)
		roFh := roResp.Msg.GetFh()

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    fileIno,
			Fh:     roFh,
			Offset: 0,
			Data:   []byte("read only violation"),
		}))
		requireE2EConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 3. 以只写模式 (O_WRONLY) 打开文件，对其执行 Read
		woResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   fileIno,
			Flags: posixO_WRONLY,
		}))
		require.NoError(t, err)
		woFh := woResp.Msg.GetFh()

		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     woFh,
			Offset: 0,
			Size:   10,
		}))
		requireE2EConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 4. 关闭句柄 (Release) 后再次调用
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: fileIno,
			Fh:  roFh,
		}))
		require.NoError(t, err)

		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     roFh,
			Offset: 0,
			Size:   10,
		}))
		requireE2EConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 清理只写句柄
		_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: fileIno,
			Fh:  woFh,
		}))
	})

	t.Run("非法参数返回 InvalidArgument (EINVAL)", func(t *testing.T) {
		// ReadRequest ino 为 0
		_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    0,
			Fh:     1,
			Offset: 0,
			Size:   10,
		}))
		requireE2EConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// WriteRequest ino 为 0
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    0,
			Fh:     1,
			Offset: 0,
			Data:   []byte("test"),
		}))
		requireE2EConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")
	})

	t.Run("超出磁盘 max_bytes 配额写入返回 ResourceExhausted (ENOSPC) 且 StatFS 已用空间与本地物理块回滚零残留", func(t *testing.T) {
		// 创建容量严格受限的磁盘 (配额 4096 字节)
		quotaDiskName := fmt.Sprintf("quota_disk_%d", time.Now().UnixNano())
		quotaDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     quotaDiskName,
			MaxBytes: 4096,
		}))
		require.NoError(t, err)
		qDisk := quotaDiskResp.Msg.GetDisk()
		qRootIno := qDisk.GetRootIno()
		t.Cleanup(func() {
			_, _ = client.DeleteDisk(context.Background(), connect.NewRequest(&storagev1.DeleteDiskRequest{
				DiskId: qDisk.GetDiskId(),
			}))
		})

		// 创建文件并获取可写句柄
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: qRootIno,
			Name:      []byte("quota_file.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		qIno := createResp.Msg.GetInode().GetIno()
		qFh := createResp.Msg.GetFh()

		// 首次合规写入 2048 字节
		initialPayload := bytes.Repeat([]byte{0x55}, 2048)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    qIno,
			Fh:     qFh,
			Offset: 0,
			Data:   initialPayload,
		}))
		require.NoError(t, err)

		// 检查 StatFS 已用空间为 2048
		stat1, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: qRootIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(2048), stat1.Msg.GetUsedBytes())

		// 记录当前磁盘上物理块文件清单
		entriesBefore, err := os.ReadDir(dataDir)
		require.NoError(t, err)
		fileCountBefore := len(entriesBefore)

		// 尝试写入 4096 字节，超出配额上限 (2048 + 4096 = 6144 > 4096)
		overflowPayload := bytes.Repeat([]byte{0x77}, 4096)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    qIno,
			Fh:     qFh,
			Offset: 2048,
			Data:   overflowPayload,
		}))
		requireE2EConnectError(t, err, connect.CodeResourceExhausted, "ENOSPC")

		// 验证 StatFS 已用空间未被污染，依然精确为 2048
		stat2, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: qRootIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(2048), stat2.Msg.GetUsedBytes(), "超配额写入失败后 StatFS used_bytes 必须原子回滚，零残留")

		// 验证 dataDir 下物理块文件未产生任何残留孤儿块
		entriesAfter, err := os.ReadDir(dataDir)
		require.NoError(t, err)
		assert.Equal(t, fileCountBefore, len(entriesAfter), "超配额写入失败后临时写入的物理块必须被彻底删除，零孤儿文件残留")
	})
}

// TestServer_DataLifecycle_FullFlow 验证通过 ConnectRPC 客户端驱动的数据层全生命周期黑盒测试。
func TestServer_DataLifecycle_FullFlow(t *testing.T) {
	client, mgr, store, dataDir := setupLifecycleDataServer(t)
	ctx := context.Background()

	// 1. CreateDisk: 创建商业化独立磁盘 (200 MiB)
	diskName := fmt.Sprintf("lifecycle_flow_%d", time.Now().UnixNano())
	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     diskName,
		MaxBytes: 200 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := createDiskResp.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	t.Cleanup(func() {
		_, _ = client.DeleteDisk(context.Background(), connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: disk.GetDiskId(),
		}))
	})

	// 2. Create: 创建常规文件
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: rootIno,
		Name:      []byte("data_lifecycle.dat"),
		Mode:      0o100644,
		Flags:     posixO_RDWR,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()
	fileFh := createResp.Msg.GetFh()

	// 3. 顺序写入、跨 4 MiB Block 多次写入与稀疏全零断言
	t.Run("跨 4 MiB Block 写入与稀疏区间返回 0x00", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("cross_block.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		cbIno := createResp.Msg.GetInode().GetIno()
		cbFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: cbIno,
				Fh:  cbFh,
			}))
		}()

		// 在 offset 0 写入 1 MiB (1048576)
		part1 := bytes.Repeat([]byte{0x11}, 1024*1024)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    cbIno,
			Fh:     cbFh,
			Offset: 0,
			Data:   part1,
		}))
		require.NoError(t, err)

		// 跨越 4 MiB (4194304) Block 边界写入：在 4 MiB - 1024 处写入 4096 字节 (覆盖 4 MiB 前后)
		fourMB := uint64(4 * 1024 * 1024)
		crossBlockData := bytes.Repeat([]byte{0x22}, 4096)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    cbIno,
			Fh:     cbFh,
			Offset: fourMB - 1024,
			Data:   crossBlockData,
		}))
		require.NoError(t, err)

		// 验证 offset 0 回读
		readResp1, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    cbIno,
			Fh:     cbFh,
			Offset: 0,
			Size:   uint32(len(part1)),
		}))
		require.NoError(t, err)
		assert.Equal(t, part1, readResp1.Msg.GetData())

		// 读取文件内部未写入的稀疏空洞区间 [1 MiB, 4 MiB - 1024)，验证逐字节返回 0x00
		sparseRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    cbIno,
			Fh:     cbFh,
			Offset: 1024 * 1024,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), sparseRead.Msg.GetData(), "未写入稀疏区间必须逐字节返回 0x00")

		// 跨 4 MiB 边界读取
		readCrossBlock, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    cbIno,
			Fh:     cbFh,
			Offset: fourMB - 1024,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, crossBlockData, readCrossBlock.Msg.GetData(), "跨 4 MiB Block 边界回读必须一致")
	})

	// 4. 跨 64 MiB Chunk 边界写入与回读
	t.Run("跨 64 MiB Chunk 边界 (64 MiB - 2048 写入 4096 字节) 并逐字节校验", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("cross_chunk.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		ccIno := createResp.Msg.GetInode().GetIno()
		ccFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: ccIno,
				Fh:  ccFh,
			}))
		}()

		chunkSize := uint64(64 * 1024 * 1024)
		crossOffset := chunkSize - 2048
		crossData := make([]byte, 4096)
		for i := range crossData {
			crossData[i] = byte(0x20 + (i % 90))
		}

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ccIno,
			Fh:     ccFh,
			Offset: crossOffset,
			Data:   crossData,
		}))
		require.NoError(t, err)

		// 跨 Chunk 读取回写的数据
		readCross, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ccIno,
			Fh:     ccFh,
			Offset: crossOffset,
			Size:   uint32(len(crossData)),
		}))
		require.NoError(t, err)
		assert.Equal(t, crossData, readCross.Msg.GetData(), "跨 64 MiB 边界读取数据必须逐字节完全一致")

		// 检查跨 Chunk 前部的稀疏区间全为 0x00
		sparseBefore, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ccIno,
			Fh:     ccFh,
			Offset: crossOffset - 4096,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), sparseBefore.Msg.GetData(), "跨块前部未写稀疏区间必须为 0x00")
	})

	// 5. 多层随机覆盖 Write (连续覆写 >= 4 次触发 CompactChunk)
	t.Run("多层随机覆盖 Write 触发 CompactChunk 并逐字节比对内存视图", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("compact_file.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		cpIno := createResp.Msg.GetInode().GetIno()
		cpFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: cpIno,
				Fh:  cpFh,
			}))
		}()

		// 在 [0, 8192) 区间进行多次随机覆写
		viewSize := 8192
		expectedView := make([]byte, viewSize)

		// 先写入基底填满 [0, 8192)
		basePayload := bytes.Repeat([]byte{0x41}, viewSize)
		copy(expectedView, basePayload)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    cpIno,
			Fh:     cpFh,
			Offset: 0,
			Data:   basePayload,
		}))
		require.NoError(t, err)

		r := rand.New(rand.NewSource(42))
		for i := 0; i < 6; i++ {
			writeOffset := uint64(r.Intn(viewSize - 512))
			writeLen := 256 + r.Intn(256)
			writePayload := make([]byte, writeLen)
			for j := range writePayload {
				writePayload[j] = byte(0x30 + i + (j % 10))
			}

			// 更新内存期望视图
			copy(expectedView[writeOffset:], writePayload)

			// 通过 RPC 写入
			_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
				Ino:    cpIno,
				Fh:     cpFh,
				Offset: writeOffset,
				Data:   writePayload,
			}))
			require.NoError(t, err)
		}

		// 显式触发 CompactChunk 确保碎片合并完成
		err = mgr.CompactChunk(ctx, cpIno, 0)
		require.NoError(t, err, "显式 CompactChunk 碎片合并必须成功")

		// 逐字节验证合并后的内存视图
		readCompacted, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    cpIno,
			Fh:     cpFh,
			Offset: 0,
			Size:   uint32(viewSize),
		}))
		require.NoError(t, err)
		assert.Equal(t, expectedView, readCompacted.Msg.GetData(), "碎片合并后读取数据与内存期望视图必须完全一致")
	})

	// 6. Open(O_WRONLY | O_APPEND) 强制尾部追加
	t.Run("Open(O_WRONLY | O_APPEND) 忽略客户端 offset 强制追加到文件尾部", func(t *testing.T) {
		// 获取当前文件大小
		attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: fileIno}))
		require.NoError(t, err)
		curSize := attrResp.Msg.GetInode().GetAttr().GetSize()

		// 打开追加句柄
		openAppendResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   fileIno,
			Flags: posixO_WRONLY | posixO_APPEND,
		}))
		require.NoError(t, err)
		appendFh := openAppendResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: fileIno,
				Fh:  appendFh,
			}))
		}()

		// 故意传入 Offset: 0，期望服务端忽略该 offset 并强制追加至 curSize
		appendPayload := []byte("FORCED_APPEND_CONTENT_TEST")
		writeAppendResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    fileIno,
			Fh:     appendFh,
			Offset: 0,
			Data:   appendPayload,
		}))
		require.NoError(t, err)
		assert.Equal(t, curSize+uint64(len(appendPayload)), writeAppendResp.Msg.GetInode().GetAttr().GetSize())

		// 通过只读/读写句柄读取追加位置的数据
		readAppendResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: curSize,
			Size:   uint32(len(appendPayload)),
		}))
		require.NoError(t, err)
		assert.Equal(t, appendPayload, readAppendResp.Msg.GetData(), "追加模式写入内容必须精确追加在旧文件末尾")
	})

	// 7. SetAttr 非零缩容截断后再扩容回原大小（无旧数据复活）
	t.Run("SetAttr 非零缩容截断后再扩容回原大小，截断点之后全为 0x00", func(t *testing.T) {
		// 创建专门的测试文件
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("trunc_file.bin"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		truncIno := createResp.Msg.GetInode().GetIno()
		truncFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: truncIno,
				Fh:  truncFh,
			}))
		}()

		// 写入 8192 字节已知数据 (0xCC)
		fullPayload := bytes.Repeat([]byte{0xCC}, 8192)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    truncIno,
			Fh:     truncFh,
			Offset: 0,
			Data:   fullPayload,
		}))
		require.NoError(t, err)

		// 缩容截断至 4096 字节
		setAttrResp, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  truncIno,
			Size: proto.Uint64(4096),
		}))
		require.NoError(t, err)
		assert.Equal(t, uint64(4096), setAttrResp.Msg.GetInode().GetAttr().GetSize())

		// 再度扩容回 8192 字节
		setAttrResp2, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  truncIno,
			Size: proto.Uint64(8192),
		}))
		require.NoError(t, err)
		assert.Equal(t, uint64(8192), setAttrResp2.Msg.GetInode().GetAttr().GetSize())

		// 验证 [0, 4096) 保持 0xCC，而 [4096, 8192) 必须全为 0x00（绝不复活历史数据）
		headResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    truncIno,
			Fh:     truncFh,
			Offset: 0,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0xCC}, 4096), headResp.Msg.GetData())

		tailResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    truncIno,
			Fh:     truncFh,
			Offset: 4096,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), tailResp.Msg.GetData(), "截断后再扩容的尾部必须全部返回 0x00，绝不复活历史旧数据")
	})

	// 8. Fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE) 局部打洞
	t.Run("Fallocate 局部打洞验证打洞区间为 0x00 且 size 不变", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("punch_hole_file.bin"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		punchIno := createResp.Msg.GetInode().GetIno()
		punchFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: punchIno,
				Fh:  punchFh,
			}))
		}()

		// 写入 8192 字节初始数据 (0x88)
		initialData := bytes.Repeat([]byte{0x88}, 8192)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    punchIno,
			Fh:     punchFh,
			Offset: 0,
			Data:   initialData,
		}))
		require.NoError(t, err)

		// 在 [2048, 6144) 区间打洞 (length = 4096)
		_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
			Ino:    punchIno,
			Fh:     punchFh,
			Offset: 2048,
			Length: 4096,
			Mode:   fallocPunchHole | fallocKeepSize,
		}))
		require.NoError(t, err)

		// 验证文件大小依然为 8192 字节
		attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: punchIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(8192), attrResp.Msg.GetInode().GetAttr().GetSize(), "打洞操作后文件大小必须保持不变")

		// 验证头部 [0, 2048) 依然为 0x88
		headResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    punchIno,
			Fh:     punchFh,
			Offset: 0,
			Size:   2048,
		}))
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0x88}, 2048), headResp.Msg.GetData())

		// 验证打洞区间 [2048, 6144) 全为 0x00
		holeResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    punchIno,
			Fh:     punchFh,
			Offset: 2048,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), holeResp.Msg.GetData(), "打洞区间读取必须全为 0x00")

		// 验证尾部 [6144, 8192) 依然为 0x88
		tailResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    punchIno,
			Fh:     punchFh,
			Offset: 6144,
			Size:   2048,
		}))
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0x88}, 2048), tailResp.Msg.GetData())
	})

	// 9. 打开句柄期间执行 Unlink 转为 Orphan Inode，活跃句柄持续 Write/Read，Release 后 GC 回收
	t.Run("Orphan Inode 活跃句柄持续读写并在 Release/DeleteDisk 后经 GC 物理块彻底删除", func(t *testing.T) {
		// 9.1 创建孤儿测试文件
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("orphan_file.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		orphanIno := createResp.Msg.GetInode().GetIno()
		orphanFh := createResp.Msg.GetFh()

		// 写入 4096 字节
		initData := bytes.Repeat([]byte{0xEE}, 4096)
		writeResp1, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    orphanIno,
			Fh:     orphanFh,
			Offset: 0,
			Data:   initData,
		}))
		require.NoError(t, err)
		assert.Equal(t, uint32(4096), writeResp1.Msg.GetSize())

		// 获取孤儿文件的 sliceID 与对应物理块路径
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(orphanIno), Indx: 0})
		require.NoError(t, err)
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		require.NoError(t, err)
		orphanSliceID := slices[0].ID
		orphanBlockPath := localBlockPath(dataDir, orphanSliceID, 0, 4096)
		require.FileExists(t, orphanBlockPath, "写入后物理块文件必须存在")

		// 9.2 在句柄打开期间调用 Unlink 移除目录项
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: rootIno,
			Name:      []byte("orphan_file.dat"),
		}))
		require.NoError(t, err)

		// 校验 Inode 成为孤儿节点 (nlink == 0)
		attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: orphanIno}))
		require.NoError(t, err)
		assert.Equal(t, uint32(0), attrResp.Msg.GetInode().GetAttr().GetNlink(), "Unlink 后孤儿 Inode nlink 必须为 0")

		// 9.3 孤儿节点持有活跃句柄期间，继续执行 Write 与 Read
		extraData := bytes.Repeat([]byte{0xFF}, 2048)
		writeResp2, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    orphanIno,
			Fh:     orphanFh,
			Offset: 4096,
			Data:   extraData,
		}))
		require.NoError(t, err, "持有活跃句柄的孤儿节点必须能继续写入")
		assert.Equal(t, uint32(2048), writeResp2.Msg.GetSize())

		readAllResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    orphanIno,
			Fh:     orphanFh,
			Offset: 0,
			Size:   6144,
		}))
		require.NoError(t, err, "持有活跃句柄的孤儿节点必须能继续读取")
		assert.Equal(t, append(initData, extraData...), readAllResp.Msg.GetData())

		// 9.4 创建另一块用于验证 DeleteDisk 级联销毁的独立磁盘与文件
		cascadeDiskName := fmt.Sprintf("cascade_disk_%d", time.Now().UnixNano())
		cascadeDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     cascadeDiskName,
			MaxBytes: 10 * 1024 * 1024,
		}))
		require.NoError(t, err)
		cascadeDisk := cascadeDiskResp.Msg.GetDisk()

		cascadeCreateResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: cascadeDisk.GetRootIno(),
			Name:      []byte("cascade_file.dat"),
			Mode:      0o100644,
			Flags:     posixO_RDWR,
		}))
		require.NoError(t, err)
		cascadeIno := cascadeCreateResp.Msg.GetInode().GetIno()
		cascadeFh := cascadeCreateResp.Msg.GetFh()

		cascadePayload := bytes.Repeat([]byte{0x99}, 4096)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    cascadeIno,
			Fh:     cascadeFh,
			Offset: 0,
			Data:   cascadePayload,
		}))
		require.NoError(t, err)

		cascadeChunk, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(cascadeIno), Indx: 0})
		require.NoError(t, err)
		cascadeSlices, err := meta.DecodeSlices(cascadeChunk.Slices)
		require.NoError(t, err)
		cascadeSliceID := cascadeSlices[0].ID
		cascadeBlockPath := localBlockPath(dataDir, cascadeSliceID, 0, 4096)
		require.FileExists(t, cascadeBlockPath)

		// 9.5 关闭孤儿文件最后一个活跃句柄 -> 触发 Inode 物理删除与 slice_refs 引用归零
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: orphanIno,
			Fh:  orphanFh,
		}))
		require.NoError(t, err)

		// 9.6 销毁独立磁盘 -> 触发该盘下全部 Inode 的 chunks 清理与 slice_refs 引用归零
		_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: cascadeDisk.GetDiskId(),
		}))
		require.NoError(t, err)

		// 9.7 执行后台物理 GC 回收
		reclaimed, err := mgr.CollectGarbage(ctx, 1000)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, reclaimed, 2, "GC 必须成功回收孤儿文件与被删磁盘的所有物理块切片")

		// 9.8 验证对应切片的本地物理 Block 文件均已被彻底物理删除！
		assert.NoFileExists(t, orphanBlockPath, "孤儿文件在 Release 并执行 GC 后，其物理块文件必须被彻底删除")
		assert.NoFileExists(t, cascadeBlockPath, "被删磁盘在 DeleteDisk 并执行 GC 后，其物理块文件必须被彻底删除")
	})
}
