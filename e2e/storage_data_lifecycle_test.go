//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	posixFlagRO uint32 = 0o0
	posixFlagWO uint32 = 0o1
	posixFlagRW uint32 = 0o2
	posixFlagAP uint32 = 0o2000

	fallocHole uint32 = 0x2
	fallocKeep uint32 = 0x1
)

// TestStorageDataLifecycle_ReadWriteTruncatePunchHoleOrphanAndQuota 覆盖面向 live storage-service 的
// 数据层全生命周期黑盒端到端验收测试。
// 验证链路：
// CreateDisk -> Create -> 顺序与跨 64 MiB Chunk 边界 Write/Read -> 多层高频随机覆写 (>= 64 次 Compaction)
// -> O_APPEND 强制尾部追加 -> SetAttr 缩容/扩容与 Fallocate(PUNCH_HOLE) 区间打洞全零断言
// -> ENOSPC 配额拦截与 EISDIR / EBADF / EINVAL 错误契约校验
// -> Unlink 孤儿句柄持续读写 -> Release 孤儿回收与 StatFS used_bytes 归零 -> DeleteDisk
func TestStorageDataLifecycle_ReadWriteTruncatePunchHoleOrphanAndQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := testutil.NewStorageClient(connect.WithReadMaxBytes(10 * 1024 * 1024))

	// 1. CreateDisk: 创建独立主测试磁盘 (200 MiB)
	diskName := fmt.Sprintf("e2e_data_disk_%d", time.Now().UnixNano())
	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     diskName,
		MaxBytes: 200 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 应该成功")
	disk := createDiskResp.Msg.GetDisk()
	require.NotNil(t, disk, "新建 Disk 不能为空")
	diskID := disk.GetDiskId()
	rootIno := disk.GetRootIno()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: diskID,
		}))
	})

	// 2. 顺序与跨 64 MiB Chunk 边界 Write/Read
	t.Run("顺序与跨 64 MiB Chunk 边界 Write/Read 校验", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("cross_chunk_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		fileIno := createResp.Msg.GetInode().GetIno()
		fileFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: fileIno,
				Fh:  fileFh,
			}))
		}()

		// 2.1 顺序写入头部 64 KiB
		headData := bytes.Repeat([]byte{0x3A}, 64*1024)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: 0,
			Data:   headData,
		}))
		require.NoError(t, err)

		readHead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: 0,
			Size:   uint32(len(headData)),
		}))
		require.NoError(t, err)
		assert.Equal(t, headData, readHead.Msg.GetData(), "顺序写入数据回读必须完全一致")

		// 2.2 跨 64 MiB Chunk 边界写入 (64 MiB - 2048 处写入 4096 字节)
		chunkSize := uint64(64 * 1024 * 1024)
		crossOffset := chunkSize - 2048
		crossData := make([]byte, 4096)
		for i := range crossData {
			crossData[i] = byte(0x30 + (i % 60))
		}

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: crossOffset,
			Data:   crossData,
		}))
		require.NoError(t, err)

		readCross, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: crossOffset,
			Size:   uint32(len(crossData)),
		}))
		require.NoError(t, err)
		assert.Equal(t, crossData, readCross.Msg.GetData(), "跨 64 MiB 边界读取数据必须逐字节完全一致")

		// 2.3 验证跨 Chunk 前部稀疏区间为 0x00
		sparseBefore, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    fileIno,
			Fh:     fileFh,
			Offset: crossOffset - 4096,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), sparseBefore.Msg.GetData(), "跨块前部未写稀疏区间必须为 0x00")
	})

	// 3. 多层高频随机覆写 (>= 64 次小写入以触发服务端自动 Compaction) 后 Read 逐字节校验
	t.Run("多层高频随机覆写触发服务端自动 Compaction 后 Read 逐字节校验", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("compaction_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		compIno := createResp.Msg.GetInode().GetIno()
		compFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: compIno,
				Fh:  compFh,
			}))
		}()

		viewSize := 16384
		expectedView := make([]byte, viewSize)

		// 写入初始基底数据
		baseData := bytes.Repeat([]byte{0x5A}, viewSize)
		copy(expectedView, baseData)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    compIno,
			Fh:     compFh,
			Offset: 0,
			Data:   baseData,
		}))
		require.NoError(t, err)

		// 连续进行 68 次局部随机小写入（>= 64 次触发系统自动 Compaction）
		r := rand.New(rand.NewSource(12345))
		for i := 0; i < 68; i++ {
			writeOffset := uint64(r.Intn(viewSize - 256))
			writeLen := 64 + r.Intn(128)
			payload := make([]byte, writeLen)
			for j := range payload {
				payload[j] = byte(0x40 + (i % 20) + (j % 10))
			}

			copy(expectedView[writeOffset:], payload)

			_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
				Ino:    compIno,
				Fh:     compFh,
				Offset: writeOffset,
				Data:   payload,
			}))
			require.NoError(t, err)
		}

		// 等待少量时间使后台异步 Compaction 任务稳定
		time.Sleep(50 * time.Millisecond)

		// 回读整个 [0, viewSize) 区间，验证与内存期望视图逐字节完全一致
		readCompacted, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    compIno,
			Fh:     compFh,
			Offset: 0,
			Size:   uint32(viewSize),
		}))
		require.NoError(t, err)
		assert.Equal(t, expectedView, readCompacted.Msg.GetData(), "高频覆写触发 Compaction 后读取数据必须与期望内存视图完全一致")
	})

	// 4. O_APPEND 强制尾部追加
	t.Run("O_APPEND 强制尾部追加", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("append_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		appIno := createResp.Msg.GetInode().GetIno()
		appFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: appIno,
				Fh:  appFh,
			}))
		}()

		// 写入初始 1024 字节
		initData := bytes.Repeat([]byte{0x7E}, 1024)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    appIno,
			Fh:     appFh,
			Offset: 0,
			Data:   initData,
		}))
		require.NoError(t, err)

		// 以 O_WRONLY | O_APPEND 打开新句柄
		openAppendResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   appIno,
			Flags: posixFlagWO | posixFlagAP,
		}))
		require.NoError(t, err)
		appendOnlyFh := openAppendResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: appIno,
				Fh:  appendOnlyFh,
			}))
		}()

		// 故意传 Offset: 0，服务端必须强制追加至尾部 (offset = 1024)
		appendPayload := []byte("TAIL_APPEND_PAYLOAD_E2E")
		writeAppendResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    appIno,
			Fh:     appendOnlyFh,
			Offset: 0,
			Data:   appendPayload,
		}))
		require.NoError(t, err)
		assert.Equal(t, uint64(1024+len(appendPayload)), writeAppendResp.Msg.GetInode().GetAttr().GetSize())

		// 读取追加位置数据校验
		readTail, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    appIno,
			Fh:     appFh,
			Offset: 1024,
			Size:   uint32(len(appendPayload)),
		}))
		require.NoError(t, err)
		assert.Equal(t, appendPayload, readTail.Msg.GetData(), "追加模式写入内容必须精确追加在旧文件末尾")
	})

	// 5. SetAttr 缩容/扩容与 Fallocate(PUNCH_HOLE) 区间打洞零字节验证
	t.Run("SetAttr 缩容扩容与 Fallocate 打洞零字节验证", func(t *testing.T) {
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("trunc_punch_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		tpIno := createResp.Msg.GetInode().GetIno()
		tpFh := createResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: tpIno,
				Fh:  tpFh,
			}))
		}()

		// 5.1 写入 8192 字节已知数据 (0x66)
		fullData := bytes.Repeat([]byte{0x66}, 8192)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    tpIno,
			Fh:     tpFh,
			Offset: 0,
			Data:   fullData,
		}))
		require.NoError(t, err)

		// 5.2 缩容截断至 4096 字节
		setAttr1, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  tpIno,
			Size: proto.Uint64(4096),
		}))
		require.NoError(t, err)
		assert.Equal(t, uint64(4096), setAttr1.Msg.GetInode().GetAttr().GetSize())

		// 5.3 扩容回 8192 字节
		setAttr2, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
			Ino:  tpIno,
			Size: proto.Uint64(8192),
		}))
		require.NoError(t, err)
		assert.Equal(t, uint64(8192), setAttr2.Msg.GetInode().GetAttr().GetSize())

		// 校验 [4096, 8192) 全为 0x00，绝无旧数据复活
		readTailHole, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    tpIno,
			Fh:     tpFh,
			Offset: 4096,
			Size:   4096,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 4096), readTailHole.Msg.GetData(), "缩容后扩容的截断区间回读必须全部为 0x00")

		// 5.4 Fallocate(PUNCH_HOLE) 局部打洞：在 [1024, 3072) 打洞 (length = 2048)
		_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
			Ino:    tpIno,
			Fh:     tpFh,
			Offset: 1024,
			Length: 2048,
			Mode:   fallocHole | fallocKeep,
		}))
		require.NoError(t, err)

		// 验证打洞区间回读全为 0x00
		readHole, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    tpIno,
			Fh:     tpFh,
			Offset: 1024,
			Size:   2048,
		}))
		require.NoError(t, err)
		assert.Equal(t, make([]byte, 2048), readHole.Msg.GetData(), "打洞区间回读必须全部为 0x00")

		// 验证头部 [0, 1024) 保持原样 0x66
		readHead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    tpIno,
			Fh:     tpFh,
			Offset: 0,
			Size:   1024,
		}))
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte{0x66}, 1024), readHead.Msg.GetData())
	})

	// 6. ENOSPC 配额拦截与 EISDIR / EBADF / EINVAL 错误契约校验
	t.Run("错误契约校验 (EISDIR, EBADF, EINVAL, ENOSPC)", func(t *testing.T) {
		// 6.1 EISDIR: 目录读写
		openDirResp, err := client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:   rootIno,
			Flags: posixFlagRO,
		}))
		require.NoError(t, err)
		dirFh := openDirResp.Msg.GetFh()
		defer func() {
			_, _ = client.ReleaseDir(context.Background(), connect.NewRequest(&storagev1.ReleaseDirRequest{
				Ino: rootIno,
				Fh:  dirFh,
			}))
		}()

		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    rootIno,
			Fh:     dirFh,
			Offset: 0,
			Size:   1024,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EISDIR")

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    rootIno,
			Fh:     dirFh,
			Offset: 0,
			Data:   []byte("test_dir_write"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EISDIR")

		// 6.2 EBADF: 无效句柄与读写权限不匹配
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("ebadf_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		ebadfIno := createResp.Msg.GetInode().GetIno()

		// 只读句柄写
		openROResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   ebadfIno,
			Flags: posixFlagRO,
		}))
		require.NoError(t, err)
		roFh := openROResp.Msg.GetFh()

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ebadfIno,
			Fh:     roFh,
			Offset: 0,
			Data:   []byte("should_fail"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 只写句柄读
		openWOResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   ebadfIno,
			Flags: posixFlagWO,
		}))
		require.NoError(t, err)
		woFh := openWOResp.Msg.GetFh()

		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ebadfIno,
			Fh:     woFh,
			Offset: 0,
			Size:   10,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 句柄关闭后调用
		_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{Ino: ebadfIno, Fh: roFh}))
		_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{Ino: ebadfIno, Fh: woFh}))

		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ebadfIno,
			Fh:     roFh,
			Offset: 0,
			Size:   10,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 6.3 EINVAL: 非法参数
		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    0,
			Fh:     1,
			Offset: 0,
			Size:   10,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    0,
			Fh:     1,
			Offset: 0,
			Data:   []byte("test"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// 6.4 ENOSPC: 超配额拦截与已用空间原子回滚
		quotaDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("quota_e2e_%d", time.Now().UnixNano()),
			MaxBytes: 4096,
		}))
		require.NoError(t, err)
		qDisk := quotaDiskResp.Msg.GetDisk()
		defer func() {
			_, _ = client.DeleteDisk(context.Background(), connect.NewRequest(&storagev1.DeleteDiskRequest{
				DiskId: qDisk.GetDiskId(),
			}))
		}()

		qCreateResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: qDisk.GetRootIno(),
			Name:      []byte("quota_file.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		qIno := qCreateResp.Msg.GetInode().GetIno()
		qFh := qCreateResp.Msg.GetFh()
		defer func() {
			_, _ = client.Release(context.Background(), connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: qIno,
				Fh:  qFh,
			}))
		}()

		// 首次合规写入 2048 字节
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    qIno,
			Fh:     qFh,
			Offset: 0,
			Data:   bytes.Repeat([]byte{0x12}, 2048),
		}))
		require.NoError(t, err)

		statBefore, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: qDisk.GetRootIno()}))
		require.NoError(t, err)
		assert.Equal(t, uint64(2048), statBefore.Msg.GetUsedBytes())

		// 超出配额写入 (2048 + 4096 = 6144 > 4096)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    qIno,
			Fh:     qFh,
			Offset: 2048,
			Data:   bytes.Repeat([]byte{0x34}, 4096),
		}))
		testutil.RequireConnectError(t, err, connect.CodeResourceExhausted, "ENOSPC")

		// 验证 StatFS 已用空间未被污染，精确回滚至 2048
		statAfter, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: qDisk.GetRootIno()}))
		require.NoError(t, err)
		assert.Equal(t, uint64(2048), statAfter.Msg.GetUsedBytes(), "超配额写入拦截后 StatFS used_bytes 必须原子回滚")
	})

	// 7. Unlink 孤儿句柄持续读写 -> Release 孤儿回收与 StatFS used_bytes 归零 -> DeleteDisk
	t.Run("Unlink 孤儿句柄持续读写 -> Release 孤儿回收与 StatFS used_bytes 归零", func(t *testing.T) {
		// 创建专门测试孤儿生命周期的独立磁盘 (20 MiB)
		orphanDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("orphan_e2e_%d", time.Now().UnixNano()),
			MaxBytes: 20 * 1024 * 1024,
		}))
		require.NoError(t, err)
		oDisk := orphanDiskResp.Msg.GetDisk()
		oRootIno := oDisk.GetRootIno()
		t.Cleanup(func() {
			_, _ = client.DeleteDisk(context.Background(), connect.NewRequest(&storagev1.DeleteDiskRequest{
				DiskId: oDisk.GetDiskId(),
			}))
		})

		// 创建常规文件
		oCreateResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: oRootIno,
			Name:      []byte("orphan_doc.dat"),
			Mode:      0o100644,
			Flags:     posixFlagRW,
		}))
		require.NoError(t, err)
		oIno := oCreateResp.Msg.GetInode().GetIno()
		oFh := oCreateResp.Msg.GetFh()

		// 初始写入 4096 字节
		firstPayload := bytes.Repeat([]byte{0x4F}, 4096)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    oIno,
			Fh:     oFh,
			Offset: 0,
			Data:   firstPayload,
		}))
		require.NoError(t, err)

		// 检查 StatFS used_bytes 为 4096
		stat1, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: oRootIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(4096), stat1.Msg.GetUsedBytes())

		// 在句柄打开状态下调用 Unlink
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: oRootIno,
			Name:      []byte("orphan_doc.dat"),
		}))
		require.NoError(t, err)

		// 校验成为孤儿节点 (nlink == 0)
		attrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: oIno}))
		require.NoError(t, err)
		assert.Equal(t, uint32(0), attrResp.Msg.GetInode().GetAttr().GetNlink(), "Unlink 后孤儿 Inode nlink 必须为 0")

		// 孤儿节点继续 Write 2048 字节
		secondPayload := bytes.Repeat([]byte{0x52}, 2048)
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    oIno,
			Fh:     oFh,
			Offset: 4096,
			Data:   secondPayload,
		}))
		require.NoError(t, err, "持有活跃句柄的孤儿节点必须能继续写入")

		// 孤儿节点继续 Read 全部 6144 字节
		readAll, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    oIno,
			Fh:     oFh,
			Offset: 0,
			Size:   6144,
		}))
		require.NoError(t, err, "持有活跃句柄的孤儿节点必须能继续读取")
		assert.Equal(t, append(firstPayload, secondPayload...), readAll.Msg.GetData())

		// 校验 StatFS used_bytes 递增为 6144
		stat2, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: oRootIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(6144), stat2.Msg.GetUsedBytes())

		// Release 关闭孤儿文件的最后一个句柄，触发物理销毁
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: oIno,
			Fh:  oFh,
		}))
		require.NoError(t, err)

		// 校验孤儿销毁后 StatFS used_bytes 完全归零！
		stat3, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: oRootIno}))
		require.NoError(t, err)
		assert.Equal(t, uint64(0), stat3.Msg.GetUsedBytes(), "孤儿文件句柄全部关闭并销毁后 StatFS used_bytes 必须归零")

		// DeleteDisk 销毁整盘
		_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: oDisk.GetDiskId(),
		}))
		require.NoError(t, err)
	})
}
