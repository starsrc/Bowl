package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
)

const e2eChunkSize = 64 * 1024 * 1024

type e2eSlice struct {
	Pos  uint32
	ID   uint64
	Size uint32
	Off  uint32
	Len  uint32
}

func decodeE2ESlices(data []byte) ([]e2eSlice, error) {
	if len(data)%24 != 0 {
		return nil, fmt.Errorf("非 24 字节整数倍切片缓冲区: %d", len(data))
	}
	count := len(data) / 24
	res := make([]e2eSlice, 0, count)
	for i := 0; i < len(data); i += 24 {
		res = append(res, e2eSlice{
			Pos:  binary.BigEndian.Uint32(data[i : i+4]),
			ID:   binary.BigEndian.Uint64(data[i+4 : i+12]),
			Size: binary.BigEndian.Uint32(data[i+12 : i+16]),
			Off:  binary.BigEndian.Uint32(data[i+16 : i+20]),
			Len:  binary.BigEndian.Uint32(data[i+20 : i+24]),
		})
	}
	return res, nil
}

// TestE2E_ChunkDriver_FastIngestionAndBoundarySplitting 验证基于真实二进制服务的切片写入与 NVMe Staging 缓存通路
func TestE2E_ChunkDriver_FastIngestionAndBoundarySplitting(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化数据库连接池用于底层元数据账本校验
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 准备运行目录与 NVMe Staging 暂存目录
	baseDir := t.TempDir()
	binDir := filepath.Join(baseDir, "bin")
	dataDir := filepath.Join(baseDir, "data")
	stagingDir := filepath.Join(baseDir, "staging")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))

	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)

	diskBin := filepath.Join(binDir, "disk-service")
	storageBin := filepath.Join(binDir, "storage-service")

	// 3. 静态编译真实服务二进制
	cmdDiskBuild := exec.Command("go", "build", "-o", diskBin, "./services/disk/cmd")
	cmdDiskBuild.Dir = repoRoot
	out, err := cmdDiskBuild.CombinedOutput()
	require.NoError(t, err, "编译 disk-service 失败: %s", string(out))

	cmdStorageBuild := exec.Command("go", "build", "-o", storageBin, "./services/storage/cmd")
	cmdStorageBuild.Dir = repoRoot
	out, err = cmdStorageBuild.CombinedOutput()
	require.NoError(t, err, "编译 storage-service 失败: %s", string(out))

	// 4. 启动 DiskService 进程
	diskPort := getFreePort(t)
	diskAddr := fmt.Sprintf("http://127.0.0.1:%d", diskPort)

	ctxDisk, cancelDisk := context.WithCancel(context.Background())
	defer cancelDisk()

	cmdDisk := exec.CommandContext(ctxDisk, diskBin)
	cmdDisk.Dir = repoRoot
	cmdDisk.Env = append(os.Environ(),
		fmt.Sprintf("SERVER_PORT=%d", diskPort),
		fmt.Sprintf("PORT=%d", diskPort),
		"DB_DSN="+dsn,
		"DATA_DIR="+dataDir,
	)
	cmdDisk.Stdout = os.Stdout
	cmdDisk.Stderr = os.Stderr
	require.NoError(t, cmdDisk.Start())
	defer func() {
		cancelDisk()
		_ = cmdDisk.Wait()
	}()

	waitForHTTP(t, diskAddr, 10*time.Second)

	// 5. 启动 StorageService 进程 (启用 STORAGE_DRIVER=chunk 与 NVMe Staging)
	storagePort := getFreePort(t)
	storageAddr := fmt.Sprintf("http://127.0.0.1:%d", storagePort)

	ctxStorage, cancelStorage := context.WithCancel(context.Background())
	defer cancelStorage()

	cmdStorage := exec.CommandContext(ctxStorage, storageBin)
	cmdStorage.Dir = repoRoot
	cmdStorage.Env = append(os.Environ(),
		fmt.Sprintf("PORT=%d", storagePort),
		fmt.Sprintf("SERVER_PORT=%d", storagePort),
		"DB_DSN="+dsn,
		"DATA_DIR="+dataDir,
		"STAGING_DIR="+stagingDir,
		"STORAGE_DRIVER=chunk",
		"DISK_SERVICE_ADDR="+diskAddr,
	)
	cmdStorage.Stdout = os.Stdout
	cmdStorage.Stderr = os.Stderr
	require.NoError(t, cmdStorage.Start())
	defer func() {
		cancelStorage()
		_ = cmdStorage.Wait()
	}()

	waitForHTTP(t, storageAddr, 10*time.Second)

	// 6. 构造 Connect-RPC 客户端并创建虚拟磁盘
	httpClient := &http.Client{Timeout: 10 * time.Second}
	diskConnectClient := connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr))
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	storageConnectClient := connect.NewClient(connecthttp.NewTransport(httpClient, storageAddr))
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	createDiskResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-chunk-ingestion-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskIDStr := createDiskResp.GetDisk().GetDiskId()
	parsedDiskUUID, err := uuid.Parse(diskIDStr)
	require.NoError(t, err)

	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	defer func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	clientID := "e2e-chunk-writer-client"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskIDStr,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 7. 创建测试文件
	createResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "chunk-e2e-payload.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 8. 场景 A: 增量 128KB 载荷极速写入 (<10ms ACK)
	payloadSize := 128 * 1024
	payload128KB := make([]byte, payloadSize)
	for i := range payload128KB {
		payload128KB[i] = byte((i * 47) % 256)
	}

	start := time.Now()
	writeResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload128KB,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Equal(t, uint32(payloadSize), writeResp.GetWritten())
	assert.Less(t, elapsed, 10*time.Millisecond, "端到端 128KB 写入耗时必须在 10ms 以内，实际为: %v", elapsed)

	// 9. 校验 NVMe Staging 缓存目录中生成了物理切片文件
	stagingSlicePath := filepath.Join(stagingDir, diskIDStr, "slice_1.bin")
	fi, err := os.Stat(stagingSlicePath)
	require.NoError(t, err, "NVMe Staging 切片文件必须存在: %s", stagingSlicePath)
	assert.Equal(t, int64(payloadSize), fi.Size())

	// 10. 校验 PostgreSQL chunks 表中切片元数据已记录
	var rawSlices []byte
	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName)
	err = pool.QueryRow(ctx, queryChunks, int64(ino)).Scan(&rawSlices)
	require.NoError(t, err)
	slices, err := decodeE2ESlices(rawSlices)
	require.NoError(t, err)
	require.Len(t, slices, 1)
	assert.Equal(t, uint32(0), slices[0].Pos)
	assert.Equal(t, uint64(1), slices[0].ID)
	assert.Equal(t, uint32(payloadSize), slices[0].Size)
	assert.Equal(t, uint32(payloadSize), slices[0].Len)

	// 11. 校验 PostgreSQL slice_refs 引用账本
	var refCount int32
	var refSize int32
	queryRef := fmt.Sprintf(`SELECT refs, size FROM "%s".slice_refs WHERE id = 1`, schemaName)
	err = pool.QueryRow(ctx, queryRef).Scan(&refCount, &refSize)
	require.NoError(t, err)
	assert.Equal(t, int32(1), refCount)
	assert.Equal(t, int32(payloadSize), refSize)

	// 12. 场景 B: 跨越 64MB Chunk 边界切分写入
	boundaryOffset := uint64(e2eChunkSize - 64*1024)
	boundaryPayload := make([]byte, payloadSize)
	for i := range boundaryPayload {
		boundaryPayload[i] = byte((i * 101) % 256)
	}

	startBoundary := time.Now()
	writeBoundaryResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    boundaryOffset,
		Data:      boundaryPayload,
	})
	elapsedBoundary := time.Since(startBoundary)
	require.NoError(t, err)
	assert.Equal(t, uint32(payloadSize), writeBoundaryResp.GetWritten())
	assert.Less(t, elapsedBoundary, 10*time.Millisecond)

	// 校验跨越边界切分生成的 Chunk 0 与 Chunk 1 切片元数据
	var rawSlicesChunk0, rawSlicesChunk1 []byte
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName), int64(ino)).Scan(&rawSlicesChunk0)
	require.NoError(t, err)
	slicesChunk0, err := decodeE2ESlices(rawSlicesChunk0)
	require.NoError(t, err)
	require.Len(t, slicesChunk0, 2) // 包含场景 A 的 1 个切片 + 跨边界第 1 半段切片
	assert.Equal(t, uint32(e2eChunkSize-64*1024), slicesChunk0[1].Pos)
	assert.Equal(t, uint32(64*1024), slicesChunk0[1].Len)

	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 1`, schemaName), int64(ino)).Scan(&rawSlicesChunk1)
	require.NoError(t, err)
	slicesChunk1, err := decodeE2ESlices(rawSlicesChunk1)
	require.NoError(t, err)
	require.Len(t, slicesChunk1, 1) // 跨边界第 2 半段切片
	assert.Equal(t, uint32(0), slicesChunk1[0].Pos)
	assert.Equal(t, uint32(64*1024), slicesChunk1[0].Len)

	// 13. 回读数据校验完整性
	readResp, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    boundaryOffset,
		Size:      uint32(payloadSize),
	})
	require.NoError(t, err)
	assert.Equal(t, boundaryPayload, readResp.GetData())

	// 14. 释放文件句柄
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}
