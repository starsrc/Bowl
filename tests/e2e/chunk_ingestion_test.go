package e2e

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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

// TestE2E_ChunkDriver_Read_JuiceFSOverlayAndColdS3Fallback 验证基于真实二进制服务的切片图层覆盖读取、稀疏空洞填充与 S3 透明 HTTP Range 回源全通路
func TestE2E_ChunkDriver_Read_JuiceFSOverlayAndColdS3Fallback(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化数据库连接池
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 初始化 MinIO 客户端并确保 bucket 存在
	minioEndpoint := "127.0.0.1:9000"
	minioBucket := "bowl-data"
	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	require.NoError(t, err)

	bucketExists, err := minioClient.BucketExists(ctx, minioBucket)
	require.NoError(t, err)
	if !bucketExists {
		err = minioClient.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	// 3. 准备独立测试目录
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

	// 4. 静态编译二进制
	cmdDiskBuild := exec.Command("go", "build", "-o", diskBin, "./services/disk/cmd")
	cmdDiskBuild.Dir = repoRoot
	out, err := cmdDiskBuild.CombinedOutput()
	require.NoError(t, err, "编译 disk-service 失败: %s", string(out))

	cmdStorageBuild := exec.Command("go", "build", "-o", storageBin, "./services/storage/cmd")
	cmdStorageBuild.Dir = repoRoot
	out, err = cmdStorageBuild.CombinedOutput()
	require.NoError(t, err, "编译 storage-service 失败: %s", string(out))

	// 5. 启动 DiskService 进程
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

	// 6. 启动 StorageService 进程 (启用 STORAGE_DRIVER=chunk 与 S3 回源)
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
		"S3_ENABLED=true",
		"S3_ENDPOINT=http://127.0.0.1:9000",
		"S3_BUCKET="+minioBucket,
		"S3_ACCESS_KEY=minioadmin",
		"S3_SECRET_KEY=minioadmin",
		"S3_REGION=us-east-1",
		"S3_USE_SSL=false",
		"S3_FORCE_PATH_STYLE=true",
	)
	cmdStorage.Stdout = os.Stdout
	cmdStorage.Stderr = os.Stderr
	require.NoError(t, cmdStorage.Start())
	defer func() {
		cancelStorage()
		_ = cmdStorage.Wait()
	}()

	waitForHTTP(t, storageAddr, 10*time.Second)

	// 7. 构造 Connect-RPC 客户端并创建虚拟磁盘
	httpClient := &http.Client{Timeout: 10 * time.Second}
	diskClient := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr)))
	storageClient := bowlv1connect.NewStorageServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, storageAddr)))

	createDiskResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-read-overlay-disk",
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

	// 8. 挂载虚拟磁盘并创建文件
	clientID := "e2e-read-client"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskIDStr,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "read-overlay-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 9. 阶段一：写入多层重叠数据并在远端留下稀疏空洞
	// 写入 1: 0..64KB 写入 'A'
	size64K := 64 * 1024
	payloadA := bytes.Repeat([]byte("A"), size64K)
	_, err = storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payloadA,
	})
	require.NoError(t, err)

	// 写入 2: 16KB..32KB 覆写 'B' (密集覆写)
	size16K := 16 * 1024
	payloadB := bytes.Repeat([]byte("B"), size16K)
	_, err = storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    16 * 1024,
		Data:      payloadB,
	})
	require.NoError(t, err)

	// 写入 3: 1MB..1MB+64KB 写入 'C' (中间 64KB..1MB 形成稀疏空洞)
	offset1MB := uint64(1024 * 1024)
	payloadC := bytes.Repeat([]byte("C"), size64K)
	_, err = storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset1MB,
		Data:      payloadC,
	})
	require.NoError(t, err)

	// 10. 阶段二：热路径读取与稀疏空洞验证
	// 10.1 回读 0..32KB (前 16KB 为 'A'，后 16KB 为 'B')
	readHotResp, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(32 * 1024),
	})
	require.NoError(t, err)
	expectedHot := append(bytes.Repeat([]byte("A"), size16K), bytes.Repeat([]byte("B"), size16K)...)
	assert.Equal(t, expectedHot, readHotResp.GetData(), "热缓存图层覆写回读必须返回最新写入")

	// 10.2 回读稀疏空洞区间 64KB..128KB (必须全零)
	readHoleResp, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    uint64(size64K),
		Size:      uint32(size64K),
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, size64K), readHoleResp.GetData(), "稀疏空洞区间必须全为 0x00")

	// 11. 阶段三：等待后台 S3 上传并将切片 3 转换为冷数据
	// 触发 Fsync 等待 S3 持久化屏障达成（阻塞直至全部在途分块提交成功，无需额外 sleep）
	fsyncResp, err := storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)

	// 清除切片 3 的本地 Staging 文件
	slice3Path := filepath.Join(stagingDir, diskIDStr, "slice_3.bin")
	_ = os.Remove(slice3Path)

	// 12. 阶段四：冷路径回源读取 (从 S3 HTTP Range 局部拉取 1MB 处的 'C' 数据)
	readColdResp, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    offset1MB,
		Size:      uint32(size64K),
	})
	require.NoError(t, err)
	assert.Equal(t, payloadC, readColdResp.GetData(), "S3 HTTP Range 透明冷读回源数据必须完全一致")

	// 13. 释放句柄
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_ChunkDriver_S3AsyncPipeline_FlushFsyncDurabilityBarrier 验证 S3 后台 4MB 分块异步推流管道与 Flush/Fsync 持久化同步屏障，并立即从冷存储校验物理数据完整性
func TestE2E_ChunkDriver_S3AsyncPipeline_FlushFsyncDurabilityBarrier(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化数据库连接池
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 初始化 MinIO 客户端并确保 bucket 存在
	minioEndpoint := "127.0.0.1:9000"
	minioBucket := "bowl-data"
	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	require.NoError(t, err)

	bucketExists, err := minioClient.BucketExists(ctx, minioBucket)
	require.NoError(t, err)
	if !bucketExists {
		err = minioClient.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	// 3. 准备独立临时目录
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

	// 4. 静态编译真实服务二进制
	cmdDiskBuild := exec.Command("go", "build", "-o", diskBin, "./services/disk/cmd")
	cmdDiskBuild.Dir = repoRoot
	out, err := cmdDiskBuild.CombinedOutput()
	require.NoError(t, err, "编译 disk-service 失败: %s", string(out))

	cmdStorageBuild := exec.Command("go", "build", "-o", storageBin, "./services/storage/cmd")
	cmdStorageBuild.Dir = repoRoot
	out, err = cmdStorageBuild.CombinedOutput()
	require.NoError(t, err, "编译 storage-service 失败: %s", string(out))

	// 5. 启动 DiskService 进程
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

	// 6. 启动 StorageService 进程 (启用 STORAGE_DRIVER=chunk, S3=true)
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
		"S3_ENABLED=true",
		"S3_ENDPOINT=http://127.0.0.1:9000",
		"S3_BUCKET="+minioBucket,
		"S3_ACCESS_KEY=minioadmin",
		"S3_SECRET_KEY=minioadmin",
		"S3_REGION=us-east-1",
		"S3_USE_SSL=false",
		"S3_FORCE_PATH_STYLE=true",
	)
	cmdStorage.Stdout = os.Stdout
	cmdStorage.Stderr = os.Stderr
	require.NoError(t, cmdStorage.Start())
	defer func() {
		cancelStorage()
		_ = cmdStorage.Wait()
	}()

	waitForHTTP(t, storageAddr, 10*time.Second)

	// 7. 构造客户端并创建测试磁盘
	httpClient := &http.Client{Timeout: 10 * time.Second}
	diskClient := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr)))
	storageClient := bowlv1connect.NewStorageServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, storageAddr)))

	createDiskResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-durability-barrier-disk",
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

	// 8. 挂载虚拟磁盘并创建测试文件
	clientID := "e2e-durability-client"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskIDStr,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	createResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "barrier-persistence-test.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 9. 写入 10MB 数据 (跨越两个 4MB 物理分块边界: 4MB block 0, 4MB block 1, 2MB block 2)
	totalSize := 10 * 1024 * 1024
	blockSize4MB := 4 * 1024 * 1024
	payload10MB := make([]byte, totalSize)
	for i := range payload10MB {
		payload10MB[i] = byte((i*73 + 19) % 256)
	}

	startWrite := time.Now()
	writeResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload10MB,
	})
	elapsedWrite := time.Since(startWrite)
	require.NoError(t, err)
	assert.Equal(t, uint32(totalSize), writeResp.GetWritten())
	assert.Less(t, elapsedWrite, 50*time.Millisecond, "前台 10MB 写入 NVMe Staging 耗时应极短")

	// 10. 触发 Fsync 持久化同步屏障：服务端挂起直至在途 4MB 分块全部完成持久化
	startFsync := time.Now()
	fsyncResp, err := storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
	})
	elapsedFsync := time.Since(startFsync)
	require.NoError(t, err)
	require.NotNil(t, fsyncResp)
	t.Logf("Fsync 持久化屏障同步耗时: %v", elapsedFsync)

	// 11. 立即从冷存储 (MinIO S3) 直接校验 4MB 物理分块完整性 (无需 sleep 等待)
	slice1Key0 := fmt.Sprintf("chunks/%s/000/0/1_0", diskIDStr)
	slice1Key1 := fmt.Sprintf("chunks/%s/000/0/1_1", diskIDStr)
	slice1Key2 := fmt.Sprintf("chunks/%s/000/0/1_2", diskIDStr)

	// 校验分块 0 (4MB)
	stat0, err := minioClient.StatObject(ctx, minioBucket, slice1Key0, minio.StatObjectOptions{})
	require.NoError(t, err, "S3 物理分块 0 必须立即存在")
	assert.Equal(t, int64(blockSize4MB), stat0.Size)

	obj0, err := minioClient.GetObject(ctx, minioBucket, slice1Key0, minio.GetObjectOptions{})
	require.NoError(t, err)
	data0, err := io.ReadAll(obj0)
	require.NoError(t, err)
	_ = obj0.Close()
	assert.Equal(t, payload10MB[:blockSize4MB], data0, "S3 分块 0 数据载荷必须与原始数据一致")

	// 校验分块 1 (4MB)
	stat1, err := minioClient.StatObject(ctx, minioBucket, slice1Key1, minio.StatObjectOptions{})
	require.NoError(t, err, "S3 物理分块 1 必须立即存在")
	assert.Equal(t, int64(blockSize4MB), stat1.Size)

	obj1, err := minioClient.GetObject(ctx, minioBucket, slice1Key1, minio.GetObjectOptions{})
	require.NoError(t, err)
	data1, err := io.ReadAll(obj1)
	require.NoError(t, err)
	_ = obj1.Close()
	assert.Equal(t, payload10MB[blockSize4MB:2*blockSize4MB], data1, "S3 分块 1 数据载荷必须与原始数据一致")

	// 校验分块 2 (2MB)
	stat2, err := minioClient.StatObject(ctx, minioBucket, slice1Key2, minio.StatObjectOptions{})
	require.NoError(t, err, "S3 物理分块 2 必须立即存在")
	assert.Equal(t, int64(2*1024*1024), stat2.Size)

	obj2, err := minioClient.GetObject(ctx, minioBucket, slice1Key2, minio.GetObjectOptions{})
	require.NoError(t, err)
	data2, err := io.ReadAll(obj2)
	require.NoError(t, err)
	_ = obj2.Close()
	assert.Equal(t, payload10MB[2*blockSize4MB:], data2, "S3 分块 2 数据载荷必须与原始数据一致")

	// 12. 删除本地 NVMe Staging 缓存切片，模拟本地缓存完全丢失
	stagingSlicePath := filepath.Join(stagingDir, diskIDStr, "slice_1.bin")
	require.NoError(t, os.Remove(stagingSlicePath))

	// 13. Connect-RPC 客户端调用 Read，按 2MB 分块验证透明从冷存储 S3 HTTP Range 回源读取数据完整性
	var readBackAll []byte
	chunkReadSize := uint32(2 * 1024 * 1024)
	for off := uint64(0); off < uint64(totalSize); off += uint64(chunkReadSize) {
		sz := chunkReadSize
		if off+uint64(sz) > uint64(totalSize) {
			sz = uint32(uint64(totalSize) - off)
		}
		readResp, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    off,
			Size:      sz,
		})
		require.NoError(t, err)
		readBackAll = append(readBackAll, readResp.GetData()...)
	}
	assert.Equal(t, payload10MB, readBackAll, "冷存储回源读取的数据必须与原始写入完全一致")

	// 14. 释放句柄
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_ChunkDriver_Truncate_Fallocate_Lseek 验证文件截断缩短与扩容空洞、打洞及 Lseek 跨切片探测的真实服务完整闭环
func TestE2E_ChunkDriver_Truncate_Fallocate_Lseek(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化数据库连接池
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 准备临时工作目录
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

	// 3. 编译服务二进制
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

	// 5. 启动 StorageService 进程 (启用 STORAGE_DRIVER=chunk)
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

	// 6. 构造 DiskClient 与 StorageClient
	httpClient := &http.Client{Timeout: 10 * time.Second}
	diskConnectClient := connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr))
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	storageTransport := connecthttp.NewTransport(httpClient, storageAddr)
	storageConnectClient := connect.NewClient(storageTransport)
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	createDiskResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-trunc-falloc-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskIDStr := createDiskResp.GetDisk().GetDiskId()
	parsedDiskUUID, err := uuid.Parse(diskIDStr)
	require.NoError(t, err)

	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE;`, schemaName))
	}()

	// 7. 挂载虚拟磁盘
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskIDStr,
		ClientId: "client-e2e-trunc-falloc",
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 8. 创建测试文件
	createResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "semantics_e2e.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 9. 写入 8192 字节基准数据
	payload8K := make([]byte, 8192)
	for i := range payload8K {
		payload8K[i] = byte((i * 7) % 256)
	}
	_, err = storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload8K,
	})
	require.NoError(t, err)

	// 10. Truncate 扩容语义验证：将文件扩容至 1MB (1048576 字节)
	target1MB := uint64(1024 * 1024)
	setAttrExtend, err := storageClient.SetAttr(ctx, &bowlv1.SetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
		Valid:     uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_SIZE),
		Size:      target1MB,
	})
	require.NoError(t, err)
	assert.Equal(t, target1MB, setAttrExtend.GetInode().GetSize())

	// 校验基础写入数据完好
	readOrig, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      8192,
	})
	require.NoError(t, err)
	assert.Equal(t, payload8K, readOrig.GetData())

	// 校验扩容形成的稀疏空洞读取为全零 (0x00)
	readZero, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    8192,
		Size:      8192,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 8192), readZero.GetData())

	// 校验读取超出 1MB 返回 EOF
	readEOF, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    target1MB,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.True(t, readEOF.GetEof())
	assert.Empty(t, readEOF.GetData())

	// 11. Fallocate 打洞语义验证：在区间 [2048, 4096) 打洞清零 (长度 2048)
	fallocPunch, err := storageClient.Fallocate(ctx, &bowlv1.FallocateRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Mode:      uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE),
		Offset:    2048,
		Length:    2048,
	})
	require.NoError(t, err)
	require.NotNil(t, fallocPunch)

	// 校验打洞后文件大小不变
	attrAfterPunch, err := storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
	})
	require.NoError(t, err)
	assert.Equal(t, target1MB, attrAfterPunch.GetInode().GetSize())

	// 校验打洞前区间 [0, 2048) 保持不变
	readPart1, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, payload8K[:2048], readPart1.GetData())

	// 校验打洞区间 [2048, 4096) 读出全零 (0x00)
	readPunchedHole, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Size:      2048,
	})
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 2048), readPunchedHole.GetData())

	// 校验打洞后区间 [4096, 8192) 保持不变
	readPart2, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    4096,
		Size:      4096,
	})
	require.NoError(t, err)
	assert.Equal(t, payload8K[4096:], readPart2.GetData())

	// 12. Lseek 跨切片与稀疏空洞探测语义验证
	// 探测有效数据：偏移量 0 处为有效数据，返回 0
	lseekData0, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), lseekData0.GetOffset())

	// 探测有效数据：偏移量 2048 处于打洞空洞内，跳过空洞返回下一个有效数据偏移量 4096
	lseekDataHole, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), lseekDataHole.GetOffset())

	// 探测稀疏空洞：偏移量 0 处扫描，首个空洞起点位于 2048
	lseekHole0, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2048), lseekHole0.GetOffset())

	// 探测稀疏空洞：偏移量 2048 处已处于空洞内，直接返回当前偏移量 2048
	lseekHoleInside, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    2048,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2048), lseekHoleInside.GetOffset())

	// 探测稀疏空洞：偏移量 4096 处于数据区间 [4096, 8192) 中，返回下一个稀疏空洞起点 8192
	lseekHoleNext, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    4096,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), lseekHoleNext.GetOffset())

	// 探测有效数据：偏移量 8192 处于尾部稀疏空洞且后续无数据，期望返回 OutOfRange 错误
	_, err = storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    8192,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_DATA,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeOutOfRange, connect.CodeOf(err))

	// 探测稀疏空洞：偏移量 1048576 位于文件末尾 EOF 处，返回文件总长度作为隐式空洞
	lseekHoleEOF, err := storageClient.Lseek(ctx, &bowlv1.LseekRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    target1MB,
		Whence:    bowlv1.LseekWhence_LSEEK_WHENCE_HOLE,
	})
	require.NoError(t, err)
	assert.Equal(t, target1MB, lseekHoleEOF.GetOffset())

	// 13. Truncate 缩短截断语义验证：将文件缩短至 1024 字节
	setAttrShrink, err := storageClient.SetAttr(ctx, &bowlv1.SetAttrRequest{
		SessionId: sessionID,
		Ino:       ino,
		Valid:     uint32(bowlv1.SetAttrMask_SET_ATTR_MASK_SIZE),
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), setAttrShrink.GetInode().GetSize())

	// 读取前 1024 字节正常
	readShrunk, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.Equal(t, payload8K[:1024], readShrunk.GetData())

	// 读取 1024 处返回 EOF
	readShrunkEOF, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024,
		Size:      1024,
	})
	require.NoError(t, err)
	assert.True(t, readShrunkEOF.GetEof())
	assert.Empty(t, readShrunkEOF.GetData())

	// 14. 释放句柄
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_ChunkDriver_CopyFileRange_ZeroCopyAndCOWIsolation 验证基于真实二进制服务的秒级写时复制克隆语义 (CopyFileRange) 与 COW 严格数据隔离性
func TestE2E_ChunkDriver_CopyFileRange_ZeroCopyAndCOWIsolation(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化数据库连接池
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 准备运行目录
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

	// 5. 启动 StorageService 进程 (启用 STORAGE_DRIVER=chunk)
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
	diskClient := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr)))
	storageClient := bowlv1connect.NewStorageServiceClient(connect.NewClient(connecthttp.NewTransport(httpClient, storageAddr)))

	createDiskResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-cow-disk",
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

	// 7. 挂载虚拟磁盘
	clientID := "e2e-cow-client"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskIDStr,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 8. 创建源文件 A 并写入 64KB 数据
	createSrcResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "source_file.dat",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	srcIno := createSrcResp.GetInode().GetIno()
	srcFH := createSrcResp.GetFileHandle().GetFh()

	const dataSize = 64 * 1024
	payloadA := bytes.Repeat([]byte{0x5A}, dataSize)
	writeResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Data:      payloadA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(dataSize), writeResp.GetWritten())

	// 9. 创建目标文件 B
	createDstResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "cloned_file.dat",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	dstIno := createDstResp.GetInode().GetIno()
	dstFH := createDstResp.GetFileHandle().GetFh()

	// 10. 执行 CopyFileRange：秒级零拷贝克隆
	startCopy := time.Now()
	copyResp, err := storageClient.CopyFileRange(ctx, &bowlv1.CopyFileRangeRequest{
		SessionId: sessionID,
		SrcIno:    srcIno,
		SrcFh:     srcFH,
		SrcOffset: 0,
		DstIno:    dstIno,
		DstFh:     dstFH,
		DstOffset: 0,
		Length:    dataSize,
	})
	elapsedCopy := time.Since(startCopy)
	require.NoError(t, err)
	assert.Equal(t, uint64(dataSize), copyResp.GetCopied())
	assert.Less(t, elapsedCopy, 50*time.Millisecond, "端到端 CopyFileRange 元数据克隆耗时必须低于 50ms")

	// 11. 校验 PostgreSQL 元数据层：slice_refs 引用计数原子递增为 2
	var sliceRefCount int32
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&sliceRefCount)
	require.NoError(t, err)
	assert.Equal(t, int32(2), sliceRefCount, "克隆后切片 1 引用计数必须原子递增为 2")

	// 校验目标 Inode 大小已自动更新为 64KB
	dstAttr, err := storageClient.GetAttr(ctx, &bowlv1.GetAttrRequest{SessionId: sessionID, Ino: dstIno})
	require.NoError(t, err)
	assert.Equal(t, uint64(dataSize), dstAttr.GetInode().GetSize())

	// 12. 回读目标文件 B，验证初始克隆内容完全一致
	readDstInitial, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, payloadA, readDstInitial.GetData())

	// 13. 写时复制 (COW) 隔离性验证：覆写目标文件 B 内部中间区间 [16KB, 32KB)
	const overwriteOffset = 16 * 1024
	const overwriteSize = 16 * 1024
	payloadB := bytes.Repeat([]byte{0x7B}, overwriteSize)
	writeOverwriteResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    overwriteOffset,
		Data:      payloadB,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(overwriteSize), writeOverwriteResp.GetWritten())

	// 14. 核心 COW 数据隔离断言：
	// 14.1 源文件 A 必须保持 100% 原始数据，完全不受目标文件覆写影响！
	readSrcAfter, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        srcFH,
		Ino:       srcIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, payloadA, readSrcAfter.GetData(), "源文件数据必须绝对保持原始状态不受波及")

	// 14.2 目标文件 B 读取应返回合成后的图层数据
	expectedDstData := make([]byte, dataSize)
	copy(expectedDstData, payloadA)
	copy(expectedDstData[overwriteOffset:], payloadB)

	readDstAfter, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedDstData, readDstAfter.GetData(), "目标文件数据必须包含覆写后的最新图层内容")

	// 15. 生命周期与孤儿切片安全保护验证：
	// 释放句柄、Unlink 并 Forget 源文件 A
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{SessionId: sessionID, Fh: srcFH})
	require.NoError(t, err)
	_, err = storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{SessionId: sessionID, ParentIno: 1, Name: "source_file.dat"})
	require.NoError(t, err)
	_, err = storageClient.Forget(ctx, &bowlv1.ForgetRequest{SessionId: sessionID, Ino: srcIno, Nlookup: 1})
	require.NoError(t, err)

	// 切片 1 引用计数递减为 1，不属于孤儿切片
	var sliceRefAfterA int32
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&sliceRefAfterA)
	require.NoError(t, err)
	assert.Equal(t, int32(1), sliceRefAfterA)

	// 目标文件 B 数据依然可正常完整读取
	readDstFinal, err := storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        dstFH,
		Ino:       dstIno,
		Offset:    0,
		Size:      dataSize,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedDstData, readDstFinal.GetData(), "源文件物理清理后克隆文件仍可正常完整读取")

	// 释放目标文件 B
	_, err = storageClient.Release(ctx, &bowlv1.ReleaseRequest{SessionId: sessionID, Fh: dstFH})
	require.NoError(t, err)
	_, err = storageClient.Unlink(ctx, &bowlv1.UnlinkRequest{SessionId: sessionID, ParentIno: 1, Name: "cloned_file.dat"})
	require.NoError(t, err)
	_, err = storageClient.Forget(ctx, &bowlv1.ForgetRequest{SessionId: sessionID, Ino: dstIno, Nlookup: 1})
	require.NoError(t, err)

	// 最终切片 1 引用计数归零
	var sliceRefZero int32
	err = pool.QueryRow(ctx, fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = 1`, schemaName)).Scan(&sliceRefZero)
	require.NoError(t, err)
	assert.Equal(t, int32(0), sliceRefZero)
}


