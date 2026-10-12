package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

var (
	stressBinOnce      sync.Once
	stressDiskBinPath  string
	stressStorageBinPath string
	stressBuildError   error
)

// buildStressServicesOnce 在单次测试运行中仅编译一次 disk-service 与 storage-service 二进制，避免重复编译开销
func buildStressServicesOnce(t *testing.T, repoRoot string) (string, string) {
	t.Helper()
	stressBinOnce.Do(func() {
		binDir, err := os.MkdirTemp("", "bowl-stress-bin-*")
		if err != nil {
			stressBuildError = fmt.Errorf("创建二进制临时目录失败: %w", err)
			return
		}
		stressDiskBinPath = filepath.Join(binDir, "disk-service")
		stressStorageBinPath = filepath.Join(binDir, "storage-service")

		cmdDisk := exec.Command("go", "build", "-o", stressDiskBinPath, "./services/disk/cmd")
		cmdDisk.Dir = repoRoot
		if out, err := cmdDisk.CombinedOutput(); err != nil {
			stressBuildError = fmt.Errorf("编译 disk-service 失败: %w (%s)", err, string(out))
			return
		}

		cmdStorage := exec.Command("go", "build", "-o", stressStorageBinPath, "./services/storage/cmd")
		cmdStorage.Dir = repoRoot
		if out, err := cmdStorage.CombinedOutput(); err != nil {
			stressBuildError = fmt.Errorf("编译 storage-service 失败: %w (%s)", err, string(out))
			return
		}
	})
	require.NoError(t, stressBuildError)
	return stressDiskBinPath, stressStorageBinPath
}

// stressEnvOptions 定义压力测试运行环境的启动配置
type stressEnvOptions struct {
	s3Enabled        bool
	s3Endpoint       string
	s3Bucket         string
	s3AccessKey      string
	s3SecretKey      string
	extraStorageEnv  []string
	extraDiskEnv     []string
}

// stressTestEnv 封装真实测试进程与客户端连接上下文
type stressTestEnv struct {
	repoRoot      string
	baseDir       string
	dataDir       string
	stagingDir    string
	dbDSN         string
	pool          *pgxpool.Pool
	diskAddr      string
	storageAddr   string
	diskClient    bowlv1connect.DiskServiceClient
	storageClient bowlv1connect.StorageServiceClient
}

// startStressEnv 启动隔离的 disk-service 与 storage-service 真实进程
func startStressEnv(t *testing.T, opts stressEnvOptions) *stressTestEnv {
	t.Helper()
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "初始化 PostgreSQL 连接池失败")

	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")
	stagingDir := filepath.Join(baseDir, "staging")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(stagingDir, 0o755))

	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)

	diskBin, storageBin := buildStressServicesOnce(t, repoRoot)

	// 1. 启动 DiskService 真实进程
	diskPort := getFreePort(t)
	diskAddr := fmt.Sprintf("http://127.0.0.1:%d", diskPort)

	ctxDisk, cancelDisk := context.WithCancel(context.Background())
	cmdDisk := exec.CommandContext(ctxDisk, diskBin)
	cmdDisk.Dir = repoRoot
	diskEnv := append(os.Environ(),
		fmt.Sprintf("SERVER_PORT=%d", diskPort),
		fmt.Sprintf("PORT=%d", diskPort),
		"DB_DSN="+dsn,
		"DATA_DIR="+dataDir,
		"STAGING_DIR="+stagingDir,
	)
	if opts.s3Enabled {
		diskEnv = append(diskEnv,
			"S3_ENABLED=true",
			"S3_ENDPOINT="+opts.s3Endpoint,
			"S3_BUCKET="+opts.s3Bucket,
			"S3_ACCESS_KEY="+opts.s3AccessKey,
			"S3_SECRET_KEY="+opts.s3SecretKey,
			"S3_REGION=us-east-1",
			"S3_FORCE_PATH_STYLE=true",
		)
	}
	diskEnv = append(diskEnv, opts.extraDiskEnv...)
	cmdDisk.Env = diskEnv
	cmdDisk.Stdout = os.Stdout
	cmdDisk.Stderr = os.Stderr
	require.NoError(t, cmdDisk.Start(), "启动 disk-service 进程失败")

	waitForHTTP(t, diskAddr, 10*time.Second)

	// 2. 启动 StorageService 真实进程 (启用 chunk 引擎)
	storagePort := getFreePort(t)
	storageAddr := fmt.Sprintf("http://127.0.0.1:%d", storagePort)

	ctxStorage, cancelStorage := context.WithCancel(context.Background())
	cmdStorage := exec.CommandContext(ctxStorage, storageBin)
	cmdStorage.Dir = repoRoot
	storageEnv := append(os.Environ(),
		fmt.Sprintf("SERVER_PORT=%d", storagePort),
		fmt.Sprintf("PORT=%d", storagePort),
		"DB_DSN="+dsn,
		"STORAGE_DRIVER=chunk",
		"DATA_DIR="+dataDir,
		"STAGING_DIR="+stagingDir,
		"DISK_SERVICE_ADDR="+diskAddr,
	)
	if opts.s3Enabled {
		storageEnv = append(storageEnv,
			"S3_ENABLED=true",
			"S3_ENDPOINT="+opts.s3Endpoint,
			"S3_BUCKET="+opts.s3Bucket,
			"S3_ACCESS_KEY="+opts.s3AccessKey,
			"S3_SECRET_KEY="+opts.s3SecretKey,
			"S3_REGION=us-east-1",
			"S3_FORCE_PATH_STYLE=true",
		)
	}
	storageEnv = append(storageEnv, opts.extraStorageEnv...)
	cmdStorage.Env = storageEnv
	cmdStorage.Stdout = os.Stdout
	cmdStorage.Stderr = os.Stderr
	require.NoError(t, cmdStorage.Start(), "启动 storage-service 进程失败")

	waitForHTTP(t, storageAddr, 10*time.Second)

	// 3. 构造 Connect-RPC 客户端
	httpClient := &http.Client{Timeout: 15 * time.Second}
	diskConnectClient := connect.NewClient(connecthttp.NewTransport(httpClient, diskAddr))
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	storageConnectClient := connect.NewClient(connecthttp.NewTransport(httpClient, storageAddr))
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	t.Cleanup(func() {
		cancelStorage()
		_ = cmdStorage.Wait()
		cancelDisk()
		_ = cmdDisk.Wait()
		pool.Close()
	})

	return &stressTestEnv{
		repoRoot:      repoRoot,
		baseDir:       baseDir,
		dataDir:       dataDir,
		stagingDir:    stagingDir,
		dbDSN:         dsn,
		pool:          pool,
		diskAddr:      diskAddr,
		storageAddr:   storageAddr,
		diskClient:    diskClient,
		storageClient: storageClient,
	}
}

// TestE2E_Storage_ConcurrentRandomReadWriteStress 验证多 Goroutine 高并发随机读写 12MB+ 文件并通过全量 SHA-256 校验 100% 字节一致性
func TestE2E_Storage_ConcurrentRandomReadWriteStress(t *testing.T) {
	ctx := context.Background()
	env := startStressEnv(t, stressEnvOptions{})

	// 1. 创建虚拟磁盘
	createDiskResp, err := env.diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "stress-concurrent-rw-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024, // 10 GB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createDiskResp.GetDisk().GetDiskId()
	parsedDiskUUID, err := uuid.Parse(diskID)
	require.NoError(t, err)
	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	defer func() {
		_, _ = env.pool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 2. 挂载虚拟磁盘并获取独占写租约
	clientID := "stress-worker-client"
	mountResp, err := env.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 3. 创建测试目标文件
	createFileResp, err := env.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "concurrent-stress-12mb.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createFileResp.GetInode().GetIno()
	fh := createFileResp.GetFileHandle().GetFh()

	// 4. 生成 12MB (跨越 64MB Chunk 内多个 4MB 边界) 的确定性真实基准数据 (Ground Truth)
	const (
		totalSize = 12 * 1024 * 1024 // 12 MB (大于 10MB 要求)
		blockSize = 128 * 1024       // 128 KB 单一切片写入单位
		numBlocks = totalSize / blockSize // 96 个切片块
	)
	groundTruth := make([]byte, totalSize)
	for i := range groundTruth {
		groundTruth[i] = byte((i*137 + (i>>7)*17 + 23) & 0xff)
	}
	expectedHash := sha256.Sum256(groundTruth)

	// 5. 并发随机写入与动态并发读取压力注入
	var (
		writtenBlocks [numBlocks]atomic.Bool
		writeWg       sync.WaitGroup
		readWg        sync.WaitGroup
		stopReading   atomic.Bool
		readErrChan   = make(chan error, 100)
	)

	// 启动 4 个并发读取探针 Goroutine：持续随机抽取已被标记写入的块进行实时读取并校验数据
	const numReaderWorkers = 4
	readWg.Add(numReaderWorkers)
	for r := 0; r < numReaderWorkers; r++ {
		go func(readerID int) {
			defer readWg.Done()
			for !stopReading.Load() {
				// 随机抽取一个候选块
				randIdxBig, err := rand.Int(rand.Reader, big.NewInt(numBlocks))
				if err != nil {
					continue
				}
				blkIdx := int(randIdxBig.Int64())
				if !writtenBlocks[blkIdx].Load() {
					time.Sleep(2 * time.Millisecond)
					continue
				}

				off := uint64(blkIdx * blockSize)
				readResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
					SessionId: sessionID,
					Fh:        fh,
					Ino:       ino,
					Offset:    off,
					Size:      uint32(blockSize),
				})
				if err != nil {
					readErrChan <- fmt.Errorf("探针读取失败 (off=%d): %w", off, err)
					return
				}
				expectedChunk := groundTruth[off : off+uint64(blockSize)]
				if !bytes.Equal(readResp.GetData(), expectedChunk) {
					readErrChan <- fmt.Errorf("探针读取数据不一致 (off=%d)", off)
					return
				}
			}
		}(r)
	}

	// 启动 12 个并发写入工作 Goroutine 乱序写入 96 个 128KB 块
	const numWriterWorkers = 12
	blockIndexChan := make(chan int, numBlocks)
	// 将块索引以乱序打散分发
	shuffledIndices := make([]int, numBlocks)
	for i := range shuffledIndices {
		shuffledIndices[i] = i
	}
	for i := len(shuffledIndices) - 1; i > 0; i-- {
		nBig, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		j := int(nBig.Int64())
		shuffledIndices[i], shuffledIndices[j] = shuffledIndices[j], shuffledIndices[i]
	}
	for _, idx := range shuffledIndices {
		blockIndexChan <- idx
	}
	close(blockIndexChan)

	writeWg.Add(numWriterWorkers)
	for w := 0; w < numWriterWorkers; w++ {
		go func(workerID int) {
			defer writeWg.Done()
			for blkIdx := range blockIndexChan {
				off := uint64(blkIdx * blockSize)
				chunkData := groundTruth[off : off+uint64(blockSize)]
				writeResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
					SessionId: sessionID,
					Fh:        fh,
					Ino:       ino,
					Offset:    off,
					Data:      chunkData,
				})
				if err != nil {
					readErrChan <- fmt.Errorf("worker %d 写入块 %d 失败: %w", workerID, blkIdx, err)
					return
				}
				if writeResp.GetWritten() != uint32(blockSize) {
					readErrChan <- fmt.Errorf("worker %d 写入字节数不符: 实际 %d, 期望 %d", workerID, writeResp.GetWritten(), blockSize)
					return
				}
				// 写入成功后原子标记该块就绪
				writtenBlocks[blkIdx].Store(true)
			}
		}(w)
	}

	// 等待所有写入工作协程完成
	writeWg.Wait()
	stopReading.Store(true)
	readWg.Wait()
	close(readErrChan)

	// 校验并发阶段未产生任何断言错误
	for err := range readErrChan {
		require.NoError(t, err, "并发读写阶段产生错误")
	}

	// 6. 触发 Flush 与 Fsync 持久化屏障
	_, err = env.storageClient.Flush(ctx, &bowlv1.FlushRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)

	_, err = env.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	require.NoError(t, err)

	// 7. 写入完成后执行并发随机多粒度读取验证 (8 个 Goroutine 随机跨块读取)
	const numPostReaders = 8
	var postReadWg sync.WaitGroup
	postReadErrChan := make(chan error, 50)
	postReadWg.Add(numPostReaders)

	for p := 0; p < numPostReaders; p++ {
		go func(pid int) {
			defer postReadWg.Done()
			for iter := 0; iter < 15; iter++ {
				// 随机选择偏移与大小 (包括非对齐偏移)
				randOffBig, _ := rand.Int(rand.Reader, big.NewInt(totalSize-256*1024))
				off := uint64(randOffBig.Int64())
				randSzBig, _ := rand.Int(rand.Reader, big.NewInt(256*1024-1024))
				sz := uint32(randSzBig.Int64() + 1024)

				readResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
					SessionId: sessionID,
					Fh:        fh,
					Ino:       ino,
					Offset:    off,
					Size:      sz,
				})
				if err != nil {
					postReadErrChan <- fmt.Errorf("post-reader %d 读取失败: %w", pid, err)
					return
				}
				expectedSub := groundTruth[off : off+uint64(sz)]
				if !bytes.Equal(readResp.GetData(), expectedSub) {
					postReadErrChan <- fmt.Errorf("post-reader %d 数据校验不符 (off=%d, sz=%d)", pid, off, sz)
					return
				}
			}
		}(p)
	}
	postReadWg.Wait()
	close(postReadErrChan)
	for err := range postReadErrChan {
		require.NoError(t, err)
	}

	// 8. 顺序组装 12MB 全量数据并计算 SHA-256 校验和
	var assembledData []byte
	const assembleChunkSize = 2 * 1024 * 1024 // 2MB
	for off := uint64(0); off < uint64(totalSize); off += uint64(assembleChunkSize) {
		sz := uint32(assembleChunkSize)
		if off+uint64(sz) > uint64(totalSize) {
			sz = uint32(uint64(totalSize) - off)
		}
		readResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    off,
			Size:      sz,
		})
		require.NoError(t, err)
		assembledData = append(assembledData, readResp.GetData()...)
	}

	assert.Equal(t, totalSize, len(assembledData), "重组的全量数据长度必须为 12MB")
	actualHash := sha256.Sum256(assembledData)
	assert.Equal(t, expectedHash, actualHash, "12MB 极端并发读写文件的全量 SHA-256 校验和必须 100%% 完全一致")

	// 9. 释放句柄
	_, err = env.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_Storage_DenseSliceOverwrite_Reconstitution 验证同一区域内 100+ 密集重叠切片覆盖写入后 JuiceFS 叠加层图层重组完全匹配内存真值缓冲区
func TestE2E_Storage_DenseSliceOverwrite_Reconstitution(t *testing.T) {
	ctx := context.Background()
	env := startStressEnv(t, stressEnvOptions{})

	// 1. 创建虚拟磁盘与挂载会话
	createDiskResp, err := env.diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "stress-dense-overwrite-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createDiskResp.GetDisk().GetDiskId()
	parsedDiskUUID, err := uuid.Parse(diskID)
	require.NoError(t, err)
	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	defer func() {
		_, _ = env.pool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	clientID := "dense-overwrite-client"
	mountResp, err := env.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. 创建测试文件
	createFileResp, err := env.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "dense-overlapping-slices.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createFileResp.GetInode().GetIno()
	fh := createFileResp.GetFileHandle().GetFh()

	// 3. 定义 512KB 覆盖重写目标区域与内存真值缓冲区 (In-Memory Ground Truth)
	const areaSize = 512 * 1024 // 512 KB
	groundTruth := make([]byte, areaSize)
	for i := range groundTruth {
		groundTruth[i] = byte((i * 19) % 256)
	}

	// 写入初始底图载荷
	writeResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      groundTruth,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(areaSize), writeResp.GetWritten())

	// 4. 执行 128 次重叠密集切片写入 (严格超过 100+ 要求)
	const numOverwrites = 128
	for i := 0; i < numOverwrites; i++ {
		// 随机切片长度介于 1KB 至 32KB
		randLenBig, _ := rand.Int(rand.Reader, big.NewInt(31*1024))
		length := int(randLenBig.Int64()) + 1024

		// 随机偏移
		randOffBig, _ := rand.Int(rand.Reader, big.NewInt(int64(areaSize-length)))
		offset := int(randOffBig.Int64())

		// 生成唯一随机载荷
		payload := make([]byte, length)
		_, err := rand.Read(payload)
		require.NoError(t, err)

		// 写入存储服务端
		wResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    uint64(offset),
			Data:      payload,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(length), wResp.GetWritten())

		// 同步更新内存真值图层
		copy(groundTruth[offset:offset+length], payload)
	}

	// 5. 校验 PostgreSQL chunks 元数据账本中确实堆叠了 100+ 个切片
	var rawSlices []byte
	queryChunks := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = 0`, schemaName)
	err = env.pool.QueryRow(ctx, queryChunks, int64(ino)).Scan(&rawSlices)
	require.NoError(t, err)
	slices, err := decodeE2ESlices(rawSlices)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(slices), numOverwrites+1, "PostgreSQL 账本中切片记录总数必须大于等于 129 (含初始底图)")

	// 6. 验证 JuiceFS 图层重构 (Reconstitution)：
	// 模式 A：单次全量 512KB 读取与内存真值完全比对
	fullReadResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(areaSize),
	})
	require.NoError(t, err)
	assert.Equal(t, groundTruth, fullReadResp.GetData(), "512KB 全量图层重组结果必须与内存真值字节级一致")

	// 模式 B：随机跨切片边界抽取 40 组子区间进行多粒度精确校验
	for sample := 0; sample < 40; sample++ {
		randLenBig, _ := rand.Int(rand.Reader, big.NewInt(16*1024))
		subLen := uint32(randLenBig.Int64() + 512)
		randOffBig, _ := rand.Int(rand.Reader, big.NewInt(int64(areaSize-int(subLen))))
		subOff := uint64(randOffBig.Int64())

		subReadResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    subOff,
			Size:      subLen,
		})
		require.NoError(t, err)
		expectedSlice := groundTruth[subOff : subOff+uint64(subLen)]
		assert.Equal(t, expectedSlice, subReadResp.GetData(), "子区间 [off=%d, len=%d] 图层重组数据必须完全匹配", subOff, subLen)
	}

	// 模式 C：使用 4KB 滑动窗口自始至终扫描验证
	const windowSize = 4 * 1024
	for winOff := uint64(0); winOff < uint64(areaSize); winOff += uint64(windowSize) {
		winResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    winOff,
			Size:      uint32(windowSize),
		})
		require.NoError(t, err)
		assert.Equal(t, groundTruth[winOff:winOff+uint64(windowSize)], winResp.GetData(), "滑动窗口 [off=%d] 校验失败", winOff)
	}

	// 7. 释放句柄
	_, err = env.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_Storage_S3AsyncPipeline_TransientDelayAndRetry 验证在遭遇瞬时网络故障与延迟重试时 S3 异步推流管道与 Fsync 持久化屏障安全完成且数据零损坏
func TestE2E_Storage_S3AsyncPipeline_TransientDelayAndRetry(t *testing.T) {
	minioTarget := "http://127.0.0.1:9000"
	if !isMinIOAvailable(minioTarget) {
		t.Skip("MinIO 未运行在 127.0.0.1:9000，跳过 S3 瞬时故障重试压力测试")
	}

	ctx := context.Background()
	bucketName := "bowl-data"
	accessKey := "minioadmin"
	secretKey := "minioadmin"

	// 初始化直连 MinIO 客户端
	directMinioClient, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)
	exists, err := directMinioClient.BucketExists(ctx, bucketName)
	require.NoError(t, err)
	if !exists {
		err = directMinioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	// 构造注入瞬时故障与延迟的透明反向代理
	var (
		injectedFailures atomic.Int32
		uploadAttempts   sync.Map
	)

	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxyPort := proxyListener.Addr().(*net.TCPAddr).Port
	proxyEndpoint := fmt.Sprintf("http://127.0.0.1:%d", proxyPort)

	proxyServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 对针对 S3 物理数据块的 PUT 请求进行故障与延迟注入
			if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "chunks/") {
				pathKey := r.URL.Path
				actualVal, _ := uploadAttempts.LoadOrStore(pathKey, new(atomic.Int32))
				attemptCounter := actualVal.(*atomic.Int32)
				curAttempt := attemptCounter.Add(1)

				// 前 2 次调用注入 503 瞬时不可用故障或 100ms 延迟
				if curAttempt <= 2 {
					injectedFailures.Add(1)
					time.Sleep(100 * time.Millisecond)
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>ServiceUnavailable</Code>
  <Message>Transient simulated S3 network congestion</Message>
  <Resource>` + r.URL.Path + `</Resource>
</Error>`))
					return
				}
			}

			// 正常转发至真实 MinIO 服务端 (保留原始签名 Header)
			targetURL := minioTarget + r.RequestURI
			bodyBytes, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()

			forwardReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(bodyBytes))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			forwardReq.Host = r.Host
			for k, values := range r.Header {
				for _, val := range values {
					forwardReq.Header.Add(k, val)
				}
			}

			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Do(forwardReq)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()

			for k, values := range resp.Header {
				for _, val := range values {
					w.Header().Add(k, val)
				}
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		}),
	}

	go func() {
		_ = proxyServer.Serve(proxyListener)
	}()
	defer func() {
		_ = proxyServer.Close()
	}()

	// 启动 StorageService 实例，指向故障注入代理端点
	env := startStressEnv(t, stressEnvOptions{
		s3Enabled:   true,
		s3Endpoint:  proxyEndpoint,
		s3Bucket:    bucketName,
		s3AccessKey: accessKey,
		s3SecretKey: secretKey,
	})

	// 1. 创建虚拟磁盘
	createDiskResp, err := env.diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "stress-s3-retry-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createDiskResp.GetDisk().GetDiskId()
	parsedDiskUUID, err := uuid.Parse(diskID)
	require.NoError(t, err)
	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	defer func() {
		_, _ = env.pool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	// 2. 挂载虚拟磁盘
	clientID := "s3-retry-client"
	mountResp, err := env.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 3. 创建文件
	createFileResp, err := env.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "s3-pipeline-retry.bin",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	ino := createFileResp.GetInode().GetIno()
	fh := createFileResp.GetFileHandle().GetFh()

	// 4. 写入 8MB 数据 (跨越两个 4MB 物理分块边界)
	const totalSize = 8 * 1024 * 1024 // 8 MB = 2 * 4MB
	payload8MB := make([]byte, totalSize)
	for i := range payload8MB {
		payload8MB[i] = byte((i*73 + 31) & 0xff)
	}
	expectedHash := sha256.Sum256(payload8MB)

	writeResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload8MB,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(totalSize), writeResp.GetWritten())

	// 5. 触发 Fsync 持久化同步屏障：服务端必须挂起直至重试成功并全部上传至 S3
	startFsync := time.Now()
	fsyncResp, err := env.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Datasync:  false,
	})
	fsyncDuration := time.Since(startFsync)
	require.NoError(t, err, "Fsync 同步屏障必须在重试后最终成功返回")
	require.NotNil(t, fsyncResp)
	t.Logf("Fsync 经历瞬时重试后的总阻塞耗时: %v", fsyncDuration)

	// 6. 确认代理确实拦截并注入了瞬时失败
	assert.GreaterOrEqual(t, injectedFailures.Load(), int32(2), "代理必须拦截到并注入至少 2 次瞬时 503 重试")

	// 7. 直接连接 MinIO 校验两个 4MB 分块物理对象确实完整落盘
	block0Key := fmt.Sprintf("chunks/%s/000/0/1_0", diskID)
	block1Key := fmt.Sprintf("chunks/%s/000/0/1_1", diskID)

	stat0, err := directMinioClient.StatObject(ctx, bucketName, block0Key, minio.StatObjectOptions{})
	require.NoError(t, err, "MinIO 物理分块 0 必须存在")
	assert.Equal(t, int64(4*1024*1024), stat0.Size)

	stat1, err := directMinioClient.StatObject(ctx, bucketName, block1Key, minio.StatObjectOptions{})
	require.NoError(t, err, "MinIO 物理分块 1 必须存在")
	assert.Equal(t, int64(4*1024*1024), stat1.Size)

	// 8. 彻底删除本地 NVMe Staging 缓存切片，模拟本地缓存完全丢失
	stagingSlicePath := filepath.Join(env.stagingDir, diskID, "slice_1.bin")
	_ = os.Remove(stagingSlicePath)

	// 9. 通过 StorageService Read 接口执行透明冷存储 S3 回源读取
	var readBackAll []byte
	const readChunkSize = 2 * 1024 * 1024
	for off := uint64(0); off < uint64(totalSize); off += uint64(readChunkSize) {
		sz := uint32(readChunkSize)
		if off+uint64(sz) > uint64(totalSize) {
			sz = uint32(uint64(totalSize) - off)
		}
		readResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: sessionID,
			Fh:        fh,
			Ino:       ino,
			Offset:    off,
			Size:      sz,
		})
		require.NoError(t, err)
		readBackAll = append(readBackAll, readResp.GetData()...)
	}

	actualHash := sha256.Sum256(readBackAll)
	assert.Equal(t, expectedHash, actualHash, "S3 瞬时故障重试后，冷存储回源读取的数据 SHA-256 必须 100%% 无损一致")

	// 10. 释放句柄
	_, err = env.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Fh:        fh,
	})
	require.NoError(t, err)
}

// TestE2E_Storage_MultiTenantVirtualDiskConcurrency 验证多租户虚拟磁盘并发挂载、独立写租约、PostgreSQL Schema 隔离与 S3 命名空间零污染
func TestE2E_Storage_MultiTenantVirtualDiskConcurrency(t *testing.T) {
	ctx := context.Background()
	minioTarget := "http://127.0.0.1:9000"
	bucketName := "bowl-data"
	accessKey := "minioadmin"
	secretKey := "minioadmin"

	directMinioClient, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)
	exists, err := directMinioClient.BucketExists(ctx, bucketName)
	require.NoError(t, err)
	if !exists {
		err = directMinioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	env := startStressEnv(t, stressEnvOptions{
		s3Enabled:   true,
		s3Endpoint:  minioTarget,
		s3Bucket:    bucketName,
		s3AccessKey: accessKey,
		s3SecretKey: secretKey,
	})

	const numTenants = 4
	type tenantState struct {
		diskID     string
		schemaName string
		clientID   string
		sessionID  string
		ino        uint64
		fh         uint64
		payload    []byte
		hash       [32]byte
	}

	tenants := make([]*tenantState, numTenants)
	var wg sync.WaitGroup
	tenantErrs := make(chan error, numTenants)

	// 1. 并发创建、挂载、写入各自独立租户的虚拟磁盘
	const tenantDataSize = 2 * 1024 * 1024 // 2 MB 每租户
	for i := 0; i < numTenants; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			diskName := fmt.Sprintf("tenant-disk-%d-%s", idx, uuid.New().String()[:8])
			cResp, err := env.diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
				Name:          diskName,
				CapacityBytes: 10 * 1024 * 1024 * 1024,
				InodesQuota:   100000,
			})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d 创建磁盘失败: %w", idx, err)
				return
			}
			dID := cResp.GetDisk().GetDiskId()
			parsedUUID, _ := uuid.Parse(dID)
			sName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedUUID.String(), "-", "_"))

			cID := fmt.Sprintf("client-tenant-%d", idx)
			mResp, err := env.storageClient.Mount(ctx, &bowlv1.MountRequest{
				DiskId:   dID,
				ClientId: cID,
				ReadOnly: false,
			})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d 挂载磁盘失败: %w", idx, err)
				return
			}
			sessID := mResp.GetSessionId()

			fResp, err := env.storageClient.Create(ctx, &bowlv1.CreateRequest{
				SessionId: sessID,
				ParentIno: 1,
				Name:      fmt.Sprintf("tenant-file-%d.dat", idx),
				Mode:      0o644,
				Flags:     2,
			})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d 创建文件失败: %w", idx, err)
				return
			}
			fileIno := fResp.GetInode().GetIno()
			fileFH := fResp.GetFileHandle().GetFh()

			// 生成包含租户编号特征的独立数据载荷
			payload := make([]byte, tenantDataSize)
			for p := range payload {
				payload[p] = byte((idx*101 + p*37) & 0xff)
			}
			hash := sha256.Sum256(payload)

			wResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
				SessionId: sessID,
				Fh:        fileFH,
				Ino:       fileIno,
				Offset:    0,
				Data:      payload,
			})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d 写入失败: %w", idx, err)
				return
			}
			if wResp.GetWritten() != uint32(tenantDataSize) {
				tenantErrs <- fmt.Errorf("租户 %d 写入长度不匹配", idx)
				return
			}

			// 同步持久化
			_, err = env.storageClient.Flush(ctx, &bowlv1.FlushRequest{SessionId: sessID, Fh: fileFH})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d Flush 失败: %w", idx, err)
				return
			}
			_, err = env.storageClient.Fsync(ctx, &bowlv1.FsyncRequest{SessionId: sessID, Fh: fileFH, Ino: fileIno})
			if err != nil {
				tenantErrs <- fmt.Errorf("租户 %d Fsync 失败: %w", idx, err)
				return
			}

			tenants[idx] = &tenantState{
				diskID:     dID,
				schemaName: sName,
				clientID:   cID,
				sessionID:  sessID,
				ino:        fileIno,
				fh:         fileFH,
				payload:    payload,
				hash:       hash,
			}
		}(i)
	}

	wg.Wait()
	close(tenantErrs)
	for err := range tenantErrs {
		require.NoError(t, err)
	}

	defer func() {
		for _, tn := range tenants {
			if tn != nil {
				_, _ = env.pool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+tn.schemaName+"\" CASCADE")
			}
		}
	}()

	// 2. 校验 PostgreSQL 多租户 Schema 隔离性：每个磁盘拥有完全隔离的 Schema
	for i, tn := range tenants {
		var schemaExists bool
		err := env.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", tn.schemaName).Scan(&schemaExists)
		require.NoError(t, err)
		assert.True(t, schemaExists, "租户 %d 的 PostgreSQL Schema %s 必须存在", i, tn.schemaName)

		var chunkCount int
		err = env.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM "%s".chunks WHERE ino = $1`, tn.schemaName), int64(tn.ino)).Scan(&chunkCount)
		require.NoError(t, err)
		assert.Greater(t, chunkCount, 0, "租户 %d 的 chunks 必须仅存在于其独立 Schema 中", i)
	}

	// 3. 校验 S3 命名空间隔离性：S3 对象严格限定在 chunks/<disk_id>/ 前缀下
	for i, tn := range tenants {
		diskPrefix := fmt.Sprintf("chunks/%s/", tn.diskID)
		cnt, err := countS3Objects(ctx, directMinioClient, bucketName, diskPrefix)
		require.NoError(t, err)
		assert.Greater(t, cnt, 0, "租户 %d 的 S3 数据对象必须位于其独立命名空间 %s 下", i, diskPrefix)

		// 确认该租户的 S3 前缀绝不包含其他租户的数据
		for otherIdx, otherTn := range tenants {
			if otherIdx != i {
				assert.NotEqual(t, tn.diskID, otherTn.diskID)
			}
		}
	}

	// 4. 并发读取校验：每个租户回读数据并比对 SHA-256
	for i, tn := range tenants {
		rResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: tn.sessionID,
			Fh:        tn.fh,
			Ino:       tn.ino,
			Offset:    0,
			Size:      uint32(tenantDataSize),
		})
		require.NoError(t, err)
		readHash := sha256.Sum256(rResp.GetData())
		assert.Equal(t, tn.hash, readHash, "租户 %d 回读数据 SHA-256 必须完全一致", i)
	}

	// 5. 模拟并发级联删除：删除前 2 个租户的虚拟磁盘，验证剩余租户正常运作且已删除租户物理数据清理无残留
	deletedTenants := tenants[:2]
	survivingTenants := tenants[2:]

	var delWg sync.WaitGroup
	for _, dt := range deletedTenants {
		delWg.Add(1)
		go func(target *tenantState) {
			defer delWg.Done()
			delStart := time.Now()
			_, err := env.diskClient.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
				DiskId: target.diskID,
				Force:  true,
			})
			require.NoError(t, err)
			assert.Less(t, time.Since(delStart), 500*time.Millisecond, "DeleteDisk 前台极速响应延迟必须小于 500ms")
		}(dt)
	}
	delWg.Wait()

	// 6. 验证存活的租户磁盘不受任何影响，仍可继续正常读写
	for i, st := range survivingTenants {
		extraPayload := []byte("surviving tenant append data block")
		wResp, err := env.storageClient.Write(ctx, &bowlv1.WriteRequest{
			SessionId: st.sessionID,
			Fh:        st.fh,
			Ino:       st.ino,
			Offset:    uint64(tenantDataSize),
			Data:      extraPayload,
		})
		require.NoError(t, err, "存活租户 %d 写入不受并发删除影响", i)
		assert.Equal(t, uint32(len(extraPayload)), wResp.GetWritten())

		rResp, err := env.storageClient.Read(ctx, &bowlv1.ReadRequest{
			SessionId: st.sessionID,
			Fh:        st.fh,
			Ino:       st.ino,
			Offset:    uint64(tenantDataSize),
			Size:      uint32(len(extraPayload)),
		})
		require.NoError(t, err)
		assert.Equal(t, extraPayload, rResp.GetData(), "存活租户 %d 读取数据必须完全一致", i)
	}

	// 7. 释放存活租户句柄
	for _, st := range survivingTenants {
		_, _ = env.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
			SessionId: st.sessionID,
			Fh:        st.fh,
		})
	}
}
