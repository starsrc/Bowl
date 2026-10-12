package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
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

func getFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for service at %s to be available", url)
}

func isMinIOAvailable(endpoint string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(endpoint + "/minio/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func countS3Objects(ctx context.Context, client *minio.Client, bucket, prefix string) (int, error) {
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	count := 0
	for obj := range client.ListObjects(ctx, bucket, opts) {
		if obj.Err != nil {
			return 0, obj.Err
		}
		count++
	}
	return count, nil
}

func TestE2E_FullLifecycleCascadeCleanup(t *testing.T) {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. Setup Postgres connection pool for direct database assertions
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. Prepare temp directories for binaries and data storage
	baseDir := t.TempDir()
	binDir := filepath.Join(baseDir, "bin")
	dataDir := filepath.Join(baseDir, "data")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	// Find repository root
	repoRoot, err := filepath.Abs("../..")
	require.NoError(t, err)

	diskBin := filepath.Join(binDir, "disk-service")
	storageBin := filepath.Join(binDir, "storage-service")

	// 3. Compile real binaries for DiskService and StorageService
	cmdDiskBuild := exec.Command("go", "build", "-o", diskBin, "./services/disk/cmd")
	cmdDiskBuild.Dir = repoRoot
	out, err := cmdDiskBuild.CombinedOutput()
	require.NoError(t, err, "failed to compile disk-service: %s", string(out))

	cmdStorageBuild := exec.Command("go", "build", "-o", storageBin, "./services/storage/cmd")
	cmdStorageBuild.Dir = repoRoot
	out, err = cmdStorageBuild.CombinedOutput()
	require.NoError(t, err, "failed to compile storage-service: %s", string(out))

	// 4. Allocate ports and launch DiskService process
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

	// 5. Allocate ports and launch StorageService process
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

	// 6. Connect-RPC Client setup
	diskConnectClient := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, diskAddr))
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	storageConnectClient := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, storageAddr))
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	// =========================================================================
	// Scenario Step 1: Create Virtual Disk (Control Plane)
	// =========================================================================
	createResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-cascade-test-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024, // 10 GB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.GetDisk().GetDiskId()
	require.NotEmpty(t, diskID)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, createResp.GetDisk().GetStatus())

	parsedDiskUUID, err := uuid.Parse(diskID)
	require.NoError(t, err)
	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedDiskUUID.String(), "-", "_"))
	diskDataDir := filepath.Join(dataDir, diskID)

	// =========================================================================
	// Scenario Step 2: Mount Virtual Disk (Data Plane)
	// =========================================================================
	clientID := "e2e-client-writer-01"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()
	require.NotEmpty(t, sessionID)
	assert.True(t, mountResp.GetIsWriteLease())
	assert.Equal(t, uint64(1), mountResp.GetRootInode().GetIno())

	// Verify Disk in DiskService is now ATTACHED with clientID as writer
	getDiskResp, err := diskClient.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, getDiskResp.GetDisk().GetStatus())
	assert.Equal(t, clientID, getDiskResp.GetDisk().GetWriterClientId())

	// Verify per-disk schema exists in PostgreSQL
	var schemaCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1", schemaName).Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 1, schemaCount, "PostgreSQL schema %s must exist", schemaName)

	// Verify chunk storage directory exists on local disk
	dirStat, err := os.Stat(diskDataDir)
	require.NoError(t, err)
	assert.True(t, dirStat.IsDir())

	// =========================================================================
	// Scenario Step 3: Namespace Operations (Directories & Files)
	// =========================================================================
	mkdirResp, err := storageClient.Mkdir(ctx, &bowlv1.MkdirRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "logs",
		Mode:      0o755,
	})
	require.NoError(t, err)
	logsIno := mkdirResp.GetInode().GetIno()
	assert.Greater(t, logsIno, uint64(1))

	createFileResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: logsIno,
		Name:      "app.log",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createFileResp.GetInode().GetIno()
	fh := createFileResp.GetFileHandle().GetFh()
	assert.Greater(t, fileIno, logsIno)
	assert.Greater(t, fh, uint64(0))

	// =========================================================================
	// Scenario Step 4: Write Chunk Data, Flush & Extended Attributes
	// =========================================================================
	payload := make([]byte, 32768) // 32 KB payload
	_, err = rand.Read(payload)
	require.NoError(t, err)

	writeResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       fileIno,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(32768), writeResp.GetWritten())

	// Sync dirty buffers
	_, err = storageClient.Flush(ctx, &bowlv1.FlushRequest{SessionId: sessionID, Fh: fh})
	require.NoError(t, err)
	_, err = storageClient.Fsync(ctx, &bowlv1.FsyncRequest{SessionId: sessionID, Fh: fh, Datasync: false})
	require.NoError(t, err)

	// Verify physical chunk file exists
	chunkFilePath := filepath.Join(diskDataDir, fmt.Sprintf("file_%d.bin", fileIno))
	fileInfo, err := os.Stat(chunkFilePath)
	require.NoError(t, err)
	assert.Equal(t, int64(32768), fileInfo.Size())

	// Set extended attributes
	xattrVal := []byte("confidential-prod-data")
	_, err = storageClient.SetXAttr(ctx, &bowlv1.SetXAttrRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Name:      "user.classification",
		Value:     xattrVal,
	})
	require.NoError(t, err)

	getXAttrResp, err := storageClient.GetXAttr(ctx, &bowlv1.GetXAttrRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Name:      "user.classification",
	})
	require.NoError(t, err)
	assert.Equal(t, xattrVal, getXAttrResp.GetValue())

	// =========================================================================
	// Scenario Step 5: Acquire POSIX Byte-Range Lock and BSD Flock
	// =========================================================================
	_, err = storageClient.SetLk(ctx, &bowlv1.SetLkRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		LockOwner: 10001,
		Lk: &bowlv1.PlockRecord{
			Start: 0,
			End:   4096,
			Type:  bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
			Pid:   1234,
		},
	})
	require.NoError(t, err)

	_, err = storageClient.Flock(ctx, &bowlv1.FlockRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		LockOwner: 10001,
		Type:      bowlv1.LockType_LOCK_TYPE_EXCLUSIVE,
	})
	require.NoError(t, err)

	// =========================================================================
	// Scenario Step 6: Non-Graceful Forced Cascade Deletion (DeleteDisk force=true)
	// =========================================================================
	// Without unmounting or closing handles, directly trigger DeleteDisk(force = true)
	_, err = diskClient.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskID,
		Force:  true,
	})
	require.NoError(t, err)

	// =========================================================================
	// Scenario Step 7: Comprehensive Zero-Remnants Verification
	// =========================================================================
	// 1. Control Plane: GetDisk returns NotFound
	_, err = diskClient.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. Database: disks table record is removed
	var diskCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM disks WHERE disk_id = $1", parsedDiskUUID).Scan(&diskCount)
	require.NoError(t, err)
	assert.Equal(t, 0, diskCount, "disks table entry must be deleted")

	// 3. Database: disk_attachments records are deleted
	var attachmentCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM disk_attachments WHERE disk_id = $1", parsedDiskUUID).Scan(&attachmentCount)
	require.NoError(t, err)
	assert.Equal(t, 0, attachmentCount, "disk_attachments entries must be deleted")

	// 4. Database: PostgreSQL isolated schema is dropped CASCADE
	err = pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1", schemaName).Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 0, schemaCount, "PostgreSQL schema %s must be completely dropped via CASCADE", schemaName)

	// 5. Filesystem: Local chunk directory is physically deleted by background worker
	require.Eventually(t, func() bool {
		_, err = os.Stat(diskDataDir)
		return os.IsNotExist(err)
	}, 5*time.Second, 50*time.Millisecond, "local chunk data directory %s must be physically removed", diskDataDir)

	// 6. Data Plane: Subsequent operations on deleted session or handle fail
	// KeepAlive detects disk removal, triggers orphan handle cleanup, and reports LeaseRevocation
	keepAliveResp, err := storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: sessionID,
	})
	if err == nil {
		assert.True(t, keepAliveResp.GetLeaseRevocation(), "expected lease revocation on deleted disk")
	}

	// Subsequent Read with previously open handle must fail
	_, err = storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       fileIno,
		Offset:    0,
		Size:      1024,
	})
	assert.Error(t, err, "subsequent Read operations must fail after cascade disk deletion")

	// Subsequent Write must fail
	_, err = storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       fileIno,
		Offset:    0,
		Data:      []byte("more data"),
	})
	assert.Error(t, err, "subsequent Write operations must fail after cascade disk deletion")

	// Subsequent Mount for deleted disk must fail with NotFound
	_, err = storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: "new-client",
		ReadOnly: false,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestE2E_CascadeLifecycle_WithS3AndZeroRemnants 验证基于 S3/MinIO 与常驻 Worker 的全链路级联物理清理闭环：
// 1. 数据面写入切片并调用 Fsync 确保物理落盘同步至 S3；
// 2. 调用 DeleteDisk(force=true) 校验前台极速响应延迟严格低于 500ms；
// 3. 轮询数据库 disk_cleanup_tasks 确认任务流转为 COMPLETED；
// 4. 彻底校验 S3 chunks/<disk_id>/、本地 DATA_DIR 与 STAGING_DIR 100% 零残留。
func TestE2E_CascadeLifecycle_WithS3AndZeroRemnants(t *testing.T) {
	minioEndpoint := os.Getenv("S3_ENDPOINT")
	if minioEndpoint == "" {
		minioEndpoint = "http://127.0.0.1:9000"
	}
	if !isMinIOAvailable(minioEndpoint) {
		t.Skip("MinIO 未在 " + minioEndpoint + " 运行，跳过 S3 级联物理清理闭环测试")
	}

	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	// 1. 初始化 PostgreSQL 连接池
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// 2. 初始化 MinIO 客户端与测试 Bucket
	bucketName := "bowl-data"
	accessKey := "minioadmin"
	secretKey := "minioadmin"

	rawMinioClient, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	exists, err := rawMinioClient.BucketExists(ctx, bucketName)
	require.NoError(t, err)
	if !exists {
		err = rawMinioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	// 3. 准备临时目录
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

	// 4. 编译 disk-service 与 storage-service 二进制
	cmdDiskBuild := exec.Command("go", "build", "-o", diskBin, "./services/disk/cmd")
	cmdDiskBuild.Dir = repoRoot
	out, err := cmdDiskBuild.CombinedOutput()
	require.NoError(t, err, "编译 disk-service 失败: %s", string(out))

	cmdStorageBuild := exec.Command("go", "build", "-o", storageBin, "./services/storage/cmd")
	cmdStorageBuild.Dir = repoRoot
	out, err = cmdStorageBuild.CombinedOutput()
	require.NoError(t, err, "编译 storage-service 失败: %s", string(out))

	// 5. 分配端口并启动启用 S3 的 DiskService 进程
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
		"STAGING_DIR="+stagingDir,
		"S3_ENABLED=true",
		"S3_ENDPOINT="+minioEndpoint,
		"S3_BUCKET="+bucketName,
		"S3_ACCESS_KEY="+accessKey,
		"S3_SECRET_KEY="+secretKey,
		"S3_REGION=us-east-1",
		"S3_FORCE_PATH_STYLE=true",
	)
	cmdDisk.Stdout = os.Stdout
	cmdDisk.Stderr = os.Stderr
	require.NoError(t, cmdDisk.Start())
	defer func() {
		cancelDisk()
		_ = cmdDisk.Wait()
	}()

	waitForHTTP(t, diskAddr, 10*time.Second)

	// 6. 分配端口并启动基于 chunk driver 与 S3 的 StorageService 进程
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
		"STORAGE_DRIVER=chunk",
		"DATA_DIR="+dataDir,
		"STAGING_DIR="+stagingDir,
		"DISK_SERVICE_ADDR="+diskAddr,
		"S3_ENABLED=true",
		"S3_ENDPOINT="+minioEndpoint,
		"S3_BUCKET="+bucketName,
		"S3_ACCESS_KEY="+accessKey,
		"S3_SECRET_KEY="+secretKey,
		"S3_REGION=us-east-1",
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

	// 7. 构造 Connect-RPC 客户端
	diskConnectClient := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, diskAddr))
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	storageConnectClient := connect.NewClient(connecthttp.NewTransport(http.DefaultClient, storageAddr))
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	// 步骤 1：控制面创建虚拟磁盘
	createResp, err := diskClient.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "e2e-s3-cascade-test-disk",
		CapacityBytes: 10 * 1024 * 1024 * 1024, // 10 GB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.GetDisk().GetDiskId()
	require.NotEmpty(t, diskID)
	parsedDiskUUID, err := uuid.Parse(diskID)
	require.NoError(t, err)

	diskDataDir := filepath.Join(dataDir, diskID)
	diskStagingDir := filepath.Join(stagingDir, diskID)
	s3ChunkPrefix := fmt.Sprintf("chunks/%s/", diskID)

	// 步骤 2：数据面挂载虚拟磁盘并获取写租约
	clientID := "e2e-s3-writer-01"
	mountResp, err := storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID,
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()
	require.NotEmpty(t, sessionID)
	assert.True(t, mountResp.GetIsWriteLease())

	// 步骤 3：创建文件并写入切片数据
	createFileResp, err := storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "large-dataset.dat",
		Mode:      0o644,
	})
	require.NoError(t, err)
	fileIno := createFileResp.GetInode().GetIno()
	fh := createFileResp.GetFileHandle().GetFh()

	// 写入 64 KB 数据
	payload := make([]byte, 65536)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	writeResp, err := storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       fileIno,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(65536), writeResp.GetWritten())

	// 调用 Flush 与 Fsync，触发数据块落盘并同步上传至 S3/MinIO
	_, err = storageClient.Flush(ctx, &bowlv1.FlushRequest{SessionId: sessionID, Fh: fh})
	require.NoError(t, err)
	_, err = storageClient.Fsync(ctx, &bowlv1.FsyncRequest{SessionId: sessionID, Fh: fh, Datasync: false})
	require.NoError(t, err)

	// 步骤 4：确认 S3 中已存在该磁盘的切片对象
	require.Eventually(t, func() bool {
		cnt, err := countS3Objects(ctx, rawMinioClient, bucketName, s3ChunkPrefix)
		return err == nil && cnt > 0
	}, 10*time.Second, 100*time.Millisecond, "S3 存储桶中应存在已上传的物理切片对象")

	// 步骤 5：测试 DeleteDisk(force=true) 前台极速响应性能 (< 500ms)
	start := time.Now()
	deleteResp, err := diskClient.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskID,
		Force:  true,
	})
	deleteLatency := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
	assert.Less(t, deleteLatency, 500*time.Millisecond, "DeleteDisk 前台响应延迟必须严格小于 500ms")

	// 步骤 6：控制面与数据库瞬时状态校验
	_, err = diskClient.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	var diskCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM disks WHERE disk_id = $1", parsedDiskUUID).Scan(&diskCount)
	require.NoError(t, err)
	assert.Equal(t, 0, diskCount, "disks 元数据表必须已被删除")

	var attachmentCount int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM disk_attachments WHERE disk_id = $1", parsedDiskUUID).Scan(&attachmentCount)
	require.NoError(t, err)
	assert.Equal(t, 0, attachmentCount, "disk_attachments 绑定记录必须已被清空")

	// 步骤 7：轮询等待后台 DiskCleanupWorker 执行任务至 COMPLETED 终态
	require.Eventually(t, func() bool {
		var status int16
		err := pool.QueryRow(ctx, "SELECT status FROM disk_cleanup_tasks WHERE disk_id = $1 ORDER BY created_at DESC LIMIT 1", parsedDiskUUID).Scan(&status)
		return err == nil && status == 3 // CleanupTaskStatusCompleted
	}, 15*time.Second, 100*time.Millisecond, "disk_cleanup_tasks 任务状态必须在超时前流转为 COMPLETED (3)")

	// 步骤 8：全量零残留（Zero-Remnants）闭环验证
	// 8.1 S3/MinIO 对象 100% 物理消除
	s3RemainingCount, err := countS3Objects(ctx, rawMinioClient, bucketName, s3ChunkPrefix)
	require.NoError(t, err)
	assert.Equal(t, 0, s3RemainingCount, "S3 物理对象在 chunks/%s/ 下必须 100%% 彻底清除，无残留", diskID)

	// 8.2 本地 DATA_DIR/<disk_id> 100% 物理消除
	_, err = os.Stat(diskDataDir)
	assert.True(t, os.IsNotExist(err), "本地 DATA_DIR/%s 必须被物理清除", diskID)

	// 8.3 本地 STAGING_DIR/<disk_id> 100% 物理消除
	_, err = os.Stat(diskStagingDir)
	assert.True(t, os.IsNotExist(err), "本地 STAGING_DIR/%s 必须被物理清除", diskID)

	// 步骤 9：验证后续数据面针对已销毁磁盘的操作被彻底拒绝
	_, err = storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       fileIno,
		Offset:    0,
		Size:      1024,
	})
	assert.Error(t, err, "对已级联销毁的虚拟磁盘文件句柄进行 Read 操作必须失败")
}
