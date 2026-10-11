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

	// 5. Filesystem: Local chunk directory is physically deleted
	_, err = os.Stat(diskDataDir)
	assert.True(t, os.IsNotExist(err), "local chunk data directory %s must be physically removed", diskDataDir)

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
