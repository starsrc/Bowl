//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// execPsqlQuery 在 Docker 容器中执行 SQL 并返回单行或纯文本输出。
// 支持通过环境变量 BOWL_POSTGRES_CONTAINER（默认 "bowl-postgres"）与 BOWL_POSTGRES_DB（默认 "bowl"）配置。
func execPsqlQuery(ctx context.Context, sqlQuery string) (string, error) {
	container := os.Getenv("BOWL_POSTGRES_CONTAINER")
	if container == "" {
		container = "bowl-postgres"
	}
	dbName := os.Getenv("BOWL_POSTGRES_DB")
	if dbName == "" {
		dbName = "bowl"
	}
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", dbName, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	cmd.Stdin = strings.NewReader(sqlQuery)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("执行 psql 失败: %w, stderr: %s, sql: %s", err, stderr.String(), sqlQuery)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// verifyLockBackendDBConsistency 查询 SELECT root_ino FROM disks WHERE disk_id = '$diskID' 断言等于 wantRootIno，
// 确保 RPC 客户端与 SQL 审计连接指向同一个后端数据库实例。
func verifyLockBackendDBConsistency(t *testing.T, ctx context.Context, diskID string, wantRootIno uint64) {
	t.Helper()
	query := fmt.Sprintf("SELECT root_ino FROM disks WHERE disk_id = '%s';", diskID)
	out, err := execPsqlQuery(ctx, query)
	require.NoError(t, err, "核验后端数据库一致性 SQL 查询失败")
	gotRootIno, err := strconv.ParseUint(strings.TrimSpace(out), 10, 64)
	require.NoError(t, err, "解析 disks.root_ino 失败: %q", out)
	require.Equal(t, wantRootIno, gotRootIno, "RPC 创建的磁盘 root_ino 必须与 SQL 直接查询到的 root_ino 完全一致，以确保审计目标同一")
}

// waitForPendingWaiterInDB 通过直接轮询 PostgreSQL 确认等待者记录已真实提交落盘为 PENDING。
// 彻底替代不可靠的固定休眠 time.Sleep(150ms)。
func waitForPendingWaiterInDB(t *testing.T, ctx context.Context, ino, sessionID, requestID, fh uint64, waitErrCh <-chan error) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT COUNT(*) FROM file_lock_waiters WHERE ino = %d AND session_id = %d AND request_id = %d AND fh = %d AND status = 1;",
		ino, sessionID, requestID, fh)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err, ok := <-waitErrCh:
			if ok && err != nil {
				t.Fatalf("后台 SetLkw 协程在等待者记录落盘前提前退出: %v", err)
			}
		case <-pollCtx.Done():
			t.Fatalf("等待 file_lock_waiters 记录提交为 PENDING 超时 (ino=%d, session=%d, req=%d, fh=%d): %v",
				ino, sessionID, requestID, fh, pollCtx.Err())
		case <-ticker.C:
			stepCtx, stepCancel := context.WithTimeout(pollCtx, 1*time.Second)
			out, err := execPsqlQuery(stepCtx, query)
			stepCancel()
			if err != nil {
				// 单次查询抖动允许重试，直至 pollCtx 超时
				continue
			}
			if out == "1" {
				return
			}
		}
	}
}

// startSessionKeepalive 为指定的一个或多个会话启动后台续期保活协程。
// 协程受 keepaliveCtx 控制，每 500ms 发起一次 RenewSession，并使用 WaitGroup 与 t.Cleanup 保证退出汇合与错误收集。
func startSessionKeepalive(t *testing.T, ctx context.Context, client storagev1connect.StorageServiceClient, sessionIDs ...uint64) {
	t.Helper()
	keepaliveCtx, keepaliveCancel := context.WithCancel(ctx)
	var keepaliveWg sync.WaitGroup
	errCh := make(chan error, len(sessionIDs)*32)

	// t.Cleanup 执行顺序为后进先出 (LIFO)，在 CreateDisk 之后注册的 Cleanup 将优先于 DeleteDisk 执行。
	t.Cleanup(func() {
		keepaliveCancel()
		keepaliveWg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil && !errors.Is(err, context.Canceled) && connect.CodeOf(err) != connect.CodeCanceled {
				t.Errorf("后台会话保活续租失败: %v", err)
			}
		}
	})

	for _, sid := range sessionIDs {
		sid := sid
		keepaliveWg.Add(1)
		go func() {
			defer keepaliveWg.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-keepaliveCtx.Done():
					return
				case <-ticker.C:
					stepCtx, cancel := context.WithTimeout(keepaliveCtx, 2*time.Second)
					_, err := client.RenewSession(stepCtx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
					cancel()
					if err != nil {
						if keepaliveCtx.Err() != nil || errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled {
							return
						}
						select {
						case errCh <- fmt.Errorf("会话 %d 续租失败: %w", sid, err):
						default:
						}
					}
				}
			}
		}()
	}
}

// TestE2E_H3_SessionlessFlushDeletesOtherSessionLocks 复现 H3：
// 不带会话 ID 的 Flush(SessionId: nil) 会跨客户端误删别人的 POSIX 文件锁。
func TestE2E_H3_SessionlessFlushDeletesOtherSessionLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h3-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
			t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
		}
	})

	// 注册两个独立活跃会话 sessionA (310001) 与 sessionC (310002)
	sessionA := uint64(310001)
	sessionC := uint64(310002)

	renewA, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sessionA}))
	require.NoError(t, err, "RenewSession sessionA 必须成功")
	require.NotNil(t, renewA.Msg.GetExpiresAt())

	renewC, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sessionC}))
	require.NoError(t, err, "RenewSession sessionC 必须成功")
	require.NotNil(t, renewC.Msg.GetExpiresAt())

	// 启动受控会话保活协程，确保在 DeleteDisk 之前汇合退出
	startSessionKeepalive(t, ctx, client, sessionA, sessionC)

	// 2. 客户端 A 在 sessionA 下调用 Create 创建文件 fileIno 并获得句柄 fhA
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("h3_locked_file.bin"),
		Mode:      0o666,
		Flags:     oRdWr,
		SessionId: &sessionA,
	}))
	require.NoError(t, err, "客户端 A Create 文件应成功")
	fileIno := createResp.Msg.GetInode().GetIno()
	fhA := createResp.Msg.GetFh()

	// 3. 客户端 A 调用 SetLk 获取区间 [0, 100] 的排他写锁 F_WRLCK
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: 8888,
		SessionId: sessionA,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "客户端 A 加排他写锁应成功")

	// 4. 客户端 B 使用无会话模式 (SessionId: nil) 调用 Open 打开同一文件获得 fhB
	openBResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdOnly,
		SessionId: nil,
	}))
	require.NoError(t, err, "客户端 B 无会话 Open 应成功")
	fhB := openBResp.Msg.GetFh()

	// 客户端 B 调用无会话 Flush(Ino: fileIno, Fh: fhB, LockOwner: 8888, SessionId: nil)
	lockOwner8888 := uint64(8888)
	_, err = client.Flush(ctx, connect.NewRequest(&storagev1.FlushRequest{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: &lockOwner8888,
		SessionId: nil,
	}))
	require.NoError(t, err, "客户端 B 无会话 Flush 应成功")

	// 5. 客户端 C 在 sessionC 下调用 Open 获得 fhC
	openCResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &sessionC,
	}))
	require.NoError(t, err, "客户端 C 打开文件应成功")
	fhC := openCResp.Msg.GetFh()

	// 6. 正确性断言（预期无 Bug 行为，当前因 H3 缺陷将触发报红）：
	// 客户端 C 调用 GetLk 探测 [0, 100] 区间时，必须仍然探测到客户端 A (Pid: 1001) 持有的 F_WRLCK 冲突写锁
	probeResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fhC,
		LockOwner: 9999,
		SessionId: sessionC,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1002,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "客户端 C 探测锁状态应成功")
	assert.Equal(t, fWrLck, probeResp.Msg.GetLk().GetType(), "客户端 A 的锁应依然存在，GetLk 应探测到冲突写锁")
	assert.Equal(t, uint32(1001), probeResp.Msg.GetLk().GetPid(), "冲突锁持有人 PID 应为客户端 A 的 PID 1001")

	// 客户端 C 调用非阻塞 SetLk 申请排他写锁，必须因客户端 A 仍持有锁而被拒绝并返回 EAGAIN
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fhC,
		LockOwner: 9999,
		SessionId: sessionC,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1002,
		},
		LkFlags: 0,
	}))
	testutil.RequireConnectError(t, err, connect.CodeResourceExhausted, "EAGAIN")
}

// TestE2E_H4_SetLkwReleaseFhLeavesZombieWaiter 复现 H4：
// 阻塞等锁 SetLkw 期间关闭文件句柄，留下僵尸排队记录导致该文件永远无法再加锁。
func TestE2E_H4_SetLkwReleaseFhLeavesZombieWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h4-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
			t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
		}
	})

	// 注册三个活跃会话 session1 (320001), session2 (320002), session3 (320003)
	session1 := uint64(320001)
	session2 := uint64(320002)
	session3 := uint64(320003)

	for _, sid := range []uint64{session1, session2, session3} {
		renew, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
		require.NoError(t, err, "RenewSession 必须成功")
		require.NotNil(t, renew.Msg.GetExpiresAt())
	}
	startSessionKeepalive(t, ctx, client, session1, session2, session3)

	// 2. 会话 1 创建并打开文件 fileIno (fh1)，获取 [0, 100] 排他写锁
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("h4_zombie_waiter_file.bin"),
		Mode:      0o666,
		Flags:     oRdWr,
		SessionId: &session1,
	}))
	require.NoError(t, err, "会话 1 Create 文件应成功")
	fileIno := createResp.Msg.GetInode().GetIno()
	fh1 := createResp.Msg.GetFh()

	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "会话 1 加排他写锁应成功")

	// 3. 会话 2 打开 fileIno 得到 fh2
	open2Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session2,
	}))
	require.NoError(t, err, "会话 2 Open 文件应成功")
	fh2 := open2Resp.Msg.GetFh()

	// 启动后台 goroutine 在 fh2 上调用阻塞 SetLkw，并注册取消与汇合清理屏障
	waiterCtx, waiterCancel := context.WithTimeout(ctx, 5*time.Second)
	waiterDone := make(chan struct{})
	waitErrCh := make(chan error, 1)
	go func() {
		defer close(waiterDone)
		defer waiterCancel()

		_, waitErr := client.SetLkw(waiterCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			SessionId: session2,
			RequestId: 70001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   2002,
			},
			LkFlags: 0,
		}))
		waitErrCh <- waitErr
	}()
	t.Cleanup(func() {
		waiterCancel()
		<-waiterDone
	})

	// 4. 真实数据库轮询：确认会话 2 的 SetLkw 已在 file_lock_waiters 中登记为 PENDING 并提交落盘
	waitForPendingWaiterInDB(t, ctx, fileIno, session2, 70001, fh2, waitErrCh)

	// 5. 会话 2 调用 Release 关闭正在等待锁的句柄 fh2
	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino:       fileIno,
		Fh:        fh2,
		SessionId: &session2,
	}))
	require.NoError(t, err, "会话 2 Release fh2 应成功")

	// 6. 等待后台 SetLkw 协程返回，断言其因句柄已关闭而返回 EBADF
	var waitErr error
	select {
	case waitErr = <-waitErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("等待后台 SetLkw 退出超时")
	}
	testutil.RequireConnectError(t, waitErr, connect.CodeFailedPrecondition, "EBADF")

	// 审计 file_lock_waiters 表中是否存在残留僵尸等待记录
	auditCtx, auditCancel := context.WithTimeout(ctx, 3*time.Second)
	zombieCountStr, err := execPsqlQuery(auditCtx, fmt.Sprintf("SELECT COUNT(*) FROM file_lock_waiters WHERE ino = %d AND session_id = %d AND request_id = %d;",
		fileIno, session2, 70001))
	auditCancel()
	require.NoError(t, err, "直接表审计 file_lock_waiters 失败")
	zombieWaiterCount, err := strconv.Atoi(zombieCountStr)
	require.NoError(t, err, "解析 zombieWaiterCount 失败: %q", zombieCountStr)

	// 7. 会话 1 调用 SetLk(F_UNLCK) 释放 [0, 100] 上的写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "会话 1 释放写锁应成功")

	// 8. 此时 fileIno 上已没有任何活跃锁，也没有任何存活的等待协程！会话 3 打开 fileIno 得到 fh3，调用非阻塞 SetLk 申请 [0, 100] 排他写锁
	open3Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session3,
	}))
	require.NoError(t, err, "会话 3 Open 文件应成功")
	fh3 := open3Resp.Msg.GetFh()

	// 9. 正确性断言（预期无 Bug 行为，当前因 H4 缺陷残留僵尸等待者将触发报红）：
	// 原持锁者已解锁且原等待句柄已关闭，新加锁请求不应被僵尸等待记录拦截，必须立即获锁成功
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))

	assert.Equal(t, 0, zombieWaiterCount, "句柄 Release 且 SetLkw 返回 EBADF 退出后，file_lock_waiters 表中绝不应残留 PENDING 僵尸记录")
	require.NoError(t, err, "原持锁者已解锁且原等待句柄已关闭，新加锁请求不应被僵尸等待记录拦截")
}

// TestE2E_H11_BlockingSetLkwTimeoutAndWakeupLiveness 复现 H11：
// 阻塞文件锁 SetLkw 显式 context.WithTimeout 超时保护与异常挂起拦截。
// 分为两个子测试：真实 Docker 环境验证挂起拦截与单元测试套件 AST 静态审计。
func TestE2E_H11_BlockingSetLkwTimeoutAndWakeupLiveness(t *testing.T) {
	t.Run("LiveDocker_SetLkwExplicitTimeoutCatchesUnboundedHang", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := testutil.NewStorageClient()

		// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
		diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "e2e-h11-disk",
			MaxBytes: 50 * 1024 * 1024,
		}))
		require.NoError(t, err, "CreateDisk 必须成功")
		disk := diskResp.Msg.GetDisk()
		verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
			if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
				t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
			}
		})

		// 注册三个活跃会话 holderSession (330001), abortedWaiterSession (330002), callerSession (330003)
		holderSession := uint64(330001)
		abortedWaiterSession := uint64(330002)
		callerSession := uint64(330003)

		for _, sid := range []uint64{holderSession, abortedWaiterSession, callerSession} {
			renew, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
			require.NoError(t, err, "RenewSession 必须成功")
			require.NotNil(t, renew.Msg.GetExpiresAt())
		}
		startSessionKeepalive(t, ctx, client, holderSession, abortedWaiterSession, callerSession)

		// 2. holderSession 创建文件并在 [0, 4096] 上持有写锁
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte("h11_liveness_file.bin"),
			Mode:      0o666,
			Flags:     oRdWr,
			SessionId: &holderSession,
		}))
		require.NoError(t, err, "holderSession Create 文件应成功")
		fileIno := createResp.Msg.GetInode().GetIno()
		fhHolder := createResp.Msg.GetFh()

		_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "holderSession 加锁应成功")

		// 3. abortedWaiterSession 打开文件并在 [0, 4096] 上发起带超时的后台 SetLkw
		openWaiterResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:       fileIno,
			Flags:     oRdWr,
			SessionId: &abortedWaiterSession,
		}))
		require.NoError(t, err, "abortedWaiterSession Open 文件应成功")
		fhWaiter := openWaiterResp.Msg.GetFh()

		waiterCtx, waiterCancel := context.WithTimeout(ctx, 5*time.Second)
		waiterDone := make(chan struct{})
		waiterErrCh := make(chan error, 1)
		go func() {
			defer close(waiterDone)
			defer waiterCancel()

			_, waiterErr := client.SetLkw(waiterCtx, connect.NewRequest(&storagev1.SetLkwRequest{
				Ino:       fileIno,
				Fh:        fhWaiter,
				LockOwner: 202,
				SessionId: abortedWaiterSession,
				RequestId: 70002,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   4096,
					Type:  fWrLck,
					Pid:   2002,
				},
				LkFlags: 0,
			}))
			waiterErrCh <- waiterErr
		}()
		t.Cleanup(func() {
			waiterCancel()
			<-waiterDone
		})

		// 轮询数据库确认 abortedWaiterSession 的 SetLkw 成功入队为 PENDING
		waitForPendingWaiterInDB(t, ctx, fileIno, abortedWaiterSession, 70002, fhWaiter, waiterErrCh)

		// abortedWaiterSession 关闭 fhWaiter
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:       fileIno,
			Fh:        fhWaiter,
			SessionId: &abortedWaiterSession,
		}))
		require.NoError(t, err, "Release fhWaiter 应成功")

		// 等待后台协程退出并断言 EBADF
		var waiterErr error
		select {
		case waiterErr = <-waiterErrCh:
		case <-time.After(5 * time.Second):
			t.Fatal("等待后台 SetLkw 退出超时")
		}
		testutil.RequireConnectError(t, waiterErr, connect.CodeFailedPrecondition, "EBADF")

		// 4. holderSession 释放 [0, 4096] 写锁
		_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fUnLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "holderSession 释放写锁应成功")

		// 调用 GetLk 确认当前文件 [0, 4096] 区间已无任何活跃冲突锁
		getLkResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "GetLk 探测应成功")
		assert.Equal(t, fUnLck, getLkResp.Msg.GetLk().GetType(), "文件当前应无任何活跃锁 (F_UNLCK)")

		// 5. callerSession 打开文件
		openCallerResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:       fileIno,
			Flags:     oRdWr,
			SessionId: &callerSession,
		}))
		require.NoError(t, err, "callerSession Open 文件应成功")
		fhCaller := openCallerResp.Msg.GetFh()

		// 此时文件已无任何活跃冲突锁！callerSession 遵循 H11 规范，绑定显式 context.WithTimeout 调用预期立即获锁返回的 SetLkw
		stepCtx, stepCancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		defer stepCancel()

		_, err = client.SetLkw(stepCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fhCaller,
			LockOwner: 303,
			SessionId: callerSession,
			RequestId: 80001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   3003,
			},
			LkFlags: 0,
		}))

		// 6. 正确性断言（预期无 Bug 行为，当前因 H11/H4 异常挂起缺陷将触发超时报红）：
		// 既然 GetLk 已确认文件无任何活跃冲突锁，预期不阻塞的 SetLkw 在显式 Timeout 保护下必须在几十毫秒内立即成功获锁返回，绝不能无限挂起至超时
		require.NoError(t, err, "预期不阻塞的 SetLkw 在显式 Timeout 保护下必须立即成功，绝不能无限挂起至超时")
	})
}
