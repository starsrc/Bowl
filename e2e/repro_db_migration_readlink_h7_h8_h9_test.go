//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// getPostgresContainer 返回 PostgreSQL 容器名称，默认值为 "bowl-postgres"。
func getPostgresContainer() string {
	if c := os.Getenv("BOWL_POSTGRES_CONTAINER"); c != "" {
		return c
	}
	return "bowl-postgres"
}

// getPostgresDB 返回目标数据库名称，默认值为 "bowl"。
func getPostgresDB() string {
	if db := os.Getenv("BOWL_POSTGRES_DB"); db != "" {
		return db
	}
	return "bowl"
}

// runPsql 在真实 PostgreSQL 容器中执行 SQL 语句并返回标准输出或错误。
// 所有 exec.Command 统一使用 exec.CommandContext(ctx, ...) 保证受测试超时控制。
func runPsql(ctx context.Context, dbName string, sqlText string) (string, error) {
	container := getPostgresContainer()
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", dbName, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	cmd.Stdin = strings.NewReader(sqlText)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("执行 psql 失败 (container: %s, db: %s): %w, stderr: %s, sql:\n%s",
			container, dbName, err, stderr.String(), sqlText)
	}

	return strings.TrimSpace(stdout.String()), nil
}

// execPsql 在真实 PostgreSQL 容器中执行 SQL 语句，并在失败时调用 t.Fatalf。
func execPsql(t testing.TB, ctx context.Context, dbName string, sqlText string) string {
	t.Helper()
	out, err := runPsql(ctx, dbName, sqlText)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

// verifyBackendDatabaseConsistency 核验 RPC 客户端与 SQL Helper 必须连接同一个后端数据库实例。
func verifyBackendDatabaseConsistency(t testing.TB, ctx context.Context, dbName string, diskID string, rootIno uint64) {
	t.Helper()
	query := fmt.Sprintf("SELECT root_ino FROM disks WHERE disk_id = '%s';", diskID)
	out := execPsql(t, ctx, dbName, query)
	require.Equal(t, fmt.Sprintf("%d", rootIno), out,
		"RPC 客户端创建的 disk 与 SQL Helper 连接的数据库实例不一致 (diskID=%s)", diskID)
}

// startLockWaitMonitor 启动一个轻量协程每 15ms 监控一次 disks/client_sessions/inodes 跨表锁等待，
// 返回 stop 闭包，在调用时停止监控并返回记录的锁等待详情。
func startLockWaitMonitor(ctx context.Context, dbName string) func() []string {
	monCtx, cancel := context.WithCancel(ctx)
	var obs []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		query := `SELECT format('pid=%s blocked on rel=%s query=[%s]', l.pid, l.relation::regclass, a.query) ` +
			`FROM pg_locks l JOIN pg_stat_activity a ON l.pid = a.pid ` +
			`WHERE a.datname = current_database() AND NOT l.granted ` +
			`AND l.relation IN ('disks'::regclass, 'client_sessions'::regclass, 'inodes'::regclass);`
		for {
			select {
			case <-monCtx.Done():
				return
			case <-time.After(25 * time.Millisecond):
				out, err := runPsql(monCtx, dbName, query)
				if err == nil && strings.TrimSpace(out) != "" {
					mu.Lock()
					for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
						if line != "" {
							obs = append(obs, line)
						}
					}
					mu.Unlock()
				}
			}
		}
	}()

	return func() []string {
		cancel()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		res := make([]string, len(obs))
		copy(res, obs)
		return res
	}
}

// fetchPostgresDeadlockLogs 从 PostgreSQL 服务端提取 startTs 以来的死锁检测日志详情。
func fetchPostgresDeadlockLogs(ctx context.Context, startTs string) string {
	cmd := exec.CommandContext(ctx, "docker", "logs", "--since", startTs, getPostgresContainer())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("获取 PostgreSQL 日志失败: %v", err)
	}
	var matched []string
	for _, line := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "deadlock detected") ||
			strings.Contains(trimmed, "waits for") ||
			strings.Contains(trimmed, "blocked by process") ||
			strings.HasPrefix(trimmed, "Process ") {
			matched = append(matched, trimmed)
		}
	}
	if len(matched) == 0 {
		return ""
	}
	// 最多返回前 20 条匹配行，避免超长日志截断
	if len(matched) > 20 {
		matched = matched[:20]
	}
	return strings.Join(matched, "\n")
}

// newStressStorageClient 创建一个针对高并发测试优化连接池与超时设置的客户端。
func newStressStorageClient() storagev1connect.StorageServiceClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     90 * time.Second,
	}
	return storagev1connect.NewStorageServiceClient(
		&http.Client{
			Timeout:   120 * time.Second,
			Transport: transport,
		},
		testutil.GetStorageServiceAddr(),
	)
}

// TestE2E_H7_CrossTableLockOrderDeadlock 复现 H7 缺陷：
// 1. Create (Disk->Session) 与 TmpFile (Session->Disk) 锁序反转死锁；
// 2. DeleteDisk (Disk->Inode) 与 SetAttr 扩容 (Inode->Disk) 锁序反转死锁。
func TestE2E_H7_CrossTableLockOrderDeadlock(t *testing.T) {
	client := newStressStorageClient()
	dbName := getPostgresDB()

	// 子测试 1：Create (Disk -> Session) 与 TmpFile (Session -> Disk) 锁序反转
	t.Run("CreateVsTmpFile_DiskSessionLockOrderInversion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "repro-h7-create-tmpfile-disk",
			MaxBytes: 1024 * 1024 * 1024,
		}))
		require.NoError(t, err, "创建测试虚拟磁盘失败")
		disk := diskResp.Msg.GetDisk()
		rootIno := disk.GetRootIno()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
			if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
				t.Errorf("清理磁盘失败 (diskID: %s): %v", disk.GetDiskId(), err)
			}
		})

		// 核验后端一致性
		verifyBackendDatabaseConsistency(t, ctx, dbName, disk.GetDiskId(), rootIno)

		// 注册共享客户端会话 (SessionId: 410001)
		const sessionID = uint64(410001)
		_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: sessionID,
		}))
		require.NoError(t, err, "注册测试会话失败")

		// 在磁盘根目录下预先创建 24 个子目录，使并发请求分散于不同父目录 Inode
		const numDirs = 24
		dirInos := make([]uint64, numDirs)
		for i := 0; i < numDirs; i++ {
			mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: rootIno,
				Name:      []byte(fmt.Sprintf("dir_deadlock_%d", i)),
				Mode:      0o755,
			}))
			require.NoError(t, err, "创建测试子目录 %d 失败", i)
			dirInos[i] = mkdirResp.Msg.GetInode().GetIno()
		}

		// 记录测试开始前的数据库时间戳（UTC ISO8601，带 5 秒前向冗余以便完整提取服务端日志）与 deadlocks 计数
		startTs := execPsql(t, ctx, dbName, "SELECT to_char((clock_timestamp() - interval '5 seconds') at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"');")
		deadlocksBeforeStr := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
		deadlocksBefore, err := strconv.ParseInt(deadlocksBeforeStr, 10, 64)
		require.NoError(t, err, "解析 deadlocksBefore 失败: %s", deadlocksBeforeStr)

		// 启动轻量监控协程观测跨表锁等待
		stopMonitor := startLockWaitMonitor(ctx, dbName)

		// 启动 12 个 Create 协程与 12 个 TmpFile 协程并发执行，每个协程执行 15 次循环，
		// 共 12 * 15 = 180 次 Create 与 180 次 TmpFile 并发交错，确保未修复版本稳定触发死锁
		const numWorkers = 12
		const loopCount = 15
		type opResult struct {
			op  string
			err error
		}
		totalOps := numWorkers * loopCount * 2
		resCh := make(chan opResult, totalOps)
		var wg sync.WaitGroup
		startCh := make(chan struct{})

		// 启动 12 个 Create 协程（锁序：Disk -> Session），分别作用于子目录 0..11
		for i := 0; i < numWorkers; i++ {
			workerID := i
			dirIno := dirInos[workerID]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				for j := 0; j < loopCount; j++ {
					reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
					resp, err := client.Create(reqCtx, connect.NewRequest(&storagev1.CreateRequest{
						ParentIno: dirIno,
						Name:      []byte(fmt.Sprintf("create_file_%d_%d", workerID, j)),
						Mode:      0o644,
						Flags:     1, // O_WRONLY
						SessionId: protoUint64(sessionID),
					}))
					reqCancel()
					if err == nil {
						if resp.Msg.GetInode() == nil || resp.Msg.GetFh() == 0 {
							err = fmt.Errorf("Create 返回了无效的 Inode 或 Fh: inode=%v, fh=%d", resp.Msg.GetInode(), resp.Msg.GetFh())
						}
					}
					resCh <- opResult{op: "Create", err: err}
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		// 启动 12 个 TmpFile 协程（锁序：Session -> Disk），分别作用于子目录 12..23
		for i := 0; i < numWorkers; i++ {
			workerID := i
			dirIno := dirInos[workerID+numWorkers]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				for j := 0; j < loopCount; j++ {
					reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
					resp, err := client.TmpFile(reqCtx, connect.NewRequest(&storagev1.TmpFileRequest{
						ParentIno: dirIno,
						Mode:      0o644,
						Flags:     2, // O_RDWR
						SessionId: protoUint64(sessionID),
					}))
					reqCancel()
					if err == nil {
						if resp.Msg.GetInode() == nil || resp.Msg.GetFh() == 0 {
							err = fmt.Errorf("TmpFile 返回了无效的 Inode 或 Fh: inode=%v, fh=%d", resp.Msg.GetInode(), resp.Msg.GetFh())
						}
					}
					resCh <- opResult{op: "TmpFile", err: err}
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		close(startCh)
		wg.Wait()
		close(resCh)

		// 停止锁等待监控
		obs := stopMonitor()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer verifyCancel()

		// 最多 500ms 内轮询 pg_stat_database 的 deadlocks 计数
		deadline := time.Now().Add(500 * time.Millisecond)
		var deadlocksAfter int64
		for {
			deadlocksAfterStr := execPsql(t, verifyCtx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
			deadlocksAfter, err = strconv.ParseInt(deadlocksAfterStr, 10, 64)
			require.NoError(t, err, "解析 deadlocksAfter 失败: %s", deadlocksAfterStr)
			if deadlocksAfter > deadlocksBefore || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		deadlockLogs := fetchPostgresDeadlockLogs(verifyCtx, startTs)
		assert.Equal(t, deadlocksBefore, deadlocksAfter,
			"Create(Disk->Session) 与 TmpFile(Session->Disk) 锁序反转触发了 %d 次 PostgreSQL 40P01 死锁\n观测到的锁等待: %s\nPostgreSQL 日志:\n%s",
			deadlocksAfter-deadlocksBefore, strings.Join(obs, "; "), deadlockLogs)

		// 逐一检查所有并发 RPC 结果
		for res := range resCh {
			require.NoError(t, res.err, "并发 %s 请求不应失败", res.op)
		}
	})

	// 子测试 2：DeleteDisk (Disk -> Inode) 与 SetAttr (Inode -> Disk) 锁序反转
	t.Run("DeleteDiskVsSetAttr_DiskInodeLockOrderInversion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		const numDisks = 3
		const filesPerDisk = 12

		startTs := execPsql(t, ctx, dbName, "SELECT to_char((clock_timestamp() - interval '5 seconds') at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"');")
		deadlocksBeforeStr := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
		deadlocksBefore, err := strconv.ParseInt(deadlocksBeforeStr, 10, 64)
		require.NoError(t, err, "解析 deadlocksBefore 失败: %s", deadlocksBeforeStr)

		stopMonitor := startLockWaitMonitor(ctx, dbName)

		var allDeleteErrs []error
		var allSetAttrErrs []error
		var setAttrSuccessCount int64

		for d := 0; d < numDisks; d++ {
			diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
				Name:     fmt.Sprintf("repro-h7-del-setattr-disk-%d", d),
				MaxBytes: 100 * 1024 * 1024,
			}))
			require.NoError(t, err, "创建测试虚拟磁盘失败")
			disk := diskResp.Msg.GetDisk()
			diskID := disk.GetDiskId()
			rootIno := disk.GetRootIno()

			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
				if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
					t.Errorf("清理磁盘失败 (diskID: %s): %v", diskID, err)
				}
			})

			if d == 0 {
				verifyBackendDatabaseConsistency(t, ctx, dbName, diskID, rootIno)
			}

			// 在该磁盘下预先创建常规文件
			fileInos := make([]uint64, filesPerDisk)
			for f := 0; f < filesPerDisk; f++ {
				mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
					ParentIno: rootIno,
					Name:      []byte(fmt.Sprintf("file_%d", f)),
					Mode:      0o100644,
				}))
				require.NoError(t, err, "预先创建常规文件失败 (disk %d, file %d)", d, f)
				fileInos[f] = mknodResp.Msg.GetInode().GetIno()
			}

			// 启动并发协程：多个协程连续调用 SetAttr 修改不同 Size（触发 Inode -> Disk 加锁），
			// 同时另一个协程并发调用 DeleteDisk 删除该磁盘（触发 Disk -> Inode 加锁）。
			// 通过 channel 同步信号确保首个 SetAttr 成功后立即放行 DeleteDisk 与剩余并发 SetAttr 交错竞争，
			// 避免 DeleteDisk 过早完成导致 SetAttr 零成功假绿。
			const numSetAttrWorkers = 4
			const setattrLoops = 6
			setAttrCh := make(chan error, numSetAttrWorkers*setattrLoops)
			deleteCh := make(chan error, 1)

			var roundSuccessCount int64
			var setAttrWg sync.WaitGroup
			var deleteWg sync.WaitGroup
			startCh := make(chan struct{})
			startDeleteCh := make(chan struct{})
			var signalDeleteOnce sync.Once

			for w := 0; w < numSetAttrWorkers; w++ {
				setAttrWg.Add(1)
				go func(workerID int) {
					defer setAttrWg.Done()
					<-startCh
					for l := 0; l < setattrLoops; l++ {
						ino := fileInos[(workerID+l)%filesPerDisk]
						newSize := uint64(1024 * (l + 1))
						_, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
							Ino:  ino,
							Size: protoUint64(newSize),
						}))
						if err == nil {
							atomic.AddInt64(&setAttrSuccessCount, 1)
							atomic.AddInt64(&roundSuccessCount, 1)
							signalDeleteOnce.Do(func() {
								close(startDeleteCh)
							})
						}
						setAttrCh <- err
					}
				}(w)
			}

			// 若所有 SetAttr 执行完毕仍未成功（如生产端异常），兜底放行 DeleteDisk 避免测试挂起，
			// 由末尾断言显式捕获并判负
			go func() {
				setAttrWg.Wait()
				signalDeleteOnce.Do(func() {
					close(startDeleteCh)
				})
			}()

			deleteWg.Add(1)
			go func() {
				defer deleteWg.Done()
				<-startCh
				select {
				case <-startDeleteCh:
				case <-ctx.Done():
					deleteCh <- ctx.Err()
					return
				}
				_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
					DiskId: diskID,
				}))
				deleteCh <- err
			}()

			close(startCh)
			setAttrWg.Wait()
			deleteWg.Wait()
			close(setAttrCh)
			close(deleteCh)

			for dErr := range deleteCh {
				allDeleteErrs = append(allDeleteErrs, dErr)
			}
			for sErr := range setAttrCh {
				allSetAttrErrs = append(allSetAttrErrs, sErr)
			}

			// 显式断言当前磁盘轮次必须至少有 1 次有效扩容成功，严禁单轮零样本空跑假绿
			require.Greater(t, atomic.LoadInt64(&roundSuccessCount), int64(0), "磁盘轮次 %d 必须至少有 1 次有效扩容成功，严禁单轮零样本空跑假绿", d)
		}

		obs := stopMonitor()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer verifyCancel()

		// 汇合后轮询 pg_stat_database.deadlocks，断言死锁未增加
		deadline := time.Now().Add(500 * time.Millisecond)
		var deadlocksAfter int64
		for {
			deadlocksAfterStr := execPsql(t, verifyCtx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
			deadlocksAfter, err = strconv.ParseInt(deadlocksAfterStr, 10, 64)
			require.NoError(t, err, "解析 deadlocksAfter 失败: %s", deadlocksAfterStr)
			if deadlocksAfter > deadlocksBefore || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		deadlockLogs := fetchPostgresDeadlockLogs(verifyCtx, startTs)
		require.Equal(t, deadlocksBefore, deadlocksAfter,
			"DeleteDisk(Disk->Inode) 与 SetAttr(Inode->Disk) 锁序反转触发了 PostgreSQL 40P01 死锁\n观测到的锁等待: %s\nPostgreSQL 日志:\n%s",
			strings.Join(obs, "; "), deadlockLogs)

		// 严格校验所有 DeleteDisk 与 SetAttr 的返回错误（修复 Standards #11，无条件校验 NotFound + ENOENT）
		for _, dErr := range allDeleteErrs {
			require.NoError(t, dErr, "DeleteDisk 必须成功")
		}
		for _, sErr := range allSetAttrErrs {
			if sErr != nil {
				testutil.RequireConnectError(t, sErr, connect.CodeNotFound, "ENOENT")
			}
		}

		// 显式断言 SetAttr 成功扩容次数：每轮至少成功 1 次，严禁零成功空跑假绿
		finalSuccessCount := atomic.LoadInt64(&setAttrSuccessCount)
		t.Logf("SetAttr 有效成功扩容总次数: %d (轮次: %d)", finalSuccessCount, numDisks)
		require.Greater(t, finalSuccessCount, int64(0), "SetAttr 必须至少成功完成有效扩容，严禁零成功空跑假绿")
		require.GreaterOrEqual(t, finalSuccessCount, int64(numDisks), "每个磁盘轮次必须至少成功完成 1 次有效扩容 (总成功数: %d, 轮次: %d)", finalSuccessCount, numDisks)
	})
}

func protoUint64(v uint64) *uint64 {
	return &v
}
