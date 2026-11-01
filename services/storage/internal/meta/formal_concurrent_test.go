package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// ============================================================================
// T14 (STARSRC-428): 多协程高并发混沌冲突压测与底表 10 大数学不变量总账证明 (ADR-0016)
// ============================================================================

const (
	defaultConcurrentRuns           = 1
	defaultConcurrentWorkers        = 6
	defaultConcurrentStepsPerWorker = 20
	deepConcurrentWorkers           = 8
)

// formalConcurrentConfig 定义多协程并发混沌压测的运行参数。
type formalConcurrentConfig struct {
	BaseSeed       uint64
	HasFixedSeed   bool
	Runs           int
	Workers        int
	StepsPerWorker int
}

// loadFormalConcurrentConfig 解析环境变量 BOWL_FORMAL_SEED、BOWL_FORMAL_RUNS、BOWL_FORMAL_STEPS、BOWL_FORMAL_WORKERS。
func loadFormalConcurrentConfig(t *testing.T) formalConcurrentConfig {
	t.Helper()

	ov := parseFormalEnvOverrides(t, defaultConcurrentRuns, defaultConcurrentStepsPerWorker)
	baseSeed := uint64(time.Now().UnixNano())
	if ov.HasFixedSeed {
		baseSeed = ov.FixedSeed
	}
	workers := defaultConcurrentWorkers
	if isFormalDeepMode() {
		workers = deepConcurrentWorkers
	}
	if wStr := strings.TrimSpace(os.Getenv("BOWL_FORMAL_WORKERS")); wStr != "" {
		if wVal, err := strconv.Atoi(wStr); err == nil && wVal > 0 {
			workers = wVal
		}
	}

	return formalConcurrentConfig{
		BaseSeed:       baseSeed,
		HasFixedSeed:   ov.HasFixedSeed,
		Runs:           ov.Runs,
		Workers:        workers,
		StepsPerWorker: ov.Steps,
	}
}

// formalConcurrentHandleRec 记录单个 Worker 持有的打开文件或目录句柄。
type formalConcurrentHandleRec struct {
	Fh        uint64
	Ino       uint64
	SessionID *uint64
	Flags     uint32
	IsDir     bool
}

// formalConcurrentSharedEnv 维护多协程并发混沌压测的共享池与线程安全追踪状态。
type formalConcurrentSharedEnv struct {
	T              *testing.T
	Seed           uint64
	RunIndex       int
	Workers        int
	StepsPerWorker int
	Pool           *pgxpool.Pool
	Store          db.Store
	Mgr            meta.Manager // 标准 60s 租约 Manager，用于并发阶段稳态操作
	ShortMgr       meta.Manager // 短租约 (30ms) Manager，用于模拟客户端断连超期与同步清扫
	LeaseTTL       time.Duration
	Disk1          *meta.Disk // 主共享冲突压测虚拟磁盘（并发收敛审计后还将执行 DeleteDisk 级联删盘验证）
	Disk2          *meta.Disk // 辅共享冲突压测虚拟磁盘（用于跨盘 EXDEV 碰撞与删盘后邻盘存活对照验证）
	PinnedIno1     uint64     // Disk1 上固定的锚点常规文件 (4096 字节，保证 UsedBytes >= 4096 以稳定验证 ErrInvalidShrink)
	PinnedIno2     uint64     // Disk2 上固定的锚点常规文件 (4096 字节)

	seqCounter        atomic.Uint64
	nextRequestID     atomic.Uint64
	writeSuccessCount atomic.Int64

	mu                  sync.RWMutex
	dirInos             []uint64          // 候选目录 Ino 池（含双盘根目录及各 Worker 创建的子目录）
	fileInos            []uint64          // 候选非目录 Ino 池（含常规文件、符号链接、特殊节点、TmpFile）
	symlinkInos         []uint64          // 候选符号链接 Ino 池
	entryPool           []formalEntryKey  // 近期创建的候选目录项 (ParentIno, Name)
	workerSessionIDs    []uint64          // 各 Worker 专属会话 ID
	crashSessionIDs     []uint64          // 并发阶段创建的短命断连会话 ID（将在收尾阶段全部超期并被清扫）
	cancelledWaiters    []formalWaiterKey // 所有因超时/取消退出的 SetLkw 请求键
	materializedTmpInos []uint64          // 所有曾通过 Link 成功具名化的 TmpFile 节点 Ino（一旦具名化，即便后续 Unlink 至 nlink=0，其关联句柄也绝不能再带 O_TMPFILE）
	workerTraces        [][]string        // 各 Worker 的中文操作轨迹账单
}

func (e *formalConcurrentSharedEnv) RecordWriteSuccess() {
	e.writeSuccessCount.Add(1)
}

func (e *formalConcurrentSharedEnv) NextSeq() uint64 {
	return e.seqCounter.Add(1)
}

func (e *formalConcurrentSharedEnv) NextRequestID() uint64 {
	return e.nextRequestID.Add(1)
}

func (e *formalConcurrentSharedEnv) AddDirIno(ino uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dirInos = append(e.dirInos, ino)
}

func (e *formalConcurrentSharedEnv) AddFileIno(ino uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.fileInos = append(e.fileInos, ino)
}

func (e *formalConcurrentSharedEnv) AddSymlinkIno(ino uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.symlinkInos = append(e.symlinkInos, ino)
	e.fileInos = append(e.fileInos, ino)
}

func (e *formalConcurrentSharedEnv) AddEntryHint(parentIno uint64, name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.entryPool) >= 128 {
		e.entryPool = e.entryPool[1:]
	}
	e.entryPool = append(e.entryPool, formalEntryKey{ParentIno: parentIno, Name: name})
}

func (e *formalConcurrentSharedEnv) RecordCrashSession(sid uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.crashSessionIDs = append(e.crashSessionIDs, sid)
}

func (e *formalConcurrentSharedEnv) RecordCancelledWaiter(sid, reqID uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancelledWaiters = append(e.cancelledWaiters, formalWaiterKey{
		SessionID: sid,
		RequestID: reqID,
	})
}

func (e *formalConcurrentSharedEnv) RecordMaterializedTmpIno(ino uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.materializedTmpInos = append(e.materializedTmpInos, ino)
}

func (e *formalConcurrentSharedEnv) PickDirIno(rng *rand.Rand) uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.dirInos) == 0 {
		return e.Disk1.RootIno
	}
	return e.dirInos[rng.IntN(len(e.dirInos))]
}

func (e *formalConcurrentSharedEnv) PickActiveDirIno(rng *rand.Rand) uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	// 优先选择前几个基础目录以提升同目录/跨目录冲突密度
	if len(e.dirInos) == 0 || rng.IntN(3) == 0 {
		return e.Disk1.RootIno
	}
	return e.dirInos[rng.IntN(len(e.dirInos))]
}

func (e *formalConcurrentSharedEnv) PickFileIno(rng *rand.Rand) uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.fileInos) == 0 {
		return e.PinnedIno1
	}
	return e.fileInos[rng.IntN(len(e.fileInos))]
}

func (e *formalConcurrentSharedEnv) PickSymlinkIno(rng *rand.Rand) (uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.symlinkInos) == 0 {
		return 0, false
	}
	return e.symlinkInos[rng.IntN(len(e.symlinkInos))], true
}

func (e *formalConcurrentSharedEnv) PickEntryHint(rng *rand.Rand) (formalEntryKey, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.entryPool) == 0 {
		return formalEntryKey{}, false
	}
	return e.entryPool[rng.IntN(len(e.entryPool))], true
}

func (e *formalConcurrentSharedEnv) FormatDiagnosticReport() string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var b strings.Builder
	b.WriteString("\n================================================================================\n")
	b.WriteString("[多协程高并发混沌压测诊断报告]\n")
	b.WriteString(fmt.Sprintf("复现种子 (Seed): 0x%016x (十进制: %d), 轮次: %d\n", e.Seed, e.Seed, e.RunIndex))
	b.WriteString(fmt.Sprintf("单轮精准重放命令:\n  GOWORK=off BOWL_FORMAL_SEED=0x%016x BOWL_FORMAL_RUNS=1 BOWL_FORMAL_WORKERS=%d BOWL_FORMAL_STEPS=%d go -C services/storage test -v ./internal/meta -run '^TestFormalConcurrentChaos$'\n", e.Seed, e.Workers, e.StepsPerWorker))
	for wID, lines := range e.workerTraces {
		b.WriteString(fmt.Sprintf("--- Worker %d 轨迹 (共 %d 步) ---\n", wID, len(lines)))
		for _, line := range lines {
			b.WriteString("  ")
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	b.WriteString("================================================================================\n")
	return b.String()
}

// isLegalConcurrentDomainError 校验高并发冲突下返回的非 nil 错误是否属于合法的领域哨兵错误。
// 严禁出现未恢复的 PostgreSQL 死锁错误 (SQLSTATE 40P01)、串行化失败 (40001)、底层 ErrIO、超时或未知 SQL 错误。
func isLegalConcurrentDomainError(err error) bool {
	if err == nil {
		return true
	}

	// 1. 严禁包含未恢复的 PostgreSQL 死锁 (40P01) 或串行化冲突 (40001) 或重试耗尽
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	if strings.Contains(errMsg, "40p01") ||
		strings.Contains(errMsg, "deadlock detected") ||
		strings.Contains(errMsg, "exceeded max transaction retries") ||
		strings.Contains(errMsg, "conn closed") ||
		strings.Contains(errMsg, "connection refused") {
		return false
	}

	// 2. 必须匹配已知的 meta.Err* 领域哨兵错误（绝不包含 ErrIO，绝不包含通用超时或取消）
	// 收紧为仅包含并发拓扑/空间/句柄竞争下真实可能出现的领域错误
	legalSentinels := []error{
		meta.ErrNotFound,
		meta.ErrAlreadyExists,
		meta.ErrNotEmpty,
		meta.ErrNotDir,
		meta.ErrIsDir,
		meta.ErrNoSpace,
		meta.ErrBadFileHandle,
		meta.ErrAgain,
		meta.ErrDeadlock,
		meta.ErrInvalidArgument,
	}
	for _, sentinel := range legalSentinels {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// assertConcurrentStepErr 断言单步并发操作返回的 err 绝不出现未恢复数据库死锁、底层 ErrIO 或非预期的超时取消。
func assertConcurrentStepErr(t *testing.T, env *formalConcurrentSharedEnv, workerID, step int, opDesc string, err error) {
	t.Helper()
	if !isLegalConcurrentDomainError(err) {
		t.Fatalf("Worker %d 第 %d 步 [%s] 出现非法或未恢复的数据库错误: %v%s",
			workerID, step, opDesc, err, env.FormatDiagnosticReport())
	}
}

// assertConcurrentSetLkwStepErr 断言 SetLkw 操作返回的 err 合法，仅在显式设置超时取消的阻塞锁操作中允许 ErrInterrupted/超时/取消。
func assertConcurrentSetLkwStepErr(t *testing.T, env *formalConcurrentSharedEnv, workerID, step int, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return
	}
	if !isLegalConcurrentDomainError(err) {
		t.Fatalf("Worker %d 第 %d 步 [SetLkw] 出现非法或未恢复的数据库错误: %v%s",
			workerID, step, err, env.FormatDiagnosticReport())
	}
}

func (e *formalConcurrentSharedEnv) assertAllowedErr(t *testing.T, workerID, step int, opDesc string, err error) {
	t.Helper()
	assertConcurrentStepErr(t, e, workerID, step, opDesc, err)
}

// sharedSlotNames 提供高冲突密度的短文件名集合，促使多个 Worker 在相同父目录下频繁碰撞同名目录项。
var sharedSlotNames = []string{
	"slot_a", "slot_b", "slot_c", "slot_d",
	"slot_e", "slot_f", "slot_g", "slot_h",
}

func pickSharedOrUniqueName(rng *rand.Rand, env *formalConcurrentSharedEnv, prefix string) string {
	if rng.IntN(10) < 7 {
		return sharedSlotNames[rng.IntN(len(sharedSlotNames))]
	}
	return fmt.Sprintf("%s_%d", prefix, env.NextSeq())
}

// runConcurrentWorker 执行单个并发 Worker 的随机混沌动作序列。
// 覆盖全部要求动作：
// - Rename (0, RenameNoReplace, RenameExchange 及跨盘 EXDEV)
// - Mkdir, Rmdir, Mknod, Symlink, Create, TmpFile, Link, Unlink, UpdateDisk
// - Open (含 O_TRUNC), OpenDir, SetAttr, Fallocate, Release, ReleaseDir, RenewSession
// - SetLk (POSIX & BSD flock), SetLkw (带短超时), GetLk, Flush (带/不带 lock_owner), Release (带 FuseReleaseFlockUnlock)
// - 只读查账探针 (Lookup, GetAttr, Readlink, ReadDir, ReadDirPlus, GetDisk, StatFS, Fsync, FsyncDir)
func runConcurrentWorker(
	ctx context.Context,
	t *testing.T,
	env *formalConcurrentSharedEnv,
	workerID int,
	workerSeed uint64,
	steps int,
) {
	rng := newFormalRNG(workerSeed)
	workerSID := env.workerSessionIDs[workerID]
	owners := []uint64{
		uint64(1000 + workerID*10 + 1),
		uint64(1000 + workerID*10 + 2),
		uint64(9001), // 跨 Worker 共享 LockOwner，制造跨会话同/异 owner 冲突
	}

	var myFileHandles []formalConcurrentHandleRec
	var myDirHandles []formalConcurrentHandleRec

	recordTrace := func(step int, msg string) {
		env.mu.Lock()
		env.workerTraces[workerID] = append(env.workerTraces[workerID], fmt.Sprintf("[W%d-S%03d] %s", workerID, step, msg))
		env.mu.Unlock()
	}

	// 每个 Worker 初始在共享锚点文件上打开一个专属 O_RDWR 句柄，确保随时可执行文件锁与 Fallocate
	initOpen, initErr := env.Mgr.Open(ctx, meta.OpenParams{
		Ino:       env.PinnedIno1,
		Flags:     meta.O_RDWR,
		SessionID: &workerSID,
	})
	require.NoError(t, initErr, "Worker 专属初始锚点文件打开句柄必须成功")
	require.NotNil(t, initOpen)
	sidCopy := workerSID
	myFileHandles = append(myFileHandles, formalConcurrentHandleRec{
		Fh:        initOpen.Fh,
		Ino:       env.PinnedIno1,
		SessionID: &sidCopy,
		Flags:     meta.O_RDWR,
	})

	for step := 1; step <= steps; step++ {
		roll := rng.IntN(100)

		switch {
		case roll < 12:
			// 1. 跨目录 / 同目录 Rename（含 flags = 0, RenameNoReplace, RenameExchange）
			oldParent := env.PickActiveDirIno(rng)
			newParent := env.PickActiveDirIno(rng)
			oldName := pickSharedOrUniqueName(rng, env, "rn_src")
			if hint, ok := env.PickEntryHint(rng); ok && rng.IntN(2) == 0 {
				oldParent = hint.ParentIno
				oldName = hint.Name
			}
			newName := pickSharedOrUniqueName(rng, env, "rn_dst")
			var flags uint32
			switch rng.IntN(3) {
			case 0:
				flags = 0
			case 1:
				flags = meta.RenameNoReplace
			case 2:
				flags = meta.RenameExchange
			}
			err := env.Mgr.Rename(ctx, meta.RenameParams{
				OldParentIno: oldParent,
				OldName:      []byte(oldName),
				NewParentIno: newParent,
				NewName:      []byte(newName),
				Flags:        flags,
			})
			assertConcurrentStepErr(t, env, workerID, step, "Rename", err)
			if err == nil {
				env.RecordWriteSuccess()
				env.AddEntryHint(newParent, newName)
			}
			recordTrace(step, fmt.Sprintf("Rename(old=%d/%s, new=%d/%s, flags=0x%x) -> err=%v",
				oldParent, oldName, newParent, newName, flags, err))

		case roll < 20:
			// 2. Mkdir 与 Rmdir 高频目录拓扑变更
			if rng.IntN(2) == 0 {
				parentIno := env.Disk1.RootIno
				if rng.IntN(3) == 0 {
					parentIno = env.PickDirIno(rng)
				}
				name := pickSharedOrUniqueName(rng, env, "dir")
				mode := uint32(0o755)
				if rng.IntN(3) == 0 {
					mode |= meta.S_ISGID
				}
				inode, err := env.Mgr.Mkdir(ctx, meta.MkdirParams{
					ParentIno: parentIno,
					Name:      []byte(name),
					Mode:      mode,
					UID:       uint32(1000 + workerID),
					GID:       uint32(2000 + workerID),
					Umask:     0o022,
				})
				assertConcurrentStepErr(t, env, workerID, step, "Mkdir", err)
				if err == nil && inode != nil {
					env.RecordWriteSuccess()
					env.AddDirIno(inode.Ino)
					env.AddEntryHint(parentIno, name)
				}
				recordTrace(step, fmt.Sprintf("Mkdir(parent=%d, name=%s) -> err=%v", parentIno, name, err))
			} else {
				parentIno := env.PickDirIno(rng)
				name := pickSharedOrUniqueName(rng, env, "dir")
				if hint, ok := env.PickEntryHint(rng); ok && rng.IntN(2) == 0 {
					parentIno = hint.ParentIno
					name = hint.Name
				}
				err := env.Mgr.Rmdir(ctx, parentIno, []byte(name))
				assertConcurrentStepErr(t, env, workerID, step, "Rmdir", err)
				if err == nil {
					env.RecordWriteSuccess()
				}
				recordTrace(step, fmt.Sprintf("Rmdir(parent=%d, name=%s) -> err=%v", parentIno, name, err))
			}

		case roll < 28:
			// 3. Mknod (五类文件类型) 与 Symlink
			parentIno := env.PickDirIno(rng)
			name := pickSharedOrUniqueName(rng, env, "node")
			if rng.IntN(2) == 0 {
				types := []uint32{meta.S_IFREG, meta.S_IFCHR, meta.S_IFBLK, meta.S_IFIFO, meta.S_IFSOCK}
				ft := types[rng.IntN(len(types))]
				inode, err := env.Mgr.Mknod(ctx, meta.MknodParams{
					ParentIno: parentIno,
					Name:      []byte(name),
					Mode:      ft | 0o644,
					UID:       uint32(1000 + workerID),
					GID:       uint32(1000 + workerID),
					Rdev:      uint32(rng.IntN(256) + 1),
					Umask:     0o022,
				})
				assertConcurrentStepErr(t, env, workerID, step, "Mknod", err)
				if err == nil && inode != nil {
					env.RecordWriteSuccess()
					if ft == meta.S_IFREG {
						env.AddFileIno(inode.Ino)
					}
					env.AddEntryHint(parentIno, name)
				}
				recordTrace(step, fmt.Sprintf("Mknod(parent=%d, name=%s, type=0o%o) -> err=%v", parentIno, name, ft, err))
			} else {
				target := fmt.Sprintf("/concurrent/target/%d/%d", workerID, step)
				inode, err := env.Mgr.Symlink(ctx, meta.SymlinkParams{
					ParentIno: parentIno,
					Name:      []byte(name),
					Target:    []byte(target),
					UID:       uint32(1000 + workerID),
					GID:       uint32(1000 + workerID),
				})
				assertConcurrentStepErr(t, env, workerID, step, "Symlink", err)
				if err == nil && inode != nil {
					env.RecordWriteSuccess()
					env.AddSymlinkIno(inode.Ino)
					env.AddEntryHint(parentIno, name)
				}
				recordTrace(step, fmt.Sprintf("Symlink(parent=%d, name=%s) -> err=%v", parentIno, name, err))
			}

		case roll < 38:
			// 4. Create 与 TmpFile (含绑定会话与无会话句柄，以及短命断连会话句柄)
			parentIno := env.PickDirIno(rng)
			var sidPtr *uint64
			useShortCrash := false
			switch rng.IntN(4) {
			case 0:
				// 无会话句柄 (session_id IS NULL)
				sidPtr = nil
			case 1:
				// 短命断连会话（通过 ShortMgr 创建，使其在压测中和收尾时超期被清扫；按种子隔离会话空间避免碰撞）
				crashSID := (env.Seed&0xfffff)*1_000_000 + 9_000_000_000 + env.NextSeq()
				env.RecordCrashSession(crashSID)
				sidPtr = &crashSID
				useShortCrash = true
			default:
				sidCopy := workerSID
				sidPtr = &sidCopy
			}

			if rng.IntN(2) == 0 {
				name := pickSharedOrUniqueName(rng, env, "cr")
				res, err := env.Mgr.Create(ctx, meta.CreateParams{
					ParentIno: parentIno,
					Name:      []byte(name),
					Mode:      meta.S_IFREG | meta.S_ISUID | 0o755,
					Flags:     meta.O_CREAT | meta.O_RDWR,
					UID:       uint32(1000 + workerID),
					GID:       uint32(1000 + workerID),
					SessionID: sidPtr,
				})
				assertConcurrentStepErr(t, env, workerID, step, "Create", err)
				if err == nil && res != nil {
					env.RecordWriteSuccess()
					env.AddFileIno(res.Inode.Ino)
					env.AddEntryHint(parentIno, name)
					if !useShortCrash {
						myFileHandles = append(myFileHandles, formalConcurrentHandleRec{
							Fh:        res.Fh,
							Ino:       res.Inode.Ino,
							SessionID: sidPtr,
							Flags:     meta.O_CREAT | meta.O_RDWR,
						})
					} else {
						_, _ = env.Pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() + interval '30 milliseconds' WHERE session_id = $1", int64(*sidPtr))
						if rng.IntN(2) == 0 {
							// 50% 概率立即 Unlink，构造仅绑定在短命断连会话上的孤儿文件
							uErr := env.Mgr.Unlink(ctx, parentIno, []byte(name))
							env.assertAllowedErr(t, workerID, step, "Unlink(ShortCrash)", uErr)
						}
					}
				}
				recordTrace(step, fmt.Sprintf("Create(parent=%d, name=%s) -> err=%v", parentIno, name, err))
			} else {
				flags := uint32(meta.O_RDWR)
				if rng.IntN(4) == 0 {
					flags |= meta.O_EXCL
				}
				res, err := env.Mgr.TmpFile(ctx, meta.TmpFileParams{
					ParentIno: parentIno,
					Mode:      0o640,
					Flags:     flags,
					UID:       uint32(1000 + workerID),
					GID:       uint32(1000 + workerID),
					SessionID: sidPtr,
				})
				assertConcurrentStepErr(t, env, workerID, step, "TmpFile", err)
				if err == nil && res != nil {
					env.RecordWriteSuccess()
					env.AddFileIno(res.Inode.Ino)
					if !useShortCrash {
						myFileHandles = append(myFileHandles, formalConcurrentHandleRec{
							Fh:        res.Fh,
							Ino:       res.Inode.Ino,
							SessionID: sidPtr,
							Flags:     flags | meta.O_TMPFILE,
						})
					} else {
						_, _ = env.Pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() + interval '30 milliseconds' WHERE session_id = $1", int64(*sidPtr))
					}
					// 立即尝试一次 Link 具名化（覆盖 TmpFile 一次性具名化与 O_TMPFILE 清除）
					if rng.IntN(2) == 0 {
						linkName := pickSharedOrUniqueName(rng, env, "tmp_mat")
						_, lErr := env.Mgr.Link(ctx, meta.LinkParams{
							Ino:          res.Inode.Ino,
							NewParentIno: parentIno,
							NewName:      []byte(linkName),
						})
						assertConcurrentStepErr(t, env, workerID, step, "Link(TmpFile)", lErr)
						if lErr == nil {
							env.RecordWriteSuccess()
							env.RecordMaterializedTmpIno(res.Inode.Ino)
							env.AddEntryHint(parentIno, linkName)
						}
					}
				}
				recordTrace(step, fmt.Sprintf("TmpFile(parent=%d, flags=0x%x) -> err=%v", parentIno, flags, err))
			}

		case roll < 48:
			// 5. Link 与 Unlink (含多硬链接并发增减、跨盘 EXDEV 与 unlink-whilst-open)
			if rng.IntN(2) == 0 {
				srcIno := env.PickFileIno(rng)
				newParent := env.PickDirIno(rng)
				newName := pickSharedOrUniqueName(rng, env, "hl")
				_, err := env.Mgr.Link(ctx, meta.LinkParams{
					Ino:          srcIno,
					NewParentIno: newParent,
					NewName:      []byte(newName),
				})
				assertConcurrentStepErr(t, env, workerID, step, "Link", err)
				if err == nil {
					env.RecordWriteSuccess()
					env.RecordMaterializedTmpIno(srcIno)
					env.AddEntryHint(newParent, newName)
				}
				recordTrace(step, fmt.Sprintf("Link(ino=%d, newParent=%d, name=%s) -> err=%v", srcIno, newParent, newName, err))
			} else {
				parentIno := env.PickDirIno(rng)
				name := pickSharedOrUniqueName(rng, env, "slot")
				if hint, ok := env.PickEntryHint(rng); ok && rng.IntN(2) == 0 {
					parentIno = hint.ParentIno
					name = hint.Name
				}
				err := env.Mgr.Unlink(ctx, parentIno, []byte(name))
				assertConcurrentStepErr(t, env, workerID, step, "Unlink", err)
				if err == nil {
					env.RecordWriteSuccess()
				}
				recordTrace(step, fmt.Sprintf("Unlink(parent=%d, name=%s) -> err=%v", parentIno, name, err))
			}

		case roll < 57:
			// 6. Open (含 O_TRUNC)、OpenDir、Release、ReleaseDir
			subOp := rng.IntN(4)
			switch subOp {
			case 0:
				targetIno := env.PickFileIno(rng)
				// 避免截断固定锚点文件 PinnedIno1 / PinnedIno2
				flags := uint32(meta.O_RDWR)
				if targetIno != env.PinnedIno1 && targetIno != env.PinnedIno2 && rng.IntN(2) == 0 {
					flags |= meta.O_TRUNC
				}
				res, err := env.Mgr.Open(ctx, meta.OpenParams{
					Ino:       targetIno,
					Flags:     flags,
					SessionID: nil,
				})
				assertConcurrentStepErr(t, env, workerID, step, "Open", err)
				if err == nil && res != nil {
					myFileHandles = append(myFileHandles, formalConcurrentHandleRec{
						Fh:        res.Fh,
						Ino:       targetIno,
						SessionID: nil,
						Flags:     flags,
					})
				}
				recordTrace(step, fmt.Sprintf("Open(ino=%d, flags=0x%x) -> err=%v", targetIno, flags, err))
			case 1:
				dirIno := env.PickDirIno(rng)
				sidCopy := workerSID
				res, err := env.Mgr.OpenDir(ctx, meta.OpenDirParams{
					Ino:       dirIno,
					Flags:     meta.O_RDONLY,
					SessionID: &sidCopy,
				})
				assertConcurrentStepErr(t, env, workerID, step, "OpenDir", err)
				if err == nil && res != nil {
					myDirHandles = append(myDirHandles, formalConcurrentHandleRec{
						Fh:        res.Fh,
						Ino:       dirIno,
						SessionID: &sidCopy,
						Flags:     meta.O_RDONLY,
						IsDir:     true,
					})
				}
				recordTrace(step, fmt.Sprintf("OpenDir(ino=%d) -> err=%v", dirIno, err))
			case 2:
				// Release 文件句柄（保留第 0 个锚点句柄供后续锁操作使用）
				if len(myFileHandles) > 1 {
					idx := 1 + rng.IntN(len(myFileHandles)-1)
					h := myFileHandles[idx]
					myFileHandles = append(myFileHandles[:idx], myFileHandles[idx+1:]...)
					relFlags := uint32(0)
					if rng.IntN(2) == 0 {
						relFlags |= meta.FuseReleaseFlockUnlock
					}
					if rng.IntN(2) == 0 {
						relFlags |= meta.FuseReleaseFlush
					}
					owner := owners[rng.IntN(len(owners))]
					err := env.Mgr.Release(ctx, meta.ReleaseParams{
						Ino:          h.Ino,
						Fh:           h.Fh,
						LockOwner:    &owner,
						ReleaseFlags: relFlags,
						SessionID:    h.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "Release", err)
					recordTrace(step, fmt.Sprintf("Release(ino=%d, fh=%d, flags=0x%x) -> err=%v", h.Ino, h.Fh, relFlags, err))
				}
			default:
				// ReleaseDir 目录句柄
				if len(myDirHandles) > 0 {
					idx := rng.IntN(len(myDirHandles))
					dh := myDirHandles[idx]
					myDirHandles = append(myDirHandles[:idx], myDirHandles[idx+1:]...)
					err := env.Mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
						Ino:       dh.Ino,
						Fh:        dh.Fh,
						SessionID: dh.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "ReleaseDir", err)
					recordTrace(step, fmt.Sprintf("ReleaseDir(ino=%d, fh=%d) -> err=%v", dh.Ino, dh.Fh, err))
				}
			}

		case roll < 67:
			// 7. SetAttr、Fallocate 与 UpdateDisk 配额并发争抢
			switch rng.IntN(3) {
			case 0:
				targetIno := env.PickFileIno(rng)
				if targetIno == env.PinnedIno1 || targetIno == env.PinnedIno2 {
					// 对锚点文件仅修改权限/时间戳，保持其 >= 4096 字节
					mode := uint32(meta.S_IFREG | 0o644)
					_, err := env.Mgr.SetAttr(ctx, targetIno, meta.SetAttrInput{
						Mode:     &mode,
						MtimeNow: true,
					})
					require.NoError(t, err)
					if err == nil {
						env.RecordWriteSuccess()
					}
					recordTrace(step, fmt.Sprintf("SetAttr(pinned ino=%d) -> err=%v", targetIno, err))
				} else {
					var newSize uint64
					if rng.IntN(6) == 0 {
						// 故意超配额触发 ErrNoSpace (ENOSPC)
						newSize = 64 * 1024 * 1024
					} else {
						newSize = uint64(rng.IntN(8192))
					}
					uid := uint32(1000 + workerID)
					_, err := env.Mgr.SetAttr(ctx, targetIno, meta.SetAttrInput{
						Size: &newSize,
						UID:  &uid,
					})
					assertConcurrentStepErr(t, env, workerID, step, "SetAttr(Size)", err)
					if err == nil {
						env.RecordWriteSuccess()
					}
					recordTrace(step, fmt.Sprintf("SetAttr(ino=%d, size=%d) -> err=%v", targetIno, newSize, err))
				}
			case 1:
				if len(myFileHandles) > 0 {
					h := myFileHandles[rng.IntN(len(myFileHandles))]
					modes := []uint32{0, meta.FallocFlKeepSize, meta.FallocFlPunchHole | meta.FallocFlKeepSize}
					fMode := modes[rng.IntN(len(modes))]
					offset := uint64(rng.IntN(4096))
					length := uint64(rng.IntN(4096) + 512)
					if h.Ino == env.PinnedIno1 && fMode == 0 {
						// 确保 PinnedIno1 大小只增不减 (>= 4096)
						offset = 4096
						length = uint64(rng.IntN(2048) + 1)
					}
					err := env.Mgr.Fallocate(ctx, meta.FallocateParams{
						Ino:       h.Ino,
						Fh:        h.Fh,
						Offset:    offset,
						Length:    length,
						Mode:      fMode,
						SessionID: h.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "Fallocate", err)
					if err == nil {
						env.RecordWriteSuccess()
					}
					recordTrace(step, fmt.Sprintf("Fallocate(ino=%d, fh=%d, mode=0x%x) -> err=%v", h.Ino, h.Fh, fMode, err))
				}
			default:
				// UpdateDisk：正常扩容/改名或故意缩容至 < 4096 触发 ErrInvalidShrink
				targetDisk := env.Disk1
				if rng.IntN(3) == 0 {
					targetDisk = env.Disk2
				}
				if rng.IntN(3) == 0 {
					// 缩容低于锚点文件 4096 字节 -> 必触发 ErrInvalidShrink (包装 ErrInvalidArgument)
					shrinkMax := uint64(512)
					_, err := env.Mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
						DiskID:   targetDisk.DiskID,
						MaxBytes: &shrinkMax,
					})
					require.ErrorIs(t, err, meta.ErrInvalidShrink, "确定性缩容拦截必须返回 ErrInvalidShrink")
					require.ErrorIs(t, err, meta.ErrInvalidArgument, "ErrInvalidShrink 必须包装 ErrInvalidArgument")
					recordTrace(step, fmt.Sprintf("UpdateDisk(shrink disk=%s) -> err=%v", targetDisk.DiskID, err))
				} else {
					newMax := uint64(8*1024*1024 + rng.IntN(4*1024*1024))
					newName := fmt.Sprintf("conc-disk-w%d-s%d", workerID, step)
					_, err := env.Mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
						DiskID:   targetDisk.DiskID,
						Name:     &newName,
						MaxBytes: &newMax,
					})
					require.NoError(t, err)
					if err == nil {
						env.RecordWriteSuccess()
					}
					recordTrace(step, fmt.Sprintf("UpdateDisk(disk=%s, max=%d) -> err=%v", targetDisk.DiskID, newMax, err))
				}
			}

		case roll < 84:
			// 8. 文件锁并发争抢：SetLk、SetLkw (带短超时)、GetLk、Flush(带 lock_owner)
			if len(myFileHandles) == 0 {
				continue
			}
			// 选取绑定了 workerSID 的有效句柄
			var targetH *formalConcurrentHandleRec
			for i := range myFileHandles {
				if myFileHandles[i].SessionID != nil && *myFileHandles[i].SessionID == workerSID {
					targetH = &myFileHandles[i]
					break
				}
			}
			if targetH == nil {
				continue
			}
			owner := owners[rng.IntN(len(owners))]
			lockTypes := []uint32{meta.F_RDLCK, meta.F_WRLCK, meta.F_UNLCK}
			lkType := lockTypes[rng.IntN(len(lockTypes))]
			var lkFlags uint32
			var start, end uint64
			if rng.IntN(3) == 0 {
				lkFlags = meta.FuseLkFlock
				start = 0
				end = meta.OffsetMax
			} else {
				lkFlags = 0
				start, end = generateRandomPosixRange(rng)
			}

			switch rng.IntN(4) {
			case 0:
				err := env.Mgr.SetLk(ctx, meta.SetLkParams{
					Ino:       targetH.Ino,
					Fh:        targetH.Fh,
					LockOwner: owner,
					Lk: meta.FileLock{
						Start: start,
						End:   end,
						Type:  lkType,
						PID:   uint32(2000 + workerID),
					},
					LkFlags:   lkFlags,
					SessionID: workerSID,
				})
				assertConcurrentStepErr(t, env, workerID, step, "SetLk", err)
				if err == nil {
					env.RecordWriteSuccess()
				}
				recordTrace(step, fmt.Sprintf("SetLk(ino=%d, flags=%d, type=%d, [%d,%d]) -> err=%v",
					targetH.Ino, lkFlags, lkType, start, end, err))
			case 1:
				// SetLkw 携带短超时 (6ms) 防止多协程死等，同时验证死锁检测 EDEADLK 与超时取消清理 EINTR
				reqID := env.NextRequestID()
				lkwCtx, cancel := context.WithTimeout(ctx, 6*time.Millisecond)
				err := env.Mgr.SetLkw(lkwCtx, meta.SetLkwParams{
					Ino:       targetH.Ino,
					Fh:        targetH.Fh,
					LockOwner: owner,
					Lk: meta.FileLock{
						Start: start,
						End:   end,
						Type:  lkType,
						PID:   uint32(2000 + workerID),
					},
					LkFlags:   lkFlags,
					SessionID: workerSID,
					RequestID: reqID,
				})
				cancel()
				assertConcurrentSetLkwStepErr(t, env, workerID, step, err)
				if errors.Is(err, meta.ErrInterrupted) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					env.RecordCancelledWaiter(workerSID, reqID)
				}
				recordTrace(step, fmt.Sprintf("SetLkw(ino=%d, req=%d, flags=%d, type=%d, [%d,%d]) -> err=%v",
					targetH.Ino, reqID, lkFlags, lkType, start, end, err))
			case 2:
				probeType := meta.F_WRLCK
				if rng.IntN(2) == 0 {
					probeType = meta.F_RDLCK
				}
				_, err := env.Mgr.GetLk(ctx, meta.GetLkParams{
					Ino:       targetH.Ino,
					Fh:        targetH.Fh,
					LockOwner: owner,
					Lk: meta.FileLock{
						Start: start,
						End:   end,
						Type:  probeType,
						PID:   uint32(2000 + workerID),
					},
					LkFlags:   lkFlags,
					SessionID: workerSID,
				})
				assertConcurrentStepErr(t, env, workerID, step, "GetLk", err)
				recordTrace(step, fmt.Sprintf("GetLk(ino=%d, flags=%d, [%d,%d]) -> err=%v", targetH.Ino, lkFlags, start, end, err))
			default:
				sidCopy := workerSID
				ownerCopy := owner
				err := env.Mgr.Flush(ctx, meta.FlushParams{
					Ino:       targetH.Ino,
					Fh:        targetH.Fh,
					LockOwner: &ownerCopy,
					SessionID: &sidCopy,
				})
				assertConcurrentStepErr(t, env, workerID, step, "Flush(LockOwner)", err)
				recordTrace(step, fmt.Sprintf("Flush(ino=%d, fh=%d, owner=%d) -> err=%v", targetH.Ino, targetH.Fh, owner, err))
			}

		case roll < 90:
			// 9. RenewSession / RenewSessionWithStatus 会话续期与并发超期孤儿清扫
			if rng.IntN(2) == 0 {
				exp, err := env.Mgr.RenewSession(ctx, workerSID)
				require.NoError(t, err)
				require.False(t, exp.IsZero())
				recordTrace(step, fmt.Sprintf("RenewSession(sid=%d) -> exp=%v", workerSID, exp))
			} else {
				res, err := env.Mgr.RenewSessionWithStatus(ctx, workerSID)
				require.NoError(t, err)
				require.False(t, res.ExpiresAt.IsZero())
				recordTrace(step, fmt.Sprintf("RenewSessionWithStatus(sid=%d) -> exp=%v", workerSID, res.ExpiresAt))
			}

		default:
			// 10. 并发只读探针 (Lookup, GetAttr, Readlink, ReadDir, ReadDirPlus, GetDisk, StatFS, Fsync, FsyncDir, 只读 Flush)
			switch rng.IntN(9) {
			case 0:
				dirIno := env.PickDirIno(rng)
				name := pickSharedOrUniqueName(rng, env, "slot")
				_, err := env.Mgr.Lookup(ctx, dirIno, []byte(name))
				assertConcurrentStepErr(t, env, workerID, step, "Lookup", err)
			case 1:
				ino := env.PickFileIno(rng)
				_, err := env.Mgr.GetAttr(ctx, ino)
				assertConcurrentStepErr(t, env, workerID, step, "GetAttr", err)
			case 2:
				if symIno, ok := env.PickSymlinkIno(rng); ok {
					_, err := env.Mgr.Readlink(ctx, symIno)
					assertConcurrentStepErr(t, env, workerID, step, "Readlink", err)
				}
			case 3:
				if len(myDirHandles) > 0 {
					dh := myDirHandles[rng.IntN(len(myDirHandles))]
					_, err := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
						Ino:       dh.Ino,
						Fh:        dh.Fh,
						Offset:    0,
						Limit:     64,
						SessionID: dh.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "ReadDir", err)
				}
			case 4:
				if len(myDirHandles) > 0 {
					dh := myDirHandles[rng.IntN(len(myDirHandles))]
					_, err := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
						Ino:       dh.Ino,
						Fh:        dh.Fh,
						Offset:    0,
						Limit:     64,
						SessionID: dh.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "ReadDirPlus", err)
				}
			case 5:
				disk, err := env.Mgr.GetDisk(ctx, env.Disk1.DiskID)
				require.NoError(t, err)
				require.Equal(t, env.Disk1.DiskID, disk.DiskID)
				require.Equal(t, env.Disk1.RootIno, disk.RootIno)
			case 6:
				stat, err := env.Mgr.StatFS(ctx, env.Disk1.RootIno)
				require.NoError(t, err)
				require.True(t, stat.Bsize > 0)
			case 7:
				if len(myFileHandles) > 0 {
					h := myFileHandles[rng.IntN(len(myFileHandles))]
					err := env.Mgr.Fsync(ctx, meta.FsyncParams{
						Ino:       h.Ino,
						Fh:        h.Fh,
						Datasync:  true,
						SessionID: h.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "Fsync", err)
				}
			default:
				if len(myDirHandles) > 0 {
					dh := myDirHandles[rng.IntN(len(myDirHandles))]
					err := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
						Ino:       dh.Ino,
						Fh:        dh.Fh,
						Datasync:  false,
						SessionID: dh.SessionID,
					})
					assertConcurrentStepErr(t, env, workerID, step, "FsyncDir", err)
				}
			}
			recordTrace(step, "ReadOnlyProbe -> ok")
		}
	}
}

// ============================================================================
// 停机收敛后 PostgreSQL 底表 10 大数学不变量全量 SQL 独立审计器
// ============================================================================

// auditConcurrentPhase1ActiveInvariants 在并发 Worker 刚停止、会话依然存活且未清扫时，
// 审计活跃态下的 INV-1、INV-2、INV-4、INV-5、INV-6 以及 LOCK-INV-1、LOCK-INV-2、LOCK-INV-3、LOCK-INV-4(a)（已取消等待者零残留）。
func auditConcurrentPhase1ActiveInvariants(
	ctx context.Context,
	t *testing.T,
	env *formalConcurrentSharedEnv,
	activeRootInos []uint64,
) {
	t.Helper()
	auditConcurrentInvariantsInternal(ctx, t, env, activeRootInos, nil, true)
}

// auditConcurrentAll10Invariants 在并发 Worker 停止且完成短租约过期与 RenewSession 同步清扫后，
// 直接基于 PostgreSQL 底表自身满足的数学恒等式对全部 10 条不变量执行全量 SQL 独立审计：
// - INV-1: 目录树无环与父子双向一致
// - INV-2: 硬链接计数守恒（非目录 nlink == count(entries)，存活目录 nlink == 2 + 子目录数，孤儿目录无子项）
// - INV-3: 孤儿节点生命周期与清扫收敛（无有效句柄的 nlink==0 孤儿数恒为 0；存活 nlink==0 孤儿必关联至少 1 条有效句柄）
// - INV-4: 磁盘容量与节点配额账本守恒（used_bytes == sum(S_IFREG size)，used_inodes == count(inodes)，0 <= used_bytes <= max_bytes）
// - INV-5: 跨盘拓扑隔离（entries 中 parent.root_ino == child.root_ino）
// - INV-6: 匿名临时文件一次性具名化（任何 nlink > 0 节点的句柄中 (flags & O_TMPFILE) == 0）
// - LOCK-INV-1: POSIX 字节区间规整与互斥（跨拥有者重叠必同为 F_RDLCK，同拥有者不重叠且无紧邻同类型碎片）
// - LOCK-INV-2: BSD flock 与 POSIX 双命名空间正交（flock 必为 [0, OffsetMax]，同 (session_id, ino, fh) 至多 1 条，跨句柄共存必同为 F_RDLCK）
// - LOCK-INV-3: 底表 file_locks 与 file_lock_waiters 构成的局部等待有向图恒无环
// - LOCK-INV-4: 所有已取消请求与已过期清理会话在 file_lock_waiters 和 file_locks 中零残留
func auditConcurrentAll10Invariants(
	ctx context.Context,
	t *testing.T,
	env *formalConcurrentSharedEnv,
	activeRootInos []uint64,
	purgedSessionIDs []uint64,
) {
	t.Helper()
	auditConcurrentInvariantsInternal(ctx, t, env, activeRootInos, purgedSessionIDs, false)
}

func auditConcurrentInvariantsInternal(
	ctx context.Context,
	t *testing.T,
	env *formalConcurrentSharedEnv,
	activeRootInos []uint64,
	purgedSessionIDs []uint64,
	isPhase1Active bool,
) {
	t.Helper()
	defer func() {
		if t.Failed() {
			t.Log(env.FormatDiagnosticReport())
		}
	}()
	require.NotEmpty(t, activeRootInos)

	rootInoInt64s := make([]int64, len(activeRootInos))
	activeRootSet := make(map[uint64]bool, len(activeRootInos))
	for i, r := range activeRootInos {
		rootInoInt64s[i] = int64(r)
		activeRootSet[r] = true
	}

	// -------------------------------------------------------------------------
	// 1. 加载指定 root_ino 集合下的全部 inodes 与 entries 快照
	// -------------------------------------------------------------------------
	inodeRows, err := env.Pool.Query(ctx, `
		SELECT ino, mode, size, nlink, uid, gid, rdev, flags, parent_ino, root_ino
		FROM inodes
		WHERE root_ino = ANY($1::bigint[])
	`, rootInoInt64s)
	require.NoError(t, err, "并发总账审计: 查询底表 inodes 失败")
	defer inodeRows.Close()

	inodesByIno := make(map[uint64]dbAuditInodeRow)
	for inodeRows.Next() {
		var ino, size, rdev, parentIno, rootIno int64
		var mode, nlink, uid, gid, flags int32
		require.NoError(t, inodeRows.Scan(&ino, &mode, &size, &nlink, &uid, &gid, &rdev, &flags, &parentIno, &rootIno))
		inodesByIno[uint64(ino)] = dbAuditInodeRow{
			Ino:       uint64(ino),
			Mode:      uint32(mode),
			Size:      size,
			Nlink:     nlink,
			UID:       uint32(uid),
			GID:       uint32(gid),
			Rdev:      rdev,
			Flags:     uint32(flags),
			ParentIno: uint64(parentIno),
			RootIno:   uint64(rootIno),
		}
	}
	require.NoError(t, inodeRows.Err())

	entryRows, err := env.Pool.Query(ctx, `
		SELECT e.parent_ino, e.name, e.ino, p.root_ino, p.mode, p.nlink, c.root_ino, c.mode, c.nlink
		FROM entries e
		JOIN inodes p ON p.ino = e.parent_ino
		JOIN inodes c ON c.ino = e.ino
		WHERE p.root_ino = ANY($1::bigint[]) OR c.root_ino = ANY($1::bigint[])
	`, rootInoInt64s)
	require.NoError(t, err, "并发总账审计: 查询底表 entries 失败")
	defer entryRows.Close()

	incomingEntriesByChild := make(map[uint64][]dbAuditEntryRow)
	totalChildrenByParent := make(map[uint64]int32)
	subDirChildrenByParent := make(map[uint64]int32)

	for entryRows.Next() {
		var pIno, cIno, pRoot, cRoot int64
		var pMode, pNlink, cMode, cNlink int32
		var nameBytes []byte
		require.NoError(t, entryRows.Scan(&pIno, &nameBytes, &cIno, &pRoot, &pMode, &pNlink, &cRoot, &cMode, &cNlink))

		nameStr := string(nameBytes)
		// INV-5: entries 表中任意 (parent_ino, child_ino) 恒满足 parent.root_ino == child.root_ino
		require.Equalf(t, pRoot, cRoot,
			"INV-5 违例: 目录项 (%d, %q -> %d) 跨盘绑定: parent.root_ino=%d != child.root_ino=%d",
			pIno, nameStr, cIno, pRoot, cRoot)
		require.Equalf(t, uint32(meta.S_IFDIR), uint32(pMode)&meta.S_IFMT,
			"INV-5 违例: 目录项 (%d, %q) 的父节点不是目录", pIno, nameStr)
		require.Greaterf(t, pNlink, int32(0),
			"INV-5 违例: 目录项 (%d, %q) 的父目录处于 nlink=0 孤儿态", pIno, nameStr)
		require.Greaterf(t, cNlink, int32(0),
			"INV-5 违例: 目录项 (%d, %q -> %d) 的子节点处于 nlink<=0 状态 (%d)", pIno, nameStr, cIno, cNlink)

		uParent := uint64(pIno)
		uChild := uint64(cIno)
		rec := dbAuditEntryRow{ParentIno: uParent, Name: nameStr, ChildIno: uChild}
		incomingEntriesByChild[uChild] = append(incomingEntriesByChild[uChild], rec)
		totalChildrenByParent[uParent]++
		if (uint32(cMode) & meta.S_IFMT) == meta.S_IFDIR {
			subDirChildrenByParent[uParent]++
		}
	}
	require.NoError(t, entryRows.Err())

	// -------------------------------------------------------------------------
	// 2. 审计 INV-1（目录树无环与父子双向一致）与 INV-2（目录与非目录 nlink 守恒）
	// -------------------------------------------------------------------------
	for _, rootIno := range activeRootInos {
		rootRow, exists := inodesByIno[rootIno]
		require.Truef(t, exists, "INV-1 违例: 存活磁盘根目录 root_ino=%d 在底表 inodes 中缺失", rootIno)
		require.Truef(t, rootRow.IsDir(), "INV-1 违例: root_ino=%d 不是目录", rootIno)
		require.Equalf(t, rootIno, rootRow.ParentIno, "INV-1 违例: 根目录 root_ino=%d 的 parent_ino (%d) != root_ino", rootIno, rootRow.ParentIno)
		require.Emptyf(t, incomingEntriesByChild[rootIno], "INV-1 违例: 根目录 root_ino=%d 在 entries 中存在父记录", rootIno)
	}

	maxWalkSteps := len(inodesByIno) + 1
	for ino, row := range inodesByIno {
		if row.IsDir() {
			if row.Nlink == 0 {
				// INV-1 & INV-2: 孤儿目录无父 entries 且名下无任何子 entries
				require.Emptyf(t, incomingEntriesByChild[ino],
					"INV-1 违例: 孤儿目录 ino=%d (nlink=0) 在 entries 中仍存在父记录", ino)
				require.Zerof(t, totalChildrenByParent[ino],
					"INV-2 违例: 孤儿目录 ino=%d (nlink=0) 名下仍残留 %d 条子目录项", ino, totalChildrenByParent[ino])
				continue
			}

			// INV-2: 存活目录 inodes.nlink == 2 + count(子目录 entries)
			expectedDirNlink := int32(2) + subDirChildrenByParent[ino]
			require.Equalf(t, expectedDirNlink, row.Nlink,
				"INV-2 违例: 存活目录 ino=%d 的 nlink (%d) != 2 + 子目录数 (%d)",
				ino, row.Nlink, subDirChildrenByParent[ino])

			if ino != row.RootIno {
				// INV-1: 非根存活目录恰有唯一父记录且 entries.parent_ino == inodes.parent_ino，沿父链回溯必唯一到达 root_ino 且无环
				inc := incomingEntriesByChild[ino]
				require.Lenf(t, inc, 1,
					"INV-1 违例: 非根存活目录 ino=%d 在 entries 中的父记录数 (%d) != 1", ino, len(inc))
				require.Equalf(t, row.ParentIno, inc[0].ParentIno,
					"INV-1 违例: 非根存活目录 ino=%d 的 inodes.parent_ino (%d) != entries.parent_ino (%d)",
					ino, row.ParentIno, inc[0].ParentIno)

				curr := ino
				visited := make(map[uint64]bool)
				steps := 0
				for curr != row.RootIno {
					require.Falsef(t, visited[curr], "INV-1 违例: 目录树从 ino=%d 回溯在 %d 处发现环路", ino, curr)
					visited[curr] = true
					steps++
					require.LessOrEqualf(t, steps, maxWalkSteps, "INV-1 违例: 目录 ino=%d 回溯步数超限", ino)

					currRow, ok := inodesByIno[curr]
					require.Truef(t, ok && currRow.IsDir() && currRow.Nlink > 0,
						"INV-1 违例: 回溯节点 ino=%d 状态非法", curr)
					require.Equalf(t, row.RootIno, currRow.RootIno,
						"INV-1 违例: 回溯节点 ino=%d 的 root_ino (%d) != 起点 root_ino (%d)", curr, currRow.RootIno, row.RootIno)
					pInc := incomingEntriesByChild[curr]
					require.Lenf(t, pInc, 1, "INV-1 违例: 回溯节点 ino=%d 父记录不唯一", curr)
					require.Equalf(t, currRow.ParentIno, pInc[0].ParentIno,
						"INV-1 违例: 回溯节点 ino=%d parent_ino 不一致", curr)
					curr = pInc[0].ParentIno
				}
			}
		} else {
			// 非目录节点：INV-1 parent_ino == 0；INV-2 inodes.nlink == count(entries)
			require.Zerof(t, row.ParentIno,
				"INV-1/INV-2 违例: 非目录节点 ino=%d 的 parent_ino (%d) 必须为 0", ino, row.ParentIno)
			actualLinks := int32(len(incomingEntriesByChild[ino]))
			require.Equalf(t, actualLinks, row.Nlink,
				"INV-2 违例: 非目录节点 ino=%d 的 inodes.nlink (%d) != count(entries) (%d)",
				ino, row.Nlink, actualLinks)
		}
	}

	// -------------------------------------------------------------------------
	// 3. 审计 INV-3（孤儿节点生命周期与清扫收敛）与 INV-6（TmpFile 一次性具名化）
	// -------------------------------------------------------------------------
	if isPhase1Active {
		// 阶段 1（活跃未清扫态）：任意 nlink == 0 的孤儿节点在 file_handles 中必存在至少 1 条关联句柄（允许包含短命 crashSID 已到期但尚未经阶段 2 同步清扫的惰性句柄）
		for ino, row := range inodesByIno {
			if row.Nlink == 0 {
				var totalFhCount int64
				err := env.Pool.QueryRow(ctx, `
					SELECT COUNT(*)
					FROM file_handles fh
					WHERE fh.ino = $1
				`, int64(ino)).Scan(&totalFhCount)
				require.NoError(t, err)
				require.GreaterOrEqualf(t, totalFhCount, int64(1),
					"INV-3 违例(阶段1活跃态): 孤儿节点 ino=%d (nlink=0) 在底表 file_handles 中无任何关联句柄", ino)
			}
		}
	} else {
		var unreferencedOrphanCount int64
		err = env.Pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM inodes i
			WHERE i.root_ino = ANY($1::bigint[])
			  AND i.nlink = 0
			  AND NOT EXISTS (
			      SELECT 1
			      FROM file_handles fh
			      LEFT JOIN client_sessions s ON fh.session_id = s.session_id
			      WHERE fh.ino = i.ino
			        AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
			  )
		`, rootInoInt64s).Scan(&unreferencedOrphanCount)
		require.NoError(t, err, "INV-3 审计: 查询无有效句柄的孤儿节点失败")
		require.Zerof(t, unreferencedOrphanCount,
			"INV-3 违例: RenewSession 同步清扫后，底表中仍残留 %d 个无有效句柄引用的 nlink=0 孤儿节点", unreferencedOrphanCount)

		// 验证同步清扫后任意仍存活的 nlink == 0 孤儿节点必关联至少 1 条未过期有效 FileHandle
		for ino, row := range inodesByIno {
			if row.Nlink == 0 {
				var validFhCount int64
				err := env.Pool.QueryRow(ctx, `
					SELECT COUNT(*)
					FROM file_handles fh
					LEFT JOIN client_sessions s ON fh.session_id = s.session_id
					WHERE fh.ino = $1
					  AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
				`, int64(ino)).Scan(&validFhCount)
				require.NoError(t, err)
				require.GreaterOrEqualf(t, validFhCount, int64(1),
					"INV-3 违例: 存活孤儿节点 ino=%d (nlink=0) 未关联任何有效 FileHandle", ino)
			}
		}
	}

	// INV-6: 任意 nlink > 0 的节点关联的所有 file_handles 中 (flags & O_TMPFILE) == 0；
	// 任意携带 O_TMPFILE 的句柄必关联 S_IFREG 且 nlink == 0 且 parent_ino == 0；
	// 且任何曾通过 Link 成功具名化过的节点（哪怕后续被 Unlink 解链回 nlink == 0），其关联句柄中的 O_TMPFILE 位也必须已被永久清零！
	var badNamedTmpHandleCount int64
	err = env.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM file_handles fh
		JOIN inodes i ON i.ino = fh.ino
		WHERE i.root_ino = ANY($1::bigint[])
		  AND (fh.flags & $2) != 0
		  AND (i.nlink > 0 OR (i.mode & $3) != $4 OR i.parent_ino != 0)
	`, rootInoInt64s, int32(meta.O_TMPFILE), int32(meta.S_IFMT), int32(meta.S_IFREG)).Scan(&badNamedTmpHandleCount)
	require.NoError(t, err, "INV-6 审计: 查询 O_TMPFILE 违例句柄失败")
	require.Zerof(t, badNamedTmpHandleCount,
		"INV-6 违例: 存在 %d 条 nlink>0 或非 S_IFREG 节点的句柄仍残留 O_TMPFILE 标志位", badNamedTmpHandleCount)

	if len(env.materializedTmpInos) > 0 {
		matInoInt64s := make([]int64, len(env.materializedTmpInos))
		for i, ino := range env.materializedTmpInos {
			matInoInt64s[i] = int64(ino)
		}
		var badHistoricalTmpHandleCount int64
		err = env.Pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM file_handles fh
			WHERE fh.ino = ANY($1::bigint[])
			  AND (fh.flags & $2) != 0
		`, matInoInt64s, int32(meta.O_TMPFILE)).Scan(&badHistoricalTmpHandleCount)
		require.NoError(t, err, "INV-6 审计: 查询曾具名化节点的 O_TMPFILE 残留失败")
		require.Zerof(t, badHistoricalTmpHandleCount,
			"INV-6 违例: 存在 %d 条曾具名化后解链节点的句柄仍残留 O_TMPFILE 标志位", badHistoricalTmpHandleCount)
	}

	// -------------------------------------------------------------------------
	// 4. 审计 INV-4（磁盘容量与节点配额账本守恒）
	// -------------------------------------------------------------------------
	for _, rootIno := range activeRootInos {
		var diskID, diskName string
		var maxBytes int64
		err := env.Pool.QueryRow(ctx, `
			SELECT disk_id::text, name, max_bytes
			FROM disks
			WHERE root_ino = $1
		`, int64(rootIno)).Scan(&diskID, &diskName, &maxBytes)
		require.NoErrorf(t, err, "INV-4 审计: 查询存活磁盘 root_ino=%d 失败", rootIno)

		var actualInodeCount, actualRegBytes int64
		err = env.Pool.QueryRow(ctx, `
			SELECT
				COUNT(*)::bigint,
				COALESCE(SUM(CASE WHEN (mode & $2) = $3 THEN size ELSE 0 END), 0)::bigint
			FROM inodes
			WHERE root_ino = $1
		`, int64(rootIno), int32(meta.S_IFMT), int32(meta.S_IFREG)).Scan(&actualInodeCount, &actualRegBytes)
		require.NoError(t, err, "INV-4 审计: 聚合 inodes 配额失败")

		require.GreaterOrEqualf(t, actualRegBytes, int64(0), "INV-4 违例: root_ino=%d used_bytes < 0", rootIno)
		require.LessOrEqualf(t, actualRegBytes, maxBytes, "INV-4 违例: root_ino=%d used_bytes (%d) > max_bytes (%d)", rootIno, actualRegBytes, maxBytes)
		require.GreaterOrEqualf(t, actualInodeCount, int64(1), "INV-4 违例: root_ino=%d used_inodes (%d) < 1", rootIno, actualInodeCount)

		// 同时核对公开接口 GetDisk 与 StatFS 与底表账本完全一致
		gotDisk, err := env.Mgr.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, uint64(actualRegBytes), gotDisk.UsedBytes)
		require.Equal(t, uint64(actualInodeCount), gotDisk.UsedInodes)

		gotStatFS, err := env.Mgr.StatFS(ctx, rootIno)
		require.NoError(t, err)
		require.Equal(t, uint64(actualRegBytes), gotStatFS.UsedBytes)
		require.Equal(t, uint64(actualInodeCount), gotStatFS.Files)
	}

	// -------------------------------------------------------------------------
	// 5. 审计 LOCK-INV-1（POSIX 区间锁规整与互斥）与 LOCK-INV-2（BSD flock 规整与双命名空间正交）
	// -------------------------------------------------------------------------
	type dbLockAuditRow struct {
		ID        int64
		SessionID uint64
		Ino       uint64
		Fh        uint64
		LockOwner uint64
		LkFlags   uint32
		Start     uint64
		End       uint64
		Type      uint32
		PID       uint32
	}

	lockRows, err := env.Pool.Query(ctx, `
		SELECT l.id, l.session_id, l.ino, l.fh, l.lock_owner, l.lk_flags, l.start_offset, l.end_offset, l.lock_type, l.pid
		FROM file_locks l
		JOIN inodes i ON i.ino = l.ino
		JOIN client_sessions s ON s.session_id = l.session_id
		WHERE i.root_ino = ANY($1::bigint[])
		  AND s.expires_at > clock_timestamp()
		ORDER BY l.ino ASC, l.lk_flags ASC, l.start_offset ASC, l.end_offset ASC, l.id ASC
	`, rootInoInt64s)
	require.NoError(t, err, "并发锁审计: 查询 file_locks 失败")
	defer lockRows.Close()

	posixByIno := make(map[uint64][]dbLockAuditRow)
	flockByIno := make(map[uint64][]dbLockAuditRow)

	for lockRows.Next() {
		var id, sid, ino, fh, owner, startOff, endOff int64
		var lkFlags, pid int32
		var lockType int16
		require.NoError(t, lockRows.Scan(&id, &sid, &ino, &fh, &owner, &lkFlags, &startOff, &endOff, &lockType, &pid))
		require.GreaterOrEqualf(t, startOff, int64(0), "LOCK-INV-1 违例: start_offset < 0")
		require.GreaterOrEqualf(t, endOff, startOff, "LOCK-INV-1 违例: end_offset < start_offset")

		rec := dbLockAuditRow{
			ID:        id,
			SessionID: uint64(sid),
			Ino:       uint64(ino),
			Fh:        uint64(fh),
			LockOwner: uint64(owner),
			LkFlags:   uint32(lkFlags),
			Start:     uint64(startOff),
			End:       uint64(endOff),
			Type:      uint32(lockType),
			PID:       uint32(pid),
		}
		if rec.LkFlags == meta.FuseLkFlock {
			flockByIno[rec.Ino] = append(flockByIno[rec.Ino], rec)
		} else {
			require.Zerof(t, rec.LkFlags, "LOCK-INV-1 违例: 非法 lk_flags=%d", rec.LkFlags)
			posixByIno[rec.Ino] = append(posixByIno[rec.Ino], rec)
		}
	}
	require.NoError(t, lockRows.Err())

	// 审计 LOCK-INV-1
	for ino, locks := range posixByIno {
		// (a) 不同 (session_id, lock_owner) 的相交活跃锁必同为 F_RDLCK
		for i := 0; i < len(locks); i++ {
			for j := i + 1; j < len(locks); j++ {
				a, b := locks[i], locks[j]
				if a.SessionID == b.SessionID && a.LockOwner == b.LockOwner {
					continue
				}
				if lockOracleRangesOverlap(a.Start, a.End, b.Start, b.End) {
					require.Truef(t, a.Type == meta.F_RDLCK && b.Type == meta.F_RDLCK,
						"LOCK-INV-1 违例: ino=%d 上不同拥有者 (%d,%d)[%d,%d](type=%d) 与 (%d,%d)[%d,%d](type=%d) 区间重叠但非全读锁",
						ino, a.SessionID, a.LockOwner, a.Start, a.End, a.Type,
						b.SessionID, b.LockOwner, b.Start, b.End, b.Type)
				}
			}
		}

		// (b) 同一 (session_id, ino, lock_owner) 下区间互不相交且无紧邻同类型碎片
		type posixOwnerKey struct {
			SessionID uint64
			LockOwner uint64
		}
		byOwner := make(map[posixOwnerKey][]dbLockAuditRow)
		for _, l := range locks {
			k := posixOwnerKey{SessionID: l.SessionID, LockOwner: l.LockOwner}
			byOwner[k] = append(byOwner[k], l)
		}
		for k, list := range byOwner {
			sort.Slice(list, func(i, j int) bool {
				if list[i].Start != list[j].Start {
					return list[i].Start < list[j].Start
				}
				return list[i].End < list[j].End
			})
			for i := 1; i < len(list); i++ {
				prev, next := list[i-1], list[i]
				require.Lessf(t, prev.End, next.Start,
					"LOCK-INV-1 违例: ino=%d 同拥有者 (%d,%d) 存在相交锁 [%d,%d] 与 [%d,%d]",
					ino, k.SessionID, k.LockOwner, prev.Start, prev.End, next.Start, next.End)
				require.Falsef(t, prev.End < meta.OffsetMax && prev.End+1 == next.Start && prev.Type == next.Type,
					"LOCK-INV-1 违例: ino=%d 同拥有者 (%d,%d) 存在紧邻同类型未合并碎片 [%d,%d] 与 [%d,%d]",
					ino, k.SessionID, k.LockOwner, prev.Start, prev.End, next.Start, next.End)
			}
		}
	}

	// 审计 LOCK-INV-2
	for ino, flocks := range flockByIno {
		type flockOwnerKey struct {
			SessionID uint64
			Fh        uint64
		}
		seenOwnerKeys := make(map[flockOwnerKey]bool)
		for _, fl := range flocks {
			require.Equalf(t, uint64(0), fl.Start,
				"LOCK-INV-2 违例: ino=%d BSD flock start_offset (%d) != 0", ino, fl.Start)
			require.Equalf(t, meta.OffsetMax, fl.End,
				"LOCK-INV-2 违例: ino=%d BSD flock end_offset (%d) != OffsetMax", ino, fl.End)

			ok := flockOwnerKey{SessionID: fl.SessionID, Fh: fl.Fh}
			require.Falsef(t, seenOwnerKeys[ok],
				"LOCK-INV-2 违例: ino=%d 同一 (session_id=%d, fh=%d) 存在多条 BSD flock 记录",
				ino, fl.SessionID, fl.Fh)
			seenOwnerKeys[ok] = true
		}
		if len(flocks) > 1 {
			for _, fl := range flocks {
				require.Equalf(t, meta.F_RDLCK, fl.Type,
					"LOCK-INV-2 违例: ino=%d 存在多个共存 BSD flock 持有者，但 (session_id=%d, fh=%d) 类型为 %d (非 F_RDLCK)",
					ino, fl.SessionID, fl.Fh, fl.Type)
			}
		}
	}

	// LOCK-INV-2 & LOCK-INV-4(a) 悬挂锁核验:
	// 若 PinnedIno2 仍在 activeRootInos 集合中，验证 keeperSID 在 PinnedIno2 上的 POSIX 裂解三段锁 ([0,29]W, [30,60]R, [61,100]W) 与 BSD flock 读锁完整存续，
	// 且已被 Flush(LockOwner=9999) 清除的 POSIX 锁和已被超时取消的 LockOwner=7701 阻塞锁请求绝未在 file_locks 中留下任何悬挂锁记录！
	if activeRootSet[env.Disk2.RootIno] {
		require.Lenf(t, posixByIno[env.PinnedIno2], 3,
			"LOCK-INV-1/2/4 违例: PinnedIno2 上的 POSIX 锁段数应恰为 3 段（无 LockOwner=9999 残留或 LockOwner=7701 取消悬挂锁），实际=%+v",
			posixByIno[env.PinnedIno2])
		for _, pl := range posixByIno[env.PinnedIno2] {
			require.Equalf(t, uint64(8801), pl.LockOwner,
				"LOCK-INV-2/4 违例: PinnedIno2 上存在非预期 LockOwner=%d 的悬挂 POSIX 锁", pl.LockOwner)
		}
		require.Lenf(t, flockByIno[env.PinnedIno2], 1,
			"LOCK-INV-2 违例: Flush(LockOwner) 后 PinnedIno2 上的 BSD flock 读锁必须正交保留 1 条，实际=%+v",
			flockByIno[env.PinnedIno2])
	}

	// -------------------------------------------------------------------------
	// 6. 审计 LOCK-INV-3（底表局部等待有向图恒无环）与 LOCK-INV-4（取消请求与过期会话零残留）
	// -------------------------------------------------------------------------
	type dbWaiterAuditRow struct {
		SessionID uint64
		RequestID uint64
		Ino       uint64
		Fh        uint64
		LockOwner uint64
		LkFlags   uint32
		Start     uint64
		End       uint64
		Type      uint32
		Status    int16
	}

	waiterRows, err := env.Pool.Query(ctx, `
		SELECT w.session_id, w.request_id, w.ino, w.fh, w.lock_owner, w.lk_flags, w.start_offset, w.end_offset, w.lock_type, w.status
		FROM file_lock_waiters w
		JOIN inodes i ON i.ino = w.ino
		JOIN client_sessions s ON s.session_id = w.session_id
		WHERE i.root_ino = ANY($1::bigint[])
		  AND s.expires_at > clock_timestamp()
		  AND w.status = 1
	`, rootInoInt64s)
	require.NoError(t, err, "LOCK-INV-3 审计: 查询 file_lock_waiters 失败")
	defer waiterRows.Close()

	type lockNSKey struct {
		Ino     uint64
		LkFlags uint32
	}
	waitersByNS := make(map[lockNSKey][]dbWaiterAuditRow)
	for waiterRows.Next() {
		var sid, reqID, ino, fh, owner, startOff, endOff int64
		var lkFlags int32
		var lockType, status int16
		require.NoError(t, waiterRows.Scan(&sid, &reqID, &ino, &fh, &owner, &lkFlags, &startOff, &endOff, &lockType, &status))
		ns := lockNSKey{Ino: uint64(ino), LkFlags: uint32(lkFlags)}
		waitersByNS[ns] = append(waitersByNS[ns], dbWaiterAuditRow{
			SessionID: uint64(sid),
			RequestID: uint64(reqID),
			Ino:       uint64(ino),
			Fh:        uint64(fh),
			LockOwner: uint64(owner),
			LkFlags:   uint32(lkFlags),
			Start:     uint64(startOff),
			End:       uint64(endOff),
			Type:      uint32(lockType),
			Status:    status,
		})
	}
	require.NoError(t, waiterRows.Err())

	// LOCK-INV-3: 对每个存在等待者的 (ino, lk_flags) 构建局部等待有向图并证明无环
	for ns, waiters := range waitersByNS {
		var activeEdgeInputs []formalWaitGraphLockEdgeInput
		var waiterEdgeInputs []formalWaitGraphLockEdgeInput
		if ns.LkFlags == meta.FuseLkFlock {
			for _, l := range flockByIno[ns.Ino] {
				activeEdgeInputs = append(activeEdgeInputs, formalWaitGraphLockEdgeInput{
					OwnerKey: formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh},
					Start:    0,
					End:      meta.OffsetMax,
					Type:     l.Type,
				})
			}
			for _, w := range waiters {
				waiterEdgeInputs = append(waiterEdgeInputs, formalWaitGraphLockEdgeInput{
					OwnerKey: formalLockOwnerKey{SessionID: w.SessionID, ID: w.Fh},
					Start:    w.Start,
					End:      w.End,
					Type:     w.Type,
				})
			}
		} else {
			for _, l := range posixByIno[ns.Ino] {
				activeEdgeInputs = append(activeEdgeInputs, formalWaitGraphLockEdgeInput{
					OwnerKey: formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner},
					Start:    l.Start,
					End:      l.End,
					Type:     l.Type,
				})
			}
			for _, w := range waiters {
				waiterEdgeInputs = append(waiterEdgeInputs, formalWaitGraphLockEdgeInput{
					OwnerKey: formalLockOwnerKey{SessionID: w.SessionID, ID: w.LockOwner},
					Start:    w.Start,
					End:      w.End,
					Type:     w.Type,
				})
			}
		}
		assertLockWaitGraphNoCycle(t, ns.Ino, ns.LkFlags, activeEdgeInputs, waiterEdgeInputs, env.FormatDiagnosticReport())
	}

	// LOCK-INV-4(a): 所有已取消/超时的 SetLkw 请求在 file_lock_waiters 中零残留
	for _, ck := range env.cancelledWaiters {
		var cnt int64
		err := env.Pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM file_lock_waiters
			WHERE session_id = $1 AND request_id = $2
		`, int64(ck.SessionID), int64(ck.RequestID)).Scan(&cnt)
		require.NoError(t, err)
		require.Zerof(t, cnt,
			"LOCK-INV-4 违例: 已取消的阻塞锁请求 (session_id=%d, request_id=%d) 在 file_lock_waiters 中仍残留 %d 条记录",
			ck.SessionID, ck.RequestID, cnt)
	}

	// LOCK-INV-4(b): 所有已超期并经 RenewSession 清扫的会话在 client_sessions、file_handles、file_locks、file_lock_waiters 中零残留
	if len(purgedSessionIDs) > 0 {
		purgedInt64s := make([]int64, len(purgedSessionIDs))
		for i, sid := range purgedSessionIDs {
			purgedInt64s[i] = int64(sid)
		}
		var sessCnt, handleCnt, lockCnt, waiterCnt int64
		err := env.Pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM client_sessions WHERE session_id = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_handles WHERE session_id = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_locks WHERE session_id = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = ANY($1::bigint[]))
		`, purgedInt64s).Scan(&sessCnt, &handleCnt, &lockCnt, &waiterCnt)
		require.NoError(t, err)
		require.Zerof(t, sessCnt, "LOCK-INV-4 违例: 已过期清理会话在 client_sessions 残留 %d 条", sessCnt)
		require.Zerof(t, handleCnt, "LOCK-INV-4 违例: 已过期清理会话在 file_handles 残留 %d 条", handleCnt)
		require.Zerof(t, lockCnt, "LOCK-INV-4 违例: 已过期清理会话在 file_locks 残留 %d 条", lockCnt)
		require.Zerof(t, waiterCnt, "LOCK-INV-4 违例: 已过期清理会话在 file_lock_waiters 残留 %d 条", waiterCnt)
	}
}

// assertDeletedDiskZeroResidue 审计执行 DeleteDisk 后归属该 root_ino 的全部底表记录数归零 (INV-4)。
// 直接基于删盘前记录的 deletedInos 执行无 INNER JOIN 假阳性的直接扫描校验。
func assertDeletedDiskZeroResidue(ctx context.Context, t *testing.T, pool *pgxpool.Pool, deletedRootIno uint64, deletedInos []int64) {
	t.Helper()
	var diskCnt, inodeCnt int64
	err := pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM disks WHERE root_ino = $1),
			(SELECT COUNT(*) FROM inodes WHERE root_ino = $1)
	`, int64(deletedRootIno)).Scan(&diskCnt, &inodeCnt)
	require.NoError(t, err, "DeleteDisk 零残留审计查询失败")
	require.Zerof(t, diskCnt, "INV-4 违例: DeleteDisk 后 root_ino=%d 在 disks 中残留 %d 条", deletedRootIno, diskCnt)
	require.Zerof(t, inodeCnt, "INV-4 违例: DeleteDisk 后 root_ino=%d 在 inodes 中残留 %d 条", deletedRootIno, inodeCnt)

	if len(deletedInos) > 0 {
		var entryCnt, handleCnt, lockCnt, waiterCnt int64
		err := pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM entries WHERE parent_ino = ANY($1::bigint[]) OR ino = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_handles WHERE ino = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_locks WHERE ino = ANY($1::bigint[])),
				(SELECT COUNT(*) FROM file_lock_waiters WHERE ino = ANY($1::bigint[]))
		`, deletedInos).Scan(&entryCnt, &handleCnt, &lockCnt, &waiterCnt)
		require.NoError(t, err, "DeleteDisk 级联子表零残留审计查询失败")
		require.Zerof(t, entryCnt, "INV-4 违例: DeleteDisk 后 entries 中残留 %d 条", entryCnt)
		require.Zerof(t, handleCnt, "INV-4 违例: DeleteDisk 后 file_handles 中残留 %d 条", handleCnt)
		require.Zerof(t, lockCnt, "INV-4 违例: DeleteDisk 后 file_locks 中残留 %d 条", lockCnt)
		require.Zerof(t, waiterCnt, "INV-4 违例: DeleteDisk 后 file_lock_waiters 中残留 %d 条", waiterCnt)
	}
}

// closeSessionlessHandlesForRoots 通过公开接口 Release / ReleaseDir 关闭指定测试盘下所有 session_id IS NULL 的无会话句柄。
func closeSessionlessHandlesForRoots(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mgr meta.Manager, rootInos []uint64) {
	t.Helper()
	rootInt64s := make([]int64, len(rootInos))
	for i, r := range rootInos {
		rootInt64s[i] = int64(r)
	}
	rows, err := pool.Query(ctx, `
		SELECT fh.fh, fh.ino, i.mode
		FROM file_handles fh
		JOIN inodes i ON i.ino = fh.ino
		WHERE i.root_ino = ANY($1::bigint[])
		  AND fh.session_id IS NULL
	`, rootInt64s)
	require.NoError(t, err)
	defer rows.Close()

	type nilHandleItem struct {
		Fh   uint64
		Ino  uint64
		Mode uint32
	}
	var items []nilHandleItem
	for rows.Next() {
		var fh, ino int64
		var mode int32
		require.NoError(t, rows.Scan(&fh, &ino, &mode))
		items = append(items, nilHandleItem{Fh: uint64(fh), Ino: uint64(ino), Mode: uint32(mode)})
	}
	require.NoError(t, rows.Err())

	for _, item := range items {
		if (item.Mode & meta.S_IFMT) == meta.S_IFDIR {
			err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: item.Ino, Fh: item.Fh, SessionID: nil})
			require.NoError(t, err)
		} else {
			err := mgr.Release(ctx, meta.ReleaseParams{
				Ino:          item.Ino,
				Fh:           item.Fh,
				ReleaseFlags: meta.FuseReleaseFlockUnlock,
				SessionID:    nil,
			})
			require.NoError(t, err)
		}
	}
}

// TestFormalConcurrentChaos 运行多协程高并发混沌冲突压测（T14 - STARSRC-428）：
// 1. 启动 6~8 个并发 Worker 协程，各持基于 64 位主种子派生的独立 PCG 随机源，对共享测试 Disk 目录树同时发起高频写冲突与只读探针；
// 2. 断言全程零未恢复的 PostgreSQL 死锁 (40P01) 或数据库连接崩溃；
// 3. 并发停止后，使所有 Worker 会话超期并显式调用 RenewSession 触发同步清扫，对底表直接运行全部 10 条数学不变量 (INV-1 ~ INV-6, LOCK-INV-1 ~ LOCK-INV-4) 的 SQL 独立审计；
// 4. 对并发压测后的主测试盘执行 DeleteDisk，断言归属该 root_ino 的 6 张底表记录数全部归零，且邻盘依然满足全部 10 大数学不变量。
func TestFormalConcurrentChaos(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	cfg := loadFormalConcurrentConfig(t)

	for runIdx := 0; runIdx < cfg.Runs; runIdx++ {
		runSeed := cfg.BaseSeed + uint64(runIdx)*0x9e3779b97f4a7c15
		t.Run(fmt.Sprintf("Run_%d_Seed_0x%x", runIdx, runSeed), func(t *testing.T) {
			ctx := context.Background()
			store := db.NewStore(pool)
			const shortLeaseTTL = 30 * time.Millisecond
			mgr := meta.New(store)
			shortMgr := meta.New(store, meta.WithSessionLeaseTTL(shortLeaseTTL))

			disk1, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
				Name:     fmt.Sprintf("conc-disk1-%x-r%d", runSeed&0xffffffff, runIdx),
				MaxBytes: 8 * 1024 * 1024,
			})
			require.NoError(t, err)

			disk2, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
				Name:     fmt.Sprintf("conc-disk2-%x-r%d", runSeed&0xffffffff, runIdx),
				MaxBytes: 8 * 1024 * 1024,
			})
			require.NoError(t, err)

			env := &formalConcurrentSharedEnv{
				T:              t,
				Seed:           runSeed,
				RunIndex:       runIdx,
				Workers:        cfg.Workers,
				StepsPerWorker: cfg.StepsPerWorker,
				Pool:           pool,
				Store:          store,
				Mgr:            mgr,
				ShortMgr:       shortMgr,
				LeaseTTL:       shortLeaseTTL,
				Disk1:          disk1,
				Disk2:          disk2,
				dirInos:        []uint64{disk1.RootIno},
				workerTraces:   make([][]string, cfg.Workers),
			}

			// 注册 Worker 专属会话与收尾保留会话 keeperSID（由 runSeed 确定性派生，避免与 crashSID 空间碰撞）
			baseSID := (runSeed&0xfffff)*1_000_000 + 1_000_000_000 + uint64(runIdx)*100000
			keeperSID := baseSID + 9999
			_, err = mgr.RenewSession(ctx, keeperSID)
			require.NoError(t, err)

			for w := 0; w < cfg.Workers; w++ {
				wSID := baseSID + uint64(w+1)*100
				_, err := mgr.RenewSession(ctx, wSID)
				require.NoError(t, err)
				env.workerSessionIDs = append(env.workerSessionIDs, wSID)
			}

			t.Cleanup(func() {
				cleanCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = pool.Exec(cleanCtx, `DELETE FROM disks WHERE root_ino = ANY($1::bigint[])`,
					[]int64{int64(disk1.RootIno), int64(disk2.RootIno)})
				_, _ = pool.Exec(cleanCtx, `DELETE FROM inodes WHERE root_ino = ANY($1::bigint[])`,
					[]int64{int64(disk1.RootIno), int64(disk2.RootIno)})

				allSIDs := []int64{int64(keeperSID)}
				for _, s := range env.workerSessionIDs {
					allSIDs = append(allSIDs, int64(s))
				}
				for _, s := range env.crashSessionIDs {
					allSIDs = append(allSIDs, int64(s))
				}
				_, _ = pool.Exec(cleanCtx, `DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])`, allSIDs)
			})

			// 在 Disk1 与 Disk2 上各创建 1 个 4096 字节的锚点常规文件与 2 个初始共享子目录
			pin1, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: disk1.RootIno,
				Name:      []byte("__pinned_anchor_1__"),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
			pinSize := uint64(4096)
			_, err = mgr.SetAttr(ctx, pin1.Ino, meta.SetAttrInput{Size: &pinSize})
			require.NoError(t, err)
			env.PinnedIno1 = pin1.Ino
			env.AddFileIno(pin1.Ino)

			pin2, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: disk2.RootIno,
				Name:      []byte("__pinned_anchor_2__"),
				Mode:      meta.S_IFREG | 0o644,
			})
			require.NoError(t, err)
			_, err = mgr.SetAttr(ctx, pin2.Ino, meta.SetAttrInput{Size: &pinSize})
			require.NoError(t, err)
			env.PinnedIno2 = pin2.Ino
			// pin2 专属 Disk2 锁对照，不加入 fileInos 避免并发 Worker 跨盘 Link 导致 ErrCrossDevice

			for i := 0; i < 3; i++ {
				sub, mkErr := mgr.Mkdir(ctx, meta.MkdirParams{
					ParentIno: disk1.RootIno,
					Name:      []byte(fmt.Sprintf("shared_dir_%d", i)),
					Mode:      0o755,
				})
				require.NoError(t, mkErr)
				env.AddDirIno(sub.Ino)
			}

			// 构造 keeperSID 持有的存活孤儿文件 (unlink-whilst-open) 与存活孤儿目录 (rmdir-whilst-open)
			// 以及在锚点文件上的多段 POSIX 锁与 BSD flock，使得收尾审计时同时覆盖非空活跃孤儿与活跃锁不变量！
			keepOrphanFileRes, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: disk1.RootIno,
				Name:      []byte("__keeper_orphan_file__"),
				Mode:      meta.S_IFREG | 0o600,
				Flags:     meta.O_CREAT | meta.O_RDWR,
				SessionID: &keeperSID,
			})
			require.NoError(t, err)
			keepOrphanSz := uint64(2048)
			_, err = mgr.SetAttr(ctx, keepOrphanFileRes.Inode.Ino, meta.SetAttrInput{
				Size:      &keepOrphanSz,
				Fh:        &keepOrphanFileRes.Fh,
				SessionID: &keeperSID,
			})
			require.NoError(t, err)
			require.NoError(t, mgr.Unlink(ctx, disk1.RootIno, []byte("__keeper_orphan_file__")))

			keepOrphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
				ParentIno: disk1.RootIno,
				Name:      []byte("__keeper_orphan_dir__"),
				Mode:      0o755,
			})
			require.NoError(t, err)
			keepOrphanDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
				Ino:       keepOrphanDir.Ino,
				Flags:     meta.O_RDONLY,
				SessionID: &keeperSID,
			})
			require.NoError(t, err)
			require.NoError(t, mgr.Rmdir(ctx, disk1.RootIno, []byte("__keeper_orphan_dir__")))

			// 为 keeperSID 在 Disk2 的锚点文件上设置 POSIX 区间锁（含裂解/合并）与 BSD flock 读锁，
			// 并立即执行带 LockOwner=9999 的 Flush（验证 Flush 仅清理对应 POSIX 锁，绝不误删同句柄 BSD flock 读锁或 LockOwner=8801 的 POSIX 锁）！
			keepPin2Open, err := mgr.Open(ctx, meta.OpenParams{
				Ino:       env.PinnedIno2,
				Flags:     meta.O_RDWR,
				SessionID: &keeperSID,
			})
			require.NoError(t, err)
			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2Open.Fh,
				LockOwner: 8801,
				SessionID: keeperSID,
				LkFlags:   0,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 88},
			}))
			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2Open.Fh,
				LockOwner: 8801,
				SessionID: keeperSID,
				LkFlags:   0,
				Lk:        meta.FileLock{Start: 30, End: 60, Type: meta.F_RDLCK, PID: 89},
			}))
			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2Open.Fh,
				LockOwner: 9999,
				SessionID: keeperSID,
				LkFlags:   0,
				Lk:        meta.FileLock{Start: 200, End: 300, Type: meta.F_WRLCK, PID: 90},
			}))
			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2Open.Fh,
				LockOwner: 8801,
				SessionID: keeperSID,
				LkFlags:   meta.FuseLkFlock,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 88},
			}))
			flushOwner9999 := uint64(9999)
			require.NoError(t, mgr.Flush(ctx, meta.FlushParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2Open.Fh,
				LockOwner: &flushOwner9999,
				SessionID: &keeperSID,
			}))
			// 构造 keeperSID 在 PinnedIno2 [0, 50] 上的一次真实阻塞 SetLkw 超时取消（验证取消后既无 PENDING 残留，亦未在 file_locks 中对阻塞区间产生悬挂写锁）
			keepPin2CancelOpen, err := mgr.Open(ctx, meta.OpenParams{
				Ino:       env.PinnedIno2,
				Flags:     meta.O_RDWR,
				SessionID: &keeperSID,
			})
			require.NoError(t, err)
			cancelReqID := env.NextRequestID()
			cancelCtx, cancelFn := context.WithTimeout(ctx, 8*time.Millisecond)
			cancelErr := mgr.SetLkw(cancelCtx, meta.SetLkwParams{
				Ino:       env.PinnedIno2,
				Fh:        keepPin2CancelOpen.Fh,
				LockOwner: 7701,
				SessionID: keeperSID,
				RequestID: cancelReqID,
				LkFlags:   0,
				Lk:        meta.FileLock{Start: 0, End: 50, Type: meta.F_WRLCK, PID: 91},
			})
			cancelFn()
			require.ErrorIs(t, cancelErr, meta.ErrInterrupted, "keeper 阻塞 SetLkw 超时取消必须返回 ErrInterrupted")
			env.RecordCancelledWaiter(keeperSID, cancelReqID)

			// -----------------------------------------------------------------
			// 启动多协程 Worker 并发混沌冲突压测
			// -----------------------------------------------------------------
			startBarrier := make(chan struct{})
			var wg sync.WaitGroup
			for w := 0; w < cfg.Workers; w++ {
				wg.Add(1)
				wID := w
				wSeed := runSeed ^ (uint64(wID+1) * 0x517cc1b727220a95)
				go func() {
					defer wg.Done()
					<-startBarrier
					runConcurrentWorker(ctx, t, env, wID, wSeed, cfg.StepsPerWorker)
				}()
			}
			close(startBarrier)
			wg.Wait()

			// -----------------------------------------------------------------
			// 阶段 1：并发活跃态底表审计
			// Worker 会话与打开句柄依然有效，活跃锁与等待图均真实存在。
			// 先对 keeperSID 与全部 workerSessionIDs 执行 RenewSession 续租保活，
			// 立即全量扫描底表校验 INV-1、INV-2、INV-4、INV-5、INV-6 及 LOCK-INV-1、LOCK-INV-2、LOCK-INV-3、LOCK-INV-4(a)！
			// -----------------------------------------------------------------
			_, err = mgr.RenewSession(ctx, keeperSID)
			require.NoError(t, err)
			for _, wSID := range env.workerSessionIDs {
				_, err := mgr.RenewSession(ctx, wSID)
				require.NoError(t, err)
			}
			auditConcurrentPhase1ActiveInvariants(ctx, t, env, []uint64{disk1.RootIno, disk2.RootIno})

			// -----------------------------------------------------------------
			// 阶段 2：短租约过期与级联收敛清扫
			// 1. 将所有 Worker 会话缩短租约 (30ms TTL) 并等待 42ms 使其全部超期；
			// 2. 关闭所有测试盘上的无会话句柄 (session_id IS NULL)；
			// 3. 显式调用 mgr.RenewSession(keeperSID) 触发同步清扫（级联清理所有过期会话的句柄/锁/等待者，并物理回收所有无引用孤儿节点）。
			// -----------------------------------------------------------------
			for _, wSID := range env.workerSessionIDs {
				_, err := pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() + interval '30 milliseconds' WHERE session_id = $1", int64(wSID))
				require.NoError(t, err)
			}
			time.Sleep(shortLeaseTTL + 12*time.Millisecond)

			// 确保并发期间经长租约续期的 Worker 会话全部进入超期状态，以便同步清扫
			workerSIDsInt64 := make([]int64, len(env.workerSessionIDs))
			for i, sid := range env.workerSessionIDs {
				workerSIDsInt64[i] = int64(sid)
			}
			_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '100 millisecond' WHERE session_id = ANY($1::bigint[])`, workerSIDsInt64)
			require.NoError(t, err)

			closeSessionlessHandlesForRoots(ctx, t, pool, mgr, []uint64{disk1.RootIno, disk2.RootIno})

			_, err = mgr.RenewSession(ctx, keeperSID)
			require.NoError(t, err, "并发停止后调用 RenewSession 同步清扫不应失败")

			var allPurgedSIDs []uint64
			allPurgedSIDs = append(allPurgedSIDs, env.workerSessionIDs...)
			allPurgedSIDs = append(allPurgedSIDs, env.crashSessionIDs...)

			// 停机清扫后全量 SQL 独立审计 10 大数学不变量（含 keeperSID 存活孤儿与活跃锁，以及 INV-3 孤儿清扫与 LOCK-INV-4(b) 会话清理）
			auditConcurrentAll10Invariants(ctx, t, env, []uint64{disk1.RootIno, disk2.RootIno}, allPurgedSIDs)

			// -----------------------------------------------------------------
			// 释放 keeperSID 持有的孤儿文件句柄与孤儿目录句柄，
			// 验证所有测试盘上的 nlink == 0 孤儿节点数彻底归零！
			// -----------------------------------------------------------------
			require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
				Ino:       keepOrphanFileRes.Inode.Ino,
				Fh:        keepOrphanFileRes.Fh,
				SessionID: &keeperSID,
			}))

			require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
				Ino:       keepOrphanDir.Ino,
				Fh:        keepOrphanDirRes.Fh,
				SessionID: &keeperSID,
			}))

			var totalRemainingOrphans int64
			err = pool.QueryRow(ctx, `
				SELECT COUNT(*)
				FROM inodes
				WHERE root_ino = ANY($1::bigint[]) AND nlink = 0
			`, []int64{int64(disk1.RootIno), int64(disk2.RootIno)}).Scan(&totalRemainingOrphans)
			require.NoError(t, err)
			require.Zerof(t, totalRemainingOrphans,
				"INV-3 违例: 释放全部孤儿句柄后，底表中仍残留 %d 个 nlink=0 孤儿节点", totalRemainingOrphans)

			// 再次对双盘执行 10 大数学不变量全量审计
			auditConcurrentAll10Invariants(ctx, t, env, []uint64{disk1.RootIno, disk2.RootIno}, allPurgedSIDs)

			// -----------------------------------------------------------------
			// 阶段 3 (INV-4 删盘级联归零证明)：
			// 对经受高并发混沌压测后的 Disk1 执行 DeleteDisk，
			// 先记录 Disk1 名下所有 ino，断言归属 Disk1.RootIno 的 disks、inodes、entries、file_handles、file_locks、file_lock_waiters 记录数全部归零，
			// 且存活邻盘 Disk2 的 10 大数学不变量依然 100% 成立！
			// -----------------------------------------------------------------
			var disk1Inos []int64
			dRows, err := pool.Query(ctx, `SELECT ino FROM inodes WHERE root_ino = $1`, int64(disk1.RootIno))
			require.NoError(t, err)
			for dRows.Next() {
				var dino int64
				require.NoError(t, dRows.Scan(&dino))
				disk1Inos = append(disk1Inos, dino)
			}
			require.NoError(t, dRows.Err())
			dRows.Close()

			require.NoError(t, mgr.DeleteDisk(ctx, disk1.DiskID))
			assertDeletedDiskZeroResidue(ctx, t, pool, disk1.RootIno, disk1Inos)
			auditConcurrentAll10Invariants(ctx, t, env, []uint64{disk2.RootIno}, allPurgedSIDs)

			// 断言高并发阶段真实发生了写操作成功
			require.Greater(t, env.writeSuccessCount.Load(), int64(0), "并发压测期间至少应发生一次写操作成功")
		})
	}
}
