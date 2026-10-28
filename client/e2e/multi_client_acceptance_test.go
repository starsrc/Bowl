//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	multiClientLockHelperEnv = "BOWL_TEST_MULTI_CLIENT_LOCK_HELPER"
	multiUserHelperEnv       = "BOWL_TEST_MULTI_USER_HELPER"
)

func init() {
	if os.Getenv(multiClientLockHelperEnv) == "1" {
		runMultiClientLockInterruptHelper()
		os.Exit(0)
	}
}

// runMultiClientLockInterruptHelper 用于子进程阻塞在 F_SETLKW 并接收信号测试中断返回 EINTR。
func runMultiClientLockInterruptHelper() {
	path := os.Getenv("BOWL_TEST_LOCK_TARGET")
	if path == "" {
		fmt.Fprintln(os.Stderr, "missing BOWL_TEST_LOCK_TARGET")
		os.Exit(10)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGUSR1)

	// 显式清除 SA_RESTART 标志位，确保系统调用收到信号立即被内核中断返回 EINTR
	var sa struct {
		handler  uintptr
		flags    uint64
		restorer uintptr
		mask     uint64
	}
	_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGACTION, uintptr(syscall.SIGUSR1), 0, uintptr(unsafe.Pointer(&sa)), 8, 0, 0)
	const saRestart = 0x10000000
	sa.flags &^= saRestart
	_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGACTION, uintptr(syscall.SIGUSR1), uintptr(unsafe.Pointer(&sa)), 0, 8, 0, 0)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open error: %v\n", err)
		os.Exit(11)
	}
	defer f.Close()

	fmt.Println("READY_TO_LOCK")
	_ = os.Stdout.Sync()

	fl := unix.Flock_t{
		Type:   unix.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}

	err = unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &fl)
	if err != nil {
		if errors.Is(err, syscall.EINTR) {
			fmt.Println("INTERRUPTED_EINTR")
			_ = os.Stdout.Sync()
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "fcntl error: %v\n", err)
		os.Exit(12)
	}

	fmt.Println("ACQUIRED_UNEXPECTED")
	_ = os.Stdout.Sync()
	os.Exit(13)
}

// TestMultiUserHelperProcess 作为非 root 普通用户凭据（uid/gid）派生的测试辅助进程。
func TestMultiUserHelperProcess(t *testing.T) {
	if os.Getenv(multiUserHelperEnv) != "1" {
		return
	}

	args := os.Args
	var subArgs []string
	for i, arg := range args {
		if arg == "--" {
			subArgs = args[i+1:]
			break
		}
	}

	if len(subArgs) < 2 {
		fmt.Fprintf(os.Stderr, "缺少参数: %v\n", subArgs)
		os.Exit(99)
	}

	op := subArgs[0]
	target := subArgs[1]

	handleErr := func(err error) {
		if err == nil {
			fmt.Println("SUCCESS")
			os.Exit(0)
		}
		if errors.Is(err, syscall.EACCES) {
			fmt.Fprintf(os.Stderr, "ERR_EACCES: %v\n", err)
			os.Exit(10)
		}
		if errors.Is(err, syscall.EPERM) {
			fmt.Fprintf(os.Stderr, "ERR_EPERM: %v\n", err)
			os.Exit(11)
		}
		fmt.Fprintf(os.Stderr, "ERR_OTHER: %v\n", err)
		os.Exit(12)
	}

	switch op {
	case "read":
		_, err := os.ReadFile(target)
		handleErr(err)
	case "write":
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			_, err = f.Write([]byte("unauthorized append\n"))
			_ = f.Close()
		}
		handleErr(err)
	case "create":
		f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			_, err = f.Write([]byte("created-by-helper\n"))
			_ = f.Close()
		}
		handleErr(err)
	case "remove":
		err := os.Remove(target)
		handleErr(err)
	case "rename":
		if len(subArgs) < 3 {
			fmt.Fprintln(os.Stderr, "rename 需要新目标路径")
			os.Exit(99)
		}
		newTarget := subArgs[2]
		err := os.Rename(target, newTarget)
		handleErr(err)
	default:
		fmt.Fprintf(os.Stderr, "未知操作: %s\n", op)
		os.Exit(99)
	}
	os.Exit(0)
}

// ensureFuseAllowOther 配置系统 /etc/fuse.conf 允许 user_allow_other。
func ensureFuseAllowOther() {
	data, err := os.ReadFile("/etc/fuse.conf")
	if err == nil {
		if !strings.Contains(string(data), "user_allow_other") {
			_ = os.WriteFile("/etc/fuse.conf", append(data, []byte("\nuser_allow_other\n")...), 0644)
		}
	} else {
		_ = os.WriteFile("/etc/fuse.conf", []byte("user_allow_other\n"), 0644)
	}
}

// findMultiClientDaemonPID 扫描 /proc 查找与目标 diskID 及 mountpoint 绑定的后台 bowl 守护进程 PID。
func findMultiClientDaemonPID(diskID, mountpoint string) (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("读取 /proc 失败: %w", err)
	}

	cleanMP := filepath.Clean(mountpoint)
	myPID := os.Getpid()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == myPID {
			continue
		}

		cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}

		cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
		if strings.Contains(cmdline, "bowl") && strings.Contains(cmdline, "mount") &&
			strings.Contains(cmdline, diskID) && strings.Contains(cmdline, cleanMP) {
			return pid, nil
		}
	}

	return 0, fmt.Errorf("未找到挂载点 %s (diskID: %s) 的后台守护进程", mountpoint, diskID)
}

// isProcessDeadOrZombie 检查进程是否已在系统中终止或已成为僵尸状态。
func isProcessDeadOrZombie(pid int) bool {
	statusBytes, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return true
	}
	for _, line := range strings.Split(string(statusBytes), "\n") {
		if strings.HasPrefix(line, "State:") {
			if strings.Contains(line, "Z") || strings.Contains(line, "X") {
				return true
			}
			return false
		}
	}
	return false
}

// waitForDaemonProcessExit 轮询等待指定 PID 彻底退出。
func waitForDaemonProcessExit(t *testing.T, pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isProcessDeadOrZombie(pid) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("守护进程 PID %d 在 %v 内未退出", pid, timeout)
}

// assertNoBowlDaemonResiduals 扫描系统进程表断言没有任何指向当前 diskID 或挂载点的 bowl 守护进程存活残留。
func assertNoBowlDaemonResiduals(t *testing.T, diskID string, mountpoints ...string) {
	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)

	cleanedMPs := make([]string, len(mountpoints))
	for i, mp := range mountpoints {
		cleanedMPs[i] = filepath.Clean(mp)
	}
	myPID := os.Getpid()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == myPID {
			continue
		}

		cmdlineBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if len(cmdlineBytes) == 0 {
			continue // 僵尸进程在 Linux 下 cmdline 为空
		}

		cmdline := string(bytes.ReplaceAll(cmdlineBytes, []byte{0}, []byte(" ")))
		if strings.Contains(cmdline, "bowl") && strings.Contains(cmdline, "mount") {
			matchesDisk := strings.Contains(cmdline, diskID)
			matchesMP := false
			for _, cmp := range cleanedMPs {
				if strings.Contains(cmdline, cmp) {
					matchesMP = true
					break
				}
			}
			if matchesDisk || matchesMP {
				if !isProcessDeadOrZombie(pid) {
					t.Fatalf("检测到残留后台 bowl 守护进程 PID=%d cmdline: %s", pid, cmdline)
				}
			}
		}
	}
}

// runMultiUserHelperCmd 执行带有真实非 root 普通用户凭据的子进程操作。
func runMultiUserHelperCmd(t *testing.T, uid, gid uint32, op string, args ...string) (int, string, string) {
	t.Helper()

	// 确保测试二进制及其父目录可被普通用户读取与执行
	for p := os.Args[0]; p != "/" && p != "."; p = filepath.Dir(p) {
		_ = os.Chmod(p, 0755)
	}

	cmdArgs := []string{"-test.run=^TestMultiUserHelperProcess$", "--", op}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), multiUserHelperEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid: uid,
			Gid: gid,
		},
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("运行多用户辅助进程失败: %v", err)
		}
	}
	return exitCode, stdout.String(), stderr.String()
}

// TestE2E_MultiClient_ComprehensiveAcceptance 跨客户端多挂载点综合验收测试套件 (STARSRC-521)。
func TestE2E_MultiClient_ComprehensiveAcceptance(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	ensureFuseAllowOther()

	// 准备具有 root_ino > 1 的目标磁盘
	disk := createDiskWithRootInoGT1(t, client)
	diskID := disk.GetDiskId()
	require.Greater(t, disk.GetRootIno(), uint64(1), "磁盘 root_ino 必须大于 1")

	mpA := t.TempDir()
	mpB := t.TempDir()

	// 确保挂载点及其所有父级目录具备 0755 权限，使非 root 普通用户可穿透访问
	for _, mp := range []string{mpA, mpB} {
		for p := mp; p != "/" && p != "."; p = filepath.Dir(p) {
			_ = os.Chmod(p, 0755)
		}
	}

	// 确保退出时若未被场景 4 卸载，则兜底强制卸载以防泄露
	defer func() {
		if isMounted(mpA) {
			_ = exec.Command(getBowlBin(), "umount", "-f", mpA).Run()
		}
		if isMounted(mpB) {
			_ = exec.Command(getBowlBin(), "umount", "-f", mpB).Run()
		}
	}()

	// 步骤 1：通过默认后台守护模式（不带 -f）分别挂载 mpA 与 mpB
	t.Logf("启动客户端 A 守护挂载: %s", mpA)
	mountCmdA := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, diskID, mpA, "--allow-other", "--debug")
	outA, errA := mountCmdA.CombinedOutput()
	require.NoError(t, errA, "客户端 A 后台挂载失败: %s", string(outA))
	require.NoError(t, waitForMount(mpA, 10*time.Second), "客户端 A 挂载点就绪")

	pidA, err := findMultiClientDaemonPID(diskID, mpA)
	require.NoError(t, err, "必须能探测到客户端 A 活跃守护进程")
	require.Greater(t, pidA, 0)
	t.Logf("客户端 A 活跃守护进程 PID: %d", pidA)

	t.Logf("启动客户端 B 守护挂载: %s", mpB)
	mountCmdB := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, diskID, mpB, "--allow-other", "--debug")
	outB, errB := mountCmdB.CombinedOutput()
	require.NoError(t, errB, "客户端 B 后台挂载失败: %s", string(outB))
	require.NoError(t, waitForMount(mpB, 10*time.Second), "客户端 B 挂载点就绪")

	pidB, err := findMultiClientDaemonPID(diskID, mpB)
	require.NoError(t, err, "必须能探测到客户端 B 活跃守护进程")
	require.Greater(t, pidB, 0)
	require.NotEqual(t, pidA, pidB, "客户端 A 与客户端 B 必须运行在独立后台守护进程中")
	t.Logf("客户端 B 活跃守护进程 PID: %d", pidB)

	// 场景 1：双挂载点跨 ClientSession 共享读写（Close-to-Open 一致性）
	t.Run("Scenario1_CloseToOpenConsistency", func(t *testing.T) {
		sharedFile := "c2o_shared.dat"
		pathA := filepath.Join(mpA, sharedFile)
		pathB := filepath.Join(mpB, sharedFile)

		// 1.1 客户端 A 创建文件并写入初始内容，关闭后客户端 B 立即读取并校验
		initialData := bytes.Repeat([]byte("AlphaBetaGammaDelta"), 512) // 10240 bytes
		fA, err := os.OpenFile(pathA, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		require.NoError(t, err, "客户端 A 创建文件失败")
		n, err := fA.Write(initialData)
		require.NoError(t, err)
		require.Equal(t, len(initialData), n)
		require.NoError(t, fA.Close(), "客户端 A 关闭文件失败")

		// 客户端 B 打开读取验证
		readB, err := os.ReadFile(pathB)
		require.NoError(t, err, "客户端 B 读取新创建文件失败")
		require.Equal(t, initialData, readB, "客户端 B 读取内容必须与客户端 A 写入内容完全一致")

		stB, err := os.Stat(pathB)
		require.NoError(t, err)
		require.Equal(t, int64(len(initialData)), stB.Size(), "客户端 B stat 文件大小必须一致")

		// 1.2 客户端 A 覆盖写：在 offset 2048 覆盖写入 1024 字节
		overwritePayload := bytes.Repeat([]byte("OVERWRITE_BLOCK_"), 64) // 1024 bytes
		fAOver, err := os.OpenFile(pathA, os.O_WRONLY, 0)
		require.NoError(t, err)
		nOver, err := fAOver.WriteAt(overwritePayload, 2048)
		require.NoError(t, err)
		require.Equal(t, len(overwritePayload), nOver)
		require.NoError(t, fAOver.Close())

		// 客户端 B 重新打开读取验证覆盖写一致性
		readBAfterOver, err := os.ReadFile(pathB)
		require.NoError(t, err)
		expectedOver := make([]byte, len(initialData))
		copy(expectedOver, initialData)
		copy(expectedOver[2048:2048+len(overwritePayload)], overwritePayload)
		require.Equal(t, expectedOver, readBAfterOver, "客户端 B 必须立即观测到客户端 A 的覆盖写入")

		// 1.3 客户端 A 截断 (Truncate) 缩容与扩容
		// 缩容至 4096 字节
		require.NoError(t, os.Truncate(pathA, 4096), "客户端 A 截断缩容失败")
		stBTruncDown, err := os.Stat(pathB)
		require.NoError(t, err)
		require.Equal(t, int64(4096), stBTruncDown.Size(), "客户端 B 必须立即感知缩容至 4096")
		readBTruncDown, err := os.ReadFile(pathB)
		require.NoError(t, err)
		require.Equal(t, expectedOver[:4096], readBTruncDown)

		// 扩容至 16384 字节（形成稀疏空洞）
		require.NoError(t, os.Truncate(pathA, 16384), "客户端 A 截断扩容失败")
		stBTruncUp, err := os.Stat(pathB)
		require.NoError(t, err)
		require.Equal(t, int64(16384), stBTruncUp.Size(), "客户端 B 必须立即感知扩容至 16384")
		readBTruncUp, err := os.ReadFile(pathB)
		require.NoError(t, err)
		require.Equal(t, expectedOver[:4096], readBTruncUp[:4096])
		require.Equal(t, make([]byte, 16384-4096), readBTruncUp[4096:], "扩容区间必须全部读为全零字节")

		// 1.4 客户端 A 执行 unix.Fallocate 打洞 (PUNCH_HOLE|KEEP_SIZE)
		// 对 offset 1024, len 1024 打洞
		fAHole, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		require.NoError(t, unix.Fallocate(int(fAHole.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 1024, 1024), "打洞操作必须成功")
		require.NoError(t, fAHole.Close())

		// 客户端 B 读取验证打洞区间变为全零
		readBHole, err := os.ReadFile(pathB)
		require.NoError(t, err)
		require.Equal(t, expectedOver[:1024], readBHole[:1024], "打洞前区间数据无损")
		require.Equal(t, make([]byte, 1024), readBHole[1024:2048], "打洞区间必须全为零字节")
		require.Equal(t, expectedOver[2048:4096], readBHole[2048:4096], "打洞后区间数据无损")

		// 1.5 客户端 A 执行 Rename 与 unix.Renameat2(RENAME_EXCHANGE)
		// 1.5.1 标准改名验证
		fileRenameSrc := "rename_src.txt"
		fileRenameDst := "rename_dst.txt"
		renameSrcA := filepath.Join(mpA, fileRenameSrc)
		renameDstA := filepath.Join(mpA, fileRenameDst)
		renameDstB := filepath.Join(mpB, fileRenameDst)

		contentRename := []byte("RENAME_CONTENT_CHECK")
		require.NoError(t, os.WriteFile(renameSrcA, contentRename, 0644))
		require.NoError(t, os.Rename(renameSrcA, renameDstA), "客户端 A 重命名失败")

		// 客户端 B 目录遍历确认旧文件消失、新文件存在且内容正确
		entriesB, err := os.ReadDir(mpB)
		require.NoError(t, err)
		foundOld := false
		foundNew := false
		for _, e := range entriesB {
			if e.Name() == fileRenameSrc {
				foundOld = true
			}
			if e.Name() == fileRenameDst {
				foundNew = true
			}
		}
		require.False(t, foundOld, "客户端 B 不应再看到原文件名")
		require.True(t, foundNew, "客户端 B 必须立即可见新文件名")

		readDstB, err := os.ReadFile(renameDstB)
		require.NoError(t, err)
		require.Equal(t, contentRename, readDstB, "客户端 B 读取重命名后目标文件内容必须一致")

		// 1.5.2 原子双向原地互换：通过 RENAME_EXCHANGE 交换两文件
		fileEx1 := "exchange_file_1.txt"
		fileEx2 := "exchange_file_2.txt"
		ex1A := filepath.Join(mpA, fileEx1)
		ex2A := filepath.Join(mpA, fileEx2)
		ex1B := filepath.Join(mpB, fileEx1)
		ex2B := filepath.Join(mpB, fileEx2)

		contentEx1 := []byte("PAYLOAD_EXCHANGE_ALPHA_111")
		contentEx2 := []byte("PAYLOAD_EXCHANGE_BRAVO_222")
		require.NoError(t, os.WriteFile(ex1A, contentEx1, 0644))
		require.NoError(t, os.WriteFile(ex2A, contentEx2, 0644))

		// 客户端 A 执行原子双向交换
		err = unix.Renameat2(unix.AT_FDCWD, ex1A, unix.AT_FDCWD, ex2A, unix.RENAME_EXCHANGE)
		require.NoError(t, err, "客户端 A RENAME_EXCHANGE 原地互换应成功")

		// 客户端 B 打开读取验证两文件内容完全对调互换
		readAfterEx1, err := os.ReadFile(ex1B)
		require.NoError(t, err)
		require.Equal(t, contentEx2, readAfterEx1, "客户端 B 读取 ex1 必须为对调后的 contentEx2")

		readAfterEx2, err := os.ReadFile(ex2B)
		require.NoError(t, err)
		require.Equal(t, contentEx1, readAfterEx2, "客户端 B 读取 ex2 必须为对调后的 contentEx1")

		// 1.6 客户端 A 修改 xattr 扩展属性，客户端 B 实时无缓存感知
		xattrFile := filepath.Join(mpA, "xattr_target.txt")
		xattrFileB := filepath.Join(mpB, "xattr_target.txt")
		require.NoError(t, os.WriteFile(xattrFile, []byte("xattr-content"), 0644))

		attrName := "user.c2o_prop"
		attrVal1 := []byte("prop-value-client-a-init")
		require.NoError(t, unix.Setxattr(xattrFile, attrName, attrVal1, 0), "客户端 A 设置 xattr 失败")

		// 客户端 B 立即 Getxattr
		destBuf := make([]byte, 128)
		szB, err := unix.Getxattr(xattrFileB, attrName, destBuf)
		require.NoError(t, err, "客户端 B 获取 xattr 失败")
		require.Equal(t, attrVal1, destBuf[:szB], "客户端 B 必须立即获取最新 xattr")

		// 客户端 A 更新 xattr
		attrVal2 := []byte("prop-value-client-a-updated-v2")
		require.NoError(t, unix.Setxattr(xattrFile, attrName, attrVal2, 0))
		szB2, err := unix.Getxattr(xattrFileB, attrName, destBuf)
		require.NoError(t, err)
		require.Equal(t, attrVal2, destBuf[:szB2], "客户端 B 必须立即获取更新后的 xattr")

		// 客户端 A 删除 xattr
		require.NoError(t, unix.Removexattr(xattrFile, attrName))
		_, err = unix.Getxattr(xattrFileB, attrName, destBuf)
		require.Error(t, err)
		require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "删除后客户端 B 必须返回 ENODATA")
	})

	// 场景 2：跨客户端并发追加与分布式文件锁互斥
	t.Run("Scenario2_ConcurrentAppendAndDistributedLocks", func(t *testing.T) {
		// 2.1 客户端 A 与客户端 B 跨挂载点并发 O_APPEND 追加写入
		appendFile := "concurrent_append.log"
		appendPathA := filepath.Join(mpA, appendFile)
		appendPathB := filepath.Join(mpB, appendFile)

		fInit, err := os.OpenFile(appendPathA, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		require.NoError(t, err)
		require.NoError(t, fInit.Close())

		const (
			workersPerClient = 4
			recordsPerWorker = 50
			recordSize       = 64
		)
		totalRecords := workersPerClient * 2 * recordsPerWorker // 400 条

		var wg sync.WaitGroup
		errs := make(chan error, workersPerClient*2)

		writerRoutine := func(clientTag string, workerID int, targetPath string) {
			defer wg.Done()
			f, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				errs <- fmt.Errorf("[%s-%d] 打开文件失败: %w", clientTag, workerID, err)
				return
			}
			defer f.Close()

			for seq := 0; seq < recordsPerWorker; seq++ {
				// 构造定长 64 字节记录，携带 SHA-256 校验截断与覆写
				recHeader := fmt.Sprintf("REC:%s:%02d:%04d:", clientTag, workerID, seq)
				h := sha256.Sum256([]byte(recHeader))
				recBody := fmt.Sprintf("%s%x", recHeader, h[:16]) // 15 + 32 = 47
				padding := strings.Repeat("-", recordSize-len(recBody)-1)
				recLine := recBody + padding + "\n"
				if len(recLine) != recordSize {
					errs <- fmt.Errorf("[%s-%d] 记录长度错误: %d != %d", clientTag, workerID, len(recLine), recordSize)
					return
				}

				if _, err := f.Write([]byte(recLine)); err != nil {
					errs <- fmt.Errorf("[%s-%d] 写入记录失败: %w", clientTag, workerID, err)
					return
				}
			}
		}

		for i := 0; i < workersPerClient; i++ {
			wg.Add(2)
			go writerRoutine("CLIA", i, appendPathA)
			go writerRoutine("CLIB", i, appendPathB)
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err, "并发追加协程出现错误")
		}

		// 从两端校验文件总大小与全部 400 条记录零丢失、零截断覆写
		for _, checkPath := range []string{appendPathA, appendPathB} {
			st, err := os.Stat(checkPath)
			require.NoError(t, err)
			require.Equal(t, int64(totalRecords*recordSize), st.Size(), "追加文件总大小必须精确对齐")

			content, err := os.ReadFile(checkPath)
			require.NoError(t, err)
			require.Equal(t, totalRecords*recordSize, len(content))

			scanner := bufio.NewScanner(bytes.NewReader(content))
			foundRecords := make(map[string]bool)
			for scanner.Scan() {
				line := scanner.Text()
				require.Equal(t, recordSize-1, len(line), "每行记录长度必须精确符合规范")
				require.False(t, foundRecords[line], "发现重复记录行: %s", line)
				foundRecords[line] = true

				parts := strings.Split(line, ":")
				require.GreaterOrEqual(t, len(parts), 5, "记录格式必须完整")
				clientTag := parts[1]
				workerID, _ := strconv.Atoi(parts[2])
				seq, _ := strconv.Atoi(parts[3])
				recHeader := fmt.Sprintf("REC:%s:%02d:%04d:", clientTag, workerID, seq)
				expectedHashArr := sha256.Sum256([]byte(recHeader))
				expectedHash := fmt.Sprintf("%x", expectedHashArr[:16])
				require.True(t, strings.HasPrefix(parts[4], expectedHash), "记录哈希校验失败")
			}
			require.NoError(t, scanner.Err())
			require.Equal(t, totalRecords, len(foundRecords), "全部 400 条记录必须一条不丢")
		}

		// 2.2 跨客户端 POSIX fcntl 记录锁互斥、冲突探测与唤醒
		posixLockFile := "posix_lock_test.dat"
		posixPathA := filepath.Join(mpA, posixLockFile)
		posixPathB := filepath.Join(mpB, posixLockFile)
		require.NoError(t, os.WriteFile(posixPathA, make([]byte, 1024), 0644))

		fA, err := os.OpenFile(posixPathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fA != nil {
				_ = fA.Close()
			}
		}()

		// 客户端 A 在 mpA 持有 [0, 100] F_WRLCK
		flWrA := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    100,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &flWrA), "客户端 A 加写锁 [0, 100] 必须成功")

		fB, err := os.OpenFile(posixPathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fB != nil {
				_ = fB.Close()
			}
		}()

		// 客户端 B 在 mpB 对重叠区间 [50, 50] 发起非阻塞 F_SETLK 立即返回 syscall.EAGAIN
		flConflictB := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  50,
			Len:    50,
		}
		errConflict := unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flConflictB)
		require.ErrorIs(t, errConflict, syscall.EAGAIN, "客户端 B 在重叠区间必须返回 EAGAIN")

		// F_GETLK 准确探测出冲突写锁
		flProbe := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  50,
			Len:    50,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_GETLK, &flProbe), "探测锁冲突必须成功")
		require.Equal(t, int16(unix.F_WRLCK), flProbe.Type, "探测到冲突写锁")
		require.Equal(t, int64(0), flProbe.Start, "冲突写锁起始位置为 0")

		// 非重叠区间 [200, 300] 加锁立即成功
		flNonOverlap := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  200,
			Len:    100,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flNonOverlap), "非重叠区间加锁必须成功")
		flNonOverlap.Type = unix.F_UNLCK
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flNonOverlap), "解锁非重叠区间必须成功")

		// 客户端 B 发起阻塞 F_SETLKW 在 A 关闭文件（触发 Flush 携带 lock_owner）后立即唤醒获锁
		wakeCh := make(chan error, 1)
		go func() {
			flBlock := unix.Flock_t{
				Type:   unix.F_WRLCK,
				Whence: 0,
				Start:  0,
				Len:    100,
			}
			wakeCh <- unix.FcntlFlock(fB.Fd(), unix.F_SETLKW, &flBlock)
		}()

		// 确保 B 已进入等待排队状态
		time.Sleep(150 * time.Millisecond)

		// 客户端 A 关闭句柄，释放所有 POSIX 锁
		if err := fA.Close(); err != nil && !errors.Is(err, syscall.EINTR) {
			require.NoError(t, err)
		}
		fA = nil

		select {
		case errWake := <-wakeCh:
			require.NoError(t, errWake, "客户端 A 关闭文件后客户端 B 必须立即唤醒获锁")
		case <-time.After(5 * time.Second):
			t.Fatal("客户端 B 在 A 关闭文件后未在 5s 内唤醒获锁")
		}

		// 释放 B 端锁
		flUnB := unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: 0,
			Start:  0,
			Len:    100,
		}
		_ = unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flUnB)
		_ = fB.Close()
		fB = nil

		// 2.3 客户端 B 阻塞等待 F_SETLKW 期间向该等待子进程发送 SIGUSR1 信号触发内核 FUSE_INTERRUPT 立即中断返回 EINTR 且不残留悬挂锁
		posixIntFile := "posix_interrupt_test.dat"
		posixIntPathA := filepath.Join(mpA, posixIntFile)
		posixIntPathB := filepath.Join(mpB, posixIntFile)
		require.NoError(t, os.WriteFile(posixIntPathA, make([]byte, 512), 0644))

		fAInt, err := os.OpenFile(posixIntPathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fAInt != nil {
				_ = fAInt.Close()
			}
		}()

		flWrAll := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fAInt.Fd(), unix.F_SETLK, &flWrAll), "客户端 A 持有整文件写锁")

		// 启动阻塞子进程
		cmdSub := exec.Command(os.Args[0], "-test.run=TestE2E_MultiClient_ComprehensiveAcceptance", "--")
		cmdSub.Env = append(os.Environ(), multiClientLockHelperEnv+"=1", "BOWL_TEST_LOCK_TARGET="+posixIntPathB)

		subStdout, err := cmdSub.StdoutPipe()
		require.NoError(t, err)
		subStderr, err := cmdSub.StderrPipe()
		require.NoError(t, err)
		require.NoError(t, cmdSub.Start())

		subReader := bufio.NewReader(subStdout)
		line, err := subReader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "READY_TO_LOCK\n", line)

		// 确保进入服务端等待队列
		time.Sleep(200 * time.Millisecond)

		// 发送 SIGUSR1
		require.NoError(t, cmdSub.Process.Signal(syscall.SIGUSR1), "向阻塞等待子进程发送信号")

		line2, err := subReader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "INTERRUPTED_EINTR\n", line2, "阻塞锁必须立即被信号中断返回 EINTR")

		stderrScan := bufio.NewScanner(subStderr)
		if cmdSub.Wait() != nil {
			var errDetail string
			for stderrScan.Scan() {
				errDetail += stderrScan.Text() + "\n"
			}
			t.Fatalf("阻塞子进程异常退出: %s", errDetail)
		}

		// A 释放锁后，第三方在 B 端可立即获锁，证明无残留悬挂锁
		require.NoError(t, fAInt.Close())
		fAInt = nil

		fVerify, err := os.OpenFile(posixIntPathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fVerify.Close()

		require.NoError(t, unix.FcntlFlock(fVerify.Fd(), unix.F_SETLK, &flWrAll), "A 释放后 B 必须立即可重新加锁，证明无残留悬挂锁")

		// 2.4 跨客户端 BSD flock 排他锁互斥、dup 副本不释放与最后一个 fd 关闭唤醒
		flockFile := "flock_test.dat"
		flockPathA := filepath.Join(mpA, flockFile)
		flockPathB := filepath.Join(mpB, flockFile)
		require.NoError(t, os.WriteFile(flockPathA, make([]byte, 512), 0644))

		fAFlock, err := os.OpenFile(flockPathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fAFlock != nil {
				_ = fAFlock.Close()
			}
		}()

		// 客户端 A 持有排他 flock
		require.NoError(t, unix.Flock(int(fAFlock.Fd()), unix.LOCK_EX|unix.LOCK_NB), "客户端 A 获取 LOCK_EX 必须成功")

		// 复制 fd
		dupFd, err := unix.Dup(int(fAFlock.Fd()))
		require.NoError(t, err)

		fBFlock, err := os.OpenFile(flockPathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fBFlock != nil {
				_ = fBFlock.Close()
			}
		}()

		// 客户端 B 非阻塞获取 flock 立即返回 EWOULDBLOCK / EAGAIN
		errFlockConflict := unix.Flock(int(fBFlock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		require.True(t, errors.Is(errFlockConflict, syscall.EWOULDBLOCK) || errors.Is(errFlockConflict, syscall.EAGAIN))

		// 关闭 dup 副本 fd，BSD flock 依然持有不释放
		require.NoError(t, unix.Close(dupFd))

		errStillHeld := unix.Flock(int(fBFlock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		require.True(t, errors.Is(errStillHeld, syscall.EWOULDBLOCK) || errors.Is(errStillHeld, syscall.EAGAIN), "关闭 dup fd 后 flock 必须依然互斥")

		// 客户端 B 启动协程阻塞等待 flock
		flockWakeCh := make(chan error, 1)
		go func() {
			flockWakeCh <- unix.Flock(int(fBFlock.Fd()), unix.LOCK_EX)
		}()

		time.Sleep(150 * time.Millisecond)

		// 客户端 A 关闭最后一个活跃句柄，触发 Release 并释放 flock 锁
		if err := fAFlock.Close(); err != nil && !errors.Is(err, syscall.EINTR) {
			require.NoError(t, err)
		}
		fAFlock = nil

		select {
		case errWake := <-flockWakeCh:
			require.NoError(t, errWake, "最后一个 fd 关闭后客户端 B 必须立即唤醒获 flock")
		case <-time.After(5 * time.Second):
			t.Fatal("客户端 B 在 A 关闭最后一个 fd 后未在 5s 内唤醒获 flock")
		}

		_ = unix.Flock(int(fBFlock.Fd()), unix.LOCK_UN)
		require.NoError(t, fBFlock.Close())
		fBFlock = nil
	})

	// 场景 3：多用户 default_permissions 与 --allow-other 权限隔离
	t.Run("Scenario3_MultiUserPermissionsAndStickyBit", func(t *testing.T) {
		// 准备 1: root 属主 0600 文件 secret.txt
		secretPath := filepath.Join(mpA, "secret.txt")
		require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_TOP_SECRET_DATA"), 0600))

		// 准备 2: root 属主 0755 目录 readonly-dir
		roDirPath := filepath.Join(mpA, "readonly-dir")
		require.NoError(t, os.Mkdir(roDirPath, 0755))

		// 准备 3: 01777 带 Sticky Bit 的共享目录 shared-tmp
		sharedTmpPath := filepath.Join(mpA, "shared-tmp")
		require.NoError(t, os.Mkdir(sharedTmpPath, 0777|os.ModeSticky))
		require.NoError(t, unix.Chmod(sharedTmpPath, 01777), "设置 Sticky Bit (01777) 失败")

		// 3.1 uid=1000 读取或写入 secret.txt (0600) 均被拒绝并返回 EACCES
		codeRead, _, errRead := runMultiUserHelperCmd(t, 1000, 1000, "read", secretPath)
		require.Equal(t, 10, codeRead, "普通用户读取 root 0600 必须被拒绝: %s", errRead)
		require.Contains(t, errRead, "ERR_EACCES")

		codeWrite, _, errWrite := runMultiUserHelperCmd(t, 1000, 1000, "write", secretPath)
		require.Equal(t, 10, codeWrite, "普通用户写入 root 0600 必须被拒绝: %s", errWrite)
		require.Contains(t, errWrite, "ERR_EACCES")

		// 3.2 uid=1000 在 readonly-dir (0755) 下创建文件被拒绝并返回 EACCES
		forbiddenFile := filepath.Join(roDirPath, "forbidden.txt")
		codeCreateRO, _, errCreateRO := runMultiUserHelperCmd(t, 1000, 1000, "create", forbiddenFile)
		require.Equal(t, 10, codeCreateRO, "普通用户在 0755 目录下创建文件必须被拒绝: %s", errCreateRO)
		require.Contains(t, errCreateRO, "ERR_EACCES")

		// 3.3 uid=1000 在 shared-tmp (01777) 下成功创建文件 u1000.txt
		u1000File := filepath.Join(sharedTmpPath, "u1000.txt")
		codeCreateTmp, _, errCreateTmp := runMultiUserHelperCmd(t, 1000, 1000, "create", u1000File)
		require.Equal(t, 0, codeCreateTmp, "uid=1000 在 01777 共享目录下创建文件必须成功: %s", errCreateTmp)

		// 校验属主确为 uid=1000, gid=1000
		st1000, err := os.Stat(u1000File)
		require.NoError(t, err)
		sysStat := st1000.Sys().(*syscall.Stat_t)
		require.Equal(t, uint32(1000), sysStat.Uid, "文件属主 UID 必须为 1000")
		require.Equal(t, uint32(1000), sysStat.Gid, "文件属组 GID 必须为 1000")

		// 切换 uid=1001 尝试 os.Remove u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM (或 EACCES)
		codeRemove1001, _, errRemove1001 := runMultiUserHelperCmd(t, 1001, 1001, "remove", u1000File)
		require.True(t, codeRemove1001 == 11 || codeRemove1001 == 10, "非属主删除 Sticky Bit 目录下文件必须被拦截: %s (code: %d)", errRemove1001, codeRemove1001)

		// 切换 uid=1001 尝试 os.Rename u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM (或 EACCES)
		u1001Renamed := filepath.Join(sharedTmpPath, "u1001_renamed.txt")
		codeRename1001, _, errRename1001 := runMultiUserHelperCmd(t, 1001, 1001, "rename", u1000File, u1001Renamed)
		require.True(t, codeRename1001 == 11 || codeRename1001 == 10, "非属主重命名 Sticky Bit 目录下文件必须被拦截: %s (code: %d)", errRename1001, codeRename1001)

		// uid=1000 自己可以正常删除 u1000.txt
		codeRemove1000, _, errRemove1000 := runMultiUserHelperCmd(t, 1000, 1000, "remove", u1000File)
		require.Equal(t, 0, codeRemove1000, "属主删除自己创建的文件必须成功: %s", errRemove1000)
		_, err = os.Stat(u1000File)
		require.True(t, os.IsNotExist(err), "删除后文件必须不再存在")
	})

	// 场景 4：全套 bowl umount 卸载与零守护进程泄漏门禁
	t.Run("Scenario4_UmountAndZeroDaemonLeak", func(t *testing.T) {
		// 4.1 执行 bowl umount <mpA>，断言退出码 0
		umountCmdA := exec.Command(getBowlBin(), "umount", mpA)
		outUmountA, err := umountCmdA.CombinedOutput()
		require.NoError(t, err, "卸载 mpA 失败: %s", string(outUmountA))

		// 4.2 执行 bowl umount <mpB>，断言退出码 0
		umountCmdB := exec.Command(getBowlBin(), "umount", mpB)
		outUmountB, err := umountCmdB.CombinedOutput()
		require.NoError(t, err, "卸载 mpB 失败: %s", string(outUmountB))

		// 4.3 校验 /proc/self/mountinfo 中对应挂载记录已彻底清空
		require.NoError(t, waitForUnmount(mpA, 5*time.Second), "mpA 在 mountinfo 中仍有残留")
		require.NoError(t, waitForUnmount(mpB, 5*time.Second), "mpB 在 mountinfo 中仍有残留")

		// 4.4 等待两个后台守护进程完全退出
		require.NoError(t, waitForDaemonProcessExit(t, pidA, 5*time.Second), "客户端 A 守护进程未正常退出")
		require.NoError(t, waitForDaemonProcessExit(t, pidB, 5*time.Second), "客户端 B 守护进程未正常退出")

		// 4.5 扫描 /proc/<pid>/cmdline 断言没有任何指向本次测试 diskID 或挂载点的后台 bowl 守护进程残留
		assertNoBowlDaemonResiduals(t, diskID, mpA, mpB)
	})
}
