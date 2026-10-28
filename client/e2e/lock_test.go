//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const lockHelperEnv = "BOWL_TEST_LOCK_HELPER"

func init() {
	if os.Getenv(lockHelperEnv) == "1" {
		runLockHelper()
		os.Exit(0)
	}
}

func runLockHelper() {
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

	// 通知父进程：子进程已打开文件并准备阻塞加锁
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

func TestE2E_Lock(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	disk := createDiskWithRootInoGT1(t, client)

	mpA := t.TempDir()
	mpB := t.TempDir()

	cmdA, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mpA, "--debug")
	require.NoError(t, waitForMount(mpA, 10*time.Second), "挂载点 A 就绪")
	defer stopBowlMountGracefully(t, cmdA, mpA, 5*time.Second)

	cmdB, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mpB, "--debug")
	require.NoError(t, waitForMount(mpB, 10*time.Second), "挂载点 B 就绪")
	defer stopBowlMountGracefully(t, cmdB, mpB, 5*time.Second)

	createFile := func(path string) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	// 1. 同一进程 dup(fd) 后关闭任一重复 fd（触发 Flush 携带 lock_owner）立即释放全部 POSIX 记录锁
	t.Run("dup_fd_close_releases_posix_locks_on_flush", func(t *testing.T) {
		fileName := "posix-dup.txt"
		pathA := filepath.Join(mpA, fileName)
		pathB := filepath.Join(mpB, fileName)

		createFile(pathA)

		fA1, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fA1 != nil {
				_ = fA1.Close()
			}
		}()

		// A 端持有排他 POSIX 写锁
		flWr := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fA1.Fd(), unix.F_SETLK, &flWr), "A1 获锁必须成功")

		fB, err := os.OpenFile(pathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fB.Close()

		// B 端尝试非阻塞加写锁必须冲突 (EAGAIN)
		errConflict := unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flWr)
		require.ErrorIs(t, errConflict, syscall.EAGAIN, "A 端持锁时 B 端加锁必须返回 EAGAIN")

		// A 端通过 dup 创建同进程重复 fd
		dupFd, err := unix.Dup(int(fA1.Fd()))
		require.NoError(t, err)

		// POSIX 语义：关闭任一同一进程持有该文件的 fd，触发 Flush 并释放该进程所有 POSIX 锁
		require.NoError(t, unix.Close(dupFd))

		// 验证 B 端能立即获得写锁
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flWr), "dupFd 关闭触发 Flush 后 B 端必须能立即获写锁")

		// 释放 B 端锁
		flUn := unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		_ = unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flUn)
	})

	// 2. BSD unix.Flock 在 dup(fd) 后关闭一个 fd 不会释放锁，仅在最后一个 fd 关闭（Release）后才释放锁
	t.Run("bsd_flock_preserved_across_dup_until_last_close", func(t *testing.T) {
		fileName := "flock-dup.txt"
		pathA := filepath.Join(mpA, fileName)
		pathB := filepath.Join(mpB, fileName)

		createFile(pathA)

		fA, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fA != nil {
				_ = fA.Close()
			}
		}()

		// A 端获取 BSD 排他锁
		require.NoError(t, unix.Flock(int(fA.Fd()), unix.LOCK_EX|unix.LOCK_NB), "A 获排他 flock 必须成功")

		fB, err := os.OpenFile(pathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fB.Close()

		// B 端尝试获取 flock 必须冲突 (EAGAIN)
		errConflict := unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		require.ErrorIs(t, errConflict, syscall.EAGAIN, "A 端持 flock 时 B 端必须返回 EAGAIN")

		// A 端 dup(fd)
		dupFd, err := unix.Dup(int(fA.Fd()))
		require.NoError(t, err)

		// 关闭 dupFd，BSD flock 绝不释放
		require.NoError(t, unix.Close(dupFd))

		errStillConflict := unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		require.ErrorIs(t, errStillConflict, syscall.EAGAIN, "dupFd 关闭后原始句柄未关，flock 必须依然互斥")

		// 关闭最后一个活跃句柄 fA，触发 Release 并释放 flock 锁
		require.NoError(t, fA.Close())
		fA = nil

		// B 端现在能够成功获取 flock 锁
		require.NoError(t, unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB), "最后一个句柄关闭后 B 端必须能获取 flock")
		_ = unix.Flock(int(fB.Fd()), unix.LOCK_UN)
	})

	// 3. POSIX 区间裂解合并、F_GETLK 冲突探测、非阻塞 F_SETLK 返回 syscall.EAGAIN
	t.Run("posix_range_split_merge_getlk_and_setlk_eagain", func(t *testing.T) {
		fileName := "posix-ranges.txt"
		pathA := filepath.Join(mpA, fileName)
		pathB := filepath.Join(mpB, fileName)

		createFile(pathA)

		fA, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fA.Close()

		fB, err := os.OpenFile(pathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fB.Close()

		// A 端锁定区间 [0, 100]
		fl1 := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    100,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &fl1))

		// A 端连续锁定区间 [100, 100]，在服务端自动合并为 [0, 200]
		fl2 := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  100,
			Len:    100,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &fl2))

		// B 端通过 F_GETLK 探测重叠区间 [50, 50]，预期探测到写锁冲突
		flProbe := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  50,
			Len:    50,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_GETLK, &flProbe))
		require.Equal(t, int16(unix.F_WRLCK), flProbe.Type, "探测到冲突写锁")
		require.Equal(t, int64(0), flProbe.Start, "合并后的起始偏移为 0")

		// B 端通过 F_GETLK 探测无重叠区间 [500, 100]，预期返回 F_UNLCK
		flProbeNoConflict := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  500,
			Len:    100,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_GETLK, &flProbeNoConflict))
		require.Equal(t, int16(unix.F_UNLCK), flProbeNoConflict.Type, "无重叠区间应返回 F_UNLCK")

		// B 端非阻塞加锁冲突区间返回 EAGAIN
		flConflict := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  50,
			Len:    50,
		}
		require.ErrorIs(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flConflict), syscall.EAGAIN)

		// A 端释放中间空洞 [40, 20]（即 [40, 59]），使得区间裂解为 [0, 39] 与 [60, 199]
		flHole := unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: 0,
			Start:  40,
			Len:    20,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &flHole))

		// B 端现在能够成功锁定该空洞区间 [40, 20]
		flFill := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  40,
			Len:    20,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flFill), "B 端加锁空洞区间必须成功")

		// 清理 B 端锁
		flFill.Type = unix.F_UNLCK
		_ = unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flFill)
	})

	// 4. 双主体各持共享读锁后相继阻塞申请升级写锁触发服务端死锁环检测返回 syscall.EDEADLK
	t.Run("deadlock_detection_edeadlk", func(t *testing.T) {
		fileName := "posix-deadlock.txt"
		pathA := filepath.Join(mpA, fileName)
		pathB := filepath.Join(mpB, fileName)

		createFile(pathA)

		fA, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fA.Close()

		fB, err := os.OpenFile(pathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fB.Close()

		// A 端获取整文件共享读锁
		flRd := unix.Flock_t{
			Type:   unix.F_RDLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &flRd), "A 获读锁成功")

		// B 端也获取整文件共享读锁
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flRd), "B 获读锁成功")

		// A 端发起阻塞升级写锁 (F_SETLKW)，由于 B 仍持有读锁，A 进入阻塞等待
		errACh := make(chan error, 1)
		go func() {
			flWr := unix.Flock_t{
				Type:   unix.F_WRLCK,
				Whence: 0,
				Start:  0,
				Len:    0,
			}
			errACh <- unix.FcntlFlock(fA.Fd(), unix.F_SETLKW, &flWr)
		}()

		// 确保 A 请求已入队并处于等待状态
		time.Sleep(150 * time.Millisecond)

		// B 端也发起阻塞升级写锁 (F_SETLKW)，形成双向依赖死锁环：A 等 B，B 等 A
		flWrB := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		errB := unix.FcntlFlock(fB.Fd(), unix.F_SETLKW, &flWrB)
		require.ErrorIs(t, errB, syscall.EDEADLK, "形成等待环时服务端必须精确拒绝并返回 EDEADLK")

		// B 端释放其读锁，打破僵局，使 A 端顺利完成升级
		flUn := unix.Flock_t{
			Type:   unix.F_UNLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fB.Fd(), unix.F_SETLK, &flUn))

		select {
		case errA := <-errACh:
			require.NoError(t, errA, "B 释放读锁后 A 端必须成功升级写锁")
		case <-time.After(5 * time.Second):
			t.Fatal("A 端在 B 释放锁后未在 5s 内唤醒完成升级")
		}
	})

	// 5. 子进程阻塞在 fcntl(F_SETLKW) 时向该子进程发送 SIGUSR1 信号，断言系统调用立即返回 EINTR 且未遗留悬挂锁
	t.Run("blocking_setlkw_signal_interrupt_eintr", func(t *testing.T) {
		fileName := "posix-interrupt.txt"
		pathA := filepath.Join(mpA, fileName)
		pathB := filepath.Join(mpB, fileName)

		createFile(pathA)

		fA, err := os.OpenFile(pathA, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() {
			if fA != nil {
				_ = fA.Close()
			}
		}()

		// A 端持有整文件写锁
		flWr := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fA.Fd(), unix.F_SETLK, &flWr), "A 端持写锁成功")

		// 派生独立子进程，在 B 端挂载点发起阻塞写锁 F_SETLKW
		cmd := exec.Command(os.Args[0], "-test.run=TestE2E_Lock", "--")
		cmd.Env = append(os.Environ(), lockHelperEnv+"=1", "BOWL_TEST_LOCK_TARGET="+pathB)

		stdoutPipe, err := cmd.StdoutPipe()
		require.NoError(t, err)
		stderrPipe, err := cmd.StderrPipe()
		require.NoError(t, err)

		require.NoError(t, cmd.Start(), "启动锁辅助子进程")

		reader := bufio.NewReader(stdoutPipe)
		line, err := reader.ReadString('\n')
		require.NoError(t, err, "读取子进程就绪输出")
		require.Equal(t, "READY_TO_LOCK\n", line)

		// 稍作等待，确保子进程已发起 SetLkw RPC 并在服务端数据库进入 waiters 阻塞排队状态
		time.Sleep(200 * time.Millisecond)

		// 向子进程发送 SIGUSR1
		require.NoError(t, cmd.Process.Signal(syscall.SIGUSR1), "向阻塞子进程发送 SIGUSR1")

		line2, err := reader.ReadString('\n')
		require.NoError(t, err, "读取子进程中断响应")
		require.Equal(t, "INTERRUPTED_EINTR\n", line2, "系统调用必须立即返回 EINTR")

		stderrScanner := bufio.NewScanner(stderrPipe)
		if cmd.Wait() != nil {
			var stderrOut string
			for stderrScanner.Scan() {
				stderrOut += stderrScanner.Text() + "\n"
			}
			t.Fatalf("子进程异常退出: %s", stderrOut)
		}

		// A 端释放主写锁
		require.NoError(t, fA.Close())
		fA = nil

		// 验证未残留悬挂锁：第三方在 B 端可立即获取排他写锁
		fC, err := os.OpenFile(pathB, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fC.Close()

		flProbe := unix.Flock_t{
			Type:   unix.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		require.NoError(t, unix.FcntlFlock(fC.Fd(), unix.F_SETLK, &flProbe), "主锁释放后第三方必须能立即获取写锁，证明无残留悬挂锁")
	})
}
