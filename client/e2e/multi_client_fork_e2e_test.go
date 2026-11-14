//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type forkDiskJSON struct {
	DiskID       string `json:"disk_id"`
	ParentDiskID string `json:"parent_disk_id"`
	Name         string `json:"name"`
	MaxBytes     uint64 `json:"max_bytes"`
	UsedBytes    uint64 `json:"used_bytes"`
}

// startIsolatedForkTrigger 在打开待测未刷句柄前启动独立子进程触发 `bowl disk fork`，
// 避免当前测试进程 `fork+exec` 时触发内核 `do_close_on_exec` 提前冲刷聚合槽。
func startIsolatedForkTrigger(t *testing.T, client storagev1connect.StorageServiceClient) func(sourceDiskID, childName string) forkDiskJSON {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), mountProcessTimeout)
	t.Cleanup(cancel)

	const pyDriver = `import json, subprocess, sys
for line in sys.stdin:
    req = json.loads(line)
    p = subprocess.run(req["argv"], capture_output=True, text=True)
    sys.stdout.write(json.dumps({"code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}) + "\n")
    sys.stdout.flush()
`
	cmd := exec.CommandContext(ctx, "python3", "-u", "-c", pyDriver)
	stdinPipe, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdoutPipe, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdinPipe.Close()
		_ = cmd.Wait()
	})
	reader := bufio.NewReader(stdoutPipe)

	return func(sourceDiskID, childName string) forkDiskJSON {
		t.Helper()
		argv := []string{
			getBowlBin(), "disk", "fork", sourceDiskID,
			"--endpoint", getStorageEndpoint(),
			"--name", childName,
			"--json",
		}
		reqBytes, mErr := json.Marshal(map[string]any{"argv": argv})
		require.NoError(t, mErr)
		_, wErr := stdinPipe.Write(append(reqBytes, '\n'))
		require.NoError(t, wErr)

		respLine, rErr := reader.ReadBytes('\n')
		require.NoError(t, rErr, "隔离进程执行 `bowl disk fork` 失败: %s", stderrBuf.String())
		var res struct {
			Code   int    `json:"code"`
			Stdout string `json:"stdout"`
			Stderr string `json:"stderr"`
		}
		require.NoError(t, json.Unmarshal(respLine, &res))
		require.Zero(t, res.Code, "`bowl disk fork` 失败: stdout=%s stderr=%s", res.Stdout, res.Stderr)

		var forked forkDiskJSON
		require.NoError(t, json.Unmarshal([]byte(res.Stdout), &forked), "解析 fork JSON 失败: %s", res.Stdout)
		require.NotEmpty(t, forked.DiskID)
		require.Equal(t, sourceDiskID, forked.ParentDiskID)
		registerDiskCleanup(t, client, forked.DiskID)
		return forked
	}
}

func forkOnlineDisk(t *testing.T, client storagev1connect.StorageServiceClient, sourceDiskID, childName string) forkDiskJSON {
	t.Helper()
	out := runBowlSuccess(t, "disk", "fork", sourceDiskID, "--endpoint", getStorageEndpoint(), "--name", childName, "--json")
	var forked forkDiskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &forked))
	require.NotEmpty(t, forked.DiskID)
	require.Equal(t, sourceDiskID, forked.ParentDiskID)
	registerDiskCleanup(t, client, forked.DiskID)
	return forked
}

func writeStaged32KiBChunks(t *testing.T, f *os.File, payload []byte) {
	t.Helper()
	const step = 32 * 1024
	for off := 0; off < len(payload); off += step {
		written, err := f.Write(payload[off : off+step])
		require.NoError(t, err)
		require.Equal(t, step, written)
	}
}

func runGitCmd(t *testing.T, repoDir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoDir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "执行 `git %v` 失败 (dir=%s): %s", args, repoDir, string(out))
	return string(out)
}

func expectedGitStepContent(commitNum int) string {
	payload := hex.EncodeToString(deterministicPayload("git-step-"+strconv.Itoa(commitNum), 512))
	return fmt.Sprintf("commit=%d\nsha256=%s\npayload=%s\n", commitNum, sha256Hex([]byte(payload)), payload)
}

func expectedGitTrackedContent(slot, commitNum int) string {
	payload := hex.EncodeToString(deterministicPayload(fmt.Sprintf("git-tracked-%d-%d", slot, commitNum), 512))
	return fmt.Sprintf("slot=%d\nupdated_at_commit=%d\npayload=%s\n", slot, commitNum, payload)
}

type gitForkSnap struct {
	child           forkDiskJSON
	committedBefore int64
}

func seedGitRepo(t *testing.T, repoDir string, trackedSlots int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "tracked"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "history"), 0o755))
	runGitCmd(t, repoDir, "init", "-b", "main")
	runGitCmd(t, repoDir, "config", "user.name", "Bowl E2E")
	runGitCmd(t, repoDir, "config", "user.email", "e2e@bowl.local")
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# Bowl Online Fork Git E2E\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "state.json"), []byte("{\"commit\":0}\n"), 0o644))
	for s := range trackedSlots {
		require.NoError(t, os.WriteFile(
			filepath.Join(repoDir, "tracked", fmt.Sprintf("mod_%d.txt", s)),
			[]byte(expectedGitTrackedContent(s, 0)),
			0o644,
		))
	}
	runGitCmd(t, repoDir, "add", "-A")
	runGitCmd(t, repoDir, "commit", "-m", "commit-0")
}

func runGitCommitWorker(repoDir string, totalCommits, trackedSlots int, lastCommittedN *atomic.Int64) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		for n := 1; n <= totalCommits; n++ {
			slot := n % trackedSlots
			if err := os.WriteFile(
				filepath.Join(repoDir, "tracked", fmt.Sprintf("mod_%d.txt", slot)),
				[]byte(expectedGitTrackedContent(slot, n)),
				0o644,
			); err != nil {
				errCh <- err
				return
			}
			if err := os.WriteFile(
				filepath.Join(repoDir, "history", fmt.Sprintf("step_%02d.txt", n)),
				[]byte(expectedGitStepContent(n)),
				0o644,
			); err != nil {
				errCh <- err
				return
			}
			if err := os.WriteFile(filepath.Join(repoDir, "state.json"), fmt.Appendf(nil, "{\"commit\":%d}\n", n), 0o644); err != nil {
				errCh <- err
				return
			}
			addCtx, addCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
			addOut, addErr := exec.CommandContext(addCtx, "git", "-C", repoDir, "add", "-A").CombinedOutput()
			addCancel()
			if addErr != nil {
				errCh <- fmt.Errorf("git add commit-%d 失败: %w (%s)", n, addErr, string(addOut))
				return
			}
			cCtx, cCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
			cOut, cErr := exec.CommandContext(cCtx, "git", "-C", repoDir, "commit", "-m", fmt.Sprintf("commit-%d", n)).CombinedOutput()
			cCancel()
			if cErr != nil {
				errCh <- fmt.Errorf("git commit-%d 失败: %w (%s)", n, cErr, string(cOut))
				return
			}
			lastCommittedN.Store(int64(n))
		}
		errCh <- nil
	}()
	return errCh
}

func captureGitForks(t *testing.T, client storagev1connect.StorageServiceClient, parentID string, lastCommittedN *atomic.Int64, thresholds []int64) []gitForkSnap {
	t.Helper()
	snaps := make([]gitForkSnap, 0, len(thresholds))
	for i, minCommit := range thresholds {
		require.Eventually(t, func() bool {
			return lastCommittedN.Load() >= minCommit
		}, 30*time.Second, 10*time.Millisecond, "等待 Git 提交推进至 commit-%d 超时", minCommit)
		before := lastCommittedN.Load()
		child := forkOnlineDisk(t, client, parentID, fmt.Sprintf("e2e-git-fork-child-%d", i+1))
		snaps = append(snaps, gitForkSnap{child: child, committedBefore: before})
	}
	return snaps
}

func verifyGitChildForks(t *testing.T, endpoint string, snaps []gitForkSnap, trackedSlots int) {
	t.Helper()
	for idx, snap := range snaps {
		childMnt, stopChild := mountWithCleanup(t, endpoint, snap.child.DiskID)
		childRepo := filepath.Join(childMnt, "repo")

		require.NoError(t, filepath.WalkDir(filepath.Join(childRepo, ".git"), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(d.Name(), ".lock") {
				return os.Remove(p)
			}
			return nil
		}))

		runGitCmd(t, childRepo, "reset", "--hard", "HEAD")
		runGitCmd(t, childRepo, "clean", "-fd")

		fsckOut := strings.ToLower(runGitCmd(t, childRepo, "fsck", "--full"))
		require.NotContains(t, fsckOut, "error:")
		require.NotContains(t, fsckOut, "fatal:")
		require.NotContains(t, fsckOut, "corrupt")
		require.NotContains(t, fsckOut, "missing")

		revCountStr := strings.TrimSpace(runGitCmd(t, childRepo, "rev-list", "--count", "HEAD"))
		revCount, err := strconv.ParseInt(revCountStr, 10, 64)
		require.NoError(t, err)
		require.GreaterOrEqual(t, revCount, snap.committedBefore+1)

		headSubject := strings.TrimSpace(runGitCmd(t, childRepo, "log", "-1", "--format=%s", "HEAD"))
		headNStr, ok := strings.CutPrefix(headSubject, "commit-")
		require.True(t, ok, "未预期的 HEAD 提交标题: %s", headSubject)
		headN, err := strconv.Atoi(headNStr)
		require.NoError(t, err)

		for step := 1; step <= headN; step++ {
			stepRaw, sErr := os.ReadFile(filepath.Join(childRepo, "history", fmt.Sprintf("step_%02d.txt", step)))
			require.NoError(t, sErr)
			require.Equal(t, expectedGitStepContent(step), string(stepRaw))
		}
		for s := range trackedSlots {
			lastModCommit := 0
			for c := headN; c >= 1; c-- {
				if c%trackedSlots == s {
					lastModCommit = c
					break
				}
			}
			trackedRaw, tErr := os.ReadFile(filepath.Join(childRepo, "tracked", fmt.Sprintf("mod_%d.txt", s)))
			require.NoError(t, tErr)
			require.Equal(t, expectedGitTrackedContent(s, lastModCommit), string(trackedRaw))
		}

		childBranchFile := filepath.Join(childRepo, fmt.Sprintf("child_fork_%d.txt", idx+1))
		require.NoError(t, os.WriteFile(childBranchFile, []byte("child-diverged-commit\n"), 0o644))
		runGitCmd(t, childRepo, "add", filepath.Base(childBranchFile))
		runGitCmd(t, childRepo, "commit", "-m", fmt.Sprintf("child-%d-commit", idx+1))
		afterRevCount := strings.TrimSpace(runGitCmd(t, childRepo, "rev-list", "--count", "HEAD"))
		require.Equal(t, strconv.Itoa(headN+2), afterRevCount)
		stopChild()
	}
}

// TestMultiClient_ReadOnlyAndWriterEpochPreemption 验证只读并发挂载、
// 新可写客户端零等待抢占 Epoch 围栏、旧已打开句柄被拦截至 `EBUSY` 及前任孤儿自动清扫。
func TestMultiClient_ReadOnlyAndWriterEpochPreemption(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()

	// 参数门禁：--ro 与 --force 冲突必须在 fork 守护进程前即以退出码 2 失败
	roForceOut := runBowlExpectExit(t, 2, "mount", "--endpoint", endpoint, diskID, t.TempDir(), "--ro", "--force")
	require.Contains(t, roForceOut, "--ro 与 --force 不能同时使用")

	// Given: 客户端 A 以后台守护进程挂载并写入文件、打开预持句柄与已解链孤儿句柄
	mpA := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, diskID, mpA)
	waitForMount(t, mpA)
	daemonAPID := findBowlDaemonPID(t, mpA)
	t.Cleanup(func() {
		_ = exec.Command("fusermount3", "-uz", mpA).Run()
		if syscall.Kill(daemonAPID, 0) == nil {
			_ = exec.Command(getBowlBin(), "umount", "-f", mpA).Run()
		}
	})

	snapPath := filepath.Join(mpA, "snapshot.txt")
	require.NoError(t, os.WriteFile(snapPath, []byte("v1"), 0o644))

	// When: 两路只读客户端并发挂载，写操作返回 EROFS，且可实时读到客户端 A 新写入内容
	mpRO1, stopRO1 := mountWithCleanup(t, endpoint, diskID, "--ro", "--direct-io")
	mpRO2, stopRO2 := mountWithCleanup(t, endpoint, diskID, "--ro", "--direct-io")
	for _, roMP := range []string{mpRO1, mpRO2} {
		roData, err := os.ReadFile(filepath.Join(roMP, "snapshot.txt"))
		require.NoError(t, err)
		require.Equal(t, []byte("v1"), roData)
		require.ErrorIs(t, os.WriteFile(filepath.Join(roMP, "denied.txt"), []byte("x"), 0o644), syscall.EROFS)
	}

	require.NoError(t, os.WriteFile(filepath.Join(mpA, "live.txt"), []byte("live-from-A"), 0o644))
	for _, roMP := range []string{mpRO1, mpRO2} {
		liveRO, err := os.ReadFile(filepath.Join(roMP, "live.txt"))
		require.NoError(t, err)
		require.Equal(t, []byte("live-from-A"), liveRO)
	}
	stopRO1()
	stopRO2()

	preOpenFile, err := os.OpenFile(filepath.Join(mpA, "preopened.txt"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	orphanPath := filepath.Join(mpA, "orphan.bin")
	orphanFile, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	closeOpenFiles := func() {
		if preOpenFile != nil {
			_ = preOpenFile.Close()
			preOpenFile = nil
		}
		if orphanFile != nil {
			_ = orphanFile.Close()
			orphanFile = nil
		}
	}
	t.Cleanup(closeOpenFiles)

	_, err = preOpenFile.WriteString("before-takeover")
	require.NoError(t, err)
	require.NoError(t, preOpenFile.Sync())

	orphanPayload := bytes.Repeat([]byte{'O'}, 64*1024)
	_, err = orphanFile.Write(orphanPayload)
	require.NoError(t, err)
	require.NoError(t, orphanFile.Sync())

	var orphanStat unix.Stat_t
	require.NoError(t, unix.Fstat(int(orphanFile.Fd()), &orphanStat))
	orphanIno := orphanStat.Ino
	require.NoError(t, os.Remove(orphanPath))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	diskBefore, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	usedBefore := diskBefore.Msg.GetDisk().GetUsedBytes()

	// 未传 --force 的第二个 RW 挂载必须被未过期活跃写租约拦截并返回单行明确错误
	conflictMP := t.TempDir()
	conflictOut := runBowlExpectExit(t, 1, "mount", "--endpoint", endpoint, diskID, conflictMP)
	require.Contains(t, conflictOut, "存在未过期的活跃读写挂载")
	require.Contains(t, conflictOut, "--force")
	require.False(t, isMounted(conflictMP))

	// When: 客户端 B 传入 --force 零等待可写挂载抢占 Epoch
	takeoverStart := time.Now()
	mpB, stopB := mountWithCleanup(t, endpoint, diskID, "--force")
	require.Less(t, time.Since(takeoverStart), 3*time.Second, "新可写挂载必须零等待立即接管")

	// Then: 前任孤儿被清扫释放配额，A 的新写入及旧句柄写/截断/同步均被围栏拦截返回 EBUSY
	_, orphanAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: orphanIno}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(orphanAttrErr))

	diskAfter, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Less(t, diskAfter.Msg.GetDisk().GetUsedBytes(), usedBefore)

	require.ErrorIs(t, os.Mkdir(filepath.Join(mpA, "fenced-dir"), 0o755), syscall.EBUSY)
	require.ErrorIs(t, preOpenFile.Truncate(0), syscall.EBUSY)
	_, _ = preOpenFile.WriteAt([]byte("stale"), 0)
	require.ErrorIs(t, preOpenFile.Sync(), syscall.EBUSY)

	require.NoError(t, os.WriteFile(filepath.Join(mpB, "taken-over.txt"), []byte("leader-B"), 0o644))
	gotPreOpen, err := os.ReadFile(filepath.Join(mpB, "preopened.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("before-takeover"), gotPreOpen)
	stopB()

	// 启动忙碌进程占住 mpA；等 A 的 RenewMount 收到终态失败后应触发自动卸载并回退懒卸载
	cleanupOccupier := startMountBusyOccupier(t, mpA)
	t.Cleanup(cleanupOccupier)

	require.Eventually(t, func() bool {
		return !isMounted(mpA)
	}, 20*time.Second, 100*time.Millisecond, "旧写者续期收到终态拒绝后必须自动触发懒卸载")

	// 趁旧进程仍被打开句柄与忙碌进程挂起时，在同一挂载点重挂其他磁盘并验证 mountMeta PID 归属保护
	disk2 := createTestDisk(t, client)
	runBowlSuccess(t, "mount", "--endpoint", endpoint, disk2.GetDiskId(), mpA)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mpA).Run() })
	waitForMount(t, mpA)

	metaFile := mountMetaPath(t, mpA)
	readMeta := func() (pid int, gotDiskID string) {
		data, rErr := os.ReadFile(metaFile)
		require.NoError(t, rErr)
		var m struct {
			PID    int    `json:"pid"`
			DiskID string `json:"disk_id"`
		}
		require.NoError(t, json.Unmarshal(data, &m))
		return m.PID, m.DiskID
	}
	newPID, gotDisk2ID := readMeta()
	require.NotEqual(t, daemonAPID, newPID)
	require.Equal(t, disk2.GetDiskId(), gotDisk2ID)

	// 释放旧占用句柄与忙碌进程，旧守护进程 A 必须正常退出且不得误删新挂载的 mountMeta
	closeOpenFiles()
	cleanupOccupier()
	waitForProcessExit(t, daemonAPID)

	require.True(t, isMounted(mpA))
	afterPID, afterDiskID := readMeta()
	require.Equal(t, newPID, afterPID, "旧守护进程退出时不得误删同路径新挂载的 mountMeta")
	require.Equal(t, disk2.GetDiskId(), afterDiskID)
	require.NoError(t, os.WriteFile(filepath.Join(mpA, "remounted.txt"), []byte("ok"), 0o644))

	runBowlSuccess(t, "umount", mpA)
	waitForProcessExit(t, newPID)
}

// TestOnlineFork_DualMountIsolationAndParentDelete 验证在线挂载期间触发 `bowl disk fork` 的内存缓冲隔离、
// 孤儿排除、父子双挂载并发独立修改，以及卸载并删除父盘后子盘共享数据块依然完整可读可写。
func TestOnlineFork_DualMountIsolationAndParentDelete(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	// Given: 创建并挂载父盘，同时预先启动隔离触发进程防止 exec 提前冲刷聚合槽
	createOut := runBowlSuccess(t, "disk", "create", "--endpoint", endpoint, "--size", "2GiB", "--name", "e2e-fork-parent", "--json")
	var parentDisk forkDiskJSON
	require.NoError(t, json.Unmarshal([]byte(createOut), &parentDisk))
	parentID := parentDisk.DiskID
	registerDiskCleanup(t, client, parentID)

	parentMnt := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, parentID, parentMnt)
	parentPID := findBowlDaemonPID(t, parentMnt)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", parentMnt).Run() })

	triggerIsolatedFork := startIsolatedForkTrigger(t, client)

	require.NoError(t, os.MkdirAll(filepath.Join(parentMnt, "app"), 0o755))
	baseCfg := []byte("{\"env\":\"staging\"}\n")
	require.NoError(t, os.WriteFile(filepath.Join(parentMnt, "app", "config.json"), baseCfg, 0o644))

	largePayload := deterministicPayload("fork-shared-512k", 512*1024)
	largeF, err := os.OpenFile(filepath.Join(parentMnt, "large.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = largeF.Write(largePayload)
	require.NoError(t, err)
	require.NoError(t, largeF.Sync())
	require.NoError(t, largeF.Close())

	orphanPath := filepath.Join(parentMnt, "orphan_unlinked.bin")
	orphanF, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = orphanF.Close() })
	_, err = orphanF.Write(deterministicPayload("fork-orphan", 1024*1024))
	require.NoError(t, err)
	require.NoError(t, os.Remove(orphanPath))

	stagedData := deterministicPayload("fork-staged-256k", 256*1024)
	stagedF, err := os.OpenFile(filepath.Join(parentMnt, "staged_unflushed.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stagedF.Close() })
	writeStaged32KiBChunks(t, stagedF, stagedData)

	// When: 保持未刷句柄与孤儿打开期间触发在线 Fork，并同时挂载子盘
	childDisk := triggerIsolatedFork(parentID, "e2e-fork-child")
	childID := childDisk.DiskID
	require.Equal(t, uint64(len(baseCfg)+len(largePayload)), childDisk.UsedBytes,
		"子盘 `used_bytes` 必须仅包含已落盘存活文件，排除孤儿与未刷阶段缓冲")

	childMnt, stopChild := mountWithCleanup(t, endpoint, childID, "--direct-io")

	// Then: 子盘继承已落盘数据，不含孤儿且未刷文件为 0 B；关闭父盘未刷句柄不穿透子盘
	gotChildLarge, err := os.ReadFile(filepath.Join(childMnt, "large.bin"))
	require.NoError(t, err)
	require.Equal(t, largePayload, gotChildLarge)

	_, err = os.Stat(filepath.Join(childMnt, "orphan_unlinked.bin"))
	require.ErrorIs(t, err, os.ErrNotExist)
	stagedChildInfo, err := os.Stat(filepath.Join(childMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Zero(t, stagedChildInfo.Size())

	require.NoError(t, stagedF.Close())
	gotParentStaged, err := os.ReadFile(filepath.Join(parentMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Equal(t, stagedData, gotParentStaged)
	gotChildStagedAfter, err := os.ReadFile(filepath.Join(childMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Empty(t, gotChildStagedAfter)

	// When: 父子双挂载点各自覆写同路径与新建独有文件，随后卸载并删除父盘
	parentCfg := []byte("{\"env\":\"production\"}\n")
	childCfg := []byte("{\"env\":\"experiment\"}\n")
	require.NoError(t, os.WriteFile(filepath.Join(parentMnt, "app", "config.json"), parentCfg, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "app", "config.json"), childCfg, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "only_child.txt"), []byte("child-only"), 0o644))

	_, err = os.Stat(filepath.Join(parentMnt, "only_child.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, orphanF.Close())
	runBowlSuccess(t, "umount", parentMnt)
	waitForUnmount(t, parentMnt)
	waitForProcessExit(t, parentPID)
	runBowlSuccess(t, "disk", "delete", "--endpoint", endpoint, parentID, "-y")

	// Then: 删除父盘后子盘共享块依然完好可读且可继续写入
	gotChildAfterParentDel, err := os.ReadFile(filepath.Join(childMnt, "large.bin"))
	require.NoError(t, err)
	require.Equal(t, largePayload, gotChildAfterParentDel)
	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "after_parent_del.txt"), []byte("alive"), 0o644))
	stopChild()
}

// TestOnlineFork_RealGitCommitLoop 在真实 `git add/commit` 提交循环期间并发触发 3 次在线 `bowl disk fork`，
// 验证子盘对象库零损坏、提交计数单调不退且可继续分叉提交。
func TestOnlineFork_RealGitCommitLoop(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	createOut := runBowlSuccess(t, "disk", "create", "--endpoint", endpoint, "--size", "2GiB", "--name", "e2e-git-fork-parent", "--json")
	var parentDisk forkDiskJSON
	require.NoError(t, json.Unmarshal([]byte(createOut), &parentDisk))
	parentID := parentDisk.DiskID
	registerDiskCleanup(t, client, parentID)

	parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
	repoDir := filepath.Join(parentMnt, "repo")
	const trackedSlots = 5
	const totalCommits = 20
	seedGitRepo(t, repoDir, trackedSlots)

	var lastCommittedN atomic.Int64
	workerErrCh := runGitCommitWorker(repoDir, totalCommits, trackedSlots, &lastCommittedN)
	snaps := captureGitForks(t, client, parentID, &lastCommittedN, []int64{3, 9, 15})
	require.NoError(t, <-workerErrCh, "父盘 Git 连续提交循环失败")
	require.Equal(t, int64(totalCommits), lastCommittedN.Load())

	verifyGitChildForks(t, endpoint, snaps, trackedSlots)

	parentFsck := strings.ToLower(runGitCmd(t, repoDir, "fsck", "--full"))
	require.NotContains(t, parentFsck, "error:")
	require.NotContains(t, parentFsck, "corrupt")
	require.Equal(t, strconv.Itoa(totalCommits+1), strings.TrimSpace(runGitCmd(t, repoDir, "rev-list", "--count", "HEAD")))
	_, childLeakErr := os.Stat(filepath.Join(repoDir, "child_fork_1.txt"))
	require.ErrorIs(t, childLeakErr, os.ErrNotExist)
}
