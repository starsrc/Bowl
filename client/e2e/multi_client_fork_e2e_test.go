//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
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

func TestMultiClient_ReadOnlyAndWriterEpochPreemption(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()

	mpA, _ := mountWithCleanup(t, endpoint, diskID)
	snapPath := filepath.Join(mpA, "snapshot.txt")
	require.NoError(t, os.WriteFile(snapPath, []byte("v1"), 0o644))

	mpRO, stopRO := mountWithCleanup(t, endpoint, diskID, "--mode", "ro", "--direct-io")
	require.Eventually(t, func() bool {
		roData, readErr := os.ReadFile(filepath.Join(mpRO, "snapshot.txt"))
		return readErr == nil && string(roData) == "v1"
	}, 5*time.Second, 20*time.Millisecond, "只读挂载必须读到客户端 A 写入的 snapshot.txt")
	require.ErrorIs(t, os.WriteFile(filepath.Join(mpRO, "denied.txt"), []byte("x"), 0o644), syscall.EROFS)

	require.NoError(t, os.WriteFile(filepath.Join(mpA, "live.txt"), []byte("live-from-A"), 0o644))
	require.Eventually(t, func() bool {
		liveRO, readErr := os.ReadFile(filepath.Join(mpRO, "live.txt"))
		return readErr == nil && string(liveRO) == "live-from-A"
	}, 5*time.Second, 20*time.Millisecond, "只读挂载必须读到客户端 A 异步上传的 live.txt")
	stopRO()

	preOpenFile, err := os.OpenFile(filepath.Join(mpA, "preopened.txt"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = preOpenFile.Close() })
	_, err = preOpenFile.WriteString("before-takeover")
	require.NoError(t, err)
	require.NoError(t, preOpenFile.Sync())

	orphanPath := filepath.Join(mpA, "orphan.bin")
	orphanFile, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = orphanFile.Close() })
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
	statBefore, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{DiskId: diskID, Ino: 1}))
	require.NoError(t, err)
	usedBefore := statBefore.Msg.GetUsedBytes()

	takeoverStart := time.Now()
	mpB, _ := mountWithCleanup(t, endpoint, diskID, "--force")
	require.Less(t, time.Since(takeoverStart), 3*time.Second, "新可写挂载必须零等待立即接管")

	_, orphanAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{DiskId: diskID, Ino: orphanIno}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(orphanAttrErr))

	statAfter, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{DiskId: diskID, Ino: 1}))
	require.NoError(t, err)
	require.Less(t, statAfter.Msg.GetUsedBytes(), usedBefore)

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
}

func TestOnlineFork_DualMountIsolationAndParentDelete(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

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
	cfgF, err := os.OpenFile(filepath.Join(parentMnt, "app", "config.json"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = cfgF.Write(baseCfg)
	require.NoError(t, err)
	require.NoError(t, cfgF.Sync())
	require.NoError(t, cfgF.Close())

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

	childDisk := triggerIsolatedFork(parentID, "e2e-fork-child")
	childID := childDisk.DiskID
	require.Equal(t, uint64(len(baseCfg)+len(largePayload)), childDisk.UsedBytes,
		"子盘 `used_bytes` 必须仅包含已落盘存活文件，排除孤儿与未刷阶段缓冲")

	childMnt, stopChild := mountWithCleanup(t, endpoint, childID, "--direct-io")

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

	gotChildAfterParentDel, err := os.ReadFile(filepath.Join(childMnt, "large.bin"))
	require.NoError(t, err)
	require.Equal(t, largePayload, gotChildAfterParentDel)
	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "after_parent_del.txt"), []byte("alive"), 0o644))
	stopChild()
}

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

func overwriteFileInPlace(t *testing.T, path string, data []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	written := 0
	for written < len(data) {
		n, wErr := f.Write(data[written:])
		require.NoError(t, wErr)
		written += n
	}
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}

func tryReadFromStartFD(f *os.File, chunkSize int) ([]byte, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, chunkSize)
	var out []byte
	for {
		n, err := f.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func TestMultiClient_ReadOnlyCacheCoherence(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()

	mpW, stopW := mountWithCleanup(t, endpoint, diskID)
	defer stopW()

	unchangedBytes := deterministicPayload("ro-unchanged", 256*1024)
	unchangedOpenBytes := deterministicPayload("ro-unchanged-open", 256*1024)
	smallV1 := []byte("small-v1-exact-len")
	smallV2 := []byte("small-v2-exact-len")
	smallOpenV1 := []byte("open-small-v1-payload")
	smallOpenV2 := []byte("open-small-v2-payload")
	appendBase := []byte("line-1\n")
	appendTail := []byte("line-2\n")
	const largeSize = 5 * 1024 * 1024
	largeV1 := deterministicPayload("ro-large-v1", largeSize)
	largeV2 := deterministicPayload("ro-large-v2", largeSize)
	largeOpenV1 := deterministicPayload("ro-large-open-v1", largeSize)
	largeOpenV2 := deterministicPayload("ro-large-open-v2", largeSize)
	directOpenV1 := deterministicPayload("ro-direct-open-v1", largeSize)
	directOpenV2 := deterministicPayload("ro-direct-open-v2", largeSize)

	writeFullFile(t, filepath.Join(mpW, "unchanged.bin"), unchangedBytes, 64*1024)
	writeFullFile(t, filepath.Join(mpW, "unchanged_open.bin"), unchangedOpenBytes, 64*1024)
	writeFullFile(t, filepath.Join(mpW, "small_reopen.txt"), smallV1, len(smallV1))
	writeFullFile(t, filepath.Join(mpW, "small_open.txt"), smallOpenV1, len(smallOpenV1))
	writeFullFile(t, filepath.Join(mpW, "append.log"), appendBase, len(appendBase))
	writeFullFile(t, filepath.Join(mpW, "to_unlink.txt"), []byte("gone-soon"), 9)
	writeFullFile(t, filepath.Join(mpW, "rename_src.txt"), []byte("moved-content"), 13)
	writeFullFile(t, filepath.Join(mpW, "large_reopen.bin"), largeV1, 512*1024)
	writeFullFile(t, filepath.Join(mpW, "large_open.bin"), largeOpenV1, 512*1024)
	writeFullFile(t, filepath.Join(mpW, "direct_open.bin"), directOpenV1, 512*1024)

	mpRO, _, roAdminAddr, stopRO := startMountedDaemon(t, endpoint, diskID, "--mode", "ro")
	defer stopRO()
	mpRODirect, stopRODirect := mountWithCleanup(t, endpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopRODirect()

	_, missingErr := os.Stat(filepath.Join(mpRO, "late_created.txt"))
	require.ErrorIs(t, missingErr, os.ErrNotExist)
	_, dirErr := os.ReadDir(mpRO)
	require.NoError(t, dirErr)

	gotUnchanged, err := os.ReadFile(filepath.Join(mpRO, "unchanged.bin"))
	require.NoError(t, err)
	require.Equal(t, unchangedBytes, gotUnchanged)

	unchangedOpenFD, err := os.Open(filepath.Join(mpRO, "unchanged_open.bin"))
	require.NoError(t, err)
	defer func() { _ = unchangedOpenFD.Close() }()
	gotUnchangedOpen, err := tryReadFromStartFD(unchangedOpenFD, 64*1024)
	require.NoError(t, err)
	require.Equal(t, unchangedOpenBytes, gotUnchangedOpen)

	gotSmallV1, err := os.ReadFile(filepath.Join(mpRO, "small_reopen.txt"))
	require.NoError(t, err)
	require.Equal(t, smallV1, gotSmallV1)

	smallOpenFD, err := os.Open(filepath.Join(mpRO, "small_open.txt"))
	require.NoError(t, err)
	defer func() { _ = smallOpenFD.Close() }()
	gotSmallOpenV1, err := tryReadFromStartFD(smallOpenFD, 4096)
	require.NoError(t, err)
	require.Equal(t, smallOpenV1, gotSmallOpenV1)

	appendOpenFD, err := os.Open(filepath.Join(mpRO, "append.log"))
	require.NoError(t, err)
	defer func() { _ = appendOpenFD.Close() }()
	gotAppendV1, err := tryReadFromStartFD(appendOpenFD, 4096)
	require.NoError(t, err)
	require.Equal(t, appendBase, gotAppendV1)

	gotLargeV1, err := os.ReadFile(filepath.Join(mpRO, "large_reopen.bin"))
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(largeV1), sha256.Sum256(gotLargeV1))

	largeOpenFD, err := os.Open(filepath.Join(mpRO, "large_open.bin"))
	require.NoError(t, err)
	defer func() { _ = largeOpenFD.Close() }()
	gotLargeOpenV1, err := tryReadFromStartFD(largeOpenFD, 512*1024)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(largeOpenV1), sha256.Sum256(gotLargeOpenV1))

	directOpenFD, err := os.Open(filepath.Join(mpRODirect, "direct_open.bin"))
	require.NoError(t, err)
	defer func() { _ = directOpenFD.Close() }()
	gotDirectOpenV1, err := tryReadFromStartFD(directOpenFD, 512*1024)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(directOpenV1), sha256.Sum256(gotDirectOpenV1))

	writeFullFile(t, filepath.Join(mpW, "late_created.txt"), []byte("fresh-entry"), 11)
	require.NoError(t, os.Remove(filepath.Join(mpW, "to_unlink.txt")))
	require.NoError(t, os.Rename(filepath.Join(mpW, "rename_src.txt"), filepath.Join(mpW, "rename_dst.txt")))

	appendW, err := os.OpenFile(filepath.Join(mpW, "append.log"), os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = appendW.Write(appendTail)
	require.NoError(t, err)
	require.NoError(t, appendW.Sync())
	require.NoError(t, appendW.Close())

	overwriteFileInPlace(t, filepath.Join(mpW, "small_reopen.txt"), smallV2)
	overwriteFileInPlace(t, filepath.Join(mpW, "small_open.txt"), smallOpenV2)
	overwriteFileInPlace(t, filepath.Join(mpW, "large_reopen.bin"), largeV2)
	overwriteFileInPlace(t, filepath.Join(mpW, "large_open.bin"), largeOpenV2)
	overwriteFileInPlace(t, filepath.Join(mpW, "direct_open.bin"), directOpenV2)

	require.Eventually(t, func() bool {
		b, rErr := os.ReadFile(filepath.Join(mpRO, "late_created.txt"))
		return rErr == nil && string(b) == "fresh-entry"
	}, 5*time.Second, 100*time.Millisecond, "RO must observe newly created file")

	require.Eventually(t, func() bool {
		_, uErr := os.Stat(filepath.Join(mpRO, "to_unlink.txt"))
		return errors.Is(uErr, os.ErrNotExist)
	}, 5*time.Second, 100*time.Millisecond, "RO must observe unlinked file removal")

	require.Eventually(t, func() bool {
		_, oldErr := os.Stat(filepath.Join(mpRO, "rename_src.txt"))
		dst, newErr := os.ReadFile(filepath.Join(mpRO, "rename_dst.txt"))
		return errors.Is(oldErr, os.ErrNotExist) && newErr == nil && string(dst) == "moved-content"
	}, 5*time.Second, 100*time.Millisecond, "RO must observe renamed file")

	wantAppended := append(append([]byte(nil), appendBase...), appendTail...)
	require.Eventually(t, func() bool {
		reopenGot, rErr := os.ReadFile(filepath.Join(mpRO, "append.log"))
		openGot, oErr := tryReadFromStartFD(appendOpenFD, 4096)
		return rErr == nil && oErr == nil && bytes.Equal(wantAppended, reopenGot) && bytes.Equal(wantAppended, openGot)
	}, 5*time.Second, 100*time.Millisecond, "RO reopened and open fds must observe appended bytes")

	require.Eventually(t, func() bool {
		reopenGot, rErr := os.ReadFile(filepath.Join(mpRO, "small_reopen.txt"))
		openGot, oErr := tryReadFromStartFD(smallOpenFD, 4096)
		return rErr == nil && oErr == nil && bytes.Equal(smallV2, reopenGot) && bytes.Equal(smallOpenV2, openGot)
	}, 5*time.Second, 100*time.Millisecond, "RO reopened and open fds must observe small same-size in-place overwrites")

	wantLargeV2Sum := sha256.Sum256(largeV2)
	wantLargeOpenV2Sum := sha256.Sum256(largeOpenV2)
	require.Eventually(t, func() bool {
		reopenGot, rErr := os.ReadFile(filepath.Join(mpRO, "large_reopen.bin"))
		openGot, oErr := tryReadFromStartFD(largeOpenFD, 512*1024)
		return rErr == nil && oErr == nil &&
			sha256.Sum256(reopenGot) == wantLargeV2Sum &&
			sha256.Sum256(openGot) == wantLargeOpenV2Sum
	}, 5*time.Second, 100*time.Millisecond, "RO reopened and open fds must observe >=512 KiB same-size in-place overwrites")

	wantDirectV2Sum := sha256.Sum256(directOpenV2)
	require.Eventually(t, func() bool {
		directGot, dErr := tryReadFromStartFD(directOpenFD, 512*1024)
		return dErr == nil && sha256.Sum256(directGot) == wantDirectV2Sum
	}, 5*time.Second, 100*time.Millisecond, "RO --direct-io open fd must refresh read-ahead window after same-size overwrite")

	time.Sleep(1200 * time.Millisecond)
	snapBefore := fetchDebugStats(t, roAdminAddr)

	reopenedUnchanged, err := os.ReadFile(filepath.Join(mpRO, "unchanged.bin"))
	require.NoError(t, err)
	require.Equal(t, unchangedBytes, reopenedUnchanged)

	reReadOpenUnchanged, err := tryReadFromStartFD(unchangedOpenFD, 64*1024)
	require.NoError(t, err)
	require.Equal(t, unchangedOpenBytes, reReadOpenUnchanged)

	snapAfter := fetchDebugStats(t, roAdminAddr)
	require.Greater(t, snapAfter.FUSE.Ops[fusefs.OpGetAttr].Calls, snapBefore.FUSE.Ops[fusefs.OpGetAttr].Calls,
		"kernel CAP_AUTO_INVAL_DATA must revalidate attr after AttrTimeoutSeconds")
	require.Equal(t, snapBefore.FUSE.Ops[fusefs.OpRead].Calls, snapAfter.FUSE.Ops[fusefs.OpRead].Calls,
		"unchanged files must retain kernel page cache with zero extra FUSE Read ops")
}

func TestMultiClient_ReadOnlyObservesSecurityCapabilityAfterInitialMiss(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()

	mpW, stopW := mountWithCleanup(t, endpoint, diskID)
	defer stopW()
	rwPath := filepath.Join(mpW, "cap-tool.bin")
	writeFullFile(t, rwPath, []byte("binary-v1"), 9)
	require.NoError(t, os.Chmod(rwPath, 0o755))

	rwSym := filepath.Join(mpW, "live-sym")
	require.NoError(t, os.Symlink("/target/v1", rwSym))

	mpRO, _, roAdminAddr, stopRO := startMountedDaemon(t, endpoint, diskID, "--mode", "ro")
	defer stopRO()
	roPath := filepath.Join(mpRO, "cap-tool.bin")
	roSym := filepath.Join(mpRO, "live-sym")

	var roStat unix.Stat_t
	require.Eventually(t, func() bool {
		return unix.Stat(roPath, &roStat) == nil
	}, 5*time.Second, 50*time.Millisecond, "RO mount must see cap-tool.bin")
	requireXattrAbsent(t, roPath, "security.capability")

	gotSym1, err := os.Readlink(roSym)
	require.NoError(t, err)
	require.Equal(t, "/target/v1", gotSym1)

	roRawFS := fusefs.NewRawFS(client, fusefs.Config{
		DiskID:              diskID,
		RootIno:             1,
		Mode:                fusefs.MountModeRO,
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	})
	defer roRawFS.OnUnmount()

	buf := make([]byte, 64)
	_, st1 := roRawFS.GetXAttr(nil, &fuse.InHeader{NodeId: roStat.Ino}, "security.capability", buf)
	require.Equal(t, fuse.Status(syscall.ENODATA), st1, "initial RO GetXAttr probe must observe ENODATA")

	capVal := []byte{0x01, 0x00, 0x00, 0x02, 0x00, 0x20, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	require.NoError(t, unix.Setxattr(rwPath, "security.capability", capVal, 0))

	rwSymTmp := filepath.Join(mpW, "live-sym.tmp")
	require.NoError(t, os.Symlink("/target/v2", rwSymTmp))
	require.NoError(t, unix.Rename(rwSymTmp, rwSym))

	requireXattrPresent(t, roPath, "security.capability")
	sz2, st2 := roRawFS.GetXAttr(nil, &fuse.InHeader{NodeId: roStat.Ino}, "security.capability", buf)
	require.Equal(t, fuse.OK, st2, "RO GetXAttr must bypass capabilityCache and observe newly set capability")
	require.Equal(t, capVal, buf[:sz2])

	require.Eventually(t, func() bool {
		got, rErr := os.Readlink(roSym)
		return rErr == nil && got == "/target/v2"
	}, 5*time.Second, 100*time.Millisecond, "RO mount must observe replaced symlink target after TTL expiry")

	overwriteFileInPlace(t, rwPath, []byte("binary-v2"))

	requireXattrAbsent(t, roPath, "security.capability")
	_, st3 := roRawFS.GetXAttr(nil, &fuse.InHeader{NodeId: roStat.Ino}, "security.capability", buf)
	require.Equal(t, fuse.Status(syscall.ENODATA), st3, "RO GetXAttr must observe ENODATA after writer killpriv")

	rawCapStats := roRawFS.Snapshot().CapabilityCache
	require.Zero(t, rawCapStats.Entries, "RO RawFS must keep capabilityCache empty")
	require.Zero(t, rawCapStats.Hits, "RO RawFS must not hit capabilityCache")
	require.Zero(t, rawCapStats.Misses, "RO RawFS must not probe capabilityCache")
	require.Zero(t, rawCapStats.Grants, "RO RawFS must not grant capabilityCache")

	roStats := fetchDebugStats(t, roAdminAddr)
	require.Zero(t, roStats.CapabilityCache.Entries, "mounted RO daemon must keep capabilityCache empty")
	require.Zero(t, roStats.CapabilityCache.Hits, "mounted RO daemon must not hit capabilityCache")
	require.Zero(t, roStats.CapabilityCache.Grants, "mounted RO daemon must not grant capabilityCache")
}
