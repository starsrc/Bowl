//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type countingReadCloser struct {
	io.ReadCloser
	counter *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 {
		c.counter.Add(int64(n))
	}
	return n, err
}

// controllableStorageProxy 为黑盒 E2E 测试提供可观测、可注入延迟与阻塞门控的 HTTP/2 反向代理。
type controllableStorageProxy struct {
	Endpoint string

	gateMu           sync.Mutex
	gateCh           chan struct{}
	writeDelay       atomic.Int64
	fsyncDelay       atomic.Int64
	writeCalls       atomic.Int64
	writeActive      atomic.Int64
	writeDone        atomic.Int64
	writeBytes       atomic.Int64
	readCalls        atomic.Int64
	fsyncCalls       atomic.Int64
	fsyncActive      atomic.Int64
	fsyncMaxInflight atomic.Int64
	fsyncDirCalls    atomic.Int64
}

func startControllableStorageProxy(t *testing.T, targetEndpoint string) *controllableStorageProxy {
	t.Helper()
	targetURL, err := url.Parse(targetEndpoint)
	require.NoError(t, err)

	h2Cfg := &http.HTTP2Config{
		MaxReadFrameSize:              rpc.HTTP2MaxReadFrameSize,
		MaxReceiveBufferPerStream:     rpc.HTTP2MaxReceiveBufferPerStream,
		MaxReceiveBufferPerConnection: rpc.HTTP2DataMaxReceiveBufferPerConnection,
	}
	var trProto http.Protocols
	trProto.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		Protocols: &trProto,
		HTTP2:     h2Cfg,
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = transport

	p := &controllableStorageProxy{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case storagev1connect.StorageServiceReadProcedure:
			p.readCalls.Add(1)
		case storagev1connect.StorageServiceFsyncProcedure:
			p.fsyncCalls.Add(1)
			cur := p.fsyncActive.Add(1)
			for {
				prev := p.fsyncMaxInflight.Load()
				if cur <= prev || p.fsyncMaxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}
			defer p.fsyncActive.Add(-1)
			if delay := time.Duration(p.fsyncDelay.Load()); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-r.Context().Done():
					timer.Stop()
					return
				}
			}
		case storagev1connect.StorageServiceFsyncDirProcedure:
			p.fsyncDirCalls.Add(1)
		case storagev1connect.StorageServiceWriteProcedure:
			p.writeCalls.Add(1)
			p.writeActive.Add(1)
			defer func() {
				p.writeActive.Add(-1)
				p.writeDone.Add(1)
			}()
			r.Body = &countingReadCloser{
				ReadCloser: r.Body,
				counter:    &p.writeBytes,
			}
			if !p.waitWriteGate(r.Context()) {
				return
			}
			if delay := time.Duration(p.writeDelay.Load()); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-r.Context().Done():
					timer.Stop()
					return
				}
			}
		}
		proxy.ServeHTTP(w, r)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var srvProto http.Protocols
	srvProto.SetHTTP1(true)
	srvProto.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:           handler,
		Protocols:         &srvProto,
		HTTP2:             h2Cfg,
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(ln)
	}()

	p.Endpoint = "http://" + ln.Addr().String()
	t.Cleanup(func() {
		p.UnblockWrites()
		_ = srv.Close()
		transport.CloseIdleConnections()
		if sErr := <-serveErrCh; sErr != nil && !errors.Is(sErr, http.ErrServerClosed) {
			t.Errorf("可控存储代理服务异常退出: %v", sErr)
		}
	})
	return p
}

func (p *controllableStorageProxy) BlockWrites() {
	p.gateMu.Lock()
	if p.gateCh == nil {
		p.gateCh = make(chan struct{})
	}
	p.gateMu.Unlock()
}

func (p *controllableStorageProxy) UnblockWrites() {
	p.gateMu.Lock()
	if p.gateCh != nil {
		close(p.gateCh)
		p.gateCh = nil
	}
	p.gateMu.Unlock()
}

func (p *controllableStorageProxy) waitWriteGate(ctx context.Context) bool {
	p.gateMu.Lock()
	ch := p.gateCh
	p.gateMu.Unlock()
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *controllableStorageProxy) SetWriteDelay(d time.Duration) {
	p.writeDelay.Store(int64(d))
}

func (p *controllableStorageProxy) SetFsyncDelay(d time.Duration) {
	p.fsyncDelay.Store(int64(d))
}

func (p *controllableStorageProxy) WriteInflight() int64    { return p.writeActive.Load() }
func (p *controllableStorageProxy) WriteCompleted() int64   { return p.writeDone.Load() }
func (p *controllableStorageProxy) WriteCalls() int64       { return p.writeCalls.Load() }
func (p *controllableStorageProxy) WriteWireBytes() int64   { return p.writeBytes.Load() }
func (p *controllableStorageProxy) ReadCalls() int64        { return p.readCalls.Load() }
func (p *controllableStorageProxy) FsyncCalls() int64       { return p.fsyncCalls.Load() }
func (p *controllableStorageProxy) FsyncMaxInflight() int64 { return p.fsyncMaxInflight.Load() }
func (p *controllableStorageProxy) FsyncDirCalls() int64    { return p.fsyncDirCalls.Load() }

func readMountAdminAddr(t *testing.T, mountpoint string) string {
	t.Helper()
	data, err := os.ReadFile(mountMetaPath(t, mountpoint))
	require.NoError(t, err)
	var meta struct {
		AdminAddr string `json:"admin_addr"`
	}
	require.NoError(t, json.Unmarshal(data, &meta))
	require.NotEmpty(t, meta.AdminAddr)
	require.NotEqual(t, "off", meta.AdminAddr)
	return meta.AdminAddr
}

func tryFetchDebugStats(adminAddr string) (fusefs.Snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+adminAddr+"/debug/stats", http.NoBody)
	if err != nil {
		return fusefs.Snapshot{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fusefs.Snapshot{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fusefs.Snapshot{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return fusefs.Snapshot{}, fmt.Errorf("unexpected /debug/stats status %d: %s", resp.StatusCode, string(body))
	}
	var snap fusefs.Snapshot
	if unmarshalErr := json.Unmarshal(body, &snap); unmarshalErr != nil {
		return fusefs.Snapshot{}, unmarshalErr
	}
	return snap, nil
}

func fetchDebugStats(t *testing.T, adminAddr string) fusefs.Snapshot {
	t.Helper()
	snap, err := tryFetchDebugStats(adminAddr)
	require.NoError(t, err)
	return snap
}

func tryReadProcVMRSSBytes(pid int) (uint64, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if valStr, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			fields := strings.Fields(valStr)
			if len(fields) == 0 {
				return 0, fmt.Errorf("VmRSS 格式异常: %q", line)
			}
			kb, parseErr := strconv.ParseUint(fields[0], 10, 64)
			if parseErr != nil {
				return 0, parseErr
			}
			return kb * 1024, nil
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return 0, scanErr
	}
	return 0, fmt.Errorf("未在 /proc/%d/status 中找到 VmRSS", pid)
}

func readProcVMRSSBytes(t *testing.T, pid int) uint64 {
	t.Helper()
	rss, err := tryReadProcVMRSSBytes(pid)
	require.NoError(t, err)
	return rss
}

func stageDirChunksUsage(stageDir string) (count int, logicalBytes, diskBytes int64) {
	instEntries, err := os.ReadDir(stageDir)
	if err != nil {
		return 0, 0, 0
	}
	for _, instEntry := range instEntries {
		if !instEntry.IsDir() {
			continue
		}
		chunkEntries, subErr := os.ReadDir(filepath.Join(stageDir, instEntry.Name()))
		if subErr != nil {
			continue
		}
		for _, chunkEntry := range chunkEntries {
			if chunkEntry.IsDir() {
				continue
			}
			name := chunkEntry.Name()
			if !strings.HasPrefix(name, "chunk-") || !strings.HasSuffix(name, ".stage") {
				continue
			}
			info, infoErr := chunkEntry.Info()
			if infoErr != nil {
				continue
			}
			count++
			logicalBytes += info.Size()
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				diskBytes += st.Blocks * 512
			}
		}
	}
	return count, logicalBytes, diskBytes
}

func fileSHA256(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func writeFullFD(t *testing.T, fd int, data []byte, offset int64) {
	t.Helper()
	written := 0
	for written < len(data) {
		n, err := unix.Pwrite(fd, data[written:], offset+int64(written))
		if n > 0 {
			written += n
		}
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			require.NoError(t, err)
		}
		require.Positive(t, n)
	}
}

func writeRepeatedChunksFD(t *testing.T, fd int, chunk []byte, count int) {
	t.Helper()
	for i := range count {
		writeFullFD(t, fd, chunk, int64(i*len(chunk)))
	}
}

func writeFullFile(t *testing.T, path string, data []byte, chunkSize int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	for off := 0; off < len(data); off += chunkSize {
		end := min(off+chunkSize, len(data))
		n, writeErr := f.Write(data[off:end])
		require.NoError(t, writeErr)
		require.Equal(t, end-off, n)
	}
	require.NoError(t, f.Close())
}

func writeCycledBlocksAndSHA256(t *testing.T, path string, totalBlocks, uniqueBlocks, blockBytes, writeStep int) [sha256.Size]byte {
	t.Helper()
	blocks := make([][]byte, uniqueBlocks)
	for i := range uniqueBlocks {
		blocks[i] = deterministicPayload(fmt.Sprintf("multi-blk-%d", i), blockBytes)
	}
	hash := sha256.New()
	for blkIdx := range totalBlocks {
		_, _ = hash.Write(blocks[blkIdx%uniqueBlocks])
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	for blkIdx := range totalBlocks {
		blk := blocks[blkIdx%uniqueBlocks]
		for off := 0; off < len(blk); off += writeStep {
			n, wErr := f.Write(blk[off : off+writeStep])
			require.NoError(t, wErr)
			require.Equal(t, writeStep, n)
		}
	}
	require.NoError(t, f.Close())
	var sum [sha256.Size]byte
	copy(sum[:], hash.Sum(nil))
	return sum
}

func buildSparse32MiBAndWriteFD(t *testing.T, fd int) [sha256.Size]byte {
	t.Helper()
	const sparseTotalSize = 33_554_432
	want := make([]byte, sparseTotalSize)
	ext0 := deterministicPayload("sparse-tmp-ext0", 4_194_304)
	ext1 := deterministicPayload("sparse-tmp-ext1", 4_194_304)
	ext2 := deterministicPayload("sparse-tmp-ext2", 4_194_304)
	copy(want[0:4_194_304], ext0)
	copy(want[16_777_216:20_971_520], ext1)
	copy(want[29_360_128:33_554_432], ext2)

	writeFullFD(t, fd, ext0, 0)
	writeFullFD(t, fd, ext1, 16_777_216)
	writeFullFD(t, fd, ext2, 29_360_128)
	return sha256.Sum256(want)
}

type stageTelemetrySample struct {
	maxStageLogical int64
	maxStageDisk    int64
	maxMemBytes     uint64
	maxRSSBytes     uint64
}

func startStageTelemetrySampler(adminAddr, stageDir string, daemonPID int, baseMem, baseRSS uint64) func() stageTelemetrySample {
	var (
		mu      sync.Mutex
		summary = stageTelemetrySample{
			maxMemBytes: baseMem,
			maxRSSBytes: baseRSS,
		}
		stopCh = make(chan struct{})
		doneCh = make(chan struct{})
	)
	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		record := func() {
			_, logicalB, diskB := stageDirChunksUsage(stageDir)
			snap, sErr := tryFetchDebugStats(adminAddr)
			rss, rErr := tryReadProcVMRSSBytes(daemonPID)
			mu.Lock()
			summary.maxStageLogical = max(summary.maxStageLogical, logicalB)
			summary.maxStageDisk = max(summary.maxStageDisk, diskB)
			if sErr == nil {
				summary.maxMemBytes = max(summary.maxMemBytes, snap.MemoryBytes)
			}
			if rErr == nil {
				summary.maxRSSBytes = max(summary.maxRSSBytes, rss)
			}
			mu.Unlock()
		}
		for {
			select {
			case <-stopCh:
				record()
				return
			case <-ticker.C:
				record()
			}
		}
	}()
	return func() stageTelemetrySample {
		close(stopCh)
		<-doneCh
		mu.Lock()
		defer mu.Unlock()
		return summary
	}
}

func startMountedDaemon(t *testing.T, endpoint, diskID string, extraFlags ...string) (mp string, daemonPID int, adminAddr string, stop func()) {
	t.Helper()
	mp = t.TempDir()
	args := []string{"mount", "--endpoint", endpoint, "--admin-addr", "127.0.0.1:0"}
	args = append(args, extraFlags...)
	args = append(args, diskID, mp)
	runBowlSuccess(t, args...)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp).Run() })

	daemonPID = findBowlDaemonPID(t, mp)
	adminAddr = readMountAdminAddr(t, mp)
	var once sync.Once
	stop = func() {
		once.Do(func() {
			runBowlSuccess(t, "umount", mp)
			waitForUnmount(t, mp)
			waitForProcessExit(t, daemonPID)
		})
	}
	return mp, daemonPID, adminAddr, stop
}

func requireStageDrained(t *testing.T, adminAddr string, timeout time.Duration, scene string) fusefs.Snapshot {
	t.Helper()
	var latest fusefs.Snapshot
	require.Eventually(t, func() bool {
		latest = fetchDebugStats(t, adminAddr)
		return latest.Stage.UsedBytes == 0 && latest.Stage.PendingChunks == 0
	}, timeout, 20*time.Millisecond, "scene=%s want=used_bytes_0_and_pending_0 got=used_%d_pending_%d",
		scene, latest.Stage.UsedBytes, latest.Stage.PendingChunks)
	return latest
}

func requireStageDirZeroChunks(t *testing.T, stageDir, scene string) {
	t.Helper()
	cnt, logicalB, _ := stageDirChunksUsage(stageDir)
	require.Zero(t, cnt, "scene=%s want=0_chunk_files got=%d", scene, cnt)
	require.Zero(t, logicalB, "scene=%s want=0_chunk_bytes got=%d", scene, logicalB)
}

func requireNoDirEntries(t *testing.T, dir, scene string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "scene=%s dir=%s", scene, dir)
	require.Empty(t, entries, "scene=%s dir=%s want=empty got=%d_entries", scene, dir, len(entries))
}

func requirePathIsTmpfs(t *testing.T, path string) {
	t.Helper()
	var st unix.Statfs_t
	require.NoError(t, unix.Statfs(path, &st))
	require.Equal(t, uint64(unix.TMPFS_MAGIC), uint64(st.Type), "scene=tmpfs_check path=%s want=TMPFS_MAGIC got=%#x", path, st.Type)
}

func findSingleInstanceDir(t *testing.T, stageDir string, expectedPID int) string {
	t.Helper()
	entries, err := os.ReadDir(stageDir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "scene=single_instance_dir stageDir=%s want=1 got=%d", stageDir, len(entries))
	name := entries[0].Name()
	wantPrefix := fmt.Sprintf("inst-%d-", expectedPID)
	require.True(t, strings.HasPrefix(name, wantPrefix), "scene=instance_dir_prefix want=%s* got=%s", wantPrefix, name)
	return filepath.Join(stageDir, name)
}

func requireRenameBlockedWhileWritesGated(t *testing.T, proxy *controllableStorageProxy, src, dst string, blockWindow time.Duration) {
	t.Helper()
	renameErrCh := make(chan error, 1)
	go func() {
		renameErrCh <- os.Rename(src, dst)
	}()
	select {
	case rErr := <-renameErrCh:
		t.Fatalf("scene=rename_barrier want=blocked_while_writes_gated got=returned_early(%v)", rErr)
	case <-time.After(blockWindow):
	}
	proxy.UnblockWrites()
	require.NoError(t, <-renameErrCh, "scene=rename_barrier want=nil_after_unblock")
}

func requireDiskUsageUnchanged(t *testing.T, client storagev1connect.StorageServiceClient, diskID string, wantBytes, wantInodes uint64, scene string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	gotBytes := resp.Msg.GetDisk().GetUsedBytes()
	gotInodes := resp.Msg.GetDisk().GetUsedInodes()
	require.Equal(t, wantBytes, gotBytes, "scene=%s field=used_bytes want=%d got=%d", scene, wantBytes, gotBytes)
	require.Equal(t, wantInodes, gotInodes, "scene=%s field=used_inodes want=%d got=%d", scene, wantInodes, gotInodes)
}

func fetchDiskUsage(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) (usedBytes, usedInodes uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	return resp.Msg.GetDisk().GetUsedBytes(), resp.Msg.GetDisk().GetUsedInodes()
}

// TestStagedMode_FlagValidation_GivenInvalidArgsOrEnv_Exits2 验证所有非法挂载模式与暂存参数组合均返回退出码 2。
func TestStagedMode_FlagValidation_GivenInvalidArgsOrEnv_Exits2(t *testing.T) {
	endpoint := getStorageEndpoint()
	invalidMP := t.TempDir()
	dummyID := uuid.NewString()
	regularFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(regularFile, []byte("x"), 0o600))

	cases := []struct {
		name string
		args []string
	}{
		{name: "LegacyROFlag", args: []string{"mount", "--endpoint", endpoint, "--ro", dummyID, invalidMP}},
		{name: "InvalidModeBogus", args: []string{"mount", "--endpoint", endpoint, "--mode", "bogus", dummyID, invalidMP}},
		{name: "ROWithStageSize", args: []string{"mount", "--endpoint", endpoint, "--mode", "ro", "--stage-size", "8MiB", dummyID, invalidMP}},
		{name: "ROWithStageDir", args: []string{"mount", "--endpoint", endpoint, "--mode", "ro", "--stage-dir", "/tmp/x", dummyID, invalidMP}},
		{name: "SyncWithStageSize", args: []string{"mount", "--endpoint", endpoint, "--mode", "sync", "--stage-size", "8MiB", dummyID, invalidMP}},
		{name: "SyncWithStageDir", args: []string{"mount", "--endpoint", endpoint, "--mode", "sync", "--stage-dir", "/tmp/x", dummyID, invalidMP}},
		{name: "StageSizeBelowMin", args: []string{"mount", "--endpoint", endpoint, "--stage-size", "1MiB", dummyID, invalidMP}},
		{name: "StageDirNestedInMountpoint", args: []string{"mount", "--endpoint", endpoint, "--stage-dir", filepath.Join(invalidMP, "sub"), dummyID, invalidMP}},
		{name: "StageDirEqualToMountpoint", args: []string{"mount", "--endpoint", endpoint, "--stage-dir", invalidMP, dummyID, invalidMP}},
		{name: "StageDirEmpty", args: []string{"mount", "--endpoint", endpoint, "--stage-dir", "", dummyID, invalidMP}},
		{name: "StageDirRegularFile", args: []string{"mount", "--endpoint", endpoint, "--stage-dir", regularFile, dummyID, invalidMP}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := runBowlExpectExit(t, 2, tc.args...)
			require.Contains(t, out, "Error:", "scene=%s want=Error_prefix got=%s", tc.name, out)
		})
	}
}

// TestStagedMode_DefaultMode_GivenNoModeFlag_UsesStaged512MiBAndPersistsWrite 验证默认模式为 staged 且默认容量为 512 MiB。
func TestStagedMode_DefaultMode_GivenNoModeFlag_UsesStaged512MiBAndPersistsWrite(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()
	mp, stop := mountWithCleanup(t, endpoint, diskID, "--admin-addr", "127.0.0.1:0")

	// When
	snap := fetchDebugStats(t, readMountAdminAddr(t, mp))
	require.NoError(t, os.WriteFile(filepath.Join(mp, "default-staged.txt"), []byte("staged-ok"), 0o644))
	stop()

	// Then
	require.Equal(t, uint64(536_870_912), snap.Stage.CapacityBytes, "scene=default_stage_capacity want=536870912 got=%d", snap.Stage.CapacityBytes)
	mpVerify, stopVerify := mountWithCleanup(t, endpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopVerify()
	got, err := os.ReadFile(filepath.Join(mpVerify, "default-staged.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("staged-ok"), got)
}

// TestStagedMode_ReadOnlyMode_GivenAmbientStageEnv_IgnoresStageDirAndRejectsWritesAndTmpfileWithEROFS 验证 --mode ro 忽略暂存环境变量并拒绝常规写与 O_TMPFILE。
func TestStagedMode_ReadOnlyMode_GivenAmbientStageEnv_IgnoresStageDirAndRejectsWritesAndTmpfileWithEROFS(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()
	unusedStageDir := filepath.Join(t.TempDir(), "unused-ro-stage")
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)

	// When
	mpRO, stopRO := mountWithCleanup(t, endpoint, diskID, "--mode", "ro", "--admin-addr", "127.0.0.1:0")
	defer stopRO()
	roSnap := fetchDebugStats(t, readMountAdminAddr(t, mpRO))
	writeErr := os.WriteFile(filepath.Join(mpRO, "ro-deny.txt"), []byte("nope"), 0o644)
	_, tmpErr := unix.Open(mpRO, unix.O_TMPFILE|unix.O_RDWR, 0o600)

	// Then
	require.NoDirExists(t, unusedStageDir, "scene=ro_mode want=stage_dir_not_created")
	require.Zero(t, roSnap.Stage.CapacityBytes, "scene=ro_mode want=stage_capacity_0 got=%d", roSnap.Stage.CapacityBytes)
	require.ErrorIs(t, writeErr, syscall.EROFS)
	require.ErrorIs(t, tmpErr, syscall.EROFS)
}

// TestStagedMode_TmpfsStageDir_GivenDevShmPath_LogsStageDirOnTmpfsWarning 验证暂存目录位于 /dev/shm 时记录 stage_dir_on_tmpfs 告警日志。
func TestStagedMode_TmpfsStageDir_GivenDevShmPath_LogsStageDirOnTmpfsWarning(t *testing.T) {
	// Given
	requirePathIsTmpfs(t, "/dev/shm")
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	diskID := createTestDisk(t, client).GetDiskId()

	shmStageDir := fmt.Sprintf("/dev/shm/bowl-e2e-tmpfs-%d", time.Now().UnixNano())
	require.NoError(t, os.MkdirAll(shmStageDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(shmStageDir) })

	// When
	logPath := filepath.Join(t.TempDir(), "mount-tmpfs.log")
	_, _, _, stopMount := startMountedDaemon(t, endpoint, diskID, "--stage-dir", shmStageDir, "--log-file", logPath)
	defer stopMount()

	// Then
	logBytes, err := os.ReadFile(logPath)
	require.NoError(t, err)
	logStr := string(logBytes)
	require.Contains(t, logStr, `"level":"WARN"`)
	require.Contains(t, logStr, `"msg":"stage_dir_on_tmpfs"`)
	require.Contains(t, logStr, shmStageDir)
}

// TestStagedMode_StageDirStartup_GivenDefunctLockAndLiveInstance_CleansOnlyDefunct 验证启动扫描清理失效 inst-999999-dead/.lock 残留并保留存活实例目录。
func TestStagedMode_StageDirStartup_GivenDefunctLockAndLiveInstance_CleansOnlyDefunct(t *testing.T) {
	// Given: 实例 #1 持锁运行并写入已死进程遗留 inst-999999-dead/.lock
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk1ID := createTestDisk(t, client).GetDiskId()
	disk2ID := createTestDisk(t, client).GetDiskId()

	stageDir := filepath.Join(t.TempDir(), "shared-stage-root")
	require.NoError(t, os.MkdirAll(stageDir, 0o755))

	_, pid1, _, stop1 := startMountedDaemon(t, endpoint, disk1ID, "--stage-dir", stageDir)
	liveInstDir := findSingleInstanceDir(t, stageDir, pid1)

	defunctDir := filepath.Join(stageDir, "inst-999999-dead")
	require.NoError(t, os.MkdirAll(defunctDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(defunctDir, ".lock"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(defunctDir, "chunk-1.stage"), []byte("stale"), 0o600))

	// When: 启动共享同一暂存根目录的实例 #2，然后双双卸载
	_, _, _, stop2 := startMountedDaemon(t, endpoint, disk2ID, "--stage-dir", stageDir)

	// Then
	require.NoDirExists(t, defunctDir, "scene=startup_cleanup want=defunct_dir_removed")
	require.DirExists(t, liveInstDir, "scene=startup_cleanup want=live_instance_dir_preserved")

	stop2()
	stop1()
	requireNoDirEntries(t, stageDir, "post_umount_shared_stage_dir")
}

// TestStagedMode_SlowServerReadYourOwnWrites_GivenBlockedWriteRPCs_ReadsExactBytesWithoutFsyncOrReadRPC 验证慢服务端期间 write/close/open/read 无需 fsync 即命中本地暂存并零 Read RPC。
func TestStagedMode_SlowServerReadYourOwnWrites_GivenBlockedWriteRPCs_ReadsExactBytesWithoutFsyncOrReadRPC(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	proxy := startControllableStorageProxy(t, getStorageEndpoint())
	diskID := createTestDisk(t, client).GetDiskId()
	stageDir := filepath.Join(t.TempDir(), "ryow-stage")
	mp, _, adminAddr, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "8MiB", "--stage-dir", stageDir)
	defer stopMount()

	proxy.BlockWrites()
	wantRYOW := deterministicPayload("staged-ryow-5m", 5_242_880)
	ryowPath := filepath.Join(mp, "ryow.bin")

	// When: 写 5 MiB 后关闭（无 fsync）并立即打开读取
	writeFullFile(t, ryowPath, wantRYOW, 524_288)
	require.Eventually(t, func() bool {
		return proxy.WriteInflight() >= 1
	}, 5*time.Second, 10*time.Millisecond, "scene=ryow want=background_write_in_flight")
	require.Zero(t, proxy.WriteCompleted(), "scene=ryow want=0_completed_writes_while_blocked got=%d", proxy.WriteCompleted())

	readsBefore := proxy.ReadCalls()
	gotRYOW, err := os.ReadFile(ryowPath)

	// Then
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(wantRYOW), sha256.Sum256(gotRYOW), "scene=ryow want=exact_sha256_match")
	require.Equal(t, readsBefore, proxy.ReadCalls(), "scene=ryow want=0_read_rpcs")

	proxy.UnblockWrites()
	requireStageDrained(t, adminAddr, 10*time.Second, "ryow_drain")
	stopMount()
}

// TestStagedMode_MultiXStageSize_Given256MiBWriteUnderSlowUpload_BoundsDiskAndMemoryAndPersistsSHA256 验证 32x --stage-size 大文件写入下背压、磁盘与内存受控且重挂 SHA-256 一致。
func TestStagedMode_MultiXStageSize_Given256MiBWriteUnderSlowUpload_BoundsDiskAndMemoryAndPersistsSHA256(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()
	proxy := startControllableStorageProxy(t, realEndpoint)
	diskID := createTestDisk(t, client).GetDiskId()
	stageDir := filepath.Join(t.TempDir(), "multi-x-stage")

	mp, daemonPID, adminAddr, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "8MiB", "--stage-dir", stageDir)
	baseStats := fetchDebugStats(t, adminAddr)
	baseRSS := readProcVMRSSBytes(t, daemonPID)
	require.Equal(t, uint64(8_388_608), baseStats.Stage.CapacityBytes)

	proxy.SetWriteDelay(20 * time.Millisecond)
	stopSampler := startStageTelemetrySampler(adminAddr, stageDir, daemonPID, baseStats.MemoryBytes, baseRSS)

	// When: 写入 256 MiB（64 块 x 4 MiB = 32x --stage-size）并等待排空与卸载
	multiPath := filepath.Join(mp, "multi-256m.bin")
	wantMultiSHA := writeCycledBlocksAndSHA256(t, multiPath, 64, 8, 4_194_304, 1_048_576)
	afterStats := requireStageDrained(t, adminAddr, 45*time.Second, "multi_256m_drain")
	telemetry := stopSampler()
	proxy.SetWriteDelay(0)

	// Then: 触发背压、暂存磁盘与客户端内存远低于文件大小、上传后清零、重挂 --mode ro --direct-io 校验一致
	t.Logf("memory telemetry: baseRSS=%d MiB maxRSS=%d MiB baseMem=%d MiB maxMem=%d MiB",
		baseRSS>>20, telemetry.maxRSSBytes>>20, baseStats.MemoryBytes>>20, telemetry.maxMemBytes>>20)
	require.Positive(t, afterStats.Stage.BackpressureWaits, "scene=multi_256m want=backpressure_waits>0 got=0")
	require.LessOrEqual(t, telemetry.maxStageLogical, int64(8_388_608), "scene=multi_256m want=stage_logical<=8MiB got=%d", telemetry.maxStageLogical)
	require.LessOrEqual(t, telemetry.maxStageDisk, int64(9_437_184), "scene=multi_256m want=stage_disk<=9MiB got=%d", telemetry.maxStageDisk)
	requireStageDirZeroChunks(t, stageDir, "multi_256m_post_drain")
	require.Less(t, telemetry.maxMemBytes, uint64(134_217_728), "scene=multi_256m want=maxMem<128MiB got=%d", telemetry.maxMemBytes)
	require.Less(t, telemetry.maxRSSBytes, uint64(134_217_728), "scene=multi_256m want=maxRSS<128MiB got=%d", telemetry.maxRSSBytes)
	require.Less(t, telemetry.maxMemBytes-baseStats.MemoryBytes, uint64(134_217_728), "scene=multi_256m want=memDelta<128MiB")
	require.Less(t, telemetry.maxRSSBytes-baseRSS, uint64(134_217_728), "scene=multi_256m want=rssDelta<128MiB")

	stopMount()
	requireNoDirEntries(t, stageDir, "multi_256m_post_umount")

	mpVerify, stopVerify := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopVerify()
	require.Equal(t, wantMultiSHA, fileSHA256(t, filepath.Join(mpVerify, "multi-256m.bin")), "scene=multi_256m_verify want=sha256_match")
}

// TestStagedMode_UnlinkedTmpfile_Given16MiBWrittenWithoutLinkat_ProducesZeroServerUsageDelta 验证 16 MiB O_TMPFILE 未 linkat 关闭后服务端用量与 Write RPC 均零增长。
func TestStagedMode_UnlinkedTmpfile_Given16MiBWrittenWithoutLinkat_ProducesZeroServerUsageDelta(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	proxy := startControllableStorageProxy(t, getStorageEndpoint())
	diskID := createTestDisk(t, client).GetDiskId()
	stageDir := filepath.Join(t.TempDir(), "unlinked-tmp-stage")
	mp, _, adminAddr, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "8MiB", "--stage-dir", stageDir)
	defer stopMount()

	usedBytesBefore, usedInodesBefore := fetchDiskUsage(t, client, diskID)
	writeCallsBefore := proxy.WriteCalls()

	// When: 打开 O_TMPFILE 写入 16 MiB（超出 --stage-size 8MiB）后不经 linkat 直接关闭
	fd, err := unix.Open(mp, unix.O_TMPFILE|unix.O_RDWR, 0o600)
	require.NoError(t, err)
	writeRepeatedChunksFD(t, fd, deterministicPayload("unlinked-tmp-16m", 4_194_304), 4)

	midStats := fetchDebugStats(t, adminAddr)
	require.Equal(t, 1, midStats.TmpFile.ActiveFiles, "scene=unlinked_tmp_open want=active_files_1 got=%d", midStats.TmpFile.ActiveFiles)
	require.GreaterOrEqual(t, midStats.TmpFile.ResidentBytes, uint64(16_777_216), "scene=unlinked_tmp_open want=resident>=16MiB got=%d", midStats.TmpFile.ResidentBytes)
	require.NoError(t, unix.Close(fd))

	// Then
	require.Eventually(t, func() bool {
		snap := fetchDebugStats(t, adminAddr)
		return snap.TmpFile.ActiveFiles == 0 && snap.TmpFile.ResidentBytes == 0
	}, 5*time.Second, 20*time.Millisecond, "scene=unlinked_tmp_close want=tmpfile_stats_zero")
	require.Equal(t, writeCallsBefore, proxy.WriteCalls(), "scene=unlinked_tmp_close want=0_write_rpcs")
	requireDiskUsageUnchanged(t, client, diskID, usedBytesBefore, usedInodesBefore, "unlinked_tmp_16m")
}

// TestStagedMode_SparseTmpfilePromotion_Given32MiBSparseLinkat_SkipsHolesAndPersistsSHA256OnDirectIORemount 验证 32 MiB 稀疏 O_TMPFILE linkat 跳过空洞上传并在 --mode ro --direct-io 重挂校验 SHA-256。
func TestStagedMode_SparseTmpfilePromotion_Given32MiBSparseLinkat_SkipsHolesAndPersistsSHA256OnDirectIORemount(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()
	proxy := startControllableStorageProxy(t, realEndpoint)
	diskID := createTestDisk(t, client).GetDiskId()
	stageDir := filepath.Join(t.TempDir(), "sparse-tmp-stage")
	mp, _, adminAddr, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "8MiB", "--stage-dir", stageDir)

	writeBytesBefore := proxy.WriteWireBytes()
	writeCallsBefore := proxy.WriteCalls()

	// When: 写入 3 段共 12 MiB 数据（逻辑总长 32 MiB，含 20 MiB 空洞）并 linkat 转正后卸载
	fd, err := unix.Open(mp, unix.O_TMPFILE|unix.O_RDWR, 0o600)
	require.NoError(t, err)
	wantTmpSHA := buildSparse32MiBAndWriteFD(t, fd)

	promotedPath := filepath.Join(mp, "sparse-tmp-32m.bin")
	require.NoError(t, unix.Linkat(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd), unix.AT_FDCWD, promotedPath, unix.AT_SYMLINK_FOLLOW))
	require.NoError(t, unix.Close(fd))

	snapAfterDrain := requireStageDrained(t, adminAddr, 15*time.Second, "sparse_tmp_32m_drain")
	uploadedCalls := proxy.WriteCalls() - writeCallsBefore
	uploadedBytes := proxy.WriteWireBytes() - writeBytesBefore
	t.Logf("promoted tmpfile telemetry: writeCalls=%d uploadedBytes=%d", uploadedCalls, uploadedBytes)

	// Then
	require.Equal(t, int64(3), uploadedCalls, "scene=sparse_tmp_promote want=3_write_calls got=%d", uploadedCalls)
	require.GreaterOrEqual(t, uploadedBytes, int64(12_582_912), "scene=sparse_tmp_promote want>=12MiB got=%d", uploadedBytes)
	require.Less(t, uploadedBytes, int64(16_777_216), "scene=sparse_tmp_promote want<16MiB_skip_holes got=%d", uploadedBytes)
	require.GreaterOrEqual(t, snapAfterDrain.TmpFile.Promotes, uint64(1), "scene=sparse_tmp_promote want=promotes>=1 got=%d", snapAfterDrain.TmpFile.Promotes)

	stopMount()
	mpVerify, stopVerify := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopVerify()
	require.Equal(t, wantTmpSHA, fileSHA256(t, filepath.Join(mpVerify, "sparse-tmp-32m.bin")), "scene=sparse_tmp_verify want=sha256_match")
}

// TestStagedMode_RenameBarrier_GivenBlockedUpload_BlocksRenameUntilCommittedAndVisibleToConcurrentRO 验证 write->close->rename 等待暂存落盘且并发 --mode ro 挂载在 rename 返回后立即可读完整文件。
func TestStagedMode_RenameBarrier_GivenBlockedUpload_BlocksRenameUntilCommittedAndVisibleToConcurrentRO(t *testing.T) {
	// Given
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()
	proxy := startControllableStorageProxy(t, realEndpoint)
	diskID := createTestDisk(t, client).GetDiskId()
	stageDir := filepath.Join(t.TempDir(), "rename-barrier-stage")

	mp, _, _, stopMount := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "8MiB", "--stage-dir", stageDir)
	defer stopMount()
	mpRO, stopRO := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
	defer stopRO()

	proxy.BlockWrites()
	wantRename := deterministicPayload("rename-barrier-4m", 4_194_304)
	renameSrc := filepath.Join(mp, "rename-barrier-src.bin")
	renameDst := filepath.Join(mp, "rename-barrier-dst.bin")
	writeFullFile(t, renameSrc, wantRename, 524_288)
	require.Eventually(t, func() bool {
		return proxy.WriteInflight() >= 1
	}, 5*time.Second, 10*time.Millisecond, "scene=rename_barrier want=write_rpc_in_flight")

	// When: 在 Write RPC 阻塞期间发起 Rename，150ms 后放行写入
	requireRenameBlockedWhileWritesGated(t, proxy, renameSrc, renameDst, 150*time.Millisecond)

	// Then: Rename 返回后并发 --mode ro --direct-io 挂载无需等待即可一次读回完整文件
	gotRename, err := os.ReadFile(filepath.Join(mpRO, "rename-barrier-dst.bin"))
	require.NoError(t, err, "scene=rename_barrier_ro_read want=immediate_success")
	require.Equal(t, sha256.Sum256(wantRename), sha256.Sum256(gotRename), "scene=rename_barrier_ro_read want=sha256_match")
}

type syncE2EBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncE2EBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncE2EBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func fetchDebugUnmount(t *testing.T, adminAddr string) fusefs.UnmountProgress {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+adminAddr+"/debug/unmount", http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var prog fusefs.UnmountProgress
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&prog))
	return prog
}

// TestStagedMode_UmountDrainProgress_GivenUnsyncedWrites_ReportsSyncProgressOnStderr verifies
// bowl umount drains unsynced writes with empty stdout, determinate stderr progress when admin
// is enabled, zero output when clean, and fallback notice after 1s when admin is off.
func TestStagedMode_UmountDrainProgress_GivenUnsyncedWrites_ReportsSyncProgressOnStderr(t *testing.T) {
	client := newTestStorageClient(t)
	realEndpoint := getStorageEndpoint()

	t.Run("UnsyncedWritesWithAdmin", func(t *testing.T) {
		proxy := startControllableStorageProxy(t, realEndpoint)
		diskID := createTestDisk(t, client).GetDiskId()
		stageDir := filepath.Join(t.TempDir(), "umount-progress-stage")
		mp, daemonPID, adminAddr, _ := startMountedDaemon(t, proxy.Endpoint, diskID, "--stage-size", "16MiB", "--stage-dir", stageDir)

		proxy.BlockWrites()
		t.Cleanup(proxy.UnblockWrites)
		wantA := deterministicPayload("umount-prog-a", 4_194_304)
		wantB := deterministicPayload("umount-prog-b", 1_048_576)
		writeFullFile(t, filepath.Join(mp, "umount-a.bin"), wantA, 524_288)
		writeFullFile(t, filepath.Join(mp, "umount-b.bin"), wantB, 524_288)
		require.Eventually(t, func() bool {
			return proxy.WriteInflight() >= 1
		}, 5*time.Second, 10*time.Millisecond, "scene=umount_progress want=write_rpc_in_flight")
		require.Zero(t, proxy.WriteCompleted())

		ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
		defer cancel()
		var outBuf, errBuf syncE2EBuffer
		umountCmd := exec.CommandContext(ctx, getBowlBin(), "umount", mp)
		umountCmd.Stdout = &outBuf
		umountCmd.Stderr = &errBuf
		require.NoError(t, umountCmd.Start())

		require.Eventually(t, func() bool {
			prog := fetchDebugUnmount(t, adminAddr)
			return prog.Active && prog.Total > 0 && strings.Contains(errBuf.String(), "Syncing to cloud:")
		}, 5*time.Second, 20*time.Millisecond, "scene=umount_progress want=active_debug_unmount_and_stderr_progress")

		proxy.UnblockWrites()
		require.NoError(t, umountCmd.Wait(), "bowl umount failed: stderr=%s", errBuf.String())
		waitForUnmount(t, mp)
		waitForProcessExit(t, daemonPID)

		require.Empty(t, outBuf.String(), "bowl umount stdout must remain empty")
		errText := errBuf.String()
		require.Contains(t, errText, "Syncing to cloud:")
		require.Contains(t, errText, "100%")
		require.NotContains(t, errText, "\r")
		require.NotContains(t, errText, "\x1b")

		mpVerify, stopVerify := mountWithCleanup(t, realEndpoint, diskID, "--mode", "ro", "--direct-io")
		defer stopVerify()
		require.Equal(t, sha256.Sum256(wantA), fileSHA256(t, filepath.Join(mpVerify, "umount-a.bin")))
		require.Equal(t, sha256.Sum256(wantB), fileSHA256(t, filepath.Join(mpVerify, "umount-b.bin")))
	})

	t.Run("CleanMountEmitsNoOutput", func(t *testing.T) {
		diskID := createTestDisk(t, client).GetDiskId()
		mp, daemonPID, _, _ := startMountedDaemon(t, realEndpoint, diskID)

		ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
		defer cancel()
		var outBuf, errBuf bytes.Buffer
		umountCmd := exec.CommandContext(ctx, getBowlBin(), "umount", mp)
		umountCmd.Stdout = &outBuf
		umountCmd.Stderr = &errBuf
		require.NoError(t, umountCmd.Run(), "clean bowl umount failed: stderr=%s", errBuf.String())
		waitForUnmount(t, mp)
		waitForProcessExit(t, daemonPID)

		require.Empty(t, outBuf.String())
		require.Empty(t, errBuf.String())
	})

	t.Run("AdminOffSlowDrainFallsBackAfterGrace", func(t *testing.T) {
		proxy := startControllableStorageProxy(t, realEndpoint)
		diskID := createTestDisk(t, client).GetDiskId()
		mp := t.TempDir()
		runBowlSuccess(t, "mount", "--endpoint", proxy.Endpoint, "--admin-addr", "off", "--stage-size", "8MiB", diskID, mp)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp).Run() })
		daemonPID := findBowlDaemonPID(t, mp)

		proxy.BlockWrites()
		t.Cleanup(proxy.UnblockWrites)
		writeFullFile(t, filepath.Join(mp, "fallback.bin"), deterministicPayload("umount-fallback", 1_048_576), 524_288)
		require.Eventually(t, func() bool {
			return proxy.WriteInflight() >= 1
		}, 5*time.Second, 10*time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
		defer cancel()
		var outBuf, errBuf syncE2EBuffer
		umountCmd := exec.CommandContext(ctx, getBowlBin(), "umount", mp)
		umountCmd.Stdout = &outBuf
		umountCmd.Stderr = &errBuf
		require.NoError(t, umountCmd.Start())

		require.Eventually(t, func() bool {
			return strings.Contains(errBuf.String(), "Syncing to cloud...\n")
		}, 5*time.Second, 20*time.Millisecond, "bowl umount must emit fallback notice after 1s grace period")

		proxy.UnblockWrites()
		require.NoError(t, umountCmd.Wait(), "fallback bowl umount failed: stderr=%s", errBuf.String())
		waitForUnmount(t, mp)
		waitForProcessExit(t, daemonPID)

		require.Empty(t, outBuf.String())
		errText := errBuf.String()
		require.Contains(t, errText, "Syncing to cloud...\n")
		require.NotContains(t, errText, "\r")
		require.NotContains(t, errText, "\x1b")
		require.NotContains(t, errText, "%")
	})
}
