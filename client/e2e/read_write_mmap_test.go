//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_ReadWrite(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("large_file_span_blocks_and_chunks_sha256", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("stdout: %s\nstderr: %s\n", stdout.String(), stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		filePath := filepath.Join(mountpoint, "large-66m.bin")

		const totalSize = 66 * 1024 * 1024 // 66 MiB: 跨 4 MiB Block 与跨 64 MiB Chunk 边界
		fullBuf := make([]byte, totalSize)
		prng := rand.New(rand.NewSource(123456789))
		_, err := prng.Read(fullBuf)
		require.NoError(t, err)

		// 1. 流式顺序写入 66 MiB
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		require.NoError(t, err)

		const writeChunk = 2 * 1024 * 1024 // 2 MiB 每次写入
		for off := 0; off < totalSize; off += writeChunk {
			end := off + writeChunk
			if end > totalSize {
				end = totalSize
			}
			n, err := f.Write(fullBuf[off:end])
			require.NoError(t, err)
			require.Equal(t, end-off, n)
		}
		require.NoError(t, f.Close())

		// 2. 随机偏移覆盖写（包含跨 Block 与跨 64 MiB Chunk 边界）
		f, err = os.OpenFile(filePath, os.O_WRONLY, 0644)
		require.NoError(t, err)

		overwrites := []struct {
			offset int64
			data   []byte
		}{
			{offset: 2 * 1024 * 1024, data: []byte("OVERWRITE_AT_2MIB_BLOCK")},
			{offset: 63*1024*1024 + 900*1024, data: bytes.Repeat([]byte("CHUNK0_EDGE_"), 1000)},
			{offset: 64*1024*1024 - 10, data: []byte("STRADDLE_CHUNK_BOUNDARY_1234567890")},
			{offset: 64*1024*1024 + 500*1024, data: bytes.Repeat([]byte("CHUNK1_BODY_"), 1000)},
			{offset: totalSize - 128, data: []byte("TAIL_OVERWRITE_AT_66MIB_END_PADDING_BUFFER")},
		}

		for _, ow := range overwrites {
			_, err := f.WriteAt(ow.data, ow.offset)
			require.NoError(t, err)
			copy(fullBuf[ow.offset:int(ow.offset)+len(ow.data)], ow.data)
		}
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())

		// 3. 计算内存全量数据的预期 SHA-256
		expectedSum := sha256.Sum256(fullBuf)

		// 4. 打开读取并计算实际 SHA-256
		readF, err := os.Open(filePath)
		require.NoError(t, err)
		defer readF.Close()

		hasher := sha256.New()
		copied, err := io.Copy(hasher, readF)
		require.NoError(t, err)
		require.Equal(t, int64(totalSize), copied)

		actualSum := hasher.Sum(nil)
		require.Equal(t, expectedSum[:], actualSum, "66 MiB 跨块跨分片文件读写 SHA-256 必须 100% 一致")
	})

	t.Run("concurrent_append_o_append", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("stdout: %s\nstderr: %s\n", stdout.String(), stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		filePath := filepath.Join(mountpoint, "append-test.log")

		// 初始化空文件
		initF, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
		require.NoError(t, err)
		require.NoError(t, initF.Close())

		const (
			numWorkers     = 8
			linesPerWorker = 50
		)

		var wg sync.WaitGroup
		errCh := make(chan error, numWorkers*linesPerWorker)

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					errCh <- fmt.Errorf("worker %d open: %w", workerID, err)
					return
				}
				defer f.Close()

				for seq := 0; seq < linesPerWorker; seq++ {
					line := fmt.Sprintf("W%02d-S%04d-%040d\n", workerID, seq, 0)
					n, err := f.Write([]byte(line))
					if err != nil {
						errCh <- fmt.Errorf("worker %d write seq %d: %w", workerID, seq, err)
						return
					}
					if n != len(line) {
						errCh <- fmt.Errorf("worker %d short write seq %d: %d != %d", workerID, seq, n, len(line))
						return
					}
				}
			}(w)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err, "并发追加写不应出错")
		}

		// 读取并验证总字节数与记录完整性
		content, err := os.ReadFile(filePath)
		require.NoError(t, err)

		lines := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
		require.Len(t, lines, numWorkers*linesPerWorker, "总行数必须为 400")

		seen := make(map[string]int)
		for _, l := range lines {
			seen[l]++
		}

		for w := 0; w < numWorkers; w++ {
			for seq := 0; seq < linesPerWorker; seq++ {
				expected := fmt.Sprintf("W%02d-S%04d-%040d", w, seq, 0)
				require.Equal(t, 1, seen[expected], "每条记录必须出现且仅出现一次: %s", expected)
			}
		}
	})

	t.Run("quota_enforcement_enospc", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-disk-quota-%d", time.Now().UnixNano()),
			MaxBytes: 1 * 1024 * 1024, // 1 MiB 配额
		}))
		require.NoError(t, err)
		disk := resp.Msg.GetDisk()
		diskID := disk.GetDiskId()
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, delErr := client.DeleteDisk(cleanCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
				DiskId: diskID,
			}))
			require.NoError(t, delErr, "清理测试磁盘失败: %s", diskID)
			_, getErr := client.GetDisk(cleanCtx, connect.NewRequest(&storagev1.GetDiskRequest{
				DiskId: diskID,
			}))
			require.Error(t, getErr, "删除磁盘后 GetDisk 必须返回错误: %s", diskID)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(getErr), "删除磁盘后 GetDisk 应返回 CodeNotFound")
		})

		mountpoint := t.TempDir()
		cmd, stdout, stderr := startBowlMount(t, endpoint, diskID, mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("stdout: %s\nstderr: %s\n", stdout.String(), stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		filePath := filepath.Join(mountpoint, "quota-file.bin")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer f.Close()

		halfMB := make([]byte, 512*1024)
		n, err := f.Write(halfMB)
		require.NoError(t, err)
		require.Equal(t, len(halfMB), n)

		// 超过配额写入，必须同步返回 ENOSPC
		excessData := make([]byte, 1024*1024)
		_, writeErr := f.Write(excessData)
		require.Error(t, writeErr, "超出配额写入必须报错")
		require.True(t, errors.Is(writeErr, syscall.ENOSPC), "错误必须为 syscall.ENOSPC，实际: %v", writeErr)
	})

	t.Run("mmap_shared_sync_and_persist", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("stdout: %s\nstderr: %s\n", stdout.String(), stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		filePath := filepath.Join(mountpoint, "mmap-test.bin")
		const mmapSize = 8192

		initial := bytes.Repeat([]byte{'A'}, mmapSize)
		require.NoError(t, os.WriteFile(filePath, initial, 0644))

		f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		require.NoError(t, err)

		mmapData, err := unix.Mmap(int(f.Fd()), 0, mmapSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		require.NoError(t, err)

		copy(mmapData[100:111], []byte("MMAP_MUTATE"))
		copy(mmapData[5000:5011], []byte("MMAP_PAGE2_"))

		require.NoError(t, unix.Msync(mmapData, unix.MS_SYNC))
		require.NoError(t, unix.Munmap(mmapData))
		require.NoError(t, f.Close())

		persisted, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Len(t, persisted, mmapSize)
		require.Equal(t, []byte("MMAP_MUTATE"), persisted[100:111])
		require.Equal(t, []byte("MMAP_PAGE2_"), persisted[5000:5011])
	})

	t.Run("exec_linux_elf_binary_direct_run", func(t *testing.T) {
		disk := createDiskWithRootInoGT1(t, client)
		mountpoint := t.TempDir()

		cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		require.NoError(t, waitForMount(mountpoint, 10*time.Second), "挂载点就绪")
		defer func() {
			if t.Failed() {
				t.Logf("stdout: %s\nstderr: %s\n", stdout.String(), stderr.String())
			}
			stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
		}()

		binPath := getBowlBin()
		binData, err := os.ReadFile(binPath)
		require.NoError(t, err, "读取 host bowl 二进制失败")

		execPath := filepath.Join(mountpoint, "bowl-exec")
		require.NoError(t, os.WriteFile(execPath, binData, 0755), "写入挂载点可执行文件失败")

		execCmd := exec.Command(execPath, "version")
		out, err := execCmd.CombinedOutput()
		require.NoError(t, err, "在挂载点执行二进制失败: %s", string(out))
		require.Contains(t, string(out), "bowl version", "输出必须包含 bowl version")
	})
}
