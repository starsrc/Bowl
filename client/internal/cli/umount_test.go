package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func withUmountToolStub(t *testing.T, availableTool, toolPath string, out []byte, err error) (calledName *string, calledArgs *[]string) {
	t.Helper()
	prevLook := lookPath
	prevRun := runCommand

	var name string
	var args []string
	lookPath = func(file string) (string, error) {
		if file == availableTool {
			return toolPath, nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(bin string, a ...string) ([]byte, error) {
		name = bin
		args = append([]string(nil), a...)
		return out, err
	}
	t.Cleanup(func() {
		lookPath = prevLook
		runCommand = prevRun
	})
	return &name, &args
}

func TestUmount_GivenHelpAndInvalidArgs_ReturnsExpectedExitCodes(t *testing.T) {
	// Given / When / Then
	code, out, errOut := runCLI("umount", "-h")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "bowl umount <mountpoint>")
	require.Contains(t, out, "umount, unmount")

	code, out, errOut = runCLI("unmount", "--help")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "bowl umount <mountpoint>")
	require.Contains(t, out, "umount, unmount")

	code, out, errOut = runCLI("umount")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Error: accepts 1 arg(s), received 0")

	code, out, errOut = runCLI("umount", "/mnt/a", "/mnt/b")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Error: accepts 1 arg(s), received 2")

	for _, badArgs := range [][]string{
		{"umount", "--bogus", "/mnt/a"},
		{"umount", "--force=bad", "/mnt/a"},
		{"umount", "-f=bad", "/mnt/a"},
		{"umount", "-yes", "/mnt/a"},
		{"umount", "--y", "/mnt/a"},
		{"umount", "---json", "/mnt/a"},
	} {
		code, out, errOut = runCLI(badArgs...)
		require.Equal(t, 2, code, "args=%v stderr=%s", badArgs, errOut)
		require.Empty(t, out, "args=%v", badArgs)
		require.Contains(t, errOut, "Error:", "args=%v", badArgs)
	}
}

func TestUmount_GivenForceFalseFlag_PreservesNormalUnmountAndDrain(t *testing.T) {
	// Given
	mp := "/mnt/bowl-force-false"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	binName, binArgs := withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)

	// When: --force=false must use normal -u rather than lazy -uz
	code, out, errOut := runCLI("umount", "--force=false", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/usr/bin/fusermount3", *binName)
	require.Equal(t, []string{"-u", mp}, *binArgs)
}

func TestUmount_GivenUnmountedOrUnreadableMountInfo_ReturnsExit1(t *testing.T) {
	// Given: not mounted
	withMountInfoContent(t, "100 30 0:50 / /mnt/other rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("umount", "/mnt/not-mounted")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "is not a mounted filesystem")

	// Given: unreadable mountinfo
	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("umount", "/mnt/not-mounted")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "failed to check mountpoint status")
}

func TestUmount_GivenFusermount3Available_ExecutesNormalAndForceArgs(t *testing.T) {
	// Given
	mp := "/mnt/bowl-a"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	binName, binArgs := withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)

	// When: 常规卸载
	code, out, errOut := runCLI("umount", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/usr/bin/fusermount3", *binName)
	require.Equal(t, []string{"-u", mp}, *binArgs)

	// When: unmount 别名 + -f 强制卸载
	code, out, errOut = runCLI("unmount", "-f", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-uz", mp}, *binArgs)
}

func TestUmount_GivenFusermountAndUmountFallbacks_SelectsExpectedToolAndFlags(t *testing.T) {
	// Given: 仅 fusermount 可用
	mp := "/mnt/bowl-b"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	binName, binArgs := withUmountToolStub(t, "fusermount", "/bin/fusermount", nil, nil)

	// When
	code, _, errOut := runCLI("umount", "--force", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "/bin/fusermount", *binName)
	require.Equal(t, []string{"-uz", mp}, *binArgs)

	// Given: 仅系统 umount 可用
	binName, binArgs = withUmountToolStub(t, "umount", "/bin/umount", nil, nil)

	// When: 普通与 -f 模式
	code, _, errOut = runCLI("umount", mp)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "/bin/umount", *binName)
	require.Equal(t, []string{mp}, *binArgs)

	code, _, errOut = runCLI("umount", "-f", mp)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-l", mp}, *binArgs)
}

func TestUmount_GivenMissingToolOrToolFailure_ReturnsExit1WithErrorMessage(t *testing.T) {
	// Given: no unmount tool available
	mp := "/mnt/bowl-c"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	withUmountToolStub(t, "none", "", nil, nil)

	// When
	code, _, errOut := runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "no system unmount tool found")

	// Given: unmount tool returns failure
	withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", []byte("device busy"), errors.New("exit status 1"))

	// When
	code, _, errOut = runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "unmount failed")
	require.Contains(t, errOut, "device busy")
}

func withUmountDrainStub(t *testing.T, timeout, poll time.Duration) {
	t.Helper()
	prevTimeout := umountDrainTimeout
	prevPoll := umountDrainPollInterval
	umountDrainTimeout = timeout
	umountDrainPollInterval = poll
	t.Cleanup(func() {
		umountDrainTimeout = prevTimeout
		umountDrainPollInterval = prevPoll
	})
}

func TestUmount_GivenActiveMountMeta_WaitsForDrainOnNormalAndTimesOutWhenStuck(t *testing.T) {
	// Given: 常规卸载前存在 mountMeta，卸载命令返回 0 后异步清理 mountMeta
	mp := "/mnt/bowl-drain"
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	withUmountDrainStub(t, 500*time.Millisecond, 5*time.Millisecond)

	metaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: mp,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	prevLook := lookPath
	prevRun := runCommand
	lookPath = func(file string) (string, error) {
		if file == "fusermount3" {
			return "/usr/bin/fusermount3", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, _ ...string) ([]byte, error) {
		go func() {
			time.Sleep(30 * time.Millisecond)
			_ = os.Remove(metaPath)
		}()
		return nil, nil
	}
	t.Cleanup(func() {
		lookPath = prevLook
		runCommand = prevRun
	})

	// When
	code, out, errOut := runCLI("umount", mp)

	// Then: 等待 mountMeta 消失后才返回 0
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	_, statErr := os.Stat(metaPath)
	require.True(t, os.IsNotExist(statErr), "bowl umount 返回 0 时 mountMeta 必须已消失")

	// Given: 常规卸载等待超时（mountMeta 始终未消失且 PID 存活）
	metaPath2, err := writeMountMeta(&mountMeta{
		Mountpoint: mp,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	withUmountDrainStub(t, 30*time.Millisecond, 5*time.Millisecond)
	runCommand = func(_ string, _ ...string) ([]byte, error) {
		return nil, nil
	}

	// When
	code, _, errOut = runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "timed out")

	// Given: -f 强制卸载即使 mountMeta 仍存在也不等待，直接返回 0
	_, statErr2 := os.Stat(metaPath2)
	require.NoError(t, statErr2)
	code, out, errOut = runCLI("umount", "-f", mp)
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
}

func TestUmount_GivenDefaultDrainTimingAndNoTimeoutFlag_Enforces5mBudgetAndRejectsTimeoutFlag(t *testing.T) {
	// Given / When / Then
	require.Equal(t, 5*time.Minute, umountDrainTimeout)
	require.Equal(t, 20*time.Millisecond, umountDrainPollInterval)

	code, out, errOut := runCLI("umount", "--timeout", "1s", "/mnt/a")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "unknown flag: --timeout")
}

func withUmountProgressSeams(
	t *testing.T,
	isTTY bool,
	client *http.Client,
	drainTimeout, drainPoll, progressPoll, nonTTYInterval, fallbackGrace, fallbackInterval time.Duration,
) {
	t.Helper()
	prevTTY := umountIsTerminal
	prevClient := umountHTTPClient
	prevTimeout := umountDrainTimeout
	prevDrainPoll := umountDrainPollInterval
	prevProgPoll := umountProgressPollInterval
	prevNonTTY := umountNonTTYLogInterval
	prevGrace := umountFallbackGracePeriod
	prevFallback := umountFallbackLogInterval

	umountIsTerminal = func(io.Writer) bool { return isTTY }
	if client != nil {
		umountHTTPClient = client
	}
	umountDrainTimeout = drainTimeout
	umountDrainPollInterval = drainPoll
	umountProgressPollInterval = progressPoll
	umountNonTTYLogInterval = nonTTYInterval
	umountFallbackGracePeriod = fallbackGrace
	umountFallbackLogInterval = fallbackInterval

	t.Cleanup(func() {
		umountIsTerminal = prevTTY
		umountHTTPClient = prevClient
		umountDrainTimeout = prevTimeout
		umountDrainPollInterval = prevDrainPoll
		umountProgressPollInterval = prevProgPoll
		umountNonTTYLogInterval = prevNonTTY
		umountFallbackGracePeriod = prevGrace
		umountFallbackLogInterval = prevFallback
	})
}

func TestUmount_GivenStaleDeadMetaAndZeroTotal_PrefersAlivePIDAndEmitsNoOutput(t *testing.T) {
	for _, isTTY := range []bool{false, true} {
		name := "NonTTY"
		if isTTY {
			name = "TTY"
		}
		t.Run(name, func(t *testing.T) {
			// Given
			mp := "/mnt/bowl-zero-progress"
			dir1 := t.TempDir()
			dir2 := t.TempDir()
			withTempMountMetaDirs(t, dir1, dir2)
			withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

			staleData, err := json.Marshal(mountMeta{
				Mountpoint: mp,
				PID:        99999999,
				DiskID:     testDiskID1,
				AdminAddr:  "127.0.0.1:19991",
				StartedAt:  time.Now().UTC(),
			})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir1, mountMetaFileName(mp)), staleData, 0o644))

			liveData, err := json.Marshal(mountMeta{
				Mountpoint: mp,
				PID:        os.Getpid(),
				DiskID:     testDiskID1,
				AdminAddr:  "127.0.0.1:19992",
				StartedAt:  time.Now().UTC(),
			})
			require.NoError(t, err)
			liveMetaPath := filepath.Join(dir2, mountMetaFileName(mp))
			require.NoError(t, os.WriteFile(liveMetaPath, liveData, 0o644))

			var queriedURLs []string
			var urlMu sync.Mutex
			client := &http.Client{
				Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					urlMu.Lock()
					queriedURLs = append(queriedURLs, req.URL.String())
					urlMu.Unlock()
					body, _ := json.Marshal(fusefs.UnmountProgress{Active: true, Completed: 0, Total: 0})
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
						Body:       io.NopCloser(bytes.NewReader(body)),
						Request:    req,
					}, nil
				}),
			}
			withUmountProgressSeams(
				t,
				isTTY,
				client,
				500*time.Millisecond,
				5*time.Millisecond,
				10*time.Millisecond,
				40*time.Millisecond,
				30*time.Millisecond,
				40*time.Millisecond,
			)

			prevLook := lookPath
			prevRun := runCommand
			lookPath = func(file string) (string, error) {
				if file == "fusermount3" {
					return "/usr/bin/fusermount3", nil
				}
				return "", errors.New("not found")
			}
			runCommand = func(_ string, _ ...string) ([]byte, error) {
				go func() {
					time.Sleep(45 * time.Millisecond)
					_ = os.Remove(liveMetaPath)
				}()
				return nil, nil
			}
			t.Cleanup(func() {
				lookPath = prevLook
				runCommand = prevRun
			})

			// When
			code, out, errOut := runCLI("umount", mp)

			// Then
			require.Equal(t, 0, code)
			require.Empty(t, out)
			require.Empty(t, errOut)
			_, statErr := os.Stat(liveMetaPath)
			require.True(t, os.IsNotExist(statErr), "live mountMeta must be removed before umount exits")

			urlMu.Lock()
			defer urlMu.Unlock()
			require.NotEmpty(t, queriedURLs, "umount must query /debug/unmount on the alive admin address")
			for _, u := range queriedURLs {
				require.Equal(t, "http://127.0.0.1:19992/debug/unmount", u)
			}
		})
	}
}

func TestUmount_GivenNonTTYAndPendingWork_LogsPlainLinesAtIntervalAndFinal100Percent(t *testing.T) {
	// Given: verify default polling and logging cadences before overriding seams.
	require.Equal(t, 100*time.Millisecond, umountProgressPollInterval)
	require.Equal(t, 2*time.Second, umountNonTTYLogInterval)
	require.Equal(t, 1*time.Second, umountFallbackGracePeriod)
	require.Equal(t, 5*time.Second, umountFallbackLogInterval)

	t.Run("PrintsFirstIntervalAndFinal100Percent", func(t *testing.T) {
		mp := "/mnt/bowl-nontty-progress"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "127.0.0.1:19993",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		var pollNum atomic.Int64
		var req1TimeMu sync.Mutex
		var req1Time time.Time
		midPrintedCh := make(chan struct{})
		var midOnce sync.Once
		client := &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				n := pollNum.Add(1)
				var completed uint64
				if n == 1 {
					req1TimeMu.Lock()
					req1Time = time.Now()
					req1TimeMu.Unlock()
					completed = 0
				} else {
					completed = 2
					req1TimeMu.Lock()
					elapsed := time.Since(req1Time)
					req1TimeMu.Unlock()
					if elapsed >= 45*time.Millisecond {
						midOnce.Do(func() { close(midPrintedCh) })
					}
				}
				body, _ := json.Marshal(fusefs.UnmountProgress{
					Active:    true,
					Completed: completed,
					Total:     4,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}, nil
			}),
		}
		withUmountProgressSeams(
			t,
			false,
			client,
			2*time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			35*time.Millisecond,
			time.Second,
			time.Second,
		)

		prevLook := lookPath
		prevRun := runCommand
		lookPath = func(file string) (string, error) {
			if file == "fusermount3" {
				return "/usr/bin/fusermount3", nil
			}
			return "", errors.New("not found")
		}
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				<-midPrintedCh
				time.Sleep(15 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() {
			lookPath = prevLook
			runCommand = prevRun
		})

		// When
		code, out, errOut := runCLI("umount", mp)

		// Then
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.NotContains(t, errOut, "\r")
		require.NotContains(t, errOut, "\x1b")

		lines := strings.Split(strings.TrimSuffix(errOut, "\n"), "\n")
		require.GreaterOrEqual(t, len(lines), 3, "expected initial, interval, and final lines, got stderr=%q", errOut)
		require.Equal(t, "Syncing to cloud: 0% (0/4)", lines[0])
		require.Contains(t, errOut, "Syncing to cloud: 50% (2/4)\n")
		require.Equal(t, "Syncing to cloud: 100% (4/4)", lines[len(lines)-1])
		require.Equal(t, 1, strings.Count(errOut, "100% (4/4)"))
	})

	t.Run("SkipsDuplicateFinalLineWhen100PercentAlreadyPrinted", func(t *testing.T) {
		mp := "/mnt/bowl-nontty-dedup100"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "127.0.0.1:19994",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		client := &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				body, _ := json.Marshal(fusefs.UnmountProgress{
					Active:    false,
					Completed: 4,
					Total:     4,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}, nil
			}),
		}
		withUmountProgressSeams(
			t,
			false,
			client,
			time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			50*time.Millisecond,
			time.Second,
			time.Second,
		)

		prevLook := lookPath
		prevRun := runCommand
		lookPath = func(file string) (string, error) {
			if file == "fusermount3" {
				return "/usr/bin/fusermount3", nil
			}
			return "", errors.New("not found")
		}
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				time.Sleep(25 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() {
			lookPath = prevLook
			runCommand = prevRun
		})

		// When
		code, out, errOut := runCLI("umount", mp)

		// Then
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.Equal(t, "Syncing to cloud: 100% (4/4)\n", errOut)
	})
}

func TestUmount_GivenTTYAndPendingWork_RendersSingleLineProgressBarTo100PercentWithNewline(t *testing.T) {
	// Given
	mp := "/mnt/bowl-tty-progress"
	withTempMountMetaDirs(t, t.TempDir())
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	metaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: mp,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "127.0.0.1:19995",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	var pollNum atomic.Int64
	reachedMidCh := make(chan struct{})
	var midOnce sync.Once
	client := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			n := pollNum.Add(1)
			var completed uint64
			switch n {
			case 1:
				completed = 0
			case 2:
				completed = 2
				midOnce.Do(func() { close(reachedMidCh) })
			default:
				completed = 3
			}
			body, _ := json.Marshal(fusefs.UnmountProgress{
				Active:    true,
				Completed: completed,
				Total:     4,
			})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
				Body:       io.NopCloser(bytes.NewReader(body)),
				Request:    req,
			}, nil
		}),
	}
	withUmountProgressSeams(
		t,
		true,
		client,
		2*time.Second,
		5*time.Millisecond,
		10*time.Millisecond,
		time.Second,
		time.Second,
		time.Second,
	)

	prevLook := lookPath
	prevRun := runCommand
	lookPath = func(file string) (string, error) {
		if file == "fusermount3" {
			return "/usr/bin/fusermount3", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, _ ...string) ([]byte, error) {
		go func() {
			<-reachedMidCh
			time.Sleep(15 * time.Millisecond)
			_ = os.Remove(metaPath)
		}()
		return nil, nil
	}
	t.Cleanup(func() {
		lookPath = prevLook
		runCommand = prevRun
	})

	// When
	code, out, errOut := runCLI("umount", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "Syncing to cloud")
	require.Contains(t, errOut, "\r")
	require.Contains(t, errOut, "50%")
	require.Contains(t, errOut, "100%")
	require.Contains(t, errOut, "(4/4)")
	require.True(t, strings.HasSuffix(errOut, "\n"), "TTY progress bar must end with newline, got=%q", errOut)
	require.Equal(t, 1, strings.Count(errOut, "\n"), "TTY progress bar must stay single-line until final newline, got=%q", errOut)
}

func TestUmount_GivenAdminOffOrUnreachable_StaysSilentDuringGraceAndShowsIndeterminateAfterGrace(t *testing.T) {
	t.Run("SilentDuringGracePeriod", func(t *testing.T) {
		for _, isTTY := range []bool{false, true} {
			mp := "/mnt/bowl-fallback-silent"
			withTempMountMetaDirs(t, t.TempDir())
			withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

			metaPath, err := writeMountMeta(&mountMeta{
				Mountpoint: mp,
				PID:        os.Getpid(),
				DiskID:     testDiskID1,
				AdminAddr:  "off",
				StartedAt:  time.Now().UTC(),
			})
			require.NoError(t, err)

			withUmountProgressSeams(
				t,
				isTTY,
				nil,
				time.Second,
				5*time.Millisecond,
				10*time.Millisecond,
				time.Second,
				150*time.Millisecond,
				time.Second,
			)
			withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
			prevRun := runCommand
			runCommand = func(_ string, _ ...string) ([]byte, error) {
				go func() {
					time.Sleep(20 * time.Millisecond)
					_ = os.Remove(metaPath)
				}()
				return nil, nil
			}
			t.Cleanup(func() { runCommand = prevRun })

			code, out, errOut := runCLI("umount", mp)
			require.Equal(t, 0, code)
			require.Empty(t, out)
			require.Empty(t, errOut)
		}
	})

	t.Run("NonTTY_AdminOff_LogsAtGraceAndInterval", func(t *testing.T) {
		mp := "/mnt/bowl-fallback-nontty-off"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "off",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		withUmountProgressSeams(
			t,
			false,
			nil,
			2*time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			time.Second,
			20*time.Millisecond,
			30*time.Millisecond,
		)
		withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
		prevRun := runCommand
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				time.Sleep(70 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() { runCommand = prevRun })

		code, out, errOut := runCLI("umount", mp)
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.NotContains(t, errOut, "\r")
		require.NotContains(t, errOut, "\x1b")
		require.NotContains(t, errOut, "%")
		require.GreaterOrEqual(t, strings.Count(errOut, "Syncing to cloud...\n"), 2, "stderr=%q", errOut)
	})

	t.Run("NonTTY_UnreachableEndpoint_FallsBackAfterGrace", func(t *testing.T) {
		mp := "/mnt/bowl-fallback-nontty-err"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "127.0.0.1:19996",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		client := &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("connection refused")
			}),
		}
		withUmountProgressSeams(
			t,
			false,
			client,
			2*time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			time.Second,
			20*time.Millisecond,
			35*time.Millisecond,
		)
		withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
		prevRun := runCommand
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				time.Sleep(50 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() { runCommand = prevRun })

		code, out, errOut := runCLI("umount", mp)
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.Contains(t, errOut, "Syncing to cloud...\n")
	})

	t.Run("TTY_AdminOff_RendersSpinnerAndFinishesCleanly", func(t *testing.T) {
		mp := "/mnt/bowl-fallback-tty-spinner"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		withUmountProgressSeams(
			t,
			true,
			nil,
			2*time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			time.Second,
			20*time.Millisecond,
			time.Second,
		)
		withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
		prevRun := runCommand
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				time.Sleep(60 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() { runCommand = prevRun })

		code, out, errOut := runCLI("umount", mp)
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.Contains(t, errOut, "Syncing to cloud")
		require.Contains(t, errOut, "\r")
		require.NotContains(t, errOut, "%")
		require.True(t, strings.HasSuffix(errOut, "\n"), "TTY spinner must finish with newline, got=%q", errOut)
		require.Equal(t, 1, strings.Count(errOut, "\n"), "TTY spinner must stay single-line until Finish, got=%q", errOut)
	})

	t.Run("TTY_UnreachableThenRecovered_TransitionsCleanlyToDeterminate", func(t *testing.T) {
		mp := "/mnt/bowl-fallback-recover"
		withTempMountMetaDirs(t, t.TempDir())
		withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

		metaPath, err := writeMountMeta(&mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     testDiskID1,
			AdminAddr:  "127.0.0.1:19997",
			StartedAt:  time.Now().UTC(),
		})
		require.NoError(t, err)

		var allowSuccess atomic.Bool
		var recovered atomic.Bool
		recoveredCh := make(chan struct{})
		var recOnce sync.Once
		client := &http.Client{
			Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if !allowSuccess.Load() {
					return nil, errors.New("connection refused")
				}
				recovered.Store(true)
				recOnce.Do(func() { close(recoveredCh) })
				body, _ := json.Marshal(fusefs.UnmountProgress{
					Active:    true,
					Completed: 1,
					Total:     2,
				})
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}, nil
			}),
		}
		withUmountProgressSeams(
			t,
			true,
			client,
			2*time.Second,
			5*time.Millisecond,
			10*time.Millisecond,
			time.Second,
			15*time.Millisecond,
			time.Second,
		)
		withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
		prevRun := runCommand
		runCommand = func(_ string, _ ...string) ([]byte, error) {
			go func() {
				time.Sleep(35 * time.Millisecond)
				allowSuccess.Store(true)
				<-recoveredCh
				time.Sleep(15 * time.Millisecond)
				_ = os.Remove(metaPath)
			}()
			return nil, nil
		}
		t.Cleanup(func() { runCommand = prevRun })

		code, out, errOut := runCLI("umount", mp)
		require.Equal(t, 0, code)
		require.Empty(t, out)
		require.True(t, recovered.Load())
		require.Contains(t, errOut, "Syncing to cloud")
		require.Contains(t, errOut, "100%")
		require.Contains(t, errOut, "(2/2)")
		require.True(t, strings.HasSuffix(errOut, "\n"))
	})
}

func TestUmount_GivenDefaultTerminalDetector_ReturnsFalseForBuffersAndPipes(t *testing.T) {
	var nilFile *os.File
	require.False(t, umountIsTerminal(nil))
	require.False(t, umountIsTerminal(nilFile))
	require.False(t, umountIsTerminal(&bytes.Buffer{}))

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer func() {
		_ = r.Close()
		_ = w.Close()
	}()
	require.False(t, umountIsTerminal(w))
}

func TestUmount_GivenSymlinkMountpoint_PassesCanonicalPathToToolAndDrainsMeta(t *testing.T) {
	// Given
	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := canonicalMountpoint(realMP)
	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withMountInfoContent(t, "100 30 0:50 / "+canonMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	withUmountDrainStub(t, 500*time.Millisecond, 5*time.Millisecond)

	metaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: canonMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	_, _ = withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
	prevRun := runCommand
	var calledBin string
	var calledArgs []string
	runCommand = func(bin string, args ...string) ([]byte, error) {
		calledBin = bin
		calledArgs = append([]string(nil), args...)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = os.Remove(metaPath)
		}()
		return nil, nil
	}
	t.Cleanup(func() { runCommand = prevRun })

	// When
	code, out, errOut := runCLI("umount", symMP)

	// Then
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/usr/bin/fusermount3", calledBin)
	require.Equal(t, []string{"-u", canonMP}, calledArgs)
	_, statErr := os.Stat(metaPath)
	require.True(t, os.IsNotExist(statErr), "mountMeta must be drained after umount via symlink")
}

func TestUmount_GivenUnmountedSymlink_ReportsOriginalAbsPath(t *testing.T) {
	// Given
	realMP := filepath.Join(t.TempDir(), "unmounted-real")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	symMP := filepath.Join(t.TempDir(), "unmounted-sym")
	require.NoError(t, os.Symlink(realMP, symMP))
	withMountInfoContent(t, "100 30 0:50 / /mnt/other rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("umount", symMP)

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, filepath.Clean(symMP))
	require.Contains(t, errOut, "is not a mounted filesystem")
}

func TestUmount_GivenWedgedAndHealthyMounts_ResolvesUmountMetaAndUnmountsWithoutMountRootLstat(t *testing.T) {
	// Given: realWedgedMP and realOkMP are active mount roots in mountinfo
	const (
		realWedgedMP = "/wedged-root/mnt"
		realOkMP     = "/ok-root/mnt"
		symWedgedMP  = "/tmp/sym-wedged"
		symOkMP      = "/tmp/sym-ok"
	)
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withMountInfoContent(t,
		"30 61 8:1 / / rw - ext4 /dev/sda1 rw\n"+
			"100 30 0:50 / "+realWedgedMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n"+
			"101 30 0:51 / "+realOkMP+" rw - fuse.bowl bowl:"+testDiskID2+" rw\n",
	)
	lstatCalls := withMountInfoNoRootLstatStub(t, []string{realWedgedMP, realOkMP}, map[string]fakeFSNode{
		"/tmp":      {mode: os.ModeDir | 0o755},
		symWedgedMP: {mode: os.ModeSymlink | 0o777, target: realWedgedMP},
		symOkMP:     {mode: os.ModeSymlink | 0o777, target: realOkMP},
	})
	withUmountDrainStub(t, 500*time.Millisecond, 5*time.Millisecond)

	_, err := writeMountMeta(&mountMeta{
		Mountpoint: realWedgedMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	okMetaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: realOkMP,
		PID:        os.Getpid(),
		DiskID:     testDiskID2,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	// When: resolving umount metadata via symlinked paths
	gotWedgedPath, gotWedgedMeta, okWedged := resolveUmountMountMeta(symWedgedMP)
	require.True(t, okWedged)
	require.Equal(t, filepath.Join(metaDir, mountMetaFileName(realWedgedMP)), gotWedgedPath)
	require.Equal(t, realWedgedMP, gotWedgedMeta.Mountpoint)

	gotOkPath, gotOkMeta, okHealthy := resolveUmountMountMeta(symOkMP)
	require.True(t, okHealthy)
	require.Equal(t, okMetaPath, gotOkPath)
	require.Equal(t, realOkMP, gotOkMeta.Mountpoint)

	// When: normal unmount via symOkMP while realWedgedMP is mounted
	_, _ = withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)
	prevRun := runCommand
	var calledArgs []string
	runCommand = func(_ string, args ...string) ([]byte, error) {
		calledArgs = append([]string(nil), args...)
		go func() {
			time.Sleep(15 * time.Millisecond)
			_ = os.Remove(okMetaPath)
		}()
		return nil, nil
	}
	t.Cleanup(func() { runCommand = prevRun })

	code, out, errOut := runCLI("umount", symOkMP)
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-u", realOkMP}, calledArgs)
	_, statErr := os.Stat(okMetaPath)
	require.True(t, os.IsNotExist(statErr))

	// When: force unmount via symWedgedMP
	code, out, errOut = runCLI("umount", "-f", symWedgedMP)
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-uz", realWedgedMP}, calledArgs)

	require.NotContains(t, *lstatCalls, realWedgedMP)
	require.NotContains(t, *lstatCalls, realOkMP)
}

func TestUmount_GivenForceAndWedgedMountPaths_AvoidsLstatOnMountRootAndPassesCanonicalPath(t *testing.T) {
	// Given: /tmp/sym points to /data/mnt while /data/mnt is mounted, so Lstat on /data/mnt must not run
	withMountInfoContent(t, "30 61 8:1 / / rw - ext4 /dev/sda1 rw\n100 30 0:50 / /data/mnt rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	lstatCalls := withMountInfoFSStub(t, map[string]fakeFSNode{
		"/tmp":     {mode: os.ModeDir | 0o755},
		"/tmp/sym": {mode: os.ModeSymlink | 0o777, target: "/data/mnt"},
	})
	binName, binArgs := withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)

	// When: force umount via direct canonical path invokes zero Lstat calls
	code, out, errOut := runCLI("umount", "-f", "/data/mnt")

	// Then
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/usr/bin/fusermount3", *binName)
	require.Equal(t, []string{"-uz", "/data/mnt"}, *binArgs)
	require.Empty(t, *lstatCalls)

	// When: force umount via symlink resolves to /data/mnt without calling Lstat on /data/mnt
	code, out, errOut = runCLI("umount", "-f", "/tmp/sym")

	// Then
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-uz", "/data/mnt"}, *binArgs)
	require.Equal(t, []string{"/tmp", "/tmp/sym"}, *lstatCalls)
}
