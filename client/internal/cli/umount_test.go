package cli_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func TestUmount_ArgValidation(t *testing.T) {
	t.Run("missing positional argument returns exit code 2", func(t *testing.T) {
		for _, cmd := range []string{"umount", "unmount"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{cmd}, &stdout, &stderr)
			require.Equal(t, 2, code)
			require.Contains(t, stderr.String(), "用法:")
		}
	})

	t.Run("unknown flag returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"umount", "--invalid-flag", "/mnt/foo"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "未知标志")
	})

	t.Run("help flag returns exit code 0", func(t *testing.T) {
		for _, arg := range []string{"--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"umount", arg}, &stdout, &stderr)
			require.Equal(t, 0, code)
			require.Contains(t, stdout.String(), "用法:")
		}
	})
}

func TestUmount_NotMountedRejection(t *testing.T) {
	tmpDir := t.TempDir()
	sampleMountInfo := "23 61 0:21 / /sys rw,nosuid shared:7 - sysfs sysfs rw\n"
	mountinfoFile := filepath.Join(tmpDir, "mountinfo")
	require.NoError(t, os.WriteFile(mountinfoFile, []byte(sampleMountInfo), 0o644))
	cli.SetMountInfoPathForTest(t, mountinfoFile)

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"umount", "/mnt/not-mounted"}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Contains(t, stderr.String(), "不是已挂载")
}

func TestUmount_ToolPriorityAndFlags(t *testing.T) {
	tmpDir := t.TempDir()
	targetMP := filepath.Join(tmpDir, "mounted_point")
	require.NoError(t, os.Mkdir(targetMP, 0o755))

	sampleMountInfo := "100 30 0:50 / " + targetMP + " rw,nosuid shared:55 - fuse.bowl bowl rw\n"
	mountinfoFile := filepath.Join(tmpDir, "mountinfo")
	require.NoError(t, os.WriteFile(mountinfoFile, []byte(sampleMountInfo), 0o644))
	cli.SetMountInfoPathForTest(t, mountinfoFile)

	t.Run("prefers fusermount3 normal", func(t *testing.T) {
		var executedTool string
		var executedArgs []string
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				return "/usr/bin/" + tool, nil
			},
			func(name string, args ...string) ([]byte, error) {
				executedTool = name
				executedArgs = args
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"umount", targetMP}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/usr/bin/fusermount3", executedTool)
		require.Equal(t, []string{"-u", targetMP}, executedArgs)
	})

	t.Run("fusermount3 with force flag before mountpoint", func(t *testing.T) {
		var executedTool string
		var executedArgs []string
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				return "/usr/bin/" + tool, nil
			},
			func(name string, args ...string) ([]byte, error) {
				executedTool = name
				executedArgs = args
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"unmount", "-f", targetMP}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/usr/bin/fusermount3", executedTool)
		require.Equal(t, []string{"-uz", targetMP}, executedArgs)
	})

	t.Run("fusermount3 with force flag after mountpoint", func(t *testing.T) {
		var executedTool string
		var executedArgs []string
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				return "/usr/bin/" + tool, nil
			},
			func(name string, args ...string) ([]byte, error) {
				executedTool = name
				executedArgs = args
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"umount", targetMP, "--force"}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/usr/bin/fusermount3", executedTool)
		require.Equal(t, []string{"-uz", targetMP}, executedArgs)
	})

	t.Run("falls back to fusermount if fusermount3 missing", func(t *testing.T) {
		var executedTool string
		var executedArgs []string
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				if tool == "fusermount3" {
					return "", errors.New("not found")
				}
				return "/usr/bin/" + tool, nil
			},
			func(name string, args ...string) ([]byte, error) {
				executedTool = name
				executedArgs = args
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"umount", "-f", targetMP}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/usr/bin/fusermount", executedTool)
		require.Equal(t, []string{"-uz", targetMP}, executedArgs)
	})

	t.Run("falls back to umount if both fusermount missing", func(t *testing.T) {
		var executedTool string
		var executedArgs []string
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				if tool == "fusermount3" || tool == "fusermount" {
					return "", errors.New("not found")
				}
				return "/bin/umount", nil
			},
			func(name string, args ...string) ([]byte, error) {
				executedTool = name
				executedArgs = args
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		// 常规 umount <mp>
		code := cli.Execute([]string{"umount", targetMP}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/bin/umount", executedTool)
		require.Equal(t, []string{targetMP}, executedArgs)

		// 强制 umount -l <mp>
		code = cli.Execute([]string{"umount", "--force", targetMP}, &stdout, &stderr)
		require.Equal(t, 0, code)
		require.Equal(t, "/bin/umount", executedTool)
		require.Equal(t, []string{"-l", targetMP}, executedArgs)
	})

	t.Run("returns error when no umount tools found", func(t *testing.T) {
		cli.SetUmountToolRunnerForTest(t,
			func(tool string) (string, error) {
				return "", errors.New("not found")
			},
			func(name string, args ...string) ([]byte, error) {
				return nil, nil
			},
		)

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"umount", targetMP}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Contains(t, stderr.String(), "未找到系统卸载工具")
	})
}
