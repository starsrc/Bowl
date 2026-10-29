package cli_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
	"github.com/starsrc/bowl/client/internal/version"
)

func TestCLI_Version(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "subcommand version", args: []string{"version"}},
		{name: "flag --version", args: []string{"--version"}},
		{name: "flag -v", args: []string{"-v"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := cli.Execute(tc.args, &stdout, &stderr)

			require.Equal(t, 0, exitCode, "版本查询应返回退出码 0")
			require.Empty(t, stderr.String(), "stderr 应为空")
			require.Contains(t, stdout.String(), version.Version, "stdout 应包含版本号")
			require.True(t, strings.HasPrefix(strings.TrimSpace(stdout.String()), "bowl version"), "输出格式应为 bowl version <version>")
		})
	}
}

func TestCLI_Help(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "flag --help", args: []string{"--help"}},
		{name: "flag -h", args: []string{"-h"}},
		{name: "subcommand help", args: []string{"help"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := cli.Execute(tc.args, &stdout, &stderr)

			require.Equal(t, 0, exitCode, "帮助信息应返回退出码 0")
			require.Empty(t, stderr.String(), "stderr 应为空")
			out := stdout.String()
			require.Contains(t, out, "用法:", "帮助信息应包含用法")
			require.Contains(t, out, "disk", "帮助信息应包含 disk 命令")
			require.Contains(t, out, "mount", "帮助信息应包含 mount 命令")
			require.Contains(t, out, "umount", "帮助信息应包含 umount 命令")
			require.Contains(t, out, "version", "帮助信息应包含 version 命令")
		})
	}
}

func TestCLI_DiskSubcommandRouting(t *testing.T) {
	t.Run("disk help", func(t *testing.T) {
		for _, args := range [][]string{
			{"disk", "--help"},
			{"disk", "-h"},
			{"disk", "help"},
		} {
			var stdout, stderr bytes.Buffer
			code := cli.Execute(args, &stdout, &stderr)
			require.Equal(t, 0, code, "args: %v", args)
			require.Empty(t, stderr.String())
			out := stdout.String()
			require.Contains(t, out, "bowl disk <subcommand>")
			require.Contains(t, out, "create")
			require.Contains(t, out, "list")
			require.Contains(t, out, "get")
			require.Contains(t, out, "update")
			require.Contains(t, out, "resize")
			require.Contains(t, out, "delete")
		}
	})

	t.Run("disk without subcommand returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "bowl disk <subcommand>")
	})

	t.Run("disk unknown subcommand returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"disk", "unknown-sub"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Empty(t, stdout.String())
		require.Contains(t, stderr.String(), "未知")
		require.Contains(t, stderr.String(), "unknown-sub")
	})
}

func TestCLI_NoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := cli.Execute([]string{}, &stdout, &stderr)

	require.NotEqual(t, 0, exitCode, "无参数调用应返回非零退出码")
	require.Empty(t, stdout.String(), "stdout 应为空")
	errOut := stderr.String()
	require.Contains(t, errOut, "用法:", "stderr 应打印使用说明")
	require.Contains(t, errOut, "mount", "stderr 应包含可用命令")
}

func TestCLI_UnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := cli.Execute([]string{"nonexistent"}, &stdout, &stderr)

	require.NotEqual(t, 0, exitCode, "未知子命令应返回非零退出码")
	require.Empty(t, stdout.String(), "stdout 应为空")
	errOut := stderr.String()
	require.Contains(t, errOut, "未知命令", "stderr 应包含未知命令提示")
	require.Contains(t, errOut, "nonexistent", "stderr 应包含未知命令名称")
	require.Contains(t, errOut, "用法:", "stderr 应包含使用提示")
}
