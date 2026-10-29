package cli

import (
	"io"
	"os"
	"testing"
)

// SetStdinForTest 在单元测试中临时替换标准输入读取器并在测试结束后自动还原。
func SetStdinForTest(t testing.TB, r io.Reader) {
	old := defaultStdin
	defaultStdin = r
	t.Cleanup(func() {
		defaultStdin = old
	})
}

// SetMountRunnerForTest 在单元测试中临时替换挂载运行器并在测试结束后自动还原。
func SetMountRunnerForTest(t testing.TB, runner MountServerRunner) {
	oldRunner := defaultMountRunner
	defaultMountRunner = runner
	t.Cleanup(func() {
		defaultMountRunner = oldRunner
	})
}

// SetMountInfoPathForTest 在单元测试中临时替换 mountinfo 读取路径。
func SetMountInfoPathForTest(t testing.TB, path string) {
	oldPath := defaultMountInfoPath
	defaultMountInfoPath = path
	t.Cleanup(func() {
		defaultMountInfoPath = oldPath
	})
}

// SetSystemLogOpenerForTest 在单元测试中临时替换系统日志打开器。
func SetSystemLogOpenerForTest(t testing.TB, fn func(diskID string) (*os.File, string, error)) {
	oldFn := tryOpenSystemLog
	tryOpenSystemLog = fn
	t.Cleanup(func() {
		tryOpenSystemLog = oldFn
	})
}

// SetProcessLauncherForTest 在单元测试中临时替换守护进程启动器。
func SetProcessLauncherForTest(t testing.TB, launcher ProcessLauncher) {
	oldLauncher := defaultProcessLauncher
	defaultProcessLauncher = launcher
	t.Cleanup(func() {
		defaultProcessLauncher = oldLauncher
	})
}

// SetUmountToolRunnerForTest 在单元测试中临时替换 umount 工具查找与命令执行器。
func SetUmountToolRunnerForTest(t testing.TB, looker func(file string) (string, error), runner func(name string, args ...string) ([]byte, error)) {
	oldLooker := defaultToolLooker
	oldRunner := defaultCmdRunner
	defaultToolLooker = looker
	defaultCmdRunner = runner
	t.Cleanup(func() {
		defaultToolLooker = oldLooker
		defaultCmdRunner = oldRunner
	})
}
