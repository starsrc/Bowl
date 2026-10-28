package cli

import (
	"testing"
)

// SetMountRunnerForTest 在单元测试中临时替换挂载运行器并在测试结束后自动还原。
func SetMountRunnerForTest(t testing.TB, runner MountServerRunner) {
	oldRunner := defaultMountRunner
	defaultMountRunner = runner
	t.Cleanup(func() {
		defaultMountRunner = oldRunner
	})
}
