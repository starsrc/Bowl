package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSboxSplitCpSpec_识别沙箱侧并剥离容器名前缀(t *testing.T) {
	id, path, inSandbox := sboxSplitCpSpec("abc:/mnt/data/a.txt")
	assert.True(t, inSandbox)
	assert.Equal(t, "abc", id)
	assert.Equal(t, "/mnt/data/a.txt", path)

	id, _, inSandbox = sboxSplitCpSpec("bowl-sbx-abc:/mnt/data")
	assert.True(t, inSandbox)
	assert.Equal(t, "abc", id)

	for _, local := range []string{"/tmp/a.txt", "./a.txt", "/tmp/a:b/c", "", "relative.name"} {
		_, path, inSandbox = sboxSplitCpSpec(local)
		assert.False(t, inSandbox, "local path %q", local)
		assert.Equal(t, local, path)
	}
}

func TestSboxHoistFlags_把标志挪到位置参数前(t *testing.T) {
	got := sboxHoistFlags(
		[]string{"sbx-1", "--count", "2", "-q", "--image=ubuntu:24.04"},
		[]string{"count", "image"},
	)
	assert.Equal(t, []string{"--count", "2", "-q", "--image=ubuntu:24.04", "sbx-1"}, got)
}

func TestSandboxExecFlags_参数透传与前置标志(t *testing.T) {
	bg, cwd, rest, code, done := sandboxExecFlags([]string{"-b", "--cwd", "/src", "sbx-1", "--", "npm", "run", "dev"})
	assert.False(t, done)
	assert.Equal(t, 0, code)
	assert.True(t, bg)
	assert.Equal(t, "/src", cwd)
	assert.Equal(t, []string{"sbx-1", "--", "npm", "run", "dev"}, rest)

	// 没有沙箱 ID 时为用法错误
	_, _, _, code, done = sandboxExecFlags([]string{"-b"})
	assert.True(t, done)
	assert.Equal(t, 0, code)

	// 未知前置标志直接判错，不当成命令
	_, _, _, code, done = sandboxExecFlags([]string{"--nope", "sbx-1", "ls"})
	assert.True(t, done)
	assert.Equal(t, 2, code)
}
