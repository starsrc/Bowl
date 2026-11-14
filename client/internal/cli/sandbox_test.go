package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/starsrc/bowl/client/internal/sandbox"
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

func TestSboxConflictingWriter_识别写者冲突(t *testing.T) {
	writers := []sandbox.Writer{
		{SandboxID: "sbx-a", Path: "/mnt/x"},
	}
	// 别的沙箱在写 → 冲突
	w, ok := sboxConflictingWriter(writers, "sbx-b", "/mnt/y")
	assert.True(t, ok)
	assert.Equal(t, "sbx-a", w.SandboxID)

	// 同一沙箱同一路径 → 视为刷新，不冲突
	_, ok = sboxConflictingWriter(writers, "sbx-a", "/mnt/x")
	assert.False(t, ok)

	// 同一沙箱不同路径 → 仍然是两个写者，冲突
	_, ok = sboxConflictingWriter(writers, "sbx-a", "/mnt/other")
	assert.True(t, ok)

	// 没有写者
	_, ok = sboxConflictingWriter(nil, "sbx-b", "/mnt/y")
	assert.False(t, ok)
}

func TestDiskMove_参数校验(t *testing.T) {
	t.Setenv("BOWL_CONFIG_DIR", t.TempDir())
	var out, errOut bytes.Buffer
	assert.Equal(t, 2, Execute([]string{"disk", "move", "d1"}, &out, &errOut), "缺少 --to")
	errOut.Reset()
	assert.Equal(t, 2, Execute([]string{"disk", "move", "d1", "--to", "http://127.0.0.1:9100", "--from", "http://127.0.0.1:9100"}, &out, &errOut))
	assert.Contains(t, errOut.String(), "同一个节点")
	assert.Equal(t, 0, Execute([]string{"disk", "move", "--help"}, &out, &errOut))
}
