package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSboxSplitCpSpec(t *testing.T) {
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

// Usage errors must be caught before anything touches docker or the storage service.
func TestSandboxUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"nope"},
		{"exec"},
		{"exec", "sbx-1"},
		{"exec", "sbx-1", "--"},
		{"exec", "--nope", "sbx-1", "ls"},
		{"cp", "/tmp/a", "/tmp/b"},
		{"cp", "a:/x", "b:/y"},
		{"kill"},
		{"kill", "sbx-1", "--all"},
		{"wait", "sbx-1"},
	} {
		var stdout, stderr bytes.Buffer
		assert.Equal(t, 2, ExecuteSandbox(append([]string{"--runtime", "docker"}, args...), &stdout, &stderr),
			"args %v, stderr: %s", args, stderr.String())
	}
}

func TestSandboxRuntimeFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, ExecuteSandbox([]string{"--runtime", "lxc", "list"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "unknown --runtime")

	stderr.Reset()
	assert.Equal(t, 2, ExecuteSandbox([]string{"--runtime", "microvm", "create"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "--from")
}
