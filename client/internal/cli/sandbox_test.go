package cli

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/starsrc/bowl/client/internal/sandbox"
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

// The API refuses to listen beyond loopback without a key, and checks the key when there is one.
func TestSandboxAPIAuth(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, ExecuteSandbox([]string{"--runtime", "docker", "serve", "--listen", "0.0.0.0:0"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "api-key")

	sboxRuntime, sboxRuntimeName = &sandbox.Runtime{}, runtimeDocker
	h := (&apiServer{}).routes("secret")
	for auth, want := range map[string]int{"": 401, "Bearer nope": 401, "Bearer secret": 400} {
		req := httptest.NewRequest("POST", "/sandboxes/x/commands", strings.NewReader(`{"cmd":"true","argv":["true"]}`))
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		// With the right key the request reaches the handler, which rejects the unknown sandbox or the body.
		h.ServeHTTP(rec, req)
		if want == 400 {
			assert.NotEqual(t, 401, rec.Code, "auth %q", auth)
			continue
		}
		assert.Equal(t, want, rec.Code, "auth %q", auth)
	}
}
