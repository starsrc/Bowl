package sandboxcli

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

func TestSplitCpSpec(t *testing.T) {
	id, path, inSandbox := splitCpSpec("abc:/root/a.txt")
	assert.True(t, inSandbox)
	assert.Equal(t, "abc", id)
	assert.Equal(t, "/root/a.txt", path)

	for _, local := range []string{"/tmp/a.txt", "./a.txt", "/tmp/a:b/c", "", "relative.name"} {
		_, path, inSandbox = splitCpSpec(local)
		assert.False(t, inSandbox, "local path %q", local)
		assert.Equal(t, local, path)
	}
}

func TestUsageErrors(t *testing.T) {
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
		{"timeout", "sbx-1", "soon"},
		{"template", "create", "ubuntu:24.04", "--size", "4G"},
	} {
		var stdout, stderr bytes.Buffer
		assert.Equal(t, 2, Execute(append([]string{"--state-dir", t.TempDir()}, args...), &stdout, &stderr),
			"args %v, stderr: %s", args, stderr.String())
	}
}

func TestCreateNeedsTemplate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, Execute([]string{"--state-dir", t.TempDir(), "create"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "--template")

	stderr.Reset()
	assert.Equal(t, 1, Execute([]string{"--state-dir", t.TempDir(), "create", "--template", "missing"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "missing")
}

func TestParseSize(t *testing.T) {
	n, err := parseSize("4GiB")
	require.NoError(t, err)
	assert.Equal(t, int64(4<<30), n)
	n, err = parseSize("512MiB")
	require.NoError(t, err)
	assert.Equal(t, int64(512<<20), n)
	for _, bad := range []string{"", "4", "4G", "0GiB", "-1MiB", "xGiB"} {
		_, err = parseSize(bad)
		assert.Error(t, err, bad)
	}
}

func TestAPIAuth(t *testing.T) {
	var stdout, stderr bytes.Buffer
	assert.Equal(t, 2, Execute([]string{"serve", "--listen", "0.0.0.0:0"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "api-key")

	vm = &sandbox.MicroVM{StateDir: t.TempDir()}
	h := (&apiServer{}).routes("secret")
	for auth, want := range map[string]int{"": 401, "Bearer nope": 401, "Bearer secret": 404} {
		req := httptest.NewRequest("POST", "/sandboxes/x/commands", strings.NewReader(`{"cmd":"true"}`))
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, want, rec.Code, "auth %q", auth)
	}
}
