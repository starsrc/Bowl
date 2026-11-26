package sandbox

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMicroVM_EndToEnd(t *testing.T) {
	template := os.Getenv("BOWL_MICROVM_TEMPLATE")
	if template == "" {
		t.Skip("set BOWL_MICROVM_TEMPLATE to a template name in BOWL_MICROVM_STATE to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("needs root")
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Skip("needs /dev/kvm")
	}
	ctx := context.Background()
	m := &MicroVM{StateDir: os.Getenv("BOWL_MICROVM_STATE")}
	disk := "e2e-" + strconv.Itoa(os.Getpid())

	start := time.Now()
	require.NoError(t, m.Create(ctx, &CreateSpec{ID: disk, Template: template}))
	t.Logf("create took %s", time.Since(start))
	t.Cleanup(func() { assert.NoError(t, m.Remove(context.Background(), disk)) })

	info, err := m.Info(disk)
	require.NoError(t, err)
	assert.Equal(t, "running", info.Status)

	var out, errOut bytes.Buffer
	start = time.Now()
	code, err := m.Exec(ctx, disk, "/etc", []string{"sh", "-c", `pwd; echo "$FOO"; cat; echo oops >&2; exit 7`},
		ExecOptions{Env: []string{"FOO=bar"}}, strings.NewReader("from-stdin\n"), &out, &errOut)
	t.Logf("exec took %s", time.Since(start))
	require.NoError(t, err)
	assert.Equal(t, 7, code)
	assert.Equal(t, "/etc\nbar\nfrom-stdin\n", out.String())
	assert.Equal(t, "oops\n", errOut.String())

	code, err = m.Exec(ctx, disk, "", []string{"no-such-command"}, ExecOptions{}, nil, &out, &errOut)
	require.NoError(t, err)
	assert.Equal(t, 127, code)

	runID, err := m.ExecBackground(ctx, disk, "/tmp", []string{"sh", "-c", "sleep 1; pwd; exit 3"})
	require.NoError(t, err)
	code, err = m.Wait(ctx, disk, runID)
	require.NoError(t, err)
	assert.Equal(t, 3, code)
	out.Reset()
	require.NoError(t, m.Logs(ctx, disk, runID, false, &out, &errOut))
	assert.Equal(t, "/tmp\n", out.String())

	local := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(local, "a.txt"), []byte("hello"), 0o600))
	require.NoError(t, m.CopyInto(ctx, disk, filepath.Join(local, "a.txt"), "/root/b.txt", &errOut))
	require.NoError(t, m.CopyFrom(ctx, disk, "/root/b.txt", filepath.Join(local, "c.txt"), &errOut))
	got, err := os.ReadFile(filepath.Join(local, "c.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))

	if deb := os.Getenv("BOWL_MICROVM_DEB"); deb != "" {
		require.NoError(t, m.CopyInto(ctx, disk, deb, "/root/pkg.deb", &errOut))
		start = time.Now()
		out.Reset()
		errOut.Reset()
		code, err = m.Exec(ctx, disk, "", []string{"dpkg", "-i", "/root/pkg.deb"}, ExecOptions{}, nil, &out, &errOut)
		t.Logf("dpkg -i took %s", time.Since(start))
		require.NoError(t, err)
		require.Equal(t, 0, code, "stdout: %s\nstderr: %s", out.String(), errOut.String())
		out.Reset()
		code, err = m.Exec(ctx, disk, "", strings.Fields(os.Getenv("BOWL_MICROVM_DEB_RUN")), ExecOptions{}, nil, &out, &errOut)
		require.NoError(t, err)
		assert.Equal(t, 0, code)
		t.Logf("installed package says: %s", strings.TrimSpace(out.String()))
	}
}
