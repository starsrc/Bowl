package sandbox

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMicroVM_EndToEnd 在真机上起一台 MicroVM 走一遍所有动作。需要 root、/dev/kvm、
// cloud-hypervisor、virtiofsd 和一个跑着的存储服务,所以默认跳过。用法:
//
//	BOWL_MICROVM_DISK=<一块装好系统的盘,会被改写> BOWL_MICROVM_CLIENT=/usr/local/bin/bowl \
//	BOWL_MICROVM_STATE=/var/lib/bowl-sandbox go test -run TestMicroVM_EndToEnd ./internal/sandbox/
//
// BOWL_MICROVM_DEB 指向一个 .deb 时,还会拷进去用 dpkg 装上再运行 BOWL_MICROVM_DEB_RUN。
func TestMicroVM_EndToEnd(t *testing.T) {
	disk := os.Getenv("BOWL_MICROVM_DISK")
	if disk == "" {
		t.Skip("set BOWL_MICROVM_DISK to run")
	}
	ctx := context.Background()
	m := &MicroVM{StateDir: os.Getenv("BOWL_MICROVM_STATE")}

	start := time.Now()
	require.NoError(t, m.Create(ctx, &CreateSpec{
		ID: disk, Image: "test", ClientBinary: os.Getenv("BOWL_MICROVM_CLIENT"),
		HostEndpoint: cmpEnv("BOWL_STORAGE_ENDPOINT", "http://127.0.0.1:9000"),
	}))
	t.Logf("create took %s", time.Since(start))
	t.Cleanup(func() { assert.NoError(t, m.Remove(context.Background(), disk)) })

	info, err := m.Info(ctx, disk)
	require.NoError(t, err)
	assert.Equal(t, "running", info.Status)

	// 前台执行:输出、退出码、工作目录、环境变量、stdin。
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

	// 后台执行、等结束、读日志。
	runID, err := m.ExecBackground(ctx, disk, "/tmp", []string{"sh", "-c", "sleep 1; pwd; exit 3"})
	require.NoError(t, err)
	code, err = m.Wait(ctx, disk, runID)
	require.NoError(t, err)
	assert.Equal(t, 3, code)
	out.Reset()
	require.NoError(t, m.Logs(ctx, disk, runID, false, &out, &errOut))
	assert.Equal(t, "/tmp\n", out.String())

	// 文件拷进拷出,目标名和源名不同。
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

func cmpEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
