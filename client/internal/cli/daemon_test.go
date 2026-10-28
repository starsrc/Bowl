package cli_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func TestMount_DaemonParentHandshake(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
	}

	mux := http.NewServeMux()
	path, h := storagev1connect.NewStorageServiceHandler(mockSrv)
	mux.Handle(path, h)
	ts := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	t.Cleanup(ts.Close)

	t.Run("child sends OK causes parent to exit 0", func(t *testing.T) {
		cli.SetProcessLauncherForTest(t, func(cmd *exec.Cmd) error {
			w := cmd.ExtraFiles[0]
			_, _ = w.WriteString("OK\n")
			return nil
		})

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount", "--endpoint", ts.URL, validUUID, tmpDir}, &stdout, &stderr)
		require.Equal(t, 0, code)
	})

	t.Run("child sends ERR causes parent to print error and exit non-zero", func(t *testing.T) {
		cli.SetProcessLauncherForTest(t, func(cmd *exec.Cmd) error {
			w := cmd.ExtraFiles[0]
			_, _ = w.WriteString("ERR:内核 /dev/fuse 打开失败\n")
			return nil
		})

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount", "--endpoint", ts.URL, validUUID, tmpDir}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Contains(t, stderr.String(), "内核 /dev/fuse 打开失败")
	})

	t.Run("child crashes with EOF causes parent to exit non-zero", func(t *testing.T) {
		cli.SetProcessLauncherForTest(t, func(cmd *exec.Cmd) error {
			// 不写入任何数据即返回，父进程关闭 w 后读 r 遇到 EOF
			return nil
		})

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount", "--endpoint", ts.URL, validUUID, tmpDir}, &stdout, &stderr)
		require.Equal(t, 1, code)
		require.Contains(t, stderr.String(), "守护进程")
	})
}
