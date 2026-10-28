package cli_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func TestLog_ExplicitLogFile(t *testing.T) {
	diskID := uuid.New().String()
	tmpDir := t.TempDir()
	explicitPath := filepath.Join(tmpDir, "sub", "custom", "test.log")

	file, path, err := cli.ResolveLogFile(explicitPath, diskID)
	require.NoError(t, err)
	require.NotNil(t, file)
	defer file.Close()

	require.Equal(t, explicitPath, path)
	_, err = file.WriteString("test log content\n")
	require.NoError(t, err)

	content, err := os.ReadFile(explicitPath)
	require.NoError(t, err)
	require.Equal(t, "test log content\n", string(content))
}

func TestLog_FallbackToXDGStateHomeOnPermissionDenied(t *testing.T) {
	diskID := uuid.New().String()
	tmpDir := t.TempDir()

	// 注入模拟 /var/log/bowl 无权限 (EACCES)
	cli.SetSystemLogOpenerForTest(t, func(diskID string) (*os.File, string, error) {
		return nil, "/var/log/bowl/mount-" + diskID + ".log", syscall.EACCES
	})

	xdgStateHome := filepath.Join(tmpDir, "xdg_state")
	t.Setenv("XDG_STATE_HOME", xdgStateHome)

	file, path, err := cli.ResolveLogFile("", diskID)
	require.NoError(t, err)
	require.NotNil(t, file)
	defer file.Close()

	expectedPath := filepath.Join(xdgStateHome, "bowl", "logs", "mount-"+diskID+".log")
	require.Equal(t, expectedPath, path)

	_, err = file.WriteString("fallback log content\n")
	require.NoError(t, err)

	content, err := os.ReadFile(expectedPath)
	require.NoError(t, err)
	require.Equal(t, "fallback log content\n", string(content))
}

func TestLog_FallbackToUserHomeStateWhenNoXDG(t *testing.T) {
	diskID := uuid.New().String()
	tmpDir := t.TempDir()

	cli.SetSystemLogOpenerForTest(t, func(diskID string) (*os.File, string, error) {
		return nil, "/var/log/bowl/mount-" + diskID + ".log", syscall.EACCES
	})

	fakeHome := filepath.Join(tmpDir, "fake_home")
	require.NoError(t, os.Mkdir(fakeHome, 0o755))

	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("HOME", fakeHome)

	file, path, err := cli.ResolveLogFile("", diskID)
	require.NoError(t, err)
	require.NotNil(t, file)
	defer file.Close()

	expectedPath := filepath.Join(fakeHome, ".local", "state", "bowl", "logs", "mount-"+diskID+".log")
	require.Equal(t, expectedPath, path)
}
