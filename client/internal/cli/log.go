package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const defaultSystemLogDir = "/var/log/bowl"

var openSystemLogFn = openSystemLog

func openSystemLog(diskID string) (*os.File, string, error) {
	systemPath := filepath.Join(defaultSystemLogDir, fmt.Sprintf("mount-%s.log", diskID))
	mkdirErr := os.MkdirAll(defaultSystemLogDir, 0o755)
	if mkdirErr == nil {
		f, openErr := os.OpenFile(systemPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if openErr == nil {
			return f, systemPath, nil
		}
		return nil, systemPath, openErr
	}
	return nil, systemPath, mkdirErr
}

// isPermissionError reports whether err indicates a permission or read-only filesystem failure.
func isPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsPermission(err) {
		return true
	}
	if errno, ok := errors.AsType[syscall.Errno](err); ok {
		switch errno {
		case syscall.EACCES, syscall.EPERM, syscall.EROFS:
			return true
		}
	}
	return false
}

// ResolveLogFile opens the daemon log file at explicitPath, /var/log/bowl, or $XDG_STATE_HOME/bowl/logs on permission errors.
func ResolveLogFile(explicitPath, diskID string) (*os.File, string, error) {
	if explicitPath != "" {
		dir := filepath.Dir(explicitPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, "", fmt.Errorf("create log directory %q: %w", dir, err)
			}
		}
		f, err := os.OpenFile(explicitPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, "", fmt.Errorf("open log file %q: %w", explicitPath, err)
		}
		return f, explicitPath, nil
	}

	f, systemPath, err := openSystemLogFn(diskID)
	if err == nil {
		return f, systemPath, nil
	}
	if !isPermissionError(err) {
		return nil, "", fmt.Errorf("open system log file %q: %w", systemPath, err)
	}

	var stateDir string
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		stateDir = xdg
	} else {
		var home string
		home, err = os.UserHomeDir()
		if err != nil {
			return nil, "", fmt.Errorf("resolve user home directory: %w", err)
		}
		stateDir = filepath.Join(home, ".local", "state")
	}

	fallbackDir := filepath.Join(stateDir, "bowl", "logs")
	if err = os.MkdirAll(fallbackDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("create fallback log directory %q: %w", fallbackDir, err)
	}

	fallbackPath := filepath.Join(fallbackDir, fmt.Sprintf("mount-%s.log", diskID))
	f, err = os.OpenFile(fallbackPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, "", fmt.Errorf("open fallback log file %q: %w", fallbackPath, err)
	}
	return f, fallbackPath, nil
}
