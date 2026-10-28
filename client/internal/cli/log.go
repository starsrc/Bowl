package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var defaultSystemLogDir = "/var/log/bowl"

var tryOpenSystemLog = func(diskID string) (*os.File, string, error) {
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

// isPermissionError 判断是否属于无写权限/只读文件系统等权限类错误。
func isPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsPermission(err) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.EACCES, syscall.EPERM, syscall.EROFS:
			return true
		}
	}
	return false
}

// ResolveLogFile 解析并准备后台模式的日志文件。
// 若 explicitPath 非空，优先使用指定路径（自动 MkdirAll 父目录，失败报错）；
// 若未指定，优先尝试 /var/log/bowl/mount-<disk-id>.log；
// 若无写权限，自动回退至 <state_dir>/bowl/logs/mount-<disk-id>.log（遵循 $XDG_STATE_HOME 或 ~/.local/state）。
func ResolveLogFile(explicitPath, diskID string) (*os.File, string, error) {
	if explicitPath != "" {
		dir := filepath.Dir(explicitPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, "", fmt.Errorf("创建日志目录 \"%s\" 失败: %w", dir, err)
			}
		}
		f, err := os.OpenFile(explicitPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, "", fmt.Errorf("打开日志文件 \"%s\" 失败: %w", explicitPath, err)
		}
		return f, explicitPath, nil
	}

	// 1. 尝试系统默认路径 /var/log/bowl/mount-<disk-id>.log
	f, systemPath, err := tryOpenSystemLog(diskID)
	if err == nil {
		return f, systemPath, nil
	}
	if !isPermissionError(err) {
		return nil, "", fmt.Errorf("打开系统日志文件 \"%s\" 失败: %w", systemPath, err)
	}

	// 2. 权限不足，自动回退到用户状态目录
	var stateDir string
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		stateDir = xdg
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, "", fmt.Errorf("获取用户家目录失败: %w", err)
		}
		stateDir = filepath.Join(home, ".local", "state")
	}

	fallbackDir := filepath.Join(stateDir, "bowl", "logs")
	if err := os.MkdirAll(fallbackDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("创建回退日志目录 \"%s\" 失败: %w", fallbackDir, err)
	}

	fallbackPath := filepath.Join(fallbackDir, fmt.Sprintf("mount-%s.log", diskID))
	f, err = os.OpenFile(fallbackPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, "", fmt.Errorf("打开回退日志文件 \"%s\" 失败: %w", fallbackPath, err)
	}
	return f, fallbackPath, nil
}
