package cli

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const primaryMountMetaDir = "/run/bowl/mounts"

type mountMeta struct {
	Mountpoint       string    `json:"mountpoint"`
	PID              int       `json:"pid"`
	DiskID           string    `json:"disk_id"`
	AdminAddr        string    `json:"admin_addr"`
	StartedAt        time.Time `json:"started_at"`
	UnmountTimeoutMs int64     `json:"unmount_timeout_ms,omitempty"`
}

func fallbackMountMetaDir() string {
	return filepath.Join(os.TempDir(), "bowl-mounts")
}

var mountMetaDirs = func() []string {
	return []string{primaryMountMetaDir, fallbackMountMetaDir()}
}

func canonicalMountpoint(mountpoint string) string {
	if resolved, _, err := ResolveMountPoint(mountpoint); err == nil {
		return resolved
	}
	if abs, err := filepath.Abs(mountpoint); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(mountpoint)
}

func mountMetaFileName(mountpoint string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(mountpoint)))
	return hex.EncodeToString(sum[:16]) + ".json"
}

func writeMountMeta(meta *mountMeta) (string, error) {
	normalized := *meta
	normalized.Mountpoint = canonicalMountpoint(normalized.Mountpoint)
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshal mount metadata: %w", err)
	}
	fallbackDir := fallbackMountMetaDir()
	var errs error
	for _, dir := range mountMetaDirs() {
		path, werr := writeFileAtomic(dir, mountMetaFileName(normalized.Mountpoint), data, dir == fallbackDir)
		if werr == nil {
			return path, nil
		}
		errs = errors.Join(errs, werr)
	}
	return "", errs
}

func writeFileAtomic(dir, name string, data []byte, sharedSticky bool) (string, error) {
	_, statErr := os.Stat(dir)
	createdDir := errors.Is(statErr, os.ErrNotExist)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if createdDir && sharedSticky {

		_ = os.Chmod(dir, 0o777|os.ModeSticky)
	}
	tmp, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, writeErr := tmp.Write(data); writeErr != nil {
		_ = tmp.Close()
		return "", writeErr
	}
	if closeErr := tmp.Close(); closeErr != nil {
		return "", closeErr
	}
	if chmodErr := os.Chmod(tmpName, 0o644); chmodErr != nil {
		return "", chmodErr
	}
	path := filepath.Join(dir, name)
	if renameErr := os.Rename(tmpName, path); renameErr != nil {
		return "", renameErr
	}
	return path, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func listMountMetas() []mountMeta {
	seen := make(map[string]struct{})
	var out []mountMeta
	for _, dir := range mountMetaDirs() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(dir, name))
			if readErr != nil {
				continue
			}
			var m mountMeta
			if json.Unmarshal(data, &m) != nil || !processAlive(m.PID) {
				continue
			}
			m.Mountpoint = filepath.Clean(m.Mountpoint)
			if _, dup := seen[m.Mountpoint]; dup {
				continue
			}
			seen[m.Mountpoint] = struct{}{}
			out = append(out, m)
		}
	}
	slices.SortFunc(out, func(a, b mountMeta) int {
		return cmp.Compare(a.Mountpoint, b.Mountpoint)
	})
	return out
}

func resolveUmountMountMeta(mountpoint string) (metaPath string, meta mountMeta, ok bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	names := []string{mountMetaFileName(cleaned)}
	if canon != cleaned {
		names = append(names, mountMetaFileName(canon))
	}

	var fallbackPath string
	var fallbackMeta mountMeta
	hasFallback := false

	for _, dir := range mountMetaDirs() {
		for _, name := range names {
			candidate := filepath.Join(dir, name)
			data, err := os.ReadFile(candidate)
			if err != nil {
				continue
			}
			var m mountMeta
			if json.Unmarshal(data, &m) != nil {
				m = mountMeta{}
			} else if processAlive(m.PID) {
				return candidate, m, true
			}
			if !hasFallback {
				fallbackPath = candidate
				fallbackMeta = m
				hasFallback = true
			}
		}
	}
	if hasFallback {
		return fallbackPath, fallbackMeta, true
	}
	return "", mountMeta{}, false
}

func findMountMeta(mountpoint string) (mountMeta, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	for _, m := range listMountMetas() {
		if m.Mountpoint == cleaned || m.Mountpoint == canon {
			return m, true
		}
	}
	return mountMeta{}, false
}
