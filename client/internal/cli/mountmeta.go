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

// mountMeta stores discovery metadata written after mount readiness so bowl stats can locate the admin endpoint.
type mountMeta struct {
	Mountpoint string    `json:"mountpoint"`
	PID        int       `json:"pid"`
	DiskID     string    `json:"disk_id"`
	AdminAddr  string    `json:"admin_addr"`
	StartedAt  time.Time `json:"started_at"`
}

func fallbackMountMetaDir() string {
	return filepath.Join(os.TempDir(), "bowl-mounts")
}

// mountMetaDirs returns discovery directories in priority order, overridden in unit tests.
var mountMetaDirs = func() []string {
	return []string{primaryMountMetaDir, fallbackMountMetaDir()}
}

// canonicalMountpoint normalizes mountpoint paths by resolving symlinks when possible and falling back to Abs plus Clean.
func canonicalMountpoint(mountpoint string) string {
	abs, err := filepath.Abs(mountpoint)
	if err == nil {
		mountpoint = abs
	}
	if resolved, evalErr := filepath.EvalSymlinks(mountpoint); evalErr == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(mountpoint)
}

// mountMetaFileName returns the metadata file name derived from the first 16 SHA-256 bytes of the cleaned mountpoint.
func mountMetaFileName(mountpoint string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(mountpoint)))
	return hex.EncodeToString(sum[:16]) + ".json"
}

// writeMountMeta atomically writes discovery metadata via temp file and rename, returning the written path.
func writeMountMeta(meta *mountMeta) (string, error) {
	normalized := *meta
	normalized.Mountpoint = filepath.Clean(normalized.Mountpoint)
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
		// Shared fallback directory allows different UIDs to write and clean their own metadata files.
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

// processAlive checks whether pid is alive via signal 0, treating EPERM as alive.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// listMountMetas reads metadata files for alive processes across all discovery directories, sorted by mountpoint.
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

// mountMetaFilePath finds the metadata file path for mountpoint without checking process liveness.
func mountMetaFilePath(mountpoint string) (string, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	names := []string{mountMetaFileName(cleaned)}
	if canon != cleaned {
		names = append(names, mountMetaFileName(canon))
	}
	for _, dir := range mountMetaDirs() {
		for _, name := range names {
			path := filepath.Join(dir, name)
			if _, err := os.Stat(path); err == nil {
				return path, true
			}
		}
	}
	return "", false
}

// readMountMetaPID reads the daemon PID recorded in the mount metadata file.
func readMountMetaPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var m mountMeta
	if json.Unmarshal(data, &m) != nil {
		return 0
	}
	return m.PID
}

// findMountMeta looks up active mount metadata for mountpoint, supporting symlink paths.
func findMountMeta(mountpoint string) (mountMeta, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	for _, m := range listMountMetas() {
		if m.Mountpoint == cleaned || canonicalMountpoint(m.Mountpoint) == canon {
			return m, true
		}
	}
	return mountMeta{}, false
}
