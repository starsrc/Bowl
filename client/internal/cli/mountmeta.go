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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const primaryMountMetaDir = "/run/bowl/mounts"

type mountMeta struct {
	Mountpoint       string    `json:"mountpoint"`
	PID              int       `json:"pid"`
	ProcStartTicks   uint64    `json:"proc_start_ticks,omitempty"`
	DiskID           string    `json:"disk_id"`
	Branch           string    `json:"branch,omitempty"`
	AdminAddr        string    `json:"admin_addr"`
	StartedAt        time.Time `json:"started_at"`
	UnmountTimeoutMs int64     `json:"unmount_timeout_ms,omitempty"`
}

var (
	errProcStatUnsupported = errors.New("proc stat unsupported")
	errInvalidProcStat     = errors.New("invalid proc stat")
)

func isDeadProcState(state byte) bool {
	return state == 'Z' || state == 'X' || state == 'x'
}

func parseProcStat(raw string) (state byte, startTicks uint64, err error) {
	idx := strings.LastIndexByte(raw, ')')
	if idx < 0 || idx+2 >= len(raw) || raw[idx+1] != ' ' {
		return 0, 0, errInvalidProcStat
	}
	fields := strings.Fields(raw[idx+2:])
	if len(fields) < 20 || fields[0] == "" {
		return 0, 0, errInvalidProcStat
	}
	startTicks, err = strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return fields[0][0], startTicks, nil
}

func defaultReadProcStat(pid int) (state byte, startTicks uint64, err error) {
	if runtime.GOOS != "linux" {
		return 0, 0, errProcStatUnsupported
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, 0, err
	}
	return parseProcStat(string(data))
}

func (rt *cliRuntime) mountMetaProcessAlive(m mountMeta) bool {
	if m.PID <= 0 {
		return false
	}
	err := syscall.Kill(m.PID, 0)
	if err != nil && !errors.Is(err, syscall.EPERM) {
		return false
	}
	state, startTicks, statErr := rt.readProcStatFn(m.PID)
	if statErr == nil {
		if isDeadProcState(state) {
			return false
		}
		if m.ProcStartTicks > 0 && startTicks != m.ProcStartTicks {
			return false
		}
		return true
	}
	if errors.Is(statErr, os.ErrNotExist) {
		return false
	}
	return true
}

func fallbackMountMetaDir() string {
	return filepath.Join(os.TempDir(), "bowl-mounts")
}

func (rt *cliRuntime) canonicalMountpoint(mountpoint string) string {
	if resolved, _, err := rt.resolveMountPoint(mountpoint); err == nil {
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

func (rt *cliRuntime) writeMountMeta(meta *mountMeta) (string, error) {
	normalized := *meta
	normalized.Mountpoint = rt.canonicalMountpoint(normalized.Mountpoint)
	if normalized.ProcStartTicks == 0 && normalized.PID > 0 {
		if state, startTicks, err := rt.readProcStatFn(normalized.PID); err == nil && !isDeadProcState(state) && startTicks > 0 {
			normalized.ProcStartTicks = startTicks
			meta.ProcStartTicks = startTicks
		}
	}
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshal mount metadata: %w", err)
	}
	fallbackDir := fallbackMountMetaDir()
	var errs error
	for _, dir := range rt.mountMetaDirs() {
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

func removeOwnedMountMeta(metaPath string, pid int, startedAt time.Time) {
	if metaPath == "" || pid <= 0 {
		return
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return
	}
	var cur mountMeta
	if json.Unmarshal(data, &cur) != nil {
		return
	}
	if cur.PID == pid && cur.StartedAt.Equal(startedAt) {
		_ = os.Remove(metaPath)
	}
}

func (rt *cliRuntime) processAlive(pid int) bool {
	return rt.mountMetaProcessAlive(mountMeta{PID: pid})
}

func (rt *cliRuntime) listMountMetas() []mountMeta {
	seen := make(map[string]struct{})
	var out []mountMeta
	for _, dir := range rt.mountMetaDirs() {
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
			if json.Unmarshal(data, &m) != nil || !rt.mountMetaProcessAlive(m) {
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

func (rt *cliRuntime) resolveUmountMountMeta(mountpoint string) (metaPath string, meta mountMeta, ok bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := rt.canonicalMountpoint(cleaned)
	names := []string{mountMetaFileName(cleaned)}
	if canon != cleaned {
		names = append(names, mountMetaFileName(canon))
	}

	for _, dir := range rt.mountMetaDirs() {
		for _, name := range names {
			candidate := filepath.Join(dir, name)
			data, err := os.ReadFile(candidate)
			if err != nil {
				continue
			}
			var m mountMeta
			if json.Unmarshal(data, &m) != nil || m.PID <= 0 {
				continue
			}
			if rt.mountMetaProcessAlive(m) {
				return candidate, m, true
			}
		}
	}
	return "", mountMeta{}, false
}

func (rt *cliRuntime) findMountMeta(mountpoint string) (mountMeta, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := rt.canonicalMountpoint(cleaned)
	for _, m := range rt.listMountMetas() {
		if m.Mountpoint == cleaned || m.Mountpoint == canon {
			return m, true
		}
	}
	return mountMeta{}, false
}
