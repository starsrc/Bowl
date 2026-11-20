package fusefs_test

import (
	"encoding/json"
	"strings"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestBuildMountOptions_GivenAllMountModes_DisablesUserspaceLocks(t *testing.T) {
	modes := []string{
		fusefs.MountModeStaged,
		fusefs.MountModeSync,
		fusefs.MountModeRO,
	}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			opts := fusefs.BuildMountOptions(fusefs.Config{
				DiskID: "disk-lock-mode-test",
				Mode:   mode,
			})
			require.False(t, opts.EnableLocks, "mode=%s must disable userspace FUSE locks: want=false got=%v", mode, opts.EnableLocks)
		})
	}
}

func TestRawFS_GivenKernelDelegatedLocks_DirectLockOpsReturnENOSYS(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	fs := newTestRawFS(t, srv)
	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "kernel-lock.bin", syscall.O_RDWR, 0o644)
	defer fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

	in := &fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: f.NodeId},
		Fh:       f.Fh,
		Owner:    1,
		Lk:       fuse.FileLock{Start: 0, End: 63, Typ: syscall.F_WRLCK, Pid: 1},
	}
	var out fuse.LkOut

	requireStatus(t, "GetLk", fuse.ENOSYS, fs.GetLk(nil, in, &out))
	requireStatus(t, "SetLk", fuse.ENOSYS, fs.SetLk(nil, in))
	requireStatus(t, "SetLkw", fuse.ENOSYS, fs.SetLkw(nil, in))
}

func TestMetrics_GivenKernelDelegatedLocks_OmitsLockTableTelemetryAndPreservesInodeLocks(t *testing.T) {
	srv := newFakeStorage(t, withTTL(1.0, 1.0))
	reg := prometheus.NewRegistry()
	metrics := fusefs.NewMetrics()
	reg.MustRegister(metrics)
	fs := newTestRawFS(t, srv, withMetrics(metrics))
	t.Cleanup(fs.OnUnmount)

	f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "telemetry-lock.bin", syscall.O_RDWR, 0o644)
	mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte("x"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})

	seenNames := gatherMetricNameSet(t, reg)
	removedPromMetrics := []string{
		"bowl_client_locks_active",
		"bowl_client_lock_conflicts_total",
		"bowl_client_lock_waits_total",
		"bowl_client_lock_wait_seconds_total",
		"bowl_client_lock_wait_duration_seconds",
	}
	for _, name := range removedPromMetrics {
		require.False(t, seenNames[name], "Prometheus metric must be removed: %s", name)
	}

	preservedInodeLockMetrics := []string{
		"bowl_client_inodelock_acquisitions_total",
		"bowl_client_inodelock_wait_seconds_total",
		"bowl_client_inodelock_wait_duration_seconds",
		"bowl_client_inodelock_active",
	}
	for _, name := range preservedInodeLockMetrics {
		require.True(t, seenNames[name], "Prometheus inode lock metric must be preserved: %s", name)
	}

	opsMF := findMetricFamily(t, reg, "bowl_client_fuse_ops_total")
	seenOps := make(map[string]bool, len(opsMF.GetMetric()))
	for _, metric := range opsMF.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "op" {
				seenOps[label.GetValue()] = true
			}
		}
	}
	for _, removedOp := range []string{"GetLk", "SetLk", "SetLkw"} {
		require.False(t, seenOps[removedOp], "FUSE op label must be removed: %s", removedOp)
	}

	snap := fs.Snapshot()
	for _, op := range snap.FUSE.Ops {
		require.NotEqual(t, "GetLk", op.Op)
		require.NotEqual(t, "SetLk", op.Op)
		require.NotEqual(t, "SetLkw", op.Op)
	}

	jsonBytes, err := snap.FormatJSON()
	require.NoError(t, err)
	var rawSnap map[string]any
	require.NoError(t, json.Unmarshal(jsonBytes, &rawSnap))
	require.NotContains(t, rawSnap, "locks")
	require.Contains(t, rawSnap, "inode_locks")

	textOut := snap.FormatText()
	for line := range strings.SplitSeq(textOut, "\n") {
		require.False(t, strings.HasPrefix(line, "Locks:"), "FormatText must omit Locks line: got=%q", line)
	}
	require.Contains(t, textOut, "InodeLocks:")

	statsOut := snap.FormatStatsFile()
	for _, removedLine := range []string{
		"bowl_client_locks_active",
		"bowl_client_lock_conflicts_total",
		"bowl_client_lock_waits_total",
		"bowl_client_lock_wait_seconds_total",
	} {
		require.NotContains(t, statsOut, removedLine)
	}
	for _, preservedLine := range []string{
		"bowl_client_inodelock_fast_total",
		"bowl_client_inodelock_contended_total",
		"bowl_client_inodelock_wait_seconds_total",
		"bowl_client_inodelock_active",
	} {
		require.Contains(t, statsOut, preservedLine)
	}
}
