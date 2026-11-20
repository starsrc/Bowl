package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestMount_UnmountTimeoutValidation_GivenInvalidOrConflictingFlagsAndEnvs_Exits2(t *testing.T) {
	mp := t.TempDir()
	withMissingMountInfo(t)

	flagCases := []struct {
		name string
		args []string
	}{
		{name: "bad_unmount_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=notaduration"}},
		{name: "zero_unmount_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=0s"}},
		{name: "negative_unmount_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=-1s"}},
		{name: "zero_stage_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-stage-timeout=0s"}},
		{name: "zero_stall_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-stage-stall-timeout=0s"}},
		{name: "zero_fsync_timeout", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-fsync-timeout=0s"}},
		{name: "stage_timeout_with_ro", args: []string{"mount", testDiskID1, mp, "-f", "--mode=ro", "--unmount-stage-timeout=10s"}},
		{name: "stall_timeout_with_ro", args: []string{"mount", testDiskID1, mp, "-f", "--mode=ro", "--unmount-stage-stall-timeout=10s"}},
		{name: "stage_timeout_with_sync", args: []string{"mount", testDiskID1, mp, "-f", "--mode=sync", "--unmount-stage-timeout=10s"}},
		{name: "stall_timeout_with_sync", args: []string{"mount", testDiskID1, mp, "-f", "--mode=sync", "--unmount-stage-stall-timeout=10s"}},
		{name: "stage_gt_total", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=20s", "--unmount-stage-timeout=30s"}},
		{name: "stall_gt_total", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=20s", "--unmount-stage-stall-timeout=25s"}},
		{name: "fsync_gt_total", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-timeout=20s", "--unmount-fsync-timeout=25s"}},
		{name: "stall_gt_stage", args: []string{"mount", testDiskID1, mp, "-f", "--unmount-stage-timeout=20s", "--unmount-stage-stall-timeout=25s"}},
	}
	for _, tc := range flagCases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCLI(tc.args...)
			require.Equal(t, 2, code, "case=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, "Error:")
		})
	}

	envCases := []struct {
		name string
		envs map[string]string
	}{
		{name: "bad_env_total", envs: map[string]string{"BOWL_UNMOUNT_TIMEOUT": "nope"}},
		{name: "zero_env_total", envs: map[string]string{"BOWL_UNMOUNT_TIMEOUT": "0s"}},
		{name: "bad_env_stage", envs: map[string]string{"BOWL_UNMOUNT_STAGE_TIMEOUT": "nope"}},
		{name: "zero_env_stage", envs: map[string]string{"BOWL_UNMOUNT_STAGE_TIMEOUT": "-2s"}},
		{name: "bad_env_stall", envs: map[string]string{"BOWL_UNMOUNT_STAGE_STALL_TIMEOUT": "nope"}},
		{name: "bad_env_fsync", envs: map[string]string{"BOWL_UNMOUNT_FSYNC_TIMEOUT": "nope"}},
		{name: "env_fsync_gt_total", envs: map[string]string{"BOWL_UNMOUNT_TIMEOUT": "20s", "BOWL_UNMOUNT_FSYNC_TIMEOUT": "25s"}},
		{name: "env_stall_gt_stage", envs: map[string]string{"BOWL_UNMOUNT_STAGE_TIMEOUT": "15s", "BOWL_UNMOUNT_STAGE_STALL_TIMEOUT": "20s"}},
	}
	for _, tc := range envCases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.envs {
				t.Setenv(k, v)
			}
			code, out, errOut := runCLI("mount", testDiskID1, mp, "-f")
			require.Equal(t, 2, code, "case=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, "Error:")
		})
	}
}

func TestMount_UnmountTimeoutResolution_SmartClampingAndEnvPropagation(t *testing.T) {
	t.Run("lowered total clamps unset default sub-timeouts", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-clamp")
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "clamp-disk", 1<<30, 0)
		withMountStorageFake(t, storage)
		captured := stubMountServeFUSE(t, mp)

		code, _, errOut := runCLI(
			"mount", testDiskID1, mp,
			"-f", "--admin-addr=off", "--stage-dir", stageDir,
			"--unmount-timeout=20s",
		)
		require.Equal(t, 0, code, "stderr=%s", errOut)
		require.Equal(t, 20*time.Second, captured.cfg.UnmountTimeout)
		require.Equal(t, 20*time.Second, captured.cfg.UnmountStageTimeout)
		require.Equal(t, 20*time.Second, captured.cfg.UnmountStageStallTimeout)
		require.Equal(t, 20*time.Second, captured.cfg.UnmountFsyncTimeout)
		require.Equal(t, int64(20_000), captured.metaInReady.UnmountTimeoutMs)
	})

	t.Run("lowered stage timeout clamps unset stall timeout", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-clamp-stall")
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "clamp-stall-disk", 1<<30, 0)
		withMountStorageFake(t, storage)
		captured := stubMountServeFUSE(t, mp)

		code, _, errOut := runCLI(
			"mount", testDiskID1, mp,
			"-f", "--admin-addr=off", "--stage-dir", stageDir,
			"--unmount-stage-timeout=15s",
		)
		require.Equal(t, 0, code, "stderr=%s", errOut)
		require.Equal(t, fusefs.DefaultUnmountTimeout, captured.cfg.UnmountTimeout)
		require.Equal(t, 15*time.Second, captured.cfg.UnmountStageTimeout)
		require.Equal(t, 15*time.Second, captured.cfg.UnmountStageStallTimeout)
		require.Equal(t, fusefs.DefaultUnmountFsyncTimeout, captured.cfg.UnmountFsyncTimeout)
	})

	t.Run("env vars populate all four unmount budgets", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		withStageDirTmpfsStub(t, false)
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-env")
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		t.Setenv("BOWL_UNMOUNT_TIMEOUT", "10m")
		t.Setenv("BOWL_UNMOUNT_STAGE_TIMEOUT", "8m")
		t.Setenv("BOWL_UNMOUNT_STAGE_STALL_TIMEOUT", "1m")
		t.Setenv("BOWL_UNMOUNT_FSYNC_TIMEOUT", "90s")

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "env-disk", 1<<30, 0)
		withMountStorageFake(t, storage)
		captured := stubMountServeFUSE(t, mp)

		code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr=off", "--stage-dir", stageDir)
		require.Equal(t, 0, code, "stderr=%s", errOut)
		require.Equal(t, 10*time.Minute, captured.cfg.UnmountTimeout)
		require.Equal(t, 8*time.Minute, captured.cfg.UnmountStageTimeout)
		require.Equal(t, time.Minute, captured.cfg.UnmountStageStallTimeout)
		require.Equal(t, 90*time.Second, captured.cfg.UnmountFsyncTimeout)
		require.Equal(t, int64(600_000), captured.metaInReady.UnmountTimeoutMs)
	})

	t.Run("sync mode ignores ambient stage unmount envs and clamps fsync", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		mp := t.TempDir()
		withMissingMountInfo(t)
		withTempMountMetaDirs(t, t.TempDir())

		t.Setenv("BOWL_UNMOUNT_STAGE_TIMEOUT", "invalid-ignored-in-sync")
		t.Setenv("BOWL_UNMOUNT_STAGE_STALL_TIMEOUT", "invalid-ignored-in-sync")

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "sync-disk", 1<<30, 0)
		withMountStorageFake(t, storage)
		captured := stubMountServeFUSE(t, mp)

		code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--mode=sync", "--admin-addr=off", "--unmount-timeout=25s")
		require.Equal(t, 0, code, "stderr=%s", errOut)
		require.Equal(t, 25*time.Second, captured.cfg.UnmountTimeout)
		require.Equal(t, 25*time.Second, captured.cfg.UnmountFsyncTimeout)
		require.Zero(t, captured.cfg.UnmountStageTimeout)
		require.Zero(t, captured.cfg.UnmountStageStallTimeout)
	})
}

func TestMount_UnmountLeaseRPC_SkipsWhenBudgetExhausted(t *testing.T) {
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "exhausted-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	withServeFUSEStub(t, func(_ context.Context, rawFS *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		rawFS.OnUnmount()
		time.Sleep(80 * time.Millisecond)
		return nil
	})

	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--mode=sync", "--admin-addr=off", "--unmount-timeout=40ms")
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Contains(t, errOut, `"msg":"unmount_disk_rpc_skipped_timeout"`)
	require.Empty(t, storage.getUnmountReqs(), "UnmountDisk RPC must be skipped when total unmount budget is exhausted")
	_, metaExists := findMountMeta(mp)
	require.False(t, metaExists, "mountMeta must still be removed after skipped UnmountDisk RPC")
}

func TestUmount_HelpHasNoNewFlagsAndFallbackStillWorks(t *testing.T) {
	code, out, errOut := runCLI("umount", "--help")
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.NotContains(t, out, "--unmount-timeout")

	metaDir := t.TempDir()
	metaPath := filepath.Join(metaDir, "alive.json")
	require.NoError(t, os.WriteFile(metaPath, []byte(`{}`), 0o644))

	prevDrain := umountDrainTimeout
	prevPoll := umountDrainPollInterval
	umountDrainTimeout = 60 * time.Millisecond
	umountDrainPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		umountDrainTimeout = prevDrain
		umountDrainPollInterval = prevPoll
	})

	err := waitMountMetaDrained(nil, metaPath, &mountMeta{PID: os.Getpid()})
	require.Error(t, err)
}

func TestUmount_WaitMountMetaDrained_UsesMetaUnmountTimeoutPlusGrace(t *testing.T) {
	metaDir := t.TempDir()
	metaPath := filepath.Join(metaDir, "custom-timeout.json")
	require.NoError(t, os.WriteFile(metaPath, []byte(`{}`), 0o644))

	prevDrain := umountDrainTimeout
	prevGrace := umountDrainExitGrace
	prevPoll := umountDrainPollInterval
	umountDrainTimeout = 5 * time.Second
	umountDrainExitGrace = 30 * time.Millisecond
	umountDrainPollInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		umountDrainTimeout = prevDrain
		umountDrainExitGrace = prevGrace
		umountDrainPollInterval = prevPoll
	})

	start := time.Now()
	err := waitMountMetaDrained(nil, metaPath, &mountMeta{
		PID:              os.Getpid(),
		UnmountTimeoutMs: 70,
	})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.GreaterOrEqual(t, elapsed, 90*time.Millisecond, "must wait at least UnmountTimeoutMs + grace")
	require.Less(t, elapsed, time.Second, "must use meta UnmountTimeoutMs + grace instead of 5s fallback")
}
