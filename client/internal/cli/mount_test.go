package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type capturedMountServe struct {
	cfg          fusefs.Config
	instDirAlive bool
	metaInReady  mountMeta
	metaFoundNow bool
}

func withCleanOTelAndLogger(t *testing.T) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	prevMP := otel.GetMeterProvider()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	prevLogger := slog.Default()
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		slog.SetDefault(prevLogger)
	})
}

func setStageDirTmpfsStub(rt *cliRuntime, isTmpfs bool) {
	rt.stageDirIsTmpfs = func(*fusefs.StageDirSession) bool { return isTmpfs }
}

func withCleanDefaultStageDir(t *testing.T) {
	t.Helper()
	_, err := os.Stat(fusefs.DefaultStageDir)
	if errors.Is(err, os.ErrNotExist) {
		t.Cleanup(func() { _ = os.Remove(fusefs.DefaultStageDir) })
	}
}

func setMountRenewIntervalStub(rt *cliRuntime, interval time.Duration) {
	rt.mountRenewIntervalFn = func(uint32) time.Duration { return interval }
}

func setMountRenewRetryIntervalStub(rt *cliRuntime, interval time.Duration) {
	rt.mountRenewRetryIntervalFn = func(time.Duration) time.Duration { return interval }
}

func setReadinessPipeStub(rt *cliRuntime, w *os.File) {
	rt.readinessPipeFile = func() *os.File { return w }
}

func stubMountServeFUSE(t *testing.T, rt *cliRuntime, wantMP string) *capturedMountServe {
	t.Helper()
	wantCanon := rt.canonicalMountpoint(wantMP)
	res := &capturedMountServe{}
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, gotMP string, ready func()) error {
		res.cfg = *cfg
		require.Equal(t, wantCanon, gotMP, "scene=serveFUSE_mountpoint want=%s got=%s", wantCanon, gotMP)
		if cfg.StageSession != nil {
			_, statErr := os.Stat(cfg.StageSession.InstanceDir())
			res.instDirAlive = statErr == nil
		}
		ready()
		res.metaInReady, res.metaFoundNow = rt.findMountMeta(wantCanon)
		return nil
	}
	return res
}

func runCLIWithDeadline(t *testing.T, rt *cliRuntime, deadline time.Duration, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	type cliResult struct {
		code   int
		stdout string
		stderr string
	}
	ch := make(chan cliResult, 1)
	go func() {
		c, out, errOut := runCLIWithRuntime(rt, args...)
		ch <- cliResult{code: c, stdout: out, stderr: errOut}
	}()
	select {
	case res := <-ch:
		return res.code, res.stdout, res.stderr
	case <-time.After(deadline):
		t.Fatalf("scene=cli_validation_deadline args=%v want=completed_within_%s got=timeout", args, deadline)
		return 0, "", ""
	}
}

func requireEmptyDir(t *testing.T, dir, scene string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "scene=%s dir=%s", scene, dir)
	require.Empty(t, entries, "scene=%s dir=%s want=empty got=%d_entries", scene, dir, len(entries))
}

func requireRenewRequestsMatch(t *testing.T, renews []*storagev1.RenewMountRequest, diskID, mountID string, ro bool, epoch uint64) {
	t.Helper()
	for idx, req := range renews {
		require.Equal(t, diskID, req.GetDiskId(), "scene=renew_req[%d] want=%s got=%s", idx, diskID, req.GetDiskId())
		require.Equal(t, mountID, req.GetMountId(), "scene=renew_req[%d] want=%s got=%s", idx, mountID, req.GetMountId())
		require.Equal(t, ro, req.GetReadOnly(), "scene=renew_req[%d] want=%v got=%v", idx, ro, req.GetReadOnly())
		require.Equal(t, epoch, req.GetEpoch(), "scene=renew_req[%d] want=%d got=%d", idx, epoch, req.GetEpoch())
	}
}

func requireStorageEpochZero(t *testing.T, storage *fakeStorage, scene string) {
	t.Helper()
	storage.mu.Lock()
	epochSeq := storage.epochSeq
	storage.mu.Unlock()
	require.Zero(t, epochSeq, "scene=%s want=epochSeq_0 got=%d", scene, epochSeq)
}

func TestMount_Help_GivenHelpFlag_PrintsUpdatedUsageWithoutLegacyRO(t *testing.T) {
	cases := []struct {
		name string
		flag string
	}{
		{name: "ShortFlag", flag: "-h"},
		{name: "LongFlag", flag: "--help"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCLI("mount", tc.flag)

			require.Equal(t, 0, code, "scene=mount_help flag=%s want=0 got=%d stderr=%s", tc.flag, code, errOut)
			require.Empty(t, errOut)
			require.Contains(t, out, "bowl mount <disk-id> <mountpoint>")
			require.Contains(t, out, "staged, sync, or ro")
			require.Contains(t, out, "--stage-size")
			require.Contains(t, out, "--stage-dir")
			require.NotContains(t, out, "--ro")
			require.NotContains(t, out, "--token")
		})
	}
}

func TestMount_Validation_GivenInvalidFlagsOrArgs_Exits2(t *testing.T) {
	validMP := t.TempDir()
	missingMP := filepath.Join(t.TempDir(), "missing")
	fileMP := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(fileMP, []byte("x"), 0o600))

	cases := []struct {
		name       string
		args       []string
		wantSubstr string
	}{
		{name: "ZeroPosArgs", args: []string{"mount"}, wantSubstr: "accepts 2 arg(s), received 0"},
		{name: "OnePosArg", args: []string{"mount", testDiskID1}, wantSubstr: "accepts 2 arg(s), received 1"},
		{name: "ThreePosArgs", args: []string{"mount", testDiskID1, validMP, "extra"}, wantSubstr: "accepts 2 arg(s), received 3"},
		{name: "UnknownFlag", args: []string{"mount", testDiskID1, validMP, "--bogus"}, wantSubstr: "unknown flag: --bogus"},
		{name: "LegacyROFlag", args: []string{"mount", testDiskID1, validMP, "--ro"}, wantSubstr: "unknown flag: --ro"},
		{name: "LegacyROFalseFlag", args: []string{"mount", testDiskID1, validMP, "--ro=false"}, wantSubstr: "unknown flag: --ro"},
		{name: "LegacyTokenFlag", args: []string{"mount", testDiskID1, validMP, "--token", "mount-tok"}, wantSubstr: "unknown flag: --token"},
		{name: "ShortLookalikeYesFlag", args: []string{"mount", testDiskID1, validMP, "-yes"}, wantSubstr: "unknown shorthand flag"},
		{name: "LongYFlag", args: []string{"mount", testDiskID1, validMP, "--y"}, wantSubstr: "unknown flag: --y"},
		{name: "TripleDashJSONFlag", args: []string{"mount", testDiskID1, validMP, "---json"}, wantSubstr: "bad flag syntax: ---json"},
		{name: "MissingEndpointValue", args: []string{"mount", testDiskID1, validMP, "--endpoint"}, wantSubstr: "flag needs an argument: --endpoint"},
		{name: "MissingModeValue", args: []string{"mount", testDiskID1, validMP, "--mode"}, wantSubstr: "flag needs an argument: --mode"},
		{name: "MissingStageSizeValue", args: []string{"mount", testDiskID1, validMP, "--stage-size"}, wantSubstr: "flag needs an argument: --stage-size"},
		{name: "MissingStageDirValue", args: []string{"mount", testDiskID1, validMP, "--stage-dir"}, wantSubstr: "flag needs an argument: --stage-dir"},
		{name: "InvalidStatsBool", args: []string{"mount", testDiskID1, validMP, "--stats-file=not-bool"}, wantSubstr: "invalid argument"},
		{name: "InvalidForegroundBool", args: []string{"mount", testDiskID1, validMP, "--foreground=bad"}, wantSubstr: "invalid argument"},
		{name: "InvalidAllowOtherBool", args: []string{"mount", testDiskID1, validMP, "--allow-other=bad"}, wantSubstr: "invalid argument"},
		{name: "InvalidDirectIOBool", args: []string{"mount", testDiskID1, validMP, "--direct-io=bad"}, wantSubstr: "invalid argument"},
		{name: "InvalidDebugBool", args: []string{"mount", testDiskID1, validMP, "--debug=bad"}, wantSubstr: "invalid argument"},
		{name: "NegativeTraceRatio", args: []string{"mount", testDiskID1, validMP, "--trace-sample-ratio", "-0.1"}, wantSubstr: "invalid --trace-sample-ratio"},
		{name: "OverOneTraceRatio", args: []string{"mount", testDiskID1, validMP, "--trace-sample-ratio", "1.5"}, wantSubstr: "invalid --trace-sample-ratio"},
		{name: "NonNumericTraceRatio", args: []string{"mount", testDiskID1, validMP, "--trace-sample-ratio", "NaN-bad"}, wantSubstr: "invalid argument"},
		{name: "InvalidLogLevel", args: []string{"mount", testDiskID1, validMP, "--log-level", "verbose"}, wantSubstr: "invalid --log-level"},
		{name: "InvalidLogFormat", args: []string{"mount", testDiskID1, validMP, "--log-format", "yaml"}, wantSubstr: "invalid --log-format"},
		{name: "InvalidAdminAddr", args: []string{"mount", testDiskID1, validMP, "--admin-addr", "bad-addr"}, wantSubstr: "invalid --admin-addr"},
		{name: "InvalidModeBogus", args: []string{"mount", testDiskID1, validMP, "--mode", "bogus"}, wantSubstr: "must be staged, sync, or ro"},
		{name: "EmptyMode", args: []string{"mount", testDiskID1, validMP, "--mode="}, wantSubstr: "must be staged, sync, or ro"},
		{name: "ROWithStageSize", args: []string{"mount", testDiskID1, validMP, "--mode", "ro", "--stage-size", "8MiB"}, wantSubstr: "--stage-size cannot be set when --mode is ro"},
		{name: "ROWithStageDir", args: []string{"mount", testDiskID1, validMP, "--mode=ro", "--stage-dir", "/tmp/x"}, wantSubstr: "--stage-dir cannot be set when --mode is ro"},
		{name: "SyncWithStageSize", args: []string{"mount", testDiskID1, validMP, "--mode", "sync", "--stage-size", "8MiB"}, wantSubstr: "--stage-size cannot be set when --mode is sync"},
		{name: "SyncWithStageDir", args: []string{"mount", testDiskID1, validMP, "--mode=sync", "--stage-dir", "/tmp/x"}, wantSubstr: "--stage-dir cannot be set when --mode is sync"},
		{name: "StageSize1MiB", args: []string{"mount", testDiskID1, validMP, "--stage-size", "1MiB"}, wantSubstr: "minimum is"},
		{name: "StageSizeBelow4MiBBy1B", args: []string{"mount", testDiskID1, validMP, "--stage-size", "4194303"}, wantSubstr: "minimum is"},
		{name: "StageSizeBadUnit", args: []string{"mount", testDiskID1, validMP, "--stage-size", "bad"}, wantSubstr: "invalid --stage-size"},
		{name: "EmptyStageDir", args: []string{"mount", testDiskID1, validMP, "--stage-dir", ""}, wantSubstr: "--stage-dir cannot be empty"},
		{name: "WhitespaceStageDir", args: []string{"mount", testDiskID1, validMP, "--stage-dir=   "}, wantSubstr: "--stage-dir cannot be empty"},
		{name: "InvalidUUID", args: []string{"mount", "not-a-uuid", validMP}, wantSubstr: "invalid disk-id"},
		{name: "NilUUID", args: []string{"mount", testNilUUID, validMP}, wantSubstr: "invalid disk-id"},
		{name: "MissingMountpoint", args: []string{"mount", testDiskID1, missingMP}, wantSubstr: "does not exist or cannot be accessed"},
		{name: "NonDirMountpoint", args: []string{"mount", testDiskID1, fileMP}, wantSubstr: "is not a directory"},
		{name: "InvalidEndpoint", args: []string{"mount", testDiskID1, validMP, "--endpoint", "ftp://127.0.0.1:9000"}, wantSubstr: "invalid endpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, tc.args...)
			require.Equal(t, 2, code, "scene=%s args=%v want=2 got=%d stderr=%s", tc.name, tc.args, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenInvalidEnvVars_Exits2(t *testing.T) {
	mp := t.TempDir()

	cases := []struct {
		name       string
		envKey     string
		envVal     string
		wantSubstr string
	}{
		{name: "BadOTELSampleRatio", envKey: "OTEL_TRACES_SAMPLER_ARG", envVal: "not-float", wantSubstr: "invalid environment variable OTEL_TRACES_SAMPLER_ARG"},
		{name: "InvalidModeBogus", envKey: "BOWL_MOUNT_MODE", envVal: "bogus", wantSubstr: "must be staged, sync, or ro"},
		{name: "StageSize1MiB", envKey: "BOWL_STAGE_SIZE", envVal: "1MiB", wantSubstr: "minimum is"},
		{name: "StageSizeBogus", envKey: "BOWL_STAGE_SIZE", envVal: "bogus", wantSubstr: "BOWL_STAGE_SIZE"},
		{name: "StageDirInsideMountpoint", envKey: "BOWL_STAGE_DIR", envVal: filepath.Join(mp, "env-sub"), wantSubstr: "mountpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			t.Setenv(tc.envKey, tc.envVal)
			code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp)
			require.Equal(t, 2, code, "scene=%s env=%s=%q want=2 got=%d stderr=%s", tc.name, tc.envKey, tc.envVal, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenStageDirPathViolations_Exits2(t *testing.T) {
	mp := t.TempDir()
	regularFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(regularFile, []byte("x"), 0o600))

	symParent := t.TempDir()
	symIntoMP := filepath.Join(symParent, "stage-link")
	require.NoError(t, os.Symlink(filepath.Join(mp, "nested"), symIntoMP))

	symMP := filepath.Join(symParent, "mp-link")
	require.NoError(t, os.Symlink(mp, symMP))

	cases := []struct {
		name       string
		mountpoint string
		stageDir   string
		wantSubstr string
	}{
		{name: "ExistingRegularFile", mountpoint: mp, stageDir: regularFile, wantSubstr: "already exists and is not a directory"},
		{name: "NestedInsideRegularFile", mountpoint: mp, stageDir: filepath.Join(regularFile, "sub"), wantSubstr: "invalid path"},
		{name: "EqualToMountpoint", mountpoint: mp, stageDir: mp, wantSubstr: "mountpoint"},
		{name: "SubdirOfMountpoint", mountpoint: mp, stageDir: filepath.Join(mp, "sub"), wantSubstr: "mountpoint"},
		{name: "SymlinkIntoMountpoint", mountpoint: mp, stageDir: symIntoMP, wantSubstr: "mountpoint"},
		{name: "SymlinkMountpointWithRealSubdir", mountpoint: symMP, stageDir: filepath.Join(mp, "via-real"), wantSubstr: "mountpoint"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, tc.mountpoint, "--stage-dir", tc.stageDir)
			require.Equal(t, 2, code, "scene=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenRelativeDotInsideMountpoint_Exits2(t *testing.T) {
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	subDir := filepath.Join(mp, "nested")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	t.Chdir(subDir)

	code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", ".")

	require.Equal(t, 2, code, "scene=relative_dot_in_mountpoint want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "mountpoint")
}

func TestMount_Validation_GivenStageDirSymlinkLoop_Exits2(t *testing.T) {
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", loopA)

	require.Equal(t, 2, code, "scene=stage_dir_symlink_loop want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "Error:")
}

func TestMount_Validation_GivenLogFileWithForegroundOrEmpty_Exits2(t *testing.T) {
	validMP := t.TempDir()
	validLog := filepath.Join(t.TempDir(), "daemon.log")

	cases := []struct {
		name       string
		args       []string
		wantSubstr string
	}{
		{
			name:       "ShortForegroundWithLogFile",
			args:       []string{"mount", testDiskID1, validMP, "-f", "--log-file", validLog},
			wantSubstr: "--log-file cannot be set with -f/--foreground",
		},
		{
			name:       "LongForegroundWithLogFile",
			args:       []string{"mount", testDiskID1, validMP, "--foreground", "--log-file", validLog},
			wantSubstr: "--log-file cannot be set with -f/--foreground",
		},
		{
			name:       "ExplicitTrueForegroundWithLogFile",
			args:       []string{"mount", testDiskID1, validMP, "--foreground=true", "--log-file", validLog},
			wantSubstr: "--log-file cannot be set with -f/--foreground",
		},
		{
			name:       "EmptyLogFile",
			args:       []string{"mount", testDiskID1, validMP, "--log-file", ""},
			wantSubstr: "--log-file cannot be empty",
		},
		{
			name:       "WhitespaceLogFile",
			args:       []string{"mount", testDiskID1, validMP, "--log-file=   "},
			wantSubstr: "--log-file cannot be empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			spawnCalled := false
			rt.spawnDaemon = func([]string, io.Writer) int {
				spawnCalled = true
				return 0
			}

			code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, tc.args...)

			require.False(t, spawnCalled, "scene=%s want=no_daemon_spawn", tc.name)
			require.Equal(t, 2, code, "scene=%s args=%v want=2 got=%d stderr=%s", tc.name, tc.args, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenLogFilePathViolations_Exits2(t *testing.T) {
	mp := t.TempDir()
	existingDir := t.TempDir()
	regularFile := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(regularFile, []byte("x"), 0o600))

	regularInsideMP := filepath.Join(mp, "inside.log")
	require.NoError(t, os.WriteFile(regularInsideMP, []byte("x"), 0o600))

	symParent := t.TempDir()
	symToDir := filepath.Join(symParent, "dir-link")
	require.NoError(t, os.Symlink(existingDir, symToDir))

	symIntoMP := filepath.Join(symParent, "log-link")
	require.NoError(t, os.Symlink(filepath.Join(mp, "nested.log"), symIntoMP))

	symMP := filepath.Join(symParent, "mp-link")
	require.NoError(t, os.Symlink(mp, symMP))

	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	cases := []struct {
		name       string
		mountpoint string
		extraArgs  []string
		logFile    string
		wantSubstr string
	}{
		{
			name:       "ExistingDirectory",
			mountpoint: mp,
			logFile:    existingDir,
			wantSubstr: "already exists and is a directory",
		},
		{
			name:       "SymlinkToDirectory",
			mountpoint: mp,
			logFile:    symToDir,
			wantSubstr: "already exists and is a directory",
		},
		{
			name:       "NestedInsideRegularFile",
			mountpoint: mp,
			logFile:    filepath.Join(regularFile, "sub.log"),
			wantSubstr: "has invalid path",
		},
		{
			name:       "SymlinkLoop",
			mountpoint: mp,
			logFile:    loopA,
			wantSubstr: "has invalid path",
		},
		{
			name:       "SubpathOfMountpoint",
			mountpoint: mp,
			logFile:    filepath.Join(mp, "daemon.log"),
			wantSubstr: "cannot equal or be inside mountpoint",
		},
		{
			name:       "ExistingFileInsideMountpoint",
			mountpoint: mp,
			logFile:    regularInsideMP,
			wantSubstr: "cannot equal or be inside mountpoint",
		},
		{
			name:       "SymlinkIntoMountpoint",
			mountpoint: mp,
			logFile:    symIntoMP,
			wantSubstr: "cannot equal or be inside mountpoint",
		},
		{
			name:       "SymlinkMountpointWithRealSubpath",
			mountpoint: symMP,
			logFile:    filepath.Join(mp, "via-real.log"),
			wantSubstr: "cannot equal or be inside mountpoint",
		},
		{
			name:       "SyncModeInsideMountpoint",
			mountpoint: mp,
			extraArgs:  []string{"--mode", "sync"},
			logFile:    filepath.Join(mp, "sync.log"),
			wantSubstr: "cannot equal or be inside mountpoint",
		},
		{
			name:       "ROModeInsideMountpoint",
			mountpoint: mp,
			extraArgs:  []string{"--mode", "ro"},
			logFile:    filepath.Join(mp, "ro.log"),
			wantSubstr: "cannot equal or be inside mountpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			spawnCalled := false
			rt.spawnDaemon = func([]string, io.Writer) int {
				spawnCalled = true
				return 0
			}
			args := []string{"mount", testDiskID1, tc.mountpoint}
			args = append(args, tc.extraArgs...)
			args = append(args, "--log-file", tc.logFile)

			code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, args...)

			require.False(t, spawnCalled, "scene=%s want=no_daemon_spawn", tc.name)
			require.Equal(t, 2, code, "scene=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenRelativeLogFileInsideMountpoint_Exits2(t *testing.T) {
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	subDir := filepath.Join(mp, "nested")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	t.Chdir(subDir)
	spawnCalled := false
	rt.spawnDaemon = func([]string, io.Writer) int {
		spawnCalled = true
		return 0
	}

	code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp, "--log-file", "bowl.log")

	require.False(t, spawnCalled)
	require.Equal(t, 2, code, "scene=relative_log_in_mountpoint want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "cannot equal or be inside mountpoint")
}

func TestMount_BackgroundDaemon_GivenValidLogFile_PassesRawArgsVerbatimAndDefersCreation(t *testing.T) {
	mp := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)

	existingLog := filepath.Join(t.TempDir(), "existing.log")
	require.NoError(t, os.WriteFile(existingLog, []byte("prior\n"), 0o600))

	cases := []struct {
		name     string
		args     []string
		wantArgs []string
	}{
		{
			name:     "RelativeNonExistentLogFile",
			args:     []string{"mount", testDiskID1, mp, "--log-file", "logs/daemon.log"},
			wantArgs: []string{testDiskID1, mp, "--log-file", "logs/daemon.log"},
		},
		{
			name:     "ForegroundFalseWithRelativeLogFile",
			args:     []string{"mount", testDiskID1, mp, "--foreground=false", "--log-file", "logs/fgfalse.log"},
			wantArgs: []string{testDiskID1, mp, "--foreground=false", "--log-file", "logs/fgfalse.log"},
		},
		{
			name:     "ExistingRegularLogFile",
			args:     []string{"mount", testDiskID1, mp, "--log-file", existingLog},
			wantArgs: []string{testDiskID1, mp, "--log-file", existingLog},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			setMissingMountInfo(t, rt)
			var spawnedArgs []string
			rt.spawnDaemon = func(args []string, _ io.Writer) int {
				spawnedArgs = append([]string(nil), args...)
				return 0
			}

			code, _, errOut := runCLIWithRuntime(rt, tc.args...)
			require.Equal(t, 0, code, "scene=%s want=0 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, errOut)
			require.Equal(t, tc.wantArgs, spawnedArgs)
			require.NoDirExists(t, filepath.Join(workDir, "logs"), "scene=%s want=parent_never_creates_log_dir", tc.name)
		})
	}
}

func TestMount_AlreadyMounted_GivenActiveMountInfo_ReturnsExit1(t *testing.T) {
	rt := defaultCLIRuntime()
	mp := rt.canonicalMountpoint(t.TempDir())
	setMountInfoContent(t, rt, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	code, out, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f")

	require.Equal(t, 1, code, "scene=already_mounted want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
}

func TestMount_DefaultConfig_GivenNoModeOrStageFlags_UsesStaged512MiBAndDefaultStageDir(t *testing.T) {
	withCleanOTelAndLogger(t)
	withCleanDefaultStageDir(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	t.Setenv("BOWL_MOUNT_MODE", "")
	t.Setenv("BOWL_STAGE_SIZE", "")
	t.Setenv("BOWL_STAGE_DIR", "")

	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "default-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=default_mount want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(536_870_912), captured.cfg.StageSize)
	require.Equal(t, "/var/tmp/bowl-stage", captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir())
}

func TestMount_WhitespaceEnvFallbacks_GivenBlankStageEnvs_UsesDefaults(t *testing.T) {
	withCleanOTelAndLogger(t)
	withCleanDefaultStageDir(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	t.Setenv("BOWL_MOUNT_MODE", "   ")
	t.Setenv("BOWL_STAGE_SIZE", "   ")
	t.Setenv("BOWL_STAGE_DIR", "   ")

	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "blank-env-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=whitespace_env_defaults want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(536_870_912), captured.cfg.StageSize)
	require.Equal(t, "/var/tmp/bowl-stage", captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
}

func TestMount_ForegroundStaged_GivenExplicitFlags_AppliesStageConfigAndCleansInstanceDir(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "custom-stage")
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())

	t.Setenv("BOWL_TOKEN", "env-mount-tok")
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "mount-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, out, errOut := runCLIWithRuntime(rt,
		"mount", testDiskID1, mp,
		"-f", "--mode=staged", "--stage-size", "16MiB", "--stage-dir", stageDir,
		"--direct-io", "--allow-other", "--debug", "--stats-file",
		"--admin-addr", "off",
		"--log-level", "debug",
		"--log-format", "json",
		"--trace-sample-ratio", "0.5",
	)

	require.Equal(t, 0, code, "scene=foreground_staged want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Empty(t, storage.lastAuth)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(16_777_216), captured.cfg.StageSize)
	require.Equal(t, stageDir, captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
	require.True(t, captured.instDirAlive, "scene=during_serveFUSE want=instance_dir_exists got=false")
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir(), "scene=after_exit want=instance_dir_removed")
	require.Equal(t, uint64(1), captured.cfg.Epoch)
	require.Equal(t, uint64(1), captured.cfg.RootIno)
	require.True(t, captured.cfg.DirectIO)
	require.True(t, captured.cfg.AllowOther)
	require.True(t, captured.cfg.Debug)
	require.True(t, captured.cfg.EnableStatsFile)
	require.True(t, captured.metaFoundNow, "scene=ready_callback want=mountMeta_written got=false")
	require.Equal(t, testDiskID1, captured.metaInReady.DiskID)
	require.Equal(t, "off", captured.metaInReady.AdminAddr)
	_, metaAfterExit := rt.findMountMeta(mp)
	require.False(t, metaAfterExit, "scene=after_exit want=mountMeta_removed got=true")
	require.Contains(t, errOut, `"msg":"mount_ready"`)
	require.Contains(t, errOut, `"msg":"unmount_complete"`)
	require.NotContains(t, errOut, "stage_dir_on_tmpfs")
}

func TestMount_ForegroundStaged_GivenMinStageSize4MiB_Succeeds(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "min-stage")
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "min-stage-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--stage-size", "4MiB", "--stage-dir", stageDir, "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=min_stage_size_4MiB want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, uint64(4_194_304), captured.cfg.StageSize)
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir())
}

func TestMount_ForegroundStaged_GivenEnvFallbacksAndTmpfsSeam_AppliesEnvAndLogsTmpfsWarning(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, true)
	parent := t.TempDir()
	mp := filepath.Join(parent, "mp")
	require.NoError(t, os.MkdirAll(mp, 0o755))
	siblingStageDir := filepath.Join(parent, "mp-stage")
	t.Setenv("BOWL_MOUNT_MODE", "  StAgEd ")
	t.Setenv("BOWL_STAGE_SIZE", "32MiB")
	t.Setenv("BOWL_STAGE_DIR", siblingStageDir)

	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "staged-env-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=env_staged_tmpfs_warn want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(33_554_432), captured.cfg.StageSize)
	require.Equal(t, siblingStageDir, captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir())
	require.Contains(t, errOut, `"level":"WARN"`)
	require.Contains(t, errOut, `"msg":"stage_dir_on_tmpfs"`)
	require.Contains(t, errOut, siblingStageDir)
}

func TestMount_ForegroundRO_GivenCLIFlagAndAmbientStageEnv_IgnoresStageEnvAndSkipsSession(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, true)
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-ro")
	t.Setenv("BOWL_ADMIN_ADDR", "off")
	t.Setenv("BOWL_LOG_LEVEL", "warn")
	t.Setenv("BOWL_LOG_FORMAT", "text")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")
	t.Setenv("BOWL_MOUNT_MODE", "staged")
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)

	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "ro-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode", "ro")

	require.Equal(t, 0, code, "scene=ro_ignores_stage_env want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeRO, captured.cfg.Mode)
	require.True(t, captured.cfg.IsReadOnly())
	require.Zero(t, captured.cfg.Epoch)
	require.Zero(t, captured.cfg.StageSize)
	require.Empty(t, captured.cfg.StageDir)
	require.Nil(t, captured.cfg.StageSession)
	require.NoDirExists(t, unusedStageDir)
	require.NotContains(t, errOut, "stage_dir_on_tmpfs")
}

func TestMount_ForegroundRO_GivenEnvModeWhitespaceMixedCase_EnablesReadOnly(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, true)
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-env-ro")
	t.Setenv("BOWL_MOUNT_MODE", " Ro ")
	t.Setenv("BOWL_STAGE_SIZE", "invalid-ignored")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)

	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "ro-env-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, mp)

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=env_ro_mode want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeRO, captured.cfg.Mode)
	require.True(t, captured.cfg.IsReadOnly())
	require.Nil(t, captured.cfg.StageSession)
	require.NoDirExists(t, unusedStageDir)
}

func TestMount_ForegroundSync_GivenCLIFlagAndAmbientStageEnv_IgnoresStageEnvSkipsSessionAndMountsWritable(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, true)
	setMountRenewIntervalStub(rt, 15*time.Millisecond)
	mp := t.TempDir()
	nestedStageDir := filepath.Join(mp, "ambient-stage-inside-mp")
	t.Setenv("BOWL_MOUNT_MODE", "ro")
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", nestedStageDir)

	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "sync-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	setMountStorageFake(rt, storage)

	var captured fusefs.Config
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, gotMP string, ready func()) error {
		captured = *cfg
		require.Equal(t, rt.canonicalMountpoint(mp), gotMP)
		ready()
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 1
		}, time.Second, 5*time.Millisecond, "scene=sync_renew want>=1_renew_req")
		return nil
	}

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode", "sync", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=sync_ignores_stage_env want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeSync, captured.Mode)
	require.False(t, captured.IsReadOnly())
	require.False(t, captured.IsStaged())
	require.Equal(t, uint64(1), captured.Epoch, "scene=sync_writable_epoch want=1 got=%d", captured.Epoch)
	require.Zero(t, captured.StageSize)
	require.Empty(t, captured.StageDir)
	require.Nil(t, captured.StageSession)
	require.NoDirExists(t, nestedStageDir)
	require.NotContains(t, errOut, "stage_dir_on_tmpfs")
	requireRenewRequestsMatch(t, storage.getRenewReqs(), testDiskID1, "01950000-0000-7000-9000-000000000001", false, 1)
}

func TestMount_ForegroundSync_GivenEnvModeMixedCaseAndExplicitStageFlagsOrZeroEpoch_EnforcesSyncRules(t *testing.T) {
	t.Setenv("BOWL_MOUNT_MODE", "  SyNc  ")
	t.Setenv("BOWL_STAGE_SIZE", "bogus-ignored")
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-env-sync")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)
	mp := t.TempDir()

	rt := defaultCLIRuntime()
	codeSize, _, errSize := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp, "--stage-size", "8MiB")
	require.Equal(t, 2, codeSize, "scene=env_sync_with_stage_size want=2 got=%d stderr=%s", codeSize, errSize)
	require.Contains(t, errSize, "--stage-size cannot be set when --mode is sync")

	codeDir, _, errDir := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", "/tmp/x")
	require.Equal(t, 2, codeDir, "scene=env_sync_with_stage_dir want=2 got=%d stderr=%s", codeDir, errDir)
	require.Contains(t, errDir, "--stage-dir cannot be set when --mode is sync")

	withCleanOTelAndLogger(t)
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	faultyStorage := newFakeStorage()
	seedFaultyMountStorage(false, true)(faultyStorage)
	setMountStorageFake(rt, faultyStorage)

	codeZeroEpoch, _, errZeroEpoch := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, codeZeroEpoch, "scene=env_sync_zero_epoch want=1 got=%d stderr=%s", codeZeroEpoch, errZeroEpoch)
	require.Contains(t, errZeroEpoch, "valid write mount epoch")
	require.NoDirExists(t, unusedStageDir)

	okStorage := newFakeStorage()
	okStorage.seedDisk(testDiskID1, "sync-env-disk", 1<<30, 0)
	setMountStorageFake(rt, okStorage)
	captured := stubMountServeFUSE(t, rt, mp)

	codeOK, _, errOK := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 0, codeOK, "scene=env_sync_ok want=0 got=%d stderr=%s", codeOK, errOK)
	require.Equal(t, fusefs.MountModeSync, captured.cfg.Mode)
	require.Equal(t, uint64(1), captured.cfg.Epoch)
	require.Nil(t, captured.cfg.StageSession)
	require.NoDirExists(t, unusedStageDir)
}

func noOpStorageSetup(*fakeStorage) {}

func seedFaultyMountStorage(zeroRoot, zeroEpoch bool) func(*fakeStorage) {
	return func(s *fakeStorage) {
		s.seedDisk(testDiskID1, "d1", 1<<30, 0)
		s.mountZeroRoot = zeroRoot
		s.mountZeroEpoch = zeroEpoch
	}
}

func TestMount_HandshakeErrors_GivenServerRejectsMount_ReturnsExit1AndCleansStageSession(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(*fakeStorage)
		wantSubstr string
	}{
		{name: "DiskNotFound", setup: noOpStorageSetup, wantSubstr: `disk ` + testDiskID1 + ` branch "main" not found`},
		{name: "EmptyRootInode", setup: seedFaultyMountStorage(true, false), wantSubstr: "invalid root directory metadata"},
		{name: "RWZeroEpoch", setup: seedFaultyMountStorage(false, true), wantSubstr: "valid write mount epoch"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withCleanOTelAndLogger(t)
			rt := defaultCLIRuntime()
			mp := t.TempDir()
			stageDir := filepath.Join(t.TempDir(), "handshake-stage")
			t.Setenv("BOWL_STAGE_DIR", stageDir)
			setMissingMountInfo(t, rt)
			setTempMountMetaDirs(rt, t.TempDir())

			storage := newFakeStorage()
			tc.setup(storage)
			setMountStorageFake(rt, storage)

			code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")
			require.Equal(t, 1, code, "scene=%s want=1 got=%d stderr=%s", tc.name, code, errOut)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
			requireEmptyDir(t, stageDir, "handshake_cleanup_"+tc.name)
		})
	}
}

func TestMount_ServeError_GivenFUSEFailure_ReturnsExit1AndCleansMetaAndStageSession(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "serve-err-stage")
	t.Setenv("BOWL_STAGE_DIR", stageDir)
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	setMountStorageFake(rt, storage)
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		return errors.New("fuse device failed")
	}

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	require.Equal(t, 1, code, "scene=serve_error want=1 got=%d stderr=%s", code, errOut)
	require.Contains(t, errOut, "fuse device failed")
	_, remains := rt.findMountMeta(mp)
	require.False(t, remains, "scene=serve_error want=mountMeta_removed got=true")
	requireEmptyDir(t, stageDir, "serve_error_stage_cleanup")
}

func TestRunMount_StageSessionError_GivenUncreatableStageDir_FailsBeforeMountHandshake(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	setMountStorageFake(rt, storage)

	blockerFile := filepath.Join(t.TempDir(), "file-blocker")
	require.NoError(t, os.WriteFile(blockerFile, []byte("x"), 0o600))

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: "off",
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  filepath.Join(blockerFile, "nested"),
	}

	var logBuf bytes.Buffer
	err := rt.runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "initialize stage directory")
	requireStorageEpochZero(t, storage, "stage_session_error_before_handshake")
}

func TestMount_BackgroundDaemon_GivenROAndStagedFlags_SpawnsWithoutParentStageSession(t *testing.T) {
	mp := t.TempDir()
	deferredStageDir := filepath.Join(t.TempDir(), "daemon-only-stage")

	cases := []struct {
		name     string
		args     []string
		wantArgs []string
	}{
		{
			name:     "ReadOnlyMode",
			args:     []string{"mount", testDiskID1, mp, "--mode", "ro"},
			wantArgs: []string{testDiskID1, mp, "--mode", "ro"},
		},
		{
			name:     "ForegroundFalseExplicit",
			args:     []string{"mount", testDiskID1, mp, "--foreground=false", "--mode", "ro"},
			wantArgs: []string{testDiskID1, mp, "--foreground=false", "--mode", "ro"},
		},
		{
			name:     "StagedModeDefersStageDirCreation",
			args:     []string{"mount", testDiskID1, mp, "--stage-dir", deferredStageDir},
			wantArgs: []string{testDiskID1, mp, "--stage-dir", deferredStageDir},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			setMissingMountInfo(t, rt)
			var spawnedArgs []string
			rt.spawnDaemon = func(args []string, _ io.Writer) int {
				spawnedArgs = append([]string(nil), args...)
				return 0
			}

			code, _, errOut := runCLIWithRuntime(rt, tc.args...)
			require.Equal(t, 0, code, "scene=%s want=0 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, errOut)
			require.Equal(t, tc.wantArgs, spawnedArgs)
			require.NoDirExists(t, deferredStageDir, "scene=%s want=parent_never_opens_stage_dir", tc.name)
		})
	}
}

func TestMount_GivenBoolFalseAndRepeatedInvocations_ResetsPerCallState(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "isolation-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	captured := stubMountServeFUSE(t, rt, mp)
	code, _, errOut := runCLIWithRuntime(rt,
		"mount", testDiskID1, mp,
		"-f", "--mode=ro", "--allow-other", "--direct-io", "--debug", "--stats-file", "--admin-addr", "off",
	)
	require.Equal(t, 0, code, "first invocation stderr=%s", errOut)
	require.Equal(t, fusefs.MountModeRO, captured.cfg.Mode)
	require.True(t, captured.cfg.AllowOther)
	require.True(t, captured.cfg.DirectIO)
	require.True(t, captured.cfg.Debug)
	require.True(t, captured.cfg.EnableStatsFile)

	captured2 := stubMountServeFUSE(t, rt, mp)
	code, _, errOut = runCLIWithRuntime(rt,
		"mount", testDiskID1, mp,
		"-f", "--allow-other=false", "--direct-io=false", "--debug=false", "--stats-file=false", "--admin-addr", "off",
	)
	require.Equal(t, 0, code, "second invocation stderr=%s", errOut)
	require.Equal(t, fusefs.MountModeStaged, captured2.cfg.Mode)
	require.False(t, captured2.cfg.AllowOther)
	require.False(t, captured2.cfg.DirectIO)
	require.False(t, captured2.cfg.Debug)
	require.False(t, captured2.cfg.EnableStatsFile)
}

func TestReadDaemonHandshake_GivenOKErrorAndEOF_ReturnsExpectedExitCodes(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantCode   int
		wantSubstr string
	}{
		{name: "OK", input: "OK\n", wantCode: 0, wantSubstr: ""},
		{name: "ExplicitError", input: "ERR:mount failed\n", wantCode: 1, wantSubstr: "background daemon failed to initialize: mount failed"},
		{name: "UnexpectedEOF", input: "", wantCode: 1, wantSubstr: "before completing mount handshake"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var errBuf strings.Builder
			code := readDaemonHandshake(strings.NewReader(tc.input), &errBuf)
			require.Equal(t, tc.wantCode, code, "scene=%s want=%d got=%d stderr=%s", tc.name, tc.wantCode, code, errBuf.String())
			require.Contains(t, errBuf.String(), tc.wantSubstr)
		})
	}
}

func TestResolveLogFile_GivenExplicitPath_CreatesParentDirAndFile(t *testing.T) {
	rt := defaultCLIRuntime()
	explicitPath := filepath.Join(t.TempDir(), "nested", "logs", "custom.log")

	f, gotPath, err := rt.resolveLogFile(explicitPath, testDiskID1)

	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, explicitPath, gotPath)
}

func TestResolveLogFile_GivenSystemDirEACCES_FallsBackToXDGStateHome(t *testing.T) {
	rt := defaultCLIRuntime()
	rt.openSystemLogFn = func(string) (*os.File, string, error) {
		return nil, "/var/log/bowl/mount.log", syscall.EACCES
	}
	xdgDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", xdgDir)

	f, gotPath, err := rt.resolveLogFile("", testDiskID1)

	require.NoError(t, err)
	require.NoError(t, f.Close())
	wantFallback := filepath.Join(xdgDir, "bowl", "logs", "mount-"+testDiskID1+".log")
	require.Equal(t, wantFallback, gotPath)
}

func TestRunMount_LeaseLifecycle_GivenCanceledCtx_RenewsAndUnmountsBeforeRemovingMeta(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setMountRenewIntervalStub(rt, 15*time.Millisecond)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	setMountStorageFake(rt, storage)

	var metaExistsAtUnmount bool
	storage.onUnmountBranch = func(_ *storagev1.UnmountBranchRequest) {
		_, metaExistsAtUnmount = rt.findMountMeta(mp)
	}

	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	rt.serveFUSE = func(ctx context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 2
		}, time.Second, 5*time.Millisecond, "serveFUSE 阻塞期间必须周期发送 RenewMount")
		cancelParent()
		<-ctx.Done()
		return nil
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: "off",
		logLevel:  "info",
		logFormat: "json",
	}
	stageDir := t.TempDir()
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  stageDir,
	}

	var logBuf bytes.Buffer
	err := rt.runMount(parentCtx, opts, baseCfg, mp, &logBuf, nil)

	require.NoError(t, err)
	requireEmptyDir(t, stageDir, "runMount_lifecycle_stage_cleanup")
	renews := storage.getRenewReqs()
	require.GreaterOrEqual(t, len(renews), 2)
	requireRenewRequestsMatch(t, renews, testDiskID1, "01950000-0000-7000-9000-000000000001", false, 1)

	unmounts := storage.getUnmountReqs()
	require.Len(t, unmounts, 1, "serveFUSE 返回后必须调用一次 UnmountDisk")
	require.Equal(t, testDiskID1, unmounts[0].GetDiskId())
	require.Equal(t, "01950000-0000-7000-9000-000000000001", unmounts[0].GetMountId())
	require.True(t, metaExistsAtUnmount, "UnmountDisk 必须在清理 mountMeta 文件之前调用")
	_, metaAfter := rt.findMountMeta(mp)
	require.False(t, metaAfter, "UnmountDisk 完成后必须移除 mountMeta 文件")
}

func fetchReadyzStatusAndBody(t *testing.T, adminAddr string) (statusCode int, body string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+adminAddr+"/readyz", http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	rawBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(rawBody)
}

func TestRunMount_LeaseRenewTerminalError_GivenAllPermanentCodes_FencesRawFSFlipsReadyzStopsLoopAndSkipsUnmount(t *testing.T) {
	ebusyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale epoch"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: "bowl.storage", Reason: "EBUSY"})
	require.NoError(t, err)
	ebusyErr.AddDetail(detail)

	cases := []struct {
		name       string
		renewErr   error
		wantStatus fuse.Status
		wantReason string
	}{
		{
			name:       "CodeFailedPrecondition_EBUSY",
			renewErr:   ebusyErr,
			wantStatus: fuse.EBUSY,
			wantReason: "stale epoch",
		},
		{
			name:       "CodeNotFound",
			renewErr:   connect.NewError(connect.CodeNotFound, errors.New("mount not found")),
			wantStatus: fuse.ENOENT,
			wantReason: "mount not found",
		},
		{
			name:       "CodePermissionDenied",
			renewErr:   connect.NewError(connect.CodePermissionDenied, errors.New("permission denied")),
			wantStatus: fuse.EACCES,
			wantReason: "permission denied",
		},
		{
			name:       "CodeInvalidArgument",
			renewErr:   connect.NewError(connect.CodeInvalidArgument, errors.New("bad mount id")),
			wantStatus: fuse.EINVAL,
			wantReason: "bad mount id",
		},
		{
			name:       "CodeUnimplemented",
			renewErr:   connect.NewError(connect.CodeUnimplemented, errors.New("renew unsupported")),
			wantStatus: fuse.Status(syscall.EOPNOTSUPP),
			wantReason: "renew unsupported",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withCleanOTelAndLogger(t)
			rt := defaultCLIRuntime()
			mp := t.TempDir()
			setMissingMountInfo(t, rt)
			setTempMountMetaDirs(rt, t.TempDir())
			setMountRenewIntervalStub(rt, 10*time.Millisecond)

			storage := newFakeStorage()
			storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
			storage.renewErr = tc.renewErr
			setMountStorageFake(rt, storage)

			var (
				logBuf    syncBuffer
				gotFenced bool
				gotStatus fuse.Status
				gotReason string
				readyCode int
				readyBody string
			)
			rt.serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
				ready()
				meta, ok := rt.findMountMeta(gotMP)
				require.True(t, ok)

				require.Eventually(t, func() bool {
					return strings.Contains(logBuf.String(), "mount_lease_renew_stopped") && rawFS.IsFenced()
				}, time.Second, 5*time.Millisecond, "scene=%s want=mount_lease_renew_stopped_and_fenced", tc.name)

				gotFenced = rawFS.IsFenced()
				gotStatus = rawFS.FenceStatus()
				gotReason = rawFS.FenceReason()
				readyCode, readyBody = fetchReadyzStatusAndBody(t, meta.AdminAddr)

				time.Sleep(35 * time.Millisecond)
				return nil
			}

			opts := &mountOptions{
				endpoint:  "http://127.0.0.1:9000",
				adminAddr: "127.0.0.1:0",
				logLevel:  "info",
				logFormat: "json",
			}
			baseCfg := &fusefs.Config{
				DiskID:    testDiskID1,
				Mode:      fusefs.MountModeStaged,
				StageSize: fusefs.DefaultStageSize,
				StageDir:  t.TempDir(),
			}

			runErr := rt.runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)
			require.NoError(t, runErr)
			require.True(t, gotFenced, "scene=%s want=rawFS_fenced", tc.name)
			require.Equal(t, tc.wantStatus, gotStatus, "scene=%s", tc.name)
			require.Contains(t, gotReason, tc.wantReason, "scene=%s", tc.name)
			require.Equal(t, http.StatusServiceUnavailable, readyCode, "scene=%s readyz body=%s", tc.name, readyBody)
			require.Contains(t, readyBody, tc.wantReason, "scene=%s", tc.name)
			require.Len(t, storage.getRenewReqs(), 1, "scene=%s want=1_renew_req got=%d", tc.name, len(storage.getRenewReqs()))
			require.Empty(t, storage.getUnmountReqs(), "scene=%s fenced mount must skip UnmountDisk RPC", tc.name)
		})
	}
}

func TestPermanentRenewFenceStatus_GivenErrorInfoOverrideAndPermanentCodes_DelegatesToRPCMapError(t *testing.T) {
	newErrWithReason := func(code connect.Code, reason string) error {
		cErr := connect.NewError(code, errors.New("server detail"))
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Domain: "bowl.storage",
			Reason: reason,
		})
		require.NoError(t, err)
		cErr.AddDetail(detail)
		return cErr
	}

	st, code, permanent := permanentRenewFenceStatus(newErrWithReason(connect.CodePermissionDenied, "EPERM"))
	require.True(t, permanent)
	require.Equal(t, connect.CodePermissionDenied, code)
	require.Equal(t, fuse.EPERM, st)

	st, code, permanent = permanentRenewFenceStatus(newErrWithReason(connect.CodeFailedPrecondition, "EROFS"))
	require.True(t, permanent)
	require.Equal(t, connect.CodeFailedPrecondition, code)
	require.Equal(t, fuse.Status(syscall.EROFS), st)

	st, code, permanent = permanentRenewFenceStatus(connect.NewError(connect.CodeUnauthenticated, errors.New("unauth")))
	require.False(t, permanent)
	require.Equal(t, connect.CodeUnauthenticated, code)
	require.Equal(t, fuse.OK, st)

	st, code, permanent = permanentRenewFenceStatus(connect.NewError(connect.CodePermissionDenied, errors.New("denied")))
	require.True(t, permanent)
	require.Equal(t, connect.CodePermissionDenied, code)
	require.Equal(t, fuse.EACCES, st)

	st, code, permanent = permanentRenewFenceStatus(connect.NewError(connect.CodeUnimplemented, errors.New("unimpl")))
	require.True(t, permanent)
	require.Equal(t, connect.CodeUnimplemented, code)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)

	st, code, permanent = permanentRenewFenceStatus(connect.NewError(connect.CodeUnavailable, errors.New("transient")))
	require.False(t, permanent)
	require.Equal(t, connect.CodeUnavailable, code)
	require.Equal(t, fuse.OK, st)
}

func TestRunMount_LeaseRenewExternalFence_GivenRawFSFenced_StopsRenewerFlipsReadyzAndSkipsUnmount(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setMountRenewIntervalStub(rt, 15*time.Millisecond)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "rpc-fenced-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	setMountStorageFake(rt, storage)

	var (
		logBuf         syncBuffer
		renewsAtFence  int
		readyBefore    int
		readyAfter     int
		readyAfterBody string
	)
	rt.serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		ready()
		meta, ok := rt.findMountMeta(gotMP)
		require.True(t, ok)

		readyBefore, _ = fetchReadyzStatusAndBody(t, meta.AdminAddr)
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 1
		}, time.Second, 5*time.Millisecond)

		rawFS.Fence(fuse.EBUSY, "rpc stale epoch")

		time.Sleep(20 * time.Millisecond)
		renewsAtFence = len(storage.getRenewReqs())
		readyAfter, readyAfterBody = fetchReadyzStatusAndBody(t, meta.AdminAddr)

		time.Sleep(60 * time.Millisecond)
		return nil
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: "127.0.0.1:0",
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  t.TempDir(),
	}

	err := rt.runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, readyBefore)
	require.Equal(t, http.StatusServiceUnavailable, readyAfter)
	require.Contains(t, readyAfterBody, "rpc stale epoch")
	require.Len(t, storage.getRenewReqs(), renewsAtFence, "renewer must stop immediately after RawFS.FencedCh closes")
	require.Empty(t, storage.getUnmountReqs(), "fenced mount must skip UnmountDisk RPC")
}

func TestRunMount_LeaseRenewTransientError_GivenTransientFailuresThenRecovery_RetriesAcceleratedExpiresReadyzHealsAndRestoresCadence(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	mp := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setMountRenewIntervalStub(rt, 120*time.Millisecond)
	setMountRenewRetryIntervalStub(rt, 15*time.Millisecond)

	var (
		clockMu sync.Mutex
		nowTime = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	)
	rt.mountNowFn = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return nowTime
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		nowTime = nowTime.Add(d)
		clockMu.Unlock()
	}

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "transient-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30

	var (
		mu          sync.Mutex
		renewTimes  []time.Time
		renewCalls  int
		allowHealCh = make(chan struct{})
	)
	storage.onRenewMount = func(_ *storagev1.RenewMountRequest) (*storagev1.RenewMountResponse, error) {
		mu.Lock()
		renewCalls++
		callNum := renewCalls
		renewTimes = append(renewTimes, time.Now())
		mu.Unlock()
		switch callNum {
		case 1:
			return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("temporary timeout"))
		case 2:
			return nil, connect.NewError(connect.CodeInternal, errors.New("transient backend error"))
		case 3:
			<-allowHealCh
			return &storagev1.RenewMountResponse{LeaseTtlSeconds: 30}, nil
		default:
			return &storagev1.RenewMountResponse{LeaseTtlSeconds: 30}, nil
		}
	}
	setMountStorageFake(rt, storage)

	var (
		logBuf           syncBuffer
		readyInitial     int
		readyExpired     int
		readyHealed      int
		fencedAfterRetry bool
	)
	rt.serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		ready()
		meta, ok := rt.findMountMeta(gotMP)
		require.True(t, ok)

		readyInitial, _ = fetchReadyzStatusAndBody(t, meta.AdminAddr)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return renewCalls >= 2
		}, time.Second, 5*time.Millisecond)

		fencedAfterRetry = rawFS.IsFenced()

		advanceClock(31 * time.Second)
		readyExpired, _ = fetchReadyzStatusAndBody(t, meta.AdminAddr)

		close(allowHealCh)
		require.Eventually(t, func() bool {
			code, _ := fetchReadyzStatusAndBody(t, meta.AdminAddr)
			return code == http.StatusOK
		}, time.Second, 5*time.Millisecond)
		readyHealed, _ = fetchReadyzStatusAndBody(t, meta.AdminAddr)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return renewCalls >= 4
		}, time.Second, 5*time.Millisecond)
		return nil
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		adminAddr: "127.0.0.1:0",
		logLevel:  "info",
		logFormat: "json",
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  t.TempDir(),
	}

	err := rt.runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)

	require.NoError(t, err)
	require.Contains(t, logBuf.String(), "mount_lease_renew_failed")
	require.Equal(t, http.StatusOK, readyInitial)
	require.False(t, fencedAfterRetry, "transient error must not fence RawFS")
	require.Equal(t, http.StatusServiceUnavailable, readyExpired, "expired lease must flip /readyz to 503")
	require.Equal(t, http.StatusOK, readyHealed, "successful RenewMount must heal /readyz back to 200")
	require.Len(t, storage.getUnmountReqs(), 1, "unfenced mount must still call UnmountDisk on exit")

	mu.Lock()
	times := append([]time.Time(nil), renewTimes...)
	mu.Unlock()
	require.GreaterOrEqual(t, len(times), 4)
	acceleratedGap := times[1].Sub(times[0])
	restoredNormalGap := times[3].Sub(times[2])
	require.Less(t, acceleratedGap, 80*time.Millisecond, "transient failure must use accelerated retry interval, got %s", acceleratedGap)
	require.GreaterOrEqual(t, restoredNormalGap, 90*time.Millisecond, "recovery must restore normal renew interval, got %s", restoredNormalGap)
}

func TestDefaultMountRenewRetryInterval_GivenNormalIntervals_ClampsToMinOneSecond(t *testing.T) {
	cases := []struct {
		name   string
		normal time.Duration
		want   time.Duration
	}{
		{name: "TenSecondsNormal", normal: 10 * time.Second, want: time.Second},
		{name: "OneSecondNormal", normal: time.Second, want: time.Second},
		{name: "SubSecondNormal", normal: 200 * time.Millisecond, want: 200 * time.Millisecond},
		{name: "ZeroNormal", normal: 0, want: time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, defaultMountRenewRetryInterval(tc.normal), "scene=%s", tc.name)
		})
	}
}

func TestDefaultMountRenewInterval_GivenTTLValues_ClampsMinimumOneSecond(t *testing.T) {
	cases := []struct {
		name string
		ttl  uint32
		want time.Duration
	}{
		{name: "ZeroTTL", ttl: 0, want: time.Second},
		{name: "OneSecondTTL", ttl: 1, want: time.Second},
		{name: "ThreeSecondsTTL", ttl: 3, want: time.Second},
		{name: "ThirtySecondsTTL", ttl: 30, want: 10 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, defaultMountRenewInterval(tc.ttl), "scene=%s ttl=%d", tc.name, tc.ttl)
		})
	}
}

func TestValidatePathNotInMountpoint_GivenEqualAndSymlinkLoop_ReturnsFlagErrors(t *testing.T) {
	rt := defaultCLIRuntime()
	mp := rt.canonicalMountpoint(t.TempDir())
	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	equalErr := validatePathNotInMountpoint("--log-file", mp, mp, mp)
	require.ErrorContains(t, equalErr, "--log-file")
	require.ErrorContains(t, equalErr, "cannot equal or be inside mountpoint")

	loopErr := validatePathNotInMountpoint("--log-file", loopA, mp, mp)
	require.ErrorContains(t, loopErr, "failed to resolve --log-file")
}

func TestRunDaemonWorker_GivenValidPaths_DetachesCwdToRootAndSignalsOK(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setStageDirTmpfsStub(rt, false)

	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "daemon-stage")
	logPath := filepath.Join(t.TempDir(), "daemon.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "daemon-worker-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	setReadinessPipeStub(rt, w)

	var redirectedFile *os.File
	rt.redirectDaemonStdio = func(f *os.File) {
		redirectedFile = f
	}

	var events []string
	rt.chdirFn = func(dir string) error {
		_, statErr := os.Stat(logPath)
		require.NoError(t, statErr, "log file must be opened before chdir")
		events = append(events, "chdir:"+dir)
		return nil
	}
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		require.Equal(t, mp, gotMP)
		events = append(events, "serve")
		ready()
		return nil
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		logFile:   logPath,
		adminAddr: "off",
		logLevel:  "info",
		logFormat: "json",
		mode:      fusefs.MountModeStaged,
	}
	baseCfg := &fusefs.Config{
		DiskID:    testDiskID1,
		Mode:      fusefs.MountModeStaged,
		StageSize: fusefs.DefaultStageSize,
		StageDir:  stageDir,
	}

	code := rt.runDaemonWorker(context.Background(), opts, baseCfg, mp)

	require.Equal(t, 0, code)
	require.Equal(t, []string{"chdir:/", "serve"}, events)
	require.NotNil(t, redirectedFile)
	pipeBytes, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	require.Equal(t, "OK\n", string(pipeBytes))
}

func TestRunDaemonWorker_GivenChdirError_ReportsERRAndExits1(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	mp := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "daemon-err.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "daemon-err-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	setReadinessPipeStub(rt, w)
	rt.redirectDaemonStdio = func(*os.File) {}

	serveCalled := false
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, _ func()) error {
		serveCalled = true
		return nil
	}
	rt.chdirFn = func(dir string) error {
		require.Equal(t, "/", dir)
		return errors.New("chdir denied")
	}

	opts := &mountOptions{
		endpoint:  "http://127.0.0.1:9000",
		logFile:   logPath,
		adminAddr: "off",
		logLevel:  "info",
		logFormat: "json",
		mode:      fusefs.MountModeRO,
	}
	baseCfg := &fusefs.Config{
		DiskID: testDiskID1,
		Mode:   fusefs.MountModeRO,
	}

	code := rt.runDaemonWorker(context.Background(), opts, baseCfg, mp)

	require.Equal(t, 1, code)
	require.False(t, serveCalled, "runMount must not execute when chdirFn fails")
	pipeBytes, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	require.Equal(t, "ERR:change daemon working directory to /: chdir denied\n", string(pipeBytes))
}

func TestMount_DaemonWorker_GivenRelativePaths_ResolvesBeforeChdirToRoot(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setStageDirTmpfsStub(rt, false)

	t.Chdir(t.TempDir())
	callerDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(callerDir, "rel-mp"), 0o755))
	absMP := filepath.Join(callerDir, "rel-mp")
	absStageDir := filepath.Join(callerDir, "rel-stage")
	absLogFile := filepath.Join(callerDir, "rel-logs", "worker.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "worker-rel-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	setReadinessPipeStub(rt, w)
	rt.redirectDaemonStdio = func(*os.File) {}

	var chdirArgs []string
	rt.chdirFn = func(dir string) error {
		_, statErr := os.Stat(absLogFile)
		require.NoError(t, statErr, "relative --log-file must open against caller cwd before chdir")
		chdirArgs = append(chdirArgs, dir)
		return nil
	}
	captured := stubMountServeFUSE(t, rt, absMP)
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	code, out, errOut := runCLIWithRuntime(rt,
		"mount", testDiskID1, "rel-mp",
		"--stage-dir", "rel-stage",
		"--log-file", filepath.Join("rel-logs", "worker.log"),
		"--admin-addr", "off",
	)

	require.Equal(t, 0, code, "scene=daemon_worker_relative_paths want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"/"}, chdirArgs)
	require.Equal(t, absStageDir, captured.cfg.StageDir)

	var handshakeErr strings.Builder
	require.Equal(t, 0, readDaemonHandshake(r, &handshakeErr), "stderr=%s", handshakeErr.String())
	logData, readErr := os.ReadFile(absLogFile)
	require.NoError(t, readErr)
	require.Contains(t, string(logData), `"msg":"mount_ready"`)
}

func TestMount_Foreground_GivenRelativePaths_PreservesCallerWorkingDirectory(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	setStageDirTmpfsStub(rt, false)

	t.Chdir(t.TempDir())
	cwdBefore, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(cwdBefore, "fg-mp"), 0o755))

	chdirCalled := false
	rt.chdirFn = func(string) error {
		chdirCalled = true
		return nil
	}

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "fg-cwd-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)
	captured := stubMountServeFUSE(t, rt, filepath.Join(cwdBefore, "fg-mp"))

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, "fg-mp", "-f", "--stage-dir", "fg-stage", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=foreground_preserves_cwd want=0 got=%d stderr=%s", code, errOut)
	require.False(t, chdirCalled, "foreground mode must not call chdirFn")
	cwdAfter, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, cwdBefore, cwdAfter)
	require.Equal(t, filepath.Join(cwdBefore, "fg-stage"), captured.cfg.StageDir)
}

func TestMount_AlreadyMounted_GivenSymlinkToActiveMount_ReturnsExit1(t *testing.T) {
	rt := defaultCLIRuntime()
	realParent := t.TempDir()
	realMP := filepath.Join(realParent, "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := rt.canonicalMountpoint(realMP)

	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	symParent := filepath.Join(t.TempDir(), "sym-parent")
	require.NoError(t, os.Symlink(realParent, symParent))
	viaSymParent := filepath.Join(symParent, "real-mp")

	setMountInfoContent(t, rt, "100 30 0:50 / "+canonMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	code, out, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, symMP, "-f")
	require.Equal(t, 1, code, "scene=dup_mount_leaf_symlink want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")

	code, out, errOut = runCLIWithRuntime(rt, "mount", testDiskID1, viaSymParent, "-f")
	require.Equal(t, 1, code, "scene=dup_mount_parent_symlink want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
}

func TestMount_Foreground_GivenSymlinkMountpoint_CanonicalizesServeFUSEAndMountMeta(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setStageDirTmpfsStub(rt, false)
	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := rt.canonicalMountpoint(realMP)
	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	metaDir := t.TempDir()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, metaDir)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "symlink-mount-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	var gotServeMP string
	var gotMeta mountMeta
	var metaFileCanonExists bool
	var metaFileSymExists bool
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		gotServeMP = gotMP
		ready()
		gotMeta, _ = rt.findMountMeta(canonMP)
		_, errCanon := os.Stat(filepath.Join(metaDir, mountMetaFileName(canonMP)))
		metaFileCanonExists = errCanon == nil
		_, errSym := os.Stat(filepath.Join(metaDir, mountMetaFileName(symMP)))
		metaFileSymExists = errSym == nil
		return nil
	}

	code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, symMP, "-f", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=mount_symlink want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, canonMP, gotServeMP, "serveFUSE must receive canonical mountpoint")
	require.Equal(t, canonMP, gotMeta.Mountpoint, "mountMeta must store canonical mountpoint")
	require.True(t, metaFileCanonExists, "mountMeta file must use canonical mountpoint hash")
	require.False(t, metaFileSymExists, "mountMeta file must not use symlink mountpoint hash")
}

func TestMount_DaemonWorker_GivenRelativeSymlinkMountpoint_CanonicalizesBeforeChdir(t *testing.T) {
	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	metaDir := t.TempDir()
	setTempMountMetaDirs(rt, metaDir)
	setStageDirTmpfsStub(rt, false)

	workDir := t.TempDir()
	t.Chdir(workDir)
	callerDir, err := os.Getwd()
	require.NoError(t, err)
	realMP := filepath.Join(callerDir, "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	require.NoError(t, os.Symlink("real-mp", filepath.Join(callerDir, "sym-mp")))
	canonMP := rt.canonicalMountpoint(realMP)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "worker-sym-disk", 1<<30, 0)
	setMountStorageFake(rt, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	setReadinessPipeStub(rt, w)
	rt.redirectDaemonStdio = func(*os.File) {}

	var chdirTarget string
	rt.chdirFn = func(dir string) error {
		chdirTarget = dir
		return nil
	}

	var gotServeMP string
	rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		gotServeMP = gotMP
		ready()
		return nil
	}
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	code, out, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, "sym-mp", "--admin-addr", "off")

	require.Equal(t, 0, code, "scene=worker_symlink want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/", chdirTarget)
	require.Equal(t, canonMP, gotServeMP)
}

func TestMount_Validation_GivenSymlinkLoopMountpoint_Exits2(t *testing.T) {
	rt := defaultCLIRuntime()
	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	code, out, errOut := runCLIWithDeadline(t, rt, 2*time.Second, "mount", testDiskID1, loopA)

	require.Equal(t, 2, code, "scene=mountpoint_symlink_loop want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "Error:")
}

func TestMount_AlreadyMounted_GivenWedgedMountRoots_RejectsDuplicateAndBadFlagsWithoutMountRootLstat(t *testing.T) {
	rt := defaultCLIRuntime()
	const (
		realWedgedMP = "/wedged-root/mnt"
		realOkMP     = "/ok-root/mnt"
		symWedgedMP  = "/tmp/sym-wedged"
		symOkMP      = "/tmp/sym-ok"
	)
	setMountInfoContent(t, rt,
		"30 61 8:1 / / rw - ext4 /dev/sda1 rw\n"+
			"100 30 0:50 / "+realWedgedMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n"+
			"101 30 0:51 / "+realOkMP+" rw - fuse.bowl bowl:"+testDiskID2+" rw\n",
	)
	lstatCalls := setMountInfoNoRootLstatStub(t, rt, []string{realWedgedMP, realOkMP}, map[string]fakeFSNode{
		"/tmp":      {mode: os.ModeDir | 0o755},
		symWedgedMP: {mode: os.ModeSymlink | 0o777, target: realWedgedMP},
		symOkMP:     {mode: os.ModeSymlink | 0o777, target: realOkMP},
	})

	code, out, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, realWedgedMP, "-f")
	require.Equal(t, 1, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
	require.Contains(t, errOut, realWedgedMP)
	require.Empty(t, *lstatCalls)

	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: symWedgedMP, want: realWedgedMP},
		{input: symOkMP, want: realOkMP},
	} {
		code, out, errOut = runCLIWithRuntime(rt, "mount", testDiskID1, tc.input, "-f")
		require.Equal(t, 1, code, "input=%s stderr=%s", tc.input, errOut)
		require.Empty(t, out)
		require.Contains(t, errOut, "already mounted")
		require.Contains(t, errOut, tc.want)
	}

	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	for _, tc := range []struct {
		name       string
		args       []string
		wantSubstr string
	}{
		{
			name:       "StageDirInsideSymlinkWedgedMount",
			args:       []string{"mount", testDiskID1, symWedgedMP, "-f", "--stage-dir", filepath.Join(symWedgedMP, "sub")},
			wantSubstr: "mountpoint",
		},
		{
			name:       "LogFileInsideCanonicalWedgedMount",
			args:       []string{"mount", testDiskID1, symWedgedMP, "--log-file", filepath.Join(realWedgedMP, "daemon.log")},
			wantSubstr: "mountpoint",
		},
		{
			name:       "StageDirSymlinkLoop",
			args:       []string{"mount", testDiskID1, symWedgedMP, "-f", "--stage-dir", loopA},
			wantSubstr: "Error:",
		},
		{
			name:       "InvalidModeFlagCombo",
			args:       []string{"mount", testDiskID1, symWedgedMP, "-f", "--mode", "ro", "--stage-size", "8MiB"},
			wantSubstr: "--stage-size cannot be set",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut = runCLIWithRuntime(rt, tc.args...)
			require.Equal(t, 2, code, "scene=%s stderr=%s", tc.name, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr)
		})
	}

	require.NotContains(t, *lstatCalls, realWedgedMP)
	require.NotContains(t, *lstatCalls, realOkMP)
}

func TestMount_ForceFlag_GivenHelpAndValidation_PreservesShortFAndRejectsROMode(t *testing.T) {
	code, out, errOut := runCLI("mount", "--help")

	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Empty(t, errOut)
	require.Contains(t, out, "-f, --foreground")
	require.Contains(t, out, "--force")
	require.NotContains(t, out, "-f, --force")

	validMP := t.TempDir()
	cases := []struct {
		name       string
		envMode    string
		args       []string
		wantSubstr string
	}{
		{
			name:       "ForceWithExplicitROMode",
			args:       []string{"mount", testDiskID1, validMP, "--mode=ro", "--force"},
			wantSubstr: "--force cannot be set with --mode=ro",
		},
		{
			name:       "ForceWithEnvROMode",
			envMode:    "ro",
			args:       []string{"mount", testDiskID1, validMP, "--force"},
			wantSubstr: "--force cannot be set with --mode=ro",
		},
		{
			name:       "InvalidForceBool",
			args:       []string{"mount", testDiskID1, validMP, "--force=bad"},
			wantSubstr: "invalid argument",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := defaultCLIRuntime()
			if tc.envMode != "" {
				t.Setenv("BOWL_MOUNT_MODE", tc.envMode)
			}
			spawnCalled := false
			rt.spawnDaemon = func([]string, io.Writer) int {
				spawnCalled = true
				return 0
			}
			storage := newFakeStorage()
			storage.seedDisk(testDiskID1, "force-val-disk", 1<<30, 0)
			setMountStorageFake(rt, storage)

			gotCode, gotOut, gotErr := runCLIWithDeadline(t, rt, 2*time.Second, tc.args...)
			require.Equal(t, 2, gotCode, "scene=%s stderr=%s", tc.name, gotErr)
			require.Empty(t, gotOut)
			require.Contains(t, gotErr, tc.wantSubstr)
			require.False(t, spawnCalled, "scene=%s must not spawn daemon on exit 2", tc.name)
			require.Empty(t, storage.getMountReqs(), "scene=%s must not call MountDisk on exit 2", tc.name)
		})
	}

	withCleanOTelAndLogger(t)
	rt := defaultCLIRuntime()
	setMissingMountInfo(t, rt)
	setTempMountMetaDirs(rt, t.TempDir())
	roStorage := newFakeStorage()
	roStorage.seedDisk(testDiskID1, "ro-force-false-disk", 1<<30, 0)
	setMountStorageFake(rt, roStorage)
	stubMountServeFUSE(t, rt, validMP)

	roCode, _, roErr := runCLIWithRuntime(rt, "mount", testDiskID1, validMP, "-f", "--mode=ro", "--force=false", "--admin-addr", "off")
	require.Equal(t, 0, roCode, "stderr=%s", roErr)
	roReqs := roStorage.getMountReqs()
	require.Len(t, roReqs, 1)
	require.True(t, roReqs[0].GetReadOnly())
	require.False(t, roReqs[0].GetForce())
}

func TestMount_ForceHandshake_GivenForceFlagAndDaemonWorker_ForwardsForceAndFormatsErrDiskMounted(t *testing.T) {
	t.Run("ShortFRemainsForegroundWithoutForce", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		setTempMountMetaDirs(rt, t.TempDir())
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-short-f")

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "short-f-disk", 1<<30, 0)
		setMountStorageFake(rt, storage)
		stubMountServeFUSE(t, rt, mp)

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--stage-dir", stageDir, "--admin-addr", "off")
		require.Equal(t, 0, code, "stderr=%s", errOut)
		reqs := storage.getMountReqs()
		require.Len(t, reqs, 1)
		require.False(t, reqs[0].GetReadOnly())
		require.False(t, reqs[0].GetForce())
	})

	t.Run("ForegroundForceForwardsForceTrue", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		setTempMountMetaDirs(rt, t.TempDir())
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-fg-force")

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "fg-force-disk", 1<<30, 0)
		setMountStorageFake(rt, storage)
		stubMountServeFUSE(t, rt, mp)

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--force", "--stage-dir", stageDir, "--admin-addr", "off")
		require.Equal(t, 0, code, "stderr=%s", errOut)
		reqs := storage.getMountReqs()
		require.Len(t, reqs, 1)
		require.False(t, reqs[0].GetReadOnly())
		require.True(t, reqs[0].GetForce())
	})

	t.Run("BackgroundSpawnAndDaemonWorkerForwardForce", func(t *testing.T) {
		mp := t.TempDir()
		rtParent := defaultCLIRuntime()
		setMissingMountInfo(t, rtParent)

		var spawnedArgs []string
		rtParent.spawnDaemon = func(args []string, _ io.Writer) int {
			spawnedArgs = append([]string(nil), args...)
			return 0
		}
		code, _, errOut := runCLIWithRuntime(rtParent, "mount", testDiskID1, mp, "--force", "--mode=sync")
		require.Equal(t, 0, code, "stderr=%s", errOut)
		require.Equal(t, []string{testDiskID1, mp, "--force", "--mode=sync"}, spawnedArgs)

		withCleanOTelAndLogger(t)
		rtWorker := defaultCLIRuntime()
		setMissingMountInfo(t, rtWorker)
		setTempMountMetaDirs(rtWorker, t.TempDir())
		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "worker-force-disk", 1<<30, 0)
		setMountStorageFake(rtWorker, storage)
		stubMountServeFUSE(t, rtWorker, mp)

		r, w, err := os.Pipe()
		require.NoError(t, err)
		defer r.Close()
		setReadinessPipeStub(rtWorker, w)
		rtWorker.redirectDaemonStdio = func(*os.File) {}
		rtWorker.chdirFn = func(string) error { return nil }
		t.Setenv("BOWL_DAEMON_WORKER", "1")
		logFile := filepath.Join(t.TempDir(), "worker-force.log")

		workerCode, _, workerErr := runCLIWithRuntime(rtWorker, "mount", testDiskID1, mp, "--force", "--mode=sync", "--log-file", logFile, "--admin-addr", "off")
		require.Equal(t, 0, workerCode, "stderr=%s", workerErr)
		reqs := storage.getMountReqs()
		require.Len(t, reqs, 1)
		require.True(t, reqs[0].GetForce())
	})

	t.Run("ErrDiskMountedReturnsActionableExit1Message", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		setTempMountMetaDirs(rt, t.TempDir())
		mp := t.TempDir()
		stageDir := filepath.Join(t.TempDir(), "stage-busy")

		abortedBusyErr := connect.NewError(connect.CodeAborted, errors.New("disk has active read-write mount lease"))
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Domain: "bowl.storage",
			Reason: "EBUSY",
		})
		require.NoError(t, err)
		abortedBusyErr.AddDetail(detail)

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "busy-disk", 1<<30, 0)
		storage.forcedErr["MountBranch"] = abortedBusyErr
		setMountStorageFake(rt, storage)

		code, out, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--stage-dir", stageDir, "--admin-addr", "off")
		require.Equal(t, 1, code, "stderr=%s", errOut)
		require.Empty(t, out)
		require.Contains(t, errOut, "active read-write mount")
		require.Contains(t, errOut, "unmount")
		require.Contains(t, errOut, "--force")
		requireEmptyDir(t, stageDir, "err_disk_mounted_stage_cleanup")
	})
}

func TestMount_Teardown_GivenOverwrittenMountMeta_PreservesNewInstanceAndRemovesOwn(t *testing.T) {
	t.Run("PreservesNewInstanceWithDifferentPID", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		mp := t.TempDir()
		canonMP := rt.canonicalMountpoint(mp)
		metaFile := filepath.Join(metaDir, mountMetaFileName(canonMP))

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "mount-a-disk", 1<<30, 0)
		setMountStorageFake(rt, storage)

		mountB := mountMeta{
			Mountpoint: canonMP,
			PID:        os.Getpid() + 10000,
			DiskID:     testDiskID2,
			AdminAddr:  "127.0.0.1:29991",
			StartedAt:  time.Now().UTC().Add(time.Second),
		}
		rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
			ready()
			_, err := rt.writeMountMeta(&mountB)
			require.NoError(t, err)
			return nil
		}

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode=ro", "--admin-addr", "off")
		require.Equal(t, 0, code, "scene=teardown_diff_pid stderr=%s", errOut)

		raw, readErr := os.ReadFile(metaFile)
		require.NoError(t, readErr, "scene=teardown_diff_pid want=mountB_meta_preserved got=%v", readErr)
		var got mountMeta
		require.NoError(t, json.Unmarshal(raw, &got))
		require.Equal(t, mountB.PID, got.PID)
		require.Equal(t, mountB.DiskID, got.DiskID)
		require.Equal(t, mountB.AdminAddr, got.AdminAddr)
		require.True(t, mountB.StartedAt.Equal(got.StartedAt))
	})

	t.Run("PreservesNewInstanceWithSamePIDDifferentStartedAt", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		mp := t.TempDir()
		canonMP := rt.canonicalMountpoint(mp)
		metaFile := filepath.Join(metaDir, mountMetaFileName(canonMP))

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "mount-a-same-pid", 1<<30, 0)
		setMountStorageFake(rt, storage)

		mountB := mountMeta{
			Mountpoint: canonMP,
			PID:        os.Getpid(),
			DiskID:     testDiskID2,
			AdminAddr:  "127.0.0.1:29992",
			StartedAt:  time.Now().UTC().Add(time.Minute),
		}
		rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
			ready()
			_, err := rt.writeMountMeta(&mountB)
			require.NoError(t, err)
			return nil
		}

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode=ro", "--admin-addr", "off")
		require.Equal(t, 0, code, "scene=teardown_same_pid_new_started_at stderr=%s", errOut)

		raw, readErr := os.ReadFile(metaFile)
		require.NoError(t, readErr, "scene=teardown_same_pid_new_started_at want=mountB_meta_preserved got=%v", readErr)
		var got mountMeta
		require.NoError(t, json.Unmarshal(raw, &got))
		require.Equal(t, mountB.PID, got.PID)
		require.Equal(t, mountB.DiskID, got.DiskID)
		require.True(t, mountB.StartedAt.Equal(got.StartedAt))
	})

	t.Run("PreservesUnparseableFile", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		mp := t.TempDir()
		canonMP := rt.canonicalMountpoint(mp)
		metaFile := filepath.Join(metaDir, mountMetaFileName(canonMP))

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "mount-a-corrupt", 1<<30, 0)
		setMountStorageFake(rt, storage)

		rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
			ready()
			require.NoError(t, os.WriteFile(metaFile, []byte("{corrupt"), 0o644))
			return nil
		}

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode=ro", "--admin-addr", "off")
		require.Equal(t, 0, code, "scene=teardown_corrupt stderr=%s", errOut)

		raw, readErr := os.ReadFile(metaFile)
		require.NoError(t, readErr, "scene=teardown_corrupt want=file_not_deleted got=%v", readErr)
		require.Equal(t, "{corrupt", string(raw))
	})

	t.Run("RemovesOwnMountMetaWhenUnchanged", func(t *testing.T) {
		withCleanOTelAndLogger(t)
		rt := defaultCLIRuntime()
		setStageDirTmpfsStub(rt, false)
		setMissingMountInfo(t, rt)
		metaDir := t.TempDir()
		setTempMountMetaDirs(rt, metaDir)
		mp := t.TempDir()
		canonMP := rt.canonicalMountpoint(mp)
		metaFile := filepath.Join(metaDir, mountMetaFileName(canonMP))

		storage := newFakeStorage()
		storage.seedDisk(testDiskID1, "mount-a-own", 1<<30, 0)
		setMountStorageFake(rt, storage)

		rt.serveFUSE = func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
			ready()
			_, statErr := os.Stat(metaFile)
			require.NoError(t, statErr)
			return nil
		}

		code, _, errOut := runCLIWithRuntime(rt, "mount", testDiskID1, mp, "-f", "--mode=ro", "--admin-addr", "off")
		require.Equal(t, 0, code, "scene=teardown_own stderr=%s", errOut)

		_, statErr := os.Stat(metaFile)
		require.ErrorIs(t, statErr, os.ErrNotExist, "scene=teardown_own want=own_meta_removed got=%v", statErr)
	})
}

func TestMountAndDisk_GivenDefaultTransport_MountUsesShardedAndDiskUsesSingleConnection(t *testing.T) {
	withCleanOTelAndLogger(t)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "transport-check", 1<<30, 0)

	var (
		addrMu    sync.Mutex
		addrsByOp = make(map[string]map[string]struct{})
	)
	path, connectHandler := storagev1connect.NewStorageServiceHandler(storage)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrMu.Lock()
			if addrsByOp[r.URL.Path] == nil {
				addrsByOp[r.URL.Path] = make(map[string]struct{})
			}
			addrsByOp[r.URL.Path][r.RemoteAddr] = struct{}{}
			addrMu.Unlock()
			mux.ServeHTTP(w, r)
		}),
		Protocols:         &protocols,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	endpoint := "http://" + ln.Addr().String()

	rtMount := defaultCLIRuntime()
	setMissingMountInfo(t, rtMount)
	setTempMountMetaDirs(rtMount, t.TempDir())
	mp := t.TempDir()
	rtMount.serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		var out fuse.StatfsOut
		_ = rawFS.StatFs(nil, &fuse.InHeader{}, &out)
		return nil
	}

	code, _, errOut := runCLIWithRuntime(rtMount, "--endpoint", endpoint, "mount", testDiskID1, mp, "-f", "--mode=ro", "--admin-addr", "off")
	require.Equal(t, 0, code, "stderr=%s", errOut)

	addrMu.Lock()
	controlAddrs := addrsByOp[storagev1connect.StorageServiceMountBranchProcedure]
	metaAddrs := addrsByOp[storagev1connect.StorageServiceStatFSProcedure]
	addrMu.Unlock()
	require.Len(t, controlAddrs, 1)
	require.Len(t, metaAddrs, 1)
	for addr := range controlAddrs {
		_, overlap := metaAddrs[addr]
		require.False(t, overlap, "mount must use sharded transport with separate control and meta TCP connections")
	}

	rtDisk := defaultCLIRuntime()
	code, out, errOut := runCLIWithRuntime(rtDisk, "--endpoint", endpoint, "disk", "get", testDiskID1)
	require.Equal(t, 0, code, "stderr=%s", errOut)
	require.Contains(t, out, "transport-check")
}
