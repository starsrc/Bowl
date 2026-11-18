package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
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

func withServeFUSEStub(t *testing.T, fn func(context.Context, *fusefs.RawFS, *fusefs.Config, string, func()) error) {
	t.Helper()
	prev := serveFUSE
	serveFUSE = fn
	t.Cleanup(func() { serveFUSE = prev })
}

func withSpawnDaemonStub(t *testing.T, fn func([]string, io.Writer) int) {
	t.Helper()
	prev := spawnDaemon
	spawnDaemon = fn
	t.Cleanup(func() { spawnDaemon = prev })
}

func withStageDirTmpfsStub(t *testing.T, isTmpfs bool) {
	t.Helper()
	prev := stageDirIsTmpfs
	stageDirIsTmpfs = func(*fusefs.StageDirSession) bool { return isTmpfs }
	t.Cleanup(func() { stageDirIsTmpfs = prev })
}

func withCleanDefaultStageDir(t *testing.T) {
	t.Helper()
	_, err := os.Stat(fusefs.DefaultStageDir)
	if errors.Is(err, os.ErrNotExist) {
		t.Cleanup(func() { _ = os.Remove(fusefs.DefaultStageDir) })
	}
}

func withMountRenewIntervalStub(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := mountRenewIntervalFn
	mountRenewIntervalFn = func(uint32) time.Duration { return interval }
	t.Cleanup(func() { mountRenewIntervalFn = prev })
}

func withChdirStub(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := chdirFn
	chdirFn = fn
	t.Cleanup(func() { chdirFn = prev })
}

func withReadinessPipeStub(t *testing.T, w *os.File) {
	t.Helper()
	prev := readinessPipeFile
	readinessPipeFile = func() *os.File { return w }
	t.Cleanup(func() { readinessPipeFile = prev })
}

func withRedirectDaemonStdioStub(t *testing.T, fn func(*os.File)) {
	t.Helper()
	prev := redirectDaemonStdio
	redirectDaemonStdio = fn
	t.Cleanup(func() { redirectDaemonStdio = prev })
}

func stubMountServeFUSE(t *testing.T, wantMP string) *capturedMountServe {
	t.Helper()
	wantCanon := canonicalMountpoint(wantMP)
	res := &capturedMountServe{}
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, gotMP string, ready func()) error {
		res.cfg = *cfg
		require.Equal(t, wantCanon, gotMP, "scene=serveFUSE_mountpoint want=%s got=%s", wantCanon, gotMP)
		if cfg.StageSession != nil {
			_, statErr := os.Stat(cfg.StageSession.InstanceDir())
			res.instDirAlive = statErr == nil
		}
		ready()
		res.metaInReady, res.metaFoundNow = findMountMeta(wantCanon)
		return nil
	})
	return res
}

func runCLIWithDeadline(t *testing.T, deadline time.Duration, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	type cliResult struct {
		code   int
		stdout string
		stderr string
	}
	ch := make(chan cliResult, 1)
	go func() {
		c, out, errOut := runCLI(args...)
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
			// Given / When
			code, out, errOut := runCLI("mount", tc.flag)

			// Then
			require.Equal(t, 0, code, "scene=mount_help flag=%s want=0 got=%d stderr=%s", tc.flag, code, errOut)
			require.Empty(t, errOut)
			require.Contains(t, out, "bowl mount <disk-id> <mountpoint>")
			require.Contains(t, out, "staged, sync, or ro")
			require.Contains(t, out, "--stage-size")
			require.Contains(t, out, "--stage-dir")
			require.NotContains(t, out, "--ro")
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
			code, out, errOut := runCLIWithDeadline(t, 2*time.Second, tc.args...)
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
			t.Setenv(tc.envKey, tc.envVal)
			code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp)
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
			code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, tc.mountpoint, "--stage-dir", tc.stageDir)
			require.Equal(t, 2, code, "scene=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenRelativeDotInsideMountpoint_Exits2(t *testing.T) {
	// Given: current working directory is inside the mountpoint and --stage-dir is "."
	mp := t.TempDir()
	subDir := filepath.Join(mp, "nested")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	t.Chdir(subDir)

	// When
	code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", ".")

	// Then
	require.Equal(t, 2, code, "scene=relative_dot_in_mountpoint want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "mountpoint")
}

func TestMount_Validation_GivenStageDirSymlinkLoop_Exits2(t *testing.T) {
	// Given: --stage-dir points to a symlink loop
	mp := t.TempDir()
	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	// When
	code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", loopA)

	// Then
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
			// Given: spawnDaemon fails the test if validation lets invalid flags through
			spawnCalled := false
			withSpawnDaemonStub(t, func([]string, io.Writer) int {
				spawnCalled = true
				return 0
			})

			// When
			code, out, errOut := runCLIWithDeadline(t, 2*time.Second, tc.args...)

			// Then
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
			// Given: spawnDaemon must not run when --log-file validation fails
			spawnCalled := false
			withSpawnDaemonStub(t, func([]string, io.Writer) int {
				spawnCalled = true
				return 0
			})
			args := []string{"mount", testDiskID1, tc.mountpoint}
			args = append(args, tc.extraArgs...)
			args = append(args, "--log-file", tc.logFile)

			// When
			code, out, errOut := runCLIWithDeadline(t, 2*time.Second, args...)

			// Then
			require.False(t, spawnCalled, "scene=%s want=no_daemon_spawn", tc.name)
			require.Equal(t, 2, code, "scene=%s want=2 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
		})
	}
}

func TestMount_Validation_GivenRelativeLogFileInsideMountpoint_Exits2(t *testing.T) {
	// Given: caller working directory is a subdirectory inside the mountpoint
	mp := t.TempDir()
	subDir := filepath.Join(mp, "nested")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	t.Chdir(subDir)
	spawnCalled := false
	withSpawnDaemonStub(t, func([]string, io.Writer) int {
		spawnCalled = true
		return 0
	})

	// When
	code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp, "--log-file", "bowl.log")

	// Then
	require.False(t, spawnCalled)
	require.Equal(t, 2, code, "scene=relative_log_in_mountpoint want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "cannot equal or be inside mountpoint")
}

func TestMount_BackgroundDaemon_GivenValidLogFile_PassesRawArgsVerbatimAndDefersCreation(t *testing.T) {
	mp := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)
	withMissingMountInfo(t)

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
			var spawnedArgs []string
			withSpawnDaemonStub(t, func(args []string, _ io.Writer) int {
				spawnedArgs = append([]string(nil), args...)
				return 0
			})

			code, _, errOut := runCLI(tc.args...)
			require.Equal(t, 0, code, "scene=%s want=0 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, errOut)
			require.Equal(t, tc.wantArgs, spawnedArgs)
			require.NoDirExists(t, filepath.Join(workDir, "logs"), "scene=%s want=parent_never_creates_log_dir", tc.name)
		})
	}
}

func TestMount_AlreadyMounted_GivenActiveMountInfo_ReturnsExit1(t *testing.T) {
	// Given
	mp := canonicalMountpoint(t.TempDir())
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("mount", testDiskID1, mp, "-f")

	// Then
	require.Equal(t, 1, code, "scene=already_mounted want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
}

func TestMount_DefaultConfig_GivenNoModeOrStageFlags_UsesStaged512MiBAndDefaultStageDir(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withCleanDefaultStageDir(t)
	withStageDirTmpfsStub(t, false)
	t.Setenv("BOWL_MOUNT_MODE", "")
	t.Setenv("BOWL_STAGE_SIZE", "")
	t.Setenv("BOWL_STAGE_DIR", "")

	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "default-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=default_mount want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(536_870_912), captured.cfg.StageSize)
	require.Equal(t, "/var/tmp/bowl-stage", captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir())
}

func TestMount_WhitespaceEnvFallbacks_GivenBlankStageEnvs_UsesDefaults(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withCleanDefaultStageDir(t)
	withStageDirTmpfsStub(t, false)
	t.Setenv("BOWL_MOUNT_MODE", "   ")
	t.Setenv("BOWL_STAGE_SIZE", "   ")
	t.Setenv("BOWL_STAGE_DIR", "   ")

	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "blank-env-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=whitespace_env_defaults want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeStaged, captured.cfg.Mode)
	require.False(t, captured.cfg.IsReadOnly())
	require.Equal(t, uint64(536_870_912), captured.cfg.StageSize)
	require.Equal(t, "/var/tmp/bowl-stage", captured.cfg.StageDir)
	require.NotNil(t, captured.cfg.StageSession)
}

func TestMount_ForegroundStaged_GivenExplicitFlags_AppliesStageConfigAndCleansInstanceDir(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "custom-stage")
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "mount-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, out, errOut := runCLI(
		"mount", testDiskID1, mp,
		"-f", "--mode=staged", "--stage-size", "16MiB", "--stage-dir", stageDir,
		"--direct-io", "--allow-other", "--debug", "--stats-file",
		"--admin-addr", "off",
		"--log-level", "debug",
		"--log-format", "json",
		"--trace-sample-ratio", "0.5",
		"--token", "mount-tok",
	)

	// Then
	require.Equal(t, 0, code, "scene=foreground_staged want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Equal(t, "Bearer mount-tok", storage.lastAuth)
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
	_, metaAfterExit := findMountMeta(mp)
	require.False(t, metaAfterExit, "scene=after_exit want=mountMeta_removed got=true")
	require.Contains(t, errOut, `"msg":"mount_ready"`)
	require.Contains(t, errOut, `"msg":"unmount_complete"`)
	require.NotContains(t, errOut, "stage_dir_on_tmpfs")
}

func TestMount_ForegroundStaged_GivenMinStageSize4MiB_Succeeds(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "min-stage")
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "min-stage-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--stage-size", "4MiB", "--stage-dir", stageDir, "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=min_stage_size_4MiB want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, uint64(4_194_304), captured.cfg.StageSize)
	require.NoDirExists(t, captured.cfg.StageSession.InstanceDir())
}

func TestMount_ForegroundStaged_GivenEnvFallbacksAndTmpfsSeam_AppliesEnvAndLogsTmpfsWarning(t *testing.T) {
	// Given: 同前缀兄弟目录 mp-stage 与环境变量回退及 tmpfs 告警测试接缝
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, true)
	parent := t.TempDir()
	mp := filepath.Join(parent, "mp")
	require.NoError(t, os.MkdirAll(mp, 0o755))
	siblingStageDir := filepath.Join(parent, "mp-stage")
	t.Setenv("BOWL_MOUNT_MODE", "  StAgEd ")
	t.Setenv("BOWL_STAGE_SIZE", "32MiB")
	t.Setenv("BOWL_STAGE_DIR", siblingStageDir)

	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "staged-env-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	// Then
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
	// Given
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, true)
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-ro")
	t.Setenv("BOWL_ADMIN_ADDR", "off")
	t.Setenv("BOWL_LOG_LEVEL", "warn")
	t.Setenv("BOWL_LOG_FORMAT", "text")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0")
	t.Setenv("BOWL_MOUNT_MODE", "staged")
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)

	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "ro-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--mode", "ro")

	// Then
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
	// Given
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, true)
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-env-ro")
	t.Setenv("BOWL_MOUNT_MODE", " Ro ")
	t.Setenv("BOWL_STAGE_SIZE", "invalid-ignored")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)

	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "ro-env-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, mp)

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=env_ro_mode want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, fusefs.MountModeRO, captured.cfg.Mode)
	require.True(t, captured.cfg.IsReadOnly())
	require.Nil(t, captured.cfg.StageSession)
	require.NoDirExists(t, unusedStageDir)
}

func TestMount_ForegroundSync_GivenCLIFlagAndAmbientStageEnv_IgnoresStageEnvSkipsSessionAndMountsWritable(t *testing.T) {
	// Given ambient BOWL_STAGE_SIZE and nested BOWL_STAGE_DIR values that fail in staged mode.
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, true)
	withMountRenewIntervalStub(t, 15*time.Millisecond)
	mp := t.TempDir()
	nestedStageDir := filepath.Join(mp, "ambient-stage-inside-mp")
	t.Setenv("BOWL_MOUNT_MODE", "ro")
	t.Setenv("BOWL_STAGE_SIZE", "1MiB")
	t.Setenv("BOWL_STAGE_DIR", nestedStageDir)

	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "sync-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	withMountStorageFake(t, storage)

	var captured fusefs.Config
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, cfg *fusefs.Config, gotMP string, ready func()) error {
		captured = *cfg
		require.Equal(t, canonicalMountpoint(mp), gotMP)
		ready()
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 1
		}, time.Second, 5*time.Millisecond, "scene=sync_renew want>=1_renew_req")
		return nil
	})

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--mode", "sync", "--admin-addr", "off")

	// Then
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
	// Given BOWL_MOUNT_MODE set to mixed-case whitespace-padded sync.
	t.Setenv("BOWL_MOUNT_MODE", "  SyNc  ")
	t.Setenv("BOWL_STAGE_SIZE", "bogus-ignored")
	unusedStageDir := filepath.Join(t.TempDir(), "should-not-exist-env-sync")
	t.Setenv("BOWL_STAGE_DIR", unusedStageDir)
	mp := t.TempDir()

	// Explicit --stage-size or --stage-dir with BOWL_MOUNT_MODE=sync exits with code 2.
	codeSize, _, errSize := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp, "--stage-size", "8MiB")
	require.Equal(t, 2, codeSize, "scene=env_sync_with_stage_size want=2 got=%d stderr=%s", codeSize, errSize)
	require.Contains(t, errSize, "--stage-size cannot be set when --mode is sync")

	codeDir, _, errDir := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, mp, "--stage-dir", "/tmp/x")
	require.Equal(t, 2, codeDir, "scene=env_sync_with_stage_dir want=2 got=%d stderr=%s", codeDir, errDir)
	require.Contains(t, errDir, "--stage-dir cannot be set when --mode is sync")

	// Server returning Epoch=0 under sync mode fails handshake with exit code 1.
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	faultyStorage := newFakeStorage()
	seedFaultyMountStorage(false, true)(faultyStorage)
	withMountStorageFake(t, faultyStorage)

	codeZeroEpoch, _, errZeroEpoch := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
	require.Equal(t, 1, codeZeroEpoch, "scene=env_sync_zero_epoch want=1 got=%d stderr=%s", codeZeroEpoch, errZeroEpoch)
	require.Contains(t, errZeroEpoch, "valid write mount epoch")
	require.NoDirExists(t, unusedStageDir)

	// Valid server Epoch succeeds via BOWL_MOUNT_MODE fallback without opening a stage session.
	okStorage := newFakeStorage()
	okStorage.seedDisk(testDiskID1, "sync-env-disk", 1<<30, 0)
	withMountStorageFake(t, okStorage)
	captured := stubMountServeFUSE(t, mp)

	codeOK, _, errOK := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
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
		{name: "DiskNotFound", setup: noOpStorageSetup, wantSubstr: "disk not found: " + testDiskID1},
		{name: "EmptyRootInode", setup: seedFaultyMountStorage(true, false), wantSubstr: "invalid root directory metadata"},
		{name: "RWZeroEpoch", setup: seedFaultyMountStorage(false, true), wantSubstr: "valid write mount epoch"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withCleanOTelAndLogger(t)
			mp := t.TempDir()
			stageDir := filepath.Join(t.TempDir(), "handshake-stage")
			t.Setenv("BOWL_STAGE_DIR", stageDir)
			withMissingMountInfo(t)
			withTempMountMetaDirs(t, t.TempDir())

			storage := newFakeStorage()
			tc.setup(storage)
			withMountStorageFake(t, storage)

			code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")
			require.Equal(t, 1, code, "scene=%s want=1 got=%d stderr=%s", tc.name, code, errOut)
			require.Contains(t, errOut, tc.wantSubstr, "scene=%s", tc.name)
			requireEmptyDir(t, stageDir, "handshake_cleanup_"+tc.name)
		})
	}
}

func TestMount_ServeError_GivenFUSEFailure_ReturnsExit1AndCleansMetaAndStageSession(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "serve-err-stage")
	t.Setenv("BOWL_STAGE_DIR", stageDir)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	withMountStorageFake(t, storage)
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		return errors.New("fuse device failed")
	})

	// When
	code, _, errOut := runCLI("mount", testDiskID1, mp, "-f", "--admin-addr", "off")

	// Then
	require.Equal(t, 1, code, "scene=serve_error want=1 got=%d stderr=%s", code, errOut)
	require.Contains(t, errOut, "fuse device failed")
	_, remains := findMountMeta(mp)
	require.False(t, remains, "scene=serve_error want=mountMeta_removed got=true")
	requireEmptyDir(t, stageDir, "serve_error_stage_cleanup")
}

func TestRunMount_StageSessionError_GivenUncreatableStageDir_FailsBeforeMountHandshake(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "d1", 1<<30, 0)
	withMountStorageFake(t, storage)

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

	// When
	var logBuf bytes.Buffer
	err := runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)

	// Then
	require.Error(t, err)
	require.Contains(t, err.Error(), "initialize stage directory")
	requireStorageEpochZero(t, storage, "stage_session_error_before_handshake")
}

func TestMount_BackgroundDaemon_GivenROAndStagedFlags_SpawnsWithoutParentStageSession(t *testing.T) {
	mp := t.TempDir()
	deferredStageDir := filepath.Join(t.TempDir(), "daemon-only-stage")
	withMissingMountInfo(t)

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
			var spawnedArgs []string
			withSpawnDaemonStub(t, func(args []string, _ io.Writer) int {
				spawnedArgs = append([]string(nil), args...)
				return 0
			})

			code, _, errOut := runCLI(tc.args...)
			require.Equal(t, 0, code, "scene=%s want=0 got=%d stderr=%s", tc.name, code, errOut)
			require.Empty(t, errOut)
			require.Equal(t, tc.wantArgs, spawnedArgs)
			require.NoDirExists(t, deferredStageDir, "scene=%s want=parent_never_opens_stage_dir", tc.name)
		})
	}
}

func TestMount_GivenBoolFalseAndRepeatedInvocations_ResetsPerCallState(t *testing.T) {
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "isolation-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	captured := stubMountServeFUSE(t, mp)
	code, _, errOut := runCLI(
		"mount", testDiskID1, mp,
		"-f", "--mode=ro", "--allow-other", "--direct-io", "--debug", "--stats-file", "--admin-addr", "off",
	)
	require.Equal(t, 0, code, "first invocation stderr=%s", errOut)
	require.Equal(t, fusefs.MountModeRO, captured.cfg.Mode)
	require.True(t, captured.cfg.AllowOther)
	require.True(t, captured.cfg.DirectIO)
	require.True(t, captured.cfg.Debug)
	require.True(t, captured.cfg.EnableStatsFile)

	captured2 := stubMountServeFUSE(t, mp)
	code, _, errOut = runCLI(
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
	// Given
	explicitPath := filepath.Join(t.TempDir(), "nested", "logs", "custom.log")

	// When
	f, gotPath, err := ResolveLogFile(explicitPath, testDiskID1)

	// Then
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Equal(t, explicitPath, gotPath)
}

func TestResolveLogFile_GivenSystemDirEACCES_FallsBackToXDGStateHome(t *testing.T) {
	// Given
	prevOpenSys := openSystemLogFn
	openSystemLogFn = func(string) (*os.File, string, error) {
		return nil, "/var/log/bowl/mount.log", syscall.EACCES
	}
	t.Cleanup(func() { openSystemLogFn = prevOpenSys })
	xdgDir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", xdgDir)

	// When
	f, gotPath, err := ResolveLogFile("", testDiskID1)

	// Then
	require.NoError(t, err)
	require.NoError(t, f.Close())
	wantFallback := filepath.Join(xdgDir, "bowl", "logs", "mount-"+testDiskID1+".log")
	require.Equal(t, wantFallback, gotPath)
}

func TestRunMount_LeaseLifecycle_GivenCanceledCtx_RenewsAndUnmountsBeforeRemovingMeta(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	mp := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withMountRenewIntervalStub(t, 15*time.Millisecond)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
	storage.mountLeaseTTL = 30
	withMountStorageFake(t, storage)

	var metaExistsAtUnmount bool
	storage.onUnmountDisk = func(_ *storagev1.UnmountDiskRequest) {
		_, metaExistsAtUnmount = findMountMeta(mp)
	}

	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	withServeFUSEStub(t, func(ctx context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
		ready()
		require.Eventually(t, func() bool {
			return len(storage.getRenewReqs()) >= 2
		}, time.Second, 5*time.Millisecond, "serveFUSE 阻塞期间必须周期发送 RenewMount")
		cancelParent()
		<-ctx.Done()
		return nil
	})

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

	// When
	var logBuf bytes.Buffer
	err := runMount(parentCtx, opts, baseCfg, mp, &logBuf, nil)

	// Then
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
	_, metaAfter := findMountMeta(mp)
	require.False(t, metaAfter, "UnmountDisk 完成后必须移除 mountMeta 文件")
}

func TestRunMount_LeaseRenewTerminalError_GivenEBUSYOrNotFound_StopsRenewLoop(t *testing.T) {
	ebusyErr := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale epoch"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: "bowl.storage", Reason: "EBUSY"})
	require.NoError(t, err)
	ebusyErr.AddDetail(detail)

	cases := []struct {
		name     string
		renewErr error
	}{
		{name: "CodeFailedPrecondition_EBUSY", renewErr: ebusyErr},
		{name: "CodeNotFound", renewErr: connect.NewError(connect.CodeNotFound, errors.New("mount not found"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withCleanOTelAndLogger(t)
			mp := t.TempDir()
			withMissingMountInfo(t)
			withTempMountMetaDirs(t, t.TempDir())
			withMountRenewIntervalStub(t, 10*time.Millisecond)

			storage := newFakeStorage()
			storage.seedDisk(testDiskID1, "lease-disk", 1<<30, 0)
			storage.renewErr = tc.renewErr
			withMountStorageFake(t, storage)

			var logBuf syncBuffer
			withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, ready func()) error {
				ready()
				require.Eventually(t, func() bool {
					return strings.Contains(logBuf.String(), "mount_lease_renew_stopped")
				}, time.Second, 5*time.Millisecond, "scene=%s want=mount_lease_renew_stopped", tc.name)
				return nil
			})

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
				StageDir:  t.TempDir(),
			}

			runErr := runMount(context.Background(), opts, baseCfg, mp, &logBuf, nil)
			require.NoError(t, runErr)
			require.Len(t, storage.getRenewReqs(), 1, "scene=%s want=1_renew_req got=%d", tc.name, len(storage.getRenewReqs()))
			require.Len(t, storage.getUnmountReqs(), 1)
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
	mp := canonicalMountpoint(t.TempDir())
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
	// Given
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withStageDirTmpfsStub(t, false)

	mp := t.TempDir()
	stageDir := filepath.Join(t.TempDir(), "daemon-stage")
	logPath := filepath.Join(t.TempDir(), "daemon.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "daemon-worker-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	withReadinessPipeStub(t, w)

	var redirectedFile *os.File
	withRedirectDaemonStdioStub(t, func(f *os.File) {
		redirectedFile = f
	})

	var events []string
	withChdirStub(t, func(dir string) error {
		_, statErr := os.Stat(logPath)
		require.NoError(t, statErr, "log file must be opened before chdir")
		events = append(events, "chdir:"+dir)
		return nil
	})
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		require.Equal(t, mp, gotMP)
		events = append(events, "serve")
		ready()
		return nil
	})

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

	// When
	code := runDaemonWorker(context.Background(), opts, baseCfg, mp)

	// Then
	require.Equal(t, 0, code)
	require.Equal(t, []string{"chdir:/", "serve"}, events)
	require.NotNil(t, redirectedFile)
	pipeBytes, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	require.Equal(t, "OK\n", string(pipeBytes))
}

func TestRunDaemonWorker_GivenChdirError_ReportsERRAndExits1(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	mp := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "daemon-err.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "daemon-err-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	withReadinessPipeStub(t, w)
	withRedirectDaemonStdioStub(t, func(*os.File) {})

	serveCalled := false
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, _ string, _ func()) error {
		serveCalled = true
		return nil
	})
	withChdirStub(t, func(dir string) error {
		require.Equal(t, "/", dir)
		return errors.New("chdir denied")
	})

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

	// When
	code := runDaemonWorker(context.Background(), opts, baseCfg, mp)

	// Then
	require.Equal(t, 1, code)
	require.False(t, serveCalled, "runMount must not execute when chdirFn fails")
	pipeBytes, readErr := io.ReadAll(r)
	require.NoError(t, readErr)
	require.Equal(t, "ERR:change daemon working directory to /: chdir denied\n", string(pipeBytes))
}

func TestMount_DaemonWorker_GivenRelativePaths_ResolvesBeforeChdirToRoot(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withStageDirTmpfsStub(t, false)

	t.Chdir(t.TempDir())
	callerDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(callerDir, "rel-mp"), 0o755))
	absMP := filepath.Join(callerDir, "rel-mp")
	absStageDir := filepath.Join(callerDir, "rel-stage")
	absLogFile := filepath.Join(callerDir, "rel-logs", "worker.log")

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "worker-rel-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	withReadinessPipeStub(t, w)
	withRedirectDaemonStdioStub(t, func(*os.File) {})

	var chdirArgs []string
	withChdirStub(t, func(dir string) error {
		_, statErr := os.Stat(absLogFile)
		require.NoError(t, statErr, "relative --log-file must open against caller cwd before chdir")
		chdirArgs = append(chdirArgs, dir)
		return nil
	})
	captured := stubMountServeFUSE(t, absMP)
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	// When
	code, out, errOut := runCLI(
		"mount", testDiskID1, "rel-mp",
		"--stage-dir", "rel-stage",
		"--log-file", filepath.Join("rel-logs", "worker.log"),
		"--admin-addr", "off",
	)

	// Then
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
	// Given
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, t.TempDir())
	withStageDirTmpfsStub(t, false)

	t.Chdir(t.TempDir())
	cwdBefore, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(cwdBefore, "fg-mp"), 0o755))

	chdirCalled := false
	withChdirStub(t, func(string) error {
		chdirCalled = true
		return nil
	})

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "fg-cwd-disk", 1<<30, 0)
	withMountStorageFake(t, storage)
	captured := stubMountServeFUSE(t, filepath.Join(cwdBefore, "fg-mp"))

	// When
	code, _, errOut := runCLI("mount", testDiskID1, "fg-mp", "-f", "--stage-dir", "fg-stage", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=foreground_preserves_cwd want=0 got=%d stderr=%s", code, errOut)
	require.False(t, chdirCalled, "foreground mode must not call chdirFn")
	cwdAfter, err := os.Getwd()
	require.NoError(t, err)
	require.Equal(t, cwdBefore, cwdAfter)
	require.Equal(t, filepath.Join(cwdBefore, "fg-stage"), captured.cfg.StageDir)
}

func TestMount_AlreadyMounted_GivenSymlinkToActiveMount_ReturnsExit1(t *testing.T) {
	// Given
	realParent := t.TempDir()
	realMP := filepath.Join(realParent, "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := canonicalMountpoint(realMP)

	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	symParent := filepath.Join(t.TempDir(), "sym-parent")
	require.NoError(t, os.Symlink(realParent, symParent))
	viaSymParent := filepath.Join(symParent, "real-mp")

	withMountInfoContent(t, "100 30 0:50 / "+canonMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When / Then: leaf symlink rejected as already mounted
	code, out, errOut := runCLI("mount", testDiskID1, symMP, "-f")
	require.Equal(t, 1, code, "scene=dup_mount_leaf_symlink want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")

	// When / Then: symlinked parent rejected as already mounted
	code, out, errOut = runCLI("mount", testDiskID1, viaSymParent, "-f")
	require.Equal(t, 1, code, "scene=dup_mount_parent_symlink want=1 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
}

func TestMount_Foreground_GivenSymlinkMountpoint_CanonicalizesServeFUSEAndMountMeta(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withStageDirTmpfsStub(t, false)
	realMP := filepath.Join(t.TempDir(), "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	canonMP := canonicalMountpoint(realMP)
	symMP := filepath.Join(t.TempDir(), "sym-mp")
	require.NoError(t, os.Symlink(realMP, symMP))

	metaDir := t.TempDir()
	withMissingMountInfo(t)
	withTempMountMetaDirs(t, metaDir)
	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "symlink-mount-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	var gotServeMP string
	var gotMeta mountMeta
	var metaFileCanonExists bool
	var metaFileSymExists bool
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		gotServeMP = gotMP
		ready()
		gotMeta, _ = findMountMeta(canonMP)
		_, errCanon := os.Stat(filepath.Join(metaDir, mountMetaFileName(canonMP)))
		metaFileCanonExists = errCanon == nil
		_, errSym := os.Stat(filepath.Join(metaDir, mountMetaFileName(symMP)))
		metaFileSymExists = errSym == nil
		return nil
	})

	// When
	code, _, errOut := runCLI("mount", testDiskID1, symMP, "-f", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=mount_symlink want=0 got=%d stderr=%s", code, errOut)
	require.Equal(t, canonMP, gotServeMP, "serveFUSE must receive canonical mountpoint")
	require.Equal(t, canonMP, gotMeta.Mountpoint, "mountMeta must store canonical mountpoint")
	require.True(t, metaFileCanonExists, "mountMeta file must use canonical mountpoint hash")
	require.False(t, metaFileSymExists, "mountMeta file must not use symlink mountpoint hash")
}

func TestMount_DaemonWorker_GivenRelativeSymlinkMountpoint_CanonicalizesBeforeChdir(t *testing.T) {
	// Given
	withCleanOTelAndLogger(t)
	withMissingMountInfo(t)
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withStageDirTmpfsStub(t, false)

	workDir := t.TempDir()
	t.Chdir(workDir)
	callerDir, err := os.Getwd()
	require.NoError(t, err)
	realMP := filepath.Join(callerDir, "real-mp")
	require.NoError(t, os.MkdirAll(realMP, 0o755))
	require.NoError(t, os.Symlink("real-mp", filepath.Join(callerDir, "sym-mp")))
	canonMP := canonicalMountpoint(realMP)

	storage := newFakeStorage()
	storage.seedDisk(testDiskID1, "worker-sym-disk", 1<<30, 0)
	withMountStorageFake(t, storage)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	withReadinessPipeStub(t, w)
	withRedirectDaemonStdioStub(t, func(*os.File) {})

	var chdirTarget string
	withChdirStub(t, func(dir string) error {
		chdirTarget = dir
		return nil
	})

	var gotServeMP string
	withServeFUSEStub(t, func(_ context.Context, _ *fusefs.RawFS, _ *fusefs.Config, gotMP string, ready func()) error {
		gotServeMP = gotMP
		ready()
		return nil
	})
	t.Setenv("BOWL_DAEMON_WORKER", "1")

	// When
	code, out, errOut := runCLI("mount", testDiskID1, "sym-mp", "--admin-addr", "off")

	// Then
	require.Equal(t, 0, code, "scene=worker_symlink want=0 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/", chdirTarget)
	require.Equal(t, canonMP, gotServeMP)
}

func TestMount_Validation_GivenSymlinkLoopMountpoint_Exits2(t *testing.T) {
	// Given
	loopDir := t.TempDir()
	loopA := filepath.Join(loopDir, "loop-a")
	loopB := filepath.Join(loopDir, "loop-b")
	require.NoError(t, os.Symlink(loopB, loopA))
	require.NoError(t, os.Symlink(loopA, loopB))

	// When
	code, out, errOut := runCLIWithDeadline(t, 2*time.Second, "mount", testDiskID1, loopA)

	// Then
	require.Equal(t, 2, code, "scene=mountpoint_symlink_loop want=2 got=%d stderr=%s", code, errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "Error:")
}

func TestMount_AlreadyMounted_GivenWedgedMountRoots_RejectsDuplicateAndBadFlagsWithoutMountRootLstat(t *testing.T) {
	// Given: synthetic mount roots in mountinfo that do not exist on host disk, so os.Stat or EvalSymlinks would fail with exit 2
	const (
		realWedgedMP = "/wedged-root/mnt"
		realOkMP     = "/ok-root/mnt"
		symWedgedMP  = "/tmp/sym-wedged"
		symOkMP      = "/tmp/sym-ok"
	)
	withMountInfoContent(t,
		"30 61 8:1 / / rw - ext4 /dev/sda1 rw\n"+
			"100 30 0:50 / "+realWedgedMP+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n"+
			"101 30 0:51 / "+realOkMP+" rw - fuse.bowl bowl:"+testDiskID2+" rw\n",
	)
	lstatCalls := withMountInfoNoRootLstatStub(t, []string{realWedgedMP, realOkMP}, map[string]fakeFSNode{
		"/tmp":      {mode: os.ModeDir | 0o755},
		symWedgedMP: {mode: os.ModeSymlink | 0o777, target: realWedgedMP},
		symOkMP:     {mode: os.ModeSymlink | 0o777, target: realOkMP},
	})

	// When / Then: direct canonical wedged path returns exit 1 with zero Lstat calls
	code, out, errOut := runCLI("mount", testDiskID1, realWedgedMP, "-f")
	require.Equal(t, 1, code, "stderr=%s", errOut)
	require.Empty(t, out)
	require.Contains(t, errOut, "already mounted")
	require.Contains(t, errOut, realWedgedMP)
	require.Empty(t, *lstatCalls)

	// When / Then: symlinked wedged and ok paths return exit 1 without os.Stat, EvalSymlinks, or Lstat on active mount roots
	for _, tc := range []struct {
		input string
		want  string
	}{
		{input: symWedgedMP, want: realWedgedMP},
		{input: symOkMP, want: realOkMP},
	} {
		code, out, errOut = runCLI("mount", testDiskID1, tc.input, "-f")
		require.Equal(t, 1, code, "input=%s stderr=%s", tc.input, errOut)
		require.Empty(t, out)
		require.Contains(t, errOut, "already mounted")
		require.Contains(t, errOut, tc.want)
	}

	// When / Then: bad flags on an already-mounted wedged mountpoint still exit 2 without touching active mount roots
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
			code, out, errOut = runCLI(tc.args...)
			require.Equal(t, 2, code, "scene=%s stderr=%s", tc.name, errOut)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.wantSubstr)
		})
	}

	require.NotContains(t, *lstatCalls, realWedgedMP)
	require.NotContains(t, *lstatCalls, realOkMP)
}
