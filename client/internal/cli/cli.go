package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/version"
)

const EnvDiskID = "BOWL_DISK"

type cliRuntime struct {
	diskHTTPClient *http.Client
	diskStdin      io.Reader

	serveFUSE                 func(context.Context, *fusefs.RawFS, *fusefs.Config, string, func()) error
	newFUSEServer             func(*fusefs.RawFS, string, *fuse.MountOptions) (fuseMountServer, error)
	mountSignalContext        func(context.Context) (context.Context, context.CancelFunc)
	mountHTTPClient           *http.Client
	mountRenewIntervalFn      func(uint32) time.Duration
	mountRenewRetryIntervalFn func(time.Duration) time.Duration
	mountNowFn                func() time.Time
	stageDirIsTmpfs           func(*fusefs.StageDirSession) bool

	spawnDaemon            func([]string, io.Writer) int
	chdirFn                func(string) error
	readinessPipeFile      func() *os.File
	redirectDaemonStdio    func(*os.File)
	daemonHandshakeTimeout time.Duration
	daemonKillGrace        time.Duration
	daemonSignalNotify     func() (<-chan os.Signal, func())
	daemonCommandFn        func([]string, *os.File) (*exec.Cmd, error)

	lookPath                   func(string) (string, error)
	runCommand                 func(string, ...string) ([]byte, error)
	umountIsTerminal           func(io.Writer) bool
	umountHTTPClient           *http.Client
	umountDrainTimeout         time.Duration
	umountDrainExitGrace       time.Duration
	umountDrainPollInterval    time.Duration
	umountProgressPollInterval time.Duration
	umountNonTTYLogInterval    time.Duration
	umountFallbackGracePeriod  time.Duration
	umountFallbackLogInterval  time.Duration

	mountInfoPath     string
	mountInfoLstat    func(string) (os.FileInfo, error)
	mountInfoReadlink func(string) (string, error)

	readProcStatFn func(int) (byte, uint64, error)
	mountMetaDirs  func() []string

	statsHTTPClient *http.Client

	openSystemLogFn func(string) (*os.File, string, error)
}

func defaultCLIRuntime() *cliRuntime {
	rt := &cliRuntime{
		diskStdin: os.Stdin,

		newFUSEServer:             defaultNewFUSEServer,
		mountSignalContext:        defaultMountSignalContext,
		mountRenewIntervalFn:      defaultMountRenewInterval,
		mountRenewRetryIntervalFn: defaultMountRenewRetryInterval,
		mountNowFn:                time.Now,
		stageDirIsTmpfs:           func(s *fusefs.StageDirSession) bool { return s.IsTmpfs() },

		chdirFn:                os.Chdir,
		readinessPipeFile:      func() *os.File { return os.NewFile(3, "readiness-pipe") },
		redirectDaemonStdio:    defaultRedirectDaemonStdio,
		daemonHandshakeTimeout: 30 * time.Second,
		daemonKillGrace:        5 * time.Second,
		daemonSignalNotify:     defaultDaemonSignalNotify,
		daemonCommandFn:        defaultDaemonCommand,

		lookPath: exec.LookPath,
		runCommand: func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).CombinedOutput()
		},
		umountIsTerminal: func(w io.Writer) bool {
			f, ok := w.(*os.File)
			if !ok || f == nil {
				return false
			}
			return term.IsTerminal(int(f.Fd()))
		},
		umountHTTPClient:           http.DefaultClient,
		umountDrainTimeout:         5 * time.Minute,
		umountDrainExitGrace:       5 * time.Second,
		umountDrainPollInterval:    20 * time.Millisecond,
		umountProgressPollInterval: 100 * time.Millisecond,
		umountNonTTYLogInterval:    2 * time.Second,
		umountFallbackGracePeriod:  1 * time.Second,
		umountFallbackLogInterval:  5 * time.Second,

		mountInfoPath:     "/proc/self/mountinfo",
		mountInfoLstat:    os.Lstat,
		mountInfoReadlink: os.Readlink,

		readProcStatFn: defaultReadProcStat,
		mountMetaDirs: func() []string {
			return []string{primaryMountMetaDir, fallbackMountMetaDir()}
		},

		statsHTTPClient: http.DefaultClient,

		openSystemLogFn: openSystemLog,
	}
	rt.serveFUSE = rt.defaultServeFUSE
	rt.spawnDaemon = rt.spawnDaemonProcess
	return rt
}

type rootOptions struct {
	endpoint string
	jsonOut  bool
	diskID   string
}

type cliExitError struct {
	code      int
	err       error
	silent    bool
	showUsage bool
}

func (e *cliExitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return ""
}

func (e *cliExitError) Unwrap() error {
	return e.err
}

func usageExitErr(err error, showUsage bool) error {
	return &cliExitError{code: 2, err: err, showUsage: showUsage}
}

func runtimeExitErr(err error) error {
	return &cliExitError{code: 1, err: err}
}

func resolveDiskScope(flagVal string) (string, error) {
	raw := strings.TrimSpace(flagVal)
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(EnvDiskID))
	}
	if raw == "" {
		return "", fmt.Errorf("disk ID is required: pass -d/--disk or set %s", EnvDiskID)
	}
	return parseNonNilUUID(raw, "disk-id")
}

func extractMountDaemonArgs(args []string) []string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if arg == "--endpoint" || arg == "-d" || arg == "--disk" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		if arg == "mount" {
			out := make([]string, 0, len(args)-1)
			out = append(out, args[:i]...)
			out = append(out, args[i+1:]...)
			return out
		}
		return nil
	}
	return nil
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print client version information",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "bowl version %s\n", version.Version)
		},
	}
}

func newRootCmd(rt *cliRuntime, args []string, stdout, stderr io.Writer) *cobra.Command {
	rootOpts := &rootOptions{}

	rootCmd := &cobra.Command{
		Use:           "bowl",
		Short:         "Distributed virtual disk Linux FUSE client",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args: func(cmd *cobra.Command, posArgs []string) error {
			if len(posArgs) > 0 {
				return fmt.Errorf("unknown command %q for %q", posArgs[0], cmd.CommandPath())
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprint(cmd.ErrOrStderr(), cmd.UsageString())
			return &cliExitError{code: 2, silent: true}
		},
	}

	pflags := rootCmd.PersistentFlags()
	pflags.StringVar(&rootOpts.endpoint, "endpoint", "", "StorageService base URL, defaults to $BOWL_STORAGE_ENDPOINT or http://127.0.0.1:9000")
	pflags.BoolVar(&rootOpts.jsonOut, "json", false, "Output result in JSON format")
	pflags.StringVarP(&rootOpts.diskID, "disk", "d", "", "Target virtual disk UUID, defaults to $BOWL_DISK")

	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	if args == nil {
		args = []string{}
	}
	rootCmd.SetArgs(args)

	mountArgs := extractMountDaemonArgs(args)

	rootCmd.AddCommand(
		newDiskCmd(rt, rootOpts),
		newBranchCmd(rt, rootOpts),
		newCommitCmd(rt, rootOpts),
		newLeaseCmd(rt, rootOpts),
		newDiffCmd(rt, rootOpts),
		newMountCmd(rt, mountArgs, rootOpts),
		newUmountCmd(rt),
		newStatsCmd(rt, rootOpts),
		newVersionCmd(),
	)

	return rootCmd
}

func Execute(args []string, stdout, stderr io.Writer) int {
	return executeWithRuntime(defaultCLIRuntime(), args, stdout, stderr)
}

func executeWithRuntime(rt *cliRuntime, args []string, stdout, stderr io.Writer) int {
	if rt == nil {
		rt = defaultCLIRuntime()
	}
	rootCmd := newRootCmd(rt, args, stdout, stderr)
	executedCmd, err := rootCmd.ExecuteC()
	if err == nil {
		return 0
	}

	if exitErr, ok := errors.AsType[*cliExitError](err); ok {
		if !exitErr.silent && exitErr.err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", exitErr.err)
		}
		if exitErr.showUsage && executedCmd != nil {
			fmt.Fprintf(stderr, "\n%s", executedCmd.UsageString())
		}
		return exitErr.code
	}

	fmt.Fprintf(stderr, "Error: %v\n", err)
	if executedCmd != nil {
		fmt.Fprintf(stderr, "\n%s", executedCmd.UsageString())
	}
	return 2
}
