package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/version"
)

const EnvDiskID = "BOWL_DISK"

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

func newRootCmd(args []string, stdout, stderr io.Writer) *cobra.Command {
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

	ports := defaultDiskPorts()
	mountArgs := extractMountDaemonArgs(args)

	rootCmd.AddCommand(
		newDiskCmd(ports, rootOpts),
		newBranchCmd(ports, rootOpts),
		newCommitCmd(ports, rootOpts),
		newLeaseCmd(ports, rootOpts),
		newDiffCmd(ports, rootOpts),
		newMountCmd(mountArgs, rootOpts),
		newUmountCmd(),
		newStatsCmd(rootOpts),
		newVersionCmd(),
	)

	return rootCmd
}

func Execute(args []string, stdout, stderr io.Writer) int {
	rootCmd := newRootCmd(args, stdout, stderr)
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
