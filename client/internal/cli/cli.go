// Package cli implements bowl command-line parsing, virtual disk control-plane operations, and FUSE mounting.
package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/version"
)

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

	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	if args == nil {
		args = []string{}
	}
	rootCmd.SetArgs(args)

	var mountArgs []string
	if len(args) > 0 && args[0] == "mount" {
		mountArgs = args[1:]
	}

	rootCmd.AddCommand(
		newDiskCmd(defaultDiskPorts()),
		newMountCmd(mountArgs),
		newUmountCmd(),
		newStatsCmd(),
		newVersionCmd(),
	)

	return rootCmd
}

// Execute runs the bowl CLI with args excluding the executable name.
// It returns 0 on success, 2 on CLI syntax or validation errors, and 1 on runtime errors.
func Execute(args []string, stdout, stderr io.Writer) int {
	return execute(newRootCmd(args, stdout, stderr), stderr)
}

// ExecuteSandbox runs the bowl-sandbox CLI with args excluding the executable name.
// Exit codes follow Execute, except that exec, shell and wait pass the sandboxed command's code through.
func ExecuteSandbox(args []string, stdout, stderr io.Writer) int {
	rootCmd := newSandboxCmd()
	rootCmd.SetOut(stdout)
	rootCmd.SetErr(stderr)
	if args == nil {
		args = []string{}
	}
	rootCmd.SetArgs(args)
	return execute(rootCmd, stderr)
}

func execute(rootCmd *cobra.Command, stderr io.Writer) int {
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
