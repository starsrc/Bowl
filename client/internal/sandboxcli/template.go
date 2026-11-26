package sandboxcli

import (
	"cmp"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newTemplateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Manage templates: ext4 images built from a container image that sandboxes boot from",
		Args:  requireNoArgs,
		RunE:  printUsage,
	}
	cmd.AddCommand(newTemplateCreateCmd(), newTemplateListCmd(), newTemplateDeleteCmd())
	return cmd
}

func newTemplateCreateCmd() *cobra.Command {
	var name, size, prewarm string
	cmd := &cobra.Command{
		Use:   "create <image>",
		Short: "Turn a container image (e.g. ubuntu:24.04) into a template; needs a local Docker, mkfs.ext4 and loop devices",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			image := posArgs[0]
			name = strings.TrimSpace(cmp.Or(name, image))
			sizeBytes, err := parseSize(size)
			if err != nil {
				return usageExitErr(fmt.Errorf("invalid --size: %w", err), false)
			}
			ctx := context.Background()
			start := time.Now()
			if err = vm.BuildTemplate(ctx, image, name, runtime.GOARCH, sizeBytes, cmd.ErrOrStderr()); err != nil {
				return runtimeExitErr(err)
			}
			var warm []string
			if prewarm != "" {
				warm = []string{"sh", "-c", prewarm}
			}
			if err = vm.SnapshotTemplate(ctx, name, warm); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: no snapshot, sandboxes from this template will boot normally: %v\n", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Template %s is ready in %s\n  Use:   bowl-sandbox create --template %s\n",
				name, time.Since(start).Round(time.Millisecond), name)
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&name, "name", "", "Template name (default: the image name)")
	flags.StringVar(&size, "size", "4GiB", "Size of the template's ext4 image, e.g. 4GiB or 512MiB; every sandbox gets a disk this size")
	flags.StringVar(&prewarm, "prewarm", "", "Shell command to run in the template VM before its snapshot is taken, "+
		"so every sandbox starts with its effects in memory (e.g. a warm page cache)")
	return cmd
}

func parseSize(s string) (int64, error) {
	for suffix, unit := range map[string]int64{"GiB": 1 << 30, "MiB": 1 << 20} {
		if num, ok := strings.CutSuffix(strings.TrimSpace(s), suffix); ok {
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil || n <= 0 || n > (1<<62)/unit {
				return 0, fmt.Errorf("%q is not a positive size", s)
			}
			return n * unit, nil
		}
	}
	return 0, fmt.Errorf("%q needs a GiB or MiB suffix", s)
}

func newTemplateListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List templates",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			tpls, err := vm.Templates()
			if err != nil {
				return runtimeExitErr(err)
			}
			out := cmd.OutOrStdout()
			if len(tpls) == 0 {
				fmt.Fprintln(out, "No templates")
				return nil
			}
			const format = "%-32s %-9s %s\n"
			fmt.Fprintf(out, format, "NAME", "SNAPSHOT", "CREATED")
			for _, t := range tpls {
				snap := "no"
				if t.Snapshot {
					snap = "yes"
				}
				fmt.Fprintf(out, format, t.Name, snap, t.Created.UTC().Format(time.RFC3339))
			}
			return nil
		},
	}
}

func newTemplateDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "delete <name>",
		Aliases: []string{"rm"},
		Short:   "Delete a template; sandboxes already created from it keep working",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			if err := vm.DeleteTemplate(posArgs[0]); err != nil {
				return runtimeExitErr(err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Deleted template %s\n", posArgs[0])
			return nil
		},
	}
}
