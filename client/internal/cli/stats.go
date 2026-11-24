package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const statsRequestTimeout = 5 * time.Second

var statsHTTPClient = http.DefaultClient

func newStatsCmd(rootOpts *rootOptions) *cobra.Command {
	var (
		raw       bool
		adminAddr string
	)

	cmd := &cobra.Command{
		Use:   "stats [mountpoint]",
		Short: "Show FUSE statistics for a local mount process",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			return runStats(cmd, posArgs, rootOpts.jsonOut, raw, adminAddr)
		},
	}

	flags := cmd.Flags()
	flags.BoolVar(&raw, "raw", false, "Output statistics as .stats key-value pairs")
	flags.StringVar(&adminAddr, "admin-addr", "", "Direct admin HTTP address as host:port, bypassing discovery")
	return cmd
}

func runStats(cmd *cobra.Command, posArgs []string, asJSON, raw bool, adminAddr string) error {
	if asJSON && raw {
		return usageExitErr(errors.New("--json and --raw cannot be used together"), false)
	}

	if cmd.Flags().Changed("admin-addr") {
		normalized, err := normalizeStatsAdminAddr(adminAddr)
		if err != nil {
			return usageExitErr(err, false)
		}
		adminAddr = normalized
	} else {
		addr, err := discoverAdminAddr(posArgs)
		if err != nil {
			return runtimeExitErr(err)
		}
		adminAddr = addr
	}

	format := "text"
	switch {
	case asJSON:
		format = "json"
	case raw:
		format = "stats"
	}

	body, err := fetchStats(adminAddr, format)
	if err != nil {
		return runtimeExitErr(err)
	}
	stdout := cmd.OutOrStdout()
	_, _ = stdout.Write(body)
	if len(body) > 0 && body[len(body)-1] != '\n' {
		fmt.Fprintln(stdout)
	}
	return nil
}

func normalizeStatsAdminAddr(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "http://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" || strings.EqualFold(trimmed, adminAddrOff) {
		return "", fmt.Errorf("invalid --admin-addr %q", raw)
	}
	if _, _, err := net.SplitHostPort(trimmed); err != nil {
		return "", fmt.Errorf("invalid --admin-addr %q: %w", raw, err)
	}
	return loopbackAdminAddr(trimmed), nil
}

func discoverAdminAddr(posArgs []string) (string, error) {
	var meta mountMeta
	if len(posArgs) == 1 {
		absMP, err := filepath.Abs(posArgs[0])
		if err != nil {
			return "", fmt.Errorf("failed to resolve mountpoint path: %w", err)
		}
		m, ok := findMountMeta(absMP)
		if !ok {
			return "", fmt.Errorf("no active bowl mount process found for mountpoint %q", filepath.Clean(absMP))
		}
		meta = m
	} else {
		metas := listMountMetas()
		switch len(metas) {
		case 0:
			return "", errors.New("no active bowl mounts found")
		case 1:
			meta = metas[0]
		default:
			mps := make([]string, 0, len(metas))
			for i := range metas {
				mps = append(mps, metas[i].Mountpoint)
			}
			return "", fmt.Errorf("multiple active mounts found; specify a mountpoint: %s", strings.Join(mps, ", "))
		}
	}
	if meta.AdminAddr == "" || strings.EqualFold(meta.AdminAddr, adminAddrOff) {
		return "", fmt.Errorf("admin endpoint is disabled for mountpoint %q", meta.Mountpoint)
	}
	return loopbackAdminAddr(meta.AdminAddr), nil
}

func fetchStats(addr, format string) ([]byte, error) {
	addr = loopbackAdminAddr(addr)
	u := url.URL{Scheme: "http", Host: addr, Path: "/debug/stats", RawQuery: url.Values{"format": {format}}.Encode()}
	ctx, cancel := context.WithTimeout(context.Background(), statsRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to build stats request: %w", err)
	}
	resp, err := statsHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query admin endpoint %s: %w", addr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read stats response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("admin endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}
