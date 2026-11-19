package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/sandbox"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// move hands a sandbox's disk from one storage node to another (local to cloud or back) and
// starts the sandbox again against the new node. The sandbox ID, size and disk contents stay
// the same; which node owns the disk and where it runs change. Processes do not move.
//
// ponytail: the compute stays on this machine's runtime; only the storage node changes. When
// cloud compute exists, the last step becomes "start it over there" and the rest is the same.
func newSandboxMoveCmd() *cobra.Command {
	var to, from, binary, token string
	var force bool
	cmd := &cobra.Command{
		Use:   "move <sandbox-id> --to <endpoint>",
		Short: "Hand a sandbox's disk to another storage node and start the sandbox against it; processes do not move",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, posArgs []string) error {
			id := sandbox.StripID(posArgs[0])
			out := cmd.OutOrStdout()
			ctx := context.Background()
			target, err := rpc.ResolveEndpoint(to)
			if err != nil || to == "" {
				return usageExitErr(fmt.Errorf("--to needs the target storage node's URL: %w", errors.Join(err, errors.New("empty"))), false)
			}
			info, infoErr := sboxRuntime.Info(ctx, id)
			if infoErr != nil && !force {
				return runtimeExitErr(infoErr)
			}
			source := cmp.Or(from, info.Endpoint)
			if source == "" && !force {
				return usageExitErr(errors.New("this sandbox has no recorded storage node; pass --from"), false)
			}
			if source != "" {
				if source, err = rpc.ResolveEndpoint(source); err != nil {
					return usageExitErr(fmt.Errorf("invalid --from: %w", err), false)
				}
				if source == target {
					return usageExitErr(fmt.Errorf("sandbox %s is already on %s", id, target), false)
				}
			}

			started := time.Now()
			step := func(label string, began time.Time) {
				fmt.Fprintf(out, "  %-34s %s\n", label, time.Since(began).Round(time.Millisecond))
			}
			began := time.Now()
			if infoErr == nil {
				// Unmounting flushes the disk to the source node and releases the mount lease.
				if err = sboxRuntime.Remove(ctx, id); err != nil && !force {
					return runtimeExitErr(fmt.Errorf("stop the sandbox: %w", err))
				}
			}
			step("stop the sandbox and unmount", began)

			if !force {
				began = time.Now()
				resp, pushErr := handoffPush(ctx, source, token, id)
				if pushErr != nil {
					return runtimeExitErr(fmt.Errorf("push to the remote from %s: %w\n"+
						"The sandbox is stopped and the disk still belongs to %s; fix this and run move again", source, pushErr, source))
				}
				step("push and release on the source", began)
				fmt.Fprintf(out, "    %d packs, %d uploaded (%s)\n", resp.GetPacks(), resp.GetPacksUploaded(), FormatSize(resp.GetPackBytesUploaded()))
			}

			began = time.Now()
			pulled, err := handoffPull(ctx, target, token, id, force)
			if err != nil {
				msg := "pull on %s: %w"
				if !force {
					msg += "\nThe disk was released but not claimed; run move again with --force to claim it on the target"
				}
				return runtimeExitErr(fmt.Errorf(msg, target, err))
			}
			step("pull and claim on the target", began)
			fmt.Fprintf(out, "    %d packs, %d downloaded (%s), %d left to fetch on demand\n",
				pulled.GetPacks(), pulled.GetPacksDownloaded(), FormatSize(pulled.GetPackBytesDownloaded()), pulled.GetPacksDeferred())

			began = time.Now()
			spec := &sandbox.CreateSpec{
				ID: id, Image: info.Image, MountPath: info.MountPath, Network: info.Network,
				HostEndpoint: target, Token: rpc.ResolveToken(token), CPUs: info.CPUs, MemoryMiB: info.MemoryMiB,
			}
			if sboxRuntimeName == runtimeMicroVM {
				err = sboxStartMicroVM(spec, binary)
			} else {
				err = sboxStart(spec, binary, false, io.Discard)
			}
			if err != nil {
				return runtimeExitErr(fmt.Errorf("start the sandbox on %s: %w\nThe disk is ready on the target; only the sandbox did not start", target, err))
			}
			step("start the sandbox against the target", began)
			fmt.Fprintf(out, "Moved sandbox %s to %s in %s\n", id, target, time.Since(started).Round(time.Millisecond))
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&to, "to", "", "Target storage node URL (required)")
	flags.StringVar(&from, "from", "", "Source storage node URL (default: the one the sandbox was created against)")
	flags.BoolVar(&force, "force", false, "Source node is unreachable: skip the push and claim the disk on the target from the "+
		"remote's last synced state. Writes after that sync are lost")
	flags.StringVar(&binary, "binary", "", "Linux bowl client, as for create")
	flags.StringVar(&token, "token", "", "Access token used on both nodes, defaults to $BOWL_TOKEN")
	return cmd
}

func handoffPush(ctx context.Context, endpoint, token, diskID string) (*storagev1.PushDiskResponse, error) {
	client, _, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	resp, err := client.PushDisk(ctx, connect.NewRequest(&storagev1.PushDiskRequest{DiskId: diskID, Release: true}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func handoffPull(ctx context.Context, endpoint, token, diskID string, force bool) (*storagev1.PullDiskResponse, error) {
	client, _, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	resp, err := client.PullDisk(ctx, connect.NewRequest(&storagev1.PullDiskRequest{DiskId: diskID, Force: force}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
