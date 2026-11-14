package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/attribute"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// MaxDiskNameBytes 定义虚拟磁盘名称的最大字节数上限。
const MaxDiskNameBytes = 255

const diskUsageHelp = `用法:
  bowl disk <subcommand> [flags] [arguments]

可用子命令:
  create      创建新的虚拟磁盘实例
  fork        基于已有虚拟磁盘零拷贝派生新实例
  list        分页列出虚拟磁盘实例
  get         查看指定虚拟磁盘详情
  update      更新虚拟磁盘名称或配额容量 (别名: resize)
  delete      安全删除指定虚拟磁盘实例
  diff        比较源盘与共同祖先或目标盘之间的差异与冲突
  merge       将源盘变更三路合并至目标盘
  push        把磁盘当前状态同步到远端 (--release 同时交出写权限)
  pull        从远端认领磁盘并拉取最新状态 (--force 强行接管)
  head        查看远端记录的当前写者与最新快照
  status      查看这块盘在该存储节点上的角色、同步位置与待下载数据
  help        查看命令帮助

标志:
  -h, --help  查看帮助信息
`

// diskPorts 定义 disk 控制面深模块向远端存储进程与本机环境延伸的外部端口。
type diskPorts struct {
	newClient      func(endpoint, token string) (storagev1connect.StorageServiceClient, int, error)
	findMountpoint func(diskID string) (DiskMountInfo, error)
	stdin          io.Reader
}

var (
	diskHTTPClient *http.Client
	diskStdin      io.Reader = os.Stdin
)

func defaultDiskPorts() diskPorts {
	return diskPorts{
		newClient:      newDiskStorageClient,
		findMountpoint: FindDiskMountpoint,
		stdin:          diskStdin,
	}
}

// diskSubcommandSpec 描述单个 disk 子命令的解析约束、超时与适配器工厂。
type diskSubcommandSpec struct {
	name        string
	usageHelp   string
	posArgLabel string
	uuidLabel   string
	allowQuiet  bool
	spanName    string
	timeout     time.Duration
	newAdapter  func() diskRequestAdapter
}

// diskRequestAdapter 为各子命令提供专属标志解析、前置守卫与 RPC 调用实现。
type diskRequestAdapter interface {
	parseFlag(name, val string, hasVal bool, args []string, idx *int) (handled bool, err error)
	finalize(posArgs []string) (missUsage bool, err error)
	spanAttrs() []attribute.KeyValue
	preRPC(ports diskPorts, stderr io.Writer) *diskCLIError
	invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError)
}

// diskOutcome 统一承接单盘或非单盘 RPC 执行结果以供渲染与 Span 属性回填。
type diskOutcome struct {
	disk          *storagev1.Disk
	quietID       string
	jsonPayload   any
	renderHuman   func(w io.Writer)
	setSpanDiskID bool
}

// diskCLIError 表示携带退出码与可选根因（用于 Span 标错）的 CLI 错误。
type diskCLIError struct {
	exitCode     int
	msg          string
	cause        error
	jsonPayload  any
	renderStderr func(w io.Writer)
}

// diskRPCErrorPolicy 控制不同子命令对 Connect 错误码与业务 Reason 的映射策略。
type diskRPCErrorPolicy struct {
	diskID           string
	defaultOp        string
	preconditionMsg  func(diskID string, err error) string
	checkInvalidArg  bool
	checkEBUSYReason bool
}

// diskJSON 定义磁盘实体在 --json 模式下的结构化输出格式。
type diskJSON struct {
	DiskID       string `json:"disk_id"`
	ParentDiskID string `json:"parent_disk_id,omitempty"`
	Name         string `json:"name"`
	RootIno      uint64 `json:"root_ino"`
	MaxBytes     uint64 `json:"max_bytes"`
	UsedBytes    uint64 `json:"used_bytes"`
	UsedInodes   uint64 `json:"used_inodes"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

func toDiskJSON(d *storagev1.Disk) diskJSON {
	var createdAt, updatedAt string
	if d.GetCreatedAt() != nil {
		createdAt = d.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	if d.GetUpdatedAt() != nil {
		updatedAt = d.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return diskJSON{
		DiskID:       d.GetDiskId(),
		ParentDiskID: d.GetParentDiskId(),
		Name:         d.GetName(),
		RootIno:      d.GetRootIno(),
		MaxBytes:     d.GetMaxBytes(),
		UsedBytes:    d.GetUsedBytes(),
		UsedInodes:   d.GetUsedInodes(),
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
}

func diskUsagePct(usedBytes, maxBytes uint64) float64 {
	if maxBytes == 0 {
		return 0
	}
	return float64(usedBytes) * 100.0 / float64(maxBytes)
}

// printDiskDetail 向指定 Writer 打印人类可读的虚拟磁盘详情视图。
func printDiskDetail(w io.Writer, d *storagev1.Disk) {
	dj := toDiskJSON(d)
	fmt.Fprintf(w, "Disk ID:      %s\n", dj.DiskID)
	if dj.ParentDiskID != "" {
		fmt.Fprintf(w, "Parent Disk:  %s\n", dj.ParentDiskID)
	}
	fmt.Fprintf(w, "Name:         %s\n", dj.Name)
	fmt.Fprintf(w, "Root Ino:     %d\n", dj.RootIno)
	fmt.Fprintf(w, "Max Bytes:    %d (%s)\n", dj.MaxBytes, FormatSize(dj.MaxBytes))
	fmt.Fprintf(w, "Used Bytes:   %d (%s)\n", dj.UsedBytes, FormatSize(dj.UsedBytes))
	fmt.Fprintf(w, "Usage:        %.2f%%\n", diskUsagePct(dj.UsedBytes, dj.MaxBytes))
	fmt.Fprintf(w, "Used Inodes:  %d\n", dj.UsedInodes)
	fmt.Fprintf(w, "Created At:   %s\n", dj.CreatedAt)
	fmt.Fprintf(w, "Updated At:   %s\n", dj.UpdatedAt)
}

func writeDiskJSON(stdout, stderr io.Writer, payload any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(payload); err != nil {
		fmt.Fprintf(stderr, "错误: 序列化 JSON 输出失败: %v\n", err)
		return 1
	}
	return 0
}

func parseNonNilUUID(raw, label string) (string, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil {
		return "", fmt.Errorf("无效的 %s \"%s\"，必须为合法的非 Nil UUID", label, raw)
	}
	return raw, nil
}

func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("磁盘名称不能超过 %d 字节 (当前 %d 字节)", MaxDiskNameBytes, len(name))
	}
	if strings.Contains(name, "\x00") {
		return errors.New("磁盘名称不能包含空字节 (NUL)")
	}
	return nil
}

func requireNonNilDisk(d *storagev1.Disk) (*storagev1.Disk, *diskCLIError) {
	if d == nil {
		return nil, &diskCLIError{
			exitCode: 1,
			msg:      "服务端返回空的虚拟磁盘元数据",
		}
	}
	return d, nil
}

func mapDiskRPCError(err error, policy diskRPCErrorPolicy) *diskCLIError {
	if connectErr, ok := errors.AsType[*connect.Error](err); ok {
		switch connectErr.Code() {
		case connect.CodeNotFound:
			if policy.diskID != "" {
				return &diskCLIError{
					exitCode: 1,
					msg:      "磁盘不存在: " + policy.diskID,
					cause:    err,
				}
			}
		case connect.CodeFailedPrecondition:
			if policy.preconditionMsg != nil {
				return &diskCLIError{
					exitCode: 1,
					msg:      policy.preconditionMsg(policy.diskID, err),
					cause:    err,
				}
			}
		case connect.CodeInvalidArgument:
			if policy.checkInvalidArg {
				return &diskCLIError{
					exitCode: 2,
					msg:      fmt.Sprintf("无效的请求参数: %v", err),
					cause:    err,
				}
			}
		}
	}
	if policy.checkEBUSYReason && serverx.ExtractErrorInfo(err).GetReason() == "EBUSY" {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("磁盘 %s 当前仍处于挂载状态（存在活跃客户端会话），请先卸载后再删除", policy.diskID),
			cause:    err,
		}
	}
	return &diskCLIError{
		exitCode: 1,
		msg:      fmt.Sprintf("%s: %v", policy.defaultOp, err),
		cause:    err,
	}
}

// newDiskStorageClient 解析 endpoint 与 token 并创建 StorageServiceClient。
// 若 endpoint 配置非法，返回退出码 2；若创建客户端异常，返回退出码 1。
func newDiskStorageClient(endpoint, token string) (storagev1connect.StorageServiceClient, int, error) {
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		return nil, 2, fmt.Errorf("无效的 endpoint: %w", err)
	}
	resolvedToken := rpc.ResolveToken(token)

	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:   resolvedEndpoint,
		Token:      resolvedToken,
		HTTPClient: diskHTTPClient,
	})
	if err != nil {
		return nil, 1, fmt.Errorf("创建 RPC 客户端失败: %w", err)
	}
	return client, 0, nil
}

// executeDisk 供 cli.Execute 调用的零配置默认入口（注入生产适配器）。
func executeDisk(args []string, stdout, stderr io.Writer) int {
	return executeDiskWithPorts(args, stdout, stderr, defaultDiskPorts())
}

// executeDiskWithPorts 为携带显式外部端口的统一入口；nil 端口自动回退生产默认适配器。
func executeDiskWithPorts(args []string, stdout, stderr io.Writer, ports diskPorts) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, diskUsageHelp)
		return 2
	}

	var spec *diskSubcommandSpec
	subcmd := args[0]
	switch subcmd {
	case "help", "--help", "-h":
		fmt.Fprint(stdout, diskUsageHelp)
		return 0
	case "create":
		spec = &diskCreateSpec
	case "fork":
		spec = &diskForkSpec
	case "list":
		spec = &diskListSpec
	case "get":
		spec = &diskGetSpec
	case "update", "resize":
		spec = &diskUpdateSpec
	case "delete":
		spec = &diskDeleteSpec
	case "diff":
		spec = &diskDiffSpec
	case "merge":
		spec = &diskMergeSpec
	case "push", "pull", "head", "status":
		return executeDiskHandoff(subcmd, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知 disk 子命令 \"%s\"\n\n%s", subcmd, diskUsageHelp)
		return 2
	}

	if ports.newClient == nil {
		ports.newClient = newDiskStorageClient
	}
	if ports.findMountpoint == nil {
		ports.findMountpoint = FindDiskMountpoint
	}
	if ports.stdin == nil {
		ports.stdin = os.Stdin
	}

	adapter := spec.newAdapter()
	return runDiskSubcommand(spec, adapter, args[1:], stdout, stderr, ports)
}

func runDiskSubcommand(
	spec *diskSubcommandSpec,
	adapter diskRequestAdapter,
	args []string,
	stdout, stderr io.Writer,
	ports diskPorts,
) int {
	var (
		quiet    bool
		jsonOut  bool
		endpoint string
		token    string
		help     bool
		posArgs  []string
	)

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") {
			posArgs = append(posArgs, arg)
			continue
		}

		flagName := strings.TrimLeft(arg, "-")
		hasVal := strings.Contains(flagName, "=")
		var val string
		if hasVal {
			parts := strings.SplitN(flagName, "=", 2)
			flagName = parts[0]
			val = parts[1]
		}

		var flagErr error
		switch flagName {
		case "h", "help":
			help = true
		case "q", "quiet":
			if !spec.allowQuiet {
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, spec.usageHelp)
				return 2
			}
			quiet = true
		case "json":
			jsonOut = true
		case "endpoint":
			endpoint, flagErr = takeFlagValue(args, &i, flagName, val, hasVal)
			if flagErr != nil {
				fmt.Fprintf(stderr, "错误: %v\n", flagErr)
				return 2
			}
		case "token":
			token, flagErr = takeFlagValue(args, &i, flagName, val, hasVal)
			if flagErr != nil {
				fmt.Fprintf(stderr, "错误: %v\n", flagErr)
				return 2
			}
		default:
			handled, adapterErr := adapter.parseFlag(flagName, val, hasVal, args, &i)
			if adapterErr != nil {
				fmt.Fprintf(stderr, "错误: %v\n", adapterErr)
				return 2
			}
			if !handled {
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, spec.usageHelp)
				return 2
			}
		}
	}

	if help {
		fmt.Fprint(stdout, spec.usageHelp)
		return 0
	}

	if spec.posArgLabel == "" {
		if len(posArgs) > 0 {
			fmt.Fprintf(stderr, "错误: %s 命令不接受位置参数 (%v)\n\n%s", spec.name, posArgs, spec.usageHelp)
			return 2
		}
	} else if len(posArgs) != 1 {
		fmt.Fprintf(stderr, "错误: %s 命令需要且仅需要 1 个位置参数 (%s)\n\n%s", spec.name, spec.posArgLabel, spec.usageHelp)
		return 2
	}

	if quiet && jsonOut {
		fmt.Fprintln(stderr, "错误: -q/--quiet 与 --json 不能同时使用")
		return 2
	}

	if spec.uuidLabel != "" {
		if _, err := parseNonNilUUID(posArgs[0], spec.uuidLabel); err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 2
		}
	}

	if missUsage, err := adapter.finalize(posArgs); err != nil {
		if missUsage {
			fmt.Fprintf(stderr, "错误: %v\n\n%s", err, spec.usageHelp)
			return 2
		}
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 2
	}

	if cliErr := adapter.preRPC(ports, stderr); cliErr != nil {
		if cliErr.msg != "" {
			fmt.Fprintf(stderr, "错误: %s\n", cliErr.msg)
		}
		return cliErr.exitCode
	}

	spanCtx, span, finishSpan := startDiskCLISpan(
		context.Background(),
		spec.spanName,
		adapter.spanAttrs()...,
	)
	defer finishSpan()

	client, exitCode, clientErr := ports.newClient(endpoint, token)
	if clientErr != nil {
		finishDiskCLISpanError(span, clientErr)
		fmt.Fprintf(stderr, "错误: %v\n", clientErr)
		return exitCode
	}

	rpcCtx, rpcCancel := context.WithTimeout(spanCtx, spec.timeout)
	defer rpcCancel()

	outcome, cliErr := adapter.invoke(rpcCtx, client)
	if cliErr != nil {
		if cliErr.cause != nil {
			finishDiskCLISpanError(span, cliErr.cause)
		}
		if jsonOut && cliErr.jsonPayload != nil {
			if code := writeDiskJSON(stdout, stderr, cliErr.jsonPayload); code != 0 {
				return code
			}
			return cliErr.exitCode
		}
		if cliErr.renderStderr != nil {
			cliErr.renderStderr(stderr)
		} else if cliErr.msg != "" {
			fmt.Fprintf(stderr, "错误: %s\n", cliErr.msg)
		}
		return cliErr.exitCode
	}

	if outcome.setSpanDiskID && outcome.disk != nil {
		span.SetAttributes(attribute.String("bowl.disk_id", outcome.disk.GetDiskId()))
	}

	if quiet {
		switch {
		case outcome.disk != nil:
			fmt.Fprintln(stdout, outcome.disk.GetDiskId())
			return 0
		case outcome.quietID != "":
			fmt.Fprintln(stdout, outcome.quietID)
			return 0
		}
	}

	if jsonOut {
		payload := outcome.jsonPayload
		if payload == nil && outcome.disk != nil {
			payload = toDiskJSON(outcome.disk)
		}
		return writeDiskJSON(stdout, stderr, payload)
	}

	if outcome.renderHuman != nil {
		outcome.renderHuman(stdout)
		return 0
	}
	if outcome.disk != nil {
		printDiskDetail(stdout, outcome.disk)
	}
	return 0
}
