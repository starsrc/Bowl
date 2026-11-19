package sandbox

import (
	"context"
	"io"
)

// Backend 是一个沙箱运行时后端要提供的全部动作。调用方（CLI、未来的控制面）只依赖
// 这个接口，不关心沙箱是容器还是 MicroVM。
//
// 目前唯一的实现是 Docker 版的 *Runtime。Mount、Unmount、Mounts、Writers 是"在沙箱里
// 再挂一块数据盘"的动作，只有容器后端有，暂时不进接口；MicroVM 后端落地时再决定去留。
//
// ponytail: CreateSpec 里的 Network、SandboxEndpoint 仍是 Docker 的说法。等第二个后端
// 真的要用到不同的参数时再拆，现在拆只是猜。
type Backend interface {
	Create(ctx context.Context, in *CreateSpec) error
	Remove(ctx context.Context, id string) error
	Exists(ctx context.Context, id string) bool
	List(ctx context.Context) ([]Row, error)
	Info(ctx context.Context, id string) (Info, error)
	IDs(ctx context.Context) ([]string, error)
	// DefaultMount 是不指定工作目录时命令的默认目录。
	DefaultMount(ctx context.Context, id string) (string, error)

	// Exec 前台执行并返回退出码；ExecBackground 后台执行并返回这次运行的 ID，
	// 之后用 Logs 读输出、用 Wait 等退出码。
	Exec(ctx context.Context, id, cwd string, argv []string, opts ExecOptions, in io.Reader, out, errOut io.Writer) (int, error)
	ExecBackground(ctx context.Context, id, cwd string, argv []string) (string, error)
	Logs(ctx context.Context, id, runID string, follow bool, out, errOut io.Writer) error
	Wait(ctx context.Context, id, runID string) (int, error)

	// Stop 停掉沙箱并卸载它的盘(写入刷回存储节点),记录保留;Start 用同一块盘重新启动,进程不保留。
	Stop(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	// Sync 让沙箱里已经写下的数据全部到达存储节点,fork 和 diff 之前调用。
	Sync(ctx context.Context, id string) error

	// Pause 冻结沙箱,内存和进程保留;Resume 接着运行。
	Pause(ctx context.Context, id string) error
	Resume(ctx context.Context, id string) error

	CopyInto(ctx context.Context, id, localPath, sandboxPath string, errOut io.Writer) error
	CopyFrom(ctx context.Context, id, sandboxPath, localPath string, errOut io.Writer) error
}

var (
	_ Backend = (*Runtime)(nil)
	_ Backend = (*MicroVM)(nil)
)
