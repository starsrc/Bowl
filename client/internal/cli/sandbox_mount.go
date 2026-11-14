package cli

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/starsrc/bowl/client/internal/sandbox"
)

// sandboxMountCmd 把一块磁盘挂进正在运行的沙箱。
func sandboxMountCmd(args []string, stdout, stderr io.Writer) int {
	var (
		diskID, mountAt, subdir string
		readOnly, force         bool
		posArgs                 []string
	)
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return 0
		case (arg == "--disk" || arg == "--path" || arg == "--subdir") && i+1 < len(args):
			i++
			switch arg {
			case "--disk":
				diskID = args[i]
			case "--path":
				mountAt = args[i]
			case "--subdir":
				subdir = args[i]
			}
		case strings.HasPrefix(arg, "--disk="):
			diskID = strings.TrimPrefix(arg, "--disk=")
		case strings.HasPrefix(arg, "--path="):
			mountAt = strings.TrimPrefix(arg, "--path=")
		case strings.HasPrefix(arg, "--subdir="):
			subdir = strings.TrimPrefix(arg, "--subdir=")
		case arg == "--read-only" || arg == "-r":
			readOnly = true
		case arg == "--force":
			force = true
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
			return 2
		default:
			posArgs = append(posArgs, arg)
		}
	}
	if len(posArgs) != 1 || diskID == "" {
		fmt.Fprint(stderr, "用法: bowl sandbox mount <sandbox-id> --disk <disk-id> [--path /mnt/data] [--read-only] [--subdir /x]\n")
		return 2
	}
	if mountAt == "" {
		mountAt = sandbox.DefaultMountPath
	}
	id := sandbox.StripID(posArgs[0])
	ctx := context.Background()
	// 一块盘同时只能有一个写者。已有写者时服务端不报错、直接静默接管，
	// 被抢占的一方要到下一次写入才会撞上 EBUSY，所以这里先挡住。
	if !readOnly && !force {
		writers, err := sboxFor(id).Writers(ctx, diskID)
		if err != nil {
			fmt.Fprintf(stderr, "警告: 无法检查写者冲突(%v)，继续挂载\n", err)
		}
		if w, conflicted := sboxConflictingWriter(writers, id, mountAt); conflicted {
			fmt.Fprintf(stderr, "错误: 磁盘 %s 正被沙箱 %s 以读写方式挂在 %s。\n", diskID, w.SandboxID, w.Path)
			fmt.Fprintf(stderr, "      接管会让对方的写入失败(EBUSY)。确认要接管请加 --force；只想看内容请用 --read-only。\n")
			return 1
		}
	}
	if err := sboxFor(id).Mount(ctx, id, diskID, mountAt,
		sandbox.MountOptions{ReadOnly: readOnly, Subdir: subdir}); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已把磁盘 %s 挂到 %s:%s\n", diskID, id, mountAt)
	return 0
}

// sandboxUnmountCmd 卸载沙箱里的一个挂载点。
func sandboxUnmountCmd(args []string, stdout, stderr io.Writer) int {
	var mountAt string
	var posArgs []string
	for i := 0; i < len(args); i++ {
		switch arg := args[i]; {
		case arg == "help" || arg == "--help" || arg == "-h":
			fmt.Fprint(stdout, sandboxUsageHelp)
			return 0
		case arg == "--path" && i+1 < len(args):
			i++
			mountAt = args[i]
		case strings.HasPrefix(arg, "--path="):
			mountAt = strings.TrimPrefix(arg, "--path=")
		case strings.HasPrefix(arg, "-"):
			fmt.Fprintf(stderr, "错误: 未知标志 %s\n", arg)
			return 2
		default:
			posArgs = append(posArgs, arg)
		}
	}
	if len(posArgs) != 1 || mountAt == "" {
		fmt.Fprint(stderr, "用法: bowl sandbox unmount <sandbox-id> --path /mnt/data\n")
		return 2
	}
	if err := sboxFor(sandbox.StripID(posArgs[0])).Unmount(context.Background(), sandbox.StripID(posArgs[0]), mountAt); err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "已卸载 %s\n", mountAt)
	return 0
}

// sandboxMountsCmd 列出沙箱里当前挂着的磁盘。
func sandboxMountsCmd(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "用法: bowl sandbox mounts <sandbox-id>\n")
		return 2
	}
	mounts, err := sboxFor(sandbox.StripID(args[0])).Mounts(context.Background(), sandbox.StripID(args[0]))
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	if len(mounts) == 0 {
		fmt.Fprintln(stdout, "该沙箱没有挂载任何磁盘")
		return 0
	}
	fmt.Fprintf(stdout, "%-38s %-20s %s\n", "DISK ID", "MOUNT PATH", "MODE")
	for _, m := range mounts {
		mode := "rw"
		if m.ReadOnly {
			mode = "ro"
		}
		fmt.Fprintf(stdout, "%-38s %-20s %s\n", m.DiskID, m.Path, mode)
	}
	return 0
}

// sboxConflictingWriter 判断是否已有会冲突的写者：同一沙箱同一路径的重复挂载视为刷新，不算冲突。
func sboxConflictingWriter(writers []sandbox.Writer, id, path string) (sandbox.Writer, bool) {
	for _, w := range writers {
		if w.SandboxID == id && w.Path == path {
			continue
		}
		return w, true
	}
	return sandbox.Writer{}, false
}
