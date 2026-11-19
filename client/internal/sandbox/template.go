package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ExportImage 把一个容器镜像的文件系统解到本机目录 dst(通常是刚挂好的模板盘)。
// 借本机 Docker 拉镜像和合并各层:docker create 出一个不运行的容器,再 docker export 成 tar 解开。
//
// ponytail: 依赖本机 Docker。不想要这个依赖时改成直接按 OCI 规范拉层、按顺序解开并处理 whiteout。
func ExportImage(ctx context.Context, image, arch, dst string, errOut io.Writer) error {
	return exportImage(ctx, image, arch, errOut, exec.CommandContext(ctx, "tar", "-x", "-p", "-f", "-", "-C", dst))
}

// ImportImage 和 ExportImage 一样,但解到沙箱 id 的容器里的 dst:本机没有 FUSE(比如 macOS)、
// 盘只能挂在容器里时用它。
func (rt *Runtime) ImportImage(ctx context.Context, id, image, arch, dst string, errOut io.Writer) error {
	return exportImage(ctx, image, arch, errOut, rt.command(ctx, "exec", "-i", ContainerName(id), "tar", "-x", "-p", "-f", "-", "-C", dst))
}

func exportImage(ctx context.Context, image, arch string, errOut io.Writer, untar *exec.Cmd) error {
	args := []string{"create"}
	if arch != "" {
		args = append(args, "--platform", "linux/"+arch)
	}
	out, err := exec.CommandContext(ctx, "docker", append(args, image, "/bin/true")...).Output() //nolint:gosec // 镜像名由调用方给出,按 argv 传递
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok { //nolint:errorlint // 只取 stderr
			return fmt.Errorf("docker create %s: %s", image, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return fmt.Errorf("docker create %s: %w", image, err)
	}
	container := strings.TrimSpace(string(out))
	defer func() { _ = exec.Command("docker", "rm", "-f", container).Run() }() //nolint:gosec // 容器 ID 来自 docker 自己

	export := exec.CommandContext(ctx, "docker", "export", container) //nolint:gosec // 同上
	if code := pipeCommands(errOut, export, untar); code != 0 {
		return fmt.Errorf("解开镜像 %s 失败(退出码 %d)", image, code)
	}
	return nil
}

// InstallAgent 把执行代理放进 root(一块挂好的系统盘)。模板盘带上它之后,从模板 fork 的 sandbox
// 启动时就不用再写一份。
func (m *MicroVM) InstallAgent(root string) error {
	dst := filepath.Join(root, guestAgentPath)
	src := m.agentPath()
	if a, err := os.Stat(src); err == nil {
		if b, err := os.Stat(dst); err == nil && a.Size() == b.Size() {
			// ponytail: 按大小判断是不是同一个代理,换了版本但大小恰好相同时不会更新。
			return nil
		}
	}
	return installFile(src, dst)
}

// GuestAgentFile 返回执行代理在本机的路径;文件不存在时返回空。
func (m *MicroVM) GuestAgentFile() string {
	if isRegularFile(m.agentPath()) {
		return m.agentPath()
	}
	return ""
}

// BuildBlockImage 在 diskRoot(一块挂在本机的模板盘)里做一个 sizeBytes 大小的 ext4 镜像
// BlockImage,把容器镜像解进去,再放好执行代理和 resolv.conf。需要 Linux 的 mkfs.ext4 和 loop 设备。
func (m *MicroVM) BuildBlockImage(ctx context.Context, image, arch, diskRoot string, sizeBytes int64, errOut io.Writer) error {
	img := blockDevicePath(diskRoot)
	f, err := os.Create(img)
	if err != nil {
		return err
	}
	// 稀疏文件:没写过的块不占 Bowl 盘的空间。
	if err = f.Truncate(sizeBytes); err != nil {
		_ = f.Close()
		return err
	}
	if closeErr := f.Close(); closeErr != nil {
		return closeErr
	}
	if out, mkfsErr := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-E", "nodiscard", img).CombinedOutput(); mkfsErr != nil { //nolint:gosec // 路径由本包构造
		return fmt.Errorf("mkfs.ext4: %w: %s", mkfsErr, strings.TrimSpace(string(out)))
	}
	dir, err := os.MkdirTemp("", "bowl-block-")
	if err != nil {
		return err
	}
	defer os.Remove(dir)
	if out, mountErr := exec.CommandContext(ctx, "mount", "-o", "loop", img, dir).CombinedOutput(); mountErr != nil { //nolint:gosec // 同上
		return fmt.Errorf("挂载镜像: %w: %s", mountErr, strings.TrimSpace(string(out)))
	}
	fill := func() error {
		if err := ExportImage(ctx, image, arch, dir, errOut); err != nil {
			return err
		}
		if err := installFile(m.agentPath(), filepath.Join(dir, guestAgentPath)); err != nil {
			return fmt.Errorf("放入执行代理: %w", err)
		}
		return writeResolvConf(dir, m.DNS)
	}
	fillErr := fill()
	// umount 会把镜像里的脏页写回到 Bowl 盘上的文件。
	if out, umountErr := exec.Command("umount", dir).CombinedOutput(); umountErr != nil { //nolint:gosec // 同上
		return errors.Join(fillErr, fmt.Errorf("卸载镜像: %w: %s", umountErr, strings.TrimSpace(string(out))))
	}
	return fillErr
}
