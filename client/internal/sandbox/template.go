package sandbox

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Template struct {
	Name     string
	Snapshot bool
	Created  time.Time
}

func (m *MicroVM) BuildTemplate(ctx context.Context, image, name, arch string, sizeBytes int64, errOut io.Writer) error {
	dir, err := m.templateDir(name)
	if err != nil {
		return err
	}
	if _, statErr := os.Stat(dir); statErr == nil {
		return fmt.Errorf("模板 %q 已存在,先用 bowl-sandbox template delete %s 删掉", name, name)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := m.buildImage(ctx, image, arch, filepath.Join(dir, templateImageFile), sizeBytes, errOut); err != nil {
		_ = os.RemoveAll(dir)
		return err
	}
	return nil
}

func (m *MicroVM) buildImage(ctx context.Context, image, arch, img string, sizeBytes int64, errOut io.Writer) error {
	f, err := os.Create(img)
	if err != nil {
		return err
	}
	if err = f.Truncate(sizeBytes); err != nil {
		_ = f.Close()
		return err
	}
	if closeErr := f.Close(); closeErr != nil {
		return closeErr
	}
	if out, mkfsErr := exec.CommandContext(ctx, "mkfs.ext4", "-q", "-F", "-E", "nodiscard", img).CombinedOutput(); mkfsErr != nil {
		return fmt.Errorf("mkfs.ext4: %w: %s", mkfsErr, strings.TrimSpace(string(out)))
	}
	dir, err := os.MkdirTemp("", "bowl-template-")
	if err != nil {
		return err
	}
	defer os.Remove(dir)
	if out, mountErr := exec.CommandContext(ctx, "mount", "-o", "loop", img, dir).CombinedOutput(); mountErr != nil {
		return fmt.Errorf("挂载镜像: %w: %s", mountErr, strings.TrimSpace(string(out)))
	}
	fill := func() error {
		if err := exportImage(ctx, image, arch, dir, errOut); err != nil {
			return err
		}
		if err := installFile(m.agentPath(), filepath.Join(dir, guestAgentPath)); err != nil {
			return fmt.Errorf("放入执行代理: %w", err)
		}
		return writeResolvConf(dir, m.DNS)
	}
	fillErr := fill()
	if out, umountErr := exec.Command("umount", dir).CombinedOutput(); umountErr != nil {
		return errors.Join(fillErr, fmt.Errorf("卸载镜像: %w: %s", umountErr, strings.TrimSpace(string(out))))
	}
	return fillErr
}

func (m *MicroVM) Templates() ([]Template, error) {
	entries, err := os.ReadDir(m.templatesDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取模板目录: %w", err)
	}
	var out []Template
	for _, e := range entries {
		dir := filepath.Join(m.templatesDir(), e.Name())
		fi, statErr := os.Stat(filepath.Join(dir, templateImageFile))
		if statErr != nil {
			continue
		}
		out = append(out, Template{
			Name:     e.Name(),
			Snapshot: isRegularFile(filepath.Join(dir, snapshotDisk)),
			Created:  fi.ModTime(),
		})
	}
	return out, nil
}

func (m *MicroVM) DeleteTemplate(name string) error {
	img, err := m.templateImage(name)
	if err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(img))
}

func exportImage(ctx context.Context, image, arch, dst string, errOut io.Writer) error {
	args := []string{"create"}
	if arch != "" {
		args = append(args, "--platform", "linux/"+arch)
	}
	out, err := exec.CommandContext(ctx, "docker", append(args, image, "/bin/true")...).Output()
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			return fmt.Errorf("docker create %s: %s", image, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return fmt.Errorf("docker create %s: %w", image, err)
	}
	container := strings.TrimSpace(string(out))
	defer func() { _ = exec.Command("docker", "rm", "-f", container).Run() }()

	export := exec.CommandContext(ctx, "docker", "export", container)
	untar := exec.CommandContext(ctx, "tar", "-x", "-p", "-f", "-", "-C", dst)
	if code := pipeCommands(errOut, export, untar); code != 0 {
		return fmt.Errorf("解开镜像 %s 失败(退出码 %d)", image, code)
	}
	return nil
}

func pipeCommands(stderr io.Writer, producer, consumer *exec.Cmd) int {
	pr, pw, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建管道失败: %v\n", err)
		return 1
	}
	producer.Stdout = pw
	producer.Stderr = stderr
	consumer.Stdin = pr
	consumer.Stderr = stderr
	if err := producer.Start(); err != nil {
		_ = pw.Close()
		_ = pr.Close()
		fmt.Fprintf(stderr, "错误: 启动复制失败: %v\n", err)
		return 1
	}
	if err := consumer.Start(); err != nil {
		_ = producer.Process.Kill()
		_ = pw.Close()
		_ = pr.Close()
		fmt.Fprintf(stderr, "错误: 启动复制失败: %v\n", err)
		return 1
	}
	_ = pw.Close()
	_ = pr.Close()
	consumerErr := consumer.Wait()
	producerErr := producer.Wait()
	if producerErr != nil {
		fmt.Fprintf(stderr, "错误: 读取源失败: %v\n", producerErr)
		return 1
	}
	if consumerErr != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](consumerErr); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(stderr, "错误: 写入目标失败: %v\n", consumerErr)
		return 1
	}
	return 0
}

func writeResolvConf(root, dns string) error {
	resolv := filepath.Join(root, "etc", "resolv.conf")
	_ = os.Remove(resolv)
	if err := os.WriteFile(resolv, []byte("nameserver "+cmp.Or(dns, "1.1.1.1")+"\n"), 0o644); err != nil {
		return fmt.Errorf("写 resolv.conf: %w", err)
	}
	return nil
}
