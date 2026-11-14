package sandbox

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const linuxClientModule = "module github.com/starsrc/bowl/client"

// EnsureClientBinary 准备好注入沙箱容器的 Linux 版 bowl 客户端，arch 是沙箱所在机器的架构
// （为空取本机架构）。按顺序查找，找到即用：
//
//  1. override（--binary）或 $BOWL_LINUX_BINARY；
//  2. 当前可执行文件旁边的 bowl-linux-<arch>：发布包里随 CLI 一起分发，不需要 Go；
//  3. 当前可执行文件自身（本机就是同架构的 Linux 时）；
//  4. 能找到 client 源码时交叉编译并缓存；找不到源码就用已有的缓存。
func EnsureClientBinary(override, arch string, rebuild bool, log io.Writer) (string, error) {
	if arch == "" {
		arch = runtime.GOARCH
	}
	if p := strings.TrimSpace(override); p != "" {
		return requireFile(p)
	}
	if p := strings.TrimSpace(os.Getenv("BOWL_LINUX_BINARY")); p != "" {
		return requireFile(p)
	}
	name := "bowl-linux-" + arch
	exe, exeErr := os.Executable()
	if exeErr == nil && !rebuild {
		if sibling := filepath.Join(filepath.Dir(exe), name); isRegularFile(sibling) {
			return sibling, nil
		}
		if runtime.GOOS == "linux" && runtime.GOARCH == arch {
			return exe, nil
		}
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("无法定位缓存目录: %w", err)
	}
	out := filepath.Join(cacheDir, "bowl", name)
	srcDir := findClientSourceDir()
	if srcDir == "" {
		if isRegularFile(out) {
			return out, nil
		}
		return "", fmt.Errorf("找不到 Linux 客户端 %s：把它放在 bowl 可执行文件旁边，或用 --binary / $BOWL_LINUX_BINARY 指定", name)
	}
	if !rebuild && !sourceNewerThan(srcDir, out) {
		return out, nil
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return "", fmt.Errorf("创建缓存目录失败: %w", err)
	}
	fmt.Fprintf(log, "正在编译 Linux 客户端 -> %s\n", out)
	cmd := exec.Command("go", "build", "-o", out, "./cmd/bowl")
	cmd.Dir = srcDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+arch, "GOWORK=off")
	cmd.Stderr = log
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("编译 Linux 客户端失败: %w", err)
	}
	return out, nil
}

func isRegularFile(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.Mode().IsRegular()
}

func requireFile(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err != nil {
		return "", fmt.Errorf("指定的 Linux 客户端不可用: %w", err)
	}
	return abs, nil
}

// findClientSourceDir 找出本机 bowl client 模块的源码目录。
func findClientSourceDir() string {
	var candidates []string
	if env := strings.TrimSpace(os.Getenv("BOWL_SOURCE_DIR")); env != "" {
		candidates = append(candidates, env, filepath.Join(env, "client"))
	}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, wd, filepath.Join(wd, "client"), filepath.Join(wd, "..", "client"))
	}
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidates = append(candidates, dir, filepath.Join(dir, "client"))
	}
	for _, dir := range candidates {
		if isClientModule(dir) {
			return dir
		}
	}
	return ""
}

func isClientModule(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		return false
	}
	return strings.Contains(string(data), linuxClientModule)
}

// sourceNewerThan 判断 client 源码是否比缓存二进制新。
func sourceNewerThan(srcDir, bin string) bool {
	binInfo, err := os.Stat(bin)
	if err != nil {
		return true
	}
	binTime := binInfo.ModTime()
	newer := false
	_ = filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || newer {
			return nil //nolint:nilerr // 单个文件读不到不影响判断，继续遍历其余源码
		}
		if d.IsDir() {
			if path != srcDir && strings.HasPrefix(d.Name(), ".") || d.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") && name != "go.mod" && name != "go.sum" {
			return nil
		}
		if strings.HasSuffix(name, "_test.go") {
			return nil
		}
		if info, infoErr := d.Info(); infoErr == nil && info.ModTime().After(binTime) {
			newer = true
		}
		return nil
	})
	return newer
}
