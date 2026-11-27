package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const templateImageFile = "root.img"

func (m *MicroVM) templatesDir() string { return filepath.Join(m.stateDir(), "templates") }

func (m *MicroVM) templateDir(name string) (string, error) {
	safe := strings.NewReplacer("/", "_", ":", "_").Replace(strings.TrimSpace(name))
	if !validName(safe) {
		return "", fmt.Errorf("模板名 %q 不合法", name)
	}
	return filepath.Join(m.templatesDir(), safe), nil
}

func (m *MicroVM) templateImage(name string) (string, error) {
	dir, err := m.templateDir(name)
	if err != nil {
		return "", err
	}
	img := filepath.Join(dir, templateImageFile)
	if !isRegularFile(img) {
		return "", fmt.Errorf("模板 %q 不存在,先用 bowl-sandbox template create <image> --name %s 建一个", name, name)
	}
	return img, nil
}

func (m *MicroVM) systemDisk(id string) string { return filepath.Join(m.dir(id), "root.img") }

func (m *MicroVM) cloneSystemDisk(ctx context.Context, src, id string) error {
	return copyImage(ctx, src, m.systemDisk(id))
}

func (m *MicroVM) removeSystemDisk(id string) error {
	if err := os.Remove(m.systemDisk(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("删除系统盘: %w", err)
	}
	return nil
}

func copyImage(ctx context.Context, src, dst string) error {
	out, err := exec.CommandContext(ctx, "cp", "--reflink=auto", "--sparse=always", src, dst).CombinedOutput()
	if err != nil {
		return fmt.Errorf("复制镜像: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}
