package handoff

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirRemote 用一个本地目录顶替对象存储，供原型与测试使用。
// 两个存储节点指向同一个目录，就等价于它们共用一个 bucket。
type DirRemote struct {
	root string
}

// NewDirRemote 以 root 为 bucket 根目录创建 DirRemote。
func NewDirRemote(root string) (*DirRemote, error) {
	clean := filepath.Clean(root)
	if err := os.MkdirAll(clean, 0o755); err != nil {
		return nil, fmt.Errorf("handoff: init remote dir: %w", err)
	}
	return &DirRemote{root: clean}, nil
}

func (d *DirRemote) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("handoff: invalid key %q", key)
	}
	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

// Put 先写临时文件再改名，读者永远看不到写了一半的对象。
func (d *DirRemote) Put(ctx context.Context, key string, r io.Reader) error {
	// 和对象存储一样拒绝已取消的请求，否则误用已取消 ctx 的调用方只在真实远端上才出错。
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	p, err := d.path(key)
	if err != nil {
		return err
	}
	return writeAtomic(p, r)
}

// Get 打开对象供读取。
func (d *DirRemote) Get(_ context.Context, key string) (io.ReadCloser, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// ReadAt 读取对象的一段。
func (d *DirRemote) ReadAt(_ context.Context, key string, off int64, dst []byte) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	_, readErr := f.ReadAt(dst, off)
	return errors.Join(readErr, f.Close())
}

// Size 返回对象大小。
func (d *DirRemote) Size(_ context.Context, key string) (int64, error) {
	p, err := d.path(key)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// List 列出 prefix 下的全部对象（不含上传中的临时文件与 HEAD 的锁文件）。
func (d *DirRemote) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	walkErr := filepath.WalkDir(d.root, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil // 遍历期间被并发删除
			}
			return err
		}
		name := entry.Name()
		if entry.IsDir() || strings.HasPrefix(name, ".put-") || strings.HasSuffix(name, ".lock") {
			return nil
		}
		rel, relErr := filepath.Rel(d.root, p)
		if relErr != nil {
			return relErr
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil //nolint:nilerr // 条目在遍历期间被并发删除，跳过即可
		}
		out = append(out, ObjectInfo{Key: key, Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	return out, walkErr
}

// Delete 删除对象。
func (d *DirRemote) Delete(_ context.Context, key string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if rmErr := os.Remove(p); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
		return rmErr
	}
	return nil
}

// CompareAndSwap 用同目录下的锁文件串行化"读-比较-写"，对应对象存储的条件写（If-Match）。
// 锁是 flock，跨进程有效：两个存储服务进程抢同一个 HEAD 时只有一个能成功。
func (d *DirRemote) CompareAndSwap(_ context.Context, key string, old, next []byte) (retErr error) {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if mkErr := os.MkdirAll(filepath.Dir(p), 0o755); mkErr != nil {
		return mkErr
	}
	lock, err := os.OpenFile(p+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	current, readErr := os.ReadFile(p)
	switch {
	case errors.Is(readErr, os.ErrNotExist):
		if old != nil {
			return ErrConflict
		}
	case readErr != nil:
		return readErr
	case old == nil || !bytes.Equal(current, old):
		return ErrConflict
	}
	return writeAtomic(p, bytes.NewReader(next))
}

func writeAtomic(p string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".put-*")
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(tmp, r)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	return nil
}
