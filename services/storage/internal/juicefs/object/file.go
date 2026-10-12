package object

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type filestore struct {
	DefaultObjectStorage
	root string
}

func (d *filestore) String() string {
	if runtime.GOOS == "windows" {
		return "file:///" + d.root
	}
	return "file://" + d.root
}

func (d *filestore) path(key string) string {
	if strings.HasSuffix(d.root, "/") {
		return filepath.Join(d.root, key)
	}
	return filepath.Clean(d.root + key)
}

func (d *filestore) Head(ctx context.Context, key string) (Object, error) {
	p := d.path(key)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	return &obj{
		key:    key,
		size:   fi.Size(),
		mtime:  fi.ModTime(),
		isDir:  fi.IsDir(),
		sc:     "STANDARD",
		status: "NORMAL",
	}, nil
}

func (d *filestore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	p := d.path(key)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	if limit > 0 {
		return &limitReaderCloser{f: f, r: io.LimitReader(f, limit)}, nil
	}
	return f, nil
}

type limitReaderCloser struct {
	f *os.File
	r io.Reader
}

func (l *limitReaderCloser) Read(p []byte) (n int, err error) {
	return l.r.Read(p)
}

func (l *limitReaderCloser) Close() error {
	return l.f.Close()
}

func (d *filestore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	p := d.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", p, time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, in); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, p)
}

func (d *filestore) Copy(ctx context.Context, dst, src string) error {
	r, err := d.Get(ctx, src, 0, -1)
	if err != nil {
		return err
	}
	defer r.Close()
	return d.Put(ctx, dst, r)
}

func (d *filestore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	p := d.path(key)
	err := os.Remove(p)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (d *filestore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	var objs []Object
	err := filepath.Walk(d.root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if p == d.root {
			return nil
		}
		rel, err := filepath.Rel(d.root, p)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefix) && rel > marker {
			objs = append(objs, &obj{
				key:    rel,
				size:   info.Size(),
				mtime:  info.ModTime(),
				isDir:  info.IsDir(),
				sc:     "STANDARD",
				status: "NORMAL",
			})
		}
		return nil
	})
	if err != nil {
		return nil, false, "", err
	}
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Key() < objs[j].Key()
	})
	hasMore := false
	if limit > 0 && int64(len(objs)) > limit {
		hasMore = true
		objs = objs[:limit]
	}
	return objs, hasMore, "", nil
}

func newFile(endpoint, accesskey, secretkey, token string) (ObjectStorage, error) {
	if !strings.HasSuffix(endpoint, "/") {
		endpoint += "/"
	}
	if err := os.MkdirAll(endpoint, 0755); err != nil {
		return nil, err
	}
	return &filestore{root: endpoint}, nil
}

func init() {
	Register("file", newFile)
}
