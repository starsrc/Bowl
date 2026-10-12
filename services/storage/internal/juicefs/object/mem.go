package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type mobj struct {
	data  []byte
	mtime time.Time
	mode  os.FileMode
}

type memStore struct {
	sync.Mutex
	DefaultObjectStorage
	name    string
	objects map[string]*mobj
}

func (m *memStore) String() string {
	return fmt.Sprintf("mem://%s/", m.name)
}

func (m *memStore) Head(ctx context.Context, key string) (Object, error) {
	m.Lock()
	defer m.Unlock()
	if key == "" {
		return nil, errors.New("对象键不能为空")
	}
	o, ok := m.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &obj{
		key:    key,
		size:   int64(len(o.data)),
		mtime:  o.mtime,
		isDir:  strings.HasSuffix(key, "/"),
		sc:     "STANDARD",
		status: "NORMAL",
	}, nil
}

func (m *memStore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	m.Lock()
	defer m.Unlock()
	if key == "" {
		return nil, errors.New("对象键不能为空")
	}
	d, ok := m.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	if off > int64(len(d.data)) {
		off = int64(len(d.data))
	}
	data := d.data[off:]
	if limit > 0 && limit < int64(len(data)) {
		data = data[:limit]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memStore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	m.Lock()
	defer m.Unlock()
	if key == "" {
		return errors.New("对象键不能为空")
	}
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	m.objects[key] = &mobj{data: data, mtime: time.Now(), mode: 0644}
	return nil
}

func (m *memStore) Copy(ctx context.Context, dst, src string) error {
	d, err := m.Get(ctx, src, 0, -1)
	if err != nil {
		return err
	}
	defer d.Close()
	return m.Put(ctx, dst, d)
}

func (m *memStore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	m.Lock()
	defer m.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memStore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	m.Lock()
	defer m.Unlock()

	objs := make([]Object, 0)
	for k, o := range m.objects {
		if strings.HasPrefix(k, prefix) && k > marker {
			objs = append(objs, &obj{
				key:    k,
				size:   int64(len(o.data)),
				mtime:  o.mtime,
				isDir:  strings.HasSuffix(k, "/"),
				sc:     "STANDARD",
				status: "NORMAL",
			})
		}
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

func newMem(endpoint, accesskey, secretkey, token string) (ObjectStorage, error) {
	store := &memStore{name: endpoint}
	store.objects = make(map[string]*mobj)
	return store, nil
}

func init() {
	Register("mem", newMem)
}
