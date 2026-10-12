package object

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type withPrefix struct {
	DefaultObjectStorage
	os     ObjectStorage
	prefix string
}

// WithPrefix 返回自动为所有对象键添加指定前缀的 ObjectStorage 包装器
func WithPrefix(os ObjectStorage, prefix string) ObjectStorage {
	if prefix == "" {
		return os
	}
	return &withPrefix{os: os, prefix: prefix}
}

func (s *withPrefix) String() string {
	return fmt.Sprintf("%s%s", s.os.String(), s.prefix)
}

func (s *withPrefix) Limits() Limits {
	return s.os.Limits()
}

func (s *withPrefix) Create(ctx context.Context) error {
	return s.os.Create(ctx)
}

func (s *withPrefix) Head(ctx context.Context, key string) (Object, error) {
	o, err := s.os.Head(ctx, s.prefix+key)
	if err != nil {
		return nil, err
	}
	return &obj{
		key:    strings.TrimPrefix(o.Key(), s.prefix),
		size:   o.Size(),
		mtime:  o.Mtime(),
		isDir:  o.IsDir(),
		sc:     o.StorageClass(),
		status: o.Status(),
	}, nil
}

func (s *withPrefix) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	return s.os.Get(ctx, s.prefix+key, off, limit, getters...)
}

func (s *withPrefix) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	return s.os.Put(ctx, s.prefix+key, in, getters...)
}

func (s *withPrefix) Copy(ctx context.Context, dst, src string) error {
	return s.os.Copy(ctx, s.prefix+dst, s.prefix+src)
}

func (s *withPrefix) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	return s.os.Delete(ctx, s.prefix+key, getters...)
}

func (s *withPrefix) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	objs, hasMore, nextToken, err := s.os.List(ctx, s.prefix+prefix, s.prefix+startAfter, token, delimiter, limit, followLink)
	if err != nil {
		return nil, false, "", err
	}
	res := make([]Object, len(objs))
	for i, o := range objs {
		res[i] = &obj{
			key:    strings.TrimPrefix(o.Key(), s.prefix),
			size:   o.Size(),
			mtime:  o.Mtime(),
			isDir:  o.IsDir(),
			sc:     o.StorageClass(),
			status: o.Status(),
		}
	}
	return res, hasMore, nextToken, nil
}
