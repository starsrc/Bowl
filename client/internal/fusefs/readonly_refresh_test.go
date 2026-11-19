package fusefs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestRawFS_只读挂载周期性丢弃目录与属性缓存(t *testing.T) {
	fs := NewRawFS(nil, Config{})
	fs.cache.putInode(42, &storagev1.Inode{Ino: 42, Attr: &storagev1.InodeAttribute{}}, 1, 1)
	_, ok := fs.cache.inodeReadOnly(42)
	require.True(t, ok)

	done := make(chan struct{})
	go func() { fs.refreshReadOnlyCache(5 * time.Millisecond); close(done) }()
	require.Eventually(t, func() bool {
		_, hit := fs.cache.inodeReadOnly(42)
		return !hit
	}, time.Second, 5*time.Millisecond, "缓存应在一个周期后被丢弃")

	fs.releaseCancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("文件系统关闭后刷新协程应退出")
	}
}
