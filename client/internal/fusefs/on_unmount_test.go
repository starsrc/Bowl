package fusefs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestOnUnmount_LeaseLostAndBudgetProtection(t *testing.T) {
	t.Run("已标记leaseLost时OnUnmount跳过写缓冲刷盘Fsync与孤儿回收", func(t *testing.T) {
		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		// 1. 已写入待 Fsync 的脏文件
		dirty := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "dirty.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, dirty.NodeId, dirty.Fh, 0, []byte("dirty-payload"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirty.NodeId}, Fh: dirty.Fh})

		// 2. 未刷盘的异步写缓冲文件
		buffered := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "buffered.txt", syscall.O_RDWR, 0o644)
		mustWriteAt(t, fs, buffered.NodeId, buffered.Fh, 0, []byte("buffered-payload"))

		// 3. 入队等待异步重放的孤儿节点
		var rejectReclaim atomic.Bool
		rejectReclaim.Store(true)
		srv.reclaimOrphanHook = func(context.Context, *storagev1.ReclaimOrphanRequest) error {
			if rejectReclaim.Load() {
				return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			}
			return nil
		}
		orphan := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, "orphan.txt", syscall.O_RDWR, 0o644)
		requireStatus(t, "Unlink 孤儿文件", fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orphan.txt"))
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: orphan.NodeId}, Fh: orphan.Fh})

		// 关闭临时失败拦截并标记租约失效，之后调用 OnUnmount 不得再发任何远端刷盘或回收请求
		rejectReclaim.Store(false)
		fs.MarkLeaseLost()
		require.True(t, fs.IsLeaseLost())

		writesBefore := srv.writeCalls.Load()
		fsyncsBefore := srv.fsyncCalls.Load()
		reclaimsBefore := srv.reclaimOrphanCalls.Load()

		fs.OnUnmount()

		require.Equal(t, writesBefore, srv.writeCalls.Load(), "leaseLost 时 OnUnmount 不得触发写缓冲远端刷盘")
		require.Equal(t, fsyncsBefore, srv.fsyncCalls.Load(), "leaseLost 时 OnUnmount 不得触发脏节点 Fsync")
		require.Equal(t, reclaimsBefore, srv.reclaimOrphanCalls.Load(), "leaseLost 时 OnUnmount 不得触发孤儿回收 RPC")
	})

	for _, code := range []connect.Code{connect.CodeFailedPrecondition, connect.CodeNotFound} {
		t.Run("Fsync收到终态错误_"+code.String()+"_立即标记leaseLost并中止后续脏节点", func(t *testing.T) {
			srv := newFakeStorage(t, withTTL(1.0, 1.0))
			fs := newTestRawFS(t, srv, withDirectIO(false))

			for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
				f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, name, syscall.O_RDWR, 0o644)
				mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte(name))
				fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
			}

			srv.fsyncHook = func(context.Context, *storagev1.FsyncRequest) error {
				return connect.NewError(code, errors.New("terminal lease rejection"))
			}
			fsyncsBefore := srv.fsyncCalls.Load()

			fs.OnUnmount()

			require.True(t, fs.IsLeaseLost())
			require.Equal(t, fsyncsBefore+1, srv.fsyncCalls.Load(), "首个脏节点返回终态错误后必须立即跳出 Fsync 循环")
		})
	}

	t.Run("达到OnUnmount总超时预算时提前跳过剩余脏节点", func(t *testing.T) {
		restoreTimeout := fusefs.SetUnmountFlushTimeoutForTest(25 * time.Millisecond)
		defer restoreTimeout()

		srv := newFakeStorage(t, withTTL(1.0, 1.0))
		fs := newTestRawFS(t, srv, withDirectIO(false))

		for _, name := range []string{"slow1.txt", "slow2.txt", "slow3.txt"} {
			f := mustCreateFile(t, fs, fuse.FUSE_ROOT_ID, name, syscall.O_RDWR, 0o644)
			mustWriteAt(t, fs, f.NodeId, f.Fh, 0, []byte(name))
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f.NodeId}, Fh: f.Fh})
		}

		srv.fsyncHook = func(ctx context.Context, _ *storagev1.FsyncRequest) error {
			select {
			case <-time.After(40 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		}
		fsyncsBefore := srv.fsyncCalls.Load()
		start := time.Now()

		fs.OnUnmount()

		elapsed := time.Since(start)
		fsyncDelta := srv.fsyncCalls.Load() - fsyncsBefore
		require.Less(t, fsyncDelta, int64(3), "总预算耗尽后必须提前跳过后续脏节点 Fsync")
		require.Less(t, elapsed, 100*time.Millisecond, "OnUnmount 不得逐个节点累加等待完整超时")
		require.False(t, fs.IsLeaseLost(), "普通超时跳过不得误标 leaseLost")
	})
}
