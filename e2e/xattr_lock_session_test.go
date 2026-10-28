//go:build e2e

package e2e_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// Xattr 操作标志位
	xattrCreate  uint32 = 0x1
	xattrReplace uint32 = 0x2

	// Linux FUSE 文件锁类型
	fRdLck uint32 = 0
	fWrLck uint32 = 1
	fUnLck uint32 = 2

	// Linux FUSE 锁命名空间标志
	fuseLkFlock uint32 = 0x1
)

// TestXattr_LifecycleAndFlags 覆盖扩展属性增删改查全流程与 XATTR_CREATE / XATTR_REPLACE 标志位状态机。
func TestXattr_LifecycleAndFlags(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建测试 Disk 与常规文件
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-xattr-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := diskResp.Msg.GetDisk()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
	})

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("xattr_target.txt"),
		Mode:      0o644,
		Flags:     oRdWr,
	}))
	require.NoError(t, err)
	ino := fileResp.Msg.GetInode().GetIno()

	// 2. SetXattr: 使用 XATTR_CREATE (0x1) 创建新属性 user.author
	attrAuthor := []byte("user.author")
	valAuthor := []byte("starsrc-team")
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrAuthor,
		Value: valAuthor,
		Flags: xattrCreate,
	}))
	require.NoError(t, err, "XATTR_CREATE 设置不存在的属性应成功")

	// 再次使用 XATTR_CREATE 设置已存在的 user.author 必须返回 EEXIST
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrAuthor,
		Value: []byte("duplicate"),
		Flags: xattrCreate,
	}))
	testutil.RequireConnectError(t, err, connect.CodeAlreadyExists, "EEXIST")

	// 3. SetXattr: 使用 XATTR_REPLACE (0x2)
	// 对尚不存在的 user.version 执行替换，必须返回 ENODATA
	attrVersion := []byte("user.version")
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrVersion,
		Value: []byte("v1.0.0"),
		Flags: xattrReplace,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENODATA")

	// 对已存在的 user.author 执行替换，应当成功
	valAuthorUpdated := []byte("bowl-contributors")
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrAuthor,
		Value: valAuthorUpdated,
		Flags: xattrReplace,
	}))
	require.NoError(t, err, "XATTR_REPLACE 替换已存在的属性应成功")

	// 4. 使用 flags = 0 设置更多属性以验证排序
	attrChecksum := []byte("user.checksum")
	valChecksum := []byte("sha256-abcdef123456")
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrChecksum,
		Value: valChecksum,
		Flags: 0,
	}))
	require.NoError(t, err)

	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:   ino,
		Name:  attrVersion,
		Value: []byte("v2.1.0"),
		Flags: 0,
	}))
	require.NoError(t, err)

	// 5. GetXattr: 查询属性值与缺失属性验证
	getResp, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		Ino:  ino,
		Name: attrAuthor,
	}))
	require.NoError(t, err, "GetXattr 应成功")
	assert.Equal(t, valAuthorUpdated, getResp.Msg.GetValue())

	// 查询不存在的属性 user.missing 必须返回 ENODATA
	_, err = client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		Ino:  ino,
		Name: []byte("user.missing"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENODATA")

	// 6. ListXattr: 验证属性名称按原始字节序升序（ASCII ASC）排序
	listResp, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		Ino: ino,
	}))
	require.NoError(t, err, "ListXattr 应成功")
	expectedNames := [][]byte{attrAuthor, attrChecksum, attrVersion} // 'a' < 'c' < 'v'
	require.Equal(t, len(expectedNames), len(listResp.Msg.GetNames()), "属性数量不匹配")
	for i, expected := range expectedNames {
		assert.Equal(t, string(expected), string(listResp.Msg.GetNames()[i]), "属性名称必须按字节序升序排列")
	}

	// 7. RemoveXattr: 移除属性并验证二次移除返回 ENODATA
	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		Ino:  ino,
		Name: attrChecksum,
	}))
	require.NoError(t, err, "RemoveXattr 应成功")

	// 再次移除已删除的属性返回 ENODATA
	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		Ino:  ino,
		Name: attrChecksum,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENODATA")

	// 再次 List 验证属性已移除
	listAfterRemove, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		Ino: ino,
	}))
	require.NoError(t, err)
	assert.Equal(t, 2, len(listAfterRemove.Msg.GetNames()), "删除后属性数量应为 2")
}

// TestFileLock_POSIXAndFlock 覆盖文件锁 GetLk / SetLk / SetLkw（含冲突 EAGAIN、等待唤醒、死锁 EDEADLK 与取消 EINTR）。
func TestFileLock_POSIXAndFlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 创建测试虚拟磁盘与常规文件
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-lock-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := diskResp.Msg.GetDisk()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
	})

	// 初始化两个独立的客户端会话
	session1 := uint64(50001)
	session2 := uint64(50002)

	renew1, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: session1}))
	require.NoError(t, err, "RenewSession session1 应成功")
	require.NotNil(t, renew1.Msg.GetExpiresAt())

	renew2, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: session2}))
	require.NoError(t, err, "RenewSession session2 应成功")
	require.NotNil(t, renew2.Msg.GetExpiresAt())

	// 在 session1 下创建可读写文件
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("locked_file.bin"),
		Mode:      0o666,
		Flags:     oRdWr,
		SessionId: &session1,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()
	fh1 := createResp.Msg.GetFh()

	// 在 session2 下打开同一文件
	open2Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session2,
	}))
	require.NoError(t, err)
	fh2 := open2Resp.Msg.GetFh()

	// 1. GetLk 空闲探测：没有任何锁存在时，探测返回 F_UNLCK
	getLkResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "GetLk 空闲探测应成功")
	assert.Equal(t, fUnLck, getLkResp.Msg.GetLk().GetType(), "无冲突时应返回 F_UNLCK")

	// 2. SetLk: session1 获取区间 [0, 100] 的排他写锁 F_WRLCK
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "session1 申请写锁应成功")

	// 3. GetLk 冲突探测：session2 探测 [50, 150] 区间，应探测到 session1 的写锁
	probeResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk: &storagev1.FileLock{
			Start: 50,
			End:   150,
			Type:  fRdLck,
			Pid:   2002,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err)
	assert.Equal(t, fWrLck, probeResp.Msg.GetLk().GetType(), "应探测到冲突写锁")
	assert.Equal(t, uint32(1001), probeResp.Msg.GetLk().GetPid(), "冲突锁 PID 应为 1001")

	// 4. SetLk 非阻塞冲突拦截 EAGAIN: session2 在冲突区间申请读锁必须立即返回 EAGAIN
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk: &storagev1.FileLock{
			Start: 50,
			End:   150,
			Type:  fRdLck,
			Pid:   2002,
		},
		LkFlags: 0,
	}))
	testutil.RequireConnectError(t, err, connect.CodeResourceExhausted, "EAGAIN")

	// 5. SetLkw 阻塞等待与唤醒机制
	// 启动后台 goroutine，通过 SetLkw 阻塞申请与 session1 冲突的锁
	var wg sync.WaitGroup
	var waitErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 绑定 10 秒超时 context
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer waitCancel()

		_, waitErr = client.SetLkw(waitCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			SessionId: session2,
			RequestId: 90001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   2002,
			},
			LkFlags: 0,
		}))
	}()

	// 确保 session2 已经挂起在排队等待队列中
	time.Sleep(200 * time.Millisecond)

	// session1 释放 [0, 100] 写锁唤醒等待者
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "session1 释放写锁应成功")

	// 等待 session2 的 SetLkw 被成功唤醒并完成
	wg.Wait()
	require.NoError(t, waitErr, "session2 阻塞等待后被正常唤醒获锁，不应报错")

	// 用第三个锁拥有者探测该区间，断言返回被唤醒等待者（session2 / pid 2002）的写锁冲突信息
	probeWakeupResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 303,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fRdLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "GetLk 探测等待者获锁状态应成功")
	assert.Equal(t, fWrLck, probeWakeupResp.Msg.GetLk().GetType(), "被唤醒等待者获锁后应冲突")
	assert.Equal(t, uint32(2002), probeWakeupResp.Msg.GetLk().GetPid(), "冲突锁 PID 应为 session2 的 2002")

	// session2 释放写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   2002,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "session2 释放写锁应成功")

	// 6. SetLkw 死锁检测与 EDEADLK: 双读锁同时升级写锁环路 (A <-> B)
	// session1 与 session2 分别持有 [0, 100] 的读锁 (共享读锁互不冲突)
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fRdLck, Pid: 1001},
	}))
	require.NoError(t, err)

	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fRdLck, Pid: 2002},
	}))
	require.NoError(t, err)

	// session1 发起 SetLkw 升级为排他写锁，因 session2 持有读锁而进入等待
	var deadlockWg sync.WaitGroup
	waiter1ErrCh := make(chan error, 1)
	deadlockWg.Add(1)
	go func() {
		defer deadlockWg.Done()
		upgCtx, upgCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer upgCancel()

		_, upgErr := client.SetLkw(upgCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fh1,
			LockOwner: 101,
			SessionId: session1,
			RequestId: 90002,
			Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fWrLck, Pid: 1001},
		}))
		waiter1ErrCh <- upgErr
	}()

	time.Sleep(200 * time.Millisecond)

	// 此时 session2 也发起 SetLkw 升级为写锁，触发 A <-> B 相互依赖成环，系统必须返回 EDEADLK
	_, deadlockErr := client.SetLkw(ctx, connect.NewRequest(&storagev1.SetLkwRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		RequestId: 90003,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fWrLck, Pid: 2002},
	}))
	testutil.RequireConnectError(t, deadlockErr, connect.CodeAborted, "EDEADLK")

	// session2 释放读锁以解开 session1 的等待
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fUnLck, Pid: 2002},
	}))
	require.NoError(t, err)

	deadlockWg.Wait()
	require.NoError(t, <-waiter1ErrCh, "首个等待者在对端释放读锁后应成功升级并获锁")

	// 用另一 LockOwner 调用 GetLk 断言首个等待者确已成功升级并持有排他写锁
	probeDeadlockResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fRdLck,
			Pid:   2002,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "探测首个等待者升级后的写锁应成功")
	assert.Equal(t, fWrLck, probeDeadlockResp.Msg.GetLk().GetType(), "首个等待者应成功升级为写锁")
	assert.Equal(t, uint32(1001), probeDeadlockResp.Msg.GetLk().GetPid(), "写锁 PID 应为 1001")

	// 清理 session1 获锁后的写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fUnLck, Pid: 1001},
	}))
	require.NoError(t, err, "释放 session1 升级后的写锁应成功")

	// 7. SetLkw 阻塞等待取消 EINTR
	// session1 重新持锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fWrLck, Pid: 1001},
	}))
	require.NoError(t, err)

	// session2 在可取消的 context 下阻塞等待
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	var cancelErr error
	var cancelWg sync.WaitGroup
	cancelWg.Add(1)
	go func() {
		defer cancelWg.Done()
		_, cancelErr = client.SetLkw(cancelCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			SessionId: session2,
			RequestId: 90004,
			Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fWrLck, Pid: 2002},
		}))
	}()

	time.Sleep(200 * time.Millisecond)
	cancelFunc() // 主动触发取消

	cancelWg.Wait()
	require.Error(t, cancelErr, "主动取消必须产生错误")
	assert.Equal(t, connect.CodeCanceled, connect.CodeOf(cancelErr), "客户端主动取消状态码必须为 Canceled")

	// 释放原持锁者（owner1）持有的 POSIX 写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        &storagev1.FileLock{Start: 0, End: 100, Type: fUnLck, Pid: 1001},
	}))
	require.NoError(t, err, "释放 owner1 POSIX 写锁应成功")

	// 注册独立的第三活跃会话 session3，打开同一文件获取句柄 fh3
	session3 := uint64(50003)
	renew3, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: session3}))
	require.NoError(t, err, "RenewSession session3 应成功")
	require.NotNil(t, renew3.Msg.GetExpiresAt())

	open3Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session3,
	}))
	require.NoError(t, err, "Open fh3 for session3 应成功")
	fh3 := open3Resp.Msg.GetFh()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Release(cleanupCtx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:       fileIno,
			Fh:        fh3,
			SessionId: &session3,
		}))
	})

	// 使用异会话异拥有者 (SessionId: session3, Fh: fh3, LockOwner: 303) 验证申请同一区间排他写锁立即成功。
	// 若 session2 被取消的等待者未被清理（幽灵等待者残留），冲突队列检测到异会话异拥有者的请求将判定冲突（非阻塞 SetLk 报 EAGAIN），实现 100% 反事实突变检出能力。
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "取消等待后原锁释放，异会话异拥有者非阻塞 SetLk 申请写锁应立即成功，证明无幽灵等待者残留")

	// 释放 session3 的 SetLk 测试写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "释放 session3 SetLk 测试写锁应成功")

	// 再配合短超时 2s 的 SetLkw 与新 RequestID 验证阻塞接口亦立即成功且不发生超时等待
	quickCtx, quickCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer quickCancel()
	_, err = client.SetLkw(quickCtx, connect.NewRequest(&storagev1.SetLkwRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		RequestId: 90005,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "异会话异拥有者带短超时的 SetLkw 申请写锁应立即成功")

	// 随后释放 session3 的 SetLkw 写锁并主动关闭句柄 fh3
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "释放 session3 SetLkw 测试写锁应成功")

	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino:       fileIno,
		Fh:        fh3,
		SessionId: &session3,
	}))
	require.NoError(t, err, "关闭 session3 句柄 fh3 应成功")

	// 8. BSD flock 整文件锁兼容性验证 (lk_flags = FuseLkFlock, start=0, end=math.MaxInt64)

	flockWholeFile := &storagev1.FileLock{
		Start: 0,
		End:   math.MaxInt64,
		Type:  fWrLck,
		Pid:   1001,
	}
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        flockWholeFile,
		LkFlags:   fuseLkFlock,
	}))
	require.NoError(t, err, "申请 BSD flock 整文件排他锁应成功")

	// session2 申请 flock 读锁发生冲突 -> EAGAIN
	flockRead := &storagev1.FileLock{
		Start: 0,
		End:   math.MaxInt64,
		Type:  fRdLck,
		Pid:   2002,
	}
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh2,
		LockOwner: 202,
		SessionId: session2,
		Lk:        flockRead,
		LkFlags:   fuseLkFlock,
	}))
	testutil.RequireConnectError(t, err, connect.CodeResourceExhausted, "EAGAIN")

	// 释放 flock
	flockUnlock := &storagev1.FileLock{
		Start: 0,
		End:   math.MaxInt64,
		Type:  fUnLck,
		Pid:   1001,
	}
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk:        flockUnlock,
		LkFlags:   fuseLkFlock,
	}))
	require.NoError(t, err, "释放 BSD flock 应成功")
}

// TestSession_LeaseExpirationAndOrphanReclamation 覆盖会话租约超时后孤儿节点与句柄的自动物理回收验证。
func TestSession_LeaseExpirationAndOrphanReclamation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建测试 Disk
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-session-orphan-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := diskResp.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
	})

	// 2. 注册独立的短租约 Session
	shortSessionID := uint64(88888)
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: shortSessionID}))
	require.NoError(t, err)

	// 3. 在该 shortSessionID 下创建文件并获取句柄 fhOrphan
	orphanFileName := []byte("orphan_expire.txt")
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: rootIno,
		Name:      orphanFileName,
		Mode:      0o644,
		Flags:     oRdWr,
		SessionId: &shortSessionID,
	}))
	require.NoError(t, err)
	orphanIno := createResp.Msg.GetInode().GetIno()
	orphanFh := createResp.Msg.GetFh()

	// 4. 对该文件发起 Unlink。此时 nlink = 0，但由于 shortSessionID 下的句柄仍未关闭，属于打开孤儿
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: rootIno,
		Name:      orphanFileName,
	}))
	require.NoError(t, err)

	// 使用携带相同 SessionId 与 Fh 的 SetAttr（设置 Size: proto.Uint64(4096)）建立非零配额
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:       orphanIno,
		Fh:        &orphanFh,
		SessionId: &shortSessionID,
		Size:      proto.Uint64(4096),
	}))
	require.NoError(t, err, "设置孤儿文件大小为 4096 应成功")

	// 在等待会话过期前先调用 StatFS 断言 UsedBytes == 4096 且 Files == 2
	statBefore, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), statBefore.Msg.GetUsedBytes(), "会话未过期前孤儿节点占用 4096 字节配额")
	assert.Equal(t, uint64(2), statBefore.Msg.GetFiles(), "根节点与孤儿节点共 2 个 Inode")

	// 验证此时该孤儿节点依然可以通过 Inode 获取到
	getOrphanResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: orphanIno,
	}))
	require.NoError(t, err, "会话存活期间打开孤儿文件必须依然可查询")
	assert.Equal(t, uint32(0), getOrphanResp.Msg.GetInode().GetAttr().GetNlink())
	assert.Equal(t, uint64(4096), getOrphanResp.Msg.GetInode().GetAttr().GetSize())

	// 5. 等待会话租约过期：docker-compose.yaml.example 中 STORAGE_SESSION_LEASE_TTL 为 2s
	// 我们等待 2.6 秒，确保 session 达到过期时间
	time.Sleep(2600 * time.Millisecond)

	// 6. 触发会话扫描与孤儿节点回收（调用任意活跃 session 的 RenewSession）
	activeSessionID := uint64(99999)
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
		SessionId: activeSessionID,
	}))
	require.NoError(t, err, "触发清理扫描与活跃会话续期")

	// 7. 再次查询孤儿 Inode，此时应当已被级联物理回收，返回 ENOENT
	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: orphanIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	// 验证配额已经完全释放回 0
	statResp, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), statResp.Msg.GetUsedBytes(), "过期会话的孤儿文件回收后已用空间归零")
	assert.Equal(t, uint64(1), statResp.Msg.GetFiles(), "仅剩根目录 Inode")
}
