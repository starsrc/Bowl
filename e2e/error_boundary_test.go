//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestProtocolBoundary_ValidationErrors 覆盖基于 Protobuf/CEL 校验器拦截的参数边界错误（ENAMETOOLONG / ERANGE / EINVAL）。
func TestProtocolBoundary_ValidationErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-err-boundary-disk",
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

	// 1. ENAMETOOLONG 错误断言
	t.Run("ENAMETOOLONG 超长文件名拦截", func(t *testing.T) {
		name256 := bytes.Repeat([]byte("a"), 256)

		// Lookup
		_, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: rootIno,
			Name:      name256,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ENAMETOOLONG")

		// Create
		_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      name256,
			Mode:      0o644,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ENAMETOOLONG")

		// Mkdir
		_, err = client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      name256,
			Mode:      0o755,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ENAMETOOLONG")

		// Rename 目标名称超长
		_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: rootIno,
			OldName:      []byte("valid.txt"),
			NewParentIno: rootIno,
			NewName:      name256,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ENAMETOOLONG")

		// Symlink 目标超长 (>4095)
		target4096 := bytes.Repeat([]byte("t"), 4096)
		_, err = client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: rootIno,
			Name:      []byte("symlink_toolong"),
			Target:    target4096,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ENAMETOOLONG")
	})

	// 2. ERANGE 错误断言
	t.Run("ERANGE 扩展属性超长边界拦截", func(t *testing.T) {
		// 先在根目录下创建一个常规文件
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("xattr_boundary_file.txt"),
			Mode:      0o644,
		}))
		require.NoError(t, err)
		fileIno := createResp.Msg.GetInode().GetIno()

		// 属性名称超过 255 字节 -> ERANGE
		attrName256 := bytes.Repeat([]byte("x"), 256)
		_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   fileIno,
			Name:  attrName256,
			Value: []byte("v"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ERANGE")

		_, err = client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  fileIno,
			Name: attrName256,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ERANGE")

		_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
			Ino:  fileIno,
			Name: attrName256,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ERANGE")

		// 属性值超过 65536 字节 -> ERANGE
		attrVal65537 := bytes.Repeat([]byte("v"), 65537)
		_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   fileIno,
			Name:  []byte("user.large"),
			Value: attrVal65537,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "ERANGE")
	})

	// 3. EINVAL 静态校验非法参数拦截
	t.Run("EINVAL 格式与参数边界校验拦截", func(t *testing.T) {
		// 空文件名
		_, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte{},
			Mode:      0o644,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// 文件名包含路径分隔符 /
		_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("sub/dir.txt"),
			Mode:      0o644,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// 文件名包含 NUL 字符
		_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: rootIno,
			Name:      []byte("file\x00.txt"),
			Mode:      0o644,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// Xattr 空属性名
		_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   rootIno,
			Name:  []byte{},
			Value: []byte("val"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// Xattr 属性名含 NUL 字符
		_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   rootIno,
			Name:  []byte("user.test\x00"),
			Value: []byte("val"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// Ino 为 0
		_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: 0,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// SessionID 为 0
		_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: 0,
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

		// 全零 Nil UUID 磁盘 ID
		_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: "00000000-0000-0000-0000-000000000000",
		}))
		testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")
	})
}

// TestDomainSentinels_ContractMapping 覆盖端到端领域哨兵错误的 ConnectRPC 状态码与 bowl.storage ErrorInfo 契约。
// 覆盖：ENOENT, EEXIST, ENOTDIR, EISDIR, ENOTEMPTY, EBADF, ENODATA, EPERM, EXDEV
func TestDomainSentinels_ContractMapping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 创建 Disk A
	diskAResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-sentinel-disk-a",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	diskA := diskAResp.Msg.GetDisk()
	rootInoA := diskA.GetRootIno()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskA.GetDiskId()}))
	})

	// 创建 Disk B (用于 EXDEV 验证)
	diskBResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-sentinel-disk-b",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	diskB := diskBResp.Msg.GetDisk()
	rootInoB := diskB.GetRootIno()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskB.GetDiskId()}))
	})

	// 在 Disk A 准备子目录与常规文件
	subDirName := []byte("subdir")
	mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootInoA,
		Name:      subDirName,
		Mode:      0o755,
	}))
	require.NoError(t, err)
	subDirIno := mkdirResp.Msg.GetInode().GetIno()

	fileName := []byte("regular.txt")
	createFileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: subDirIno,
		Name:      fileName,
		Mode:      0o644,
		Flags:     oRdWr,
	}))
	require.NoError(t, err)
	fileIno := createFileResp.Msg.GetInode().GetIno()
	fh := createFileResp.Msg.GetFh()

	// 1. ENOENT (NotFound)
	t.Run("ENOENT 不存在的节点与目录项", func(t *testing.T) {
		// Lookup 不存在
		_, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: rootInoA,
			Name:      []byte("nonexistent.txt"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

		// GetAttr 不存在
		_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			Ino: 999999999,
		}))
		testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

		// Open 不存在
		_, err = client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   999999999,
			Flags: oRdWr,
		}))
		testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	})

	// 2. EEXIST (AlreadyExists)
	t.Run("EEXIST 重复创建冲突", func(t *testing.T) {
		// Mkdir 重复
		_, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootInoA,
			Name:      subDirName,
			Mode:      0o755,
		}))
		testutil.RequireConnectError(t, err, connect.CodeAlreadyExists, "EEXIST")

		// Create 重复
		_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: subDirIno,
			Name:      fileName,
			Mode:      0o644,
			Flags:     oRdWr,
		}))
		testutil.RequireConnectError(t, err, connect.CodeAlreadyExists, "EEXIST")
	})

	// 3. ENOTDIR (FailedPrecondition)
	t.Run("ENOTDIR 目录类型冲突", func(t *testing.T) {
		// 在常规文件下执行 Mkdir
		_, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: fileIno,
			Name:      []byte("impossible_dir"),
			Mode:      0o755,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTDIR")

		// 在常规文件下执行 Lookup
		_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: fileIno,
			Name:      []byte("child"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTDIR")

		// 对常规文件执行 OpenDir
		_, err = client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:   fileIno,
			Flags: oRdOnly,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTDIR")

		// 对常规文件执行 Rmdir
		_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: subDirIno,
			Name:      fileName,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTDIR")
	})

	// 4. EISDIR (FailedPrecondition)
	t.Run("EISDIR 误将目录作为文件操作", func(t *testing.T) {
		// 对目录执行常规文件 Open
		_, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   subDirIno,
			Flags: oRdWr,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EISDIR")

		// 对目录执行 Unlink
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: rootInoA,
			Name:      subDirName,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EISDIR")
	})

	// 5. ENOTEMPTY (FailedPrecondition)
	t.Run("ENOTEMPTY 递归非空目录删除拦截", func(t *testing.T) {
		// subdir 下包含 regular.txt，执行 Rmdir 应返回 ENOTEMPTY
		_, err := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
			ParentIno: rootInoA,
			Name:      subDirName,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTEMPTY")
	})

	// 6. EBADF (FailedPrecondition)
	t.Run("EBADF 无效句柄或读写模式不匹配", func(t *testing.T) {
		invalidFh := uint64(987654321)

		// 无效句柄 Flush
		_, err := client.Flush(ctx, connect.NewRequest(&storagev1.FlushRequest{
			Ino: fileIno,
			Fh:  invalidFh,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 无效句柄 Fsync
		_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
			Ino: fileIno,
			Fh:  invalidFh,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

		// 以只读模式打开文件并尝试申请写锁
		readOnlyOpen, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:   fileIno,
			Flags: oRdOnly,
		}))
		require.NoError(t, err)
		readFh := readOnlyOpen.Msg.GetFh()
		defer func() {
			_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: fileIno,
				Fh:  readFh,
			}))
		}()

		// 在只读句柄上申请排他写锁 -> EBADF
		_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        readFh,
			LockOwner: 303,
			SessionId: 70001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   3003,
			},
			LkFlags: 0,
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")
	})

	// 7. ENODATA (NotFound)
	t.Run("ENODATA 扩展属性不存在", func(t *testing.T) {
		_, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  fileIno,
			Name: []byte("user.not_exist"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENODATA")

		_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
			Ino:  fileIno,
			Name: []byte("user.not_exist"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENODATA")
	})

	// 8. EPERM (PermissionDenied)
	t.Run("EPERM 权限拒绝与目录硬链接拦截", func(t *testing.T) {
		// 严禁对目录创建硬链接
		_, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
			Ino:          subDirIno,
			NewParentIno: rootInoA,
			NewName:      []byte("dir_link_forbidden"),
		}))
		testutil.RequireConnectError(t, err, connect.CodePermissionDenied, "EPERM")

		// 创建符号链接
		symlinkResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: rootInoA,
			Name:      []byte("symlink_for_xattr"),
			Target:    []byte("target"),
		}))
		require.NoError(t, err)
		symlinkIno := symlinkResp.Msg.GetInode().GetIno()

		// 对符号链接设置 user.* 扩展属性在 Linux POSIX 规范下被禁止，返回 EPERM
		_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
			Ino:   symlinkIno,
			Name:  []byte("user.not_allowed_on_symlink"),
			Value: []byte("val"),
		}))
		testutil.RequireConnectError(t, err, connect.CodePermissionDenied, "EPERM")
	})

	// 9. EXDEV (FailedPrecondition)
	t.Run("EXDEV 跨虚拟磁盘 Link 与 Rename 拦截", func(t *testing.T) {
		// 尝试将 Disk A 的 regular.txt 硬链接到 Disk B 的根目录下
		_, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
			Ino:          fileIno,  // 归属 Disk A
			NewParentIno: rootInoB, // 归属 Disk B
			NewName:      []byte("cross_disk_link.txt"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EXDEV")

		// 尝试将 Disk A 的 regular.txt 移动到 Disk B 的根目录下
		_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
			OldParentIno: subDirIno, // 归属 Disk A
			OldName:      fileName,
			NewParentIno: rootInoB, // 归属 Disk B
			NewName:      []byte("cross_disk_rename.txt"),
		}))
		testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EXDEV")
	})

	// 释放打开的句柄
	_, _ = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino: fileIno,
		Fh:  fh,
	}))
}
