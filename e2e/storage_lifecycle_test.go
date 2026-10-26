//go:build e2e

package e2e_test

import (
	"context"
	"math"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// POSIX 模式与文件类型掩码
	sIfReg uint32 = 0o100000
	sIfDir uint32 = 0o040000
	sIfLnk uint32 = 0o120000

	// Linux FUSE / POSIX 打开标志位
	oRdOnly uint32 = 0o0
	oWrOnly uint32 = 0o1
	oRdWr   uint32 = 0o2

	// Linux FUSE 重命名标志位
	renameNoReplace uint32 = 0x1
	renameExchange  uint32 = 0x2
)

// TestStorageLifecycle_DirectoryTreeAndFileHandles 覆盖目录树与文件句柄完整生命周期的端到端黑盒验证。
// 验证链路：
// CreateDisk -> Mkdir -> Mknod -> Create -> TmpFile -> Open -> SetAttr -> Fallocate -> Lookup -> GetAttr
// -> OpenDir -> ReadDir -> ReadDirPlus -> FsyncDir -> ReleaseDir -> Symlink -> Readlink -> Link
// -> Rename (传统 / RENAME_NOREPLACE / RENAME_EXCHANGE) -> Flush -> Fsync
// -> Unlink (含 unlink-whilst-open) -> Rmdir -> Release (孤儿节点回收与 StatFS 配额归还)
func TestStorageLifecycle_DirectoryTreeAndFileHandles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. CreateDisk: 创建独立测试虚拟磁盘（100 MiB）
	diskCapacity := uint64(100 * 1024 * 1024)
	diskName := "e2e-lifecycle-disk"
	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     diskName,
		MaxBytes: diskCapacity,
	}))
	require.NoError(t, err, "CreateDisk 应该成功")
	disk := createDiskResp.Msg.GetDisk()
	require.NotNil(t, disk, "新建 Disk 不能为空")
	diskID := disk.GetDiskId()
	rootIno := disk.GetRootIno()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: diskID,
		}))
	})

	// 验证初始 StatFS: 仅包含根目录 (1 Inode, 0 Bytes)
	statInit, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err, "初始 StatFS 应成功")
	assert.Equal(t, uint64(0), statInit.Msg.GetUsedBytes(), "初始已用字节数应为 0")
	assert.Equal(t, uint64(1), statInit.Msg.GetFiles(), "初始 Inode 数应为 1")

	// 2. Mkdir: 在根目录下创建子目录 "workdir" (mode 0755)
	dirName := []byte("workdir")
	mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: rootIno,
		Name:      dirName,
		Mode:      0o755,
	}))
	require.NoError(t, err, "Mkdir 应该成功")
	dirInode := mkdirResp.Msg.GetInode()
	require.NotNil(t, dirInode, "Mkdir 返回 Inode 不能为空")
	dirIno := dirInode.GetIno()
	assert.Equal(t, uint32(2), dirInode.GetAttr().GetNlink(), "新建目录硬链接数必须为 2（父 entry 与自身 .）")
	assert.Equal(t, sIfDir|0o755, dirInode.GetAttr().GetMode()&0o177777, "新建目录模式应为 S_IFDIR|0755")

	// 验证目录配额增加：目录不占用容量配额 (0 Bytes)，总 Inode 数为 2
	statAfterMkdir, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), statAfterMkdir.Msg.GetUsedBytes(), "创建目录不占用容量配额，已用空间应为 0 字节")
	assert.Equal(t, uint64(2), statAfterMkdir.Msg.GetFiles(), "创建目录后 Inode 数应为 2")

	// 3. Mknod: 在 workdir 下创建常规文件 "mknod_file.txt" (mode 0644 | S_IFREG)
	mknodFileName := []byte("mknod_file.txt")
	mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: dirIno,
		Name:      mknodFileName,
		Mode:      sIfReg | 0o644,
	}))
	require.NoError(t, err, "Mknod 创建常规文件应成功")
	mknodIno := mknodResp.Msg.GetInode().GetIno()
	assert.Equal(t, uint32(1), mknodResp.Msg.GetInode().GetAttr().GetNlink(), "常规文件 nlink 应为 1")

	// 4. Create: 在 workdir 下原子创建常规文件 "create_file.txt" 并获取初始句柄 fh1
	createFileName := []byte("create_file.txt")
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: dirIno,
		Name:      createFileName,
		Mode:      0o644,
		Flags:     oRdWr,
	}))
	require.NoError(t, err, "Create 创建常规文件应成功")
	createFileIno := createResp.Msg.GetInode().GetIno()
	fh1 := createResp.Msg.GetFh()
	require.Greater(t, fh1, uint64(0), "Create 返回的句柄 ID 必须大于 0")

	// 5. TmpFile: 在 workdir 下创建匿名临时文件 (nlink == 0)，获取句柄 fhTmp
	tmpFileResp, err := client.TmpFile(ctx, connect.NewRequest(&storagev1.TmpFileRequest{
		ParentIno: dirIno,
		Mode:      0o600,
		Flags:     oRdWr,
	}))
	require.NoError(t, err, "TmpFile 应成功")
	tmpFileIno := tmpFileResp.Msg.GetInode().GetIno()
	fhTmp := tmpFileResp.Msg.GetFh()
	require.Greater(t, fhTmp, uint64(0), "TmpFile 返回的句柄 ID 必须大于 0")
	assert.Equal(t, uint32(0), tmpFileResp.Msg.GetInode().GetAttr().GetNlink(), "匿名 TmpFile 初始硬链接数必须为 0")

	// 6. Open: 打开 mknod_file.txt 并获取句柄 fh2
	openResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:   mknodIno,
		Flags: oRdWr,
	}))
	require.NoError(t, err, "Open mknod_file.txt 应成功")
	fh2 := openResp.Msg.GetFh()
	require.Greater(t, fh2, uint64(0), "Open 返回的句柄 ID 必须大于 0")

	// 7. SetAttr: 对 create_file.txt 调整大小为 8192 字节并调整权限为 0600
	newSize := uint64(8192)
	newMode := uint32(0o600)
	setAttrResp, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:  createFileIno,
		Size: &newSize,
		Mode: &newMode,
		Fh:   &fh1,
	}))
	require.NoError(t, err, "SetAttr 应成功")
	assert.Equal(t, newSize, setAttrResp.Msg.GetInode().GetAttr().GetSize(), "文件大小应已变更为 8192")
	assert.Equal(t, sIfReg|newMode, setAttrResp.Msg.GetInode().GetAttr().GetMode()&0o177777, "文件权限应已变更为 0600")

	// 8. Fallocate: 对 create_file.txt 扩展分配至 16384 字节 (mode 0)
	fallocateResp, err := client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		Ino:    createFileIno,
		Fh:     fh1,
		Offset: 0,
		Length: 16384,
		Mode:   0,
	}))
	require.NoError(t, err, "Fallocate 预分配空间应成功")
	require.NotNil(t, fallocateResp)

	// 9. Lookup 与 GetAttr 验证节点元数据一致性
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      createFileName,
	}))
	require.NoError(t, err, "Lookup create_file.txt 应成功")
	assert.Equal(t, createFileIno, lookupResp.Msg.GetInode().GetIno())
	assert.Equal(t, uint64(16384), lookupResp.Msg.GetInode().GetAttr().GetSize())

	getAttrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: createFileIno,
	}))
	require.NoError(t, err, "GetAttr 应成功")
	assert.Equal(t, lookupResp.Msg.GetInode().GetAttr().GetSize(), getAttrResp.Msg.GetInode().GetAttr().GetSize())

	// 10. OpenDir, ReadDir, ReadDirPlus, FsyncDir, ReleaseDir: 目录句柄与流式遍历
	openDirResp, err := client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
		Ino:   dirIno,
		Flags: oRdOnly,
	}))
	require.NoError(t, err, "OpenDir 应成功")
	dirFh := openDirResp.Msg.GetFh()
	require.Greater(t, dirFh, uint64(0), "目录句柄必须大于 0")

	readDirResp, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		Ino:    dirIno,
		Fh:     dirFh,
		Offset: 0,
		Limit:  100,
	}))
	require.NoError(t, err, "ReadDir 应成功")
	entryNames := make(map[string]bool)
	for _, entry := range readDirResp.Msg.GetEntries() {
		entryNames[string(entry.GetName())] = true
	}
	assert.True(t, entryNames["."], "ReadDir 结果必须包含当前目录 .")
	assert.True(t, entryNames[".."], "ReadDir 结果必须包含父目录 ..")
	assert.True(t, entryNames["mknod_file.txt"], "ReadDir 结果必须包含 mknod_file.txt")
	assert.True(t, entryNames["create_file.txt"], "ReadDir 结果必须包含 create_file.txt")
	assert.False(t, entryNames["tmpfile"], "匿名 TmpFile 绝不能出现在目录遍历中")

	readDirPlusResp, err := client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		Ino:    dirIno,
		Fh:     dirFh,
		Offset: 0,
		Limit:  100,
	}))
	require.NoError(t, err, "ReadDirPlus 应成功")
	require.NotEmpty(t, readDirPlusResp.Msg.GetEntries(), "ReadDirPlus 必须返回扩展目录项")
	for _, entryPlus := range readDirPlusResp.Msg.GetEntries() {
		assert.NotNil(t, entryPlus.GetInode(), "ReadDirPlus 条目必须包含完整的 Inode 元数据")
		assert.Greater(t, entryPlus.GetAttrTimeoutSeconds(), float64(0), "必须返回大于 0 的属性超时时长")
	}

	fsyncDirResp, err := client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		Ino: dirIno,
		Fh:  dirFh,
	}))
	require.NoError(t, err, "FsyncDir 应成功")
	require.NotNil(t, fsyncDirResp)

	releaseDirResp, err := client.ReleaseDir(ctx, connect.NewRequest(&storagev1.ReleaseDirRequest{
		Ino: dirIno,
		Fh:  dirFh,
	}))
	require.NoError(t, err, "ReleaseDir 应成功")
	require.NotNil(t, releaseDirResp)

	// 已释放的 dirFh 再次调用 ReadDir 应被拦截并返回 EBADF
	_, err = client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		Ino:    dirIno,
		Fh:     dirFh,
		Offset: 0,
		Limit:  10,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")

	// 11. Symlink 与 Readlink: 符号链接生命周期
	symlinkName := []byte("symlink_to_create")
	symlinkTarget := []byte("create_file.txt")
	symlinkResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		ParentIno: dirIno,
		Name:      symlinkName,
		Target:    symlinkTarget,
	}))
	require.NoError(t, err, "Symlink 应成功")
	symlinkIno := symlinkResp.Msg.GetInode().GetIno()
	assert.Equal(t, sIfLnk|0o777, symlinkResp.Msg.GetInode().GetAttr().GetMode()&0o177777, "符号链接类型与权限应为 S_IFLNK|0777")

	readlinkResp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino: symlinkIno,
	}))
	require.NoError(t, err, "Readlink 应成功")
	assert.Equal(t, symlinkTarget, readlinkResp.Msg.GetTarget(), "Readlink 返回目标路径必须与创建时一致")

	// 对常规文件调用 Readlink 应返回 EINVAL
	_, err = client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino: createFileIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

	// 12. Link: 为 create_file.txt 在同目录下创建硬链接 "create_file_hardlink.txt"
	hardlinkName := []byte("create_file_hardlink.txt")
	linkResp, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          createFileIno,
		NewParentIno: dirIno,
		NewName:      hardlinkName,
	}))
	require.NoError(t, err, "Link 创建硬链接应成功")
	assert.Equal(t, uint32(2), linkResp.Msg.GetInode().GetAttr().GetNlink(), "创建硬链接后 nlink 应为 2")

	// 对目录发起硬链接请求必须返回 EPERM
	_, err = client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          dirIno,
		NewParentIno: dirIno,
		NewName:      []byte("dir_hardlink_fail"),
	}))
	testutil.RequireConnectError(t, err, connect.CodePermissionDenied, "EPERM")

	// 13. Rename: 覆盖重命名、RENAME_NOREPLACE 与 RENAME_EXCHANGE
	// 13.1 传统重命名 (flags = 0): 将 mknod_file.txt 重命名为 mknod_renamed.txt
	mknodRenamedName := []byte("mknod_renamed.txt")
	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno: dirIno,
		OldName:      mknodFileName,
		NewParentIno: dirIno,
		NewName:      mknodRenamedName,
		Flags:        0,
	}))
	require.NoError(t, err, "标准 Rename 应成功")

	// 旧名称 Lookup 应返回 ENOENT，新名称 Lookup 成功
	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      mknodFileName,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	lookupRenamedResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      mknodRenamedName,
	}))
	require.NoError(t, err)
	assert.Equal(t, mknodIno, lookupRenamedResp.Msg.GetInode().GetIno())

	// 13.2 RENAME_NOREPLACE (flags = 0x1): 目标已存在时必须被拦截并返回 EEXIST
	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno: dirIno,
		OldName:      mknodRenamedName,
		NewParentIno: dirIno,
		NewName:      hardlinkName, // 已存在的硬链接
		Flags:        renameNoReplace,
	}))
	testutil.RequireConnectError(t, err, connect.CodeAlreadyExists, "EEXIST")

	// 13.3 RENAME_EXCHANGE (flags = 0x2): 原子双向互换两个现有文件的 Inode 绑定
	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno: dirIno,
		OldName:      mknodRenamedName,
		NewParentIno: dirIno,
		NewName:      createFileName,
		Flags:        renameExchange,
	}))
	require.NoError(t, err, "RENAME_EXCHANGE 应成功互换两个节点")

	// 验证互换后：原 mknodRenamedName 对应 createFileIno，原 createFileName 对应 mknodIno
	lookupExchanged1, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      mknodRenamedName,
	}))
	require.NoError(t, err)
	assert.Equal(t, createFileIno, lookupExchanged1.Msg.GetInode().GetIno(), "mknod_renamed.txt 现应指向 createFileIno")

	lookupExchanged2, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      createFileName,
	}))
	require.NoError(t, err)
	assert.Equal(t, mknodIno, lookupExchanged2.Msg.GetInode().GetIno(), "create_file.txt 现应指向 mknodIno")

	// 14. Flush 与 Fsync: 校验句柄与同步落盘
	flushResp, err := client.Flush(ctx, connect.NewRequest(&storagev1.FlushRequest{
		Ino: createFileIno,
		Fh:  fh1,
	}))
	require.NoError(t, err, "Flush 应成功")
	require.NotNil(t, flushResp)

	fsyncResp, err := client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		Ino:      createFileIno,
		Fh:       fh1,
		Datasync: false,
	}))
	require.NoError(t, err, "Fsync 应成功")
	require.NotNil(t, fsyncResp)

	// 15. Unlink 与 unlink-whilst-open (打开状态下的孤儿文件)
	// 目前 createFileIno 存在于两个目录项: mknodRenamedName 与 hardlinkName (nlink = 2)
	// 15.1 解链第一个目录项 mknodRenamedName
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      mknodRenamedName,
	}))
	require.NoError(t, err, "Unlink 首次解链应成功")
	getAttrUnlinked1, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: createFileIno,
	}))
	require.NoError(t, err)
	assert.Equal(t, uint32(1), getAttrUnlinked1.Msg.GetInode().GetAttr().GetNlink(), "解链一次后硬链接数应为 1")

	// 15.2 解链第二个目录项 hardlinkName (此时硬链接数归 0，但 fh1 保持打开，进入 unlink-whilst-open 状态)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      hardlinkName,
	}))
	require.NoError(t, err, "Unlink 第二次解链应成功")

	// 验证：通过 Lookup 无法再查找到该文件
	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: dirIno,
		Name:      hardlinkName,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	// 验证：通过 Inode 进行 GetAttr 依然能成功获取孤儿属性，且 nlink 为 0
	getAttrOrphan, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: createFileIno,
	}))
	require.NoError(t, err, "unlink-whilst-open 状态下 GetAttr 必须依然可用")
	assert.Equal(t, uint32(0), getAttrOrphan.Msg.GetInode().GetAttr().GetNlink(), "孤儿文件 nlink 必须为 0")

	// 验证：孤儿文件因句柄 fh1 仍处于打开状态，配额尚未物理释放 (恰好为 fallocate 的 16384 字节)
	statOrphan, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err)
	assert.Equal(t, uint64(16384), statOrphan.Msg.GetUsedBytes(), "打开中的孤儿文件配额尚未回收")

	// 16. 清理其余子项并验证 Rmdir
	// 尝试对尚有子项 (createFileName 对应的 mknodIno 及 symlinkName) 的目录调用 Rmdir 必须返回 ENOTEMPTY
	_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: rootIno,
		Name:      dirName,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "ENOTEMPTY")

	// 清理剩余子项
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      createFileName,
	}))
	require.NoError(t, err)

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      symlinkName,
	}))
	require.NoError(t, err)

	// 目录内条目全清（虽有孤儿文件挂在内存/会话中但无可见 entry），执行 Rmdir 应成功
	rmdirResp, err := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: rootIno,
		Name:      dirName,
	}))
	require.NoError(t, err, "清空后 Rmdir 应成功")
	require.NotNil(t, rmdirResp)

	// 17. Release: 关闭句柄并验证孤儿文件物理回收与 StatFS 配额完全释放
	// 释放 fh2 (mknodIno 关联句柄)
	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino: mknodIno,
		Fh:  fh2,
	}))
	require.NoError(t, err, "Release fh2 应成功")

	// 释放 fhTmp (匿名临时文件句柄)
	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino: tmpFileIno,
		Fh:  fhTmp,
	}))
	require.NoError(t, err, "Release fhTmp 应成功")

	// 释放 fh1 (最后一个打开 createFileIno 的句柄，触发物理级联删除与配额归还)
	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino: createFileIno,
		Fh:  fh1,
	}))
	require.NoError(t, err, "Release fh1 应成功")

	// 再次 GetAttr 验证孤儿节点已物理回收，返回 ENOENT
	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: createFileIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino: tmpFileIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	// 最终验证 StatFS 配额归还：所有分配的空间与 Inode 均已归还，只保留根目录
	statFinal, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: rootIno}))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), statFinal.Msg.GetUsedBytes(), "所有临时文件与目录销毁后已用字节数必须归零")
	assert.Equal(t, uint64(1), statFinal.Msg.GetFiles(), "仅剩根目录，已用 Inode 数量应为 1")
}

// 确保 math 引用不报错
var _ = math.MaxInt64
