package store_test

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func openTestActor(t *testing.T, opts *store.Options) *store.StoreActor {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	if opts.DiskID == "" {
		opts.DiskID = "test-disk"
	}
	if opts.MaxBytes == 0 && !opts.ReadOnly {
		opts.MaxBytes = 64 << 20
	}
	actor, err := store.Open(*opts)
	require.NoErrorf(t, err, "打开 StoreActor 失败: dir=%s diskID=%s", opts.Dir, opts.DiskID)
	t.Cleanup(func() {
		_ = actor.Close()
	})
	return actor
}

func requireDirEntryNames(t *testing.T, entries []store.DirEntry, wantNames []string) {
	t.Helper()
	gotNames := make([]string, len(entries))
	for i, e := range entries {
		gotNames[i] = string(e.Name)
	}
	require.Equalf(t, wantNames, gotNames, "ReadDir 目录项名称序不符合预期: want=%v got=%v", wantNames, gotNames)
}

func requireDirEntryPlusNames(t *testing.T, entries []store.DirEntryPlus, wantNames []string) {
	t.Helper()
	gotNames := make([]string, len(entries))
	for i := range entries {
		e := &entries[i]
		gotNames[i] = string(e.Name)
		require.Equalf(t, e.Ino, e.Attr.Ino, "ReadDirPlus 项 %q 属性节点号不一致: want=%d got=%d", string(e.Name), e.Ino, e.Attr.Ino)
		require.Equalf(t, e.Mode, e.Attr.Mode, "ReadDirPlus 项 %q 权限模式不一致: want=%o got=%o", string(e.Name), e.Mode, e.Attr.Mode)
	}
	require.Equalf(t, wantNames, gotNames, "ReadDirPlus 目录项名称序不符合预期: want=%v got=%v", wantNames, gotNames)
}

func TestGivenOpenOptionsWhenInvalidThenReturnsExpectedError(t *testing.T) {
	// Given: 非法打开参数（空目录、只读打开未初始化目录）
	emptyDir := t.TempDir()

	// When & Then: 空 Dir 返回 ErrInvalid
	_, err := store.Open(store.Options{Dir: ""})
	require.ErrorIsf(t, err, store.ErrInvalid, "空 Dir 打开应返回 ErrInvalid: got=%v", err)

	// When & Then: ReadOnly 打开未初始化目录返回错误（Pebble 数据库不存在）
	_, err = store.Open(store.Options{Dir: emptyDir, DiskID: "ro-empty", ReadOnly: true})
	require.Errorf(t, err, "只读打开未初始化目录应返回错误")
	require.Containsf(t, err.Error(), "database does not exist", "只读空目录错误信息应标明数据库不存在: got=%v", err)
}

func TestGivenFreshStoreWhenOpenedThenRootAndStatFSInitialized(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given & When: 首次可写打开空目录
	actor := openTestActor(t, &store.Options{
		Dir:      dir,
		DiskID:   "fresh-init",
		MaxBytes: 16 << 20,
	})

	// Then: 根目录 Inode(1) 与 StatFS 初始契约就绪
	require.Equalf(t, dir, actor.Dir(), "Dir() 返回不一致: want=%s got=%s", dir, actor.Dir())
	require.Falsef(t, actor.ReadOnly(), "可写实例 ReadOnly() 应为 false")

	rootAttr, err := actor.GetAttr(ctx, store.RootIno)
	require.NoErrorf(t, err, "GetAttr(RootIno) 失败: %v", err)
	require.Equalf(t, store.RootIno, rootAttr.Ino, "根节点编号不符: want=%d got=%d", store.RootIno, rootAttr.Ino)
	require.Equalf(t, store.S_IFDIR|0o755, rootAttr.Mode, "根节点模式不符: want=%o got=%o", store.S_IFDIR|0o755, rootAttr.Mode)
	require.Equalf(t, uint32(2), rootAttr.Nlink, "根节点初始链接数不符: want=2 got=%d", rootAttr.Nlink)
	require.Equalf(t, store.DefaultDirSize, rootAttr.Size, "目录展示大小不符: want=%d got=%d", store.DefaultDirSize, rootAttr.Size)
	require.Equalf(t, store.DefaultDirSize/512, rootAttr.Blocks, "目录 512B 块数不符: want=%d got=%d", store.DefaultDirSize/512, rootAttr.Blocks)

	st, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS 失败: %v", err)
	require.Equalf(t, uint64(16<<20), st.TotalBytes, "StatFS TotalBytes 不符: want=%d got=%d", uint64(16<<20), st.TotalBytes)
	require.Zero(t, st.UsedBytes, "空盘 UsedBytes 应为 0: got=%d", st.UsedBytes)
	require.Equalf(t, uint64(16<<20), st.AvailBytes, "空盘 AvailBytes 不符: want=%d got=%d", uint64(16<<20), st.AvailBytes)
	require.Equalf(t, uint64(1), st.Files, "空盘 Files 应为 1（仅根目录）: got=%d", st.Files)
	require.Equalf(t, uint32(store.MaxFileNameBytes), st.Namelen, "Namelen 不符: want=%d got=%d", store.MaxFileNameBytes, st.Namelen)
}

func TestGivenClosedOrReadOnlyActorWhenMutatingOrReadingThenRejectsWithLifecycleError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 先初始化有效目录并关闭，分别获得已关闭实例与只读实例
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lifecycle"})
	require.NoErrorf(t, actor.Close(), "Close 失败")

	// When & Then: 已关闭实例读写均返回 ErrClosed
	_, err := actor.GetAttr(ctx, store.RootIno)
	require.ErrorIsf(t, err, store.ErrClosed, "关闭后 GetAttr 应返回 ErrClosed: got=%v", err)
	_, err = actor.StatFS(ctx)
	require.ErrorIsf(t, err, store.ErrClosed, "关闭后 StatFS 应返回 ErrClosed: got=%v", err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("after.txt"), Mode: store.S_IFREG | 0o644})
	require.ErrorIsf(t, err, store.ErrClosed, "关闭后 Create 应返回 ErrClosed: got=%v", err)
	_, _, err = actor.Lookup(ctx, store.RootIno, []byte("."))
	require.ErrorIsf(t, err, store.ErrClosed, "关闭后 Lookup 应返回 ErrClosed: got=%v", err)
	_, err = actor.ReadDir(ctx, store.RootIno, 0, 16)
	require.ErrorIsf(t, err, store.ErrClosed, "关闭后 ReadDir 应返回 ErrClosed: got=%v", err)

	// Given: 只读重开同一目录
	roActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lifecycle-ro", ReadOnly: true})
	require.Truef(t, roActor.ReadOnly(), "只读实例 ReadOnly() 应为 true")

	// When & Then: 只读实例可查询，写入全部返回 ErrReadOnly
	_, err = roActor.GetAttr(ctx, store.RootIno)
	require.NoErrorf(t, err, "只读实例 GetAttr 不应失败: %v", err)
	_, err = roActor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ro.txt"), Mode: store.S_IFREG | 0o644})
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读 Create 应返回 ErrReadOnly: got=%v", err)
	_, err = roActor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("d"), Mode: store.S_IFDIR | 0o755})
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读 Mkdir 应返回 ErrReadOnly: got=%v", err)
	_, err = roActor.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("n"), Mode: store.S_IFIFO | 0o600})
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读 Mknod 应返回 ErrReadOnly: got=%v", err)
	_, err = roActor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("s"), Target: []byte("/a")})
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读 Symlink 应返回 ErrReadOnly: got=%v", err)
	_, err = roActor.Link(ctx, store.LinkReq{Ino: store.RootIno, NewParentIno: store.RootIno, NewName: []byte("l")})
	require.ErrorIsf(t, err, store.ErrReadOnly, "只读 Link 应返回 ErrReadOnly: got=%v", err)
	require.NoErrorf(t, roActor.FlushWAL(), "只读 FlushWAL 应无害返回 nil")
}

func TestGivenEmptyRootWhenCreatingNodesThenLookupAndPersistenceMatchContract(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "create-nodes"})

	// Given: 创建带 S_ISGID 的父目录
	dirAttr, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sgid-dir"),
		Mode:      store.S_IFDIR | store.S_ISGID | 0o755,
		Umask:     0o022,
		UID:       1000,
		GID:       2000,
	})
	require.NoErrorf(t, err, "Mkdir 失败: %v", err)
	require.Equalf(t, store.S_IFDIR|store.S_ISGID|0o755, dirAttr.Mode, "目录模式不符")
	require.Equalf(t, uint32(2), dirAttr.Nlink, "新建目录 Nlink 应为 2: got=%d", dirAttr.Nlink)

	rootAfterMkdir, err := actor.GetAttr(ctx, store.RootIno)
	require.NoError(t, err)
	require.Equalf(t, uint32(3), rootAfterMkdir.Nlink, "父目录创建子目录后 Nlink 应递增为 3: got=%d", rootAfterMkdir.Nlink)

	// When: 在 S_ISGID 目录下创建子目录、常规文件、字符设备、FIFO 与符号链接
	subDirAttr, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("child-dir"),
		Mode:      store.S_IFDIR | 0o750,
		UID:       1001,
		GID:       3000,
	})
	require.NoError(t, err)
	require.Equalf(t, uint32(2000), subDirAttr.GID, "S_ISGID 父目录下子目录必须继承 GID=2000: got=%d", subDirAttr.GID)
	require.Equalf(t, store.S_IFDIR|store.S_ISGID|0o750, subDirAttr.Mode, "S_ISGID 父目录下子目录必须继承 S_ISGID 位: got=%o", subDirAttr.Mode)

	fileAttr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("app.bin"),
		Mode:      store.S_IFREG | 0o666,
		Umask:     0o022,
		UID:       1001,
		GID:       3000,
	})
	require.NoError(t, err)
	require.Equalf(t, store.S_IFREG|0o644, fileAttr.Mode, "Create 应用 umask 022 后权限应为 0644: got=%o", fileAttr.Mode)
	require.Equalf(t, uint32(2000), fileAttr.GID, "S_ISGID 父目录下新建文件必须继承 GID=2000: got=%d", fileAttr.GID)

	chrAttr, err := actor.Mknod(ctx, store.MknodReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("tty0"),
		Mode:      store.S_IFCHR | 0o660,
		Rdev:      0x0400,
	})
	require.NoError(t, err)
	require.Equalf(t, uint32(0x0400), chrAttr.Rdev, "字符设备 Rdev 必须保留: want=0x0400 got=0x%x", chrAttr.Rdev)

	fifoAttr, err := actor.Mknod(ctx, store.MknodReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("pipe0"),
		Mode:      store.S_IFIFO | 0o600,
		Rdev:      0x9999,
	})
	require.NoError(t, err)
	require.Zero(t, fifoAttr.Rdev, "FIFO 节点暴露属性 Rdev 必须为 0: got=0x%x", fifoAttr.Rdev)

	symAttr, err := actor.Symlink(ctx, store.SymlinkReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("link-app"),
		Target:    []byte("app.bin"),
		UID:       1001,
		GID:       3000,
	})
	require.NoError(t, err)
	require.Equalf(t, store.S_IFLNK|0o777, symAttr.Mode, "Symlink 模式应为 S_IFLNK|0777: got=%o", symAttr.Mode)
	require.Equalf(t, uint64(len("app.bin")), symAttr.Size, "Symlink Size 应等于目标路径长度: want=%d got=%d", len("app.bin"), symAttr.Size)

	// When: 为常规文件创建跨目录硬链接
	linkAttr, err := actor.Link(ctx, store.LinkReq{
		Ino:          fileAttr.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("app-hardlink.bin"),
	})
	require.NoError(t, err)
	require.Equalf(t, uint32(2), linkAttr.Nlink, "创建硬链接后 Nlink 应增至 2: got=%d", linkAttr.Nlink)

	// Then: Close 并重新打开后，所有节点、Lookup(. / ..)、Readlink 与 StatFS 完整持久
	require.NoError(t, actor.Close())
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "create-nodes"})

	dotEntry, dotAttr, err := reopened.Lookup(ctx, store.RootIno, []byte(".."))
	require.NoError(t, err)
	require.Equalf(t, store.RootIno, dotEntry.Ino, "根目录 Lookup(..) 应返回自身: got=%d", dotEntry.Ino)
	require.Equalf(t, store.RootIno, dotAttr.Ino, "根目录 Lookup(..) 属性应为 RootIno")

	parentEntry, _, err := reopened.Lookup(ctx, dirAttr.Ino, []byte(".."))
	require.NoError(t, err)
	require.Equalf(t, store.RootIno, parentEntry.Ino, "子目录 Lookup(..) 应返回父目录 RootIno: got=%d", parentEntry.Ino)

	selfEntry, _, err := reopened.Lookup(ctx, dirAttr.Ino, []byte("."))
	require.NoError(t, err)
	require.Equalf(t, dirAttr.Ino, selfEntry.Ino, "子目录 Lookup(.) 应返回自身 Ino: got=%d", selfEntry.Ino)

	gotTarget, err := reopened.Readlink(ctx, symAttr.Ino)
	require.NoError(t, err)
	require.Equalf(t, []byte("app.bin"), gotTarget, "Readlink 返回不符: want=%q got=%q", "app.bin", string(gotTarget))

	st, err := reopened.StatFS(ctx)
	require.NoError(t, err)
	require.Equalf(t, uint64(7), st.Files, "节点总数应为 7 (root+sgid-dir+child-dir+app+tty0+pipe0+symlink): got=%d", st.Files)
	require.Equalf(t, uint64(len("app.bin")), st.UsedBytes, "UsedBytes 应等于 symlink 目标长度: want=%d got=%d", len("app.bin"), st.UsedBytes)
}

func TestGivenPopulatedDirWhenReadDirAndReadDirPlusPaginatedThenOrderAndAttributesMatch(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "readdir-page"})

	// Given: 创建子目录与多个有序文件（故意按非字典序创建，验证返回按字节字典序排序）
	subDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("sub"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)

	fCharly, err := actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("charlie"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("alpha"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("bravo"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// When: 分页调用 ReadDir 与 ReadDirPlus（每页 2 条，依次推进 NextOffset）
	page1, err := actor.ReadDir(ctx, subDir.Ino, 0, 2)
	require.NoError(t, err)
	requireDirEntryNames(t, page1, []string{".", ".."})
	require.Equalf(t, uint64(2), page1[1].NextOffset, "ReadDir 第 1 页末尾 NextOffset 应为 2: got=%d", page1[1].NextOffset)

	page2, err := actor.ReadDir(ctx, subDir.Ino, page1[1].NextOffset, 2)
	require.NoError(t, err)
	requireDirEntryNames(t, page2, []string{"alpha", "bravo"})
	require.Equalf(t, uint64(4), page2[1].NextOffset, "ReadDir 第 2 页末尾 NextOffset 应为 4: got=%d", page2[1].NextOffset)

	page3, err := actor.ReadDir(ctx, subDir.Ino, page2[1].NextOffset, 2)
	require.NoError(t, err)
	requireDirEntryNames(t, page3, []string{"charlie"})
	require.Equalf(t, fCharly.Ino, page3[0].Ino, "charlie 节点号不符")

	// When & Then: ReadDirPlus 全量读取并验证属性与节点号一致
	plusAll, err := actor.ReadDirPlus(ctx, subDir.Ino, 0, 10)
	require.NoError(t, err)
	requireDirEntryPlusNames(t, plusAll, []string{".", "..", "alpha", "bravo", "charlie"})
	require.Equalf(t, subDir.Ino, plusAll[0].Ino, ". 项应指向 subDir")
	require.Equalf(t, store.RootIno, plusAll[1].Ino, ".. 项应指向父目录 RootIno")

	// When & Then: 非法参数校验（maxEntries=0、超过上限、非目录调用 ReadDir/ReadDirPlus）
	_, err = actor.ReadDir(ctx, subDir.Ino, 0, 0)
	require.ErrorIsf(t, err, store.ErrInvalid, "maxEntries=0 应返回 ErrInvalid: got=%v", err)
	_, err = actor.ReadDir(ctx, subDir.Ino, 0, store.MaxReadDirLimit+1)
	require.ErrorIsf(t, err, store.ErrInvalid, "超过 MaxReadDirLimit 应返回 ErrInvalid: got=%v", err)
	_, err = actor.ReadDir(ctx, fCharly.Ino, 0, 10)
	require.ErrorIsf(t, err, store.ErrNotDir, "常规文件 ReadDir 应返回 ErrNotDir: got=%v", err)
	_, err = actor.ReadDirPlus(ctx, fCharly.Ino, 0, 10)
	require.ErrorIsf(t, err, store.ErrNotDir, "常规文件 ReadDirPlus 应返回 ErrNotDir: got=%v", err)
}

func TestGivenNamespaceValidationCasesWhenCalledThenRejectsWithExpectedErrors(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "ns-validation"})

	existingFile, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("reg.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	subDir, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dir"),
		Mode:      store.S_IFDIR | 0o755,
	})
	require.NoError(t, err)

	longName := bytes.Repeat([]byte("a"), store.MaxFileNameBytes+1)
	longTarget := bytes.Repeat([]byte("b"), store.MaxSymlinkTargetBytes+1)

	cases := []struct {
		name string
		run  func() error
		want error
	}{
		{
			name: "Create_空名称返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte(""), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_含斜杠名称返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("a/b"), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_含NUL字节名称返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("a\x00b"), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_超长名称返回ErrNameTooLong",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: longName, Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrNameTooLong,
		},
		{
			name: "Create_携带OTmpfile返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("tmp"), Mode: store.S_IFREG | 0o644, Flags: store.O_TMPFILE})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_父节点非目录返回ErrNotDir",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: existingFile.Ino, Name: []byte("x"), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrNotDir,
		},
		{
			name: "Create_重名返回ErrExist",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("reg.txt"), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrExist,
		},
		{
			name: "Create_点名称返回ErrExist",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("."), Mode: store.S_IFREG | 0o644})
				return e
			},
			want: store.ErrExist,
		},
		{
			name: "Mknod_目录类型返回ErrInvalid",
			run: func() error {
				_, e := actor.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("bad"), Mode: store.S_IFDIR | 0o755})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Mknod_UID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("bad-uid"), Mode: store.S_IFREG | 0o644, UID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Mknod_GID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("bad-gid"), Mode: store.S_IFREG | 0o644, GID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Mkdir_UID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("bad-dir-uid"), Mode: store.S_IFDIR | 0o755, UID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Mkdir_GID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("bad-dir-gid"), Mode: store.S_IFDIR | 0o755, GID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_UID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("bad-file-uid"), Mode: store.S_IFREG | 0o644, UID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Create_GID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("bad-file-gid"), Mode: store.S_IFREG | 0o644, GID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Symlink_UID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("bad-sym-uid"), Target: []byte("/a"), UID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Symlink_GID越界返回ErrInvalid",
			run: func() error {
				_, e := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("bad-sym-gid"), Target: []byte("/a"), GID: math.MaxInt32 + 1})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Symlink_空目标返回ErrInvalid",
			run: func() error {
				_, e := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("sym"), Target: nil})
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Symlink_超长目标返回ErrNameTooLong",
			run: func() error {
				_, e := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("sym"), Target: longTarget})
				return e
			},
			want: store.ErrNameTooLong,
		},
		{
			name: "Link_硬链接目录返回ErrPerm",
			run: func() error {
				_, e := actor.Link(ctx, store.LinkReq{Ino: subDir.Ino, NewParentIno: store.RootIno, NewName: []byte("dir-link")})
				return e
			},
			want: store.ErrPerm,
		},
		{
			name: "Link_目标不存在返回ErrNotFound",
			run: func() error {
				_, e := actor.Link(ctx, store.LinkReq{Ino: 999999, NewParentIno: store.RootIno, NewName: []byte("missing-link")})
				return e
			},
			want: store.ErrNotFound,
		},
		{
			name: "Readlink_常规文件返回ErrInvalid",
			run: func() error {
				_, e := actor.Readlink(ctx, existingFile.Ino)
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Lookup_非法节点号0返回ErrInvalid",
			run: func() error {
				_, _, e := actor.Lookup(ctx, 0, []byte("reg.txt"))
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Lookup_越界节点号返回ErrInvalid",
			run: func() error {
				_, _, e := actor.Lookup(ctx, uint64(math.MaxInt64)+1, []byte("reg.txt"))
				return e
			},
			want: store.ErrInvalid,
		},
		{
			name: "Lookup_空名称返回ErrNotFound",
			run: func() error {
				_, _, e := actor.Lookup(ctx, store.RootIno, []byte(""))
				return e
			},
			want: store.ErrNotFound,
		},
		{
			name: "Lookup_不存在项返回ErrNotFound",
			run: func() error {
				_, _, e := actor.Lookup(ctx, store.RootIno, []byte("nope"))
				return e
			},
			want: store.ErrNotFound,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.run()
			require.ErrorIsf(t, got, tc.want, "用例 %s 期望错误 %v，实际 %v", tc.name, tc.want, got)
		})
	}
}

func TestGivenQuotaLimitsWhenExceededOnCreateOrSymlinkThenReturnsErrNoSpace(t *testing.T) {
	ctx := context.Background()

	// Given: MaxInodes=2 且 MaxBytes=16 的受限实例（根目录已占 1 个 inode）
	actor := openTestActor(t, &store.Options{
		DiskID:    "ns-quota",
		MaxBytes:  16,
		MaxInodes: 2,
	})

	// When: 创建一个超长目标的符号链接超出 MaxBytes=16
	_, err := actor.Symlink(ctx, store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("big-sym"),
		Target:    []byte(strings.Repeat("x", 17)),
	})
	// Then: 返回 ErrNoSpace
	require.ErrorIsf(t, err, store.ErrNoSpace, "超出 MaxBytes 配额应返回 ErrNoSpace: got=%v", err)

	// When: 创建一个合法文件耗尽 MaxInodes=2，再尝试创建第三个节点
	_, err = actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("one.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	_, err = actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("two.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	// Then: 返回 ErrNoSpace
	require.ErrorIsf(t, err, store.ErrNoSpace, "超出 MaxInodes 配额应返回 ErrNoSpace: got=%v", err)
}
