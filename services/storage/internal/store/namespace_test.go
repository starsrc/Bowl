package store_test

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"

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

	emptyDir := t.TempDir()

	_, err := store.Open(store.Options{Dir: ""})
	require.ErrorIsf(t, err, store.ErrInvalid, "空 Dir 打开应返回 ErrInvalid: got=%v", err)

	_, err = store.Open(store.Options{Dir: emptyDir, DiskID: "ro-empty", ReadOnly: true})
	require.Errorf(t, err, "只读打开未初始化目录应返回错误")
	require.Containsf(t, err.Error(), "database does not exist", "只读空目录错误信息应标明数据库不存在: got=%v", err)
}

func TestGivenFreshStoreWhenOpenedThenRootAndStatFSInitialized(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	actor := openTestActor(t, &store.Options{
		Dir:      dir,
		DiskID:   "fresh-init",
		MaxBytes: 16 << 20,
	})

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

	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lifecycle"})
	require.NoErrorf(t, actor.Close(), "Close 失败")

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

	roActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "lifecycle-ro", ReadOnly: true})
	require.Truef(t, roActor.ReadOnly(), "只读实例 ReadOnly() 应为 true")

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

	linkAttr, err := actor.Link(ctx, store.LinkReq{
		Ino:          fileAttr.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("app-hardlink.bin"),
	})
	require.NoError(t, err)
	require.Equalf(t, uint32(2), linkAttr.Nlink, "创建硬链接后 Nlink 应增至 2: got=%d", linkAttr.Nlink)

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

	subDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("sub"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)

	fCharly, err := actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("charlie"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("alpha"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: subDir.Ino, Name: []byte("bravo"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

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

	plusAll, err := actor.ReadDirPlus(ctx, subDir.Ino, 0, 10)
	require.NoError(t, err)
	requireDirEntryPlusNames(t, plusAll, []string{".", "..", "alpha", "bravo", "charlie"})
	require.Equalf(t, subDir.Ino, plusAll[0].Ino, ". 项应指向 subDir")
	require.Equalf(t, store.RootIno, plusAll[1].Ino, ".. 项应指向父目录 RootIno")

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

	actor := openTestActor(t, &store.Options{
		DiskID:    "ns-quota",
		MaxBytes:  16,
		MaxInodes: 2,
	})

	_, err := actor.Symlink(ctx, store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("big-sym"),
		Target:    []byte(strings.Repeat("x", 17)),
	})

	require.ErrorIsf(t, err, store.ErrNoSpace, "超出 MaxBytes 配额应返回 ErrNoSpace: got=%v", err)

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

	require.ErrorIsf(t, err, store.ErrNoSpace, "超出 MaxInodes 配额应返回 ErrNoSpace: got=%v", err)
}

func TestGivenLiveDirectoryWhenCreateTmpfileCalledThenAllocatesUnlinkedOrphanWithoutDirEntry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	actor := openTestActor(t, &store.Options{
		Dir:    dir,
		DiskID: "create-tmpfile-live",
		Now:    func() time.Time { return now },
	})

	sgidDir, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sgid"),
		Mode:      store.S_IFDIR | store.S_ISGID | 0o755,
		UID:       1000,
		GID:       2000,
	})
	require.NoErrorf(t, err, "setup mkdir sgid failed")

	rootBefore, err := actor.GetAttr(ctx, store.RootIno)
	require.NoErrorf(t, err, "setup getattr root failed")

	now = now.Add(5 * time.Second)

	linkable, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: sgidDir.Ino,
		Mode:      store.S_IFREG | 0o666,
		Umask:     0o022,
		UID:       1001,
		GID:       3000,
		Exclusive: false,
	})
	require.NoErrorf(t, err, "CreateTmpfile linkable failed")

	exclusive, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      0o640,
		Umask:     0o027,
		UID:       1002,
		GID:       3001,
		Exclusive: true,
	})
	require.NoErrorf(t, err, "CreateTmpfile exclusive failed")

	require.Equalf(t, store.S_IFREG|0o644, linkable.Mode, "linkable mode mismatch: want=%o got=%o", store.S_IFREG|0o644, linkable.Mode)
	require.Zerof(t, linkable.Nlink, "linkable Nlink mismatch: want=0 got=%d", linkable.Nlink)
	require.Zerof(t, linkable.ParentIno, "linkable ParentIno mismatch: want=0 got=%d", linkable.ParentIno)
	require.Zerof(t, linkable.Size, "linkable Size mismatch: want=0 got=%d", linkable.Size)
	require.Zerof(t, linkable.Flags, "linkable Flags mismatch: want=0 got=%d", linkable.Flags)
	require.Equalf(t, uint64(1), linkable.Generation, "linkable Generation mismatch: want=1 got=%d", linkable.Generation)
	require.Equalf(t, uint32(1001), linkable.UID, "linkable UID mismatch: want=1001 got=%d", linkable.UID)
	require.Equalf(t, uint32(2000), linkable.GID, "linkable S_ISGID inherited GID mismatch: want=2000 got=%d", linkable.GID)
	require.Equalf(t, now.UnixNano(), linkable.Mtime, "linkable Mtime mismatch: want=%d got=%d", now.UnixNano(), linkable.Mtime)

	require.Equalf(t, store.S_IFREG|0o640, exclusive.Mode, "exclusive mode mismatch: want=%o got=%o", store.S_IFREG|0o640, exclusive.Mode)
	require.Zerof(t, exclusive.Nlink, "exclusive Nlink mismatch: want=0 got=%d", exclusive.Nlink)
	require.Zerof(t, exclusive.ParentIno, "exclusive ParentIno mismatch: want=0 got=%d", exclusive.ParentIno)
	require.Equalf(t, uint32(3001), exclusive.GID, "exclusive GID mismatch: want=3001 got=%d", exclusive.GID)

	sgidAfter, err := actor.GetAttr(ctx, sgidDir.Ino)
	require.NoErrorf(t, err, "getattr sgid after CreateTmpfile failed")
	require.Equalf(t, sgidDir.Mtime, sgidAfter.Mtime, "parent Mtime changed on CreateTmpfile: want=%d got=%d", sgidDir.Mtime, sgidAfter.Mtime)
	require.Equalf(t, sgidDir.WriteSeq, sgidAfter.WriteSeq, "parent WriteSeq changed on CreateTmpfile: want=%d got=%d", sgidDir.WriteSeq, sgidAfter.WriteSeq)

	rootAfter, err := actor.GetAttr(ctx, store.RootIno)
	require.NoErrorf(t, err, "getattr root after CreateTmpfile failed")
	require.Equalf(t, rootBefore.Mtime, rootAfter.Mtime, "root Mtime changed on CreateTmpfile: want=%d got=%d", rootBefore.Mtime, rootAfter.Mtime)

	hasOrphans, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans failed")
	require.Truef(t, hasOrphans, "HasOrphans must be true after CreateTmpfile: want=true got=%v", hasOrphans)

	st, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS failed")
	require.Equalf(t, uint64(4), st.Files, "StatFS Files mismatch: want=4 got=%d", st.Files)
	require.Zerof(t, st.UsedBytes, "StatFS UsedBytes mismatch: want=0 got=%d", st.UsedBytes)

	sgidEntries, err := actor.ReadDir(ctx, sgidDir.Ino, 0, 16)
	require.NoErrorf(t, err, "ReadDir sgid failed")
	requireDirEntryNames(t, sgidEntries, []string{".", ".."})

	rootEntries, err := actor.ReadDirPlus(ctx, store.RootIno, 0, 16)
	require.NoErrorf(t, err, "ReadDirPlus root failed")
	requireDirEntryPlusNames(t, rootEntries, []string{".", "..", "sgid"})

	require.NoErrorf(t, actor.Close(), "Close actor failed")
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "create-tmpfile-live"})

	gotLinkable, err := reopened.GetAttr(ctx, linkable.Ino)
	require.NoErrorf(t, err, "reopened GetAttr linkable failed")
	require.Zerof(t, gotLinkable.Nlink, "reopened linkable Nlink mismatch: want=0 got=%d", gotLinkable.Nlink)
	require.Equalf(t, linkable.Mode, gotLinkable.Mode, "reopened linkable Mode mismatch: want=%o got=%o", linkable.Mode, gotLinkable.Mode)

	gotExclusive, err := reopened.GetAttr(ctx, exclusive.Ino)
	require.NoErrorf(t, err, "reopened GetAttr exclusive failed")
	require.Zerof(t, gotExclusive.Nlink, "reopened exclusive Nlink mismatch: want=0 got=%d", gotExclusive.Nlink)

	reopenOrphans, err := reopened.HasOrphans()
	require.NoErrorf(t, err, "reopened HasOrphans failed")
	require.Truef(t, reopenOrphans, "reopened HasOrphans must be true")
}

func TestGivenInvalidStateOrArgsWhenCreateTmpfileCalledThenReturnsExpectedErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor := openTestActor(t, &store.Options{
		Dir:       dir,
		DiskID:    "create-tmpfile-errors",
		MaxInodes: 5,
	})

	regFile, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("file.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoErrorf(t, err, "setup create file failed")

	removedDir, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("gone"),
		Mode:      store.S_IFDIR | 0o755,
	})
	require.NoErrorf(t, err, "setup mkdir gone failed")
	require.NoErrorf(t, actor.Rmdir(ctx, store.RootIno, []byte("gone")), "setup rmdir gone failed")

	newEpoch, _, err := actor.AdvanceEpoch(ctx)
	require.NoErrorf(t, err, "setup advance epoch failed")

	unlinkedDir, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("unlinked-dir"),
		Mode:      store.S_IFDIR | 0o755,
		Epoch:     newEpoch,
	})
	require.NoErrorf(t, err, "setup mkdir unlinked-dir failed")
	_, err = actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("repl-dir"),
		Mode:      store.S_IFDIR | 0o755,
		Epoch:     newEpoch,
	})
	require.NoErrorf(t, err, "setup mkdir repl-dir failed")
	require.NoErrorf(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno:     store.RootIno,
		OldName:          []byte("repl-dir"),
		NewParentIno:     store.RootIno,
		NewName:          []byte("unlinked-dir"),
		TargetOpenHandle: true,
		Epoch:            newEpoch,
	}), "setup overwrite unlinked-dir failed")

	cases := []struct {
		name string
		req  store.CreateTmpfileReq
		want error
	}{
		{
			name: "stale_epoch_zero",
			req:  store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: 0},
			want: store.ErrStaleEpoch,
		},
		{
			name: "future_epoch",
			req:  store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: newEpoch + 1},
			want: store.ErrStaleEpoch,
		},
		{
			name: "zero_parent_ino",
			req:  store.CreateTmpfileReq{ParentIno: 0, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrInvalid,
		},
		{
			name: "overflow_parent_ino",
			req:  store.CreateTmpfileReq{ParentIno: uint64(math.MaxInt64) + 1, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrInvalid,
		},
		{
			name: "mode_above_max",
			req:  store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o200000, Epoch: newEpoch},
			want: store.ErrInvalid,
		},
		{
			name: "directory_mode",
			req:  store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: store.S_IFDIR | 0o755, Epoch: newEpoch},
			want: store.ErrInvalid,
		},
		{
			name: "symlink_mode",
			req:  store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: store.S_IFLNK | 0o777, Epoch: newEpoch},
			want: store.ErrInvalid,
		},
		{
			name: "missing_parent",
			req:  store.CreateTmpfileReq{ParentIno: 999999, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrNotFound,
		},
		{
			name: "removed_parent",
			req:  store.CreateTmpfileReq{ParentIno: removedDir.Ino, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrNotFound,
		},
		{
			name: "unlinked_parent",
			req:  store.CreateTmpfileReq{ParentIno: unlinkedDir.Ino, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrNotFound,
		},
		{
			name: "non_dir_parent",
			req:  store.CreateTmpfileReq{ParentIno: regFile.Ino, Mode: 0o600, Epoch: newEpoch},
			want: store.ErrNotDir,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, gotErr := actor.CreateTmpfile(ctx, tc.req)
			require.ErrorIsf(t, gotErr, tc.want, "case %s: want=%v got=%v", tc.name, tc.want, gotErr)
		})
	}

	_, err = actor.CreateTmpfile(ctx, store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: newEpoch})
	require.NoErrorf(t, err, "tmpfile within MaxInodes=5 quota failed")
	_, err = actor.CreateTmpfile(ctx, store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: newEpoch})
	require.ErrorIsf(t, err, store.ErrNoSpace, "tmpfile exceeding MaxInodes must return ErrNoSpace: got=%v", err)

	require.NoErrorf(t, actor.Close(), "close actor failed")
	_, err = actor.CreateTmpfile(ctx, store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: newEpoch})
	require.ErrorIsf(t, err, store.ErrClosed, "closed CreateTmpfile must return ErrClosed: got=%v", err)

	roActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "create-tmpfile-ro", ReadOnly: true})
	_, err = roActor.CreateTmpfile(ctx, store.CreateTmpfileReq{ParentIno: store.RootIno, Mode: 0o600, Epoch: newEpoch})
	require.ErrorIsf(t, err, store.ErrReadOnly, "read-only CreateTmpfile must return ErrReadOnly: got=%v", err)
}

func TestGivenUnpromotedTmpfilesWhenWrittenFallocatedSetAttrAndFsyncedThenContractsHold(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name      string
		exclusive bool
		pattern   byte
	}{
		{
			name:      "linkable_tmpfile",
			exclusive: false,
			pattern:   0x41,
		},
		{
			name:      "exclusive_tmpfile",
			exclusive: true,
			pattern:   0x51,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {

			actor := openTestActor(t, &store.Options{
				DiskID:   "tmpfile-io-" + tc.name,
				MaxBytes: 2 * store.BlobSize,
			})
			tmp, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
				ParentIno: store.RootIno,
				Mode:      store.S_IFREG | 0o644,
				Exclusive: tc.exclusive,
			})
			require.NoErrorf(t, err, "case %s: CreateTmpfile failed", tc.name)

			fullBlock := bytes.Repeat([]byte{tc.pattern}, store.BlobSize)
			n1, attr1, err := actor.Write(ctx, tmp.Ino, 0, fullBlock)
			require.NoErrorf(t, err, "case %s: full-block Write failed", tc.name)
			require.Equalf(t, uint32(store.BlobSize), n1, "case %s: full-block write count mismatch: want=%d got=%d", tc.name, store.BlobSize, n1)
			require.Equalf(t, uint64(store.BlobSize), attr1.Size, "case %s: size after full-block Write mismatch: want=%d got=%d", tc.name, store.BlobSize, attr1.Size)

			partBlock := bytes.Repeat([]byte{tc.pattern + 1}, 512)
			n2, attr2, err := actor.Write(ctx, tmp.Ino, store.BlobSize+64, partBlock)
			require.NoErrorf(t, err, "case %s: partial Write failed", tc.name)
			require.Equalf(t, uint32(512), n2, "case %s: partial write count mismatch: want=512 got=%d", tc.name, n2)
			require.Equalf(t, uint64(store.BlobSize+576), attr2.Size, "case %s: size after partial Write mismatch: want=%d got=%d", tc.name, store.BlobSize+576, attr2.Size)

			gotSpan, err := actor.Read(ctx, tmp.Ino, store.BlobSize-16, 592)
			require.NoErrorf(t, err, "case %s: Read across boundary failed", tc.name)
			wantSpan := make([]byte, 592)
			copy(wantSpan[:16], bytes.Repeat([]byte{tc.pattern}, 16))
			copy(wantSpan[80:], partBlock)
			require.Equalf(t, wantSpan, gotSpan, "case %s: readback mismatch across block boundary", tc.name)

			require.NoErrorf(t, actor.Fallocate(ctx, tmp.Ino, store.BlobSize+576, 1024, 0), "case %s: Fallocate(0) failed", tc.name)
			afterAlloc, err := actor.GetAttr(ctx, tmp.Ino)
			require.NoErrorf(t, err, "case %s: GetAttr after Fallocate(0) failed", tc.name)
			require.Equalf(t, uint64(store.BlobSize+1600), afterAlloc.Size, "case %s: size after Fallocate(0) mismatch: want=%d got=%d", tc.name, store.BlobSize+1600, afterAlloc.Size)

			require.NoErrorf(t, actor.Fallocate(ctx, tmp.Ino, store.BlobSize+1600, 1024, store.FallocKeepSize), "case %s: Fallocate(KeepSize) failed", tc.name)
			afterKeep, err := actor.GetAttr(ctx, tmp.Ino)
			require.NoErrorf(t, err, "case %s: GetAttr after Fallocate(KeepSize) failed", tc.name)
			require.Equalf(t, uint64(store.BlobSize+1600), afterKeep.Size, "case %s: size after Fallocate(KeepSize) mismatch: want=%d got=%d", tc.name, store.BlobSize+1600, afterKeep.Size)

			require.NoErrorf(t, actor.Fallocate(ctx, tmp.Ino, 512, 1024, store.FallocPunchHole|store.FallocKeepSize), "case %s: Fallocate(PunchHole) failed", tc.name)
			gotHole, err := actor.Read(ctx, tmp.Ino, 500, 1048)
			require.NoErrorf(t, err, "case %s: Read punched hole failed", tc.name)
			wantHole := make([]byte, 1048)
			copy(wantHole[:12], bytes.Repeat([]byte{tc.pattern}, 12))
			copy(wantHole[1036:], bytes.Repeat([]byte{tc.pattern}, 12))
			require.Equalf(t, wantHole, gotHole, "case %s: punched hole bytes mismatch", tc.name)

			appendFlag := store.FlagAppend
			_, err = actor.SetAttr(ctx, tmp.Ino, store.SetAttrReq{Flags: &appendFlag})
			require.NoErrorf(t, err, "case %s: SetAttr FlagAppend failed", tc.name)
			_, _, err = actor.Write(ctx, tmp.Ino, 0, []byte("denied"))
			require.ErrorIsf(t, err, store.ErrPerm, "case %s: non-EOF Write under FlagAppend must return ErrPerm: got=%v", tc.name, err)

			tail := bytes.Repeat([]byte{tc.pattern + 2}, 16)
			_, attrAppended, err := actor.Write(ctx, tmp.Ino, store.BlobSize+1600, tail)
			require.NoErrorf(t, err, "case %s: EOF Write under FlagAppend failed", tc.name)
			require.Equalf(t, uint64(store.BlobSize+1616), attrAppended.Size, "case %s: size after EOF append mismatch: want=%d got=%d", tc.name, store.BlobSize+1616, attrAppended.Size)

			newMode := uint32(0o600)
			newUID := uint32(2222)
			newGID := uint32(3333)
			zeroFlags := uint32(0)
			attrUpdated, err := actor.SetAttr(ctx, tmp.Ino, store.SetAttrReq{
				Mode:  &newMode,
				UID:   &newUID,
				GID:   &newGID,
				Flags: &zeroFlags,
			})
			require.NoErrorf(t, err, "case %s: SetAttr owner/mode/clear flags failed", tc.name)
			require.Equalf(t, store.S_IFREG|0o600, attrUpdated.Mode, "case %s: updated Mode mismatch: want=%o got=%o", tc.name, store.S_IFREG|0o600, attrUpdated.Mode)
			require.Equalf(t, uint32(2222), attrUpdated.UID, "case %s: updated UID mismatch: want=2222 got=%d", tc.name, attrUpdated.UID)
			require.Equalf(t, uint32(3333), attrUpdated.GID, "case %s: updated GID mismatch: want=3333 got=%d", tc.name, attrUpdated.GID)
			require.Zerof(t, attrUpdated.Flags, "case %s: cleared Flags mismatch: want=0 got=%d", tc.name, attrUpdated.Flags)
			require.Zerof(t, attrUpdated.Nlink, "case %s: tmpfile Nlink must remain 0: got=%d", tc.name, attrUpdated.Nlink)

			_, _, err = actor.Write(ctx, tmp.Ino, store.BlobSize+1616, bytes.Repeat([]byte{0x77}, store.BlobSize))
			require.ErrorIsf(t, err, store.ErrNoSpace, "case %s: Write exceeding MaxBytes must return ErrNoSpace: got=%v", tc.name, err)

			st, err := actor.StatFS(ctx)
			require.NoErrorf(t, err, "case %s: StatFS failed", tc.name)
			require.Equalf(t, uint64(store.BlobSize+1616), st.UsedBytes, "case %s: StatFS UsedBytes mismatch: want=%d got=%d", tc.name, store.BlobSize+1616, st.UsedBytes)

			require.NoErrorf(t, actor.Fsync(ctx, tmp.Ino), "case %s: Fsync on tmpfile failed", tc.name)
			require.Falsef(t, actor.WALDirty(), "case %s: WALDirty must be false after Fsync", tc.name)
		})
	}
}

func TestGivenExclusiveTmpfileOrUnlinkedOrphanWhenLinkCalledThenReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "link-reject-orphans"})

	exclusiveTmp, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: true,
	})
	require.NoErrorf(t, err, "CreateTmpfile exclusive failed")

	unlinkedFile, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("unlinked.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoErrorf(t, err, "Create unlinked.bin failed")
	require.NoErrorf(t, actor.Unlink(ctx, store.RootIno, []byte("unlinked.bin"), true), "Unlink openHandle failed")

	overwrittenFile, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("target.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoErrorf(t, err, "Create target.bin failed")
	_, err = actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("src.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoErrorf(t, err, "Create src.bin failed")
	require.NoErrorf(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno:     store.RootIno,
		OldName:          []byte("src.bin"),
		NewParentIno:     store.RootIno,
		NewName:          []byte("target.bin"),
		TargetOpenHandle: true,
	}), "Rename overwrite openHandle failed")

	unlinkedFIFO, err := actor.Mknod(ctx, store.MknodReq{
		ParentIno: store.RootIno,
		Name:      []byte("fifo.pipe"),
		Mode:      store.S_IFIFO | 0o600,
	})
	require.NoErrorf(t, err, "Mknod fifo.pipe failed")
	require.NoErrorf(t, actor.Unlink(ctx, store.RootIno, []byte("fifo.pipe"), true), "Unlink fifo.pipe openHandle failed")

	cases := []struct {
		name string
		ino  uint64
	}{
		{name: "exclusive_tmpfile", ino: exclusiveTmp.Ino},
		{name: "unlinked_open_orphan", ino: unlinkedFile.Ino},
		{name: "rename_overwritten_orphan", ino: overwrittenFile.Ino},
		{name: "unlinked_fifo_orphan", ino: unlinkedFIFO.Ino},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, linkErr := actor.Link(ctx, store.LinkReq{
				Ino:          tc.ino,
				NewParentIno: store.RootIno,
				NewName:      []byte("link-" + tc.name),
			})
			require.ErrorIsf(t, linkErr, store.ErrNotFound, "case %s: initial Link must fail with ErrNotFound: got=%v", tc.name, linkErr)

			appendFlag := store.FlagAppend
			_, setErr := actor.SetAttr(ctx, tc.ino, store.SetAttrReq{Flags: &appendFlag})
			require.NoErrorf(t, setErr, "case %s: SetAttr FlagAppend failed", tc.name)

			_, linkAfterFlagErr := actor.Link(ctx, store.LinkReq{
				Ino:          tc.ino,
				NewParentIno: store.RootIno,
				NewName:      []byte("link-after-flag-" + tc.name),
			})
			require.ErrorIsf(t, linkAfterFlagErr, store.ErrNotFound, "case %s: Link after FlagAppend must fail with ErrNotFound: got=%v", tc.name, linkAfterFlagErr)

			zeroFlag := uint32(0)
			_, clearErr := actor.SetAttr(ctx, tc.ino, store.SetAttrReq{Flags: &zeroFlag})
			require.NoErrorf(t, clearErr, "case %s: SetAttr clear flags failed", tc.name)

			_, linkAfterClearErr := actor.Link(ctx, store.LinkReq{
				Ino:          tc.ino,
				NewParentIno: store.RootIno,
				NewName:      []byte("link-after-clear-" + tc.name),
			})
			require.ErrorIsf(t, linkAfterClearErr, store.ErrNotFound, "case %s: Link after clearing flags must fail with ErrNotFound: got=%v", tc.name, linkAfterClearErr)

			_, _, lookupErr := actor.Lookup(ctx, store.RootIno, []byte("link-"+tc.name))
			require.ErrorIsf(t, lookupErr, store.ErrNotFound, "case %s: rejected link entry must not exist", tc.name)
		})
	}
}

func TestGivenLinkableTmpfileWhenPromotedViaLinkThenBecomesLiveFileAndBlocksRelinkAfterUnlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Unix(1_700_000_000, 0)
	actor := openTestActor(t, &store.Options{
		Dir:       dir,
		DiskID:    "tmpfile-promote",
		AtimeMode: store.AtimeModeStrictAtime,
		Now:       func() time.Time { return now },
	})

	_, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("occupied.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoErrorf(t, err, "setup create occupied.bin failed")

	tmp, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o644,
		Exclusive: false,
	})
	require.NoErrorf(t, err, "CreateTmpfile linkable failed")

	_, _, err = actor.Write(ctx, tmp.Ino, 0, bytes.Repeat([]byte{0x61}, store.BlobSize))
	require.NoErrorf(t, err, "full-block Write on tmpfile failed")
	_, _, err = actor.Write(ctx, tmp.Ino, store.BlobSize+128, bytes.Repeat([]byte{0x62}, 256))
	require.NoErrorf(t, err, "partial Write on tmpfile failed")
	require.NoErrorf(t, actor.Fallocate(ctx, tmp.Ino, 512, 1024, store.FallocPunchHole|store.FallocKeepSize), "Fallocate PunchHole failed")
	require.NoErrorf(t, actor.SetXattr(ctx, tmp.Ino, []byte("user.role"), []byte("staged"), 0), "SetXattr failed")

	appendFlag := store.FlagAppend
	_, err = actor.SetAttr(ctx, tmp.Ino, store.SetAttrReq{Flags: &appendFlag})
	require.NoErrorf(t, err, "SetAttr FlagAppend failed")
	zeroFlag := uint32(0)
	_, err = actor.SetAttr(ctx, tmp.Ino, store.SetAttrReq{Flags: &zeroFlag})
	require.NoErrorf(t, err, "SetAttr clear flags failed")

	now = now.Add(10 * time.Second)
	dirtyReadTime := now.UnixNano()
	_, err = actor.Read(ctx, tmp.Ino, 0, 4)
	require.NoErrorf(t, err, "Read for dirtyAtime failed")

	_, err = actor.Link(ctx, store.LinkReq{
		Ino:          tmp.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("occupied.bin"),
	})
	require.ErrorIsf(t, err, store.ErrExist, "Link onto existing name must return ErrExist: got=%v", err)

	stBefore, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS before promotion failed")

	now = now.Add(2 * time.Second)
	promotedAttr, err := actor.Link(ctx, store.LinkReq{
		Ino:          tmp.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("promoted.bin"),
	})
	require.NoErrorf(t, err, "Link promotion on linkable tmpfile failed")

	require.Equalf(t, uint32(1), promotedAttr.Nlink, "promoted Nlink mismatch: want=1 got=%d", promotedAttr.Nlink)
	require.Equalf(t, tmp.Ino, promotedAttr.Ino, "promoted Ino mismatch: want=%d got=%d", tmp.Ino, promotedAttr.Ino)
	require.Equalf(t, uint64(store.BlobSize+384), promotedAttr.Size, "promoted Size mismatch: want=%d got=%d", store.BlobSize+384, promotedAttr.Size)
	require.Equalf(t, dirtyReadTime, promotedAttr.Atime, "promoted Atime must reflect overlaid dirtyAtime: want=%d got=%d", dirtyReadTime, promotedAttr.Atime)
	require.Equalf(t, now.UnixNano(), promotedAttr.Ctime, "promoted Ctime mismatch: want=%d got=%d", now.UnixNano(), promotedAttr.Ctime)

	hasOrphans, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans after promotion failed")
	require.Falsef(t, hasOrphans, "HasOrphans must be false after promotion: got=%v", hasOrphans)

	stAfter, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS after promotion failed")
	require.Equalf(t, stBefore.Files, stAfter.Files, "StatFS Files must not change on promotion: want=%d got=%d", stBefore.Files, stAfter.Files)
	require.Equalf(t, stBefore.UsedBytes, stAfter.UsedBytes, "StatFS UsedBytes must not change on promotion: want=%d got=%d", stBefore.UsedBytes, stAfter.UsedBytes)

	entry, lookedUp, err := actor.Lookup(ctx, store.RootIno, []byte("promoted.bin"))
	require.NoErrorf(t, err, "Lookup promoted.bin failed")
	require.Equalf(t, tmp.Ino, entry.Ino, "Lookup Ino mismatch: want=%d got=%d", tmp.Ino, entry.Ino)
	require.Equalf(t, uint32(1), lookedUp.Nlink, "Lookup Nlink mismatch: want=1 got=%d", lookedUp.Nlink)

	dirEntries, err := actor.ReadDir(ctx, store.RootIno, 0, 16)
	require.NoErrorf(t, err, "ReadDir root failed")
	requireDirEntryNames(t, dirEntries, []string{".", "..", "occupied.bin", "promoted.bin"})

	gotPayload, err := actor.Read(ctx, tmp.Ino, 0, store.BlobSize+384)
	require.NoErrorf(t, err, "Read promoted payload failed")
	wantPayload := make([]byte, store.BlobSize+384)
	copy(wantPayload[:store.BlobSize], bytes.Repeat([]byte{0x61}, store.BlobSize))
	clear(wantPayload[512:1536])
	copy(wantPayload[store.BlobSize+128:], bytes.Repeat([]byte{0x62}, 256))
	require.Equalf(t, wantPayload, gotPayload, "promoted sparse payload mismatch")

	gotXattr, err := actor.GetXattr(ctx, tmp.Ino, []byte("user.role"))
	require.NoErrorf(t, err, "GetXattr after promotion failed")
	require.Equalf(t, []byte("staged"), gotXattr, "xattr mismatch: want=staged got=%q", string(gotXattr))

	secondAttr, err := actor.Link(ctx, store.LinkReq{
		Ino:          tmp.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("second.bin"),
	})
	require.NoErrorf(t, err, "second Link failed")
	require.Equalf(t, uint32(2), secondAttr.Nlink, "second Link Nlink mismatch: want=2 got=%d", secondAttr.Nlink)

	require.ErrorIsf(t, actor.PurgeOrphanInode(ctx, tmp.Ino), store.ErrInvalid, "PurgeOrphanInode on promoted live file must return ErrInvalid")
	require.NoErrorf(t, actor.CleanOrphans(ctx), "CleanOrphans failed")
	_, stillThere, err := actor.Lookup(ctx, store.RootIno, []byte("promoted.bin"))
	require.NoErrorf(t, err, "promoted.bin must survive CleanOrphans")
	require.Equalf(t, uint32(2), stillThere.Nlink, "Nlink after CleanOrphans mismatch: want=2 got=%d", stillThere.Nlink)

	require.NoErrorf(t, actor.Unlink(ctx, store.RootIno, []byte("second.bin"), false), "Unlink second.bin failed")
	require.NoErrorf(t, actor.Unlink(ctx, store.RootIno, []byte("promoted.bin"), true), "Unlink promoted.bin openHandle failed")

	hasOrphansAfterUnlink, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans after open-handle unlink failed")
	require.Truef(t, hasOrphansAfterUnlink, "HasOrphans must be true after open-handle unlink")

	_, err = actor.Link(ctx, store.LinkReq{
		Ino:          tmp.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("relink.bin"),
	})
	require.ErrorIsf(t, err, store.ErrNotFound, "re-linking unlinked open-handle orphan must return ErrNotFound: got=%v", err)

	require.NoErrorf(t, actor.Close(), "Close actor failed")
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "tmpfile-promote"})
	_, err = reopened.Link(ctx, store.LinkReq{
		Ino:          tmp.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("relink.bin"),
	})
	require.ErrorIsf(t, err, store.ErrNotFound, "reopened re-link of unlinked orphan must return ErrNotFound: got=%v", err)
}
