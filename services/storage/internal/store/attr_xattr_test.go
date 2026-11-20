package store_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireXattrNames(t *testing.T, got [][]byte, want []string) {
	t.Helper()
	gotStrings := make([]string, len(got))
	for i, name := range got {
		gotStrings[i] = string(name)
	}
	require.Equalf(t, want, gotStrings, "ListXattr 属性名列表不符: want=%v got=%v", want, gotStrings)
}

func fillXattrListQuota(t *testing.T, actor *store.StoreActor, ino uint64) {
	t.Helper()
	ctx := context.Background()

	for i := range 256 {
		prefix := []byte{'u', 's', 'e', 'r', '.', byte('a' + (i / 26)), byte('a' + (i % 26)), '.'}
		name := make([]byte, store.MaxXattrNameBytes)
		copy(name, prefix)
		copy(name[len(prefix):], bytes.Repeat([]byte{'k'}, store.MaxXattrNameBytes-len(prefix)))
		require.NoErrorf(t, actor.SetXattr(ctx, ino, name, []byte("v"), 0), "填充第 %d 条 xattr 失败", i)
	}
}

func TestGivenRegularFileWhenSetAttrModeUIDGIDSizeAndFlagsThenContractsEnforced(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "setattr-contract"})

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o755,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("abcdef"))
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("security.capability"), []byte{1, 2, 3}, 0))

	uid := uint32(1000)
	gid := uint32(2000)
	afterOwner, err := actor.SetAttr(ctx, f.Ino, store.SetAttrReq{UID: &uid, GID: &gid})
	require.NoError(t, err)

	require.Equalf(t, store.S_IFREG|0o755, afterOwner.Mode, "变更 UID/GID 后必须剥离 S_ISUID|S_ISGID: got=%o", afterOwner.Mode)
	require.Equalf(t, uint32(1000), afterOwner.UID, "UID 未更新")
	require.Equalf(t, uint32(2000), afterOwner.GID, "GID 未更新")
	_, err = actor.GetXattr(ctx, f.Ino, []byte("security.capability"))
	require.ErrorIsf(t, err, store.ErrNoData, "变更属主后必须清除 security.capability: got=%v", err)

	shrink := uint64(3)
	afterShrink, err := actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Size: &shrink})
	require.NoError(t, err)
	require.Equalf(t, uint64(3), afterShrink.Size, "截断后 Size 应为 3: got=%d", afterShrink.Size)

	expand := uint64(10)
	afterExpand, err := actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Size: &expand})
	require.NoError(t, err)
	require.Equalf(t, uint64(10), afterExpand.Size, "扩容后 Size 应为 10: got=%d", afterExpand.Size)
	gotBytes, err := actor.Read(ctx, f.Ino, 0, 10)
	require.NoError(t, err)
	require.Equalf(t, []byte{'a', 'b', 'c', 0, 0, 0, 0, 0, 0, 0}, gotBytes, "截断再扩容后旧尾字节不得复活")

	appendFlag := store.FlagAppend
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Flags: &appendFlag})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("z"))
	require.ErrorIsf(t, err, store.ErrPerm, "FlagAppend 禁止非 EOF 位置写入: got=%v", err)
	zeroSize := uint64(0)
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Size: &zeroSize})
	require.ErrorIsf(t, err, store.ErrPerm, "FlagAppend 禁止 SetAttr Size: got=%v", err)
	_, afterAppend, err := actor.Write(ctx, f.Ino, 10, []byte("tail"))
	require.NoError(t, err)
	require.Equalf(t, uint64(14), afterAppend.Size, "FlagAppend 允许在 EOF 写入: got=%d", afterAppend.Size)

	immutFlag := store.FlagImmutable
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Flags: &immutFlag})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 14, []byte("x"))
	require.ErrorIsf(t, err, store.ErrPerm, "FlagImmutable 禁止写入: got=%v", err)
	newMode := uint32(0o600)
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Mode: &newMode})
	require.ErrorIsf(t, err, store.ErrPerm, "FlagImmutable 禁止修改 Mode: got=%v", err)

	clearFlags := uint32(0)
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Flags: &clearFlags})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 14, []byte("ok"))
	require.NoError(t, err)
}

func TestGivenInvalidSetAttrRequestsWhenCalledThenReturnsExpectedErrors(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "setattr-errors", MaxBytes: 64})

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	sym, err := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("s"), Target: []byte("/t")})
	require.NoError(t, err)

	now := time.Unix(1700000000, 0)
	tooLargeSize := uint64(128)
	dirMode := store.S_IFDIR | 0o755
	symMode := uint32(0o755)
	zeroSize := uint64(0)

	cases := []struct {
		name string
		ino  uint64
		req  store.SetAttrReq
		want error
	}{
		{name: "空请求返回ErrInvalid", ino: f.Ino, req: store.SetAttrReq{}, want: store.ErrInvalid},
		{name: "Atime与AtimeNow冲突返回ErrInvalid", ino: f.Ino, req: store.SetAttrReq{Atime: &now, AtimeNow: true}, want: store.ErrInvalid},
		{name: "Mtime与MtimeNow冲突返回ErrInvalid", ino: f.Ino, req: store.SetAttrReq{Mtime: &now, MtimeNow: true}, want: store.ErrInvalid},
		{name: "目录截断返回ErrIsDir", ino: store.RootIno, req: store.SetAttrReq{Size: &zeroSize}, want: store.ErrIsDir},
		{name: "符号链接截断返回ErrInvalid", ino: sym.Ino, req: store.SetAttrReq{Size: &zeroSize}, want: store.ErrInvalid},
		{name: "符号链接改Mode返回ErrInvalid", ino: sym.Ino, req: store.SetAttrReq{Mode: &symMode}, want: store.ErrInvalid},
		{name: "常规文件改文件类型为目录返回ErrInvalid", ino: f.Ino, req: store.SetAttrReq{Mode: &dirMode}, want: store.ErrInvalid},
		{name: "扩容超MaxBytes返回ErrNoSpace", ino: f.Ino, req: store.SetAttrReq{Size: &tooLargeSize}, want: store.ErrNoSpace},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := actor.SetAttr(ctx, tc.ino, tc.req)
			require.ErrorIsf(t, got, tc.want, "用例 %s 期望 %v，实际 %v", tc.name, tc.want, got)
		})
	}
}

func TestGivenVirtualClockAndAtimeModesWhenReadingThenRelAtimeAndStrictAtimePersistCorrectly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	curTime := time.Unix(1700000000, 0)
	nowFn := func() time.Time { return curTime }

	actor := openTestActor(t, &store.Options{
		Dir:       dir,
		DiskID:    "atime-rel",
		AtimeMode: store.AtimeModeRelAtime,
		Now:       nowFn,
	})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("rel.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("hello"))
	require.NoError(t, err)

	curTime = curTime.Add(time.Hour)
	_, err = actor.Read(ctx, f.Ino, 0, 5)
	require.NoError(t, err)
	attr1, err := actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, curTime.UnixNano(), attr1.Atime, "RelAtime 首次读取(atime<=mtime)应更新 Atime")

	firstUpdatedAtime := curTime.UnixNano()
	curTime = curTime.Add(time.Hour)
	_, err = actor.Read(ctx, f.Ino, 0, 5)
	require.NoError(t, err)
	attr2, err := actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, firstUpdatedAtime, attr2.Atime, "RelAtime 未满 24h 且 atime>mtime 时读取不得更新 Atime")

	curTime = curTime.Add(24 * time.Hour)
	_, err = actor.Read(ctx, f.Ino, 0, 5)
	require.NoError(t, err)
	expectedPersisted := curTime.UnixNano()
	require.NoError(t, actor.Close())

	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "atime-rel", AtimeMode: store.AtimeModeStrictAtime, Now: nowFn})
	attrReopened, err := reopened.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, expectedPersisted, attrReopened.Atime, "Close 后 dirtyAtime 必须刷盘持久")

	curTime = curTime.Add(time.Second)
	_, err = reopened.Read(ctx, f.Ino, 0, 5)
	require.NoError(t, err)
	attrStrict, err := reopened.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, curTime.UnixNano(), attrStrict.Atime, "StrictAtime 必须每次读取刷新 Atime")
	require.NoError(t, reopened.CloseDiscard())

	afterDiscard := openTestActor(t, &store.Options{Dir: dir, DiskID: "atime-rel"})
	attrAfterDiscard, err := afterDiscard.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, expectedPersisted, attrAfterDiscard.Atime, "CloseDiscard 不得刷入未落盘的尾部 dirtyAtime")
}

func TestGivenNodeWhenXattrSetListedAndRemovedThenFlagsNamespaceAndQuotasEnforced(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "xattr-ops"})

	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("doc"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	sym, err := actor.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("sym"), Target: []byte("/doc")})
	require.NoError(t, err)

	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.b"), []byte("val-b"), store.XattrCreate))
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.a"), []byte{}, store.XattrCreate))
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.b"), []byte("dup"), store.XattrCreate), store.ErrExist)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.missing"), []byte("v"), store.XattrReplace), store.ErrNoData)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.b"), []byte("val-b2"), store.XattrReplace))

	gotA, err := actor.GetXattr(ctx, f.Ino, []byte("user.a"))
	require.NoError(t, err)
	require.Empty(t, gotA)
	gotB, err := actor.GetXattr(ctx, f.Ino, []byte("user.b"))
	require.NoError(t, err)
	require.Equal(t, []byte("val-b2"), gotB)

	names, err := actor.ListXattr(ctx, f.Ino)
	require.NoError(t, err)
	requireXattrNames(t, names, []string{"user.a", "user.b"})

	require.NoError(t, actor.RemoveXattr(ctx, f.Ino, []byte("user.a")))
	require.ErrorIs(t, actor.RemoveXattr(ctx, f.Ino, []byte("user.a")), store.ErrNoData)
	_, err = actor.GetXattr(ctx, f.Ino, []byte("user.a"))
	require.ErrorIs(t, err, store.ErrNoData)

	require.ErrorIs(t, actor.SetXattr(ctx, sym.Ino, []byte("user.k"), []byte("v"), 0), store.ErrPerm)
	_, err = actor.GetXattr(ctx, sym.Ino, []byte("user.k"))
	require.ErrorIs(t, err, store.ErrNoData)
	require.NoError(t, actor.SetXattr(ctx, sym.Ino, []byte("trusted.k"), []byte("tv"), 0))

	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte(""), []byte("v"), 0), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.\x00"), []byte("v"), 0), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, bytes.Repeat([]byte("n"), store.MaxXattrNameBytes+1), []byte("v"), 0), store.ErrRange)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.big"), make([]byte, store.MaxXattrValueBytes+1), 0), store.ErrRange)
	require.ErrorIs(t, actor.SetXattr(ctx, f.Ino, []byte("user.flag"), []byte("v"), 0x10), store.ErrInvalid)

	fQuota, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("xquota"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	fillXattrListQuota(t, actor, fQuota.Ino)
	err = actor.SetXattr(ctx, fQuota.Ino, []byte("user.overflow"), []byte("v"), 0)
	require.ErrorIsf(t, err, store.ErrNoSpace, "扩展属性总列表超限应返回 ErrNoSpace: got=%v", err)
}
