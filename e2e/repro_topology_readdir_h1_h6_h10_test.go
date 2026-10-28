//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func init() {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.MaxIdleConns = 256
		t.MaxIdleConnsPerHost = 128
	}
}

// TestE2E_H1_ConcurrentCrossDirRenameCycle 复现 H1 缺陷：
// 并发跨目录 Rename 绕过防环检查形成目录死循环。
// 场景：在不相交目录对 /X_i/A/B 与 /Y_i/C/D 中，并发执行 Rename(A -> D) 与 Rename(C -> B)。
// 由于双方行锁不重叠且在防环检查时对方尚未提交，导致成环 A -> B -> C -> D -> A 并脱离根目录。
func TestE2E_H1_ConcurrentCrossDirRenameCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 创建独立测试虚拟磁盘
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h1-rename-cycle-disk",
		MaxBytes: 100 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	require.NotNil(t, disk, "新建虚拟磁盘不能为空")
	diskID := disk.GetDiskId()
	rootIno := disk.GetRootIno()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: diskID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("cleanup DeleteDisk failed: %v", err)
		}
	})

	const maxRounds = 60
	for round := 1; round <= maxRounds; round++ {
		// 1. 创建 X_i 分支：/X_i/A/B并在 B 下写入 4096 字节文件
		xName := fmt.Sprintf("X_%d", round)
		xResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      []byte(xName),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 %s 必须成功", xName)
		xIno := xResp.Msg.GetInode().GetIno()

		aResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: xIno,
			Name:      []byte("A"),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 A 必须成功")
		aIno := aResp.Msg.GetInode().GetIno()

		bResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: aIno,
			Name:      []byte("B"),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 B 必须成功")
		bIno := bResp.Msg.GetInode().GetIno()

		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: bIno,
			Name:      []byte("file_in_b.bin"),
			Mode:      sIfReg | 0o644,
			Flags:     oRdWr,
		}))
		require.NoError(t, err, "在 B 下创建常规文件必须成功")
		bFileIno := createResp.Msg.GetInode().GetIno()
		bFileFh := createResp.Msg.GetFh()

		_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
			Ino:    bFileIno,
			Fh:     bFileFh,
			Offset: 0,
			Length: 4096,
			Mode:   0,
		}))
		require.NoError(t, err, "为文件预分配 4096 字节空间必须成功")

		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino: bFileIno,
			Fh:  bFileFh,
		}))
		require.NoError(t, err, "释放文件句柄必须成功")

		// 2. 创建 Y_i 分支：/Y_i/C/D
		yName := fmt.Sprintf("Y_%d", round)
		yResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      []byte(yName),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 %s 必须成功", yName)
		yIno := yResp.Msg.GetInode().GetIno()

		cResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: yIno,
			Name:      []byte("C"),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 C 必须成功")
		cIno := cResp.Msg.GetInode().GetIno()

		dResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: cIno,
			Name:      []byte("D"),
			Mode:      0o755,
		}))
		require.NoError(t, err, "创建目录 D 必须成功")
		dIno := dResp.Msg.GetInode().GetIno()

		// 3. 并发屏障：两个 goroutine 同时发起跨目录 Rename
		var wg sync.WaitGroup
		wg.Add(2)
		startCh := make(chan struct{})

		var err1, err2 error
		go func() {
			defer wg.Done()
			<-startCh
			_, err1 = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
				OldParentIno: xIno,
				OldName:      []byte("A"),
				NewParentIno: dIno,
				NewName:      []byte("A"),
				Flags:        0,
			}))
		}()

		go func() {
			defer wg.Done()
			<-startCh
			_, err2 = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
				OldParentIno: yIno,
				OldName:      []byte("C"),
				NewParentIno: bIno,
				NewName:      []byte("C"),
				Flags:        0,
			}))
		}()

		close(startCh)
		wg.Wait()

		// 4. 正确契约核验：
		successCount := 0
		if err1 == nil {
			successCount++
		} else {
			testutil.RequireConnectError(t, err1, connect.CodeInvalidArgument, "EINVAL")
		}
		if err2 == nil {
			successCount++
		} else {
			testutil.RequireConnectError(t, err2, connect.CodeInvalidArgument, "EINVAL")
		}

		// 独立验证 4 个目录 (aIno, bIno, cIno, dIno) 的父目录链均可达 rootIno
		allReachedRoot := true
		var cyclePath []uint64
		for _, targetIno := range []uint64{aIno, bIno, cIno, dIno} {
			curr := targetIno
			var path []uint64
			reachedRoot := false
			for step := 0; step < 8; step++ {
				path = append(path, curr)
				if curr == rootIno {
					reachedRoot = true
					break
				}
				lookupResp, lErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
					ParentIno: curr,
					Name:      []byte(".."),
				}))
				require.NoError(t, lErr, "追溯父目录链 (Lookup '..') 必须成功: curr=%d", curr)
				require.NotNil(t, lookupResp.Msg.GetInode(), "Lookup 返回 Inode 不能为空")
				curr = lookupResp.Msg.GetInode().GetIno()
			}
			if !reachedRoot {
				allReachedRoot = false
				cyclePath = path
				break
			}
		}

		// 断言正确契约（触发真红）：
		require.True(t, allReachedRoot,
			"轮次 %d 检测到持久化目录环且与根目录 %d 断开连接: path=%v (err1=%v, err2=%v)",
			round, rootIno, cyclePath, err1, err2)
		require.Equal(t, 1, successCount,
			"轮次 %d: 两个初始合法的跨目录 Rename 在任何串行化顺序下必须恰好 1 个成功、1 个返回 EINVAL (实际 err1=%v, err2=%v)",
			round, err1, err2)

		if err1 == nil && err2 != nil {
			lookupResp, lErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: dIno,
				Name:      []byte("A"),
			}))
			require.NoError(t, lErr, "轮次 %d: Rename(A->D) 成功后 Lookup(D, 'A') 必须成功", round)
			require.NotNil(t, lookupResp.Msg.GetInode(), "Lookup 返回 Inode 不能为空")
			require.Equal(t, aIno, lookupResp.Msg.GetInode().GetIno(), "轮次 %d: D 下的 'A' 必须指向 aIno", round)
		} else if err2 == nil && err1 != nil {
			lookupResp, lErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: bIno,
				Name:      []byte("C"),
			}))
			require.NoError(t, lErr, "轮次 %d: Rename(C->B) 成功后 Lookup(B, 'C') 必须成功", round)
			require.NotNil(t, lookupResp.Msg.GetInode(), "Lookup 返回 Inode 不能为空")
			require.Equal(t, cIno, lookupResp.Msg.GetInode().GetIno(), "轮次 %d: B 下的 'C' 必须指向 cIno", round)
		}
	}
}

// TestE2E_H6_ReadDirOffsetPaginationSkipsEntryOnUnlink 复现 H6 缺陷：
// ReadDir 与 ReadDirPlus 依赖数据库 OFFSET 序号分页，边遍历边删除文件会导致未读条目被跳过。
func TestE2E_H6_ReadDirOffsetPaginationSkipsEntryOnUnlink(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h6-readdir-offset-disk",
		MaxBytes: 100 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	require.NotNil(t, disk)
	diskID := disk.GetDiskId()
	rootIno := disk.GetRootIno()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: diskID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("cleanup DeleteDisk failed: %v", err)
		}
	})

	t.Run("ReadDir_UnlinkDuringPagination", func(t *testing.T) {
		// 1. 创建测试目录
		dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      []byte("dir_readdir"),
			Mode:      0o755,
		}))
		require.NoError(t, err)
		dirIno := dirResp.Msg.GetInode().GetIno()

		// 2. 创建常规文件 "a", "b", "c"
		for _, name := range []string{"a", "b", "c"} {
			_, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
				ParentIno: dirIno,
				Name:      []byte(name),
				Mode:      sIfReg | 0o644,
			}))
			require.NoError(t, err, "创建文件 %s 必须成功", name)
		}

		// 3. 打开目录获取句柄
		openDirResp, err := client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:   dirIno,
			Flags: oRdOnly,
		}))
		require.NoError(t, err)
		dirFh := openDirResp.Msg.GetFh()

		// 4. 读取第 1 页 (Limit: 3)
		readDirResp1, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
			Ino:    dirIno,
			Fh:     dirFh,
			Offset: 0,
			Limit:  3,
		}))
		require.NoError(t, err)
		entries1 := readDirResp1.Msg.GetEntries()
		require.Len(t, entries1, 3, "第 1 页应返回 3 个条目 (., .., a)")
		require.Equal(t, ".", string(entries1[0].GetName()))
		require.Equal(t, "..", string(entries1[1].GetName()))
		require.Equal(t, "a", string(entries1[2].GetName()))

		var allFileNames []string
		for _, e := range entries1 {
			name := string(e.GetName())
			if name != "." && name != ".." {
				allFileNames = append(allFileNames, name)
			}
		}

		nextOffset := entries1[2].GetNextOffset()
		require.Greater(t, nextOffset, uint64(0), "返回的续读游标必须大于 0")

		// 5. 模拟 rm -rf / find -delete：删除刚读出的 "a"
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: dirIno,
			Name:      []byte("a"),
		}))
		require.NoError(t, err, "Unlink('a') 必须成功")

		// 6. 携带游标 nextOffset 循环续读直至返回空切片（最大步数 10）
		var page2Names []string
		currOffset := nextOffset
		for step := 0; step < 10; step++ {
			readDirResp2, rErr := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
				Ino:    dirIno,
				Fh:     dirFh,
				Offset: currOffset,
				Limit:  10,
			}))
			require.NoError(t, rErr)
			entries := readDirResp2.Msg.GetEntries()
			if len(entries) == 0 {
				break
			}
			lastEntry := entries[len(entries)-1]
			newOffset := lastEntry.GetNextOffset()
			require.Greater(t, newOffset, currOffset, "续读游标必须严格推进")
			currOffset = newOffset

			for _, e := range entries {
				name := string(e.GetName())
				if name != "." && name != ".." {
					page2Names = append(page2Names, name)
					allFileNames = append(allFileNames, name)
				}
			}
		}

		// 正确性断言（复现触发真红）：
		// POSIX 遍历语义要求未被删除的 "b" 和 "c" 必须均被读取。
		// 由于服务端采用 OFFSET 序号分页，导致 "b" 被静默跳过，此断言将直接失败报红。
		require.Contains(t, page2Names, "b",
			"复现 H6 缺陷：ReadDir 在遍历中 Unlink('a') 后，因 OFFSET 序号错位导致未读条目 'b' 被静默跳过 (第 2 页结果: %v)",
			page2Names)
		require.ElementsMatch(t, []string{"a", "b", "c"}, allFileNames,
			"ReadDir 全遍历读到的常规文件列表必须完整等于 [a, b, c]")
	})

	t.Run("ReadDirPlus_UnlinkDuringPagination", func(t *testing.T) {
		// 1. 创建独立测试目录
		dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      []byte("dir_readdirplus"),
			Mode:      0o755,
		}))
		require.NoError(t, err)
		dirIno := dirResp.Msg.GetInode().GetIno()

		// 2. 创建常规文件 "a", "b", "c"
		for _, name := range []string{"a", "b", "c"} {
			_, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
				ParentIno: dirIno,
				Name:      []byte(name),
				Mode:      sIfReg | 0o644,
			}))
			require.NoError(t, err, "创建文件 %s 必须成功", name)
		}

		// 3. 打开目录获取句柄
		openDirResp, err := client.OpenDir(ctx, connect.NewRequest(&storagev1.OpenDirRequest{
			Ino:   dirIno,
			Flags: oRdOnly,
		}))
		require.NoError(t, err)
		dirFh := openDirResp.Msg.GetFh()

		// 4. 读取第 1 页 (Limit: 3)
		readDirPlusResp1, err := client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino:    dirIno,
			Fh:     dirFh,
			Offset: 0,
			Limit:  3,
		}))
		require.NoError(t, err)
		entries1 := readDirPlusResp1.Msg.GetEntries()
		require.Len(t, entries1, 3, "第 1 页应返回 3 个扩展条目 (., .., a)")
		require.Equal(t, ".", string(entries1[0].GetName()))
		require.Equal(t, "..", string(entries1[1].GetName()))
		require.Equal(t, "a", string(entries1[2].GetName()))

		var allFileNames []string
		for _, e := range entries1 {
			name := string(e.GetName())
			if name != "." && name != ".." {
				allFileNames = append(allFileNames, name)
			}
		}

		nextOffset := entries1[2].GetNextOffset()
		require.Greater(t, nextOffset, uint64(0), "返回的续读游标必须大于 0")

		// 5. 删除刚读出的 "a"
		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: dirIno,
			Name:      []byte("a"),
		}))
		require.NoError(t, err, "Unlink('a') 必须成功")

		// 6. 携带游标 nextOffset 循环续读直至返回空切片（最大步数 10）
		var page2Names []string
		currOffset := nextOffset
		for step := 0; step < 10; step++ {
			readDirPlusResp2, rErr := client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
				Ino:    dirIno,
				Fh:     dirFh,
				Offset: currOffset,
				Limit:  10,
			}))
			require.NoError(t, rErr)
			entries := readDirPlusResp2.Msg.GetEntries()
			if len(entries) == 0 {
				break
			}
			lastEntry := entries[len(entries)-1]
			newOffset := lastEntry.GetNextOffset()
			require.Greater(t, newOffset, currOffset, "续读游标必须严格推进")
			currOffset = newOffset

			for _, e := range entries {
				name := string(e.GetName())
				if name != "." && name != ".." {
					page2Names = append(page2Names, name)
					allFileNames = append(allFileNames, name)
				}
			}
		}

		// 正确性断言（复现触发真红）：
		// ReadDirPlus 同样使用 OFFSET 分页，断言未读条目 "b" 必须存在。
		require.Contains(t, page2Names, "b",
			"复现 H6 缺陷：ReadDirPlus 在遍历中 Unlink('a') 后，因 OFFSET 序号错位导致未读条目 'b' 被静默跳过 (第 2 页结果: %v)",
			page2Names)
		require.ElementsMatch(t, []string{"a", "b", "c"}, allFileNames,
			"ReadDirPlus 全遍历读到的常规文件列表必须完整等于 [a, b, c]")
	})
}

// TestE2E_H10_ConcurrentLinearizabilityAndStrictContract 复现 H10 缺陷：
// 移除宽泛错误白名单，严格校验并发操作返回值的线性一致性（Linearizability）。
// 覆盖三个子测试：
// 1. Rename 覆盖写 vs Unlink 并发：Unlink 绝不能非法返回 ENOENT（M6 缺陷）。
// 2. TmpFile 并发 Link/Release vs GetAttr：GetAttr 绝不能非法返回 ENOENT（M7 缺陷）。
// 3. 并发双 Rename 覆盖同一已存在目标与并发同名 Mkdir 线性一致性核验（M6/2-C3 缺陷）。
func TestE2E_H10_ConcurrentLinearizabilityAndStrictContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h10-linearizability-disk",
		MaxBytes: 100 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	require.NotNil(t, disk)
	diskID := disk.GetDiskId()
	rootIno := disk.GetRootIno()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{
			DiskId: diskID,
		}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("cleanup DeleteDisk failed: %v", err)
		}
	})

	t.Run("ConcurrentRenameOverwriteVsUnlink_Linearizability", func(t *testing.T) {
		const maxRounds = 80
		for round := 1; round <= maxRounds; round++ {
			// 每轮创建独立子目录
			subDirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: rootIno,
				Name:      []byte(fmt.Sprintf("sub_%d", round)),
				Mode:      0o755,
			}))
			require.NoError(t, err)
			subDirIno := subDirResp.Msg.GetInode().GetIno()

			// 创建 "x"
			xResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
				ParentIno: subDirIno,
				Name:      []byte("x"),
				Mode:      sIfReg | 0o644,
				Flags:     oRdWr,
			}))
			require.NoError(t, err)
			xIno := xResp.Msg.GetInode().GetIno()
			xFh := xResp.Msg.GetFh()
			_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: xIno,
				Fh:  xFh,
			}))
			require.NoError(t, err)

			// 创建 "y"
			yResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
				ParentIno: subDirIno,
				Name:      []byte("y"),
				Mode:      sIfReg | 0o644,
				Flags:     oRdWr,
			}))
			require.NoError(t, err)
			yIno := yResp.Msg.GetInode().GetIno()
			yFh := yResp.Msg.GetFh()
			_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
				Ino: yIno,
				Fh:  yFh,
			}))
			require.NoError(t, err)

			// 并发屏障：Goroutine 1 执行 Rename("y" -> "x"), Goroutine 2 执行 Unlink("x")
			var renameErr, unlinkErr error
			var wg sync.WaitGroup
			wg.Add(2)
			startCh := make(chan struct{})

			go func() {
				defer wg.Done()
				<-startCh
				_, renameErr = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
					OldParentIno: subDirIno,
					OldName:      []byte("y"),
					NewParentIno: subDirIno,
					NewName:      []byte("x"),
					Flags:        0,
				}))
			}()

			go func() {
				defer wg.Done()
				<-startCh
				_, unlinkErr = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
					ParentIno: subDirIno,
					Name:      []byte("x"),
				}))
			}()

			close(startCh)
			wg.Wait()

			// 线性一致性严格核验：
			// 1. Rename("y" -> "x") 绝不能返回错误
			require.NoError(t, renameErr, "轮次 %d: Rename('y'->'x') 必须成功", round)
			// 2. 目录项 x 在整个并发区间始终存在（原 x 或新 y->x），任何合法串行历史下 Unlink(x) 均必须成功，绝不能返回 ENOENT
			require.NoError(t, unlinkErr,
				"轮次 %d: 目录项 x 在整个并发区间始终存在，任何合法串行历史下 Unlink(x) 均必须成功，绝不能返回 ENOENT", round)

			// 3. 结束后验证最终状态与两种合法串行顺序之一完全吻合：
			// - 若 Unlink 先执行而后 Rename：最终存在 "x" 且 Ino 为 yIno
			// - 若 Rename 先执行而后 Unlink：最终 "x" 已被删除，Lookup 返回 NotFound (ENOENT)
			lookupResp, lookupErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: subDirIno,
				Name:      []byte("x"),
			}))
			if lookupErr == nil {
				require.NotNil(t, lookupResp.Msg.GetInode())
				require.Equal(t, yIno, lookupResp.Msg.GetInode().GetIno(),
					"轮次 %d: Unlink 先于 Rename 执行，目录项 x 最终必须指向原 y 的 Inode", round)
			} else {
				testutil.RequireConnectError(t, lookupErr, connect.CodeNotFound, "ENOENT")
			}

			// 补充对原目录项 "y" 的 Lookup 断言：无论哪种合法串行顺序（被覆盖或删除的节点 nlink == 0 时），
			// 目录项 "y" 均绝不能残留悬挂目录项，必须返回 connect.CodeNotFound
			_, lookupYErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: subDirIno,
				Name:      []byte("y"),
			}))
			testutil.RequireConnectError(t, lookupYErr, connect.CodeNotFound, "ENOENT")
		}
	})

	t.Run("ConcurrentTmpFileLinkReleaseVsGetAttr_Linearizability", func(t *testing.T) {
		const maxRounds = 150
		for round := 1; round <= maxRounds; round++ {
			// 0. 每轮创建独立子目录，隔离行锁
			dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: rootIno,
				Name:      []byte(fmt.Sprintf("dir_tmp_%d", round)),
				Mode:      0o755,
			}))
			require.NoError(t, err)
			roundDirIno := dirResp.Msg.GetInode().GetIno()

			// 1. 创建匿名 TmpFile (nlink == 0)
			tmpResp, err := client.TmpFile(ctx, connect.NewRequest(&storagev1.TmpFileRequest{
				ParentIno: roundDirIno,
				Mode:      sIfReg | 0o600,
				Flags:     oRdWr,
			}))
			require.NoError(t, err)
			tmpIno := tmpResp.Msg.GetInode().GetIno()
			tmpFh := tmpResp.Msg.GetFh()

			var linkErr, relErr error
			var wg sync.WaitGroup
			startCh := make(chan struct{})
			stopCh := make(chan struct{})
			readerReady := make(chan struct{})
			var readyOnce sync.Once
			var getAttrCount atomic.Uint64

			// Goroutine 1：通过 readerReady 通道握手等待 GetAttr 观察协程完成首次 RPC 探测，
			// 随后立即连续调用 Link (使 nlink 变为 1) 与 Release 关闭句柄，并在 Release 返回后 close(stopCh)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(stopCh)
				<-startCh
				<-readerReady
				linkName := fmt.Sprintf("linked_tmp_r%d", round)
				_, linkErr = client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
					Ino:          tmpIno,
					NewParentIno: roundDirIno,
					NewName:      []byte(linkName),
				}))
				if linkErr != nil {
					return
				}
				_, relErr = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
					Ino: tmpIno,
					Fh:  tmpFh,
				}))
			}()

			// Goroutines 2..13：启动 12 个并发读取 goroutine 持续不断调用 GetAttr 直到 Link + Release 完成 (stopCh 关闭)
			const getAttrWorkers = 12
			errCh := make(chan error, getAttrWorkers*100)
			for w := 0; w < getAttrWorkers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-startCh
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						gResp, gErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
							Ino: tmpIno,
						}))
						getAttrCount.Add(1)
						readyOnce.Do(func() {
							close(readerReady)
						})
						if gErr != nil {
							select {
							case errCh <- gErr:
							default:
							}
							return
						}
						if gResp == nil || gResp.Msg == nil || gResp.Msg.GetInode() == nil {
							select {
							case errCh <- fmt.Errorf("GetAttr succeeded but returned nil Inode"):
							default:
							}
							return
						}
					}
				}()
			}

			close(startCh)
			wg.Wait()
			close(errCh)

			require.NoError(t, linkErr, "轮次 %d: Link 必须成功", round)
			require.NoError(t, relErr, "轮次 %d: Release 必须成功", round)
			require.Greater(t, getAttrCount.Load(), uint64(0), "轮次 %d: GetAttr 探测次数必须 > 0", round)

			// 线性一致性严格核验：
			// 文件先 Link 成功（nlink=1）后 Release 句柄，全程合法存活，GetAttr 绝不能返回 ENOENT
			for gErr := range errCh {
				require.NoError(t, gErr,
					"轮次 %d: TmpFile 已先 Link 成功再 Release，全生命周期合法存活，GetAttr 绝不能返回 ENOENT", round)
			}
		}
	})

	t.Run("ConcurrentDualRenameOverwriteSameTarget_Linearizability", func(t *testing.T) {
		// 1. Docker storage-service 真实并发同名创建线性一致性校验
		dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
			ParentIno: rootIno,
			Name:      []byte("dir_same_name_linear"),
			Mode:      0o755,
		}))
		require.NoError(t, err)
		targetDirIno := dirResp.Msg.GetInode().GetIno()

		const mkdirWorkers = 8
		var mkdirWg sync.WaitGroup
		mkdirWg.Add(mkdirWorkers)
		startMkdirCh := make(chan struct{})

		type mkdirResult struct {
			err   error
			inode *storagev1.Inode
		}
		results := make([]mkdirResult, mkdirWorkers)

		for i := 0; i < mkdirWorkers; i++ {
			workerID := i
			go func() {
				defer mkdirWg.Done()
				<-startMkdirCh
				resp, mErr := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
					ParentIno: targetDirIno,
					Name:      []byte("same_dir"),
					Mode:      0o755,
				}))
				var ino *storagev1.Inode
				if resp != nil && resp.Msg != nil {
					ino = resp.Msg.GetInode()
				}
				results[workerID] = mkdirResult{err: mErr, inode: ino}
			}()
		}
		close(startMkdirCh)
		mkdirWg.Wait()

		successCount := 0
		for i, res := range results {
			if res.err == nil {
				successCount++
				require.NotNil(t, res.inode, "Worker %d: Mkdir 成功时 Inode 绝不能为 nil", i)
				require.Greater(t, res.inode.GetIno(), uint64(0), "Worker %d: Ino 必须大于 0", i)
			} else {
				testutil.RequireConnectError(t, res.err, connect.CodeAlreadyExists, "EEXIST")
			}
		}
		require.Equal(t, 1, successCount, "并发同名 Mkdir 必须满足线性一致性，恰好 1 个成功")

		// 2. 并发双 Rename 覆盖同一已存在目标 "x" 的多写线性一致性核验 (会话 2-C3 / manager_dir_topology.go:563)
		const dualRenameRounds = 80
		for round := 1; round <= dualRenameRounds; round++ {
			subResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: rootIno,
				Name:      []byte(fmt.Sprintf("dir_dual_rename_%d", round)),
				Mode:      0o755,
			}))
			require.NoError(t, err)
			subDirIno := subResp.Msg.GetInode().GetIno()

			// 创建三个常规文件 "a", "b", "x" 并关闭它们的初始句柄
			createFile := func(name string) uint64 {
				cResp, cErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
					ParentIno: subDirIno,
					Name:      []byte(name),
					Mode:      sIfReg | 0o644,
					Flags:     oRdWr,
				}))
				require.NoError(t, cErr, "创建文件 %s 必须成功", name)
				fIno := cResp.Msg.GetInode().GetIno()
				fFh := cResp.Msg.GetFh()
				_, rErr := client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
					Ino: fIno,
					Fh:  fFh,
				}))
				require.NoError(t, rErr, "释放文件 %s 句柄必须成功", name)
				return fIno
			}

			aIno := createFile("a")
			bIno := createFile("b")
			xIno := createFile("x")
			_ = xIno

			var errA, errB error
			var wg sync.WaitGroup
			wg.Add(2)
			startCh := make(chan struct{})

			// Goroutine 1: Rename("a" -> "x")
			go func() {
				defer wg.Done()
				<-startCh
				_, errA = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
					OldParentIno: subDirIno,
					OldName:      []byte("a"),
					NewParentIno: subDirIno,
					NewName:      []byte("x"),
					Flags:        0,
				}))
			}()

			// Goroutine 2: Rename("b" -> "x")
			go func() {
				defer wg.Done()
				<-startCh
				_, errB = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
					OldParentIno: subDirIno,
					OldName:      []byte("b"),
					NewParentIno: subDirIno,
					NewName:      []byte("x"),
					Flags:        0,
				}))
			}()

			close(startCh)
			wg.Wait()

			// 线性一致性预言机断言（触发真红）：
			// 在任何合法的串行执行顺序中（无论是 Rename(a->x) 先于 Rename(b->x)，还是 Rename(b->x) 先于 Rename(a->x)），
			// 两个源 "a" 与 "b" 互不冲突，且目标 "x" 始终存在（后执行者合法覆盖先执行者），
			// 因此两个 Rename 操作都必须 100% 返回成功，绝不可能有任何一方返回 ENOENT！
			// 在当前未修复版本上（manager_dir_topology.go:563），后加锁的一方因预读到的旧 xIno 被先完成的一方物理删除，
			// 加锁失败遇到 ErrNoRows 错误误报 ENOENT，必然触发真红失败！
			require.NoError(t, errA, "轮次 %d: Rename('a'->'x') 必须成功 (复现 manager_dir_topology.go:563 缺陷: 后加锁方查不到被覆盖删除的旧目标误报 ENOENT)", round)
			require.NoError(t, errB, "轮次 %d: Rename('b'->'x') 必须成功 (复现 manager_dir_topology.go:563 缺陷: 后加锁方查不到被覆盖删除的旧目标误报 ENOENT)", round)

			// 最终状态核验："x" 必须存在且必定指向 aIno 或 bIno
			lookupResp, lookupErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: subDirIno,
				Name:      []byte("x"),
			}))
			require.NoError(t, lookupErr, "轮次 %d: 最终 Lookup('x') 必须成功", round)
			require.NotNil(t, lookupResp.Msg.GetInode(), "轮次 %d: Lookup 返回 Inode 不能为空", round)
			finalIno := lookupResp.Msg.GetInode().GetIno()
			require.True(t, finalIno == aIno || finalIno == bIno,
				"轮次 %d: 目录项 'x' 必须指向 aIno (%d) 或 bIno (%d)，实际指向 %d", round, aIno, bIno, finalIno)

			// 补充对源目录项 "a" 和 "b" 的 Lookup 断言：两者必须均返回 connect.CodeNotFound，严禁仅断言目标存在而漏检源目录项残留
			_, errLookupA := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: subDirIno,
				Name:      []byte("a"),
			}))
			testutil.RequireConnectError(t, errLookupA, connect.CodeNotFound, "ENOENT")

			_, errLookupB := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: subDirIno,
				Name:      []byte("b"),
			}))
			testutil.RequireConnectError(t, errLookupB, connect.CodeNotFound, "ENOENT")
		}
	})
}
