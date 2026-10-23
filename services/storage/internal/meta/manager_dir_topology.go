package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// validateEntryNameAllowEmpty 校验目录项名称长度与非法字符，允许空名称进入事务在校验父目录后返回 ErrNotFound (ENOENT)。
func validateEntryNameAllowEmpty(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// Link 为已有非目录 Inode 在目标父目录下创建硬链接。
func (m *manager) Link(ctx context.Context, params LinkParams) (*Inode, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.NewName); err != nil {
		return nil, err
	}

	var result *Inode
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if isDotOrDotDot(params.NewName) {
			parent, err := q.GetInodeForUpdate(ctx, int64(params.NewParentIno))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get new parent inode for update %d: %w", params.NewParentIno, err)
			}
			if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
				return ErrNotDir
			}
			if parent.Nlink == 0 {
				return ErrNotFound
			}
			return ErrAlreadyExists
		}

		// 按全局 ino 升序锁定目标父目录与源节点，避免跨层操作逆序死锁
		var parent db.Inode
		var srcInode db.Inode
		var isTmpFileMaterializing bool
		for _, ino := range uniqueSortedInos(int64(params.NewParentIno), int64(params.Ino)) {
			row, err := q.GetInodeForUpdate(ctx, ino)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode for update %d: %w", ino, err)
			}
			if ino == int64(params.NewParentIno) {
				if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				if row.Nlink == 0 {
					return ErrNotFound
				}
				parent = row
			}
			if ino == int64(params.Ino) {
				srcInode = row
				// 禁止目录硬链接
				if (uint32(row.Mode) & S_IFMT) == S_IFDIR {
					return ErrPerm
				}
				// 硬链接数量上限检查
				if uint32(row.Nlink) >= MaxNlink {
					return ErrTooManyLinks
				}
				// 当 nlink == 0 时，仅允许持有有效且未携带 O_EXCL 的 O_TMPFILE 常规文件一次性具名化，严禁复活已 Unlink 孤儿节点
				if row.Nlink == 0 {
					if (uint32(row.Mode) & S_IFMT) != S_IFREG {
						return ErrNotFound
					}
					linkable, err := q.HasLinkableTmpFileHandleByIno(ctx, row.Ino)
					if err != nil {
						return fmt.Errorf("check linkable tmpfile handle for inode %d: %w", row.Ino, err)
					}
					if !linkable {
						return ErrNotFound
					}
					isTmpFileMaterializing = true
				}
			}
		}

		if parent.RootIno != srcInode.RootIno {
			return ErrCrossDevice
		}

		// 插入新目录项
		err := q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.NewParentIno),
			Name:      params.NewName,
			Ino:       int64(params.Ino),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		// 递增源节点 nlink 并刷新 ctime
		updatedSrc, err := q.IncrementInodeNlinkAndTouchCtime(ctx, int64(params.Ino))
		if err != nil {
			return fmt.Errorf("increment source inode %d nlink and touch ctime: %w", params.Ino, err)
		}

		// 若为匿名 TmpFile 首次具名化，原子清除关联句柄的 O_TMPFILE 位，防止后续被 Unlink 后二次复活
		if isTmpFileMaterializing {
			if err := q.ClearTmpFileFlagByIno(ctx, int64(params.Ino)); err != nil {
				return fmt.Errorf("clear tmpfile flag for inode %d: %w", params.Ino, err)
			}
		}

		// 刷新目标父目录 mtime 与 ctime (父目录 nlink 保持不变)
		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch new parent inode %d timestamps: %w", parent.Ino, err)
		}

		result = m.newInode(updatedSrc)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrPerm):
			return nil, ErrPerm
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		case errors.Is(err, ErrTooManyLinks):
			return nil, ErrTooManyLinks
		case errors.Is(err, ErrCrossDevice):
			return nil, ErrCrossDevice
		default:
			return nil, fmt.Errorf("link tx: %w", err)
		}
	}

	return result, nil
}

// Unlink 删除指定父目录下的非目录项（解链）。
func (m *manager) Unlink(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if len(name) == 0 || isDotOrDotDot(name) {
			parent, err := q.GetInodeForUpdate(ctx, int64(parentIno))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get parent inode for update %d: %w", parentIno, err)
			}
			if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
				return ErrNotDir
			}
			if parent.Nlink == 0 {
				return ErrNotFound
			}
			if len(name) == 0 {
				return ErrNotFound
			}
			return ErrInvalidArgument
		}

		// 预读目标目录项以获知 child_ino，从而按全局 ino 升序加锁
		preEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				parent, pErr := q.GetInodeForUpdate(ctx, int64(parentIno))
				if pErr != nil {
					if errors.Is(pErr, pgx.ErrNoRows) {
						return ErrNotFound
					}
					return fmt.Errorf("get parent inode for update %d: %w", parentIno, pErr)
				}
				if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				return ErrNotFound
			}
			return fmt.Errorf("get entry %d/%s: %w", parentIno, name, err)
		}

		var parent, child db.Inode
		for _, ino := range uniqueSortedInos(int64(parentIno), preEntry.Ino) {
			row, err := q.GetInodeForUpdate(ctx, ino)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode for update %d: %w", ino, err)
			}
			if ino == int64(parentIno) {
				if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				if row.Nlink == 0 {
					return ErrNotFound
				}
				parent = row
			}
			if ino == preEntry.Ino {
				child = row
			}
		}

		// 持锁二次校验目录项，防止预读与加锁之间目录项被并发删除或重绑定
		entry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("verify entry %d/%s: %w", parentIno, name, err)
		}
		if entry.Ino != preEntry.Ino {
			return errRetryTx
		}

		// Unlink 严禁删除目录项 (POSIX EISDIR)
		if (uint32(child.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		// 从 entries 删除目录项
		if err := q.DeleteEntry(ctx, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}); err != nil {
			return fmt.Errorf("delete entry %d/%s: %w", parentIno, name, err)
		}

		// 递减子节点硬链接数并刷新 ctime
		updatedChild, err := q.DecrementInodeNlinkAndTouchCtime(ctx, child.Ino)
		if err != nil {
			return fmt.Errorf("decrement child inode %d nlink and touch ctime: %w", child.Ino, err)
		}

		// 刷新父目录 mtime 与 ctime (父目录 nlink 保持不变)
		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		// 当硬链接数降为 0 时，若无活跃句柄则物理删除 Inode 行、唤醒锁等待队列并退还所属 Disk 配额
		if updatedChild.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, child); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		default:
			return fmt.Errorf("unlink tx: %w", err)
		}
	}

	return nil
}

// Rmdir 移除指定父目录下的空目录项。
func (m *manager) Rmdir(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if len(name) == 0 || isDotOrDotDot(name) {
			parent, err := q.GetInodeForUpdate(ctx, int64(parentIno))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get parent inode for update %d: %w", parentIno, err)
			}
			if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
				return ErrNotDir
			}
			if parent.Nlink == 0 {
				return ErrNotFound
			}
			if len(name) == 0 {
				return ErrNotFound
			}
			if bytes.Equal(name, []byte(".")) {
				return ErrInvalidArgument
			}
			return ErrNotEmpty
		}

		// 预读目标目录项以获知 child_ino，从而按全局 ino 升序加锁
		preEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				parent, pErr := q.GetInodeForUpdate(ctx, int64(parentIno))
				if pErr != nil {
					if errors.Is(pErr, pgx.ErrNoRows) {
						return ErrNotFound
					}
					return fmt.Errorf("get parent inode for update %d: %w", parentIno, pErr)
				}
				if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				return ErrNotFound
			}
			return fmt.Errorf("get entry %d/%s: %w", parentIno, name, err)
		}

		var parent, child db.Inode
		for _, ino := range uniqueSortedInos(int64(parentIno), preEntry.Ino) {
			row, err := q.GetInodeForUpdate(ctx, ino)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode for update %d: %w", ino, err)
			}
			if ino == int64(parentIno) {
				if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				if row.Nlink == 0 {
					return ErrNotFound
				}
				parent = row
			}
			if ino == preEntry.Ino {
				child = row
			}
		}

		// 持锁二次校验目录项，防止预读与加锁之间目录项被并发删除或重绑定
		entry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("verify entry %d/%s: %w", parentIno, name, err)
		}
		if entry.Ino != preEntry.Ino {
			return errRetryTx
		}

		// Rmdir 目标节点必须为目录 (POSIX ENOTDIR)
		if (uint32(child.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		// 空目录校验：目标目录严禁包含任何子目录项 (POSIX ENOTEMPTY)
		hasChildren, err := q.HasChildEntries(ctx, child.Ino)
		if err != nil {
			return fmt.Errorf("check child dir %d has child entries: %w", child.Ino, err)
		}
		if hasChildren {
			return ErrNotEmpty
		}

		// 从 entries 删除目录项
		if err := q.DeleteEntry(ctx, db.DeleteEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		}); err != nil {
			return fmt.Errorf("delete entry %d/%s: %w", parentIno, name, err)
		}

		// 目标目录置 nlink = 0 并刷新 ctime (保留 Inode 行)
		if _, err := q.ClearInodeNlinkAndTouchCtime(ctx, child.Ino); err != nil {
			return fmt.Errorf("clear child dir %d nlink and touch ctime: %w", child.Ino, err)
		}

		// 父目录 nlink 递减 1 并刷新 mtime 与 ctime
		if err := q.DecrementInodeNlinkAndTouch(ctx, parent.Ino); err != nil {
			return fmt.Errorf("decrement parent inode %d nlink and touch: %w", parent.Ino, err)
		}

		// 检查目标目录当前是否存在活跃目录句柄 (rmdir-whilst-open)，若无活跃句柄则物理删除并退还配额
		if err := m.reclaimOrphanIfNoHandles(ctx, q, child); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrNotEmpty):
			return ErrNotEmpty
		case errors.Is(err, ErrTooManyLinks):
			return ErrTooManyLinks
		case errors.Is(err, ErrNotSupported):
			return ErrNotSupported
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		default:
			return fmt.Errorf("rmdir tx: %w", err)
		}
	}

	return nil
}

// Rename 执行原子文件/目录重命名、移动或交换（支持 FUSE_RENAME2 标志位）。
func (m *manager) Rename(ctx context.Context, params RenameParams) error {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if (params.Flags &^ (RenameNoReplace | RenameExchange | RenameWhiteout)) != 0 {
		return ErrInvalidArgument
	}
	if (params.Flags & (RenameNoReplace | RenameExchange)) == (RenameNoReplace | RenameExchange) {
		return ErrInvalidArgument
	}
	if (params.Flags & RenameWhiteout) != 0 {
		return ErrNotSupported
	}
	if err := validateEntryNameAllowEmpty(params.OldName); err != nil {
		return err
	}
	if err := validateEntryName(params.NewName); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if len(params.OldName) == 0 || isDotOrDotDot(params.OldName) || isDotOrDotDot(params.NewName) {
			if _, _, err := lockAndValidateParents(ctx, q, params.OldParentIno, params.NewParentIno); err != nil {
				return err
			}
			if len(params.OldName) == 0 {
				return ErrNotFound
			}
			return ErrInvalidArgument
		}

		// 预读源目录项与目标目录项，以便连同父目录一起按全局 ino 升序一次性加锁
		preOldEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(params.OldParentIno),
			Name:      params.OldName,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if _, _, pErr := lockAndValidateParents(ctx, q, params.OldParentIno, params.NewParentIno); pErr != nil {
					return pErr
				}
				return ErrNotFound
			}
			return fmt.Errorf("get old entry %d/%s: %w", params.OldParentIno, params.OldName, err)
		}
		oldIno := preOldEntry.Ino

		// 同路径短路 (POSIX No-op)：锁定父目录并确认源目录项仍存在
		if params.OldParentIno == params.NewParentIno && bytes.Equal(params.OldName, params.NewName) {
			if _, _, err := lockAndValidateParents(ctx, q, params.OldParentIno, params.NewParentIno); err != nil {
				return err
			}
			if _, err := q.GetEntry(ctx, db.GetEntryParams{
				ParentIno: int64(params.OldParentIno),
				Name:      params.OldName,
			}); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("verify old entry %d/%s: %w", params.OldParentIno, params.OldName, err)
			}
			if params.Flags == RenameNoReplace {
				return ErrAlreadyExists
			}
			return nil
		}

		preNewEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(params.NewParentIno),
			Name:      params.NewName,
		})
		var hasNewEntry bool
		var newIno int64
		if err == nil {
			hasNewEntry = true
			newIno = preNewEntry.Ino
		} else if errors.Is(err, pgx.ErrNoRows) {
			if params.Flags == RenameExchange {
				if _, _, pErr := lockAndValidateParents(ctx, q, params.OldParentIno, params.NewParentIno); pErr != nil {
					return pErr
				}
				return ErrNotFound
			}
		} else {
			return fmt.Errorf("get new entry %d/%s: %w", params.NewParentIno, params.NewName, err)
		}

		inosToLock := []int64{int64(params.OldParentIno), int64(params.NewParentIno), oldIno}
		if hasNewEntry && oldIno != newIno {
			inosToLock = append(inosToLock, newIno)
		}

		locked := make(map[int64]db.Inode, len(inosToLock))
		for _, ino := range uniqueSortedInos(inosToLock...) {
			row, err := q.GetInodeForUpdate(ctx, ino)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode %d for update: %w", ino, err)
			}
			if ino == int64(params.OldParentIno) || ino == int64(params.NewParentIno) {
				if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
					return ErrNotDir
				}
				if row.Nlink == 0 {
					return ErrNotFound
				}
			}
			locked[ino] = row
		}

		// 持锁二次校验源与目标目录项，若在加锁窗口内发生并发漂移则回滚重试
		oldEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(params.OldParentIno),
			Name:      params.OldName,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("verify old entry %d/%s: %w", params.OldParentIno, params.OldName, err)
		}
		if oldEntry.Ino != oldIno {
			return errRetryTx
		}

		newEntry, err := q.GetEntry(ctx, db.GetEntryParams{
			ParentIno: int64(params.NewParentIno),
			Name:      params.NewName,
		})
		var currHasNewEntry bool
		var currNewIno int64
		if err == nil {
			currHasNewEntry = true
			currNewIno = newEntry.Ino
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("verify new entry %d/%s: %w", params.NewParentIno, params.NewName, err)
		}
		if currHasNewEntry != hasNewEntry || (currHasNewEntry && currNewIno != newIno) {
			return errRetryTx
		}

		oldParent := locked[int64(params.OldParentIno)]
		newParent := locked[int64(params.NewParentIno)]
		oldInode := locked[oldIno]

		// 跨虚拟磁盘拓扑隔离校验：oldParent、newParent 与 oldInode 必须属于同一虚拟磁盘
		if oldParent.RootIno != newParent.RootIno || oldInode.RootIno != oldParent.RootIno {
			return ErrCrossDevice
		}
		if hasNewEntry && oldIno != newIno {
			newInode := locked[newIno]
			if newInode.RootIno != oldParent.RootIno {
				return ErrCrossDevice
			}
		}

		// RenameNoReplace：只要目标 Entry 已存在（包含同一 Inode 的不同硬链接与异 Inode），一律返回 ErrAlreadyExists
		if params.Flags == RenameNoReplace && hasNewEntry {
			return ErrAlreadyExists
		}

		// 同节点硬链接短路 (POSIX No-op / RenameExchange 同节点短路)
		if hasNewEntry && oldIno == newIno {
			return nil
		}

		newInode := locked[newIno]

		oldIsDir := (uint32(oldInode.Mode) & S_IFMT) == S_IFDIR
		newIsDir := hasNewEntry && (uint32(newInode.Mode)&S_IFMT) == S_IFDIR

		// 祖先环检测：当源节点为目录且跨目录移动或交换时（正向检测）
		if oldIsDir && params.OldParentIno != params.NewParentIno {
			if params.NewParentIno == uint64(oldIno) {
				return ErrInvalidArgument
			}
			isSub, err := q.IsSubdirectory(ctx, db.IsSubdirectoryParams{
				ChildIno:    int64(params.NewParentIno),
				AncestorIno: oldIno,
			})
			if err != nil {
				return fmt.Errorf("check is subdirectory %d in %d: %w", params.NewParentIno, oldIno, err)
			}
			if isSub {
				return ErrInvalidArgument
			}
		}

		// RenameExchange 双向原子交换分支
		if params.Flags == RenameExchange {
			// 反向祖先环检测：当目标节点为目录且跨目录交换时
			if newIsDir && params.OldParentIno != params.NewParentIno {
				if params.OldParentIno == uint64(newIno) {
					return ErrInvalidArgument
				}
				isSub, err := q.IsSubdirectory(ctx, db.IsSubdirectoryParams{
					ChildIno:    int64(params.OldParentIno),
					AncestorIno: newIno,
				})
				if err != nil {
					return fmt.Errorf("check reverse is subdirectory %d in %d: %w", params.OldParentIno, newIno, err)
				}
				if isSub {
					return ErrInvalidArgument
				}
			}

			if params.OldParentIno != params.NewParentIno {
				if oldIsDir && !newIsDir && uint32(newParent.Nlink) >= MaxNlink {
					return ErrTooManyLinks
				}
				if !oldIsDir && newIsDir && uint32(oldParent.Nlink) >= MaxNlink {
					return ErrTooManyLinks
				}
			}

			// 原地互换两个目录项的目标 Ino
			if err := q.UpdateEntryIno(ctx, db.UpdateEntryInoParams{
				ParentIno: int64(params.OldParentIno),
				Name:      params.OldName,
				Ino:       newIno,
			}); err != nil {
				return fmt.Errorf("update old entry %d/%s ino -> %d: %w", params.OldParentIno, params.OldName, newIno, err)
			}
			if err := q.UpdateEntryIno(ctx, db.UpdateEntryInoParams{
				ParentIno: int64(params.NewParentIno),
				Name:      params.NewName,
				Ino:       oldIno,
			}); err != nil {
				return fmt.Errorf("update new entry %d/%s ino -> %d: %w", params.NewParentIno, params.NewName, oldIno, err)
			}

			// 刷新双节点 ctime（跨父目录移动目录时同步更新目录的 parent_ino）
			if oldIsDir && params.OldParentIno != params.NewParentIno {
				if err := q.UpdateInodeParentInoAndTouchCtime(ctx, db.UpdateInodeParentInoAndTouchCtimeParams{
					Ino:       oldIno,
					ParentIno: int64(params.NewParentIno),
				}); err != nil {
					return fmt.Errorf("update old dir %d parent_ino and touch ctime: %w", oldIno, err)
				}
			} else {
				if err := q.TouchInodeCtime(ctx, oldIno); err != nil {
					return fmt.Errorf("touch old inode %d ctime: %w", oldIno, err)
				}
			}
			if newIsDir && params.OldParentIno != params.NewParentIno {
				if err := q.UpdateInodeParentInoAndTouchCtime(ctx, db.UpdateInodeParentInoAndTouchCtimeParams{
					Ino:       newIno,
					ParentIno: int64(params.OldParentIno),
				}); err != nil {
					return fmt.Errorf("update new dir %d parent_ino and touch ctime: %w", newIno, err)
				}
			} else {
				if err := q.TouchInodeCtime(ctx, newIno); err != nil {
					return fmt.Errorf("touch new inode %d ctime: %w", newIno, err)
				}
			}

			// 维护父目录 nlink 与时间戳
			if params.OldParentIno == params.NewParentIno {
				if err := q.TouchInodeTimestamps(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("touch parent inode %d timestamps: %w", oldParent.Ino, err)
				}
			} else if oldIsDir == newIsDir {
				// 跨父目录且双方同为目录或同为非目录：双父目录 nlink 不变，仅刷新时间戳
				if err := q.TouchInodeTimestamps(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("touch old parent inode %d timestamps: %w", oldParent.Ino, err)
				}
				if err := q.TouchInodeTimestamps(ctx, newParent.Ino); err != nil {
					return fmt.Errorf("touch new parent inode %d timestamps: %w", newParent.Ino, err)
				}
			} else if oldIsDir && !newIsDir {
				// 跨父目录且源为目录、目标为非目录：oldParent 移出目录 nlink - 1，newParent 移入目录 nlink + 1
				if err := q.DecrementInodeNlinkAndTouch(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("decrement old parent inode %d nlink and touch: %w", oldParent.Ino, err)
				}
				if err := q.IncrementInodeNlinkAndTouch(ctx, newParent.Ino); err != nil {
					return fmt.Errorf("increment new parent inode %d nlink and touch: %w", newParent.Ino, err)
				}
			} else {
				// 跨父目录且源为非目录、目标为目录：oldParent 移入目录 nlink + 1，newParent 移出目录 nlink - 1
				if err := q.IncrementInodeNlinkAndTouch(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("increment old parent inode %d nlink and touch: %w", oldParent.Ino, err)
				}
				if err := q.DecrementInodeNlinkAndTouch(ctx, newParent.Ino); err != nil {
					return fmt.Errorf("decrement new parent inode %d nlink and touch: %w", newParent.Ino, err)
				}
			}

			return nil
		}

		// 异节点覆盖兼容矩阵校验
		if hasNewEntry {
			if oldIsDir && !newIsDir {
				return ErrNotDir // POSIX ENOTDIR: old is dir, new is not dir
			}
			if !oldIsDir && newIsDir {
				return ErrIsDir // POSIX EISDIR: old is not dir, new is dir
			}
			if oldIsDir && newIsDir {
				// 目录覆盖目录：目标目录必须为空
				hasChildren, err := q.HasChildEntries(ctx, newIno)
				if err != nil {
					return fmt.Errorf("check new dir %d has child entries: %w", newIno, err)
				}
				if hasChildren {
					return ErrNotEmpty
				}
			}

			// 覆盖旧目标项：从 entries 删除原 newEntry
			if err := q.DeleteEntry(ctx, db.DeleteEntryParams{
				ParentIno: int64(params.NewParentIno),
				Name:      params.NewName,
			}); err != nil {
				return fmt.Errorf("delete target entry %d/%s: %w", params.NewParentIno, params.NewName, err)
			}

			if newIsDir {
				// 被覆盖目标为空目录：nlink 直接置 0，刷新 ctime
				if _, err := q.ClearInodeNlinkAndTouchCtime(ctx, newIno); err != nil {
					return fmt.Errorf("clear target dir %d nlink: %w", newIno, err)
				}

				// 检查被覆盖空目录是否存在活跃目录句柄 (rmdir-whilst-open)，若无活跃句柄则物理删除并退还配额
				if err := m.reclaimOrphanIfNoHandles(ctx, q, newInode); err != nil {
					return err
				}
			} else {
				// 被覆盖目标为非目录：nlink 递减 1，刷新 ctime
				updatedTarget, err := q.DecrementInodeNlinkAndTouchCtime(ctx, newIno)
				if err != nil {
					return fmt.Errorf("decrement target file %d nlink: %w", newIno, err)
				}

				// 当硬链接数降为 0 时，若无活跃句柄则物理删除 Inode 行、唤醒锁等待队列并退还配额
				if updatedTarget.Nlink == 0 {
					if err := m.reclaimOrphanIfNoHandles(ctx, q, newInode); err != nil {
						return err
					}
				}
			}
		}

		if oldIsDir && params.OldParentIno != params.NewParentIno && !hasNewEntry && uint32(newParent.Nlink) >= MaxNlink {
			return ErrTooManyLinks
		}

		// 原子更新目录项至新位置
		if err := q.UpdateEntry(ctx, db.UpdateEntryParams{
			OldParentIno: int64(params.OldParentIno),
			OldName:      params.OldName,
			NewParentIno: int64(params.NewParentIno),
			NewName:      params.NewName,
		}); err != nil {
			return fmt.Errorf("update entry %d/%s -> %d/%s: %w", params.OldParentIno, params.OldName, params.NewParentIno, params.NewName, err)
		}

		// 刷新源节点 ctime（跨父目录移动目录时同步更新目录的 parent_ino）
		if oldIsDir && params.OldParentIno != params.NewParentIno {
			if err := q.UpdateInodeParentInoAndTouchCtime(ctx, db.UpdateInodeParentInoAndTouchCtimeParams{
				Ino:       oldIno,
				ParentIno: int64(params.NewParentIno),
			}); err != nil {
				return fmt.Errorf("update old dir %d parent_ino and touch ctime: %w", oldIno, err)
			}
		} else {
			if err := q.TouchInodeCtime(ctx, oldIno); err != nil {
				return fmt.Errorf("touch old inode %d ctime: %w", oldIno, err)
			}
		}

		// 维护父目录 nlink 与时间戳
		if !oldIsDir {
			// 源为非目录：父目录 nlink 恒定不变，仅刷新时间戳
			if params.OldParentIno == params.NewParentIno {
				if err := q.TouchInodeTimestamps(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("touch parent inode %d timestamps: %w", oldParent.Ino, err)
				}
			} else {
				if err := q.TouchInodeTimestamps(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("touch old parent inode %d timestamps: %w", oldParent.Ino, err)
				}
				if err := q.TouchInodeTimestamps(ctx, newParent.Ino); err != nil {
					return fmt.Errorf("touch new parent inode %d timestamps: %w", newParent.Ino, err)
				}
			}
		} else {
			// 源为目录
			if params.OldParentIno == params.NewParentIno {
				if hasNewEntry {
					// 同父目录下覆盖了空目录：被覆盖空目录移除，父目录 nlink 减 1
					if err := q.DecrementInodeNlinkAndTouch(ctx, oldParent.Ino); err != nil {
						return fmt.Errorf("decrement parent inode %d nlink and touch: %w", oldParent.Ino, err)
					}
				} else {
					// 同父目录下普通重命名：nlink 不变
					if err := q.TouchInodeTimestamps(ctx, oldParent.Ino); err != nil {
						return fmt.Errorf("touch parent inode %d timestamps: %w", oldParent.Ino, err)
					}
				}
			} else {
				// 跨父目录移动目录：源父目录 nlink 必须减 1
				if err := q.DecrementInodeNlinkAndTouch(ctx, oldParent.Ino); err != nil {
					return fmt.Errorf("decrement old parent inode %d nlink and touch: %w", oldParent.Ino, err)
				}

				if hasNewEntry {
					// 跨父目录下覆盖了空目录：+1（移入）与 -1（覆盖空目录）相抵，目标父目录 nlink 不变
					if err := q.TouchInodeTimestamps(ctx, newParent.Ino); err != nil {
						return fmt.Errorf("touch new parent inode %d timestamps: %w", newParent.Ino, err)
					}
				} else {
					// 跨父目录下未覆盖：目标父目录 nlink 加 1
					if err := q.IncrementInodeNlinkAndTouch(ctx, newParent.Ino); err != nil {
						return fmt.Errorf("increment new parent inode %d nlink and touch: %w", newParent.Ino, err)
					}
				}
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrAlreadyExists):
			return ErrAlreadyExists
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrNotEmpty):
			return ErrNotEmpty
		case errors.Is(err, ErrTooManyLinks):
			return ErrTooManyLinks
		case errors.Is(err, ErrNotSupported):
			return ErrNotSupported
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrCrossDevice):
			return ErrCrossDevice
		default:
			return fmt.Errorf("rename tx: %w", err)
		}
	}

	return nil
}

