package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// readlinkEmptyTargetError 同时支持 ErrIO（STARSRC-313）与向后兼容 ErrInvalidArgument。
type readlinkEmptyTargetError struct{}

func (e readlinkEmptyTargetError) Error() string {
	return ErrIO.Error()
}

func (e readlinkEmptyTargetError) Is(target error) bool {
	return target == ErrIO || target == ErrInvalidArgument
}

// Lookup 执行单级目录项查找与块属性动态推导。
func (m *manager) Lookup(ctx context.Context, parentIno uint64, name []byte) (*Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(name) > MaxFileNameBytes {
		return nil, ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return nil, ErrInvalidArgument
	}

	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		parent, err := q.GetInode(ctx, int64(parentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode %d: %w", parentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		// 处理 ".."（STARSRC-244, STARSRC-342）
		// 必须在 parent.Nlink == 0 拦截之前处理 ".."（支持 rmdir-whilst-open 场景下已解链孤儿目录 Lookup("..") 仍能找到原父目录）
		if bytes.Equal(name, []byte("..")) {
			if parentIno == RootIno {
				result = m.newInode(parent)
				return nil
			}
			if parent.ParentIno > 0 && uint64(parent.ParentIno) != parentIno {
				grandParent, err := q.GetInode(ctx, parent.ParentIno)
				if err == nil {
					result = m.newInode(grandParent)
					return nil
				}
				if !errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("get grandparent inode %d: %w", parent.ParentIno, err)
				}
				// 若直接点查原父节点未命中，回退通过 entries 反查
				gp, err := q.GetParentInode(ctx, int64(parentIno))
				if err == nil {
					result = m.newInode(gp)
					return nil
				}
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get parent inode fallback for %d: %w", parentIno, err)
			}
			// 若 parent.ParentIno <= 0（兼容老数据回退）
			gp, err := q.GetParentInode(ctx, int64(parentIno))
			if err == nil {
				result = m.newInode(gp)
				return nil
			}
			if errors.Is(err, pgx.ErrNoRows) {
				if parent.Nlink > 0 {
					result = m.newInode(parent)
					return nil
				}
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode fallback for %d: %w", parentIno, err)
		}

		// 孤儿父目录拦截（STARSRC-323）：父目录已解链且非查找 ".." 时返回 ErrNotFound
		if parent.Nlink == 0 {
			return ErrNotFound
		}

		// 处理 "."（STARSRC-244）
		if bytes.Equal(name, []byte(".")) {
			result = m.newInode(parent)
			return nil
		}

		// 处理空名称 ""（STARSRC-335）：父目录校验合法后对空名称返回 ErrNotFound (ENOENT)
		if len(name) == 0 {
			return ErrNotFound
		}

		// 常规目录项查找
		target, err := q.GetInodeByEntry(ctx, db.GetInodeByEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode by entry %s in parent %d: %w", name, parentIno, err)
		}

		result = m.newInode(target)
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		default:
			return nil, fmt.Errorf("lookup tx: %w", err)
		}
	}
	return result, nil
}

// GetAttr 执行单节点单语句直查与属性推导，支持所有文件类型与孤儿节点。
func (m *manager) GetAttr(ctx context.Context, ino uint64) (*Inode, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	target, err := m.store.GetInode(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get inode %d: %w", ino, err)
	}

	// 孤儿节点句柄校验（STARSRC-279）：nlink == 0 时必须持存活跃打开句柄，否则拒绝访问
	if target.Nlink == 0 {
		hasHandles, err := m.store.HasFileHandlesByIno(ctx, int64(ino))
		if err != nil {
			return nil, fmt.Errorf("check handles for orphan inode %d: %w", ino, err)
		}
		if !hasHandles {
			return nil, ErrNotFound
		}
	}

	return m.newInode(target), nil
}

// Readlink 获取符号链接节点指向的目标路径。
func (m *manager) Readlink(ctx context.Context, ino uint64) ([]byte, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	row, err := m.store.GetSymlinkTarget(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get symlink target %d: %w", ino, err)
	}

	if (uint32(row.Mode) & S_IFMT) != S_IFLNK {
		return nil, ErrInvalidArgument
	}

	if len(row.SymlinkTarget) == 0 {
		return nil, readlinkEmptyTargetError{}
	}

	if m.atimeMode != AtimeModeNoAtime {
		if inode, err := m.store.GetInode(ctx, int64(ino)); err == nil {
			if m.shouldTouchAtime(inode.Atime.Time, inode.Mtime.Time, inode.Ctime.Time, time.Now()) {
				_ = m.store.TouchInodeAtime(ctx, int64(ino))
			}
		}
	}

	return row.SymlinkTarget, nil
}
