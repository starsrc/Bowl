package meta

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Lookup 执行单级目录项查找与块属性动态推导。
func (m *manager) Lookup(ctx context.Context, parentIno uint64, name []byte) (*Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(name); err != nil {
		return nil, err
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

		result = newInodeFromDB(target)
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

	return newInodeFromDB(target), nil
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
		return nil, ErrInvalidArgument
	}

	return row.SymlinkTarget, nil
}
