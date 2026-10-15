package meta

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Manager 定义元数据领域层管理器接口。
type Manager interface {
	Lookup(ctx context.Context, parentIno uint64, name string) (*Inode, error)
}

type manager struct {
	store db.Store
}

var _ Manager = (*manager)(nil)

// New 创建 Manager 实例。
func New(store db.Store) Manager {
	return &manager{
		store: store,
	}
}

// Module 提供 meta 依赖注入模块。
var Module = fx.Module("meta",
	fx.Provide(New),
)

// Lookup 执行单级目录项查找与块属性动态推导。
func (m *manager) Lookup(ctx context.Context, parentIno uint64, name string) (*Inode, error) {
	if parentIno == 0 || len(name) == 0 || len(name) > 255 || strings.Contains(name, "/") {
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

		target, err := q.GetInodeByEntry(ctx, db.GetInodeByEntryParams{
			ParentIno: int64(parentIno),
			Name:      name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode by entry %q in parent %d: %w", name, parentIno, err)
		}

		var blocks uint64
		if target.Size > 0 {
			blocks = (uint64(target.Size) + 511) / 512
		}

		result = &Inode{
			Ino:        uint64(target.Ino),
			Generation: uint64(target.Generation),
			Attr: Attribute{
				Mode:    uint32(target.Mode),
				Size:    uint64(target.Size),
				Nlink:   uint32(target.Nlink),
				UID:     uint32(target.Uid),
				GID:     uint32(target.Gid),
				Rdev:    uint32(target.Rdev),
				Blksize: 4096,
				Blocks:  blocks,
				Atime:   target.Atime.Time,
				Mtime:   target.Mtime.Time,
				Ctime:   target.Ctime.Time,
			},
		}
		return nil
	}, tx.WithReadOnly())

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		default:
			return nil, fmt.Errorf("lookup tx: %w", err)
		}
	}
	return result, nil
}
