package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Manager 定义元数据领域层管理器接口。
type Manager interface {
	Lookup(ctx context.Context, parentIno uint64, name string) (*Inode, error)
	SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error)
	Mknod(ctx context.Context, params MknodParams) (*Inode, error)
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
	if parentIno == 0 || len(name) == 0 || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
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

func newInodeFromDB(target db.Inode) *Inode {
	var blocks uint64
	if target.Size > 0 {
		blocks = (uint64(target.Size) + 511) / 512
	}

	return &Inode{
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
}

// SetAttr 修改指定 Inode 的属性并实现读写事务悲观锁更新与时间戳联动。
func (m *manager) SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error) {
	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		current, err := q.GetInodeForUpdate(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", ino, err)
		}

		targetMode := uint32(current.Mode)
		if input.Mode != nil {
			targetMode = (targetMode & S_IFMT) | (*input.Mode & PermMask)
		}

		targetUID := uint32(current.Uid)
		if input.UID != nil {
			targetUID = *input.UID
		}

		targetGID := uint32(current.Gid)
		if input.GID != nil {
			targetGID = *input.GID
		}

		targetSize := uint64(current.Size)
		if input.Size != nil {
			targetSize = *input.Size
		}

		var setAtimeNow bool
		var atime pgtype.Timestamptz
		if input.AtimeNow {
			setAtimeNow = true
		} else if input.Atime != nil {
			atime = pgtype.Timestamptz{Time: *input.Atime, Valid: true}
		} else {
			atime = current.Atime
		}

		var setMtimeNow bool
		var mtime pgtype.Timestamptz
		if input.MtimeNow || (input.Size != nil && input.Mtime == nil) {
			setMtimeNow = true
		} else if input.Mtime != nil {
			mtime = pgtype.Timestamptz{Time: *input.Mtime, Valid: true}
		} else {
			mtime = current.Mtime
		}

		updated, err := q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
			Ino:         int64(ino),
			Mode:        int32(targetMode),
			Size:        int64(targetSize),
			Uid:         int32(targetUID),
			Gid:         int32(targetGID),
			SetAtimeNow: setAtimeNow,
			Atime:       atime,
			SetMtimeNow: setMtimeNow,
			Mtime:       mtime,
		})
		if err != nil {
			return fmt.Errorf("update inode attr %d: %w", ino, err)
		}

		result = newInodeFromDB(updated)
		return nil
	})

	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("setattr tx: %w", err)
	}

	return result, nil
}

// Mknod 创建特殊文件或普通文件节点并与父目录联动。
func (m *manager) Mknod(ctx context.Context, params MknodParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(params.Name) == 0 || len(params.Name) > MaxFileNameBytes || strings.ContainsAny(params.Name, "/\x00") {
		return nil, ErrInvalidArgument
	}
	if params.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	switch modeType {
	case S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK:
	default:
		return nil, ErrInvalidArgument
	}

	rdev := uint32(0)
	if modeType == S_IFCHR || modeType == S_IFBLK {
		rdev = params.Rdev
	}

	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if params.Name == "." || params.Name == ".." {
			return ErrAlreadyExists
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(params.Mode),
			Nlink: 1,
			Uid:   int32(params.UID),
			Gid:   int32(gid),
			Rdev:  int64(rdev),
		})
		if err != nil {
			return fmt.Errorf("create inode: %w", err)
		}

		err = q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.ParentIno),
			Name:      params.Name,
			Ino:       newInode.Ino,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		result = newInodeFromDB(newInode)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		default:
			return nil, fmt.Errorf("mknod tx: %w", err)
		}
	}

	return result, nil
}


