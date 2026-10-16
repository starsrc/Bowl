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
	GetAttr(ctx context.Context, ino uint64) (*Inode, error)
	SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error)
	Mknod(ctx context.Context, params MknodParams) (*Inode, error)
	Mkdir(ctx context.Context, params MkdirParams) (*Inode, error)
	Symlink(ctx context.Context, params SymlinkParams) (*Inode, error)
	Readlink(ctx context.Context, ino uint64) (string, error)
	Link(ctx context.Context, params LinkParams) (*Inode, error)
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

// SetAttr 修改指定 Inode 的属性并实现读写事务悲观锁更新、POSIX 约束检查与时间戳联动。
func (m *manager) SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error) {
	// 事务前入参边界防御
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// 至少包含一项有效待更新属性
	if input.Mode == nil && input.UID == nil && input.GID == nil && input.Size == nil &&
		input.Atime == nil && input.Mtime == nil && !input.AtimeNow && !input.MtimeNow {
		return nil, ErrInvalidArgument
	}

	// 时间戳策略互斥防御
	if (input.Atime != nil && input.AtimeNow) || (input.Mtime != nil && input.MtimeNow) {
		return nil, ErrInvalidArgument
	}

	// Mode 范围边界防御（不超过 16 位 0o177777 = 65535）
	if input.Mode != nil && *input.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	// UID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.UID != nil && *input.UID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// GID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.GID != nil && *input.GID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// Size 范围边界防御（防止转换为 PostgreSQL BIGINT 符号位溢出）
	if input.Size != nil && *input.Size > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		current, err := q.GetInodeForUpdate(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", ino, err)
		}

		currentMode := uint32(current.Mode)
		fileType := currentMode & S_IFMT

		// POSIX 文件类型与 Size 约束
		if input.Size != nil {
			if fileType == S_IFDIR {
				return ErrIsDir
			}
			if fileType != S_IFREG {
				return ErrInvalidArgument
			}
		}

		// POSIX Mode 篡改防御、文件类型保留与特权位剥离
		targetMode := currentMode
		if input.Mode != nil {
			inputType := *input.Mode & S_IFMT
			if inputType != 0 && inputType != fileType {
				return ErrInvalidArgument
			}
			targetMode = fileType | (*input.Mode & PermMask)
		} else if fileType == S_IFREG && (input.UID != nil || input.GID != nil || input.Size != nil) {
			// 未显式指定 mode 时，常规文件修改 uid/gid/size 自动剥离 S_ISUID 和 S_ISGID
			targetMode &^= (S_ISUID | S_ISGID)
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
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		default:
			return nil, fmt.Errorf("setattr tx: %w", err)
		}
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

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
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

// Mkdir 在指定父目录下创建子目录并与父目录硬链接联动。
func (m *manager) Mkdir(ctx context.Context, params MkdirParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(params.Name) == 0 || len(params.Name) > MaxFileNameBytes || strings.ContainsAny(params.Name, "/\x00") {
		return nil, ErrInvalidArgument
	}
	if params.Mode == 0 || params.Mode > 65535 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFDIR {
		return nil, ErrInvalidArgument
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

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if params.Name == "." || params.Name == ".." {
			return ErrAlreadyExists
		}

		gid := params.GID
		targetMode := S_IFDIR | (params.Mode & PermMask)
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
			targetMode |= S_ISGID
		}

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(targetMode),
			Nlink: 2,
			Uid:   int32(params.UID),
			Gid:   int32(gid),
			Rdev:  0,
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

		if err := q.IncrementInodeNlinkAndTouch(ctx, parent.Ino); err != nil {
			return fmt.Errorf("increment parent inode %d nlink and touch: %w", parent.Ino, err)
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
			return nil, fmt.Errorf("mkdir tx: %w", err)
		}
	}

	return result, nil
}

// Symlink 在指定父目录下创建指向目标路径的符号链接节点。
func (m *manager) Symlink(ctx context.Context, params SymlinkParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(params.Name) == 0 || len(params.Name) > MaxFileNameBytes || strings.ContainsAny(params.Name, "/\x00") {
		return nil, ErrInvalidArgument
	}
	if len(params.Target) == 0 || len(params.Target) > MaxSymlinkTargetBytes || strings.ContainsRune(params.Target, 0) {
		return nil, ErrInvalidArgument
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

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if params.Name == "." || params.Name == ".." {
			return ErrAlreadyExists
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		newInode, err := q.CreateSymlinkInode(ctx, db.CreateSymlinkInodeParams{
			Mode:          int32(S_IFLNK | 0o777),
			Size:          int64(len(params.Target)),
			Nlink:         1,
			Uid:           int32(params.UID),
			Gid:           int32(gid),
			Rdev:          0,
			SymlinkTarget: pgtype.Text{String: params.Target, Valid: true},
		})
		if err != nil {
			return fmt.Errorf("create symlink inode: %w", err)
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
			return nil, fmt.Errorf("symlink tx: %w", err)
		}
	}

	return result, nil
}

// Readlink 获取符号链接节点指向的目标路径。
func (m *manager) Readlink(ctx context.Context, ino uint64) (string, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return "", ErrInvalidArgument
	}

	row, err := m.store.GetSymlinkTarget(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("get symlink target %d: %w", ino, err)
	}

	if (uint32(row.Mode) & S_IFMT) != S_IFLNK {
		return "", ErrInvalidArgument
	}

	if !row.SymlinkTarget.Valid {
		return "", ErrInvalidArgument
	}

	return row.SymlinkTarget.String, nil
}

// Link 为已有非目录 Inode 在目标父目录下创建硬链接。
func (m *manager) Link(ctx context.Context, params LinkParams) (*Inode, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(params.NewName) == 0 || len(params.NewName) > MaxFileNameBytes || strings.ContainsAny(params.NewName, "/\x00") {
		return nil, ErrInvalidArgument
	}

	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第一级：锁定目标父目录 new_parent_ino
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

		if params.NewName == "." || params.NewName == ".." {
			return ErrAlreadyExists
		}

		// 第二级：锁定源节点 ino
		src, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get source inode for update %d: %w", params.Ino, err)
		}

		// 禁止目录硬链接
		if (uint32(src.Mode) & S_IFMT) == S_IFDIR {
			return ErrPerm
		}

		// 禁止复活孤儿节点
		if src.Nlink == 0 {
			return ErrNotFound
		}

		// 插入新目录项
		err = q.CreateEntry(ctx, db.CreateEntryParams{
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

		// 刷新目标父目录 mtime 与 ctime (父目录 nlink 保持不变)
		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch new parent inode %d timestamps: %w", parent.Ino, err)
		}

		result = newInodeFromDB(updatedSrc)
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
		default:
			return nil, fmt.Errorf("link tx: %w", err)
		}
	}

	return result, nil
}



