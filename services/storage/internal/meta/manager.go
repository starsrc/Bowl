package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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
	Unlink(ctx context.Context, parentIno uint64, name string) error
	Rmdir(ctx context.Context, parentIno uint64, name string) error
	Rename(ctx context.Context, params RenameParams) error
	Create(ctx context.Context, params CreateParams) (*CreateResult, error)
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
			if fileType == S_IFLNK {
				return ErrInvalidArgument
			}
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
	if params.Mode > 65535 {
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

// errRetryTx 表示事务预读的目录项映射在加锁前发生并发漂移，需回滚并重试当前事务。
var errRetryTx = errors.New("retry transaction due to concurrent entry drift")

const maxTxRetries = 16

func (m *manager) execWriteTxWithRetry(ctx context.Context, fn func(q db.Querier) error) error {
	for range maxTxRetries {
		err := m.store.ExecTxWithOptions(ctx, fn)
		if !errors.Is(err, errRetryTx) {
			return err
		}
	}
	return fmt.Errorf("exceeded max transaction retries (%d): %w", maxTxRetries, errRetryTx)
}

// uniqueSortedInos 将一组 Inode 号按全局升序排列并去重，确保所有多 Inode 事务遵循单一全局加锁顺序。
func uniqueSortedInos(inos ...int64) []int64 {
	slices.Sort(inos)
	return slices.Compact(inos)
}

func lockAndValidateParents(ctx context.Context, q db.Querier, oldParentIno, newParentIno uint64) (db.Inode, db.Inode, error) {
	var oldParent, newParent db.Inode
	for _, ino := range uniqueSortedInos(int64(oldParentIno), int64(newParentIno)) {
		row, err := q.GetInodeForUpdate(ctx, ino)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return db.Inode{}, db.Inode{}, ErrNotFound
			}
			return db.Inode{}, db.Inode{}, fmt.Errorf("get parent inode %d for update: %w", ino, err)
		}
		if (uint32(row.Mode) & S_IFMT) != S_IFDIR {
			return db.Inode{}, db.Inode{}, ErrNotDir
		}
		if row.Nlink == 0 {
			return db.Inode{}, db.Inode{}, ErrNotFound
		}
		if ino == int64(oldParentIno) {
			oldParent = row
		}
		if ino == int64(newParentIno) {
			newParent = row
		}
	}
	return oldParent, newParent, nil
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
		if params.NewName == "." || params.NewName == ".." {
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
				// 禁止目录硬链接
				if (uint32(row.Mode) & S_IFMT) == S_IFDIR {
					return ErrPerm
				}
				// 禁止复活孤儿节点
				if row.Nlink == 0 {
					return ErrNotFound
				}
			}
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

// Unlink 删除指定父目录下的非目录项（解链）。
func (m *manager) Unlink(ctx context.Context, parentIno uint64, name string) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if len(name) == 0 || len(name) > MaxFileNameBytes || strings.ContainsAny(name, "/\x00") {
		return ErrInvalidArgument
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if name == "." || name == ".." {
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

		// 递减子节点硬链接数并刷新 ctime (即使 nlink 降为 0 也保留 Inode 行)
		if _, err := q.DecrementInodeNlinkAndTouchCtime(ctx, child.Ino); err != nil {
			return fmt.Errorf("decrement child inode %d nlink and touch ctime: %w", child.Ino, err)
		}

		// 刷新父目录 mtime 与 ctime (父目录 nlink 保持不变)
		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
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
func (m *manager) Rmdir(ctx context.Context, parentIno uint64, name string) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if len(name) == 0 || len(name) > MaxFileNameBytes || strings.ContainsAny(name, "/\x00") {
		return ErrInvalidArgument
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if name == "." || name == ".." {
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
			if name == "." {
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
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		default:
			return fmt.Errorf("rmdir tx: %w", err)
		}
	}

	return nil
}

// Rename 执行原子文件/目录重命名或移动（支持覆盖已存在的目标项）。
func (m *manager) Rename(ctx context.Context, params RenameParams) error {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if len(params.OldName) == 0 || len(params.OldName) > MaxFileNameBytes || strings.ContainsAny(params.OldName, "/\x00") {
		return ErrInvalidArgument
	}
	if len(params.NewName) == 0 || len(params.NewName) > MaxFileNameBytes || strings.ContainsAny(params.NewName, "/\x00") {
		return ErrInvalidArgument
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if params.OldName == "." || params.OldName == ".." || params.NewName == "." || params.NewName == ".." {
			if _, _, err := lockAndValidateParents(ctx, q, params.OldParentIno, params.NewParentIno); err != nil {
				return err
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
		if params.OldParentIno == params.NewParentIno && params.OldName == params.NewName {
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
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("get new entry %d/%s: %w", params.NewParentIno, params.NewName, err)
		}

		inosToLock := []int64{int64(params.OldParentIno), int64(params.NewParentIno)}
		if !hasNewEntry || oldIno != newIno {
			inosToLock = append(inosToLock, oldIno)
			if hasNewEntry {
				inosToLock = append(inosToLock, newIno)
			}
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

		// 同节点硬链接短路 (POSIX No-op)
		if hasNewEntry && oldIno == newIno {
			return nil
		}

		oldParent := locked[int64(params.OldParentIno)]
		newParent := locked[int64(params.NewParentIno)]
		oldInode := locked[oldIno]
		newInode := locked[newIno]

		oldIsDir := (uint32(oldInode.Mode) & S_IFMT) == S_IFDIR

		// 祖先环检测：当源节点为目录且跨目录移动时
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

		// 异节点覆盖兼容矩阵校验
		if hasNewEntry {
			newIsDir := (uint32(newInode.Mode) & S_IFMT) == S_IFDIR
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
				// 被覆盖目标为空目录：nlink 直接置 0，刷新 ctime (保留 Inode 行)
				if _, err := q.ClearInodeNlinkAndTouchCtime(ctx, newIno); err != nil {
					return fmt.Errorf("clear target dir %d nlink: %w", newIno, err)
				}
			} else {
				// 被覆盖目标为非目录：nlink 递减 1，刷新 ctime (保留 Inode 行)
				if _, err := q.DecrementInodeNlinkAndTouchCtime(ctx, newIno); err != nil {
					return fmt.Errorf("decrement target file %d nlink: %w", newIno, err)
				}
			}
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

		// 刷新源节点 ctime
		if err := q.TouchInodeCtime(ctx, oldIno); err != nil {
			return fmt.Errorf("touch old inode %d ctime: %w", oldIno, err)
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
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrNotEmpty):
			return ErrNotEmpty
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		default:
			return fmt.Errorf("rename tx: %w", err)
		}
	}

	return nil
}

// Create 在指定父目录下原子创建常规文件并分配初始文件句柄。
func (m *manager) Create(ctx context.Context, params CreateParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(params.Name) == 0 || len(params.Name) > MaxFileNameBytes || strings.ContainsAny(params.Name, "/\x00") {
		return nil, ErrInvalidArgument
	}
	if params.Mode > 0xffff {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return nil, ErrInvalidArgument
	}

	targetMode := S_IFREG | (params.Mode & PermMask)

	var result *CreateResult
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
			Mode:  int32(targetMode),
			Nlink: 1,
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

		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:   newInode.Ino,
			Flags: int32(params.Flags),
		})
		if err != nil {
			return fmt.Errorf("create file handle for inode %d: %w", newInode.Ino, err)
		}

		result = &CreateResult{
			Inode:     newInodeFromDB(newInode),
			Fh:        uint64(handle.Fh),
			OpenFlags: 0,
		}
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
			return nil, fmt.Errorf("create tx: %w", err)
		}
	}

	return result, nil
}

