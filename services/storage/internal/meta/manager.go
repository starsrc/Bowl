package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// validateEntryName 校验目录项名称合法性。
// len(name) > MaxFileNameBytes 时返回 ErrNameTooLong；
// len(name) == 0 或包含 '/' 或 '\x00' 时返回 ErrInvalidArgument。
func validateEntryName(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isDotOrDotDot 判断目录项名称是否为 "." 或 ".."。
func isDotOrDotDot(name []byte) bool {
	return bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte(".."))
}

// Manager 定义元数据领域层管理器接口。
type Manager interface {
	Lookup(ctx context.Context, parentIno uint64, name []byte) (*Inode, error)
	GetAttr(ctx context.Context, ino uint64) (*Inode, error)
	SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error)
	Mknod(ctx context.Context, params MknodParams) (*Inode, error)
	Mkdir(ctx context.Context, params MkdirParams) (*Inode, error)
	Symlink(ctx context.Context, params SymlinkParams) (*Inode, error)
	Readlink(ctx context.Context, ino uint64) ([]byte, error)
	Link(ctx context.Context, params LinkParams) (*Inode, error)
	Unlink(ctx context.Context, parentIno uint64, name []byte) error
	Rmdir(ctx context.Context, parentIno uint64, name []byte) error
	Rename(ctx context.Context, params RenameParams) error
	Create(ctx context.Context, params CreateParams) (*CreateResult, error)
	Open(ctx context.Context, params OpenParams) (*OpenResult, error)
	Flush(ctx context.Context, params FlushParams) error
	Fsync(ctx context.Context, params FsyncParams) error
	Release(ctx context.Context, params ReleaseParams) error
	OpenDir(ctx context.Context, params OpenDirParams) (*OpenDirResult, error)
	ReleaseDir(ctx context.Context, params ReleaseDirParams) error
	ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error)
	ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error)
	Fallocate(ctx context.Context, params FallocateParams) error
	SetXattr(ctx context.Context, params SetXattrParams) error
	GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error)
	ListXattr(ctx context.Context, ino uint64) ([][]byte, error)
	RemoveXattr(ctx context.Context, ino uint64, name []byte) error
	GetLk(ctx context.Context, params GetLkParams) (*FileLock, error)
	SetLk(ctx context.Context, params SetLkParams) error
	SetLkw(ctx context.Context, params SetLkwParams) error
}

type manager struct {
	store           db.Store
	entryTimeout    float64
	attrTimeout     float64
	sessionLeaseTTL time.Duration
	lockNotifyMu    sync.Mutex
	lockNotifyChans map[uint64]chan struct{}
}

var _ Manager = (*manager)(nil)

// Option 定义 Manager 的配置选项。
type Option func(*manager)

// WithSessionLeaseTTL 配置客户端会话租约的续期时长（TTL <= 0 时忽略并保持默认值）。
func WithSessionLeaseTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl > 0 {
			m.sessionLeaseTTL = ttl
		}
	}
}

// New 创建 Manager 实例。
func New(store db.Store, opts ...Option) Manager {
	m := &manager{
		store:           store,
		entryTimeout:    DefaultEntryTimeoutSeconds,
		attrTimeout:     DefaultAttrTimeoutSeconds,
		sessionLeaseTTL: DefaultSessionLeaseTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	return m
}

// Module 提供 meta 依赖注入模块。
var Module = fx.Module("meta",
	fx.Provide(func(store db.Store) Manager {
		return New(store)
	}),
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
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
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

		if isDotOrDotDot(params.Name) {
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
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
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

		if isDotOrDotDot(params.Name) {
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
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if len(params.Target) == 0 || bytes.IndexByte(params.Target, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	if len(params.Target) > MaxSymlinkTargetBytes {
		return nil, ErrNameTooLong
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

		if isDotOrDotDot(params.Name) {
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
			SymlinkTarget: params.Target,
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
	if err := validateEntryName(params.NewName); err != nil {
		return nil, err
	}

	var result *Inode
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
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
func (m *manager) Unlink(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryName(name); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if isDotOrDotDot(name) {
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
func (m *manager) Rmdir(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryName(name); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if isDotOrDotDot(name) {
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

// Rename 执行原子文件/目录重命名、移动或交换（支持 FUSE_RENAME2 标志位）。
func (m *manager) Rename(ctx context.Context, params RenameParams) error {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Flags != 0 && params.Flags != RenameNoReplace && params.Flags != RenameExchange {
		return ErrInvalidArgument
	}
	if err := validateEntryName(params.OldName); err != nil {
		return err
	}
	if err := validateEntryName(params.NewName); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if isDotOrDotDot(params.OldName) || isDotOrDotDot(params.NewName) {
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

		// RenameNoReplace：只要目标 Entry 已存在（包含同一 Inode 的不同硬链接与异 Inode），一律返回 ErrAlreadyExists
		if params.Flags == RenameNoReplace && hasNewEntry {
			return ErrAlreadyExists
		}

		// 同节点硬链接短路 (POSIX No-op / RenameExchange 同节点短路)
		if hasNewEntry && oldIno == newIno {
			return nil
		}

		oldParent := locked[int64(params.OldParentIno)]
		newParent := locked[int64(params.NewParentIno)]
		oldInode := locked[oldIno]
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

			// 刷新双节点 ctime
			if err := q.TouchInodeCtime(ctx, oldIno); err != nil {
				return fmt.Errorf("touch old inode %d ctime: %w", oldIno, err)
			}
			if err := q.TouchInodeCtime(ctx, newIno); err != nil {
				return fmt.Errorf("touch new inode %d ctime: %w", newIno, err)
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
		case errors.Is(err, ErrAlreadyExists):
			return ErrAlreadyExists
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

func validateOptionalSessionID(sessionID *uint64) error {
	if sessionID != nil && (*sessionID == 0 || *sessionID > OffsetMax) {
		return ErrInvalidArgument
	}
	return nil
}

func toPgSessionID(sessionID *uint64) pgtype.Int8 {
	if sessionID == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{
		Int64: int64(*sessionID),
		Valid: true,
	}
}

// Create 在指定父目录下原子创建常规文件并分配初始文件句柄。
func (m *manager) Create(ctx context.Context, params CreateParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
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

		if isDotOrDotDot(params.Name) {
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

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       newInode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
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

// Open 打开指定 Inode 节点并分配文件句柄 (FileHandle)。
func (m *manager) Open(ctx context.Context, params OpenParams) (*OpenResult, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	var result *OpenResult
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		fileType := uint32(inode.Mode) & S_IFMT
		if fileType == S_IFDIR {
			return ErrIsDir
		}
		if fileType == S_IFLNK {
			return ErrInvalidArgument
		}

		if inode.Nlink == 0 {
			hasHandles, err := q.HasFileHandlesByIno(ctx, inode.Ino)
			if err != nil {
				return fmt.Errorf("check has file handles for inode %d: %w", inode.Ino, err)
			}
			if !hasHandles {
				return ErrNotFound
			}
		}

		if (params.Flags & O_TRUNC) != 0 {
			if (params.Flags & O_ACCMODE) == O_RDONLY {
				return ErrInvalidArgument
			}
			if fileType == S_IFREG {
				truncatedMode := uint32(inode.Mode) &^ (S_ISUID | S_ISGID)
				_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
					Ino:         inode.Ino,
					Mode:        int32(truncatedMode),
					Size:        0,
					Uid:         inode.Uid,
					Gid:         inode.Gid,
					SetAtimeNow: false,
					Atime:       inode.Atime,
					SetMtimeNow: true,
					Mtime:       pgtype.Timestamptz{},
				})
				if err != nil {
					return fmt.Errorf("truncate inode attr for inode %d: %w", inode.Ino, err)
				}
			}
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       inode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			return fmt.Errorf("create file handle for inode %d: %w", inode.Ino, err)
		}

		result = &OpenResult{
			Fh:        uint64(handle.Fh),
			OpenFlags: 0,
		}
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
			return nil, fmt.Errorf("open tx: %w", err)
		}
	}

	return result, nil
}

// checkFileHandleReadOnly 在只读 RepeatableRead 事务内校验 Inode 与 FileHandle 归属有效性。
func (m *manager) checkFileHandleReadOnly(ctx context.Context, ino uint64, fh uint64, opName string) error {
	if ino == 0 || ino > math.MaxInt64 || fh == 0 || fh > math.MaxInt64 {
		return ErrInvalidArgument
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(fh),
			Ino: int64(ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", fh, ino, err)
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("%s tx: %w", opName, err)
		}
	}

	return nil
}

// Flush 冲刷会话句柄并校验合法性（不销毁文件句柄）。
// 遵循 ADR-0012：
// 1. 当未传入 LockOwner 与 SessionID（LockOwner == nil && SessionID == nil）时，保持只读事务校验 (ino, fh) 合法性；
// 2. 当传入 LockOwner 或 SessionID 时，在 READ COMMITTED 写事务中执行 GetInodeForUpdate(ino)、校验 (ino, fh) 有效后，
//    若携带 SessionID 则续期会话并触发孤儿节点兜底回收；若携带 LockOwner 则原子删除该 (session_id, ino, *lock_owner)（若未传 session_id 则按 (ino, *lock_owner)）持有的全部 POSIX 记录锁（lk_flags == 0），
//    无论这些锁最初是通过哪个 FileHandle 申请或合并的；Flush 不销毁 FileHandle，且绝不清理 BSD flock 锁。
func (m *manager) Flush(ctx context.Context, params FlushParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	if params.LockOwner == nil && params.SessionID == nil {
		return m.checkFileHandleReadOnly(ctx, params.Ino, params.Fh, "flush")
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(params.Fh),
			Ino: int64(params.Ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		if params.LockOwner != nil {
			if params.SessionID != nil {
				_, err = q.DeletePosixFileLocksBySessionAndOwner(ctx, db.DeletePosixFileLocksBySessionAndOwnerParams{
					SessionID: int64(*params.SessionID),
					Ino:       int64(params.Ino),
					LockOwner: int64(*params.LockOwner),
				})
				if err != nil {
					return fmt.Errorf("delete posix file locks by session and owner (session_id=%d, ino=%d, lock_owner=%d): %w", *params.SessionID, params.Ino, *params.LockOwner, err)
				}
			} else {
				_, err = q.DeletePosixFileLocksByOwner(ctx, db.DeletePosixFileLocksByOwnerParams{
					Ino:       int64(params.Ino),
					LockOwner: int64(*params.LockOwner),
				})
				if err != nil {
					return fmt.Errorf("delete posix file locks by owner (ino=%d, lock_owner=%d): %w", params.Ino, *params.LockOwner, err)
				}
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("flush tx: %w", err)
		}
	}

	if params.LockOwner != nil {
		m.notifyLockWaiters(params.Ino)
	}
	return nil
}

// Fsync 同步文件节点与句柄关联的状态。
// 注：params.Datasync 保留用于 Linux FUSE / POSIX 协议契约对齐，当前元数据在 PostgreSQL 事务提交时已完成持久化。
func (m *manager) Fsync(ctx context.Context, params FsyncParams) error {
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	return m.checkFileHandleReadOnly(ctx, params.Ino, params.Fh, "fsync")
}

// Release 关闭并销毁文件句柄，按标志位清理 BSD flock 句柄锁，且在孤儿节点无活跃句柄时执行最终物理回收。
// 遵循 ADR-0012：
// 1. 静态校验 ino、fh、release_flags（仅允许 0 或 FuseReleaseFlockUnlock）与可选 session_id 范围；
// 2. 在 READ COMMITTED 写事务中锁定目标 Inode 并删除 file_handles(fh)；
// 3. 若传入 session_id，续期客户端会话并执行无引用孤儿节点兜底回收；仅当 (release_flags & FuseReleaseFlockUnlock) != 0 时，原子删除该 (session_id, ino, fh)（未传 session_id 时按 (ino, fh)）绑定的 BSD flock 锁（lk_flags == FuseLkFlock），绝不越权清理 POSIX 记录锁；
// 4. 若 nlink == 0 且无剩余活跃句柄，执行孤儿节点物理回收（数据库 ON DELETE CASCADE 自动清理残留锁）。
func (m *manager) Release(ctx context.Context, params ReleaseParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if (params.ReleaseFlags &^ FuseReleaseFlockUnlock) != 0 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.DeleteFileHandle(ctx, db.DeleteFileHandleParams{
			Fh:  int64(params.Fh),
			Ino: int64(params.Ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("delete file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		if (params.ReleaseFlags & FuseReleaseFlockUnlock) != 0 {
			if params.SessionID != nil {
				_, err = q.DeleteFlockFileLocksBySessionAndFh(ctx, db.DeleteFlockFileLocksBySessionAndFhParams{
					SessionID: int64(*params.SessionID),
					Ino:       int64(params.Ino),
					Fh:        int64(params.Fh),
				})
				if err != nil {
					return fmt.Errorf("delete flock file locks by session and fh (session_id=%d, ino=%d, fh=%d): %w", *params.SessionID, params.Ino, params.Fh, err)
				}
			} else {
				_, err = q.DeleteFlockFileLocksByFh(ctx, db.DeleteFlockFileLocksByFhParams{
					Ino: int64(params.Ino),
					Fh:  int64(params.Fh),
				})
				if err != nil {
					return fmt.Errorf("delete flock file locks by fh (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
				}
			}
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, params.Ino); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("release tx: %w", err)
		}
	}

	if (params.ReleaseFlags & FuseReleaseFlockUnlock) != 0 {
		m.notifyLockWaiters(params.Ino)
	}
	return nil
}

// OpenDir 打开指定目录 Inode 节点并分配目录句柄 (DirHandle)。
func (m *manager) OpenDir(ctx context.Context, params OpenDirParams) (*OpenDirResult, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || (params.Flags&O_ACCMODE) != O_RDONLY || (params.Flags&O_TRUNC) != 0 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	var result *OpenDirResult
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if inode.Nlink == 0 {
			hasHandles, err := q.HasFileHandlesByIno(ctx, inode.Ino)
			if err != nil {
				return fmt.Errorf("check has file handles for inode %d: %w", inode.Ino, err)
			}
			if !hasHandles {
				return ErrNotFound
			}
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       inode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			return fmt.Errorf("create file handle for inode %d: %w", inode.Ino, err)
		}

		result = &OpenDirResult{
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
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		default:
			return nil, fmt.Errorf("opendir tx: %w", err)
		}
	}

	return result, nil
}

// ReleaseDir 关闭并销毁目录句柄，且在孤儿目录无活跃句柄时执行最终物理回收。
func (m *manager) ReleaseDir(ctx context.Context, params ReleaseDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		_, err = q.DeleteFileHandle(ctx, db.DeleteFileHandleParams{
			Fh:  int64(params.Fh),
			Ino: int64(params.Ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("delete file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, params.Ino); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("releasedir tx: %w", err)
		}
	}

	return nil
}

// reclaimOrphanIfNoHandles 检查孤儿节点是否存在未释放句柄，若无任何句柄则执行物理删除。
func (m *manager) reclaimOrphanIfNoHandles(ctx context.Context, q db.Querier, ino uint64) error {
	hasRemaining, err := q.HasFileHandlesByIno(ctx, int64(ino))
	if err != nil {
		return fmt.Errorf("check remaining file handles for inode %d: %w", ino, err)
	}
	if !hasRemaining {
		if err := q.DeleteInode(ctx, int64(ino)); err != nil {
			return fmt.Errorf("delete orphan inode %d: %w", ino, err)
		}
	}
	return nil
}

func newInodeFromListDirRow(row db.ListDirEntriesRow) *Inode {
	var blocks uint64
	if row.Size > 0 {
		blocks = (uint64(row.Size) + 511) / 512
	}

	return &Inode{
		Ino:        uint64(row.Ino),
		Generation: uint64(row.Generation),
		Attr: Attribute{
			Mode:    uint32(row.Mode),
			Size:    uint64(row.Size),
			Nlink:   uint32(row.Nlink),
			UID:     uint32(row.Uid),
			GID:     uint32(row.Gid),
			Rdev:    uint32(row.Rdev),
			Blksize: 4096,
			Blocks:  blocks,
			Atime:   row.Atime.Time,
			Mtime:   row.Mtime.Time,
			Ctime:   row.Ctime.Time,
		},
	}
}

// validateDirHandle 校验目录会话有效性，确认 Inode 存在、为目录且 FileHandle 属于该 Inode。
func (m *manager) validateDirHandle(ctx context.Context, q db.Querier, ino, fh uint64) (*db.Inode, error) {
	dirInode, err := q.GetInode(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get dir inode %d: %w", ino, err)
	}

	if (uint32(dirInode.Mode) & S_IFMT) != S_IFDIR {
		return nil, ErrNotDir
	}

	_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
		Fh:  int64(fh),
		Ino: int64(ino),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBadFileHandle
		}
		return nil, fmt.Errorf("get file handle (fh=%d, ino=%d): %w", fh, ino, err)
	}

	return &dirInode, nil
}

// ReadDir 读取指定目录的子项流（基本条目），支持确定性分页游标与 . / .. 合成。
func (m *manager) ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	var entries []DirEntry
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		dirInode, err := m.validateDirHandle(ctx, q, params.Ino, params.Fh)
		if err != nil {
			return err
		}

		if dirInode.Nlink == 0 {
			entries = []DirEntry{}
			return nil
		}

		rem := int(params.Limit)
		entries = make([]DirEntry, 0, min(rem, 16))

		if params.Offset < 1 && rem > 0 {
			entries = append(entries, DirEntry{
				Ino:        uint64(dirInode.Ino),
				Name:       []byte("."),
				Mode:       uint32(dirInode.Mode),
				NextOffset: 1,
			})
			rem--
		}

		if params.Offset < 2 && rem > 0 {
			parentInode, err := q.GetParentInode(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					parentInode = *dirInode
				} else {
					return fmt.Errorf("get parent inode for %d: %w", params.Ino, err)
				}
			}
			entries = append(entries, DirEntry{
				Ino:        uint64(parentInode.Ino),
				Name:       []byte(".."),
				Mode:       uint32(parentInode.Mode),
				NextOffset: 2,
			})
			rem--
		}

		if rem > 0 {
			var sqlOffset int64
			if params.Offset >= 2 {
				sqlOffset = int64(params.Offset - 2)
			}
			rows, err := q.ListDirEntries(ctx, db.ListDirEntriesParams{
				ParentIno: int64(params.Ino),
				Limit:     int32(rem),
				Offset:    sqlOffset,
			})
			if err != nil {
				return fmt.Errorf("list dir entries for parent %d (limit=%d, offset=%d): %w", params.Ino, rem, sqlOffset, err)
			}

			for i, row := range rows {
				entries = append(entries, DirEntry{
					Ino:        uint64(row.Ino),
					Name:       row.Name,
					Mode:       uint32(row.Mode),
					NextOffset: uint64(sqlOffset) + uint64(i) + 3,
				})
			}
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("readdir tx: %w", err)
		}
	}

	if entries == nil {
		entries = []DirEntry{}
	}
	return entries, nil
}

// ReadDirPlus 读取指定目录的子项流并附带完整 Inode 属性与缓存时效，支持确定性分页游标与 . / .. 合成。
func (m *manager) ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	var entries []DirEntryPlus
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		dirInode, err := m.validateDirHandle(ctx, q, params.Ino, params.Fh)
		if err != nil {
			return err
		}

		if dirInode.Nlink == 0 {
			entries = []DirEntryPlus{}
			return nil
		}

		rem := int(params.Limit)
		entries = make([]DirEntryPlus, 0, min(rem, 16))

		if params.Offset < 1 && rem > 0 {
			entries = append(entries, DirEntryPlus{
				Ino:                 uint64(dirInode.Ino),
				Name:                []byte("."),
				NextOffset:          1,
				Inode:               newInodeFromDB(*dirInode),
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  m.attrTimeout,
			})
			rem--
		}

		if params.Offset < 2 && rem > 0 {
			parentInode, err := q.GetParentInode(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					parentInode = *dirInode
				} else {
					return fmt.Errorf("get parent inode for %d: %w", params.Ino, err)
				}
			}
			entries = append(entries, DirEntryPlus{
				Ino:                 uint64(parentInode.Ino),
				Name:                []byte(".."),
				NextOffset:          2,
				Inode:               newInodeFromDB(parentInode),
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  m.attrTimeout,
			})
			rem--
		}

		if rem > 0 {
			var sqlOffset int64
			if params.Offset >= 2 {
				sqlOffset = int64(params.Offset - 2)
			}
			rows, err := q.ListDirEntries(ctx, db.ListDirEntriesParams{
				ParentIno: int64(params.Ino),
				Limit:     int32(rem),
				Offset:    sqlOffset,
			})
			if err != nil {
				return fmt.Errorf("list dir entries for parent %d (limit=%d, offset=%d): %w", params.Ino, rem, sqlOffset, err)
			}

			for i, row := range rows {
				entries = append(entries, DirEntryPlus{
					Ino:                 uint64(row.Ino),
					Name:                row.Name,
					NextOffset:          uint64(sqlOffset) + uint64(i) + 3,
					Inode:               newInodeFromListDirRow(row),
					EntryTimeoutSeconds: m.entryTimeout,
					AttrTimeoutSeconds:  m.attrTimeout,
				})
			}
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("readdirplus tx: %w", err)
		}
	}

	if entries == nil {
		entries = []DirEntryPlus{}
	}
	return entries, nil
}

// Fallocate 为常规文件预分配物理空间或打洞去分配空间（对接 Linux FUSE FUSE_FALLOCATE 协议）。
// 遵循 Linux fallocate(2) 与 ADR-0010 规范。
func (m *manager) Fallocate(ctx context.Context, params FallocateParams) error {
	// 1. 入参边界与整型溢出防御
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Length == 0 || params.Length > math.MaxInt64 ||
		params.Offset > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Offset > uint64(math.MaxInt64)-params.Length {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	// 2. 模式位校验
	if params.Mode == FallocFlPunchHole {
		return ErrInvalidArgument
	}
	if params.Mode != 0 && params.Mode != FallocFlKeepSize && params.Mode != (FallocFlPunchHole|FallocFlKeepSize) {
		return ErrNotSupported
	}

	// 3. 开启读写事务
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		current, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		fileType := uint32(current.Mode) & S_IFMT
		if fileType == S_IFDIR {
			return ErrIsDir
		}
		if fileType != S_IFREG {
			return ErrNotSupported
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(params.Fh),
			Ino: int64(params.Ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		accMode := uint32(handle.Flags) & O_ACCMODE
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if params.SessionID != nil {
			if err := m.renewClientSession(ctx, q, *params.SessionID); err != nil {
				return err
			}
		}

		switch params.Mode {
		case 0:
			targetSize := params.Offset + params.Length
			if targetSize > uint64(current.Size) {
				_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
					Ino:         current.Ino,
					Mode:        current.Mode,
					Size:        int64(targetSize),
					Uid:         current.Uid,
					Gid:         current.Gid,
					SetAtimeNow: false,
					Atime:       current.Atime,
					SetMtimeNow: true,
					Mtime:       pgtype.Timestamptz{},
				})
				if err != nil {
					return fmt.Errorf("update inode attr %d: %w", current.Ino, err)
				}
			} else {
				if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
					return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
				}
			}
		case FallocFlKeepSize:
			if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
				return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
			}
		case FallocFlPunchHole | FallocFlKeepSize:
			newMode := uint32(current.Mode) &^ (S_ISUID | S_ISGID)
			_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
				Ino:         current.Ino,
				Mode:        int32(newMode),
				Size:        current.Size,
				Uid:         current.Uid,
				Gid:         current.Gid,
				SetAtimeNow: false,
				Atime:       current.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			})
			if err != nil {
				return fmt.Errorf("update inode attr %d: %w", current.Ino, err)
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrNotSupported):
			return ErrNotSupported
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("fallocate (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
		}
	}

	return nil
}

// validateXattrName 校验扩展属性名称合法性。
// len(name) == 0 或 len(name) > MaxXattrNameBytes 时返回 ErrRange；
// name 包含 NUL 字节 (\x00) 时返回 ErrInvalidArgument。
func validateXattrName(name []byte) error {
	if len(name) == 0 || len(name) > MaxXattrNameBytes {
		return ErrRange
	}
	if bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isAllowedUserXattrFileType 判定文件模式是否允许访问 user.* 命名空间扩展属性。
// 仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)。
func isAllowedUserXattrFileType(mode uint32) bool {
	fileType := mode & S_IFMT
	return fileType == S_IFREG || fileType == S_IFDIR || fileType == S_IFSOCK
}

// SetXattr 设置指定 Inode 的扩展属性。
// 遵循 Linux FUSE FUSE_SETXATTR 协议与 POSIX setxattr(2) 语义，执行 4 步求值状态机：
// 1. 入参静态校验（不查库）：校验 Ino、Flags、Name 与 Value 边界及非法字符；
// 2. 排他锁与存在性校验：读写事务内通过 GetInodeForUpdate 获取行锁，支持孤儿节点；
// 3. user.* 文件类型准入校验：针对 user.* 命名空间限制仅常规文件、目录与套接字，非 user.* 命名空间直接放行；
// 4. 属性存在性、Flags 状态机、配额校验、空值归一化与持久化：判断 XATTR_CREATE / XATTR_REPLACE 状态，
//    并在新增属性时执行 65536 字节名称列表配额拦截（覆盖不拦截），写入后刷新 Inode ctime。
func (m *manager) SetXattr(ctx context.Context, params SetXattrParams) error {
	// 第 1 步：入参静态校验（不查库）
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Flags&^(XattrCreate|XattrReplace) != 0 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(params.Name); err != nil {
		return err
	}
	if len(params.Value) > MaxXattrValueBytes {
		return ErrTooBig
	}

	// 开启读写事务
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：排他锁与存在性校验
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(params.Name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrPerm
		}

		// 第 4 步：属性存在性、flags 独立判定、配额与 Upsert
		_, err = q.GetInodeXattr(ctx, db.GetInodeXattrParams{
			Ino:  int64(params.Ino),
			Name: params.Name,
		})

		exists := true
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				exists = false
			} else {
				return fmt.Errorf("get inode xattr (ino=%d): %w", params.Ino, err)
			}
		}

		if exists {
			if (params.Flags & XattrCreate) != 0 {
				return ErrAlreadyExists
			}
		} else {
			if (params.Flags & XattrReplace) != 0 {
				return ErrNoData
			}

			// 仅当新增属性名时，查询该 Inode 现有全部属性名总长，校验 65536 字节配额
			totalBytes, err := q.GetInodeXattrNamesTotalBytes(ctx, int64(params.Ino))
			if err != nil {
				return fmt.Errorf("get inode xattr names total bytes %d: %w", params.Ino, err)
			}
			if totalBytes+int64(len(params.Name))+1 > MaxXattrListBytes {
				return ErrNoSpace
			}
		}

		// 若 value == nil，归一化为 []byte{}，防止 pgx 序列化为 SQL NULL
		val := params.Value
		if val == nil {
			val = []byte{}
		}

		// 写入数据库
		if err := q.UpsertInodeXattr(ctx, db.UpsertInodeXattrParams{
			Ino:   int64(params.Ino),
			Name:  params.Name,
			Value: val,
		}); err != nil {
			return fmt.Errorf("upsert inode xattr (ino=%d): %w", params.Ino, err)
		}

		// 刷新 Inode ctime
		if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
			return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrPerm):
			return ErrPerm
		case errors.Is(err, ErrAlreadyExists):
			return ErrAlreadyExists
		case errors.Is(err, ErrNoData):
			return ErrNoData
		case errors.Is(err, ErrNoSpace):
			return ErrNoSpace
		case errors.Is(err, ErrRange):
			return ErrRange
		case errors.Is(err, ErrTooBig):
			return ErrTooBig
		default:
			return fmt.Errorf("setxattr (ino=%d): %w", params.Ino, err)
		}
	}

	return nil
}

// GetXattr 获取指定 Inode 的扩展属性值。
// 遵循 Linux FUSE FUSE_GETXATTR 协议与 POSIX getxattr(2) 语义，执行 4 步求值状态机：
// 1. 入参静态校验（不查库）：校验 Ino 与 Name 边界及非法空字符；
// 2. 只读快照隔离与存在性校验：在 RepeatableRead 只读事务内通过 GetInode 查询节点，支持孤儿节点；
// 3. user.* 文件类型准入校验：针对 user.* 命名空间限制仅常规文件、目录与套接字，对其它非法文件类型直接返回 ErrNoData（对齐 Linux xattr_permission 读掩码行为），非 user.* 命名空间不限制；
// 4. 读取属性值：调用 GetInodeXattr 读取扩展属性值，未命中返回 ErrNoData，命中则原样返回 value（含 0 字节空切片与非 UTF-8 原始字节）。遵循 noatime 策略不刷新 atime。
func (m *manager) GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return nil, err
	}

	var value []byte

	// 开启只读事务 (RepeatableRead)
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 存在性校验
		inode, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrNoData
		}

		// 第 4 步：读取属性值
		xattr, err := q.GetInodeXattr(ctx, db.GetInodeXattrParams{
			Ino:  int64(ino),
			Name: name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoData
			}
			return fmt.Errorf("get inode xattr (ino=%d): %w", ino, err)
		}

		value = xattr.Value
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrRange):
			return nil, ErrRange
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNoData):
			return nil, ErrNoData
		default:
			return nil, fmt.Errorf("getxattr (ino=%d): %w", ino, err)
		}
	}

	return value, nil
}

// ListXattr 列出指定 Inode 的全部扩展属性名称列表。
// 遵循 Linux FUSE FUSE_LISTXATTR 协议与 POSIX listxattr(2) 语义，执行 3 步求值状态机：
// 1. 入参静态校验（不查库）：校验 Ino 必须大于 0 且在 int64 正数范围内 (<= math.MaxInt64)，否则返回 ErrInvalidArgument；
// 2. 只读快照隔离与 Inode 存在性校验：在 RepeatableRead 只读事务（tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead)）中调用 GetInode(ino)；若未命中返回 ErrNotFound；支持孤儿节点 (nlink == 0)；
// 3. 查询属性名列表：调用 ListInodeXattrNames(ino) 查询该节点绑定的全部属性名称；
//    - 结果按名称原始字节序升序（BYTEA ASC）返回；不按调用方过滤（对齐 Linux 内核 FUSE 行为）；
//    - 若无属性，返回空切片 [][]byte{}；
//    - 遵循 noatime 策略不刷新 atime。
func (m *manager) ListXattr(ctx context.Context, ino uint64) ([][]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	names := [][]byte{}

	// 开启只读事务 (RepeatableRead)
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 存在性校验
		_, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		// 第 3 步：查询属性名列表（按 BYTEA ASC 升序排列，不按命名空间过滤）
		items, err := q.ListInodeXattrNames(ctx, int64(ino))
		if err != nil {
			return fmt.Errorf("list inode xattr names (ino=%d): %w", ino, err)
		}

		if len(items) > 0 {
			names = items
		}
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		default:
			return nil, fmt.Errorf("listxattr (ino=%d): %w", ino, err)
		}
	}

	return names, nil
}

// RemoveXattr 移除指定 Inode 的扩展属性。
// 遵循 Linux FUSE FUSE_REMOVEXATTR 协议与 POSIX removexattr(2) 语义，执行 4 步求值状态机：
// 1. 入参静态校验（不查库）：
//    - ino == 0 || ino > math.MaxInt64 返回 ErrInvalidArgument；
//    - len(name) == 0 || len(name) > MaxXattrNameBytes 返回 ErrRange；
//    - bytes.IndexByte(name, 0) >= 0 返回 ErrInvalidArgument；
// 2. Inode 行级排他锁与存在性校验：在读写事务内调用 GetInodeForUpdate(ino)；未命中返回 ErrNotFound；支持孤儿节点 (nlink == 0)；
// 3. user.* 文件类型准入校验：若以 user. 为前缀，仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)；对符号链接、字符设备、块设备、FIFO 返回 ErrPerm（对齐 Linux xattr_permission 写掩码行为）；非 user.* 命名空间不限制；
// 4. 删除属性与刷新 ctime：调用 DeleteInodeXattr(ino, name)；若受影响行数为 0 返回 ErrNoData；删除成功后调用 TouchInodeCtime(ino) 刷新 ctime（使用 clock_timestamp()，不修改 mtime 与 atime）。
func (m *manager) RemoveXattr(ctx context.Context, ino uint64, name []byte) error {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 行级排他锁与存在性校验
		inode, err := q.GetInodeForUpdate(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrPerm
		}

		// 第 4 步：删除属性与刷新 ctime
		rowsAffected, err := q.DeleteInodeXattr(ctx, db.DeleteInodeXattrParams{
			Ino:  int64(ino),
			Name: name,
		})
		if err != nil {
			return fmt.Errorf("delete inode xattr (ino=%d): %w", ino, err)
		}
		if rowsAffected == 0 {
			return ErrNoData
		}

		if err := q.TouchInodeCtime(ctx, int64(ino)); err != nil {
			return fmt.Errorf("touch inode ctime %d: %w", ino, err)
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrPerm):
			return ErrPerm
		case errors.Is(err, ErrNoData):
			return ErrNoData
		case errors.Is(err, ErrRange):
			return ErrRange
		default:
			return fmt.Errorf("removexattr (ino=%d): %w", ino, err)
		}
	}

	return nil
}

