package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/fx"

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
	TmpFile(ctx context.Context, params TmpFileParams) (*CreateResult, error)
	Open(ctx context.Context, params OpenParams) (*OpenResult, error)
	Flush(ctx context.Context, params FlushParams) error
	Fsync(ctx context.Context, params FsyncParams) error
	Release(ctx context.Context, params ReleaseParams) error
	OpenDir(ctx context.Context, params OpenDirParams) (*OpenDirResult, error)
	FsyncDir(ctx context.Context, params FsyncDirParams) error
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
	RenewSession(ctx context.Context, sessionID uint64) (time.Time, error)
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

// renewOptionalClientSession 若传入了有效的客户端会话标识，则在当前事务中执行会话租约续期并清理相关孤儿节点。
func (m *manager) renewOptionalClientSession(ctx context.Context, q db.Querier, sessionID *uint64) error {
	if sessionID == nil {
		return nil
	}
	_, err := m.renewClientSession(ctx, q, *sessionID)
	return err
}
