package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

const (
	// DefaultDiffPageSize 为 DiffTrees 在 PageSize 为 0 时的默认单页返回条目上限。
	DefaultDiffPageSize uint32 = 200
	// MaxDiffPageSize 为 DiffTrees 单页返回条目硬上限。
	MaxDiffPageSize uint32 = 1000
	// MaxMergeConflictSamples 为 ErrMergeConflict 携带的冲突路径有序样本数上限。
	MaxMergeConflictSamples = 200
	// MaxReadNodeSliceBytes 为 ReadCommitNodeSlice 单次读取字节上限（4 MiB）。
	MaxReadNodeSliceBytes uint32 = 4 << 20

	inspectReadOnlyCacheSize = 1 << 20 // 1 MiB 轻量只读缓存
)

// DiffKind 表示目录树条目在三路或两路差异比较中的变更类型。
type DiffKind uint8

// DiffKind 枚举值。
const (
	DiffKindUnspecified DiffKind = 0
	DiffKindAdded       DiffKind = 1
	DiffKindModified    DiffKind = 2
	DiffKindDeleted     DiffKind = 3
)

// String 返回 DiffKind 可读名称。
func (k DiffKind) String() string {
	switch k {
	case DiffKindAdded:
		return "added"
	case DiffKindModified:
		return "modified"
	case DiffKindDeleted:
		return "deleted"
	default:
		return "unspecified"
	}
}

// ConflictKind 表示三路合并或差异检测中识别出的冲突类型。
type ConflictKind uint8

// ConflictKind 枚举值（对齐领域契约 0..12）。
const (
	ConflictNone             ConflictKind = 0
	ConflictAddAdd           ConflictKind = 1
	ConflictContent          ConflictKind = 2
	ConflictModifyDelete     ConflictKind = 3
	ConflictDeleteModify     ConflictKind = 4
	ConflictTreeDeleteModify ConflictKind = 5
	ConflictType             ConflictKind = 6
	ConflictDirFile          ConflictKind = 7
	ConflictAttr             ConflictKind = 8
	ConflictXattr            ConflictKind = 9
	ConflictXattrQuota       ConflictKind = 10
	ConflictSecurityAttr     ConflictKind = 11
	ConflictHardlink         ConflictKind = 12
)

// String 返回 ConflictKind 可读名称。
func (k ConflictKind) String() string {
	switch k {
	case ConflictNone:
		return "none"
	case ConflictAddAdd:
		return "add_add"
	case ConflictContent:
		return "content"
	case ConflictModifyDelete:
		return "modify_delete"
	case ConflictDeleteModify:
		return "delete_modify"
	case ConflictTreeDeleteModify:
		return "tree_delete_modify"
	case ConflictType:
		return "type"
	case ConflictDirFile:
		return "dir_file"
	case ConflictAttr:
		return "attr"
	case ConflictXattr:
		return "xattr"
	case ConflictXattrQuota:
		return "xattr_quota"
	case ConflictSecurityAttr:
		return "security_attr"
	case ConflictHardlink:
		return "hardlink"
	default:
		return "unknown"
	}
}

// MergeStrategy 表示三路合并冲突裁决策略。
type MergeStrategy uint8

// MergeStrategy 枚举值。
const (
	MergeStrategyUnspecified MergeStrategy = 0
	MergeStrategyFail        MergeStrategy = 1
	MergeStrategyOurs        MergeStrategy = 2
	MergeStrategyTheirs      MergeStrategy = 3
)

// String 返回 MergeStrategy 可读名称。
func (s MergeStrategy) String() string {
	switch s {
	case MergeStrategyFail:
		return "fail"
	case MergeStrategyOurs:
		return "ours"
	case MergeStrategyTheirs:
		return "theirs"
	default:
		return "unspecified"
	}
}

// ReadNodeResult 描述 ReadCommitNodeSlice 读取不可变提交节点切片的结果。
type ReadNodeResult struct {
	Mode          uint32
	FileSize      uint64
	Data          []byte
	EOF           bool
	SymlinkTarget []byte
}

// DiffNodeSummary 描述差异或冲突条目的节点元数据摘要。
type DiffNodeSummary struct {
	Ino  uint64
	Mode uint32
	Size uint64
}

// DiffEntry 描述单条规范化路径在三路/两路对比中的差异与冲突详情。
type DiffEntry struct {
	Path     []byte
	Kind     DiffKind
	Conflict ConflictKind
	Base     *DiffNodeSummary
	Target   *DiffNodeSummary
	Source   *DiffNodeSummary
}

// DiffSummary 汇总差异比较的全量（过滤后）统计计数。
type DiffSummary struct {
	Added     uint64
	Modified  uint64
	Deleted   uint64
	Conflicts uint64
}

// DiffOptions 配置 DiffTrees 只读三路/两路目录树比较参数。
type DiffOptions struct {
	BaseRoot      string
	TargetRoot    string
	SourceRoot    string
	PathPrefix    []byte
	ConflictsOnly bool
	PageSize      uint32
	PageToken     string
	Cache         *pebble.Cache
}

// DiffResult 承载 DiffTrees 的过滤后全量汇总与当前分页条目。
type DiffResult struct {
	Summary       DiffSummary
	Entries       []DiffEntry
	NextPageToken string
}

// MergeConflict 描述合并过程中未决的单条路径冲突详情。
type MergeConflict struct {
	Path   []byte
	Kind   ConflictKind
	Base   *DiffNodeSummary
	Target *DiffNodeSummary
	Source *DiffNodeSummary
}

// MergeConflictReport 汇总未决合并冲突报告（最多保留 200 条有序样本）。
type MergeConflictReport struct {
	TotalConflicts     uint64
	Conflicts          []MergeConflict
	ConflictsTruncated bool
}

// ErrMergeConflict 表示三路合并存在未决冲突且目标与暂存盘零副作用中止。
type ErrMergeConflict struct {
	Report MergeConflictReport
}

// Error 返回冲突错误描述。
func (e *ErrMergeConflict) Error() string {
	if e == nil {
		return "store: merge conflict"
	}
	return fmt.Sprintf("store: merge conflict (%d conflicts)", e.Report.TotalConflicts)
}

// MergeOptions 配置 MergeTrees 零拷贝三路合并与暂存目录构建参数。
type MergeOptions struct {
	BaseRoot            string
	TargetRoot          string
	SourceRoot          string
	StagingDir          string
	Strategy            MergeStrategy
	PathStrategies      map[string]MergeStrategy
	DryRun              bool
	OverrideMaxBytes    uint64
	ExpectedWriterEpoch uint64
	InoAllocator        InodeSegmentAllocator
	PackAllocator       PackSegmentAllocator
	Cache               *pebble.Cache
}

// MergeResult 承载 MergeTrees 预演或落盘后的目标盘容量与状态摘要。
type MergeResult struct {
	UsedBytes      uint64
	Inodes         uint64
	TargetMaxBytes uint64
	WriterEpoch    uint64
	NextWriteSeq   uint64
}

func openReadOnlyForInspect(dir string, cache *pebble.Cache) (*StoreActor, func() error, error) {
	if dir == "" {
		return nil, nil, ErrInvalid
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, nil, wrapIOErr(err)
	}
	var owned *pebble.Cache
	if cache == nil {
		owned = pebble.NewCache(inspectReadOnlyCacheSize)
		cache = owned
	}
	actor, err := Open(Options{
		Dir:      dir,
		ReadOnly: true,
		Cache:    cache,
	})
	if err != nil {
		if owned != nil {
			owned.Unref()
		}
		return nil, nil, err
	}
	cleanup := func() error {
		closeErr := actor.Close()
		if owned != nil {
			owned.Unref()
		}
		return closeErr
	}
	return actor, cleanup, nil
}

// InspectBranchStat 以零后台协程只读模式打开 rootDir，读取 sys:stat 的 WriterEpoch/NextWriteSeq 及是否存在孤儿前缀。
func InspectBranchStat(rootDir string) (writerEpoch, nextWriteSeq uint64, hasOrphans bool, err error) {
	actor, cleanup, openErr := openReadOnlyForInspect(rootDir, nil)
	if openErr != nil {
		return 0, 0, false, openErr
	}
	defer func() {
		err = errors.Join(err, cleanup())
		if err != nil {
			writerEpoch = 0
			nextWriteSeq = 0
			hasOrphans = false
		}
	}()

	orphans, orphErr := actor.hasOrphansInDB()
	if orphErr != nil {
		return 0, 0, false, orphErr
	}
	return actor.stat.WriterEpoch, actor.stat.NextWriteSeq, orphans, nil
}

// ReadCommitNodeSlice 以零后台协程只读模式读取 rootDir 中指定存活常规文件或符号链接的切片载荷，
// 对常规文件严格按已有稀疏空洞与尾部截断语义补齐 0x00。
func ReadCommitNodeSlice(rootDir string, ino, offset uint64, size uint32) (res *ReadNodeResult, retErr error) {
	if rootDir == "" || ino == 0 || ino > uint64(math.MaxInt64) || offset > MaxFileSize || size > MaxReadNodeSliceBytes {
		return nil, ErrInvalid
	}
	actor, cleanup, err := openReadOnlyForInspect(rootDir, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, cleanup())
		if retErr != nil {
			res = nil
		}
	}()

	ctx := context.Background()
	inode, err := actor.readInodeFromDB(ctx, ino)
	if err != nil {
		return nil, err
	}
	if err := actor.checkInodeLiveFromDB(ctx, &inode); err != nil {
		return nil, err
	}

	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return nil, ErrIsDir
	case S_IFLNK:
		return &ReadNodeResult{
			Mode:          inode.Mode,
			FileSize:      inode.Size,
			EOF:           true,
			SymlinkTarget: append([]byte(nil), inode.SymlinkTarget...),
		}, nil
	case S_IFREG:
		data, err := actor.Read(ctx, ino, offset, size)
		if err != nil {
			return nil, err
		}
		eof := offset >= inode.Size || offset+uint64(len(data)) >= inode.Size
		return &ReadNodeResult{
			Mode:     inode.Mode,
			FileSize: inode.Size,
			Data:     data,
			EOF:      eof,
		}, nil
	default:
		return nil, ErrInvalid
	}
}
