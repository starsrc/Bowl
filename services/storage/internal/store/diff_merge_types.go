package store

import (
	"context"
	"fmt"
	"math"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

const (
	DefaultDiffPageSize uint32 = 200

	MaxDiffPageSize uint32 = 1000

	MaxMergeConflictSamples = 200

	MaxReadNodeSliceBytes uint32 = 4 << 20

	inspectReadOnlyCacheSize = 1 << 20
)

type DiffKind uint8

const (
	DiffKindUnspecified DiffKind = 0
	DiffKindAdded       DiffKind = 1
	DiffKindModified    DiffKind = 2
	DiffKindDeleted     DiffKind = 3
)

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

type ConflictKind uint8

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

type MergeStrategy uint8

const (
	MergeStrategyUnspecified MergeStrategy = 0
	MergeStrategyFail        MergeStrategy = 1
	MergeStrategyOurs        MergeStrategy = 2
	MergeStrategyTheirs      MergeStrategy = 3
)

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

type ReadNodeResult struct {
	Mode          uint32
	FileSize      uint64
	Data          []byte
	EOF           bool
	SymlinkTarget []byte
}

type DiffNodeSummary struct {
	Ino  uint64
	Mode uint32
	Size uint64
}

type DiffEntry struct {
	Path     []byte
	Kind     DiffKind
	Conflict ConflictKind
	Base     *DiffNodeSummary
	Target   *DiffNodeSummary
	Source   *DiffNodeSummary
}

type DiffSummary struct {
	Added     uint64
	Modified  uint64
	Deleted   uint64
	Conflicts uint64
}

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

type DiffResult struct {
	Summary       DiffSummary
	Entries       []DiffEntry
	NextPageToken string
}

type MergeConflict struct {
	Path   []byte
	Kind   ConflictKind
	Base   *DiffNodeSummary
	Target *DiffNodeSummary
	Source *DiffNodeSummary
}

type MergeConflictReport struct {
	TotalConflicts     uint64
	Conflicts          []MergeConflict
	ConflictsTruncated bool
}

type ErrMergeConflict struct {
	Report MergeConflictReport
}

func (e *ErrMergeConflict) Error() string {
	if e == nil {
		return "store: merge conflict"
	}
	return fmt.Sprintf("store: merge conflict (%d conflicts)", e.Report.TotalConflicts)
}

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

type MergeResult struct {
	UsedBytes      uint64
	Inodes         uint64
	TargetMaxBytes uint64
	WriterEpoch    uint64
	NextWriteSeq   uint64
}

func openReadOnlyForInspect(dir string, cache *pebble.Cache) (*StoreActor, func(), error) {
	if dir == "" {
		return nil, nil, ErrInvalid
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, nil, err
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
	cleanup := func() {
		_ = actor.Close()
		if owned != nil {
			owned.Unref()
		}
	}
	return actor, cleanup, nil
}

func InspectBranchStat(rootDir string) (writerEpoch, nextWriteSeq uint64, hasOrphans bool, err error) {
	actor, cleanup, err := openReadOnlyForInspect(rootDir, nil)
	if err != nil {
		return 0, 0, false, err
	}
	defer cleanup()

	orphans, err := actor.hasOrphansInDB()
	if err != nil {
		return 0, 0, false, err
	}
	return actor.stat.WriterEpoch, actor.stat.NextWriteSeq, orphans, nil
}

func ReadCommitNodeSlice(rootDir string, ino, offset uint64, size uint32) (*ReadNodeResult, error) {
	if rootDir == "" || ino == 0 || ino > uint64(math.MaxInt64) || offset > MaxFileSize || size > MaxReadNodeSliceBytes {
		return nil, ErrInvalid
	}
	actor, cleanup, err := openReadOnlyForInspect(rootDir, nil)
	if err != nil {
		return nil, err
	}
	defer cleanup()

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
