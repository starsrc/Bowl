package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"
	"strings"
)

const (
	DefaultDiffPageSize     uint32 = 200
	MaxDiffPageSize         uint32 = 1000
	MaxMergeConflictSamples        = 200
	diffCursorVersion              = 1
	systemQuotaXattrName           = "system.bowl.max_bytes"
	securityXattrPrefixStr         = "security."
	userXattrPrefixStr             = "user."
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
	SourceBranch  string
	TargetBranch  string
	PathPrefix    []byte
	ConflictsOnly bool
	PageSize      uint32
	PageToken     string
}

type DiffCommitsOptions struct {
	BaseCommitID   uint64
	SourceCommitID uint64
	PathPrefix     []byte
	ConflictsOnly  bool
	PageSize       uint32
	PageToken      string
}

type DiffWorkOptions struct {
	PathPrefix    []byte
	ConflictsOnly bool
	PageSize      uint32
	PageToken     string
}

type DiffResult struct {
	BaseCommitID   uint64
	TargetCommitID uint64
	SourceCommitID uint64
	Summary        DiffSummary
	Entries        []DiffEntry
	NextPageToken  string
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
		return "engine: merge conflict"
	}
	return fmt.Sprintf("engine: merge conflict (%d conflicts)", e.Report.TotalConflicts)
}

type MergeOptions struct {
	SourceBranch   string
	TargetBranch   string
	Strategy       MergeStrategy
	PathStrategies map[string]MergeStrategy
	DryRun         bool
	NoFastForward  bool
	Message        string
}

type MergeResult struct {
	CommitID       uint64
	BaseCommitID   uint64
	TargetCommitID uint64
	SourceCommitID uint64
	FastForward    bool
	UpToDate       bool
	WriterEpoch    uint64
	Summary        DiffSummary
}

type diffPageCursor struct {
	Version int    `json:"v"`
	Path    []byte `json:"p"`
}

func encodeDiffPageCursor(lastPath []byte) string {
	raw, err := json.Marshal(diffPageCursor{
		Version: diffCursorVersion,
		Path:    lastPath,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDiffPageCursor(token string) ([]byte, error) {
	if strings.ContainsRune(token, 0) {
		return nil, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalid
	}
	var cursor diffPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || dec.More() || cursor.Version != diffCursorVersion || len(cursor.Path) == 0 {
		return nil, ErrInvalid
	}
	if bytes.IndexByte(cursor.Path, 0) >= 0 {
		return nil, ErrInvalid
	}
	return cursor.Path, nil
}

func joinRelPath(parent, name []byte) []byte {
	if len(parent) == 0 {
		return append([]byte(nil), name...)
	}
	out := make([]byte, len(parent)+1+len(name))
	copy(out, parent)
	out[len(parent)] = '/'
	copy(out[len(parent)+1:], name)
	return out
}

func splitParentAndName(relPath []byte) (parent string, name []byte) {
	idx := bytes.LastIndexByte(relPath, '/')
	if idx < 0 {
		return "", relPath
	}
	return string(relPath[:idx]), relPath[idx+1:]
}

func normalizePathPrefix(raw []byte) ([]byte, error) {
	if bytes.IndexByte(raw, 0) >= 0 {
		return nil, ErrInvalid
	}
	trimmed := bytes.TrimLeft(raw, "/")
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte(".")) {
		return nil, nil
	}
	return append([]byte(nil), trimmed...), nil
}

func normalizeStrategyPath(raw string) (string, error) {
	if strings.ContainsRune(raw, 0) {
		return "", ErrInvalid
	}
	trimmed := strings.Trim(raw, "/")
	if trimmed == "." {
		return "", nil
	}
	return path.Clean("/" + trimmed)[1:], nil
}

func normalizePathStrategyMap(raw map[string]MergeStrategy) (map[string]MergeStrategy, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]MergeStrategy, len(raw))
	for k, v := range raw {
		norm, err := normalizeStrategyPath(k)
		if err != nil {
			return nil, err
		}
		if v < MergeStrategyUnspecified || v > MergeStrategyTheirs {
			return nil, ErrInvalid
		}
		out[norm] = v
	}
	return out, nil
}

func resolveMergeStrategy(global MergeStrategy, byPath map[string]MergeStrategy, relPath []byte) MergeStrategy {
	if len(byPath) > 0 {
		cur := string(relPath)
		if cur == "/" {
			cur = ""
		}
		for {
			if s, ok := byPath[cur]; ok && s != MergeStrategyUnspecified {
				return s
			}
			if cur == "" {
				break
			}
			idx := strings.LastIndexByte(cur, '/')
			if idx < 0 {
				cur = ""
			} else {
				cur = cur[:idx]
			}
		}
	}
	if global == MergeStrategyUnspecified {
		return MergeStrategyFail
	}
	return global
}

func isRootDiffPath(p []byte) bool {
	return len(p) == 1 && p[0] == '/'
}

func compareDiffPath(a, b []byte) int {
	aRoot := isRootDiffPath(a)
	bRoot := isRootDiffPath(b)
	switch {
	case aRoot && bRoot:
		return 0
	case aRoot:
		return -1
	case bRoot:
		return 1
	default:
		return bytes.Compare(a, b)
	}
}

func matchPathPrefix(relPath, normPrefix []byte) bool {
	if len(normPrefix) == 0 {
		return true
	}
	if !bytes.HasPrefix(relPath, normPrefix) {
		return false
	}
	if normPrefix[len(normPrefix)-1] == '/' {
		return true
	}
	return len(relPath) == len(normPrefix) || relPath[len(normPrefix)] == '/'
}

func dirCanContainPathPrefix(dirPath, normPrefix []byte) bool {
	if len(normPrefix) == 0 || matchPathPrefix(dirPath, normPrefix) {
		return true
	}
	if len(normPrefix) <= len(dirPath) {
		return false
	}
	return bytes.HasPrefix(normPrefix, dirPath) && normPrefix[len(dirPath)] == '/'
}

func makeNodeSummary(n *Inode) *DiffNodeSummary {
	if n == nil {
		return nil
	}
	return &DiffNodeSummary{
		Ino:  n.Ino,
		Mode: n.Mode,
		Size: n.Size,
	}
}
