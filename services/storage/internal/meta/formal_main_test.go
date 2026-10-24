package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// ============================================================================
// 1. 环境变量与确定性随机种子配置 (ADR-0016)
// ============================================================================

const (
	defaultFormalRuns  = 1
	defaultFormalSteps = 10
	pcgStreamSalt      = uint64(0x9e3779b97f4a7c15)
)

// isFormalDeepMode 判断是否显式设置了深度形式化验证环境变量（BOWL_FORMAL_RUNS 或 BOWL_FORMAL_STEPS）。
func isFormalDeepMode() bool {
	return strings.TrimSpace(os.Getenv("BOWL_FORMAL_RUNS")) != "" ||
		strings.TrimSpace(os.Getenv("BOWL_FORMAL_STEPS")) != ""
}

// formalSeedConfig 保存从环境变量解析出的状态机运行配置。
type formalSeedConfig struct {
	HasFixedSeed bool
	FixedSeed    uint64
	Runs         int
	Steps        int
}

// formalEnvOverrides 保存从 BOWL_FORMAL_SEED、BOWL_FORMAL_RUNS、BOWL_FORMAL_STEPS 解析的公共环境变量配置。
type formalEnvOverrides struct {
	HasFixedSeed bool
	FixedSeed    uint64
	Runs         int
	Steps        int
}

// parseFormalSeedString 解析十进制或 0x 十六进制格式的 64 位无符号整数种子。
func parseFormalSeedString(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("empty seed string")
	}
	return strconv.ParseUint(s, 0, 64)
}

// parseFormalEnvOverrides 统一解析 BOWL_FORMAL_SEED、BOWL_FORMAL_RUNS 与 BOWL_FORMAL_STEPS 环境变量。
func parseFormalEnvOverrides(t *testing.T, defaultRuns, defaultSteps int) formalEnvOverrides {
	t.Helper()
	out := formalEnvOverrides{
		Runs:  defaultRuns,
		Steps: defaultSteps,
	}

	if rawSeed := strings.TrimSpace(os.Getenv("BOWL_FORMAL_SEED")); rawSeed != "" {
		seed, err := parseFormalSeedString(rawSeed)
		require.NoErrorf(t, err, "解析环境变量 BOWL_FORMAL_SEED=%q 失败", rawSeed)
		out.HasFixedSeed = true
		out.FixedSeed = seed
		// 复现指定种子时若未显式指定 BOWL_FORMAL_RUNS，默认执行 1 轮精准复现
		if strings.TrimSpace(os.Getenv("BOWL_FORMAL_RUNS")) == "" {
			out.Runs = 1
		}
	}

	if rawRuns := strings.TrimSpace(os.Getenv("BOWL_FORMAL_RUNS")); rawRuns != "" {
		runs, err := strconv.Atoi(rawRuns)
		require.NoErrorf(t, err, "解析环境变量 BOWL_FORMAL_RUNS=%q 失败", rawRuns)
		require.Greater(t, runs, 0, "BOWL_FORMAL_RUNS 必须大于 0")
		out.Runs = runs
	}

	if rawSteps := strings.TrimSpace(os.Getenv("BOWL_FORMAL_STEPS")); rawSteps != "" {
		steps, err := strconv.Atoi(rawSteps)
		require.NoErrorf(t, err, "解析环境变量 BOWL_FORMAL_STEPS=%q 失败", rawSteps)
		require.Greater(t, steps, 0, "BOWL_FORMAL_STEPS 必须大于 0")
		out.Steps = steps
	}

	return out
}

// loadFormalSeedConfig 从环境变量读取 BOWL_FORMAL_SEED、BOWL_FORMAL_RUNS、BOWL_FORMAL_STEPS。
func loadFormalSeedConfig(t *testing.T, defaultRuns, defaultSteps int) formalSeedConfig {
	t.Helper()
	if defaultRuns <= 0 {
		defaultRuns = defaultFormalRuns
	}
	if defaultSteps <= 0 {
		defaultSteps = defaultFormalSteps
		// 轻量档 (task test) 下，各工单内嵌的 RandomStateMachine 子测试跑精简冒烟步数，顶层 TestFormalMainStateMachine 跑完整轻量步数
		if !isFormalDeepMode() && strings.Contains(t.Name(), "/") {
			defaultSteps = 3
		}
	}

	ov := parseFormalEnvOverrides(t, defaultRuns, defaultSteps)
	if !ov.HasFixedSeed && strings.Contains(t.Name(), "/") && isFormalDeepMode() && ov.Runs > 1 {
		ov.Runs = 1
		if ov.Steps > 20 {
			ov.Steps = 20
		}
	}

	return formalSeedConfig(ov)
}

// newFormalRNG 基于 math/rand/v2.NewPCG(seed, seed^0x9e3779b97f4a7c15) 创建确定性伪随机数生成器。
func newFormalRNG(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^pcgStreamSalt))
}

// ============================================================================
// 2. 主状态机内存参考模型完整定义 (供 T1 ~ T10 共享复用)
// ============================================================================

// formalDisk 表示内存参考模型中的虚拟磁盘与配额账本状态。
type formalDisk struct {
	DiskID     string
	Name       string
	RootIno    uint64
	MaxBytes   uint64
	UsedBytes  uint64
	UsedInodes uint64
	Deleted    bool
}

// formalInode 表示内存参考模型中的 Inode 节点状态（涵盖目录、常规文件、符号链接、特殊设备及孤儿节点）。
type formalInode struct {
	Ino           uint64
	Generation    uint64
	RootIno       uint64
	ParentIno     uint64 // 目录节点为父目录 ino（根目录为自身 root_ino），非目录节点为 0
	Mode          uint32 // 含 S_IFMT 与权限位 (0o7777)
	UID           uint32
	GID           uint32
	Rdev          uint32
	Size          uint64 // S_IFREG 为逻辑字节数，S_IFLNK 为目标长度，S_IFDIR 为 4096，其余为 0
	Nlink         uint32
	Flags         uint32
	SymlinkTarget []byte
	IsTmpFile     bool // 是否由 TmpFile 创建
	TmpFileExcl   bool // TmpFile 创建时是否携带 O_EXCL（禁止 Link 具名化）
	TmpFileLinked bool // TmpFile 是否已完成过一次性 Link 具名化
	Deleted       bool // 是否已从底表物理回收
}

// FileType 返回 Inode 的文件类型位 (mode & S_IFMT)。
func (i *formalInode) FileType() uint32 {
	return i.Mode & meta.S_IFMT
}

// IsDir 判断是否为目录节点。
func (i *formalInode) IsDir() bool {
	return i.FileType() == meta.S_IFDIR
}

// IsReg 判断是否为常规文件节点。
func (i *formalInode) IsReg() bool {
	return i.FileType() == meta.S_IFREG
}

// IsLnk 判断是否为符号链接节点。
func (i *formalInode) IsLnk() bool {
	return i.FileType() == meta.S_IFLNK
}

// ExpectedAttrSize 计算公开接口 GetAttr/Lookup 预期返回的 Attribute.Size。
func (i *formalInode) ExpectedAttrSize() uint64 {
	switch i.FileType() {
	case meta.S_IFDIR:
		if i.Size == 0 {
			return meta.DefaultDirSize
		}
		return i.Size
	case meta.S_IFREG, meta.S_IFLNK:
		return i.Size
	default:
		return 0
	}
}

// ExpectedAttrBlocks 计算公开接口预期返回的 512 字节块数。
func (i *formalInode) ExpectedAttrBlocks() uint64 {
	sz := i.ExpectedAttrSize()
	if sz == 0 {
		return 0
	}
	return (sz + 511) / 512
}

// ExpectedAttrRdev 计算公开接口预期返回的设备号 Rdev。
func (i *formalInode) ExpectedAttrRdev() uint32 {
	ft := i.FileType()
	if ft == meta.S_IFCHR || ft == meta.S_IFBLK {
		return i.Rdev
	}
	return 0
}

// formalEntryKey 表示目录项 (parent_ino, name) 复合主键。
type formalEntryKey struct {
	ParentIno uint64
	Name      string
}

// formalHandle 表示内存参考模型中的打开文件或目录句柄。
type formalHandle struct {
	Fh        uint64
	Ino       uint64
	Flags     uint32
	SessionID *uint64 // nil 表示无会话句柄 (session_id IS NULL)
	IsDir     bool
	Closed    bool
}

// formalSession 表示内存参考模型中的客户端会话租约状态。
type formalSession struct {
	SessionID uint64
	ExpiresAt time.Time
	Expired   bool // 是否已在测试中因等待超期而失活
}

// formalMainModel 汇总主状态机的全部内存参考账本。
// 为保证给定相同随机种子时状态转移 100% 确定性，提供有序切片记录创建顺序，避免直接随机遍历 Go map。
type formalMainModel struct {
	Disks              map[string]*formalDisk    // DiskID -> *formalDisk
	DiskByRootIno      map[uint64]*formalDisk    // RootIno -> *formalDisk (仅存活磁盘)
	AllCreatedDiskIDs  []string                  // 按创建顺序记录全部 DiskID
	AllCreatedRootInos []uint64                  // 按创建顺序记录全部 RootIno（用于底表 SQL 隔离扫描）
	DeletedDiskIDs     map[string]bool           // 已删除 DiskID 集合
	DeletedRootInos    map[uint64]bool           // 已删除 RootIno 集合
	Inodes             map[uint64]*formalInode   // Ino -> *formalInode（存活节点及尚未物理回收的孤儿节点）
	DeletedInos        map[uint64]bool           // 已物理删除的 Ino 集合
	AllCreatedInos     []uint64                  // 按创建顺序记录全部 Ino
	Entries            map[formalEntryKey]uint64 // (ParentIno, Name) -> ChildIno
	Handles            map[uint64]*formalHandle  // Fh -> *formalHandle（未关闭句柄）
	ClosedHandles      map[uint64]*formalHandle  // Fh -> *formalHandle（已关闭句柄）
	AllCreatedFhs      []uint64                  // 按创建顺序记录全部 Fh
	Sessions           map[uint64]*formalSession // SessionID -> *formalSession
	AllSessionIDs      []uint64                  // 按创建顺序记录全部 SessionID
}

// newFormalMainModel 初始化空白内存参考模型。
func newFormalMainModel() *formalMainModel {
	return &formalMainModel{
		Disks:           make(map[string]*formalDisk),
		DiskByRootIno:   make(map[uint64]*formalDisk),
		DeletedDiskIDs:  make(map[string]bool),
		DeletedRootInos: make(map[uint64]bool),
		Inodes:          make(map[uint64]*formalInode),
		DeletedInos:     make(map[uint64]bool),
		Entries:         make(map[formalEntryKey]uint64),
		Handles:         make(map[uint64]*formalHandle),
		ClosedHandles:   make(map[uint64]*formalHandle),
		Sessions:        make(map[uint64]*formalSession),
	}
}

// RecordNewDisk 在内存模型中登记新创建的 Disk 及其根目录 Inode。
func (m *formalMainModel) RecordNewDisk(d *meta.Disk) *formalDisk {
	fd := &formalDisk{
		DiskID:     d.DiskID,
		Name:       d.Name,
		RootIno:    d.RootIno,
		MaxBytes:   d.MaxBytes,
		UsedBytes:  d.UsedBytes,
		UsedInodes: d.UsedInodes,
	}
	m.Disks[d.DiskID] = fd
	m.DiskByRootIno[d.RootIno] = fd
	m.AllCreatedDiskIDs = append(m.AllCreatedDiskIDs, d.DiskID)
	m.AllCreatedRootInos = append(m.AllCreatedRootInos, d.RootIno)

	rootInode := &formalInode{
		Ino:        d.RootIno,
		Generation: 1,
		RootIno:    d.RootIno,
		ParentIno:  d.RootIno,
		Mode:       meta.S_IFDIR | 0o755,
		Nlink:      2,
		UID:        0,
		GID:        0,
		Rdev:       0,
		Size:       meta.DefaultDirSize,
	}
	m.RecordNewInode(rootInode)
	return fd
}

// RecordDeletedDisk 在内存模型中执行级联删盘账本清理。
func (m *formalMainModel) RecordDeletedDisk(diskID string) {
	fd, ok := m.Disks[diskID]
	if !ok || fd.Deleted {
		return
	}
	fd.Deleted = true
	m.DeletedDiskIDs[diskID] = true
	m.DeletedRootInos[fd.RootIno] = true
	delete(m.DiskByRootIno, fd.RootIno)

	// 收集归属该 root_ino 的全部 Inode 并级联清理 entries、handles（按创建顺序确定性遍历）
	var toDeleteInos []uint64
	for _, ino := range m.AllCreatedInos {
		if inode, exists := m.Inodes[ino]; exists && inode.RootIno == fd.RootIno {
			toDeleteInos = append(toDeleteInos, ino)
		}
	}
	deletedSet := make(map[uint64]bool, len(toDeleteInos))
	for _, ino := range toDeleteInos {
		deletedSet[ino] = true
		if inode, exists := m.Inodes[ino]; exists {
			inode.Deleted = true
		}
		delete(m.Inodes, ino)
		m.DeletedInos[ino] = true
	}

	for _, ek := range m.SortedEntries() {
		childIno := m.Entries[ek]
		if deletedSet[ek.ParentIno] || deletedSet[childIno] {
			delete(m.Entries, ek)
		}
	}

	for _, fh := range m.AllCreatedFhs {
		if h, exists := m.Handles[fh]; exists && deletedSet[h.Ino] {
			h.Closed = true
			m.ClosedHandles[fh] = h
			delete(m.Handles, fh)
		}
	}
}

// RecordNewInode 在内存模型中登记新创建的 Inode。
func (m *formalMainModel) RecordNewInode(inode *formalInode) {
	m.Inodes[inode.Ino] = inode
	m.AllCreatedInos = append(m.AllCreatedInos, inode.Ino)
}

// RecordNewHandle 在内存模型中登记新创建的打开句柄。
func (m *formalMainModel) RecordNewHandle(h *formalHandle) {
	m.Handles[h.Fh] = h
	m.AllCreatedFhs = append(m.AllCreatedFhs, h.Fh)
}

// RecordSession 在内存模型中登记或续期客户端会话。
func (m *formalMainModel) RecordSession(sessionID uint64, expiresAt time.Time) {
	if s, exists := m.Sessions[sessionID]; exists {
		s.ExpiresAt = expiresAt
		s.Expired = false
		return
	}
	m.Sessions[sessionID] = &formalSession{
		SessionID: sessionID,
		ExpiresAt: expiresAt,
		Expired:   false,
	}
	m.AllSessionIDs = append(m.AllSessionIDs, sessionID)
}

// ActiveDisks 按创建顺序返回当前所有存活的虚拟磁盘。
func (m *formalMainModel) ActiveDisks() []*formalDisk {
	var res []*formalDisk
	for _, id := range m.AllCreatedDiskIDs {
		if d, ok := m.Disks[id]; ok && !d.Deleted {
			res = append(res, d)
		}
	}
	return res
}

// PickActiveDisk 从存活磁盘中确定性随机选择一个，若无存活磁盘返回 nil。
func (m *formalMainModel) PickActiveDisk(rng *rand.Rand) *formalDisk {
	disks := m.ActiveDisks()
	if len(disks) == 0 {
		return nil
	}
	return disks[rng.IntN(len(disks))]
}

// ActiveInodes 按创建顺序返回当前未物理删除的全部 Inode（含孤儿节点）。
func (m *formalMainModel) ActiveInodes() []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted {
			res = append(res, inode)
		}
	}
	return res
}

// ActiveDirs 按创建顺序返回当前所有存活目录（IsDir() && Nlink > 0）。
func (m *formalMainModel) ActiveDirs() []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && inode.IsDir() && inode.Nlink > 0 {
			res = append(res, inode)
		}
	}
	return res
}

// ActiveNonDirs 按创建顺序返回当前所有未物理删除的非目录节点。
func (m *formalMainModel) ActiveNonDirs() []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && !inode.IsDir() {
			res = append(res, inode)
		}
	}
	return res
}

// ActiveRegFiles 按创建顺序返回当前所有未物理删除的常规文件节点（含孤儿与 TmpFile）。
func (m *formalMainModel) ActiveRegFiles() []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && inode.IsReg() {
			res = append(res, inode)
		}
	}
	return res
}

// ActiveSymlinks 按创建顺序返回当前所有未物理删除的符号链接节点。
func (m *formalMainModel) ActiveSymlinks() []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && inode.IsLnk() {
			res = append(res, inode)
		}
	}
	return res
}

// SortedEntries 返回按 (ParentIno ASC, Name ASC) 确定性排序的全部目录项键切片。
func (m *formalMainModel) SortedEntries() []formalEntryKey {
	keys := make([]formalEntryKey, 0, len(m.Entries))
	for k := range m.Entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ParentIno != keys[j].ParentIno {
			return keys[i].ParentIno < keys[j].ParentIno
		}
		return keys[i].Name < keys[j].Name
	})
	return keys
}

// ChildEntriesOf 返回指定父目录下按原始字节字典序 (Name ASC) 排序的目录项键切片。
func (m *formalMainModel) ChildEntriesOf(parentIno uint64) []formalEntryKey {
	var keys []formalEntryKey
	for k := range m.Entries {
		if k.ParentIno == parentIno {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Name < keys[j].Name
	})
	return keys
}

// IsSessionValid 判断句柄关联的会话是否有效（nil 会话恒有效，非 nil 会话需存在且未标记过期）。
func (m *formalMainModel) IsSessionValid(sessionID *uint64) bool {
	if sessionID == nil {
		return true
	}
	s, ok := m.Sessions[*sessionID]
	return ok && !s.Expired
}

// ActiveHandles 按创建顺序返回所有未关闭且所属会话有效的句柄。
func (m *formalMainModel) ActiveHandles() []*formalHandle {
	var res []*formalHandle
	for _, fh := range m.AllCreatedFhs {
		if h, ok := m.Handles[fh]; ok && !h.Closed && m.IsSessionValid(h.SessionID) {
			res = append(res, h)
		}
	}
	return res
}

// ActiveFileHandles 按创建顺序返回所有未关闭且会话有效的非目录句柄。
func (m *formalMainModel) ActiveFileHandles() []*formalHandle {
	var res []*formalHandle
	for _, h := range m.ActiveHandles() {
		if !h.IsDir {
			res = append(res, h)
		}
	}
	return res
}

// ActiveDirHandles 按创建顺序返回所有未关闭且会话有效的目录句柄。
func (m *formalMainModel) ActiveDirHandles() []*formalHandle {
	var res []*formalHandle
	for _, h := range m.ActiveHandles() {
		if h.IsDir {
			res = append(res, h)
		}
	}
	return res
}

// HasValidHandlesForIno 判断指定 Inode 是否仍存在至少一条有效打开句柄。
func (m *formalMainModel) HasValidHandlesForIno(ino uint64) bool {
	for _, fh := range m.AllCreatedFhs {
		if h, ok := m.Handles[fh]; ok && !h.Closed && h.Ino == ino && m.IsSessionValid(h.SessionID) {
			return true
		}
	}
	return false
}

// HasLinkableTmpFileHandleForIno 判断指定 nlink==0 的 Inode 是否存在可具名化的 TmpFile 句柄（含 O_TMPFILE 且不含 O_EXCL）。
func (m *formalMainModel) HasLinkableTmpFileHandleForIno(ino uint64) bool {
	for _, fh := range m.AllCreatedFhs {
		if h, ok := m.Handles[fh]; ok && !h.Closed && h.Ino == ino && m.IsSessionValid(h.SessionID) {
			if (h.Flags&meta.O_TMPFILE) != 0 && (h.Flags&meta.O_EXCL) == 0 {
				return true
			}
		}
	}
	return false
}

// ClearTmpFileFlagForIno 清除指定 Inode 所有关联句柄上的 O_TMPFILE 标志位（对应 ClearTmpFileFlagByIno）。
func (m *formalMainModel) ClearTmpFileFlagForIno(ino uint64) {
	for _, fh := range m.AllCreatedFhs {
		if h, ok := m.Handles[fh]; ok && h.Ino == ino {
			h.Flags &^= meta.O_TMPFILE
		}
	}
}

// SortedInos 按创建顺序返回当前内存中仍存活的全部 Ino 编号。
func (m *formalMainModel) SortedInos() []uint64 {
	res := make([]uint64, 0, len(m.Inodes))
	for _, ino := range m.AllCreatedInos {
		if _, ok := m.Inodes[ino]; ok {
			res = append(res, ino)
		}
	}
	return res
}

// SortedDeletedInos 按创建顺序返回所有已物理删除的 Ino 编号。
func (m *formalMainModel) SortedDeletedInos() []uint64 {
	res := make([]uint64, 0, len(m.DeletedInos))
	for _, ino := range m.AllCreatedInos {
		if m.DeletedInos[ino] {
			res = append(res, ino)
		}
	}
	return res
}

// FirstDeletedIno 按创建顺序返回第一个已物理删除的 Ino 编号。
func (m *formalMainModel) FirstDeletedIno() (uint64, bool) {
	for _, ino := range m.AllCreatedInos {
		if m.DeletedInos[ino] {
			return ino, true
		}
	}
	return 0, false
}

// SortedFhs 按创建顺序返回所有历史创建的句柄编号。
func (m *formalMainModel) SortedFhs() []uint64 {
	return m.AllCreatedFhs
}

// SortedDiskIDs 按创建顺序返回所有历史创建的 DiskID。
func (m *formalMainModel) SortedDiskIDs() []string {
	return m.AllCreatedDiskIDs
}

// ClosedFileHandles 按创建顺序返回所有已关闭的非目录句柄。
func (m *formalMainModel) ClosedFileHandles() []*formalHandle {
	var res []*formalHandle
	for _, fh := range m.AllCreatedFhs {
		if ch, ok := m.ClosedHandles[fh]; ok && !ch.IsDir {
			res = append(res, ch)
		}
	}
	return res
}

// ClosedDirHandles 按创建顺序返回所有已关闭的目录句柄。
func (m *formalMainModel) ClosedDirHandles() []*formalHandle {
	var res []*formalHandle
	for _, fh := range m.AllCreatedFhs {
		if ch, ok := m.ClosedHandles[fh]; ok && ch.IsDir {
			res = append(res, ch)
		}
	}
	return res
}

// ProbeClosedHandles 按创建顺序遍历所有已释放句柄并核对调用结果：
// 若关联 Inode 已物理删除则断言返回 ErrNotFound，若关联 Inode 仍存续则断言返回 ErrBadFileHandle。
func (m *formalMainModel) ProbeClosedHandles(t *testing.T, isDir bool, opName string, callFn func(ch *formalHandle) error) {
	t.Helper()
	var list []*formalHandle
	if isDir {
		list = m.ClosedDirHandles()
	} else {
		list = m.ClosedFileHandles()
	}
	for _, ch := range list {
		err := callFn(ch)
		if m.DeletedInos[ch.Ino] {
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"已物理删除节点 ino=%d 的已关闭句柄 fh=%d 调用 %s 应返回 ErrNotFound, 实际: %v",
				ch.Ino, ch.Fh, opName, err)
		} else if inode, ok := m.Inodes[ch.Ino]; ok && !inode.Deleted && inode.IsDir() == isDir {
			if inode.Nlink > 0 || m.HasValidHandlesForIno(inode.Ino) {
				require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
					"存活节点 ino=%d 的已释放句柄 fh=%d 调用 %s 应返回 ErrBadFileHandle, 实际: %v",
					ch.Ino, ch.Fh, opName, err)
			}
		}
	}
}

// validateParentDirForCreate 校验在指定父目录下创建新目录项时的父目录存活、类型、孤儿拦截、"."/".." 与重名冲突。
func (m *formalMainModel) validateParentDirForCreate(parentIno uint64, name []byte) (*formalInode, error) {
	parent, exists := m.Inodes[parentIno]
	if !exists || parent.Deleted {
		return nil, meta.ErrNotFound
	}
	if !parent.IsDir() {
		return nil, meta.ErrNotDir
	}
	if parent.Nlink == 0 {
		return nil, meta.ErrNotFound
	}
	if bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) {
		return nil, meta.ErrAlreadyExists
	}
	if _, dup := m.Entries[formalEntryKey{ParentIno: parentIno, Name: string(name)}]; dup {
		return nil, meta.ErrAlreadyExists
	}
	return parent, nil
}

// ReclaimOrphanInModel 当 nlink == 0 且无有效句柄时，从内存模型中物理删除孤儿节点并退还所属磁盘配额。
func (m *formalMainModel) ReclaimOrphanInModel(ino uint64) bool {
	inode, ok := m.Inodes[ino]
	if !ok || inode.Deleted || inode.Nlink > 0 {
		return false
	}
	if m.HasValidHandlesForIno(ino) {
		return false
	}

	inode.Deleted = true
	delete(m.Inodes, ino)
	m.DeletedInos[ino] = true

	if disk, exists := m.DiskByRootIno[inode.RootIno]; exists && !disk.Deleted {
		if disk.UsedInodes > 0 {
			disk.UsedInodes--
		}
		if inode.IsReg() && inode.Size > 0 {
			if disk.UsedBytes >= inode.Size {
				disk.UsedBytes -= inode.Size
			} else {
				disk.UsedBytes = 0
			}
		}
	}
	return true
}

// IsAncestorDir 判断 ancestorIno 是否等于 targetIno 或是 targetIno 的祖先目录。
func (m *formalMainModel) IsAncestorDir(ancestorIno, targetIno uint64) bool {
	curr := targetIno
	visited := make(map[uint64]bool)
	for curr > 0 && !visited[curr] {
		if curr == ancestorIno {
			return true
		}
		visited[curr] = true
		inode, ok := m.Inodes[curr]
		if !ok || !inode.IsDir() {
			break
		}
		if inode.ParentIno == curr || inode.ParentIno == 0 {
			break
		}
		curr = inode.ParentIno
	}
	return false
}

// ============================================================================
// 3. 状态机运行时环境、轨迹账单与插件化注册表
// ============================================================================

// formalActionFunc 定义单步状态机动作签名，返回该步的中文操作账单描述。
type formalActionFunc func(ctx context.Context, t *testing.T, env *formalMainEnv) string

// formalWeightedAction 定义带权重的状态机动作。
type formalWeightedAction struct {
	Name   string
	Weight int
	Fn     formalActionFunc
}

// formalProbeFunc 定义每步写操作后执行的只读查账探针签名。
type formalProbeFunc func(ctx context.Context, t *testing.T, env *formalMainEnv)

// formalNamedProbe 定义带名称的只读探针。
type formalNamedProbe struct {
	Name string
	Fn   formalProbeFunc
}

// formalAuditorFunc 定义每步写操作后执行的 PostgreSQL 底表数学不变量审计函数签名。
type formalAuditorFunc func(ctx context.Context, t *testing.T, env *formalMainEnv)

// formalNamedAuditor 定义带名称的底表不变量审计器。
type formalNamedAuditor struct {
	Name string
	Fn   formalAuditorFunc
}

var (
	registeredFormalActions  []formalWeightedAction
	registeredFormalProbes   []formalNamedProbe
	registeredFormalAuditors []formalNamedAuditor
)

// registerFormalAction 供 formal_main_*_test.go 在 init() 中注册主状态机动作。
func registerFormalAction(name string, weight int, fn formalActionFunc) {
	for i := range registeredFormalActions {
		if registeredFormalActions[i].Name == name {
			registeredFormalActions[i].Weight = weight
			registeredFormalActions[i].Fn = fn
			return
		}
	}
	registeredFormalActions = append(registeredFormalActions, formalWeightedAction{
		Name:   name,
		Weight: weight,
		Fn:     fn,
	})
}

// registerFormalProbe 供 formal_main_*_test.go 在 init() 中注册只读探针。
func registerFormalProbe(name string, fn formalProbeFunc) {
	for i := range registeredFormalProbes {
		if registeredFormalProbes[i].Name == name {
			registeredFormalProbes[i].Fn = fn
			return
		}
	}
	registeredFormalProbes = append(registeredFormalProbes, formalNamedProbe{
		Name: name,
		Fn:   fn,
	})
}

// registerFormalAuditor 供 formal_main_*_test.go 在 init() 中注册底表数学不变量审计器。
func registerFormalAuditor(name string, fn formalAuditorFunc) {
	for i := range registeredFormalAuditors {
		if registeredFormalAuditors[i].Name == name {
			registeredFormalAuditors[i].Fn = fn
			return
		}
	}
	registeredFormalAuditors = append(registeredFormalAuditors, formalNamedAuditor{
		Name: name,
		Fn:   fn,
	})
}

func init() {
	// 注册 T1 核心动作：CreateDisk、UpdateDisk 与基础配额水位构造
	registerFormalAction("CreateDisk", 15, formalActionCreateDisk)
	registerFormalAction("UpdateDisk", 20, formalActionUpdateDisk)
	registerFormalAction("T1QuotaFileStep", 10, formalActionT1QuotaFileStep)

	// 注册 T1 核心只读探针：GetDisk、StatFS
	registerFormalProbe("GetDisk", formalProbeGetDisk)
	registerFormalProbe("StatFS", formalProbeStatFS)

	// 注册 T1 底表数学不变量审计器：INV-4 (磁盘容量与节点配额账本守恒)
	registerFormalAuditor("INV-4_DiskQuota", formalAuditINV4DiskQuota)
}

// formalMainEnv 封装单轮形式化状态机运行的上下文、连接池、被测 Manager、参考模型与轨迹记录。
type formalMainEnv struct {
	T          *testing.T
	TestName   string
	RunIndex   int
	TotalRuns  int
	StepIndex  int
	TotalSteps int
	Seed       uint64
	Rng        *rand.Rand
	ProbeRng   *rand.Rand
	Pool       *pgxpool.Pool
	Store      db.Store
	Mgr        meta.Manager
	Model      *formalMainModel
	NamePrefix string
	SeqCounter uint64
	LeaseTTL   time.Duration
	Trace      []string
}

// ProbeRNG 返回只读探针专用的隔离伪随机数生成器，避免只读探针抽样推进主动作流 env.Rng。
func (env *formalMainEnv) ProbeRNG() *rand.Rand {
	if env.ProbeRng == nil {
		env.ProbeRng = newFormalRNG(env.Seed ^ 0x6a09e667f3bcc908)
	}
	return env.ProbeRng
}

// NextSeq 生成单轮内递增的唯一序号。
func (env *formalMainEnv) NextSeq() uint64 {
	env.SeqCounter++
	return env.SeqCounter
}

// UniqueDiskName 生成带唯一测试隔离前缀的磁盘名称。
func (env *formalMainEnv) UniqueDiskName(tag string) string {
	return fmt.Sprintf("%s-%s-%d", env.NamePrefix, tag, env.NextSeq())
}

// UniqueEntryName 生成带序号的目录项名称。
func (env *formalMainEnv) UniqueEntryName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, env.NextSeq())
}

// BeginStep 在执行第 step 步动作前预登记轨迹占位行，确保动作内部断言失败时 FormatFailureReport 不会丢失当前失败步骤。
func (env *formalMainEnv) BeginStep(step int, actionName string) {
	line := fmt.Sprintf("[第 %03d 步] %s (动作执行中断于内部断言)", step, actionName)
	env.Trace = append(env.Trace, line)
}

// AppendTrace 追加或更新一步中文操作轨迹账单。
func (env *formalMainEnv) AppendTrace(step int, desc string) {
	prefix := fmt.Sprintf("[第 %03d 步] ", step)
	line := prefix + desc
	if n := len(env.Trace); n > 0 && strings.HasPrefix(env.Trace[n-1], prefix) {
		env.Trace[n-1] = line
		return
	}
	env.Trace = append(env.Trace, line)
}

// FormatFailureReport 格式化包含完整 64 位种子、重放命令与中文操作轨迹账单的诊断报告。
func (env *formalMainEnv) FormatFailureReport() string {
	var b strings.Builder
	b.WriteString("\n================================================================================\n")
	b.WriteString("[形式化状态机对账失败诊断报告]\n")
	b.WriteString(fmt.Sprintf("测试名称: %s (第 %d / %d 轮, 失败于第 %d / %d 步)\n",
		env.TestName, env.RunIndex+1, env.TotalRuns, env.StepIndex, env.TotalSteps))
	b.WriteString(fmt.Sprintf("复现种子 (Seed): 0x%016x (十进制: %d)\n", env.Seed, env.Seed))
	b.WriteString(fmt.Sprintf("单轮精准重放命令:\n  GOWORK=off BOWL_FORMAL_SEED=0x%016x BOWL_FORMAL_RUNS=1 BOWL_FORMAL_STEPS=%d go -C services/storage test -v ./internal/meta -run '^%s$'\n",
		env.Seed, env.TotalSteps, env.TestName))
	b.WriteString(fmt.Sprintf("中文操作轨迹账单 (共 %d 步):\n", len(env.Trace)))
	for _, entry := range env.Trace {
		b.WriteString("  ")
		b.WriteString(entry)
		b.WriteString("\n")
	}
	b.WriteString("================================================================================\n")
	return b.String()
}

// Fatalf 输出完整种子与操作轨迹账单并立即终止当前测试。
func (env *formalMainEnv) Fatalf(format string, args ...any) {
	env.T.Helper()
	msg := fmt.Sprintf(format, args...)
	env.T.Fatalf("%s\n%s", msg, env.FormatFailureReport())
}

// cleanupDatabase 清理本轮测试在 PostgreSQL 底表中创建的全部 Disk、Inode 与 Session 记录。
func (env *formalMainEnv) cleanupDatabase() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if len(env.Model.AllCreatedRootInos) > 0 {
		rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
		for _, r := range env.Model.AllCreatedRootInos {
			rootInos = append(rootInos, int64(r))
		}
		_, _ = env.Pool.Exec(ctx, `DELETE FROM disks WHERE root_ino = ANY($1)`, rootInos)
		_, _ = env.Pool.Exec(ctx, `DELETE FROM inodes WHERE root_ino = ANY($1)`, rootInos)
	}

	if len(env.Model.AllSessionIDs) > 0 {
		sids := make([]int64, 0, len(env.Model.AllSessionIDs))
		for _, s := range env.Model.AllSessionIDs {
			sids = append(sids, int64(s))
		}
		_, _ = env.Pool.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = ANY($1)`, sids)
	}
}

var (
	sharedFormalPoolOnce sync.Once
	sharedFormalPool     *pgxpool.Pool
	sharedFormalPoolErr  error
)

// setupFormalPostgresPool 建立并复用包级共享 PostgreSQL 测试连接池，若数据库不可用则安全跳过。
func setupFormalPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	sharedFormalPoolOnce.Do(func() {
		dsn := getStorageTestDSN()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		poolCfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			sharedFormalPoolErr = err
			return
		}
		if poolCfg.ConnConfig.RuntimeParams == nil {
			poolCfg.ConnConfig.RuntimeParams = make(map[string]string)
		}
		poolCfg.ConnConfig.RuntimeParams["deadlock_timeout"] = "15ms"
		poolCfg.MaxConns = 16

		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			sharedFormalPoolErr = err
			return
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			sharedFormalPoolErr = err
			return
		}
		sharedFormalPool = pool
	})
	if sharedFormalPoolErr != nil || sharedFormalPool == nil {
		t.Skipf("跳过形式化状态机测试: 连接 PostgreSQL 失败: %v", sharedFormalPoolErr)
	}
	return sharedFormalPool
}

// formalMainRunConfig 控制状态机运行器的参数与插件组合。
type formalMainRunConfig struct {
	Runs        int
	Steps       int
	ManagerOpts []meta.Option
	LeaseTTL    time.Duration
	InitHook    func(ctx context.Context, t *testing.T, env *formalMainEnv)
	Actions     []formalWeightedAction
	Probes      []formalNamedProbe
	Auditors    []formalNamedAuditor
}

// pickWeightedAction 按权重确定性随机选择一个动作。
func pickWeightedAction(rng *rand.Rand, actions []formalWeightedAction) formalWeightedAction {
	totalWeight := 0
	for _, a := range actions {
		if a.Weight > 0 {
			totalWeight += a.Weight
		}
	}
	target := rng.IntN(totalWeight)
	acc := 0
	for _, a := range actions {
		if a.Weight <= 0 {
			continue
		}
		acc += a.Weight
		if target < acc {
			return a
		}
	}
	return actions[len(actions)-1]
}

// runFormalMainStateMachine 驱动多轮确定性随机状态机对拍、只读探针查账与底表数学不变量审计。
func runFormalMainStateMachine(t *testing.T, cfg formalMainRunConfig) {
	t.Helper()
	pool := setupFormalPostgresPool(t)
	seedCfg := loadFormalSeedConfig(t, cfg.Runs, cfg.Steps)

	actions := cfg.Actions
	if len(actions) == 0 {
		actions = append([]formalWeightedAction(nil), registeredFormalActions...)
	}
	require.NotEmpty(t, actions, "状态机动作列表不能为空")

	probes := cfg.Probes
	if len(probes) == 0 {
		probes = append([]formalNamedProbe(nil), registeredFormalProbes...)
	}

	auditors := cfg.Auditors
	if len(auditors) == 0 {
		auditors = append([]formalNamedAuditor(nil), registeredFormalAuditors...)
	}

	baseTimeNano := uint64(time.Now().UnixNano())

	for runIdx := 0; runIdx < seedCfg.Runs; runIdx++ {
		var seed uint64
		if seedCfg.HasFixedSeed {
			seed = seedCfg.FixedSeed + uint64(runIdx)
		} else {
			seed = baseTimeNano + uint64(runIdx)*0x517cc1b727220a95
		}

		store := db.NewStore(pool)
		mgr := meta.New(store, cfg.ManagerOpts...)
		leaseTTL := cfg.LeaseTTL
		if leaseTTL <= 0 {
			leaseTTL = meta.DefaultSessionLeaseTTL
		}

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   runIdx,
			TotalRuns:  seedCfg.Runs,
			TotalSteps: seedCfg.Steps,
			Seed:       seed,
			Rng:        newFormalRNG(seed),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: fmt.Sprintf("formal-disk-%x-r%d", seed&0xffffffff, runIdx),
			LeaseTTL:   leaseTTL,
		}

		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		func() {
			defer func() {
				// 任何 require/assert 或 panic 导致当前测试失败时，自动打印 64 位种子与完整中文操作轨迹账单
				if t.Failed() {
					t.Log(env.FormatFailureReport())
				}
				env.cleanupDatabase()
			}()

			ctx := context.Background()

			// 初始保底：若未定义自定义 InitHook，则先创建 1 块基础虚拟磁盘，保证后续目录/文件/配额动作从第 1 步起即有盘可用
			if cfg.InitHook != nil {
				cfg.InitHook(ctx, t, env)
			} else {
				env.StepIndex = 0
				env.BeginStep(0, "InitDisk")
				initDesc := formalCreateValidDiskStep(ctx, t, env)
				env.AppendTrace(0, "[初始建盘] "+initDesc)
			}

			for step := 1; step <= seedCfg.Steps; step++ {
				env.StepIndex = step
				chosen := pickWeightedAction(env.Rng, actions)
				env.BeginStep(step, chosen.Name)
				desc := chosen.Fn(ctx, t, env)
				env.AppendTrace(step, desc)

				// 为本步只读探针初始化独立 RNG，确保探针内部抽样不污染主动作流 env.Rng
				env.ProbeRng = rand.New(rand.NewPCG(seed^uint64(step)*pcgStreamSalt, seed+uint64(step)))

				// 1. 执行只读查账探针 (Probes)
				for _, probe := range probes {
					probe.Fn(ctx, t, env)
				}

				// 2. 执行 PostgreSQL 底表数学不变量审计 (Auditors)
				for _, auditor := range auditors {
					auditor.Fn(ctx, t, env)
				}
			}
		}()
	}
}

// ============================================================================
// 4. T1 动作实现：CreateDisk、UpdateDisk 与配额文件水位构造
// ============================================================================

// modelValidateDiskName 对应参考模型对磁盘名称的静态校验规则。
func modelValidateDiskName(name string) error {
	if len(name) > meta.MaxDiskNameBytes {
		return meta.ErrNameTooLong
	}
	if strings.IndexByte(name, 0) >= 0 {
		return meta.ErrInvalidArgument
	}
	return nil
}

// formalCreateValidDiskStep 执行一次确定合法的 CreateDisk 并同步更新参考模型。
func formalCreateValidDiskStep(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()
	name := env.UniqueDiskName("init")
	maxBytes := uint64(env.Rng.IntN(16)+4) * 65536 // 256KB ~ 1.25MB

	got, err := env.Mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     name,
		MaxBytes: maxBytes,
	})
	require.NoErrorf(t, err, "CreateDisk 合法创建不应失败 (name=%q, maxBytes=%d)", name, maxBytes)
	require.NotNil(t, got)
	require.NotEmpty(t, got.DiskID)
	require.Greater(t, got.RootIno, uint64(0))
	require.Equal(t, name, got.Name)
	require.Equal(t, maxBytes, got.MaxBytes)
	require.Equal(t, uint64(0), got.UsedBytes)
	require.Equal(t, uint64(1), got.UsedInodes)

	env.Model.RecordNewDisk(got)
	return fmt.Sprintf("CreateDisk(name=%q, max_bytes=%d) -> 成功 (disk_id=%s, root_ino=%d)",
		name, maxBytes, got.DiskID, got.RootIno)
}

// formalActionCreateDisk 在状态机中随机生成合法或非法的 CreateDisk 请求并与参考模型对拍。
func formalActionCreateDisk(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	// 控制存活磁盘数量上限（避免单轮创建过多磁盘拖慢每步全量探针），同时保留异常分支测试
	activeDisks := env.Model.ActiveDisks()
	branch := env.Rng.IntN(100)
	if len(activeDisks) >= 6 && branch < 65 {
		branch = 65 + env.Rng.IntN(35)
	}

	var params meta.CreateDiskParams
	var caseTag string

	switch {
	case branch < 55:
		// 正常创建新盘（不同容量档位，含小于 4096 字节的极小容量盘以验证 StatFS Blocks 向上保底为 1）
		caseTag = "正常创建"
		params.Name = env.UniqueDiskName("disk")
		if env.Rng.IntN(5) == 0 {
			params.MaxBytes = uint64(env.Rng.IntN(4000) + 1) // 1 ~ 4000 字节 (< 4096)
		} else {
			params.MaxBytes = uint64(env.Rng.IntN(32)+1) * 32768 // 32KB ~ 1MB
		}
	case branch < 65:
		// 同名创建或空名创建
		if len(activeDisks) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "同名磁盘创建"
			params.Name = activeDisks[env.Rng.IntN(len(activeDisks))].Name
		} else {
			caseTag = "空名称磁盘创建"
			params.Name = ""
		}
		params.MaxBytes = uint64(env.Rng.IntN(16)+1) * 65536
	case branch < 75:
		// 非法容量：MaxBytes == 0
		caseTag = "非法容量(max_bytes=0)"
		params.Name = env.UniqueDiskName("zero-cap")
		params.MaxBytes = 0
	case branch < 85:
		// 非法容量：MaxBytes > math.MaxInt64
		caseTag = "非法容量(超MaxInt64)"
		params.Name = env.UniqueDiskName("overflow-cap")
		params.MaxBytes = uint64(math.MaxInt64) + uint64(env.Rng.IntN(1000)+1)
	case branch < 93:
		// 非法名称：包含空字节 '\x00'
		caseTag = "非法名称(含空字节)"
		params.Name = fmt.Sprintf("bad\x00disk-%d", env.NextSeq())
		params.MaxBytes = 65536
	default:
		// 非法名称：超长 (> 255 字节)
		caseTag = "非法名称(超长>255)"
		params.Name = strings.Repeat("d", meta.MaxDiskNameBytes+1+env.Rng.IntN(20))
		params.MaxBytes = 65536
	}

	// 参考模型预测结果
	var expectedErr error
	if params.MaxBytes == 0 || params.MaxBytes > math.MaxInt64 {
		expectedErr = meta.ErrInvalidArgument
	} else if err := modelValidateDiskName(params.Name); err != nil {
		expectedErr = err
	}

	got, err := env.Mgr.CreateDisk(ctx, params)
	if expectedErr != nil {
		require.Errorf(t, err, "CreateDisk[%s] 预期返回错误 %v，但实际成功: %+v", caseTag, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"CreateDisk[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("CreateDisk[%s](max_bytes=%d) -> 预期拒绝 (%v)", caseTag, params.MaxBytes, expectedErr)
	}

	require.NoErrorf(t, err, "CreateDisk[%s] 预期成功，但返回错误: %v", caseTag, err)
	require.NotNil(t, got)
	require.Equal(t, params.Name, got.Name)
	require.Equal(t, params.MaxBytes, got.MaxBytes)
	require.Equal(t, uint64(0), got.UsedBytes)
	require.Equal(t, uint64(1), got.UsedInodes)
	require.Greater(t, got.RootIno, uint64(0))

	env.Model.RecordNewDisk(got)
	return fmt.Sprintf("CreateDisk[%s](name=%q, max_bytes=%d) -> 成功 (disk_id=%s, root_ino=%d)",
		caseTag, got.Name, got.MaxBytes, got.DiskID, got.RootIno)
}

// formalActionUpdateDisk 在状态机中随机生成各类正常与边界 UpdateDisk 请求并与参考模型对拍。
func formalActionUpdateDisk(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDisks := env.Model.ActiveDisks()
	branch := env.Rng.IntN(100)

	var params meta.UpdateDiskParams
	var targetModelDisk *formalDisk
	var caseTag string

	// 若尚无存活磁盘，则测试不存在磁盘或非法 UUID
	if len(activeDisks) == 0 && branch < 75 {
		branch = 80
	}

	switch {
	case branch < 20:
		// 仅修改名称
		caseTag = "仅修改名称"
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
		newName := env.UniqueDiskName("renamed")
		params.Name = &newName
	case branch < 40:
		// 仅合法修改容量上限 (>= UsedBytes)
		caseTag = "仅修改容量上限"
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
		minAllowed := targetModelDisk.UsedBytes
		if minAllowed == 0 {
			minAllowed = 1
		}
		var newMax uint64
		if env.Rng.IntN(3) == 0 {
			// 边界：恰好缩容至 minAllowed (即等于 UsedBytes 或 1)
			newMax = minAllowed
		} else {
			newMax = minAllowed + uint64(env.Rng.IntN(16)+1)*16384
		}
		params.MaxBytes = &newMax
	case branch < 55:
		// 同时修改名称与容量上限
		caseTag = "同时修改名称与容量上限"
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
		newName := env.UniqueDiskName("both")
		minAllowed := targetModelDisk.UsedBytes
		if minAllowed == 0 {
			minAllowed = 1
		}
		newMax := minAllowed + uint64(env.Rng.IntN(20)+1)*32768
		params.Name = &newName
		params.MaxBytes = &newMax
	case branch < 70:
		// 缩容水位保护测试：尝试寻找 UsedBytes > 1 的存活磁盘并设置 1 <= *MaxBytes < UsedBytes
		for _, d := range activeDisks {
			if d.UsedBytes > 1 {
				targetModelDisk = d
				break
			}
		}
		if targetModelDisk != nil {
			caseTag = "缩容低于已用容量(ErrInvalidShrink)"
			params.DiskID = targetModelDisk.DiskID
			shrinkTarget := uint64(1)
			if targetModelDisk.UsedBytes > 2 {
				shrinkTarget = uint64(env.Rng.Uint64N(targetModelDisk.UsedBytes-1)) + 1
			}
			params.MaxBytes = &shrinkTarget
			if env.Rng.IntN(2) == 0 {
				newName := env.UniqueDiskName("shrink-fail")
				params.Name = &newName
			}
		} else {
			// 若当前无 UsedBytes > 1 的磁盘，则测试两者皆为 nil
			caseTag = "Name与MaxBytes皆为nil"
			targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
			params.DiskID = targetModelDisk.DiskID
		}
	case branch < 76:
		// Name 与 MaxBytes 皆为 nil
		caseTag = "Name与MaxBytes皆为nil"
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
	case branch < 82:
		// 不存在的合法 UUID（或已删除的 DiskID）
		caseTag = "目标磁盘不存在(ErrNotFound)"
		if len(env.Model.DeletedDiskIDs) > 0 && env.Rng.IntN(2) == 0 {
			for _, id := range env.Model.AllCreatedDiskIDs {
				if env.Model.DeletedDiskIDs[id] {
					params.DiskID = id
					break
				}
			}
		} else {
			u, err := uuid.NewV7()
			require.NoError(t, err)
			params.DiskID = u.String()
		}
		newName := env.UniqueDiskName("ghost")
		params.Name = &newName
	case branch < 88:
		// 非法 DiskID（格式非法或全零 Nil UUID）
		if env.Rng.IntN(2) == 0 {
			caseTag = "非法DiskID格式"
			params.DiskID = "not-a-valid-uuid"
		} else {
			caseTag = "全零Nil-UUID"
			params.DiskID = "00000000-0000-0000-0000-000000000000"
		}
		newName := env.UniqueDiskName("bad-id")
		params.Name = &newName
	case branch < 94:
		// 非法 MaxBytes（0 或超过 math.MaxInt64）
		caseTag = "非法MaxBytes越界"
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
		var badMax uint64
		if env.Rng.IntN(2) == 0 {
			badMax = 0
		} else {
			badMax = uint64(math.MaxInt64) + 100
		}
		params.MaxBytes = &badMax
	default:
		// 非法 Name（含 '\x00' 或长度 > 255）
		targetModelDisk = activeDisks[env.Rng.IntN(len(activeDisks))]
		params.DiskID = targetModelDisk.DiskID
		var badName string
		if env.Rng.IntN(2) == 0 {
			caseTag = "非法名称(含空字节)"
			badName = "invalid\x00name"
		} else {
			caseTag = "非法名称(超长>255)"
			badName = strings.Repeat("u", meta.MaxDiskNameBytes+5)
		}
		params.Name = &badName
	}

	// 参考模型推导预期行为（严格对齐 manager_disk.go 校验优先级）
	var expectedErr error
	var expectInvalidShrink bool

	parsedUUID, uuidErr := uuid.Parse(params.DiskID)
	if uuidErr != nil || parsedUUID == uuid.Nil {
		expectedErr = meta.ErrInvalidArgument
	} else if params.Name == nil && params.MaxBytes == nil {
		expectedErr = meta.ErrInvalidArgument
	} else if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		expectedErr = meta.ErrInvalidArgument
	} else if params.Name != nil {
		if err := modelValidateDiskName(*params.Name); err != nil {
			expectedErr = err
		}
	}

	if expectedErr == nil {
		d, exists := env.Model.Disks[params.DiskID]
		if !exists || d.Deleted {
			expectedErr = meta.ErrNotFound
		} else if params.MaxBytes != nil && *params.MaxBytes < d.UsedBytes {
			expectedErr = meta.ErrInvalidShrink
			expectInvalidShrink = true
		}
	}

	got, err := env.Mgr.UpdateDisk(ctx, params)
	if expectedErr != nil {
		require.Errorf(t, err, "UpdateDisk[%s] 预期返回错误 %v，但实际成功: %+v", caseTag, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"UpdateDisk[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		if expectInvalidShrink {
			require.True(t, errors.Is(err, meta.ErrInvalidShrink),
				"UpdateDisk 缩容低于 used_bytes 必须匹配 meta.ErrInvalidShrink")
			require.True(t, errors.Is(err, meta.ErrInvalidArgument),
				"UpdateDisk 缩容低于 used_bytes 必须同时满足 errors.Is(err, meta.ErrInvalidArgument)")
		}
		require.Nil(t, got)
		return fmt.Sprintf("UpdateDisk[%s](disk_id=%s) -> 预期拒绝 (%v)", caseTag, params.DiskID, expectedErr)
	}

	require.NoErrorf(t, err, "UpdateDisk[%s] 预期成功，但返回错误: %v", caseTag, err)
	require.NotNil(t, got)

	// 更新内存参考模型并核对返回实体
	modelDisk := env.Model.Disks[params.DiskID]
	if params.Name != nil {
		modelDisk.Name = *params.Name
	}
	if params.MaxBytes != nil {
		modelDisk.MaxBytes = *params.MaxBytes
	}

	require.Equal(t, modelDisk.DiskID, got.DiskID)
	require.Equal(t, modelDisk.Name, got.Name)
	require.Equal(t, modelDisk.RootIno, got.RootIno)
	require.Equal(t, modelDisk.MaxBytes, got.MaxBytes)
	require.Equal(t, modelDisk.UsedBytes, got.UsedBytes)
	require.Equal(t, modelDisk.UsedInodes, got.UsedInodes)

	return fmt.Sprintf("UpdateDisk[%s](disk_id=%s, name=%q, max_bytes=%d, used_bytes=%d) -> 成功",
		caseTag, modelDisk.DiskID, modelDisk.Name, modelDisk.MaxBytes, modelDisk.UsedBytes)
}

// formalActionT1QuotaFileStep 是供 T1 专项测试使用的辅助配额动作：
// 通过公开接口 Create + Release + SetAttr(Size) 在存活磁盘下创建或调整常规文件大小，
// 使磁盘的 used_bytes > 0 且 used_inodes > 1，从而让 UpdateDisk 缩容保护 (ErrInvalidShrink)、
// StatFS 剩余块计算与底表 INV-4 字节求和审计在随机序列中持续获得非零水位碰撞。
func formalActionT1QuotaFileStep(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()
	disk := env.Model.PickActiveDisk(env.Rng)
	if disk == nil {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 查找该磁盘下已有的常规文件
	var diskFiles []*formalInode
	for _, f := range env.Model.ActiveRegFiles() {
		if f.RootIno == disk.RootIno && f.Nlink > 0 {
			diskFiles = append(diskFiles, f)
		}
	}

	// 若尚无常规文件或按概率新建文件
	if len(diskFiles) == 0 || (len(diskFiles) < 4 && env.Rng.IntN(2) == 0) {
		fileName := env.UniqueEntryName("quota_file")
		createRes, err := env.Mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fileName),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, createRes)

		// 立即释放打开句柄，保持无活跃句柄的干净状态
		err = env.Mgr.Release(ctx, meta.ReleaseParams{
			Ino: createRes.Inode.Ino,
			Fh:  createRes.Fh,
		})
		require.NoError(t, err)

		fi := &formalInode{
			Ino:        createRes.Inode.Ino,
			Generation: createRes.Inode.Generation,
			RootIno:    disk.RootIno,
			ParentIno:  0,
			Mode:       createRes.Inode.Attr.Mode,
			UID:        createRes.Inode.Attr.UID,
			GID:        createRes.Inode.Attr.GID,
			Size:       0,
			Nlink:      1,
		}
		env.Model.RecordNewInode(fi)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: fileName}] = fi.Ino
		disk.UsedInodes++

		// 紧接着通过 SetAttr 赋予初始非零文件大小（若配额充足）
		avail := uint64(0)
		if disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		if avail >= 1024 {
			maxAlloc := avail
			if maxAlloc > 32768 {
				maxAlloc = 32768
			}
			targetSize := uint64(env.Rng.Uint64N(maxAlloc-512)) + 512
			updatedInode, err := env.Mgr.SetAttr(ctx, fi.Ino, meta.SetAttrInput{
				Size: &targetSize,
			})
			require.NoError(t, err)
			require.Equal(t, targetSize, updatedInode.Attr.Size)
			fi.Size = targetSize
			fi.Mode = updatedInode.Attr.Mode
			disk.UsedBytes += targetSize
		}

		return fmt.Sprintf("QuotaFileSetup(root_ino=%d, ino=%d, name=%q, size=%d) -> 成功 (used_bytes=%d, used_inodes=%d)",
			disk.RootIno, fi.Ino, fileName, fi.Size, disk.UsedBytes, disk.UsedInodes)
	}

	// 调整已有常规文件的 Size（扩容、缩容或超配额触发 ErrNoSpace）
	targetFile := diskFiles[env.Rng.IntN(len(diskFiles))]
	oldSize := targetFile.Size
	avail := uint64(0)
	if disk.MaxBytes > disk.UsedBytes {
		avail = disk.MaxBytes - disk.UsedBytes
	}

	var newSize uint64
	if env.Rng.IntN(4) == 0 {
		// 故意超出剩余配额触发 ErrNoSpace
		newSize = oldSize + avail + uint64(env.Rng.IntN(4096)+1)
	} else if env.Rng.IntN(2) == 0 && avail > 0 {
		// 合法扩容
		delta := uint64(env.Rng.Uint64N(avail)) + 1
		if delta > 16384 {
			delta = 16384
		}
		newSize = oldSize + delta
	} else {
		// 合法缩容
		if oldSize > 0 {
			newSize = uint64(env.Rng.Uint64N(oldSize))
		} else {
			newSize = 0
		}
	}

	gotInode, err := env.Mgr.SetAttr(ctx, targetFile.Ino, meta.SetAttrInput{
		Size: &newSize,
	})
	if newSize > oldSize && (newSize-oldSize) > avail {
		require.Error(t, err)
		require.True(t, errors.Is(err, meta.ErrNoSpace), "超配额扩容应返回 ErrNoSpace, 实际: %v", err)
		return fmt.Sprintf("QuotaFileResize(ino=%d, old=%d, new=%d, avail=%d) -> 预期拒绝 (ErrNoSpace)",
			targetFile.Ino, oldSize, newSize, avail)
	}

	require.NoError(t, err)
	require.NotNil(t, gotInode)
	require.Equal(t, newSize, gotInode.Attr.Size)
	if newSize > oldSize {
		disk.UsedBytes += newSize - oldSize
	} else {
		disk.UsedBytes -= oldSize - newSize
	}
	targetFile.Size = newSize
	targetFile.Mode = gotInode.Attr.Mode

	return fmt.Sprintf("QuotaFileResize(ino=%d, old=%d -> new=%d) -> 成功 (disk_used_bytes=%d)",
		targetFile.Ino, oldSize, newSize, disk.UsedBytes)
}

// ============================================================================
// 5. T1 只读查账探针：GetDisk 与 StatFS
// ============================================================================

// formalProbeGetDisk 在每步写操作后核对所有存活与已删除磁盘的 GetDisk 响应。
func formalProbeGetDisk(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	for _, diskID := range env.Model.AllCreatedDiskIDs {
		got, err := env.Mgr.GetDisk(ctx, diskID)
		if env.Model.DeletedDiskIDs[diskID] {
			require.Errorf(t, err, "已删除磁盘 %s 调用 GetDisk 应返回 ErrNotFound", diskID)
			require.True(t, errors.Is(err, meta.ErrNotFound), "预期 ErrNotFound，实际: %v", err)
			require.Nil(t, got)
			continue
		}

		expected := env.Model.Disks[diskID]
		require.NoErrorf(t, err, "存活磁盘 %s 调用 GetDisk 不应失败", diskID)
		require.NotNil(t, got)
		assert.Equal(t, expected.DiskID, got.DiskID, "GetDisk.DiskID 不一致")
		assert.Equal(t, expected.Name, got.Name, "GetDisk.Name 不一致")
		assert.Equal(t, expected.RootIno, got.RootIno, "GetDisk.RootIno 不一致")
		assert.Equal(t, expected.MaxBytes, got.MaxBytes, "GetDisk.MaxBytes 不一致")
		assert.Equal(t, expected.UsedBytes, got.UsedBytes, "GetDisk.UsedBytes 不一致")
		assert.Equal(t, expected.UsedInodes, got.UsedInodes, "GetDisk.UsedInodes 不一致")
	}

	// 探测不存在的随机 UUID 返回 ErrNotFound
	randomUUID, err := uuid.NewV7()
	require.NoError(t, err)
	_, err = env.Mgr.GetDisk(ctx, randomUUID.String())
	require.Error(t, err)
	require.True(t, errors.Is(err, meta.ErrNotFound), "未注册 UUID 调用 GetDisk 必须返回 ErrNotFound, 实际: %v", err)
}

// modelExpectedStatFS 根据内存模型中的 formalDisk 计算预期的 StatFSResult。
func modelExpectedStatFS(d *formalDisk) meta.StatFSResult {
	totalBytes := d.MaxBytes
	usedBytes := d.UsedBytes
	var availBytes uint64
	if totalBytes > usedBytes {
		availBytes = totalBytes - usedBytes
	}

	const blockSize uint64 = meta.DefaultStatFSBlockSize // 4096
	blocks := totalBytes / blockSize
	if blocks == 0 && totalBytes > 0 {
		blocks = 1
	}
	bavail := availBytes / blockSize
	bfree := bavail

	return meta.StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      meta.DefaultStatFSBlockSize,
		Frsize:     meta.DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bfree,
		Bavail:     bavail,
		Files:      d.UsedInodes,
		Ffree:      0,
		Namelen:    meta.MaxFileNameBytes,
	}
}

// formalProbeStatFS 在每步写操作后核对存活 root_ino、存活子 Inode 以及不存在/已删除 Inode 的 StatFS 响应。
func formalProbeStatFS(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	// 1. 核对所有存活 Disk 的 RootIno
	for _, disk := range env.Model.ActiveDisks() {
		got, err := env.Mgr.StatFS(ctx, disk.RootIno)
		require.NoErrorf(t, err, "存活磁盘 root_ino=%d 调用 StatFS 不应失败", disk.RootIno)
		require.NotNil(t, got)
		expected := modelExpectedStatFS(disk)
		assert.Equal(t, expected, *got, "StatFS(root_ino=%d) 指标与内存模型不一致", disk.RootIno)
	}

	// 2. 抽样核对存活的非根 Inode（含 nlink == 0 的孤儿节点）调用 StatFS
	activeInodes := env.Model.ActiveInodes()
	if len(activeInodes) > 0 {
		sampleInode := activeInodes[env.ProbeRNG().IntN(len(activeInodes))]
		if disk, ok := env.Model.DiskByRootIno[sampleInode.RootIno]; ok && !disk.Deleted {
			got, err := env.Mgr.StatFS(ctx, sampleInode.Ino)
			require.NoErrorf(t, err, "存活节点 ino=%d (root_ino=%d) 调用 StatFS 不应失败", sampleInode.Ino, sampleInode.RootIno)
			require.NotNil(t, got)
			expected := modelExpectedStatFS(disk)
			assert.Equal(t, expected, *got, "StatFS(ino=%d) 指标与所属磁盘不一致", sampleInode.Ino)
		}
	}

	// 3. 核对已删除 Disk 的 RootIno 返回 ErrNotFound
	for _, rootIno := range env.Model.AllCreatedRootInos {
		if env.Model.DeletedRootInos[rootIno] {
			got, err := env.Mgr.StatFS(ctx, rootIno)
			require.Errorf(t, err, "已删除磁盘 root_ino=%d 调用 StatFS 应返回 ErrNotFound", rootIno)
			require.True(t, errors.Is(err, meta.ErrNotFound), "预期 ErrNotFound，实际: %v", err)
			require.Nil(t, got)
		}
	}

	// 4. 核对不存在的 Ino 返回 ErrNotFound
	nonExistentIno := uint64(math.MaxInt64 - 999999)
	got, err := env.Mgr.StatFS(ctx, nonExistentIno)
	require.Error(t, err)
	require.True(t, errors.Is(err, meta.ErrNotFound), "不存在的 ino 调用 StatFS 必须返回 ErrNotFound, 实际: %v", err)
	require.Nil(t, got)
}

// ============================================================================
// 6. T1 底表数学不变量审计：INV-4 (磁盘容量与节点配额账本守恒)
// ============================================================================

// formalAuditINV4DiskQuota 直接扫描 PostgreSQL 底表 disks 与 inodes，强制校验 INV-4：
// 1. 对所有存活 Disk：
//   - disks.used_bytes == sum(inodes.size WHERE root_ino = disks.root_ino AND (mode & S_IFMT) == S_IFREG)
//   - disks.used_inodes == count(inodes WHERE root_ino = disks.root_ino)
//   - 0 <= used_bytes <= max_bytes 且 used_inodes >= 1
//   - 底表中的 used_bytes、used_inodes、max_bytes、name 与内存参考模型完全一致
//
// 2. 对所有已删除 Disk：
//   - disks、inodes、entries、file_handles、file_locks 中归属该 root_ino 的记录数恒为 0
func formalAuditINV4DiskQuota(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
	for _, r := range env.Model.AllCreatedRootInos {
		rootInos = append(rootInos, int64(r))
	}

	rows, err := env.Pool.Query(ctx, `
		SELECT disk_id::text, name, root_ino, max_bytes, used_bytes, used_inodes
		FROM disks
		WHERE root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "INV-4: 查询底表 disks 失败")
	defer rows.Close()

	type dbDiskRecord struct {
		DiskID     string
		Name       string
		RootIno    int64
		MaxBytes   int64
		UsedBytes  int64
		UsedInodes int64
	}

	dbDisksByRoot := make(map[uint64]dbDiskRecord)
	for rows.Next() {
		var rec dbDiskRecord
		err := rows.Scan(&rec.DiskID, &rec.Name, &rec.RootIno, &rec.MaxBytes, &rec.UsedBytes, &rec.UsedInodes)
		require.NoError(t, err, "INV-4: 扫描 disks 行失败")
		dbDisksByRoot[uint64(rec.RootIno)] = rec
	}
	require.NoError(t, rows.Err())

	// 核对存活 Disk 集合与已删除 Disk 集合
	for _, rootIno := range env.Model.AllCreatedRootInos {
		if env.Model.DeletedRootInos[rootIno] {
			_, exists := dbDisksByRoot[rootIno]
			require.Falsef(t, exists, "INV-4 违例: 已删除磁盘 root_ino=%d 在 disks 底表中仍有残留", rootIno)

			var orphanInodeCount, orphanEntryCount, orphanHandleCount, orphanLockCount int64
			err := env.Pool.QueryRow(ctx, `
				SELECT
					(SELECT COUNT(*) FROM inodes WHERE root_ino = $1),
					(SELECT COUNT(*) FROM entries e JOIN inodes i ON (e.parent_ino = i.ino OR e.ino = i.ino) WHERE i.root_ino = $1),
					(SELECT COUNT(*) FROM file_handles fh JOIN inodes i ON fh.ino = i.ino WHERE i.root_ino = $1),
					(SELECT COUNT(*) FROM file_locks fl JOIN inodes i ON fl.ino = i.ino WHERE i.root_ino = $1)
			`, int64(rootIno)).Scan(&orphanInodeCount, &orphanEntryCount, &orphanHandleCount, &orphanLockCount)
			require.NoError(t, err, "INV-4: 查询已删除磁盘级联残留失败")
			require.Zerof(t, orphanInodeCount, "INV-4 违例: 已删除磁盘 root_ino=%d 在 inodes 底表中残留 %d 条记录", rootIno, orphanInodeCount)
			require.Zerof(t, orphanEntryCount, "INV-4 违例: 已删除磁盘 root_ino=%d 在 entries 底表中残留 %d 条记录", rootIno, orphanEntryCount)
			require.Zerof(t, orphanHandleCount, "INV-4 违例: 已删除磁盘 root_ino=%d 在 file_handles 底表中残留 %d 条记录", rootIno, orphanHandleCount)
			require.Zerof(t, orphanLockCount, "INV-4 违例: 已删除磁盘 root_ino=%d 在 file_locks 底表中残留 %d 条记录", rootIno, orphanLockCount)
			continue
		}

		modelDisk, ok := env.Model.DiskByRootIno[rootIno]
		require.Truef(t, ok, "内存模型缺失存活 root_ino=%d", rootIno)

		dbDisk, exists := dbDisksByRoot[rootIno]
		require.Truef(t, exists, "INV-4 违例: 存活磁盘 root_ino=%d (disk_id=%s) 在底表 disks 中不存在", rootIno, modelDisk.DiskID)

		// 1. 核对底表容量与节点数边界不等式
		require.GreaterOrEqualf(t, dbDisk.UsedBytes, int64(0),
			"INV-4 违例: root_ino=%d 底表 used_bytes (%d) < 0", rootIno, dbDisk.UsedBytes)
		require.LessOrEqualf(t, dbDisk.UsedBytes, dbDisk.MaxBytes,
			"INV-4 违例: root_ino=%d 底表 used_bytes (%d) > max_bytes (%d)", rootIno, dbDisk.UsedBytes, dbDisk.MaxBytes)
		require.GreaterOrEqualf(t, dbDisk.UsedInodes, int64(1),
			"INV-4 违例: root_ino=%d 底表 used_inodes (%d) < 1", rootIno, dbDisk.UsedInodes)

		// 2. 核对底表字段与内存参考模型完全一致
		require.Equalf(t, modelDisk.DiskID, dbDisk.DiskID, "INV-4 违例: root_ino=%d disk_id 不一致", rootIno)
		require.Equalf(t, modelDisk.Name, dbDisk.Name, "INV-4 违例: root_ino=%d name 不一致", rootIno)
		require.Equalf(t, int64(modelDisk.MaxBytes), dbDisk.MaxBytes, "INV-4 违例: root_ino=%d max_bytes 不一致", rootIno)
		require.Equalf(t, int64(modelDisk.UsedBytes), dbDisk.UsedBytes, "INV-4 违例: root_ino=%d used_bytes 与内存模型不一致", rootIno)
		require.Equalf(t, int64(modelDisk.UsedInodes), dbDisk.UsedInodes, "INV-4 违例: root_ino=%d used_inodes 与内存模型不一致", rootIno)

		// 3. 聚合扫描底表 inodes 核验配额守恒等式
		var actualInodeCount int64
		var actualRegBytes int64
		err := env.Pool.QueryRow(ctx, `
			SELECT
				COUNT(*)::bigint,
				COALESCE(SUM(CASE WHEN (mode & $2) = $3 THEN size ELSE 0 END), 0)::bigint
			FROM inodes
			WHERE root_ino = $1
		`, int64(rootIno), int32(meta.S_IFMT), int32(meta.S_IFREG)).Scan(&actualInodeCount, &actualRegBytes)
		require.NoError(t, err, "INV-4: 聚合查询底表 inodes 失败")

		require.Equalf(t, actualRegBytes, dbDisk.UsedBytes,
			"INV-4 违例: root_ino=%d disks.used_bytes (%d) != sum(inodes.size WHERE S_IFREG) (%d)",
			rootIno, dbDisk.UsedBytes, actualRegBytes)
		require.Equalf(t, actualInodeCount, dbDisk.UsedInodes,
			"INV-4 违例: root_ino=%d disks.used_inodes (%d) != count(inodes) (%d)",
			rootIno, dbDisk.UsedInodes, actualInodeCount)
	}
}

// ============================================================================
// 7. 主状态机顶层测试与 T1 专属测试入口
// ============================================================================

// TestFormalMainStateMachine 是主状态机顶层联合测试入口，自动聚合所有已注册的动作、只读探针与底表数学不变量审计器。
func TestFormalMainStateMachine(t *testing.T) {
	runFormalMainStateMachine(t, formalMainRunConfig{})
}

// TestFormalMain_T1_DiskAndQuota 验证 T1 主状态机底座骨架、CreateDisk/UpdateDisk/GetDisk/StatFS 对拍与底表 INV-4 基础审计。
func TestFormalMain_T1_DiskAndQuota(t *testing.T) {
	t.Run("确定性种子复现与PCG伪随机流一致性核验", func(t *testing.T) {
		seed, err := parseFormalSeedString("0x123456789abcdef0")
		require.NoError(t, err)
		require.Equal(t, uint64(0x123456789abcdef0), seed)

		decSeed, err := parseFormalSeedString("1311768467463790320")
		require.NoError(t, err)
		require.Equal(t, seed, decSeed)

		rng1 := newFormalRNG(seed)
		rng2 := newFormalRNG(seed)
		for i := 0; i < 32; i++ {
			require.Equal(t, rng1.Uint64(), rng2.Uint64(), "相同 64 位种子的 PCG 随机序列必须 100% 一致")
		}
	})

	t.Run("CreateDisk与UpdateDisk含缩容水位保护及StatFS与INV-4随机状态机对拍", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 25, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 45, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 30, Fn: formalActionT1QuotaFileStep},
			},
			Probes: []formalNamedProbe{
				{Name: "GetDisk", Fn: formalProbeGetDisk},
				{Name: "StatFS", Fn: formalProbeStatFS},
			},
			Auditors: []formalNamedAuditor{
				{Name: "INV-4_DiskQuota", Fn: formalAuditINV4DiskQuota},
			},
		})
	})
}
