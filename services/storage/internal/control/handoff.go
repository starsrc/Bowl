package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/handoff"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// 交接（handoff）把一块盘的写权限从一个存储节点转给另一个节点，两端只通过一个远端
// （原型里是共享目录，将来是对象存储）交换数据：
//
//	Push(release)：冻结写入 → Checkpoint → 上传快照 → 改写 HEAD 把 owner 置空
//	Pull         ：下载 HEAD 指向的快照 → 就地安装 → 改写 HEAD 把 owner 置为本节点
//
// 远端的 HEAD 是"谁能写"的唯一权威：可写挂载前会核对 HEAD，不是 owner 的节点一律拒绝。
// 不带 release 的 Push 就是一次同步（备份），owner 不变。

var (
	// ErrHandoffDisabled 表示本节点没有配置交接远端。
	ErrHandoffDisabled = errors.New("handoff remote is not configured")
	// ErrNotOwner 表示远端 HEAD 记录的写者不是本节点。
	ErrNotOwner = errors.New("disk is owned by another node")
	// ErrHandoffInProgress 表示该盘正在被释放，暂不接受新的可写挂载。
	ErrHandoffInProgress = errors.New("disk handoff in progress")
	// ErrLocalDiverged 表示本地副本含有尚未同步到远端的写入，拉取会覆盖它们。
	ErrLocalDiverged = errors.New("local disk has changes that were never pushed")
	// ErrHandoffConflict 表示交接期间 HEAD 被别的节点改写。
	ErrHandoffConflict = handoff.ErrConflict
)

// WithHandoff 启用交接：remote 是两端共享的远端，nodeID 是本节点在 HEAD 里的身份。
func WithHandoff(remote handoff.Remote, nodeID string) Option {
	return func(m *manager) {
		if remote != nil && nodeID != "" {
			m.handoffRemote = remote
			m.nodeID = nodeID
		}
	}
}

// PushDiskParams 定义 PushDisk 输入。
type PushDiskParams struct {
	DiskID string
	// Release 为 true 时在上传完成后放弃写权限，供其他节点认领。
	Release bool
}

// PullDiskParams 定义 PullDisk 输入。
type PullDiskParams struct {
	DiskID string
	// Force 允许从仍被其他节点持有的 HEAD 强行接管，并允许覆盖本地未同步的写入。
	Force bool
	// Mirror 只把本地副本刷新到远端最新快照，不认领写权限：写者仍是原来的节点，
	// 本节点得到一份可以只读挂载的最新副本。
	Mirror bool
}

// HandoffResult 汇报一次交接后的远端状态。
type HandoffResult struct {
	Head  handoff.Head      `json:"head"`
	Stats handoff.PushStats `json:"stats"`
	// Pulled 是下载工作量（Pull 专用）。
	Pulled handoff.PullStats `json:"pulled"`
	// Installed 表示本次调用确实替换了本地副本（Pull 专用）。
	Installed bool `json:"installed"`
	// Unchanged 表示本地自上次同步后没有新写入，本次没有上传任何东西（Push 专用）。
	Unchanged bool `json:"unchanged"`
}

// Handoff 是控制面管理器在启用交接时额外提供的能力。
type Handoff interface {
	PushDisk(ctx context.Context, params PushDiskParams) (*HandoffResult, error)
	PullDisk(ctx context.Context, params PullDiskParams) (*HandoffResult, error)
	HeadDisk(ctx context.Context, diskID string) (*handoff.Head, error)
}

// syncMark 记录本地副本上一次与远端一致时的位置，用来判断之后本地有没有再写过。
type syncMark struct {
	Generation  uint64 `json:"generation"`
	WriterEpoch uint64 `json:"writer_epoch"`
	WriteSeq    uint64 `json:"write_seq"`
	// Snapshot 是本地副本对应的远端快照名，用来判断远端是否有比本地更新的内容。
	Snapshot string `json:"snapshot"`
}

func (m *manager) syncMarkPath(diskID string) string {
	return filepath.Join(m.storageRoot, "handoff", diskID+".json")
}

func (m *manager) writeSyncMark(diskID string, mark syncMark) error {
	p := m.syncMarkPath(diskID)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(mark)
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (m *manager) readSyncMark(diskID string) (syncMark, bool) {
	raw, err := os.ReadFile(m.syncMarkPath(diskID))
	if err != nil {
		return syncMark{}, false
	}
	var mark syncMark
	if json.Unmarshal(raw, &mark) != nil {
		return syncMark{}, false
	}
	return mark, true
}

// checkHandoffOwner 在可写挂载前核对远端 HEAD：从未推送过的盘不受约束，
// 推送过的盘只有 HEAD 记录的 owner 才能写。
func (m *manager) checkHandoffOwner(ctx context.Context, diskID string) error {
	if m.handoffRemote == nil {
		return nil
	}
	head, _, err := handoff.ReadHead(ctx, m.handoffRemote, diskID)
	if errors.Is(err, handoff.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("读取交接 HEAD: %w", err)
	}
	if head.Owner != m.nodeID {
		return fmt.Errorf("%w: head owner is %q, this node is %q", ErrNotOwner, head.Owner, m.nodeID)
	}
	return nil
}

// beginRelease 在租约锁内原子地完成"确认没有可写挂载 + 禁止新的可写挂载"，
// 避免检查之后、HEAD 改写之前又挂进来一个写者。返回的函数用于解除禁止。
func (m *manager) beginRelease(diskID string) (func(), error) {
	now := m.currentTime()
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()
	if m.handoffReleasing[diskID] {
		return nil, ErrHandoffInProgress
	}
	for _, lease := range m.pruneExpiredDiskLeasesLocked(diskID, now) {
		if !lease.readOnly {
			return nil, fmt.Errorf("%w: unmount the writer before releasing", ErrDiskMounted)
		}
	}
	if m.handoffReleasing == nil {
		m.handoffReleasing = make(map[string]bool)
	}
	m.handoffReleasing[diskID] = true
	return func() {
		m.mountsMu.Lock()
		delete(m.handoffReleasing, diskID)
		m.mountsMu.Unlock()
	}, nil
}

// HeadDisk 返回远端 HEAD。
func (m *manager) HeadDisk(ctx context.Context, diskID string) (*handoff.Head, error) {
	if m.handoffRemote == nil {
		return nil, ErrHandoffDisabled
	}
	id, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	head, _, err := handoff.ReadHead(ctx, m.handoffRemote, id.String())
	if errors.Is(err, handoff.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &head, nil
}

// PushDisk 把盘的当前状态固化并上传；Release 时同时交出写权限。
func (m *manager) PushDisk(ctx context.Context, params PushDiskParams) (*HandoffResult, error) {
	if m.handoffRemote == nil {
		return nil, ErrHandoffDisabled
	}
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := id.String()
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	row, err := m.store.GetDisk(ctx, id)
	if err != nil {
		return nil, mapDBErr(err)
	}

	prev, prevRaw, err := handoff.ReadHead(ctx, m.handoffRemote, diskID)
	switch {
	case errors.Is(err, handoff.ErrNotFound):
		prevRaw = nil // 首次推送：HEAD 必须仍不存在
	case err != nil:
		return nil, fmt.Errorf("读取交接 HEAD: %w", err)
	case prev.Owner != m.nodeID:
		return nil, fmt.Errorf("%w: head owner is %q, this node is %q", ErrNotOwner, prev.Owner, m.nodeID)
	}

	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
	if err != nil {
		return nil, err
	}

	if params.Release {
		endRelease, relErr := m.beginRelease(diskID)
		if relErr != nil {
			return nil, relErr
		}
		defer endRelease()
		// 推进写世代：租约已过期但仍握着旧世代的客户端，从这一刻起所有写入都会被拒，
		// 保证快照之后本节点不会再有任何写入落盘。
		if _, _, epochErr := actor.AdvanceEpoch(ctx); epochErr != nil {
			return nil, fmt.Errorf("冻结写世代: %w", epochErr)
		}
	}

	tmp := m.tmpSnapshotPath("handoff-push")
	if mkErr := os.MkdirAll(filepath.Dir(tmp), 0o755); mkErr != nil {
		return nil, mkErr
	}
	if _, err = actor.Checkpoint(ctx, tmp); err != nil {
		return nil, fmt.Errorf("固化交接切面: %w", err)
	}
	defer func() {
		if removeErr := m.removeAll(tmp); removeErr != nil {
			m.collector.incCleanupError()
		}
	}()
	epoch, writeSeq, _, err := store.InspectBranchStat(tmp)
	if err != nil {
		return nil, fmt.Errorf("读取交接切面状态: %w", err)
	}

	if !params.Release && prevRaw != nil {
		if mark, ok := m.readSyncMark(diskID); ok &&
			mark.Generation == prev.Generation && mark.WriterEpoch == epoch && mark.WriteSeq == writeSeq {
			return &HandoffResult{Head: prev, Unchanged: true}, nil
		}
	}

	next := handoff.Head{
		DiskID:      diskID,
		Name:        row.Name,
		Generation:  prev.Generation + 1,
		Owner:       m.nodeID,
		WriterEpoch: epoch,
		WriteSeq:    writeSeq,
		UpdatedAt:   m.currentTime().UTC(),
		UpdatedBy:   m.nodeID,
	}
	if params.Release {
		next.Owner = ""
	}
	next.Snapshot = handoff.SnapshotName(next.Generation)

	stats, err := handoff.PushSnapshot(ctx, m.handoffRemote, diskID, next.Snapshot, tmp, readPackStub)
	if err != nil {
		return nil, fmt.Errorf("上传交接快照: %w", err)
	}
	// 唯一的原子点：在这之前失败，盘仍然归本节点，远端只多了一个没人引用的快照。
	if err := handoff.WriteHead(ctx, m.handoffRemote, prevRaw, &next); err != nil {
		return nil, err
	}
	if err := m.writeSyncMark(diskID, syncMark{Generation: next.Generation, WriterEpoch: epoch, WriteSeq: writeSeq, Snapshot: next.Snapshot}); err != nil {
		m.logger.WarnContext(ctx, "handoff sync mark write failed", "disk_id", diskID, "err", err)
	}
	if params.Release {
		m.clearDiskMounts(diskID)
	}
	m.logger.InfoContext(ctx, "disk pushed",
		"disk_id", diskID,
		"generation", next.Generation,
		"released", params.Release,
		"epoch", epoch,
		"packs_uploaded", stats.PacksUploaded,
		"pack_bytes_uploaded", stats.PackBytes,
	)
	return &HandoffResult{Head: next, Stats: stats}, nil
}

// PullDisk 用远端最新快照替换本地副本，并认领写权限。
func (m *manager) PullDisk(ctx context.Context, params PullDiskParams) (*HandoffResult, error) {
	if m.handoffRemote == nil {
		return nil, ErrHandoffDisabled
	}
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := id.String()
	if openErr := m.ensureOpen(); openErr != nil {
		return nil, openErr
	}

	unlockDisk := m.lockDisk(ctx, diskID)
	defer unlockDisk()

	head, headRaw, err := handoff.ReadHead(ctx, m.handoffRemote, diskID)
	if errors.Is(err, handoff.ErrNotFound) {
		return nil, fmt.Errorf("%w: disk %s was never pushed", ErrNotFound, diskID)
	}
	if err != nil {
		return nil, fmt.Errorf("读取交接 HEAD: %w", err)
	}
	if head.Owner == m.nodeID {
		// 已经是写者：本地就是最新的，不需要也不应该用远端覆盖。
		return &HandoffResult{Head: head}, nil
	}
	if params.Mirror {
		if mark, ok := m.readSyncMark(diskID); ok && mark.Snapshot == head.Snapshot {
			return &HandoffResult{Head: head}, nil // 本地已经是远端最新的快照
		}
	} else if head.Owner != "" && !params.Force {
		return nil, fmt.Errorf("%w: head owner is %q; it must push --release first (or pull --force)", ErrNotOwner, head.Owner)
	}

	row, getErr := m.store.GetDisk(ctx, id)
	hasLocal := getErr == nil
	if getErr != nil && !errors.Is(mapDBErr(getErr), ErrNotFound) {
		return nil, mapDBErr(getErr)
	}
	if hasLocal {
		if localErr := m.checkLocalReplaceable(ctx, diskID, row.LineageID, params.Force); localErr != nil {
			return nil, localErr
		}
	}

	// 先把数据拿到手，再动本地、最后才认领：任何一步失败都不会让 HEAD 指向一个没有数据的节点。
	tmp := m.tmpSnapshotPath("handoff-pull")
	if mkErr := os.MkdirAll(filepath.Dir(tmp), 0o755); mkErr != nil {
		return nil, mkErr
	}
	// 本地已有这块盘时，内容相同的数据包直接复用，只下载离开期间新写出来的部分。
	// 复用前先关掉本地实例：它不再是写者，之后也要被替换，关掉后包文件就不会再变。
	reuseDir := ""
	if hasLocal {
		if closeErr := m.closeLocalActor(diskID); closeErr != nil {
			return nil, closeErr
		}
		reuseDir = m.diskDir(diskID)
	}
	// 本地没有的包只留占位，读到时再取（见 store/pack_remote.go）；打开后后台会逐步补齐。
	pullStats, err := handoff.PullSnapshot(ctx, m.handoffRemote, diskID, head.Snapshot, tmp, reuseDir, deferPack)
	if err != nil {
		return nil, fmt.Errorf("下载交接快照: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			_ = m.removeAll(tmp)
		}
	}()

	if installErr := m.installPulledDir(ctx, diskID, tmp); installErr != nil {
		return nil, installErr
	}
	installed = true
	if err = m.writeSyncMark(diskID, syncMark{Generation: head.Generation, WriterEpoch: head.WriterEpoch, WriteSeq: head.WriteSeq, Snapshot: head.Snapshot}); err != nil {
		m.logger.WarnContext(ctx, "handoff sync mark write failed", "disk_id", diskID, "err", err)
	}

	if !hasLocal {
		if _, createErr := m.store.CreateDisk(ctx, id, head.Name); createErr != nil {
			return nil, fmt.Errorf("登记交接磁盘: %w", mapDBErr(createErr))
		}
		row, err = m.store.GetDisk(ctx, id)
		if err != nil {
			return nil, mapDBErr(err)
		}
	}
	// 打开一次，确认下载下来的快照确实是一块能用的盘，再去认领。
	if _, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID); err != nil {
		return nil, fmt.Errorf("打开交接后的磁盘: %w", err)
	}

	if params.Mirror {
		return &HandoffResult{Head: head, Pulled: pullStats, Installed: true}, nil
	}

	next := head
	next.Generation = head.Generation + 1
	next.Owner = m.nodeID
	next.UpdatedAt = m.currentTime().UTC()
	next.UpdatedBy = m.nodeID
	if err := handoff.WriteHead(ctx, m.handoffRemote, headRaw, &next); err != nil {
		// 认领失败时本地已换成远端快照。这是安全的：本节点不是写者，可写挂载仍被 HEAD 挡住。
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk pulled",
		"disk_id", diskID,
		"generation", next.Generation,
		"snapshot", head.Snapshot,
		"forced", params.Force && head.Owner != "",
		"previous_owner", head.Owner,
	)
	return &HandoffResult{Head: next, Pulled: pullStats, Installed: true}, nil
}

// checkLocalReplaceable 确认本地副本可以被远端快照替换：没有任何挂载，且没有未同步的写入。
func (m *manager) checkLocalReplaceable(ctx context.Context, diskID string, lineageID uuid.UUID, force bool) error {
	now := m.currentTime()
	m.mountsMu.Lock()
	leases := len(m.pruneExpiredDiskLeasesLocked(diskID, now))
	m.mountsMu.Unlock()
	if leases > 0 {
		return fmt.Errorf("%w: unmount it on this node before pulling", ErrDiskMounted)
	}
	if force {
		return nil
	}
	mark, ok := m.readSyncMark(diskID)
	if !ok {
		return fmt.Errorf("%w (no sync record)", ErrLocalDiverged)
	}
	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, lineageID)
	if err != nil {
		return err
	}
	probe := m.tmpSnapshotPath("handoff-probe")
	if mkErr := os.MkdirAll(filepath.Dir(probe), 0o755); mkErr != nil {
		return mkErr
	}
	if _, err = actor.Checkpoint(ctx, probe); err != nil {
		return fmt.Errorf("探测本地写入位置: %w", err)
	}
	defer func() { _ = m.removeAll(probe) }()
	epoch, writeSeq, _, err := store.InspectBranchStat(probe)
	if err != nil {
		return err
	}
	if epoch != mark.WriterEpoch || writeSeq != mark.WriteSeq {
		return fmt.Errorf("%w: local is at epoch %d seq %d, last synced at epoch %d seq %d",
			ErrLocalDiverged, epoch, writeSeq, mark.WriterEpoch, mark.WriteSeq)
	}
	return nil
}

// closeLocalActor 把本地实例从池里摘掉并关闭；之后再访问这块盘会按需重新打开。
func (m *manager) closeLocalActor(diskID string) error {
	m.poolMu.Lock()
	actor := m.actors[diskID]
	delete(m.actors, diskID)
	m.poolMu.Unlock()
	if actor != nil {
		if err := actor.Close(); err != nil {
			return fmt.Errorf("关闭本地磁盘实例: %w", err)
		}
	}
	m.clearDiskMounts(diskID)
	return nil
}

// installPulledDir 关闭本地实例并把下载好的目录换到盘目录上。
func (m *manager) installPulledDir(ctx context.Context, diskID, pulled string) error {
	if err := m.closeLocalActor(diskID); err != nil {
		return err
	}

	dir := m.diskDir(diskID)
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return err
	}
	old := m.tmpSnapshotPath("handoff-old")
	hadOld := false
	if _, statErr := os.Stat(dir); statErr == nil {
		if err := os.Rename(dir, old); err != nil {
			return fmt.Errorf("挪开旧的本地副本: %w", err)
		}
		hadOld = true
	}
	if err := os.Rename(pulled, dir); err != nil {
		if hadOld {
			_ = os.Rename(old, dir)
		}
		return fmt.Errorf("安装交接快照: %w", err)
	}
	if hadOld {
		if err := m.removeAll(old); err != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "handoff stale copy cleanup failed", "disk_id", diskID, "dir", old, "err", err)
		}
	}
	return nil
}

// fenceIfOwnershipLost 在可写租约续期时核对远端写者。一旦发现写权限已被别的节点接管，
// 立刻推进本地写世代：还握着旧世代的客户端从这一刻起所有写入都会失败，
// 把"旧写者继续写"的窗口压到一个续租周期之内。远端暂时读不到时不做处置，照常续期。
func (m *manager) fenceIfOwnershipLost(ctx context.Context, diskID string, actor *store.StoreActor) error {
	err := m.checkHandoffOwner(ctx, diskID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrNotOwner) {
		m.logger.WarnContext(ctx, "handoff owner check failed during renew; lease kept", "disk_id", diskID, "err", err)
		return nil
	}
	if _, _, epochErr := actor.AdvanceEpoch(ctx); epochErr != nil {
		m.logger.WarnContext(ctx, "handoff fence epoch advance failed", "disk_id", diskID, "err", epochErr)
	}
	m.clearDiskMounts(diskID)
	m.logger.WarnContext(ctx, "write ownership lost to another node; local writer fenced", "disk_id", diskID, "err", err)
	return err
}

// WithHandoffSync 设置后台同步周期；0 表示不做后台同步，只在显式 Push 时上传。
func WithHandoffSync(interval time.Duration) Option {
	return func(m *manager) {
		if interval > 0 {
			m.handoffSyncInterval = interval
		}
	}
}

func (m *manager) startHandoffSync() {
	if m.handoffRemote == nil || m.handoffSyncInterval <= 0 {
		return
	}
	m.handoffStop = make(chan struct{})
	m.handoffDone = make(chan struct{})
	go func() {
		defer close(m.handoffDone)
		ticker := time.NewTicker(m.handoffSyncInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.handoffStop:
				return
			case <-ticker.C:
				m.syncDirtyDisks(context.Background())
			}
		}
	}()
}

func (m *manager) stopHandoffSync() {
	if m.handoffStop == nil {
		return
	}
	m.handoffStopOnce.Do(func() { close(m.handoffStop) })
	<-m.handoffDone
}

// syncDirtyDisks 把已经绑定远端、且可能有新写入的盘各同步一次，返回实际上传的盘数。
//
// 只同步推送过至少一次的盘（有同步记录）：首次推送是显式动作，避免把每块临时盘都自动传上去。
// 触发条件用的是固定周期，而不是"某个文件静默 N 秒"：存储引擎按数据包而不是按文件落盘，
// 而且一直在写的文件永远等不到静默。
func (m *manager) syncDirtyDisks(ctx context.Context) int {
	// 候选 = 上一轮以来有过可写租约活动的盘 ∪ 此刻仍被可写挂载的盘。
	// 后者不能省：客户端续租的间隔可能比同步周期长，只看"活动"会漏掉两次续租之间的写入。
	// 候选盘是否真的有新写入，由 PushDisk 比对写序号决定，没变就不上传。
	now := m.currentTime()
	m.mountsMu.Lock()
	candidates := make(map[string]struct{}, len(m.handoffDirty))
	for diskID := range m.handoffDirty {
		candidates[diskID] = struct{}{}
	}
	clear(m.handoffDirty)
	for diskID := range m.activeMounts {
		for _, lease := range m.pruneExpiredDiskLeasesLocked(diskID, now) {
			if !lease.readOnly {
				candidates[diskID] = struct{}{}
			}
		}
	}
	m.mountsMu.Unlock()
	dirty := make([]string, 0, len(candidates))
	for diskID := range candidates {
		dirty = append(dirty, diskID)
	}

	synced := 0
	for _, diskID := range dirty {
		if _, bound := m.readSyncMark(diskID); !bound {
			continue
		}
		res, err := m.PushDisk(ctx, PushDiskParams{DiskID: diskID})
		switch {
		case err == nil && !res.Unchanged:
			synced++
		case err == nil, errors.Is(err, ErrNotOwner), errors.Is(err, ErrNotFound), errors.Is(err, ErrClosed):
			// 没有新写入，或这块盘已经不归本节点 / 已删除：都不需要重试。
		default:
			m.logger.WarnContext(ctx, "handoff background sync failed; will retry", "disk_id", diskID, "err", err)
			m.mountsMu.Lock()
			if m.handoffDirty == nil {
				m.handoffDirty = make(map[string]bool)
			}
			m.handoffDirty[diskID] = true
			m.mountsMu.Unlock()
		}
	}
	return synced
}

// remotePackSource 让存储引擎按内容哈希从交接远端取数据包。
type remotePackSource struct{ remote handoff.Remote }

func (s remotePackSource) FetchPack(ctx context.Context, sha string, size int64, dst string) error {
	return handoff.FetchObject(ctx, s.remote, sha, size, dst)
}

func (s remotePackSource) ReadPackRange(ctx context.Context, sha string, off int64, dst []byte) error {
	return handoff.ReadObjectRange(ctx, s.remote, sha, off, dst)
}

// packSource 返回本节点的数据包来源；没有配置交接远端时为 nil。
func (m *manager) packSource() store.PackSource {
	if m.handoffRemote == nil {
		return nil
	}
	return remotePackSource{remote: m.handoffRemote}
}

func readPackStub(path string) (packName, sha string, size int64, ok bool) {
	packName, isStub := store.PackNameOfStub(filepath.Base(path))
	if !isStub {
		return "", "", 0, false
	}
	stub, err := store.ReadRemotePackStub(path)
	if err != nil {
		return "", "", 0, false
	}
	return packName, stub.SHA256, stub.Size, true
}

func deferPack(packsDir, packName, sha string, size int64) error {
	return store.WriteRemotePackStub(packsDir, packName, store.RemotePackStub{SHA256: sha, Size: size})
}

// hydrateCommitDir 把一个提交切面里尚未下载的数据包补齐。diff / merge 直接按文件处理数据包，
// 需要它们都在本地。
func (m *manager) hydrateCommitDir(ctx context.Context, dir string) error {
	m.commitHydrateMu.Lock()
	defer m.commitHydrateMu.Unlock()
	return store.HydrateDir(ctx, dir, m.packSource())
}

// syncBeforeMount 在挂载前让本地副本跟上远端。产品的前提是节点联网，所以挂载时
// 直接以远端 HEAD 为准，而不是要求用户先手动 pull：
//
//   - 可写挂载：盘已被上一个写者释放时，自动拉取并认领；仍被别的节点持有则不处理，
//     由随后的写者核对拒绝。
//   - 只读挂载：写者在别的节点时，把本地副本刷新到远端最新快照（最多落后一个同步周期）。
//     刷新失败不影响挂载，继续用本地已有的副本。
func (m *manager) syncBeforeMount(ctx context.Context, diskID string, readOnly bool) error {
	if m.handoffRemote == nil {
		return nil
	}
	head, _, err := handoff.ReadHead(ctx, m.handoffRemote, diskID)
	if err != nil || head.Owner == m.nodeID {
		// 没推送过、读不到 HEAD、或本节点就是写者：本地即权威。
		return nil //nolint:nilerr // 读不到 HEAD 时按"本地即权威"继续挂载；写权限仍由随后的写者核对把关
	}
	if readOnly {
		if _, mirrorErr := m.PullDisk(ctx, PullDiskParams{DiskID: diskID, Mirror: true}); mirrorErr != nil {
			m.logger.InfoContext(ctx, "read-only mount uses existing local copy; refresh skipped", "disk_id", diskID, "reason", mirrorErr)
		}
		return nil
	}
	if head.Owner == "" {
		_, pullErr := m.PullDisk(ctx, PullDiskParams{DiskID: diskID})
		return pullErr
	}
	return nil
}
