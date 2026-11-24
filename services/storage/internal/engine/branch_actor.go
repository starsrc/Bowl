package engine

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type epochCtxKey struct{}

func WithWriterEpoch(ctx context.Context, epoch uint64) context.Context {
	return context.WithValue(ctx, epochCtxKey{}, epoch)
}

func writerEpochFromContext(ctx context.Context) uint64 {
	if ctx == nil {
		return 0
	}
	v, ok := ctx.Value(epochCtxKey{}).(uint64)
	if !ok {
		return 0
	}
	return v
}

type SetAttrReq struct {
	Mode     *uint32
	UID      *uint32
	GID      *uint32
	Size     *uint64
	Atime    *int64
	Mtime    *int64
	AtimeNow bool
	MtimeNow bool
	CtimeNow bool
	Flags    *uint32
}

type RenameReq struct {
	OldParentIno       uint64
	OldName            string
	NewParentIno       uint64
	NewName            string
	Flags              uint32
	TargetOpenInClient bool
}

type branchOpKind uint8

const (
	branchOpMkdir branchOpKind = iota + 1
	branchOpCreate
	branchOpMknod
	branchOpCreateTmpfile
	branchOpPurgeOrphan
	branchOpSymlink
	branchOpLink
	branchOpUnlink
	branchOpRmdir
	branchOpRename
	branchOpSetAttr
	branchOpSetXattr
	branchOpRemoveXattr
	branchOpWrite
	branchOpFallocate
	branchOpFsync
	branchOpFsyncDir
	branchOpCommit
	branchOpFork
	branchOpReset
	branchOpMerge
	branchOpRelocate
	branchOpMount
	branchOpRenewMount
	branchOpEvictMount
)

type writeCmd struct {
	op    branchOpKind
	epoch uint64
	req   any
	resp  any
	err   error
	done  chan struct{}
}

type BranchActor struct {
	disk       *DiskStore
	branchName string
	commitID   uint64
	isCommit   bool

	mb struct {
		mu       sync.Mutex
		active   []*writeCmd
		draining []*writeCmd
		notify   chan struct{}
		closed   bool
	}

	inFlightSubmit atomic.Int64
	lastActiveNano atomic.Int64
	inodeLocks     [256]sync.Mutex
	loopDone       chan struct{}
}

func newBranchActor(disk *DiskStore, branchName string, commitID uint64, isCommit bool) *BranchActor {
	a := &BranchActor{
		disk:       disk,
		branchName: branchName,
		commitID:   commitID,
		isCommit:   isCommit,
		loopDone:   make(chan struct{}),
	}
	a.mb.notify = make(chan struct{}, 1)
	a.lastActiveNano.Store(disk.now().UnixNano())
	go a.runLoop()
	return a
}

func (a *BranchActor) submit(ctx context.Context, op branchOpKind, req any) (any, error) {
	cmd := &writeCmd{
		op:    op,
		epoch: writerEpochFromContext(ctx),
		req:   req,
		done:  make(chan struct{}),
	}

	a.mb.mu.Lock()
	if a.mb.closed {
		a.mb.mu.Unlock()
		return nil, ErrClosed
	}
	a.mb.active = append(a.mb.active, cmd)
	select {
	case a.mb.notify <- struct{}{}:
	default:
	}
	a.mb.mu.Unlock()

	select {
	case <-cmd.done:
		return cmd.resp, cmd.err
	case <-ctx.Done():
		<-cmd.done
		return nil, ctx.Err()
	}
}

func (a *BranchActor) stop() {
	a.mb.mu.Lock()
	a.mb.closed = true
	select {
	case a.mb.notify <- struct{}{}:
	default:
	}
	a.mb.mu.Unlock()
	<-a.loopDone
}

func (a *BranchActor) runLoop() {
	defer close(a.loopDone)
	for {
		<-a.mb.notify
		for {
			a.mb.mu.Lock()
			if len(a.mb.active) == 0 {
				closed := a.mb.closed
				a.mb.mu.Unlock()
				if closed {
					return
				}
				break
			}
			a.mb.active, a.mb.draining = a.mb.draining[:0], a.mb.active
			a.mb.mu.Unlock()

			a.processTick(a.mb.draining)
		}
	}
}

func isSyncBarrierOp(op branchOpKind) bool {
	switch op {
	case branchOpFsync,
		branchOpFsyncDir,
		branchOpCommit,
		branchOpFork,
		branchOpReset,
		branchOpMerge,
		branchOpMount,
		branchOpRenewMount,
		branchOpEvictMount,
		branchOpRelocate:
		return true
	default:
		return false
	}
}

func (a *BranchActor) processTick(cmds []*writeCmd) {
	if a.disk.beforeTickHook != nil {
		a.disk.beforeTickHook()
	}

	batch := a.disk.db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	succeeded := make([]*writeCmd, 0, len(cmds))
	syncBarrier := false
	for _, cmd := range cmds {
		resp, err := a.execCmd(batch, cmd)
		if err != nil {
			cmd.err = err
			continue
		}
		cmd.resp = resp
		succeeded = append(succeeded, cmd)
		if isSyncBarrierOp(cmd.op) {
			syncBarrier = true
		}
	}

	var commitErr error
	if syncBarrier {
		if syncErr := a.disk.packs.SyncActive(context.Background()); syncErr != nil {
			commitErr = syncErr
		} else {
			if a.disk.walCommitHook != nil {
				a.disk.walCommitHook(true)
			}
			if batch.Count() > 0 {
				commitErr = batch.Commit(pebble.Sync)
			} else {
				commitErr = a.disk.db.LogData(nil, pebble.Sync)
			}
		}
	} else if batch.Count() > 0 {
		if a.disk.walCommitHook != nil {
			a.disk.walCommitHook(false)
		}
		commitErr = batch.Commit(pebble.NoSync)
	}
	if commitErr != nil {
		for _, cmd := range succeeded {
			cmd.resp = nil
			cmd.err = commitErr
		}
	}

	a.lastActiveNano.Store(a.disk.now().UnixNano())
	for _, cmd := range cmds {
		close(cmd.done)
	}
}

func (a *BranchActor) execCmd(batch *pebble.Batch, cmd *writeCmd) (any, error) {
	if cmd.op == branchOpRelocate {
		return a.execRelocate(batch, cmd.req)
	}
	if a.isCommit {
		return nil, ErrInvalid
	}

	branch, layers, err := resolveBranchLayers(batch, a.branchName)
	if err != nil {
		return nil, err
	}
	if cmd.epoch != 0 && cmd.epoch != branch.WriterEpoch && !isMountBranchOp(cmd.op) {
		return nil, ErrStaleEpoch
	}

	switch cmd.op {
	case branchOpMkdir:
		return a.execMkdir(batch, branch, layers, cmd.req)
	case branchOpCreate:
		return a.execCreate(batch, branch, layers, cmd.req)
	case branchOpMknod:
		return a.execMknod(batch, branch, layers, cmd.req)
	case branchOpCreateTmpfile:
		return a.execCreateTmpfile(batch, branch, layers, cmd.req)
	case branchOpPurgeOrphan:
		return nil, a.execPurgeOrphan(batch, branch, layers, cmd.req)
	case branchOpSymlink:
		return a.execSymlink(batch, branch, layers, cmd.req)
	case branchOpLink:
		return a.execLink(batch, branch, layers, cmd.req)
	case branchOpUnlink:
		return nil, a.execUnlink(batch, branch, layers, cmd.req)
	case branchOpRmdir:
		return nil, a.execRmdir(batch, branch, layers, cmd.req)
	case branchOpRename:
		return nil, a.execRename(batch, branch, layers, cmd.req)
	case branchOpSetAttr:
		return a.execSetAttr(batch, branch, layers, cmd.req)
	case branchOpSetXattr:
		return nil, a.execSetXattr(batch, branch, layers, cmd.req)
	case branchOpRemoveXattr:
		return nil, a.execRemoveXattr(batch, branch, layers, cmd.req)
	case branchOpWrite:
		return a.execWrite(batch, branch, layers, cmd.req)
	case branchOpFallocate:
		return nil, a.execFallocate(batch, branch, layers, cmd.req)
	case branchOpFsync:
		return nil, a.execFsync(batch, layers, cmd.req)
	case branchOpFsyncDir:
		return nil, a.execFsyncDir(batch, layers, cmd.req)
	case branchOpCommit:
		return a.execCommit(batch, branch, cmd.req)
	case branchOpFork:
		return a.execFork(batch, branch, cmd.req)
	case branchOpReset:
		return a.execReset(batch, branch, cmd.req)
	case branchOpMerge:
		return a.execMerge(batch, branch, cmd.req)
	case branchOpMount:
		return a.execMountBranch(batch, branch, cmd.req)
	case branchOpRenewMount:
		return a.execRenewMount(batch, branch, cmd.req)
	case branchOpEvictMount:
		return a.execEvictMount(batch, branch, cmd.req)
	default:
		return nil, ErrInvalid
	}
}

func isMountBranchOp(op branchOpKind) bool {
	return op == branchOpMount || op == branchOpRenewMount || op == branchOpEvictMount
}

func (d *DiskStore) acquireBranchActor(branchName string) (*BranchActor, func(), error) {
	if d.closing.Load() {
		return nil, nil, ErrClosed
	}
	if branchName == "" {
		return nil, nil, ErrInvalid
	}

	d.actorsMu.Lock()
	if d.closing.Load() {
		d.actorsMu.Unlock()
		return nil, nil, ErrClosed
	}
	if actor, ok := d.branchActors[branchName]; ok {
		actor.inFlightSubmit.Add(1)
		d.actorsMu.Unlock()
		return actor, func() { actor.inFlightSubmit.Add(-1) }, nil
	}
	d.actorsMu.Unlock()

	if _, err := d.GetBranch(branchName); err != nil {
		return nil, nil, err
	}

	d.actorsMu.Lock()
	if d.closing.Load() {
		d.actorsMu.Unlock()
		return nil, nil, ErrClosed
	}
	actor, ok := d.branchActors[branchName]
	if !ok {
		actor = newBranchActor(d, branchName, 0, false)
		d.branchActors[branchName] = actor
	}
	actor.inFlightSubmit.Add(1)
	d.actorsMu.Unlock()
	return actor, func() { actor.inFlightSubmit.Add(-1) }, nil
}

func (d *DiskStore) acquireCommitActor(commitID uint64) (*BranchActor, func(), error) {
	if d.closing.Load() {
		return nil, nil, ErrClosed
	}
	if commitID == 0 {
		return nil, nil, ErrInvalid
	}

	d.actorsMu.Lock()
	if d.closing.Load() {
		d.actorsMu.Unlock()
		return nil, nil, ErrClosed
	}
	actor, ok := d.commitActors[commitID]
	if !ok {
		actor = newBranchActor(d, "", commitID, true)
		d.commitActors[commitID] = actor
	}
	actor.inFlightSubmit.Add(1)
	d.actorsMu.Unlock()
	return actor, func() { actor.inFlightSubmit.Add(-1) }, nil
}

func (d *DiskStore) ActiveBranchActors() []string {
	d.actorsMu.Lock()
	names := make([]string, 0, len(d.branchActors))
	for name := range d.branchActors {
		names = append(names, name)
	}
	d.actorsMu.Unlock()
	sort.Strings(names)
	return names
}

func (d *DiskStore) EvictIdleActors(maxIdle time.Duration) int {
	nowNano := d.now().UnixNano()
	var toStop []*BranchActor

	d.actorsMu.Lock()
	for name, actor := range d.branchActors {
		if canEvictActor(actor, nowNano, maxIdle) {
			delete(d.branchActors, name)
			toStop = append(toStop, actor)
		}
	}
	for id, actor := range d.commitActors {
		if canEvictActor(actor, nowNano, maxIdle) {
			delete(d.commitActors, id)
			toStop = append(toStop, actor)
		}
	}
	d.actorsMu.Unlock()

	for _, actor := range toStop {
		actor.stop()
	}
	return len(toStop)
}

func canEvictActor(actor *BranchActor, nowNano int64, maxIdle time.Duration) bool {
	if actor.inFlightSubmit.Load() > 0 {
		return false
	}
	idle := time.Duration(nowNano - actor.lastActiveNano.Load())
	if idle < maxIdle {
		return false
	}
	actor.mb.mu.Lock()
	empty := len(actor.mb.active) == 0
	if empty {
		actor.mb.closed = true
		select {
		case actor.mb.notify <- struct{}{}:
		default:
		}
	}
	actor.mb.mu.Unlock()
	return empty
}

func (d *DiskStore) removeBranchActor(branchName string) {
	d.actorsMu.Lock()
	actor, ok := d.branchActors[branchName]
	if ok {
		delete(d.branchActors, branchName)
	}
	d.actorsMu.Unlock()
	if ok {
		actor.stop()
	}
}

func (d *DiskStore) removeCommitActor(commitID uint64) {
	d.actorsMu.Lock()
	actor, ok := d.commitActors[commitID]
	if ok {
		delete(d.commitActors, commitID)
	}
	d.actorsMu.Unlock()
	if ok {
		actor.stop()
	}
}

func (d *DiskStore) stopAllActors() {
	d.actorsMu.Lock()
	all := make([]*BranchActor, 0, len(d.branchActors)+len(d.commitActors))
	for name, actor := range d.branchActors {
		delete(d.branchActors, name)
		all = append(all, actor)
	}
	for id, actor := range d.commitActors {
		delete(d.commitActors, id)
		all = append(all, actor)
	}
	d.actorsMu.Unlock()

	for _, actor := range all {
		actor.stop()
	}
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.ContainsRune(name, '\x00') {
		return ErrInvalid
	}
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	return nil
}

func validateXattrName(name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') {
		return ErrInvalid
	}
	if len(name) > MaxXattrNameBytes {
		return ErrNameTooLong
	}
	return nil
}

type mkdirReq struct {
	parentIno uint64
	name      string
	mode      uint32
	uid       uint32
	gid       uint32
}

func (d *DiskStore) Mkdir(ctx context.Context, branch string, parentIno uint64, name string, mode, uid, gid uint32) (Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if mode > 0o177777 || uid > math.MaxInt32 || gid > math.MaxInt32 {
		return Inode{}, ErrInvalid
	}
	modeType := mode & ModeTypeMask
	if modeType != 0 && modeType != ModeDir {
		return Inode{}, ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpMkdir, &mkdirReq{
		parentIno: parentIno,
		name:      name,
		mode:      ModeDir | (mode & ModePermMask),
		uid:       uid,
		gid:       gid,
	})
	if err != nil {
		return Inode{}, err
	}
	ino, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return ino, nil
}

func (a *BranchActor) execMkdir(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*mkdirReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	if _, entryErr := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}
	if parent.Nlink >= MaxNlink {
		return Inode{}, ErrTooManyLinks
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	mode := req.mode
	gid := req.gid
	if parent.Mode&ModeSetGID != 0 {
		gid = parent.GID
		mode |= ModeSetGID
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  req.parentIno,
		Mode:       mode,
		Nlink:      2,
		UID:        req.uid,
		GID:        gid,
		Size:       DefaultDirSize,
		Atime:      nowNano,
		Mtime:      nowNano,
		Ctime:      nowNano,
		Generation: 1,
	}
	parent.Nlink++
	parent.Mtime = nowNano
	parent.Ctime = nowNano

	childRaw, err := EncodeInode(child)
	if err != nil {
		return Inode{}, err
	}
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return Inode{}, err
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	ent := Entry{Ino: child.Ino, Mode: child.Mode}
	if setErr := batch.Set(LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
		return Inode{}, setErr
	}
	return child, nil
}

type createReq struct {
	parentIno uint64
	name      string
	mode      uint32
	uid       uint32
	gid       uint32
}

func (d *DiskStore) Create(ctx context.Context, branch string, parentIno uint64, name string, mode, uid, gid uint32) (Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if mode > 0o177777 || uid > math.MaxInt32 || gid > math.MaxInt32 {
		return Inode{}, ErrInvalid
	}
	modeType := mode & ModeTypeMask
	if modeType != 0 && modeType != ModeRegular {
		return Inode{}, ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpCreate, &createReq{
		parentIno: parentIno,
		name:      name,
		mode:      ModeRegular | (mode & ModePermMask),
		uid:       uid,
		gid:       gid,
	})
	if err != nil {
		return Inode{}, err
	}
	ino, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return ino, nil
}

func (a *BranchActor) execCreate(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*createReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	if _, entryErr := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	gid := req.gid
	if parent.Mode&ModeSetGID != 0 {
		gid = parent.GID
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  req.parentIno,
		Mode:       req.mode,
		Nlink:      1,
		UID:        req.uid,
		GID:        gid,
		Atime:      nowNano,
		Mtime:      nowNano,
		Ctime:      nowNano,
		Generation: 1,
	}
	parent.Mtime = nowNano
	parent.Ctime = nowNano

	childRaw, err := EncodeInode(child)
	if err != nil {
		return Inode{}, err
	}
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return Inode{}, err
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	ent := Entry{Ino: child.Ino, Mode: child.Mode}
	if setErr := batch.Set(LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
		return Inode{}, setErr
	}
	return child, nil
}

type symlinkReq struct {
	parentIno uint64
	name      string
	target    string
	uid       uint32
	gid       uint32
}

func (d *DiskStore) Symlink(ctx context.Context, branch string, parentIno uint64, name, target string, uid, gid uint32) (Inode, error) {
	if err := validateName(name); err != nil {
		return Inode{}, err
	}
	if target == "" || len(target) > MaxSymlinkTargetBytes || strings.ContainsRune(target, '\x00') {
		return Inode{}, ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpSymlink, &symlinkReq{
		parentIno: parentIno,
		name:      name,
		target:    target,
		uid:       uid,
		gid:       gid,
	})
	if err != nil {
		return Inode{}, err
	}
	ino, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return ino, nil
}

func (a *BranchActor) execSymlink(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*symlinkReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	if _, entryErr := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	gid := req.gid
	if parent.Mode&ModeSetGID != 0 {
		gid = parent.GID
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:           inoNum,
		ParentIno:     req.parentIno,
		Mode:          ModeSymlink | 0o777,
		Nlink:         1,
		UID:           req.uid,
		GID:           gid,
		Size:          uint64(len(req.target)),
		Atime:         nowNano,
		Mtime:         nowNano,
		Ctime:         nowNano,
		Generation:    1,
		SymlinkTarget: []byte(req.target),
	}
	parent.Mtime = nowNano
	parent.Ctime = nowNano

	childRaw, err := EncodeInode(child)
	if err != nil {
		return Inode{}, err
	}
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return Inode{}, err
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerInodeKey(work, child.Ino), EncodeLiveEnvelope(childRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	ent := Entry{Ino: child.Ino, Mode: child.Mode}
	if setErr := batch.Set(LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
		return Inode{}, setErr
	}
	return child, nil
}

type linkReq struct {
	ino          uint64
	newParentIno uint64
	newName      string
}

func (d *DiskStore) Link(ctx context.Context, branch string, ino, newParentIno uint64, newName string) (Inode, error) {
	if err := validateName(newName); err != nil {
		return Inode{}, err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpLink, &linkReq{
		ino:          ino,
		newParentIno: newParentIno,
		newName:      newName,
	})
	if err != nil {
		return Inode{}, err
	}
	out, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return out, nil
}

func (a *BranchActor) execLink(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*linkReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.newParentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if parent.Nlink == 0 {
		return Inode{}, ErrNotFound
	}
	target, err := overlayGetInode(batch, layers, req.ino)
	if err != nil {
		return Inode{}, err
	}
	if target.Mode&ModeTypeMask == ModeDir {
		return Inode{}, ErrPerm
	}
	if target.Nlink >= MaxNlink {
		return Inode{}, ErrTooManyLinks
	}

	work := branch.WorkLayerID
	promoteOrphan := false
	if target.Nlink == 0 {
		if target.Mode&ModeTypeMask != ModeRegular {
			return Inode{}, ErrNotFound
		}
		kind, hasOrphan, orphanErr := readLayerOrphanKind(batch, work, target.Ino)
		if orphanErr != nil {
			return Inode{}, orphanErr
		}
		if !hasOrphan || kind != OrphanTmpfileLinkable {
			return Inode{}, ErrNotFound
		}
		promoteOrphan = true
	}

	if _, entryErr := overlayGetEntry(batch, layers, req.newParentIno, []byte(req.newName)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	wasEverLinked := target.Flags&InodeFlagEverLinked != 0
	nowNano := a.disk.now().UnixNano()
	if promoteOrphan {
		target.Nlink = 1
		target.ParentIno = req.newParentIno
	} else {
		target.Nlink++
		target.Flags |= InodeFlagEverLinked
	}
	target.Ctime = nowNano
	target.WriteSeq++
	parent.Mtime = nowNano
	parent.Ctime = nowNano

	targetRaw, err := EncodeInode(target)
	if err != nil {
		return Inode{}, err
	}
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return Inode{}, err
	}

	if setErr := batch.Set(LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(targetRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return Inode{}, setErr
	}
	ent := Entry{Ino: target.Ino, Mode: target.Mode}
	if setErr := batch.Set(LayerEntryKey(work, req.newParentIno, []byte(req.newName)), EncodeLiveEnvelope(EncodeEntry(ent)), nil); setErr != nil {
		return Inode{}, setErr
	}
	if promoteOrphan {
		if delErr := batch.Delete(LayerOrphanKey(work, target.Ino), nil); delErr != nil {
			return Inode{}, delErr
		}
		return target, nil
	}
	if !wasEverLinked && target.ParentIno != 0 {
		if setErr := batch.Set(LayerParentKey(work, target.Ino, target.ParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
			return Inode{}, setErr
		}
	}
	if setErr := batch.Set(LayerParentKey(work, target.Ino, req.newParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
		return Inode{}, setErr
	}
	return target, nil
}

type unlinkReq struct {
	parentIno    uint64
	name         string
	openInClient bool
}

func (d *DiskStore) Unlink(ctx context.Context, branch string, parentIno uint64, name string, openInClient bool) error {
	if err := validateName(name); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpUnlink, &unlinkReq{
		parentIno:    parentIno,
		name:         name,
		openInClient: openInClient,
	})
	return err
}

func (a *BranchActor) execUnlink(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*unlinkReq)
	if !ok {
		return ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	if parent.Nlink == 0 {
		return ErrNotFound
	}
	ent, err := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name))
	if err != nil {
		return err
	}
	target, err := overlayGetInode(batch, layers, ent.Ino)
	if err != nil {
		return err
	}
	if target.Nlink == 0 {
		return ErrNotFound
	}
	if target.Ino == RootIno {
		return ErrInvalid
	}
	if target.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if target.Mode&ModeTypeMask == ModeDir {
		return ErrIsDir
	}

	work := branch.WorkLayerID
	nowNano := a.disk.now().UnixNano()
	parent.Mtime = nowNano
	parent.Ctime = nowNano
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return err
	}

	if setErr := batch.Set(LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return setErr
	}

	if target.Nlink <= 1 && req.openInClient {
		target.Nlink = 0
		target.Ctime = nowNano
		target.WriteSeq++
		rawTarget, encErr := EncodeInode(target)
		if encErr != nil {
			return encErr
		}
		if setErr := batch.Set(LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(rawTarget), nil); setErr != nil {
			return setErr
		}
		return batch.Set(LayerOrphanKey(work, target.Ino), []byte{OrphanUnlinked}, nil)
	}

	return tombstoneOrDecInode(batch, work, layers, target, nowNano)
}

func tombstoneOrDecInode(batch *pebble.Batch, work uint64, layers []uint64, target Inode, nowNano int64) error {
	if target.Nlink > 1 {
		target.Nlink--
		target.Ctime = nowNano
		raw, err := EncodeInode(target)
		if err != nil {
			return err
		}
		return batch.Set(LayerInodeKey(work, target.Ino), EncodeLiveEnvelope(raw), nil)
	}

	if err := batch.Set(LayerInodeKey(work, target.Ino), EncodeTombstone(), nil); err != nil {
		return err
	}
	xnames, err := overlayListXattr(batch, layers, target.Ino)
	if err != nil {
		return err
	}
	for _, xname := range xnames {
		if setErr := batch.Set(LayerXattrKey(work, target.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
	}
	liveBlks, err := overlayListLiveBlocks(batch, layers, target.Ino, 0)
	if err != nil {
		return err
	}
	for _, blkIdx := range liveBlks {
		if setErr := batch.Set(LayerDataKey(work, target.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
	}
	return nil
}

type rmdirReq struct {
	parentIno uint64
	name      string
}

func (d *DiskStore) Rmdir(ctx context.Context, branch string, parentIno uint64, name string) error {
	if name == ".." {
		return ErrNotEmpty
	}
	if err := validateName(name); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpRmdir, &rmdirReq{
		parentIno: parentIno,
		name:      name,
	})
	return err
}

func (a *BranchActor) execRmdir(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*rmdirReq)
	if !ok {
		return ErrInvalid
	}
	parent, err := overlayGetInode(batch, layers, req.parentIno)
	if err != nil {
		return err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	if parent.Nlink == 0 {
		return ErrNotFound
	}
	ent, err := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name))
	if err != nil {
		return err
	}
	target, err := overlayGetInode(batch, layers, ent.Ino)
	if err != nil {
		return err
	}
	if target.Ino == RootIno {
		return ErrInvalid
	}
	if target.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if target.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	children, err := overlayReadDir(batch, layers, target.Ino)
	if err != nil {
		return err
	}
	if len(children) > 0 {
		return ErrNotEmpty
	}

	work := branch.WorkLayerID
	nowNano := a.disk.now().UnixNano()
	if parent.Nlink > 2 {
		parent.Nlink--
	}
	parent.Mtime = nowNano
	parent.Ctime = nowNano
	parentRaw, err := EncodeInode(parent)
	if err != nil {
		return err
	}
	if setErr := batch.Set(LayerEntryKey(work, req.parentIno, []byte(req.name)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, parent.Ino), EncodeLiveEnvelope(parentRaw), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerInodeKey(work, target.Ino), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	xnames, err := overlayListXattr(batch, layers, target.Ino)
	if err != nil {
		return err
	}
	for _, xname := range xnames {
		if setErr := batch.Set(LayerXattrKey(work, target.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
			return setErr
		}
	}
	return nil
}

func isOverlaySubdirectory(r pebbleReader, layers []uint64, childIno, ancestorIno uint64) (bool, error) {
	seen := make(map[uint64]struct{})
	for cur := childIno; ; {
		if cur == ancestorIno {
			return true, nil
		}
		if cur == RootIno || cur == 0 {
			return false, nil
		}
		if _, ok := seen[cur]; ok {
			return false, ErrInvalid
		}
		seen[cur] = struct{}{}
		anc, err := overlayGetInode(r, layers, cur)
		if err != nil {
			return false, err
		}
		if anc.ParentIno == cur {
			return false, nil
		}
		cur = anc.ParentIno
	}
}

func recordEverLinkedParent(batch *pebble.Batch, work uint64, ino *Inode, newParentIno uint64) error {
	if ino.Mode&ModeTypeMask == ModeDir {
		return nil
	}
	if ino.Flags&InodeFlagEverLinked == 0 && ino.Nlink <= 1 {
		return nil
	}
	if ino.Flags&InodeFlagEverLinked == 0 {
		ino.Flags |= InodeFlagEverLinked
		if ino.ParentIno != 0 && ino.ParentIno != newParentIno {
			if setErr := batch.Set(LayerParentKey(work, ino.Ino, ino.ParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
				return setErr
			}
		}
	}
	return batch.Set(LayerParentKey(work, ino.Ino, newParentIno), EncodeLiveEnvelope(nil), nil)
}

func (d *DiskStore) Rename(ctx context.Context, branch string, req RenameReq) error {
	if req.OldParentIno == 0 || req.OldParentIno > math.MaxInt64 || req.NewParentIno == 0 || req.NewParentIno > math.MaxInt64 {
		return ErrInvalid
	}
	if req.Flags&^(RenameNoReplace|RenameExchange) != 0 || req.Flags == (RenameNoReplace|RenameExchange) {
		return ErrInvalid
	}
	if err := validateName(req.OldName); err != nil {
		return err
	}
	if err := validateName(req.NewName); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpRename, &req)
	return err
}

func (a *BranchActor) execRename(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*RenameReq)
	if !ok {
		return ErrInvalid
	}
	oldParent, err := overlayGetInode(batch, layers, req.OldParentIno)
	if err != nil {
		return err
	}
	if oldParent.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	if oldParent.Nlink == 0 {
		return ErrNotFound
	}
	crossParent := req.OldParentIno != req.NewParentIno
	newParent := oldParent
	if crossParent {
		newParent, err = overlayGetInode(batch, layers, req.NewParentIno)
		if err != nil {
			return err
		}
		if newParent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
		if newParent.Nlink == 0 {
			return ErrNotFound
		}
	}

	srcEnt, err := overlayGetEntry(batch, layers, req.OldParentIno, []byte(req.OldName))
	if err != nil {
		return err
	}
	srcIno, err := overlayGetInode(batch, layers, srcEnt.Ino)
	if err != nil {
		return err
	}
	if srcIno.Nlink == 0 {
		return ErrNotFound
	}
	if srcIno.Ino == RootIno {
		return ErrInvalid
	}
	if !crossParent && req.OldName == req.NewName {
		if req.Flags == RenameNoReplace {
			return ErrExist
		}
		return nil
	}
	if srcIno.Flags&FlagImmutable != 0 {
		return ErrPerm
	}

	var dstEnt Entry
	var dstIno Inode
	hasDst := false
	dstEnt, dstErr := overlayGetEntry(batch, layers, req.NewParentIno, []byte(req.NewName))
	if dstErr == nil {
		dstIno, err = overlayGetInode(batch, layers, dstEnt.Ino)
		if err != nil {
			return err
		}
		if dstIno.Nlink == 0 {
			return ErrNotFound
		}
		if dstIno.Ino == RootIno {
			return ErrInvalid
		}
		if dstIno.Flags&FlagImmutable != 0 {
			return ErrPerm
		}
		if req.Flags == RenameNoReplace {
			return ErrExist
		}
		if dstIno.Ino == srcIno.Ino {
			return nil
		}
		hasDst = true
	} else if !errors.Is(dstErr, ErrNotFound) {
		return dstErr
	}

	if !hasDst && req.Flags == RenameExchange {
		return ErrNotFound
	}

	srcIsDir := srcIno.Mode&ModeTypeMask == ModeDir
	dstIsDir := hasDst && dstIno.Mode&ModeTypeMask == ModeDir

	if srcIsDir && crossParent {
		isSub, subErr := isOverlaySubdirectory(batch, layers, req.NewParentIno, srcIno.Ino)
		if subErr != nil {
			return subErr
		}
		if isSub {
			return ErrInvalid
		}
	}

	work := branch.WorkLayerID
	nowNano := a.disk.now().UnixNano()

	if req.Flags == RenameExchange {
		if dstIsDir && crossParent {
			isSub, subErr := isOverlaySubdirectory(batch, layers, req.OldParentIno, dstIno.Ino)
			if subErr != nil {
				return subErr
			}
			if isSub {
				return ErrInvalid
			}
		}
		if crossParent && srcIsDir && !dstIsDir && newParent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}
		if crossParent && !srcIsDir && dstIsDir && oldParent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}

		if setErr := batch.Set(LayerEntryKey(work, req.OldParentIno, []byte(req.OldName)), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: dstIno.Ino, Mode: dstIno.Mode})), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(LayerEntryKey(work, req.NewParentIno, []byte(req.NewName)), EncodeLiveEnvelope(EncodeEntry(Entry{Ino: srcIno.Ino, Mode: srcIno.Mode})), nil); setErr != nil {
			return setErr
		}
		if recErr := recordEverLinkedParent(batch, work, &srcIno, req.NewParentIno); recErr != nil {
			return recErr
		}
		if recErr := recordEverLinkedParent(batch, work, &dstIno, req.OldParentIno); recErr != nil {
			return recErr
		}
		if crossParent {
			if srcIsDir || srcIno.Nlink == 1 {
				srcIno.ParentIno = req.NewParentIno
			}
			if dstIsDir || dstIno.Nlink == 1 {
				dstIno.ParentIno = req.OldParentIno
			}
			if srcIsDir != dstIsDir {
				if srcIsDir {
					if oldParent.Nlink > 2 {
						oldParent.Nlink--
					}
					newParent.Nlink++
				} else {
					oldParent.Nlink++
					if newParent.Nlink > 2 {
						newParent.Nlink--
					}
				}
			}
		}
		srcIno.Ctime = nowNano
		srcIno.WriteSeq++
		dstIno.Ctime = nowNano
		dstIno.WriteSeq++
		srcRaw, encErr := EncodeInode(srcIno)
		if encErr != nil {
			return encErr
		}
		dstRaw, encErr := EncodeInode(dstIno)
		if encErr != nil {
			return encErr
		}
		if setErr := batch.Set(LayerInodeKey(work, srcIno.Ino), EncodeLiveEnvelope(srcRaw), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(LayerInodeKey(work, dstIno.Ino), EncodeLiveEnvelope(dstRaw), nil); setErr != nil {
			return setErr
		}
		oldParent.Mtime = nowNano
		oldParent.Ctime = nowNano
		oldParentRaw, encErr := EncodeInode(oldParent)
		if encErr != nil {
			return encErr
		}
		if setErr := batch.Set(LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(oldParentRaw), nil); setErr != nil {
			return setErr
		}
		if crossParent {
			newParent.Mtime = nowNano
			newParent.Ctime = nowNano
			newParentRaw, newEncErr := EncodeInode(newParent)
			if newEncErr != nil {
				return newEncErr
			}
			return batch.Set(LayerInodeKey(work, newParent.Ino), EncodeLiveEnvelope(newParentRaw), nil)
		}
		return nil
	}

	if hasDst {
		if srcIsDir && !dstIsDir {
			return ErrNotDir
		}
		if !srcIsDir && dstIsDir {
			return ErrIsDir
		}
		if dstIsDir {
			children, dirErr := overlayReadDir(batch, layers, dstIno.Ino)
			if dirErr != nil {
				return dirErr
			}
			if len(children) > 0 {
				return ErrNotEmpty
			}
		}
	}
	if srcIsDir && crossParent && !hasDst && newParent.Nlink >= MaxNlink {
		return ErrTooManyLinks
	}

	if setErr := batch.Set(LayerEntryKey(work, req.OldParentIno, []byte(req.OldName)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerEntryKey(work, req.NewParentIno, []byte(req.NewName)), EncodeLiveEnvelope(EncodeEntry(srcEnt)), nil); setErr != nil {
		return setErr
	}

	if hasDst {
		switch {
		case dstIsDir:
			if setErr := batch.Set(LayerInodeKey(work, dstIno.Ino), EncodeTombstone(), nil); setErr != nil {
				return setErr
			}
			xnames, xErr := overlayListXattr(batch, layers, dstIno.Ino)
			if xErr != nil {
				return xErr
			}
			for _, xname := range xnames {
				if setErr := batch.Set(LayerXattrKey(work, dstIno.Ino, []byte(xname)), EncodeTombstone(), nil); setErr != nil {
					return setErr
				}
			}
		case dstIno.Nlink <= 1 && req.TargetOpenInClient:
			dstIno.Nlink = 0
			dstIno.Ctime = nowNano
			dstIno.WriteSeq++
			dstRaw, encErr := EncodeInode(dstIno)
			if encErr != nil {
				return encErr
			}
			if setErr := batch.Set(LayerInodeKey(work, dstIno.Ino), EncodeLiveEnvelope(dstRaw), nil); setErr != nil {
				return setErr
			}
			if setErr := batch.Set(LayerOrphanKey(work, dstIno.Ino), []byte{OrphanUnlinked}, nil); setErr != nil {
				return setErr
			}
		default:
			if tbErr := tombstoneOrDecInode(batch, work, layers, dstIno, nowNano); tbErr != nil {
				return tbErr
			}
		}
	}

	if recErr := recordEverLinkedParent(batch, work, &srcIno, req.NewParentIno); recErr != nil {
		return recErr
	}

	srcIno.ParentIno = req.NewParentIno
	srcIno.Ctime = nowNano
	srcIno.WriteSeq++
	srcRaw, err := EncodeInode(srcIno)
	if err != nil {
		return err
	}
	if setErr := batch.Set(LayerInodeKey(work, srcIno.Ino), EncodeLiveEnvelope(srcRaw), nil); setErr != nil {
		return setErr
	}

	if !crossParent {
		if hasDst && srcIsDir && oldParent.Nlink > 2 {
			oldParent.Nlink--
		}
		oldParent.Mtime = nowNano
		oldParent.Ctime = nowNano
		pRaw, encErr := EncodeInode(oldParent)
		if encErr != nil {
			return encErr
		}
		return batch.Set(LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(pRaw), nil)
	}

	if srcIsDir {
		if oldParent.Nlink > 2 {
			oldParent.Nlink--
		}
		if !hasDst {
			newParent.Nlink++
		}
	}
	oldParent.Mtime = nowNano
	oldParent.Ctime = nowNano
	newParent.Mtime = nowNano
	newParent.Ctime = nowNano

	oldParentRaw, err := EncodeInode(oldParent)
	if err != nil {
		return err
	}
	newParentRaw, err := EncodeInode(newParent)
	if err != nil {
		return err
	}
	if setErr := batch.Set(LayerInodeKey(work, oldParent.Ino), EncodeLiveEnvelope(oldParentRaw), nil); setErr != nil {
		return setErr
	}
	return batch.Set(LayerInodeKey(work, newParent.Ino), EncodeLiveEnvelope(newParentRaw), nil)
}

type setAttrCmdReq struct {
	ino           uint64
	req           SetAttrReq
	hasTailRef    bool
	tailTombstone bool
	tailBlkIdx    uint64
	tailRef       BlobRef
}

func validateSetAttrReq(req SetAttrReq) error {
	if req.Mode == nil && req.UID == nil && req.GID == nil && req.Size == nil &&
		req.Atime == nil && req.Mtime == nil && !req.AtimeNow && !req.MtimeNow &&
		!req.CtimeNow && req.Flags == nil {
		return ErrInvalid
	}
	if (req.Atime != nil && req.AtimeNow) || (req.Mtime != nil && req.MtimeNow) {
		return ErrInvalid
	}
	if req.Mode != nil && *req.Mode > 0o177777 {
		return ErrInvalid
	}
	if req.UID != nil && *req.UID > math.MaxInt32 {
		return ErrInvalid
	}
	if req.GID != nil && *req.GID > math.MaxInt32 {
		return ErrInvalid
	}
	if req.Size != nil && *req.Size > MaxFileSize {
		return ErrFileTooLarge
	}
	if req.Flags != nil && *req.Flags > math.MaxInt32 {
		return ErrInvalid
	}
	return nil
}

func validateSetAttrInode(inode Inode, req SetAttrReq) error {
	fileType := inode.Mode & ModeTypeMask
	if req.Size != nil {
		if fileType == ModeDir {
			return ErrIsDir
		}
		if fileType != ModeRegular {
			return ErrInvalid
		}
	}
	if inode.Flags&FlagImmutable != 0 && (req.Flags == nil || *req.Flags&FlagImmutable != 0) {
		return ErrPerm
	}
	if inode.Flags&FlagAppend != 0 && req.Size != nil {
		return ErrPerm
	}
	if req.Mode != nil {
		inputType := *req.Mode & ModeTypeMask
		if fileType == ModeSymlink || (inputType != 0 && inputType != fileType) {
			return ErrInvalid
		}
	}
	return nil
}

func (d *DiskStore) SetAttr(ctx context.Context, branch string, ino uint64, req SetAttrReq) (Inode, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return Inode{}, ErrInvalid
	}
	if err := validateSetAttrReq(req); err != nil {
		return Inode{}, err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return Inode{}, err
	}
	defer release()

	cmdReq := &setAttrCmdReq{
		ino: ino,
		req: req,
	}

	var stagedRefs []BlobRef
	if req.Size != nil {
		mu := &actor.inodeLocks[ino&0xff]
		mu.Lock()
		defer mu.Unlock()

		newSize := *req.Size
		snap := d.db.NewSnapshot()
		branchMeta, layers, layerErr := resolveBranchLayers(snap, branch)
		if layerErr != nil {
			_ = snap.Close()
			return Inode{}, layerErr
		}
		if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
			_ = snap.Close()
			return Inode{}, ErrStaleEpoch
		}
		curInode, inoErr := overlayGetInode(snap, layers, ino)
		if inoErr != nil {
			_ = snap.Close()
			return Inode{}, inoErr
		}
		if valErr := validateSetAttrInode(curInode, req); valErr != nil {
			_ = snap.Close()
			return Inode{}, valErr
		}
		blkSize := uint64(d.blockSize)
		if newSize < curInode.Size && newSize%blkSize != 0 {
			tailBlk := newSize / blkSize
			tailLen := uint32(newSize % blkSize)
			existingRef, exists, refErr := overlayGetDataRef(snap, layers, ino, tailBlk)
			_ = snap.Close()
			if refErr != nil {
				return Inode{}, refErr
			}
			if exists && existingRef.Length > tailLen {
				oldData, readErr := d.packs.ReadBlock(existingRef)
				if readErr != nil {
					return Inode{}, readErr
				}
				trimmed := make([]byte, tailLen)
				copy(trimmed, oldData[:tailLen])
				if isAllZero(trimmed) {
					cmdReq.hasTailRef = true
					cmdReq.tailTombstone = true
					cmdReq.tailBlkIdx = tailBlk
				} else {
					refs, appErr := d.packs.AppendBlocks(ctx, [][]byte{trimmed}, false)
					if appErr != nil {
						return Inode{}, appErr
					}
					stagedRefs = refs
					defer d.packs.ReleaseInFlight(stagedRefs)
					cmdReq.hasTailRef = true
					cmdReq.tailBlkIdx = tailBlk
					cmdReq.tailRef = refs[0]
				}
			}
		} else {
			_ = snap.Close()
		}
	}

	resp, err := actor.submit(ctx, branchOpSetAttr, cmdReq)
	if err != nil {
		return Inode{}, err
	}
	out, ok := resp.(Inode)
	if !ok {
		return Inode{}, ErrInvalidValue
	}
	return out, nil
}

func (a *BranchActor) execSetAttr(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	cmdReq, ok := raw.(*setAttrCmdReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	inode, err := overlayGetInode(batch, layers, cmdReq.ino)
	if err != nil {
		return Inode{}, err
	}
	req := cmdReq.req
	if valErr := validateSetAttrInode(inode, req); valErr != nil {
		return Inode{}, valErr
	}
	nowNano := a.disk.now().UnixNano()
	work := branch.WorkLayerID
	fileType := inode.Mode & ModeTypeMask

	if req.Mode != nil {
		inode.Mode = fileType | (*req.Mode & ModePermMask)
	} else if fileType != ModeDir && (req.UID != nil || req.GID != nil || req.Size != nil) {
		inode.Mode &^= ModeSetUID | ModeSetGID
	}
	if req.UID != nil {
		inode.UID = *req.UID
	}
	if req.GID != nil {
		inode.GID = *req.GID
	}
	if req.Flags != nil {
		inode.Flags = (*req.Flags) | (inode.Flags & InodeFlagEverLinked)
	}
	if req.AtimeNow {
		inode.Atime = nowNano
	} else if req.Atime != nil {
		inode.Atime = *req.Atime
	}
	if req.MtimeNow || (req.Size != nil && req.Mtime == nil) {
		inode.Mtime = nowNano
	} else if req.Mtime != nil {
		inode.Mtime = *req.Mtime
	}
	if req.Size != nil {
		newSize := *req.Size
		blkSize := uint64(a.disk.blockSize)
		if newSize < inode.Size {
			firstDropBlk := (newSize + blkSize - 1) / blkSize
			liveBlks, listErr := overlayListLiveBlocks(batch, layers, inode.Ino, firstDropBlk)
			if listErr != nil {
				return Inode{}, listErr
			}
			for _, blkIdx := range liveBlks {
				if setErr := batch.Set(LayerDataKey(work, inode.Ino, blkIdx), EncodeTombstone(), nil); setErr != nil {
					return Inode{}, setErr
				}
			}
			if cmdReq.hasTailRef {
				if cmdReq.tailTombstone {
					if setErr := batch.Set(LayerDataKey(work, inode.Ino, cmdReq.tailBlkIdx), EncodeTombstone(), nil); setErr != nil {
						return Inode{}, setErr
					}
				} else {
					if setErr := batch.Set(LayerDataKey(work, inode.Ino, cmdReq.tailBlkIdx), EncodeLiveEnvelope(EncodeBlobRef(cmdReq.tailRef)), nil); setErr != nil {
						return Inode{}, setErr
					}
				}
			}
		}
		inode.Size = newSize
	}
	if req.Size != nil || req.UID != nil || req.GID != nil || req.Mode != nil {
		if capErr := clearSecurityCapability(batch, work, layers, inode.Ino); capErr != nil {
			return Inode{}, capErr
		}
	}
	inode.Ctime = nowNano
	inode.WriteSeq++

	encoded, err := EncodeInode(inode)
	if err != nil {
		return Inode{}, err
	}
	if setErr := batch.Set(LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil); setErr != nil {
		return Inode{}, setErr
	}
	return inode, nil
}

type setXattrReq struct {
	ino   uint64
	name  string
	value []byte
	flags uint32
}

func (d *DiskStore) SetXattr(ctx context.Context, branch string, ino uint64, name string, value []byte, flags uint32) error {
	if ino == 0 {
		return ErrInvalid
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	if len(value) > MaxXattrValueBytes {
		return ErrInvalid
	}
	if flags&^(XattrCreate|XattrReplace) != 0 || flags == (XattrCreate|XattrReplace) {
		return ErrInvalid
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpSetXattr, &setXattrReq{
		ino:   ino,
		name:  name,
		value: append([]byte(nil), value...),
		flags: flags,
	})
	return err
}

func (a *BranchActor) execSetXattr(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*setXattrReq)
	if !ok {
		return ErrInvalid
	}
	inode, err := overlayGetInode(batch, layers, req.ino)
	if err != nil {
		return err
	}
	if strings.HasPrefix(req.name, "user.") && !isAllowedUserXattrFileType(inode.Mode) {
		return ErrPerm
	}
	_, curErr := overlayGetXattr(batch, layers, req.ino, []byte(req.name))
	exists := curErr == nil
	if curErr != nil && !errors.Is(curErr, ErrNoData) {
		return curErr
	}
	if req.flags&XattrCreate != 0 && exists {
		return ErrExist
	}
	if req.flags&XattrReplace != 0 && !exists {
		return ErrNoData
	}
	if !exists {
		existingNames, listErr := overlayListXattr(batch, layers, req.ino)
		if listErr != nil {
			return listErr
		}
		total := len(req.name) + 1
		for _, existingName := range existingNames {
			total += len(existingName) + 1
			if total > MaxXattrListBytes {
				return ErrNoSpace
			}
		}
		if total > MaxXattrListBytes {
			return ErrNoSpace
		}
	}

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerXattrKey(work, req.ino, []byte(req.name)), EncodeLiveEnvelope(req.value), nil); setErr != nil {
		return setErr
	}
	inode.Ctime = a.disk.now().UnixNano()
	inode.WriteSeq++
	encoded, err := EncodeInode(inode)
	if err != nil {
		return err
	}
	return batch.Set(LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil)
}

type removeXattrReq struct {
	ino  uint64
	name string
}

func (d *DiskStore) RemoveXattr(ctx context.Context, branch string, ino uint64, name string) error {
	if ino == 0 {
		return ErrInvalid
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpRemoveXattr, &removeXattrReq{
		ino:  ino,
		name: name,
	})
	return err
}

func (a *BranchActor) execRemoveXattr(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*removeXattrReq)
	if !ok {
		return ErrInvalid
	}
	inode, err := overlayGetInode(batch, layers, req.ino)
	if err != nil {
		return err
	}
	if strings.HasPrefix(req.name, "user.") && !isAllowedUserXattrFileType(inode.Mode) {
		return ErrPerm
	}
	if _, curErr := overlayGetXattr(batch, layers, req.ino, []byte(req.name)); curErr != nil {
		return curErr
	}
	work := branch.WorkLayerID
	if setErr := batch.Set(LayerXattrKey(work, req.ino, []byte(req.name)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	inode.Ctime = a.disk.now().UnixNano()
	inode.WriteSeq++
	encoded, err := EncodeInode(inode)
	if err != nil {
		return err
	}
	return batch.Set(LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil)
}

type writeBlockUpdate struct {
	blockIdx uint64
	ref      BlobRef
}

type writeDataReq struct {
	ino     uint64
	offset  uint64
	dataLen uint64
	endSize uint64
	updates []writeBlockUpdate
}

func validateWritableRegularInode(inode Inode, offset, dataLen uint64) error {
	switch inode.Mode & ModeTypeMask {
	case ModeDir:
		return ErrIsDir
	case ModeRegular:
	default:
		return ErrInvalid
	}
	if inode.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if dataLen > 0 && inode.Flags&FlagAppend != 0 && offset != inode.Size {
		return ErrPerm
	}
	return nil
}

func (d *DiskStore) Write(ctx context.Context, branch string, ino uint64, offset int64, data []byte) (int, error) {
	n, _, err := d.WriteWithAttr(ctx, branch, ino, offset, data)
	return n, err
}

func (d *DiskStore) WriteWithAttr(ctx context.Context, branch string, ino uint64, offset int64, data []byte) (int, Inode, error) {
	if ino == 0 || ino > math.MaxInt64 || offset < 0 {
		return 0, Inode{}, ErrInvalid
	}
	if uint64(offset) > MaxFileSize || uint64(len(data)) > MaxFileSize-uint64(offset) {
		return 0, Inode{}, ErrFileTooLarge
	}

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return 0, Inode{}, err
	}
	defer release()

	if len(data) == 0 {
		resp, subErr := actor.submit(ctx, branchOpWrite, &writeDataReq{
			ino:     ino,
			offset:  uint64(offset),
			dataLen: 0,
		})
		if subErr != nil {
			return 0, Inode{}, subErr
		}
		outInode, ok := resp.(Inode)
		if !ok {
			return 0, Inode{}, ErrInvalidValue
		}
		return 0, outInode, nil
	}

	mu := &actor.inodeLocks[ino&0xff]
	mu.Lock()
	defer mu.Unlock()

	snap := d.db.NewSnapshot()
	branchMeta, layers, layerErr := resolveBranchLayers(snap, branch)
	if layerErr != nil {
		_ = snap.Close()
		return 0, Inode{}, layerErr
	}
	if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
		_ = snap.Close()
		return 0, Inode{}, ErrStaleEpoch
	}
	curInode, inoErr := overlayGetInode(snap, layers, ino)
	if inoErr != nil {
		_ = snap.Close()
		return 0, Inode{}, inoErr
	}
	if valErr := validateWritableRegularInode(curInode, uint64(offset), uint64(len(data))); valErr != nil {
		_ = snap.Close()
		return 0, Inode{}, valErr
	}

	blkSize := uint64(d.blockSize)
	startOff := uint64(offset)
	endOff := startOff + uint64(len(data))
	startBlk := startOff / blkSize
	endBlk := (endOff - 1) / blkSize

	numBlks := int(endBlk - startBlk + 1)
	payloads := make([][]byte, 0, numBlks)
	blkIndices := make([]uint64, 0, numBlks)

	for blkIdx := startBlk; blkIdx <= endBlk; blkIdx++ {
		blkStart := blkIdx * blkSize
		inStart := uint64(0)
		if startOff > blkStart {
			inStart = startOff - blkStart
		}
		inEnd := blkSize
		if endOff < blkStart+blkSize {
			inEnd = endOff - blkStart
		}
		srcStart := blkStart + inStart - startOff
		srcEnd := blkStart + inEnd - startOff

		var existingData []byte
		if inStart > 0 || inEnd < blkSize {
			oldRef, exists, refErr := overlayGetDataRef(snap, layers, ino, blkIdx)
			if refErr != nil {
				_ = snap.Close()
				return 0, Inode{}, refErr
			}
			if exists {
				existingData, refErr = d.packs.ReadBlock(oldRef)
				if refErr != nil {
					_ = snap.Close()
					return 0, Inode{}, refErr
				}
			}
		}

		newLen := max(inEnd, uint64(len(existingData)))
		buf := make([]byte, newLen)
		copy(buf, existingData)
		copy(buf[inStart:inEnd], data[srcStart:srcEnd])

		payloads = append(payloads, buf)
		blkIndices = append(blkIndices, blkIdx)
	}
	_ = snap.Close()

	refs, err := d.packs.AppendBlocks(ctx, payloads, false)
	if err != nil {
		return 0, Inode{}, err
	}
	defer d.packs.ReleaseInFlight(refs)

	updates := make([]writeBlockUpdate, len(refs))
	for i := range refs {
		updates[i] = writeBlockUpdate{
			blockIdx: blkIndices[i],
			ref:      refs[i],
		}
	}

	resp, err := actor.submit(ctx, branchOpWrite, &writeDataReq{
		ino:     ino,
		offset:  startOff,
		dataLen: uint64(len(data)),
		endSize: endOff,
		updates: updates,
	})
	if err != nil {
		return 0, Inode{}, err
	}
	outInode, ok := resp.(Inode)
	if !ok {
		return 0, Inode{}, ErrInvalidValue
	}
	return len(data), outInode, nil
}

func (a *BranchActor) execWrite(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) (Inode, error) {
	req, ok := raw.(*writeDataReq)
	if !ok {
		return Inode{}, ErrInvalid
	}
	inode, err := overlayGetInode(batch, layers, req.ino)
	if err != nil {
		return Inode{}, err
	}
	if valErr := validateWritableRegularInode(inode, req.offset, req.dataLen); valErr != nil {
		return Inode{}, valErr
	}
	if req.dataLen == 0 {
		return inode, nil
	}

	work := branch.WorkLayerID
	for _, upd := range req.updates {
		if setErr := batch.Set(LayerDataKey(work, req.ino, upd.blockIdx), EncodeLiveEnvelope(EncodeBlobRef(upd.ref)), nil); setErr != nil {
			return Inode{}, setErr
		}
	}

	if capErr := clearSecurityCapability(batch, work, layers, inode.Ino); capErr != nil {
		return Inode{}, capErr
	}

	nowNano := a.disk.now().UnixNano()
	if req.endSize > inode.Size {
		inode.Size = req.endSize
	}
	inode.Mode &^= ModeSetUID | ModeSetGID
	inode.Mtime = nowNano
	inode.Ctime = nowNano
	inode.WriteSeq++

	encoded, err := EncodeInode(inode)
	if err != nil {
		return Inode{}, err
	}
	if setErr := batch.Set(LayerInodeKey(work, inode.Ino), EncodeLiveEnvelope(encoded), nil); setErr != nil {
		return Inode{}, setErr
	}
	return inode, nil
}
