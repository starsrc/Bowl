package engine

import (
	"context"
	"errors"
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
	Mode  *uint32
	UID   *uint32
	GID   *uint32
	Size  *uint64
	Atime *int64
	Mtime *int64
}

type branchOpKind uint8

const (
	branchOpMkdir branchOpKind = iota + 1
	branchOpCreate
	branchOpSymlink
	branchOpLink
	branchOpUnlink
	branchOpRmdir
	branchOpRename
	branchOpSetAttr
	branchOpSetXattr
	branchOpRemoveXattr
	branchOpWrite
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

func (a *BranchActor) processTick(cmds []*writeCmd) {
	batch := a.disk.db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	succeeded := make([]*writeCmd, 0, len(cmds))
	for _, cmd := range cmds {
		resp, err := a.execCmd(batch, cmd)
		if err != nil {
			cmd.err = err
			continue
		}
		cmd.resp = resp
		succeeded = append(succeeded, cmd)
	}

	var commitErr error
	if batch.Count() > 0 {
		commitErr = batch.Commit(pebble.Sync)
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
	if err := validateName(name); err != nil {
		return Inode{}, err
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
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  req.parentIno,
		Mode:       req.mode,
		Nlink:      2,
		UID:        req.uid,
		GID:        req.gid,
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
	if err := validateName(name); err != nil {
		return Inode{}, err
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
	if _, entryErr := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:        inoNum,
		ParentIno:  req.parentIno,
		Mode:       req.mode,
		Nlink:      1,
		UID:        req.uid,
		GID:        req.gid,
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
	if _, entryErr := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	inoNum, err := a.disk.allocIno()
	if err != nil {
		return Inode{}, err
	}
	nowNano := a.disk.now().UnixNano()
	child := Inode{
		Ino:           inoNum,
		ParentIno:     req.parentIno,
		Mode:          ModeSymlink | 0o777,
		Nlink:         1,
		UID:           req.uid,
		GID:           req.gid,
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
	target, err := overlayGetInode(batch, layers, req.ino)
	if err != nil {
		return Inode{}, err
	}
	if target.Mode&ModeTypeMask == ModeDir {
		return Inode{}, ErrIsDir
	}
	if target.Nlink >= MaxNlink {
		return Inode{}, ErrTooManyLinks
	}
	parent, err := overlayGetInode(batch, layers, req.newParentIno)
	if err != nil {
		return Inode{}, err
	}
	if parent.Mode&ModeTypeMask != ModeDir {
		return Inode{}, ErrNotDir
	}
	if _, entryErr := overlayGetEntry(batch, layers, req.newParentIno, []byte(req.newName)); entryErr == nil {
		return Inode{}, ErrExist
	} else if !errors.Is(entryErr, ErrNotFound) {
		return Inode{}, entryErr
	}

	wasEverLinked := target.Flags&InodeFlagEverLinked != 0
	nowNano := a.disk.now().UnixNano()
	target.Nlink++
	target.Flags |= InodeFlagEverLinked
	target.Ctime = nowNano
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

	work := branch.WorkLayerID
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
	parentIno uint64
	name      string
}

func (d *DiskStore) Unlink(ctx context.Context, branch string, parentIno uint64, name string) error {
	if err := validateName(name); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpUnlink, &unlinkReq{
		parentIno: parentIno,
		name:      name,
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
	ent, err := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name))
	if err != nil {
		return err
	}
	target, err := overlayGetInode(batch, layers, ent.Ino)
	if err != nil {
		return err
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
	ent, err := overlayGetEntry(batch, layers, req.parentIno, []byte(req.name))
	if err != nil {
		return err
	}
	target, err := overlayGetInode(batch, layers, ent.Ino)
	if err != nil {
		return err
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

type renameReq struct {
	oldParentIno uint64
	oldName      string
	newParentIno uint64
	newName      string
}

func (d *DiskStore) Rename(ctx context.Context, branch string, oldParentIno uint64, oldName string, newParentIno uint64, newName string) error {
	if err := validateName(oldName); err != nil {
		return err
	}
	if err := validateName(newName); err != nil {
		return err
	}
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return err
	}
	defer release()

	_, err = actor.submit(ctx, branchOpRename, &renameReq{
		oldParentIno: oldParentIno,
		oldName:      oldName,
		newParentIno: newParentIno,
		newName:      newName,
	})
	return err
}

func (a *BranchActor) execRename(batch *pebble.Batch, branch BranchMeta, layers []uint64, raw any) error {
	req, ok := raw.(*renameReq)
	if !ok {
		return ErrInvalid
	}
	oldParent, err := overlayGetInode(batch, layers, req.oldParentIno)
	if err != nil {
		return err
	}
	if oldParent.Mode&ModeTypeMask != ModeDir {
		return ErrNotDir
	}
	newParent := oldParent
	if req.newParentIno != req.oldParentIno {
		newParent, err = overlayGetInode(batch, layers, req.newParentIno)
		if err != nil {
			return err
		}
		if newParent.Mode&ModeTypeMask != ModeDir {
			return ErrNotDir
		}
	}

	srcEnt, err := overlayGetEntry(batch, layers, req.oldParentIno, []byte(req.oldName))
	if err != nil {
		return err
	}
	if req.oldParentIno == req.newParentIno && req.oldName == req.newName {
		return nil
	}
	srcIno, err := overlayGetInode(batch, layers, srcEnt.Ino)
	if err != nil {
		return err
	}
	srcIsDir := srcIno.Mode&ModeTypeMask == ModeDir

	if srcIsDir && req.oldParentIno != req.newParentIno {
		cur := req.newParentIno
		for cur != RootIno && cur != 0 {
			if cur == srcIno.Ino {
				return ErrInvalid
			}
			anc, ancErr := overlayGetInode(batch, layers, cur)
			if ancErr != nil {
				return ancErr
			}
			if anc.ParentIno == cur {
				break
			}
			cur = anc.ParentIno
		}
	}

	var dstIno Inode
	hasDst := false
	dstEnt, dstErr := overlayGetEntry(batch, layers, req.newParentIno, []byte(req.newName))
	if dstErr == nil {
		if dstEnt.Ino == srcEnt.Ino {
			return nil
		}
		dstIno, err = overlayGetInode(batch, layers, dstEnt.Ino)
		if err != nil {
			return err
		}
		dstIsDir := dstIno.Mode&ModeTypeMask == ModeDir
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
		hasDst = true
	} else if !errors.Is(dstErr, ErrNotFound) {
		return dstErr
	}

	work := branch.WorkLayerID
	nowNano := a.disk.now().UnixNano()

	if setErr := batch.Set(LayerEntryKey(work, req.oldParentIno, []byte(req.oldName)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	if setErr := batch.Set(LayerEntryKey(work, req.newParentIno, []byte(req.newName)), EncodeLiveEnvelope(EncodeEntry(srcEnt)), nil); setErr != nil {
		return setErr
	}

	if hasDst {
		if dstIno.Mode&ModeTypeMask == ModeDir {
			if setErr := batch.Set(LayerInodeKey(work, dstIno.Ino), EncodeTombstone(), nil); setErr != nil {
				return setErr
			}
		} else {
			if tbErr := tombstoneOrDecInode(batch, work, layers, dstIno, nowNano); tbErr != nil {
				return tbErr
			}
		}
	}

	if !srcIsDir && (srcIno.Flags&InodeFlagEverLinked != 0 || srcIno.Nlink > 1) {
		if srcIno.Flags&InodeFlagEverLinked == 0 {
			srcIno.Flags |= InodeFlagEverLinked
			if srcIno.ParentIno != 0 && srcIno.ParentIno != req.newParentIno {
				if setErr := batch.Set(LayerParentKey(work, srcIno.Ino, srcIno.ParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
					return setErr
				}
			}
		}
		if setErr := batch.Set(LayerParentKey(work, srcIno.Ino, req.newParentIno), EncodeLiveEnvelope(nil), nil); setErr != nil {
			return setErr
		}
	}

	srcIno.ParentIno = req.newParentIno
	srcIno.Ctime = nowNano
	srcRaw, err := EncodeInode(srcIno)
	if err != nil {
		return err
	}
	if setErr := batch.Set(LayerInodeKey(work, srcIno.Ino), EncodeLiveEnvelope(srcRaw), nil); setErr != nil {
		return setErr
	}

	if req.oldParentIno == req.newParentIno {
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
	ino        uint64
	req        SetAttrReq
	hasTailRef bool
	tailBlkIdx uint64
	tailRef    BlobRef
}

func (d *DiskStore) SetAttr(ctx context.Context, branch string, ino uint64, req SetAttrReq) (Inode, error) {
	if ino == 0 {
		return Inode{}, ErrInvalid
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
		if newSize > MaxFileSize {
			return Inode{}, ErrFileTooLarge
		}
		snap := d.db.NewSnapshot()
		_, layers, layerErr := resolveBranchLayers(snap, branch)
		if layerErr != nil {
			_ = snap.Close()
			return Inode{}, layerErr
		}
		curInode, inoErr := overlayGetInode(snap, layers, ino)
		if inoErr != nil {
			_ = snap.Close()
			return Inode{}, inoErr
		}
		if curInode.Mode&ModeTypeMask != ModeRegular {
			_ = snap.Close()
			return Inode{}, ErrIsDir
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
	nowNano := a.disk.now().UnixNano()
	work := branch.WorkLayerID

	if req.Mode != nil {
		inode.Mode = (inode.Mode & ModeTypeMask) | (*req.Mode & ModePermMask)
	}
	if req.UID != nil {
		inode.UID = *req.UID
	}
	if req.GID != nil {
		inode.GID = *req.GID
	}
	if req.Atime != nil {
		inode.Atime = *req.Atime
	}
	if req.Mtime != nil {
		inode.Mtime = *req.Mtime
	}
	if req.Size != nil {
		if inode.Mode&ModeTypeMask != ModeRegular {
			return Inode{}, ErrIsDir
		}
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
				if setErr := batch.Set(LayerDataKey(work, inode.Ino, cmdReq.tailBlkIdx), EncodeLiveEnvelope(EncodeBlobRef(cmdReq.tailRef)), nil); setErr != nil {
					return Inode{}, setErr
				}
			}
		}
		inode.Size = newSize
		inode.Mtime = nowNano
		inode.WriteSeq++
	}
	inode.Ctime = nowNano

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

	work := branch.WorkLayerID
	if setErr := batch.Set(LayerXattrKey(work, req.ino, []byte(req.name)), EncodeLiveEnvelope(req.value), nil); setErr != nil {
		return setErr
	}
	inode.Ctime = a.disk.now().UnixNano()
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
	if _, curErr := overlayGetXattr(batch, layers, req.ino, []byte(req.name)); curErr != nil {
		return curErr
	}
	work := branch.WorkLayerID
	if setErr := batch.Set(LayerXattrKey(work, req.ino, []byte(req.name)), EncodeTombstone(), nil); setErr != nil {
		return setErr
	}
	inode.Ctime = a.disk.now().UnixNano()
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
	endSize uint64
	updates []writeBlockUpdate
}

func (d *DiskStore) Write(ctx context.Context, branch string, ino uint64, offset int64, data []byte) (int, error) {
	if ino == 0 || offset < 0 {
		return 0, ErrInvalid
	}
	if uint64(offset) > MaxFileSize || uint64(len(data)) > MaxFileSize-uint64(offset) {
		return 0, ErrFileTooLarge
	}

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return 0, err
	}
	defer release()

	if len(data) == 0 {
		curInode, attrErr := d.GetAttr(ctx, branch, ino)
		if attrErr != nil {
			return 0, attrErr
		}
		if curInode.Mode&ModeTypeMask != ModeRegular {
			return 0, ErrIsDir
		}
		return 0, nil
	}

	mu := &actor.inodeLocks[ino&0xff]
	mu.Lock()
	defer mu.Unlock()

	snap := d.db.NewSnapshot()
	branchMeta, layers, layerErr := resolveBranchLayers(snap, branch)
	if layerErr != nil {
		_ = snap.Close()
		return 0, layerErr
	}
	if epoch := writerEpochFromContext(ctx); epoch != 0 && epoch != branchMeta.WriterEpoch {
		_ = snap.Close()
		return 0, ErrStaleEpoch
	}
	curInode, inoErr := overlayGetInode(snap, layers, ino)
	if inoErr != nil {
		_ = snap.Close()
		return 0, inoErr
	}
	if curInode.Mode&ModeTypeMask != ModeRegular {
		_ = snap.Close()
		return 0, ErrIsDir
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
				return 0, refErr
			}
			if exists {
				existingData, refErr = d.packs.ReadBlock(oldRef)
				if refErr != nil {
					_ = snap.Close()
					return 0, refErr
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
		return 0, err
	}
	defer d.packs.ReleaseInFlight(refs)

	updates := make([]writeBlockUpdate, len(refs))
	for i := range refs {
		updates[i] = writeBlockUpdate{
			blockIdx: blkIndices[i],
			ref:      refs[i],
		}
	}

	_, err = actor.submit(ctx, branchOpWrite, &writeDataReq{
		ino:     ino,
		endSize: endOff,
		updates: updates,
	})
	if err != nil {
		return 0, err
	}
	return len(data), nil
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
	if inode.Mode&ModeTypeMask != ModeRegular {
		return Inode{}, ErrIsDir
	}

	work := branch.WorkLayerID
	for _, upd := range req.updates {
		if setErr := batch.Set(LayerDataKey(work, req.ino, upd.blockIdx), EncodeLiveEnvelope(EncodeBlobRef(upd.ref)), nil); setErr != nil {
			return Inode{}, setErr
		}
	}

	nowNano := a.disk.now().UnixNano()
	if req.endSize > inode.Size {
		inode.Size = req.endSize
	}
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
