package engine

import (
	"context"
	"errors"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
)

func validateBranchName(name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') || len(name) > MaxFileNameBytes {
		return ErrInvalid
	}
	return nil
}

type commitReq struct {
	message string
}

func (d *DiskStore) Commit(ctx context.Context, branch, message string) (CommitMeta, error) {
	if err := validateBranchName(branch); err != nil {
		return CommitMeta{}, err
	}

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return CommitMeta{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpCommit, &commitReq{message: message})
	if err != nil {
		return CommitMeta{}, err
	}
	cmt, ok := resp.(CommitMeta)
	if !ok {
		return CommitMeta{}, ErrInvalidValue
	}
	return cmt, nil
}

func (a *BranchActor) execCommit(batch *pebble.Batch, branch BranchMeta, raw any) (CommitMeta, error) {
	req, ok := raw.(*commitReq)
	if !ok {
		return CommitMeta{}, ErrInvalid
	}
	commitID, err := a.disk.allocCommitID()
	if err != nil {
		return CommitMeta{}, err
	}
	newWorkLayerID, err := a.disk.allocLayerID()
	if err != nil {
		return CommitMeta{}, err
	}

	nowNano := a.disk.now().UnixNano()
	cmt := CommitMeta{
		ID:        commitID,
		ParentID:  branch.HeadCommitID,
		LayerID:   branch.WorkLayerID,
		Kind:      CommitKindManual,
		Message:   req.message,
		CreatedAt: nowNano,
	}
	branch.HeadCommitID = commitID
	branch.WorkLayerID = newWorkLayerID
	branch.UpdatedAt = nowNano

	if setErr := batch.Set(MetaCommitKey(commitID), EncodeCommitMeta(cmt), nil); setErr != nil {
		return CommitMeta{}, setErr
	}
	if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
		return CommitMeta{}, setErr
	}
	return cmt, nil
}

type forkReq struct {
	newBranch string
	message   string
}

type forkResp struct {
	commit CommitMeta
	branch BranchMeta
}

func (d *DiskStore) Fork(ctx context.Context, srcBranch, newBranch, message string) (CommitMeta, BranchMeta, error) {
	if err := validateBranchName(srcBranch); err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	if err := validateBranchName(newBranch); err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	if srcBranch == newBranch {
		return CommitMeta{}, BranchMeta{}, ErrExist
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	if _, err := d.GetBranch(newBranch); err == nil {
		return CommitMeta{}, BranchMeta{}, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return CommitMeta{}, BranchMeta{}, err
	}

	actor, release, err := d.acquireBranchActor(srcBranch)
	if err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpFork, &forkReq{
		newBranch: newBranch,
		message:   message,
	})
	if err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	fr, ok := resp.(forkResp)
	if !ok {
		return CommitMeta{}, BranchMeta{}, ErrInvalidValue
	}
	return fr.commit, fr.branch, nil
}

func (a *BranchActor) execFork(batch *pebble.Batch, branch BranchMeta, raw any) (any, error) {
	req, ok := raw.(*forkReq)
	if !ok {
		return nil, ErrInvalid
	}
	if _, err := readBranchMeta(batch, req.newBranch); err == nil {
		return nil, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	dirty, err := layerHasAnyKey(batch, branch.WorkLayerID)
	if err != nil {
		return nil, err
	}

	commitID, err := a.disk.allocCommitID()
	if err != nil {
		return nil, err
	}
	srcNewWorkID, err := a.disk.allocLayerID()
	if err != nil {
		return nil, err
	}
	dstWorkID, err := a.disk.allocLayerID()
	if err != nil {
		return nil, err
	}

	var commitLayerID uint64
	if dirty {
		commitLayerID = branch.WorkLayerID
	}

	nowNano := a.disk.now().UnixNano()
	cmt := CommitMeta{
		ID:        commitID,
		ParentID:  branch.HeadCommitID,
		LayerID:   commitLayerID,
		Kind:      CommitKindFork,
		Message:   req.message,
		CreatedAt: nowNano,
	}

	branch.HeadCommitID = commitID
	branch.WorkLayerID = srcNewWorkID
	branch.UpdatedAt = nowNano

	dstBranch := BranchMeta{
		Name:         req.newBranch,
		HeadCommitID: commitID,
		WorkLayerID:  dstWorkID,
		WriterEpoch:  1,
		CreatedAt:    nowNano,
		UpdatedAt:    nowNano,
	}

	if setErr := batch.Set(MetaCommitKey(commitID), EncodeCommitMeta(cmt), nil); setErr != nil {
		return nil, setErr
	}
	if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
		return nil, setErr
	}
	if setErr := batch.Set(MetaBranchKey(dstBranch.Name), EncodeBranchMeta(dstBranch), nil); setErr != nil {
		return nil, setErr
	}
	return forkResp{commit: cmt, branch: dstBranch}, nil
}

type resetReq struct {
	targetCommitID uint64
}

func (d *DiskStore) Reset(ctx context.Context, branch string, targetCommitID uint64) (BranchMeta, error) {
	if err := validateBranchName(branch); err != nil {
		return BranchMeta{}, err
	}
	if targetCommitID == 0 {
		return BranchMeta{}, ErrInvalid
	}

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return BranchMeta{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpReset, &resetReq{targetCommitID: targetCommitID})
	if err != nil {
		return BranchMeta{}, err
	}
	bm, ok := resp.(BranchMeta)
	if !ok {
		return BranchMeta{}, ErrInvalidValue
	}
	return bm, nil
}

func (a *BranchActor) execReset(batch *pebble.Batch, branch BranchMeta, raw any) (BranchMeta, error) {
	req, ok := raw.(*resetReq)
	if !ok {
		return BranchMeta{}, ErrInvalid
	}
	if _, err := readCommitMeta(batch, req.targetCommitID); err != nil {
		return BranchMeta{}, err
	}
	newWorkID, err := a.disk.allocLayerID()
	if err != nil {
		return BranchMeta{}, err
	}

	if branch.WorkLayerID != 0 {
		start, end := LayerRangeBounds(branch.WorkLayerID)
		if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
			return BranchMeta{}, delErr
		}
	}

	branch.HeadCommitID = req.targetCommitID
	branch.WorkLayerID = newWorkID
	branch.WriterEpoch++
	branch.UpdatedAt = a.disk.now().UnixNano()

	if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
		return BranchMeta{}, setErr
	}
	return branch, nil
}

func (d *DiskStore) DeleteBranch(_ context.Context, branch string) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if err := validateBranchName(branch); err != nil {
		return err
	}
	if branch == d.DiskMeta().DefaultBranch {
		return ErrDefaultBranch
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	bm, err := d.GetBranch(branch)
	if err != nil {
		return err
	}
	d.removeBranchActor(branch)

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if delErr := batch.Delete(MetaBranchKey(branch), nil); delErr != nil {
		return delErr
	}
	if bm.WorkLayerID != 0 {
		start, end := LayerRangeBounds(bm.WorkLayerID)
		if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
			return delErr
		}
	}
	return batch.Commit(pebble.Sync)
}

func reachableCommitsFromReader(r pebbleReader) (map[uint64]struct{}, error) {
	bPrefix := MetaBranchPrefix()
	bIter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: bPrefix,
		UpperBound: PrefixUpperBound(bPrefix),
	})
	if err != nil {
		return nil, err
	}
	var heads []uint64
	for bIter.First(); bIter.Valid(); bIter.Next() {
		bm, decErr := DecodeBranchMeta(bIter.Value())
		if decErr != nil {
			_ = bIter.Close()
			return nil, decErr
		}
		if bm.HeadCommitID != 0 {
			heads = append(heads, bm.HeadCommitID)
		}
	}
	if closeErr := bIter.Close(); closeErr != nil {
		return nil, closeErr
	}

	reachable := make(map[uint64]struct{})
	for _, head := range heads {
		cur := head
		for cur != 0 {
			if _, seen := reachable[cur]; seen {
				break
			}
			reachable[cur] = struct{}{}
			cmt, cmtErr := readCommitMeta(r, cur)
			if cmtErr != nil {
				if errors.Is(cmtErr, ErrNotFound) {
					break
				}
				return nil, cmtErr
			}
			cur = cmt.ParentID
		}
	}
	return reachable, nil
}

func (d *DiskStore) DeleteCommit(_ context.Context, commitID uint64) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if commitID == 0 {
		return ErrInvalid
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	snap := d.db.NewSnapshot()
	cmt, err := readCommitMeta(snap, commitID)
	if err != nil {
		_ = snap.Close()
		return err
	}
	reachable, err := reachableCommitsFromReader(snap)
	closeErr := snap.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if _, inUse := reachable[commitID]; inUse {
		return ErrCommitInUse
	}

	if cmt.LayerID != 0 {
		d.removeCommitActor(cmt.LayerID)
	}

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if delErr := batch.Delete(MetaCommitKey(commitID), nil); delErr != nil {
		return delErr
	}
	if cmt.LayerID != 0 {
		start, end := LayerRangeBounds(cmt.LayerID)
		if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
			return delErr
		}
	}
	return batch.Commit(pebble.Sync)
}

func (d *DiskStore) PruneCommits(_ context.Context) (int, error) {
	if d.closing.Load() {
		return 0, ErrClosed
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	snap := d.db.NewSnapshot()
	reachable, err := reachableCommitsFromReader(snap)
	if err != nil {
		_ = snap.Close()
		return 0, err
	}

	prefix := MetaCommitPrefix()
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		_ = snap.Close()
		return 0, err
	}
	var unreachable []CommitMeta
	for iter.First(); iter.Valid(); iter.Next() {
		cmt, decErr := DecodeCommitMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			_ = snap.Close()
			return 0, decErr
		}
		if _, ok := reachable[cmt.ID]; !ok {
			unreachable = append(unreachable, cmt)
		}
	}
	iterCloseErr := iter.Close()
	snapCloseErr := snap.Close()
	if iterCloseErr != nil || snapCloseErr != nil {
		return 0, errors.Join(iterCloseErr, snapCloseErr)
	}
	if len(unreachable) == 0 {
		return 0, nil
	}

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, cmt := range unreachable {
		if cmt.LayerID != 0 {
			d.removeCommitActor(cmt.LayerID)
		}
		if delErr := batch.Delete(MetaCommitKey(cmt.ID), nil); delErr != nil {
			return 0, delErr
		}
		if cmt.LayerID != 0 {
			start, end := LayerRangeBounds(cmt.LayerID)
			if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
				return 0, delErr
			}
		}
	}
	if commitErr := batch.Commit(pebble.Sync); commitErr != nil {
		return 0, commitErr
	}
	return len(unreachable), nil
}

func (d *DiskStore) RegisterMount(_ context.Context, branch string) (MountMeta, error) {
	if d.closing.Load() {
		return MountMeta{}, ErrClosed
	}
	bm, err := d.GetBranch(branch)
	if err != nil {
		return MountMeta{}, err
	}
	mount := MountMeta{
		ID:          uuid.NewString(),
		BranchName:  branch,
		WriterEpoch: bm.WriterEpoch,
		MountedAt:   d.now().UnixNano(),
	}
	if setErr := d.db.Set(MetaMountKey(mount.ID), EncodeMountMeta(mount), pebble.Sync); setErr != nil {
		return MountMeta{}, setErr
	}
	return mount, nil
}

func (d *DiskStore) GetMount(mountID string) (MountMeta, error) {
	if d.closing.Load() {
		return MountMeta{}, ErrClosed
	}
	if mountID == "" {
		return MountMeta{}, ErrInvalid
	}
	val, closer, err := d.db.Get(MetaMountKey(mountID))
	if errors.Is(err, pebble.ErrNotFound) {
		return MountMeta{}, ErrNotFound
	}
	if err != nil {
		return MountMeta{}, err
	}
	meta, decErr := DecodeMountMeta(val)
	closeErr := closer.Close()
	if decErr != nil || closeErr != nil {
		return MountMeta{}, errors.Join(decErr, closeErr)
	}
	return meta, nil
}

func (d *DiskStore) Unmount(_ context.Context, mountID string) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if mountID == "" {
		return ErrInvalid
	}
	return d.db.Delete(MetaMountKey(mountID), pebble.Sync)
}
