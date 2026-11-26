package engine

import (
	"context"
	"errors"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

func validateBranchName(name string) error {
	if name == "" || strings.ContainsRune(name, '\x00') || len(name) > MaxFileNameBytes {
		return ErrInvalid
	}
	return nil
}

func (d *DiskStore) Commit(ctx context.Context, branch, message string) (CommitMeta, error) {
	if d.closing.Load() {
		return CommitMeta{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBranchName(branch); err != nil {
		return CommitMeta{}, err
	}

	if lockErr := d.lockGCShared(ctx); lockErr != nil {
		return CommitMeta{}, lockErr
	}
	defer d.unlockGCShared()

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	var cmt CommitMeta
	err := d.execBranchOp(ctx, branch, true, true, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, _ *branchLayerCursor) error {
		if d.beforeCommitExecHook != nil {
			if hookErr := d.beforeCommitExecHook(bm.Name); hookErr != nil {
				return hookErr
			}
		}
		commitID, err := d.allocCommitID()
		if err != nil {
			return err
		}
		newWorkLayerID, err := d.allocLayerID()
		if err != nil {
			return err
		}
		dk := d.keys
		migratedOwned, orphanEff, migErr := d.migrateOrphansAcrossLayerSwap(batch, bm.WorkLayerID, newWorkLayerID, bm.HeadCommitID)
		if migErr != nil {
			return migErr
		}

		nowNano := d.now().UnixNano()
		cmt = CommitMeta{
			ID:           commitID,
			ParentID:     bm.HeadCommitID,
			LayerID:      bm.WorkLayerID,
			Kind:         CommitKindManual,
			OwnedBytes:   saturatingSub(bm.WorkOwnedBytes, migratedOwned),
			LogicalBytes: saturatingSub(bm.LogicalBytes, orphanEff),
			Message:      message,
			CreatedAt:    nowNano,
		}
		bm.HeadCommitID = commitID
		bm.WorkLayerID = newWorkLayerID
		bm.WorkOwnedBytes = migratedOwned
		bm.UpdatedAt = nowNano

		if setErr := batch.Set(dk.MetaCommitKey(commitID), EncodeCommitMeta(cmt), nil); setErr != nil {
			return setErr
		}
		return batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil)
	})
	if err != nil {
		return CommitMeta{}, err
	}
	return cmt, nil
}

func (d *DiskStore) Fork(ctx context.Context, srcBranch, newBranch, message string) (CommitMeta, BranchMeta, error) {
	if d.closing.Load() {
		return CommitMeta{}, BranchMeta{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBranchName(srcBranch); err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	if err := validateBranchName(newBranch); err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	if srcBranch == newBranch {
		return CommitMeta{}, BranchMeta{}, ErrExist
	}

	if lockErr := d.lockGCShared(ctx); lockErr != nil {
		return CommitMeta{}, BranchMeta{}, lockErr
	}
	defer d.unlockGCShared()

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	if _, err := d.GetBranch(newBranch); err == nil {
		return CommitMeta{}, BranchMeta{}, ErrExist
	} else if !errors.Is(err, ErrNotFound) {
		return CommitMeta{}, BranchMeta{}, err
	}

	var (
		cmt       CommitMeta
		dstBranch BranchMeta
	)
	err := d.execBranchOp(ctx, srcBranch, true, true, func(batch *pebble.Batch, _ *opQuotaTracker, bm BranchMeta, _ *branchLayerCursor) error {
		dk := d.keys
		if _, err := readBranchMeta(batch, dk, newBranch); err == nil {
			return ErrExist
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if d.beforeForkExecHook != nil {
			if hookErr := d.beforeForkExecHook(bm.Name, newBranch); hookErr != nil {
				return hookErr
			}
		}

		commitID, err := d.allocCommitID()
		if err != nil {
			return err
		}
		srcNewWorkID, err := d.allocLayerID()
		if err != nil {
			return err
		}
		dstWorkID, err := d.allocLayerID()
		if err != nil {
			return err
		}
		migratedOwned, orphanEff, migErr := d.migrateOrphansAcrossLayerSwap(batch, bm.WorkLayerID, srcNewWorkID, bm.HeadCommitID)
		if migErr != nil {
			return migErr
		}

		dirty, err := layerHasAnyKey(batch, dk, bm.WorkLayerID)
		if err != nil {
			return err
		}

		var commitLayerID uint64
		var commitOwned uint64
		if dirty {
			commitLayerID = bm.WorkLayerID
			commitOwned = saturatingSub(bm.WorkOwnedBytes, migratedOwned)
		}
		commitLogical := saturatingSub(bm.LogicalBytes, orphanEff)

		nowNano := d.now().UnixNano()
		cmt = CommitMeta{
			ID:           commitID,
			ParentID:     bm.HeadCommitID,
			LayerID:      commitLayerID,
			Kind:         CommitKindFork,
			OwnedBytes:   commitOwned,
			LogicalBytes: commitLogical,
			Message:      message,
			CreatedAt:    nowNano,
		}

		bm.HeadCommitID = commitID
		bm.WorkLayerID = srcNewWorkID
		bm.WorkOwnedBytes = migratedOwned
		bm.UpdatedAt = nowNano

		dstBranch = BranchMeta{
			Name:           newBranch,
			HeadCommitID:   commitID,
			WorkLayerID:    dstWorkID,
			WriterEpoch:    1,
			WorkOwnedBytes: 0,
			LogicalBytes:   commitLogical,
			CreatedAt:      nowNano,
			UpdatedAt:      nowNano,
		}

		if setErr := batch.Set(dk.MetaCommitKey(commitID), EncodeCommitMeta(cmt), nil); setErr != nil {
			return setErr
		}
		if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
			return setErr
		}
		return batch.Set(dk.MetaBranchKey(dstBranch.Name), EncodeBranchMeta(dstBranch), nil)
	})
	if err != nil {
		return CommitMeta{}, BranchMeta{}, err
	}
	return cmt, dstBranch, nil
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

	var out BranchMeta
	err := d.execBranchOp(ctx, branch, true, true, func(batch *pebble.Batch, quota *opQuotaTracker, bm BranchMeta, _ *branchLayerCursor) error {
		dk := d.keys
		mounted, err := hasActiveMountOnReader(batch, dk, bm.Name, d.now().UnixNano(), false)
		if err != nil {
			return err
		}
		if mounted {
			return ErrDiskMounted
		}
		targetCmt, cmtErr := readCommitMeta(batch, dk, targetCommitID)
		if cmtErr != nil {
			return cmtErr
		}
		newWorkID, err := d.allocLayerID()
		if err != nil {
			return err
		}

		if bm.WorkLayerID != 0 {
			start, end := dk.LayerRangeBounds(bm.WorkLayerID)
			if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
				return delErr
			}
		}

		freedWork := bm.WorkOwnedBytes
		bm.HeadCommitID = targetCommitID
		bm.WorkLayerID = newWorkID
		bm.WriterEpoch++
		bm.WorkOwnedBytes = 0
		bm.LogicalBytes = targetCmt.LogicalBytes
		bm.UpdatedAt = d.now().UnixNano()

		if setErr := batch.Set(dk.MetaBranchKey(bm.Name), EncodeBranchMeta(bm), nil); setErr != nil {
			return setErr
		}
		quota.release(freedWork)
		out = bm
		return nil
	})
	if err != nil {
		return BranchMeta{}, err
	}
	d.TriggerGC()
	return out, nil
}

func (d *DiskStore) DeleteBranch(ctx context.Context, branch string) error {
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
	dk := d.keys
	mounted, err := hasActiveMountOnReader(d.db, dk, branch, d.now().UnixNano(), false)
	if err != nil {
		return err
	}
	if mounted {
		return ErrDiskMounted
	}
	staleMounts, err := listAllMountsFromReader(d.db, dk, branch)
	if err != nil {
		return err
	}
	d.removeBranchLock(branch)

	bm, err = d.GetBranch(branch)
	if err != nil {
		return err
	}

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if delErr := batch.Delete(dk.MetaBranchKey(branch), nil); delErr != nil {
		return delErr
	}
	for _, m := range staleMounts {
		if delErr := batch.Delete(dk.MetaMountKey(m.ID), nil); delErr != nil {
			return delErr
		}
	}
	if bm.WorkLayerID != 0 {
		start, end := dk.LayerRangeBounds(bm.WorkLayerID)
		if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
			return delErr
		}
	}
	if commitErr := d.commitBatchSync(ctx, batch); commitErr != nil {
		return commitErr
	}
	d.releaseQuota(bm.WorkOwnedBytes)
	d.TriggerGC()
	return nil
}

func reachableCommitsFromReader(r pebbleReader, dk diskKeys) (map[uint64]struct{}, error) {
	bPrefix := dk.MetaBranchPrefix()
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
	stack := make([]uint64, 0, len(heads))
	stack = append(stack, heads...)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if cur == 0 {
			continue
		}
		if _, seen := reachable[cur]; seen {
			continue
		}
		reachable[cur] = struct{}{}
		cmt, cmtErr := readCommitMeta(r, dk, cur)
		if cmtErr != nil {
			if errors.Is(cmtErr, ErrNotFound) {
				continue
			}
			return nil, cmtErr
		}
		if cmt.ParentID != 0 {
			stack = append(stack, cmt.ParentID)
		}
		if cmt.MergeParentID != 0 {
			stack = append(stack, cmt.MergeParentID)
		}
	}
	return reachable, nil
}

func (d *DiskStore) DeleteCommit(ctx context.Context, commitID uint64) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if commitID == 0 {
		return ErrInvalid
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	dk := d.keys
	snap := d.db.NewSnapshot()
	cmt, err := readCommitMeta(snap, dk, commitID)
	if err != nil {
		_ = snap.Close()
		return err
	}
	reachable, err := reachableCommitsFromReader(snap, dk)
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

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()

	if delErr := batch.Delete(dk.MetaCommitKey(commitID), nil); delErr != nil {
		return delErr
	}
	if cmt.LayerID != 0 {
		start, end := dk.LayerRangeBounds(cmt.LayerID)
		if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
			return delErr
		}
	}
	if commitErr := d.commitBatchSync(ctx, batch); commitErr != nil {
		return commitErr
	}
	d.invalidateCommitCache(commitID)
	d.releaseQuota(cmt.OwnedBytes)
	d.TriggerGC()
	return nil
}

func (d *DiskStore) PruneCommits(ctx context.Context) (int, error) {
	if d.closing.Load() {
		return 0, ErrClosed
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	dk := d.keys
	snap := d.db.NewSnapshot()
	reachable, err := reachableCommitsFromReader(snap, dk)
	if err != nil {
		_ = snap.Close()
		return 0, err
	}

	prefix := dk.MetaCommitPrefix()
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

	var freedBytes uint64
	prunedIDs := make([]uint64, 0, len(unreachable))
	for _, cmt := range unreachable {
		prunedIDs = append(prunedIDs, cmt.ID)
		if delErr := batch.Delete(dk.MetaCommitKey(cmt.ID), nil); delErr != nil {
			return 0, delErr
		}
		if cmt.LayerID != 0 {
			start, end := dk.LayerRangeBounds(cmt.LayerID)
			if delErr := batch.DeleteRange(start, end, nil); delErr != nil {
				return 0, delErr
			}
		}
		freedBytes += cmt.OwnedBytes
	}
	if commitErr := d.commitBatchSync(ctx, batch); commitErr != nil {
		return 0, commitErr
	}
	d.invalidateCommitCache(prunedIDs...)
	d.releaseQuota(freedBytes)
	d.TriggerGC()
	return len(unreachable), nil
}
