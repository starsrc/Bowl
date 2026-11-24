package engine

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
)

type mountBranchReq struct {
	readOnly bool
	force    bool
	ttl      time.Duration
}

type renewMountReq struct {
	mountID  string
	readOnly bool
	epoch    uint64
	ttl      time.Duration
}

type evictMountReq struct {
	mountID        string
	explicitBranch bool
}

func validateMountID(id string) error {
	if id == "" || strings.ContainsRune(id, '\x00') {
		return ErrInvalid
	}
	return nil
}

func readMountMeta(r pebbleReader, mountID string) (MountMeta, error) {
	if err := validateMountID(mountID); err != nil {
		return MountMeta{}, err
	}
	val, closer, err := r.Get(MetaMountKey(mountID))
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

func listAllMountsFromReader(r pebbleReader, branch string) ([]MountMeta, error) {
	prefix := MetaMountPrefix()
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	var out []MountMeta
	for iter.First(); iter.Valid(); iter.Next() {
		meta, decErr := DecodeMountMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			return nil, decErr
		}
		if branch != "" && meta.BranchName != branch {
			continue
		}
		out = append(out, meta)
	}
	if closeErr := iter.Close(); closeErr != nil {
		return nil, closeErr
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func listActiveMountsFromReader(r pebbleReader, branch string, nowNano int64) ([]MountMeta, error) {
	all, err := listAllMountsFromReader(r, branch)
	if err != nil {
		return nil, err
	}
	var active []MountMeta
	for _, m := range all {
		if m.ExpiresAt > nowNano {
			active = append(active, m)
		}
	}
	return active, nil
}

func hasActiveMountOnReader(r pebbleReader, branch string, nowNano int64, rwOnly bool) (bool, error) {
	prefix := MetaMountPrefix()
	iter, err := r.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: PrefixUpperBound(prefix),
	})
	if err != nil {
		return false, err
	}
	found := false
	for iter.First(); iter.Valid(); iter.Next() {
		meta, decErr := DecodeMountMeta(iter.Value())
		if decErr != nil {
			_ = iter.Close()
			return false, decErr
		}
		if meta.ExpiresAt <= nowNano {
			continue
		}
		if branch != "" && meta.BranchName != branch {
			continue
		}
		if rwOnly && meta.ReadOnly {
			continue
		}
		found = true
		break
	}
	if closeErr := iter.Close(); closeErr != nil {
		return false, closeErr
	}
	return found, nil
}

func (d *DiskStore) MountBranch(ctx context.Context, branch string, readOnly, force bool, ttl time.Duration) (MountMeta, error) {
	if d.closing.Load() {
		return MountMeta{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBranchName(branch); err != nil {
		return MountMeta{}, err
	}
	if ttl <= 0 {
		return MountMeta{}, ErrInvalid
	}

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return MountMeta{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpMount, &mountBranchReq{
		readOnly: readOnly,
		force:    force,
		ttl:      ttl,
	})
	if err != nil {
		return MountMeta{}, err
	}
	mount, ok := resp.(MountMeta)
	if !ok {
		return MountMeta{}, ErrInvalidValue
	}
	return mount, nil
}

func (a *BranchActor) execMountBranch(batch *pebble.Batch, branch BranchMeta, raw any) (MountMeta, error) {
	req, ok := raw.(*mountBranchReq)
	if !ok {
		return MountMeta{}, ErrInvalid
	}

	now := a.disk.now()
	nowNano := now.UnixNano()
	existing, err := listAllMountsFromReader(batch, branch.Name)
	if err != nil {
		return MountMeta{}, err
	}

	var expiredIDs []string
	var activeRWIDs []string
	for _, m := range existing {
		if m.ExpiresAt <= nowNano {
			expiredIDs = append(expiredIDs, m.ID)
			continue
		}
		if !m.ReadOnly {
			activeRWIDs = append(activeRWIDs, m.ID)
		}
	}

	if !req.readOnly && len(activeRWIDs) > 0 && !req.force {
		return MountMeta{}, ErrDiskMounted
	}

	for _, id := range expiredIDs {
		if delErr := batch.Delete(MetaMountKey(id), nil); delErr != nil {
			return MountMeta{}, delErr
		}
	}

	var epoch uint64
	if !req.readOnly {
		for _, id := range activeRWIDs {
			if delErr := batch.Delete(MetaMountKey(id), nil); delErr != nil {
				return MountMeta{}, delErr
			}
		}
		if sweepErr := sweepOrphansInWorkLayer(batch, branch.WorkLayerID, branch.HeadCommitID); sweepErr != nil {
			return MountMeta{}, sweepErr
		}
		branch.WriterEpoch++
		branch.UpdatedAt = nowNano
		if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
			return MountMeta{}, setErr
		}
		epoch = branch.WriterEpoch
	}

	mount := MountMeta{
		ID:          uuid.NewString(),
		BranchName:  branch.Name,
		ReadOnly:    req.readOnly,
		WriterEpoch: epoch,
		MountedAt:   nowNano,
		ExpiresAt:   now.Add(req.ttl).UnixNano(),
	}
	if setErr := batch.Set(MetaMountKey(mount.ID), EncodeMountMeta(mount), nil); setErr != nil {
		return MountMeta{}, setErr
	}
	return mount, nil
}

func (d *DiskStore) RenewMount(ctx context.Context, branch, mountID string, readOnly bool, epoch uint64, ttl time.Duration) (MountMeta, error) {
	if d.closing.Load() {
		return MountMeta{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateBranchName(branch); err != nil {
		return MountMeta{}, err
	}
	if err := validateMountID(mountID); err != nil {
		return MountMeta{}, err
	}
	if ttl <= 0 {
		return MountMeta{}, ErrInvalid
	}
	if readOnly && epoch != 0 {
		return MountMeta{}, ErrInvalid
	}
	if !readOnly && epoch == 0 {
		return MountMeta{}, ErrInvalid
	}

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return MountMeta{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpRenewMount, &renewMountReq{
		mountID:  mountID,
		readOnly: readOnly,
		epoch:    epoch,
		ttl:      ttl,
	})
	if err != nil {
		return MountMeta{}, err
	}
	mount, ok := resp.(MountMeta)
	if !ok {
		return MountMeta{}, ErrInvalidValue
	}
	return mount, nil
}

func (a *BranchActor) execRenewMount(batch *pebble.Batch, branch BranchMeta, raw any) (MountMeta, error) {
	req, ok := raw.(*renewMountReq)
	if !ok {
		return MountMeta{}, ErrInvalid
	}
	if !req.readOnly && req.epoch != branch.WriterEpoch {
		return MountMeta{}, ErrStaleEpoch
	}
	mount, err := readMountMeta(batch, req.mountID)
	if err != nil {
		return MountMeta{}, err
	}
	if mount.BranchName != branch.Name || mount.ReadOnly != req.readOnly {
		return MountMeta{}, ErrNotFound
	}
	if !req.readOnly && mount.WriterEpoch != req.epoch {
		return MountMeta{}, ErrStaleEpoch
	}
	now := a.disk.now()
	if mount.ExpiresAt <= now.UnixNano() {
		return MountMeta{}, ErrNotFound
	}
	mount.ExpiresAt = now.Add(req.ttl).UnixNano()
	if setErr := batch.Set(MetaMountKey(mount.ID), EncodeMountMeta(mount), nil); setErr != nil {
		return MountMeta{}, setErr
	}
	return mount, nil
}

func (d *DiskStore) GetMount(mountID string) (MountMeta, error) {
	if d.closing.Load() {
		return MountMeta{}, ErrClosed
	}
	return readMountMeta(d.db, mountID)
}

func (d *DiskStore) Unmount(ctx context.Context, mountID string) error {
	if d.closing.Load() {
		return ErrClosed
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := validateMountID(mountID); err != nil {
		return err
	}
	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()
	return d.deleteKeySync(ctx, MetaMountKey(mountID))
}

func (d *DiskStore) EvictMounts(ctx context.Context, branch, mountID string) (EvictMountsResult, error) {
	if d.closing.Load() {
		return EvictMountsResult{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if branch != "" {
		if err := validateBranchName(branch); err != nil {
			return EvictMountsResult{}, err
		}
	}
	if mountID != "" {
		if err := validateMountID(mountID); err != nil {
			return EvictMountsResult{}, err
		}
	}

	if branch != "" {
		d.branchRegMu.RLock()
		defer d.branchRegMu.RUnlock()
		return d.evictMountsOnBranch(ctx, branch, mountID, true)
	}

	if mountID != "" {
		d.branchRegMu.RLock()
		defer d.branchRegMu.RUnlock()

		mount, err := readMountMeta(d.db, mountID)
		if errors.Is(err, ErrNotFound) {
			return EvictMountsResult{}, nil
		}
		if err != nil {
			return EvictMountsResult{}, err
		}
		res, evictErr := d.evictMountsOnBranch(ctx, mount.BranchName, mountID, false)
		if errors.Is(evictErr, ErrNotFound) {
			if delErr := d.deleteKeySync(ctx, MetaMountKey(mountID)); delErr != nil {
				return EvictMountsResult{}, delErr
			}
			var count uint32
			if mount.ExpiresAt > d.now().UnixNano() {
				count = 1
			}
			return EvictMountsResult{
				EvictedCount:    count,
				EvictedMountIDs: []string{mountID},
			}, nil
		}
		return res, evictErr
	}

	d.branchRegMu.Lock()
	defer d.branchRegMu.Unlock()

	allMounts, err := listAllMountsFromReader(d.db, "")
	if err != nil {
		return EvictMountsResult{}, err
	}
	if len(allMounts) == 0 {
		return EvictMountsResult{}, nil
	}

	byBranch := make(map[string][]MountMeta)
	for _, m := range allMounts {
		byBranch[m.BranchName] = append(byBranch[m.BranchName], m)
	}
	branchNames := make([]string, 0, len(byBranch))
	for bName := range byBranch {
		branchNames = append(branchNames, bName)
	}
	sort.Strings(branchNames)

	nowNano := d.now().UnixNano()
	var total EvictMountsResult
	for _, bName := range branchNames {
		part, evictErr := d.evictMountsOnBranch(ctx, bName, "", false)
		if errors.Is(evictErr, ErrNotFound) {
			orphanBatch := d.db.NewBatch()
			for _, m := range byBranch[bName] {
				if delErr := orphanBatch.Delete(MetaMountKey(m.ID), nil); delErr != nil {
					_ = orphanBatch.Close()
					return EvictMountsResult{}, delErr
				}
				total.EvictedMountIDs = append(total.EvictedMountIDs, m.ID)
				if m.ExpiresAt > nowNano {
					total.EvictedCount++
				}
			}
			commitErr := d.commitBatchSync(ctx, orphanBatch)
			_ = orphanBatch.Close()
			if commitErr != nil {
				return EvictMountsResult{}, commitErr
			}
			continue
		}
		if evictErr != nil {
			return EvictMountsResult{}, evictErr
		}
		total.EvictedCount += part.EvictedCount
		total.EvictedMountIDs = append(total.EvictedMountIDs, part.EvictedMountIDs...)
		total.UpdatedBranches = append(total.UpdatedBranches, part.UpdatedBranches...)
	}

	sort.Strings(total.EvictedMountIDs)
	sort.Slice(total.UpdatedBranches, func(i, j int) bool {
		return total.UpdatedBranches[i].Name < total.UpdatedBranches[j].Name
	})
	return total, nil
}

func (d *DiskStore) evictMountsOnBranch(ctx context.Context, branch, mountID string, explicitBranch bool) (EvictMountsResult, error) {
	actor, release, err := d.acquireBranchActor(branch)
	if err != nil {
		return EvictMountsResult{}, err
	}
	defer release()

	resp, err := actor.submit(ctx, branchOpEvictMount, &evictMountReq{
		mountID:        mountID,
		explicitBranch: explicitBranch,
	})
	if err != nil {
		return EvictMountsResult{}, err
	}
	res, ok := resp.(EvictMountsResult)
	if !ok {
		return EvictMountsResult{}, ErrInvalidValue
	}
	return res, nil
}

func (a *BranchActor) execEvictMount(batch *pebble.Batch, branch BranchMeta, raw any) (EvictMountsResult, error) {
	req, ok := raw.(*evictMountReq)
	if !ok {
		return EvictMountsResult{}, ErrInvalid
	}

	nowNano := a.disk.now().UnixNano()
	if req.mountID != "" {
		mount, err := readMountMeta(batch, req.mountID)
		if errors.Is(err, ErrNotFound) {
			return EvictMountsResult{}, nil
		}
		if err != nil {
			return EvictMountsResult{}, err
		}
		if mount.BranchName != branch.Name {
			return EvictMountsResult{}, ErrNotFound
		}
		if delErr := batch.Delete(MetaMountKey(mount.ID), nil); delErr != nil {
			return EvictMountsResult{}, delErr
		}
		res := EvictMountsResult{
			EvictedMountIDs: []string{mount.ID},
		}
		if mount.ExpiresAt > nowNano {
			res.EvictedCount = 1
		}
		if !mount.ReadOnly {
			branch.WriterEpoch++
			branch.UpdatedAt = nowNano
			if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
				return EvictMountsResult{}, setErr
			}
			res.UpdatedBranches = []BranchMeta{branch}
		}
		return res, nil
	}

	mounts, err := listAllMountsFromReader(batch, branch.Name)
	if err != nil {
		return EvictMountsResult{}, err
	}
	if len(mounts) == 0 {
		return EvictMountsResult{}, nil
	}

	var res EvictMountsResult
	hadActiveRW := false
	hadAnyRW := false
	for _, m := range mounts {
		if delErr := batch.Delete(MetaMountKey(m.ID), nil); delErr != nil {
			return EvictMountsResult{}, delErr
		}
		res.EvictedMountIDs = append(res.EvictedMountIDs, m.ID)
		if m.ExpiresAt > nowNano {
			res.EvictedCount++
			if !m.ReadOnly {
				hadActiveRW = true
			}
		}
		if !m.ReadOnly {
			hadAnyRW = true
		}
	}
	if hadActiveRW || (req.explicitBranch && hadAnyRW) {
		branch.WriterEpoch++
		branch.UpdatedAt = nowNano
		if setErr := batch.Set(MetaBranchKey(branch.Name), EncodeBranchMeta(branch), nil); setErr != nil {
			return EvictMountsResult{}, setErr
		}
		res.UpdatedBranches = []BranchMeta{branch}
	}
	sort.Strings(res.EvictedMountIDs)
	return res, nil
}

func (d *DiskStore) ListMounts(branch string, now time.Time) ([]MountMeta, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if branch != "" {
		if err := validateBranchName(branch); err != nil {
			return nil, err
		}
	}
	if now.IsZero() {
		now = d.now()
	}
	return listActiveMountsFromReader(d.db, branch, now.UnixNano())
}

func (d *DiskStore) HasActiveMounts(branch string, now time.Time) (bool, error) {
	if d.closing.Load() {
		return false, ErrClosed
	}
	if branch != "" {
		if err := validateBranchName(branch); err != nil {
			return false, err
		}
	}
	if now.IsZero() {
		now = d.now()
	}
	return hasActiveMountOnReader(d.db, branch, now.UnixNano(), false)
}

func (d *DiskStore) PruneExpiredMounts(now time.Time) ([]string, error) {
	if d.closing.Load() {
		return nil, ErrClosed
	}
	if now.IsZero() {
		now = d.now()
	}

	d.branchRegMu.RLock()
	defer d.branchRegMu.RUnlock()

	all, err := listAllMountsFromReader(d.db, "")
	if err != nil {
		return nil, err
	}
	nowNano := now.UnixNano()
	var expired []string
	for _, m := range all {
		if m.ExpiresAt <= nowNano {
			expired = append(expired, m.ID)
		}
	}
	if len(expired) == 0 {
		return nil, nil
	}

	batch := d.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, id := range expired {
		if delErr := batch.Delete(MetaMountKey(id), nil); delErr != nil {
			return nil, delErr
		}
	}
	if commitErr := d.commitBatchSync(context.Background(), batch); commitErr != nil {
		return nil, commitErr
	}
	sort.Strings(expired)
	return expired, nil
}
