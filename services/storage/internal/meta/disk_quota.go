package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type diskQuotaState struct {
	rootIno       int64
	diskID        pgtype.UUID
	maxBytes      int64
	usedBytes     int64
	usedInodes    int64
	reservedBytes int64
	pendingBytes  int64
	pendingInodes int64
	loaded        bool
}

func (s *diskQuotaState) effectiveUsedBytesWithReserved() int64 {
	v := s.usedBytes + s.reservedBytes
	if v < 0 {
		return 0
	}
	return v
}

func isNativeTxQuerier(q db.Querier) bool {
	r, ok := q.(interface{ RawDBTX() db.DBTX })
	return ok && r != nil && r.RawDBTX() != nil
}

func (m *manager) hasPool() bool {
	if m == nil {
		return false
	}
	pp, ok := m.store.(poolProvider)
	return ok && pp != nil && pp.Pool() != nil
}

func (m *manager) getOrCreateDiskQuotaStateLocked(rootIno int64) *diskQuotaState {
	if m.diskQuotas == nil {
		m.diskQuotas = make(map[int64]*diskQuotaState)
	}
	st, ok := m.diskQuotas[rootIno]
	if !ok {
		st = &diskQuotaState{rootIno: rootIno}
		m.diskQuotas[rootIno] = st
	}
	return st
}

func (m *manager) initDiskQuotaOnCreate(row db.GetDiskRow) {
	if row.RootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(row.RootIno)
	st.diskID = row.DiskID
	st.maxBytes = row.MaxBytes
	st.usedBytes = max(int64(0), row.UsedBytes)
	st.usedInodes = max(int64(0), row.UsedInodes)
	st.reservedBytes = 0
	st.pendingBytes = 0
	st.pendingInodes = 0
	st.loaded = true
	m.quotaMu.Unlock()
}

func (m *manager) updateDiskQuotaOnUpdate(row db.GetDiskRow) {
	if row.RootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(row.RootIno)
	st.diskID = row.DiskID
	st.maxBytes = row.MaxBytes
	if !st.loaded {
		st.usedBytes = max(int64(0), row.UsedBytes+st.pendingBytes)
		st.usedInodes = max(int64(0), row.UsedInodes+st.pendingInodes)
		st.pendingBytes = 0
		st.pendingInodes = 0
		st.loaded = true
	}
	m.quotaMu.Unlock()
}

func (m *manager) deleteDiskQuotaState(rootIno int64) {
	if rootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	delete(m.diskQuotas, rootIno)
	m.quotaMu.Unlock()
}

func (m *manager) evictDiskQuota(rootIno int64) {
	m.deleteDiskQuotaState(rootIno)
}

func (m *manager) ensureDiskQuotaLoaded(ctx context.Context, rootIno int64) (*diskQuotaState, bool, error) {
	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[rootIno]; ok && st.loaded {
		m.quotaMu.Unlock()
		return st, true, nil
	}
	m.quotaMu.Unlock()

	row, err := m.store.GetDiskByRootIno(ctx, rootIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get disk by root_ino %d: %w", rootIno, err)
	}

	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(rootIno)
	if !st.loaded {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.usedBytes = max(int64(0), row.UsedBytes+st.pendingBytes)
		st.usedInodes = max(int64(0), row.UsedInodes+st.pendingInodes)
		st.pendingBytes = 0
		st.pendingInodes = 0
		st.loaded = true
	}
	m.quotaMu.Unlock()
	return st, true, nil
}

func (m *manager) ensureDiskQuotaLoadedInTx(ctx context.Context, q db.Querier, rootIno int64) (*diskQuotaState, bool, error) {
	if m.hasPool() {
		return m.ensureDiskQuotaLoaded(ctx, rootIno)
	}

	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[rootIno]; ok && st.loaded {
		m.quotaMu.Unlock()
		return st, true, nil
	}
	m.quotaMu.Unlock()

	row, err := q.GetDiskByRootInoForUpdate(ctx, rootIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get disk by root_ino %d for update: %w", rootIno, err)
	}

	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(rootIno)
	if !st.loaded {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.usedBytes = max(int64(0), row.UsedBytes+st.pendingBytes)
		st.usedInodes = max(int64(0), row.UsedInodes+st.pendingInodes)
		st.pendingBytes = 0
		st.pendingInodes = 0
		st.loaded = true
	}
	m.quotaMu.Unlock()
	return st, true, nil
}

func (m *manager) syncDiskRowWithQuotaState(row *db.GetDiskRow) {
	if row == nil || row.RootIno <= 0 {
		return
	}

	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(row.RootIno)
	if !st.loaded {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.usedBytes = max(int64(0), row.UsedBytes+st.pendingBytes)
		st.usedInodes = max(int64(0), row.UsedInodes+st.pendingInodes)
		st.pendingBytes = 0
		st.pendingInodes = 0
		st.loaded = true
		row.UsedBytes = st.usedBytes
		row.UsedInodes = st.usedInodes
		m.quotaMu.Unlock()
		return
	}

	st.diskID = row.DiskID
	row.MaxBytes = st.maxBytes
	row.UsedBytes = max(int64(0), st.usedBytes)
	row.UsedInodes = max(int64(0), st.usedInodes)
	m.quotaMu.Unlock()
}

func (m *manager) currentDiskUsedBytes(rootIno int64, dbUsedBytes int64) uint64 {
	used := max(int64(0), dbUsedBytes)
	if rootIno <= 0 {
		return uint64(used)
	}
	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[rootIno]; ok && st.loaded {
		memUsed := st.effectiveUsedBytesWithReserved()
		if memUsed > used {
			used = memUsed
		}
	}
	m.quotaMu.Unlock()
	return uint64(used)
}

type quotaBufferedQuerier struct {
	db.Querier
	m                 *manager
	txReservedBytes   map[int64]int64
	txUnreservedBytes map[int64]int64
	txInodes          map[int64]int64
}

func (q *quotaBufferedQuerier) RawDBTX() db.DBTX {
	if r, ok := q.Querier.(interface{ RawDBTX() db.DBTX }); ok && r != nil {
		return r.RawDBTX()
	}
	return nil
}

func quotaTxFromQuerier(q db.Querier) *quotaBufferedQuerier {
	if bq, ok := q.(*quotaBufferedQuerier); ok {
		return bq
	}
	return nil
}

func (m *manager) reserveQuotaBytes(ctx context.Context, q db.Querier, rootIno int64, deltaBytes int64) error {
	if rootIno <= 0 || deltaBytes <= 0 {
		return nil
	}
	_, found, err := m.ensureDiskQuotaLoadedInTx(ctx, q, rootIno)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	bq := quotaTxFromQuerier(q)
	m.quotaMu.Lock()
	st := m.diskQuotas[rootIno]
	var unreserved int64
	if bq != nil {
		unreserved = bq.txUnreservedBytes[rootIno]
	}
	usedBytes := st.effectiveUsedBytesWithReserved() + unreserved
	if usedBytes < 0 {
		usedBytes = 0
	}
	if uint64(usedBytes) > uint64(st.maxBytes) || uint64(deltaBytes) > uint64(st.maxBytes)-uint64(usedBytes) {
		m.quotaMu.Unlock()
		return ErrNoSpace
	}
	st.reservedBytes += deltaBytes
	if bq != nil {
		if bq.txReservedBytes == nil {
			bq.txReservedBytes = make(map[int64]int64)
		}
		bq.txReservedBytes[rootIno] += deltaBytes
	}
	m.quotaMu.Unlock()
	return nil
}

func (m *manager) recordQuotaDelta(ctx context.Context, q db.Querier, rootIno int64, deltaBytes int64, deltaInodes int64) error {
	if rootIno <= 0 || (deltaBytes == 0 && deltaInodes == 0) {
		return nil
	}
	if m.hasPool() {
		if _, _, err := m.ensureDiskQuotaLoaded(ctx, rootIno); err != nil {
			return err
		}
	}
	if deltaBytes > 0 {
		if err := m.reserveQuotaBytes(ctx, q, rootIno, deltaBytes); err != nil {
			return err
		}
	} else if deltaBytes < 0 {
		if bq := quotaTxFromQuerier(q); bq != nil {
			if bq.txUnreservedBytes == nil {
				bq.txUnreservedBytes = make(map[int64]int64)
			}
			bq.txUnreservedBytes[rootIno] += deltaBytes
		}
	}
	if deltaInodes != 0 {
		if bq := quotaTxFromQuerier(q); bq != nil {
			if bq.txInodes == nil {
				bq.txInodes = make(map[int64]int64)
			}
			bq.txInodes[rootIno] += deltaInodes
		}
	}
	return nil
}

func (q *quotaBufferedQuerier) rollback() {
	if len(q.txReservedBytes) > 0 {
		q.m.quotaMu.Lock()
		for rootIno, res := range q.txReservedBytes {
			if st, ok := q.m.diskQuotas[rootIno]; ok {
				st.reservedBytes -= res
			}
		}
		q.m.quotaMu.Unlock()
	}
	clear(q.txReservedBytes)
	clear(q.txUnreservedBytes)
	clear(q.txInodes)
}

func (q *quotaBufferedQuerier) commit() {
	if len(q.txReservedBytes) == 0 && len(q.txUnreservedBytes) == 0 && len(q.txInodes) == 0 {
		return
	}

	q.m.quotaMu.Lock()
	seen := make(map[int64]struct{}, len(q.txReservedBytes)+len(q.txUnreservedBytes)+len(q.txInodes))
	for r := range q.txReservedBytes {
		seen[r] = struct{}{}
	}
	for r := range q.txUnreservedBytes {
		seen[r] = struct{}{}
	}
	for r := range q.txInodes {
		seen[r] = struct{}{}
	}

	for rootIno := range seen {
		st := q.m.getOrCreateDiskQuotaStateLocked(rootIno)
		res := q.txReservedBytes[rootIno]
		unres := q.txUnreservedBytes[rootIno]
		inos := q.txInodes[rootIno]
		st.reservedBytes -= res
		if st.loaded {
			st.usedBytes = max(int64(0), st.usedBytes+res+unres)
			st.usedInodes = max(int64(0), st.usedInodes+inos)
		} else {
			st.pendingBytes += res + unres
			st.pendingInodes += inos
		}
	}
	q.m.quotaMu.Unlock()

	clear(q.txReservedBytes)
	clear(q.txUnreservedBytes)
	clear(q.txInodes)
}
