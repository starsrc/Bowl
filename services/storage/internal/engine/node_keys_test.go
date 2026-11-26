package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNodeSplitKey_PrefixedLayerKeysAndNonLayerKeys(t *testing.T) {
	t.Parallel()

	diskID := uuid.MustParse("01936a1b-1234-7000-8000-000000000001")
	dk := newDiskKeys(diskID)

	prefix := dk.Prefix()
	require.Len(t, prefix, DiskPrefixLen)
	require.Equal(t, byte('d'), prefix[0])
	require.Equal(t, byte(':'), prefix[1])
	require.Equal(t, diskID[:], prefix[2:18])
	require.Equal(t, byte(':'), prefix[18])

	entryPrefix := dk.LayerEntryPrefix(42, 7)
	entryKey := dk.LayerEntryKey(42, 7, []byte("hello.txt"))
	require.Len(t, entryPrefix, DiskPrefixLen+LayerEntryPrefixLen)
	require.Equal(t, len(entryPrefix), NodeSplitKey(entryKey))
	require.Equal(t, entryPrefix, entryKey[:NodeSplitKey(entryKey)])
	require.Equal(t, len(entryPrefix), NodeSplitKey(entryPrefix))

	xattrPrefix := dk.LayerXattrPrefix(42, 7)
	xattrKey := dk.LayerXattrKey(42, 7, []byte("user.tag"))
	require.Len(t, xattrPrefix, DiskPrefixLen+LayerXattrPrefixLen)
	require.Equal(t, len(xattrPrefix), NodeSplitKey(xattrKey))
	require.Equal(t, xattrPrefix, xattrKey[:NodeSplitKey(xattrKey)])
	require.Equal(t, len(xattrPrefix), NodeSplitKey(xattrPrefix))

	dataPrefix := dk.LayerDataPrefix(42, 7)
	dataKey := dk.LayerDataKey(42, 7, 99)
	require.Len(t, dataPrefix, DiskPrefixLen+LayerDataPrefixLen)
	require.Equal(t, len(dataPrefix), NodeSplitKey(dataKey))
	require.Equal(t, dataPrefix, dataKey[:NodeSplitKey(dataKey)])
	require.Equal(t, len(dataPrefix), NodeSplitKey(dataPrefix))

	parentPrefix := dk.LayerParentPrefix(42, 7)
	parentKey := dk.LayerParentKey(42, 7, 13)
	require.Len(t, parentPrefix, DiskPrefixLen+LayerParentPrefixLen)
	require.Equal(t, len(parentPrefix), NodeSplitKey(parentKey))
	require.Equal(t, parentPrefix, parentKey[:NodeSplitKey(parentKey)])
	require.Equal(t, len(parentPrefix), NodeSplitKey(parentPrefix))

	inodeKey := dk.LayerInodeKey(42, 7)
	require.Equal(t, len(inodeKey), NodeSplitKey(inodeKey))

	orphanKey := dk.LayerOrphanKey(42, 7)
	require.Equal(t, len(orphanKey), NodeSplitKey(orphanKey))

	metaDiskKey := dk.MetaDiskKey()
	require.Equal(t, len(metaDiskKey), NodeSplitKey(metaDiskKey))

	catKey := CatalogIdxKey(time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC).UnixMicro(), diskID)
	require.Equal(t, len(catKey), NodeSplitKey(catKey))

	short := []byte("d:short")
	require.Equal(t, len(short), NodeSplitKey(short))
}

func TestDiskKeys_EncodeDecodeAndRangeIsolation(t *testing.T) {
	t.Parallel()

	idA := uuid.MustParse("01936a1b-1234-7000-8000-000000000001")
	idB := uuid.MustParse("01936a1b-1234-7000-8000-000000000002")
	dkA := newDiskKeys(idA)
	dkB := newDiskKeys(idB)

	cmtID, err := dkA.DecodeMetaCommitKey(dkA.MetaCommitKey(77))
	require.NoError(t, err)
	require.Equal(t, uint64(77), cmtID)
	_, err = dkB.DecodeMetaCommitKey(dkA.MetaCommitKey(77))
	require.ErrorIs(t, err, ErrInvalidKey)

	lid, ino, err := dkA.DecodeLayerInodeKey(dkA.LayerInodeKey(3, 9))
	require.NoError(t, err)
	require.Equal(t, uint64(3), lid)
	require.Equal(t, uint64(9), ino)

	lid, parent, name, err := dkA.DecodeLayerEntryKey(dkA.LayerEntryKey(4, 10, []byte("file.txt")))
	require.NoError(t, err)
	require.Equal(t, uint64(4), lid)
	require.Equal(t, uint64(10), parent)
	require.Equal(t, []byte("file.txt"), name)

	lid, ino, xname, err := dkA.DecodeLayerXattrKey(dkA.LayerXattrKey(5, 11, []byte("user.k")))
	require.NoError(t, err)
	require.Equal(t, uint64(5), lid)
	require.Equal(t, uint64(11), ino)
	require.Equal(t, []byte("user.k"), xname)

	lid, ino, err = dkA.DecodeLayerOrphanKey(dkA.LayerOrphanKey(6, 12))
	require.NoError(t, err)
	require.Equal(t, uint64(6), lid)
	require.Equal(t, uint64(12), ino)

	lid, ino, blk, err := dkA.DecodeLayerDataKey(dkA.LayerDataKey(7, 13, 14))
	require.NoError(t, err)
	require.Equal(t, uint64(7), lid)
	require.Equal(t, uint64(13), ino)
	require.Equal(t, uint64(14), blk)

	lid, ino, pdir, err := dkA.DecodeLayerParentKey(dkA.LayerParentKey(8, 15, 16))
	require.NoError(t, err)
	require.Equal(t, uint64(8), lid)
	require.Equal(t, uint64(15), ino)
	require.Equal(t, uint64(16), pdir)

	startA, endA := dkA.RangeBounds()
	keyA := dkA.MetaDiskKey()
	keyB := dkB.MetaDiskKey()
	require.GreaterOrEqual(t, bytes.Compare(keyA, startA), 0)
	require.Negative(t, bytes.Compare(keyA, endA))
	require.GreaterOrEqual(t, bytes.Compare(keyB, endA), 0)
}

func TestCatalogIdxKey_ReverseOrderAndRoundTrip(t *testing.T) {
	t.Parallel()

	idLow := uuid.MustParse("00000000-0000-7000-8000-000000000001")
	idHigh := uuid.MustParse("00000000-0000-7000-8000-0000000000ff")
	tNeg := time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC).UnixMicro()
	tOld := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	tNew := time.Date(2026, 11, 26, 12, 0, 0, 123456000, time.UTC).UnixMicro()

	kNewHigh := CatalogIdxKey(tNew, idHigh)
	kNewLow := CatalogIdxKey(tNew, idLow)
	kOldHigh := CatalogIdxKey(tOld, idHigh)
	kNeg := CatalogIdxKey(tNeg, idLow)

	require.Len(t, kNewHigh, CatalogIdxKeyLen)
	require.Negative(t, bytes.Compare(kNewHigh, kNewLow))
	require.Negative(t, bytes.Compare(kNewLow, kOldHigh))
	require.Negative(t, bytes.Compare(kOldHigh, kNeg))

	for _, tc := range []struct {
		us int64
		id uuid.UUID
	}{
		{tNew, idHigh},
		{tNew, idLow},
		{tOld, idHigh},
		{tNeg, idLow},
	} {
		gotUS, gotID, err := DecodeCatalogIdxKey(CatalogIdxKey(tc.us, tc.id))
		require.NoError(t, err)
		require.Equal(t, tc.us, gotUS)
		require.Equal(t, tc.id, gotID)
	}

	_, _, err := DecodeCatalogIdxKey([]byte("cat:idx:short"))
	require.ErrorIs(t, err, ErrInvalidKey)
}

func TestCursorPagination_FormatAndValidation(t *testing.T) {
	t.Parallel()

	sz, err := NormalizePageSize(0)
	require.NoError(t, err)
	require.Equal(t, DefaultPageSize, sz)

	sz, err = NormalizePageSize(100)
	require.NoError(t, err)
	require.Equal(t, uint32(100), sz)

	_, err = NormalizePageSize(MaxPageSize + 1)
	require.ErrorIs(t, err, ErrInvalid)

	ts := time.Date(2026, 11, 26, 12, 34, 56, 789000000, time.UTC)
	id := uuid.MustParse("01936a1b-1234-7000-8000-0000000000ab")
	tok := EncodeDiskPageCursor(ts, id)
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	require.NoError(t, err)
	var diskObj struct {
		T  int64  `json:"t"`
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(raw, &diskObj))
	require.Equal(t, ts.UnixMicro(), diskObj.T)
	require.Equal(t, id.String(), diskObj.ID)

	gotTS, gotID, err := DecodeDiskPageCursor(tok)
	require.NoError(t, err)
	require.Equal(t, ts, gotTS)
	require.Equal(t, id, gotID)

	for _, bad := range []string{"!!!", base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"not-a-uuid"}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"t":1}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"00000000-0000-0000-0000-000000000000"}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"01936a1b-1234-7000-8000-0000000000ab","extra":1}`))} {
		_, _, decErr := DecodeDiskPageCursor(bad)
		require.ErrorIs(t, decErr, ErrInvalid)
	}

	cTok := EncodeCommitPageCursor(42)
	cRaw, err := base64.RawURLEncoding.DecodeString(cTok)
	require.NoError(t, err)
	var cObj struct {
		CID uint64 `json:"cid"`
	}
	require.NoError(t, json.Unmarshal(cRaw, &cObj))
	require.Equal(t, uint64(42), cObj.CID)

	gotCID, err := DecodeCommitPageCursor(cTok)
	require.NoError(t, err)
	require.Equal(t, uint64(42), gotCID)

	for _, bad := range []string{"!!!", base64.RawURLEncoding.EncodeToString([]byte(`{"cid":0}`)), base64.RawURLEncoding.EncodeToString([]byte(`{}`)), base64.RawURLEncoding.EncodeToString([]byte(`{"cid":5,"x":1}`))} {
		_, decErr := DecodeCommitPageCursor(bad)
		require.ErrorIs(t, decErr, ErrInvalid)
	}
}
