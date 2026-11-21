package db

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	defaultListDisksPageSize     uint32 = 50
	defaultListSnapshotsPageSize uint32 = 50
	maxListSnapshotsPageSize     uint32 = 1000
)

type diskPageCursor struct {
	CreatedAtMicro *int64 `json:"t"`
	DiskID         string `json:"id"`
}

type snapshotPageCursor struct {
	CreatedAtMicro *int64 `json:"t"`
	SnapshotID     string `json:"sid"`
}

func parseNonNilUUID(raw string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: invalid uuid %q", ErrInvalidArgument, raw)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: uuid cannot be nil", ErrInvalidArgument)
	}
	return id, nil
}

func normalizePageSize(pageSize uint32) uint32 {
	if pageSize == 0 {
		return defaultListDisksPageSize
	}
	return pageSize
}

func normalizeSnapshotPageSize(pageSize uint32) (uint32, error) {
	if pageSize == 0 {
		return defaultListSnapshotsPageSize, nil
	}
	if pageSize > maxListSnapshotsPageSize {
		return 0, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, pageSize, maxListSnapshotsPageSize)
	}
	return pageSize, nil
}

func encodeDiskPageCursor(createdAt time.Time, diskID uuid.UUID) string {
	us := createdAt.UTC().UnixMicro()
	raw, err := json.Marshal(diskPageCursor{
		CreatedAtMicro: &us,
		DiskID:         diskID.String(),
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDiskPageCursor(token string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token base64 encoding", ErrInvalidArgument)
	}
	var cursor diskPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cursor); err != nil || dec.More() || cursor.CreatedAtMicro == nil || cursor.DiskID == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: malformed page_token payload", ErrInvalidArgument)
	}
	id, err := parseNonNilUUID(cursor.DiskID)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token disk_id", ErrInvalidArgument)
	}
	return time.UnixMicro(*cursor.CreatedAtMicro).UTC(), id, nil
}

func encodeSnapshotPageCursor(createdAt time.Time, snapshotID uuid.UUID) string {
	us := createdAt.UTC().UnixMicro()
	raw, err := json.Marshal(snapshotPageCursor{
		CreatedAtMicro: &us,
		SnapshotID:     snapshotID.String(),
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeSnapshotPageCursor(token string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token base64 encoding", ErrInvalidArgument)
	}
	var cursor snapshotPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cursor); err != nil || dec.More() || cursor.CreatedAtMicro == nil || cursor.SnapshotID == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: malformed page_token payload", ErrInvalidArgument)
	}
	id, err := parseNonNilUUID(cursor.SnapshotID)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token snapshot_id", ErrInvalidArgument)
	}
	return time.UnixMicro(*cursor.CreatedAtMicro).UTC(), id, nil
}
