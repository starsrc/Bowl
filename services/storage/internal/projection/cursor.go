package projection

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultPageSize uint32 = 50
	MaxPageSize     uint32 = 1000
)

type diskPageCursor struct {
	CreatedAtMicro *int64 `json:"t"`
	DiskID         string `json:"id"`
}

type commitPageCursor struct {
	CommitID *uint64 `json:"cid"`
}

func NormalizePageSize(pageSize uint32) (uint32, error) {
	if pageSize == 0 {
		return DefaultPageSize, nil
	}
	if pageSize > MaxPageSize {
		return 0, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, pageSize, MaxPageSize)
	}
	return pageSize, nil
}

func EncodeDiskPageCursor(createdAt time.Time, diskID uuid.UUID) string {
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

func DecodeDiskPageCursor(token string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token base64 encoding", ErrInvalidArgument)
	}
	var cursor diskPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cursor); err != nil || dec.More() || cursor.CreatedAtMicro == nil || cursor.DiskID == "" {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: malformed disk page_token payload", ErrInvalidArgument)
	}
	id, err := uuid.Parse(cursor.DiskID)
	if err != nil || id == uuid.Nil {
		return time.Time{}, uuid.Nil, fmt.Errorf("%w: invalid page_token disk_id", ErrInvalidArgument)
	}
	return time.UnixMicro(*cursor.CreatedAtMicro).UTC(), id, nil
}

func EncodeCommitPageCursor(commitID uint64) string {
	raw, err := json.Marshal(commitPageCursor{
		CommitID: &commitID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func DecodeCommitPageCursor(token string) (uint64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid commit page_token base64 encoding", ErrInvalidArgument)
	}
	var cursor commitPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cursor); err != nil || dec.More() || cursor.CommitID == nil || *cursor.CommitID == 0 {
		return 0, fmt.Errorf("%w: malformed commit page_token payload", ErrInvalidArgument)
	}
	return *cursor.CommitID, nil
}
