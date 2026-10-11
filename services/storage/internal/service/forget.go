package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	connect "connectrpc.com/connect/v2"
	"github.com/jackc/pgx/v5"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// Forget releases lookup references for an Inode (FUSE_FORGET).
// When lookup_count reaches 0 and nlink == 0, the Inode is garbage collected and reclaimed.
func (s *StorageService) Forget(ctx context.Context, req *bowlv1.ForgetRequest) (*bowlv1.ForgetResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	if err := s.forgetInode(ctx, sess.DiskID, schemaName, req.GetIno(), req.GetNlookup()); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to forget inode: %s", err)
	}

	return &bowlv1.ForgetResponse{}, nil
}

// BatchForget releases lookup references for multiple Inodes in a batch (FUSE_BATCH_FORGET).
func (s *StorageService) BatchForget(ctx context.Context, req *bowlv1.BatchForgetRequest) (*bowlv1.BatchForgetResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	for _, node := range req.GetNodes() {
		if node == nil || node.GetIno() == 0 {
			continue
		}
		if err := s.forgetInode(ctx, sess.DiskID, schemaName, node.GetIno(), node.GetNlookup()); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to batch forget inode %d: %s", node.GetIno(), err)
		}
	}

	return &bowlv1.BatchForgetResponse{}, nil
}

// forgetInode decrements lookup_count by nlookup (floored at 0) and reclaims the inode if lookup_count == 0 and nlink == 0.
func (s *StorageService) forgetInode(ctx context.Context, diskID, schemaName string, ino uint64, nlookup uint64) error {
	if s.db == nil || ino == 0 {
		return nil
	}

	updateSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET lookup_count = GREATEST(0, lookup_count - $1::bigint)
WHERE ino = $2
RETURNING lookup_count, nlink;
`, schemaName)

	var newLookupCount int64
	var nlink int
	err := s.db.QueryRow(ctx, updateSQL, int64(nlookup), int64(ino)).Scan(&newLookupCount, &nlink)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}

	if newLookupCount == 0 && nlink == 0 {
		deleteInoSQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE ino = $1;`, schemaName)
		if _, err := s.db.Exec(ctx, deleteInoSQL, int64(ino)); err != nil {
			return err
		}
		deleteXAttrSQL := fmt.Sprintf(`DELETE FROM "%s".xattrs WHERE ino = $1;`, schemaName)
		_, _ = s.db.Exec(ctx, deleteXAttrSQL, int64(ino))
		if s.driver != nil && diskID != "" {
			_ = s.driver.DeleteInode(ctx, diskID, ino)
		}
	}

	return nil
}

// GetLookupCount returns the current lookup count for a given inode in a session's disk.
func (s *StorageService) GetLookupCount(ctx context.Context, sessionID string, ino uint64) (uint64, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return 0, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if ino == 0 {
		return 0, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	_, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return 0, err
	}

	if s.db == nil {
		return 0, errors.New("database not initialized")
	}

	query := fmt.Sprintf(`SELECT lookup_count FROM "%s".inodes WHERE ino = $1;`, schemaName)
	var count int64
	if err := s.db.QueryRow(ctx, query, int64(ino)).Scan(&count); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, connect.Errorf(connect.CodeNotFound, "inode not found: %d", ino)
		}
		return 0, connect.Errorf(connect.CodeInternal, "failed to query lookup count: %s", err)
	}
	return uint64(count), nil
}
