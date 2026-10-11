package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/jackc/pgx/v5"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// EnsureXAttrsTable creates the xattrs table in the given disk schema if it does not already exist.
func (s *StorageService) EnsureXAttrsTable(ctx context.Context, schemaName string) error {
	if s.db == nil {
		return errors.New("database not initialized")
	}
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s".xattrs (ino BIGINT NOT NULL, name TEXT NOT NULL, value BYTEA NOT NULL, PRIMARY KEY(ino, name));`, schemaName)
	_, err := s.db.Exec(ctx, ddl)
	return err
}

// SetXAttr sets an extended attribute on an Inode (FUSE_SETATTR / setxattr).
func (s *StorageService) SetXAttr(ctx context.Context, req *bowlv1.SetXAttrRequest) (*bowlv1.SetXAttrResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "name is required")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	if sess.ReadOnly || !sess.IsWriteLease || sess.Revoked {
		return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only or lease expired")
	}
	if !sess.LeaseExpiresAt.IsZero() && time.Now().After(sess.LeaseExpiresAt) {
		return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: write lease expired")
	}

	if _, err := s.getInode(ctx, schemaName, req.GetIno()); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	if s.db == nil {
		return nil, connect.NewError(connect.CodeInternal, "database not initialized")
	}

	val := req.GetValue()
	if val == nil {
		val = []byte{}
	}

	flags := req.GetFlags()
	switch flags {
	case bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_CREATE:
		query := fmt.Sprintf(`
INSERT INTO "%s".xattrs (ino, name, value)
VALUES ($1, $2, $3)
ON CONFLICT (ino, name) DO NOTHING;
`, schemaName)
		tag, err := s.db.Exec(ctx, query, int64(req.GetIno()), name, val)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to insert xattr: %s", err)
		}
		if tag.RowsAffected() == 0 {
			return nil, connect.Errorf(connect.CodeAlreadyExists, "attribute '%s' already exists for inode %d", name, req.GetIno())
		}
	case bowlv1.XAttrSetFlag_X_ATTR_SET_FLAG_REPLACE:
		query := fmt.Sprintf(`
UPDATE "%s".xattrs
SET value = $1
WHERE ino = $2 AND name = $3;
`, schemaName)
		tag, err := s.db.Exec(ctx, query, val, int64(req.GetIno()), name)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to update xattr: %s", err)
		}
		if tag.RowsAffected() == 0 {
			return nil, connect.Errorf(connect.CodeNotFound, "attribute '%s' not found for inode %d", name, req.GetIno())
		}
	default:
		query := fmt.Sprintf(`
INSERT INTO "%s".xattrs (ino, name, value)
VALUES ($1, $2, $3)
ON CONFLICT (ino, name) DO UPDATE SET value = EXCLUDED.value;
`, schemaName)
		if _, err := s.db.Exec(ctx, query, int64(req.GetIno()), name, val); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to upsert xattr: %s", err)
		}
	}

	return &bowlv1.SetXAttrResponse{}, nil
}

// GetXAttr retrieves an extended attribute value by Inode and name (FUSE_GETXATTR / getxattr).
func (s *StorageService) GetXAttr(ctx context.Context, req *bowlv1.GetXAttrRequest) (*bowlv1.GetXAttrResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "name is required")
	}

	_, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	if _, err := s.getInode(ctx, schemaName, req.GetIno()); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	if s.db == nil {
		return nil, connect.NewError(connect.CodeInternal, "database not initialized")
	}

	query := fmt.Sprintf(`SELECT value FROM "%s".xattrs WHERE ino = $1 AND name = $2;`, schemaName)
	var val []byte
	if err := s.db.QueryRow(ctx, query, int64(req.GetIno()), name).Scan(&val); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "attribute '%s' not found for inode %d", name, req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to get xattr: %s", err)
	}

	return &bowlv1.GetXAttrResponse{
		Value: val,
	}, nil
}

// ListXAttr lists all extended attribute names associated with an Inode (FUSE_LISTXATTR / listxattr).
func (s *StorageService) ListXAttr(ctx context.Context, req *bowlv1.ListXAttrRequest) (*bowlv1.ListXAttrResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	_, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	if _, err := s.getInode(ctx, schemaName, req.GetIno()); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	if s.db == nil {
		return nil, connect.NewError(connect.CodeInternal, "database not initialized")
	}

	query := fmt.Sprintf(`SELECT name FROM "%s".xattrs WHERE ino = $1 ORDER BY name ASC;`, schemaName)
	rows, err := s.db.Query(ctx, query, int64(req.GetIno()))
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to list xattrs: %s", err)
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to scan xattr name: %s", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to iterate xattrs: %s", err)
	}

	return &bowlv1.ListXAttrResponse{
		Names: names,
	}, nil
}

// RemoveXAttr deletes an extended attribute from an Inode (FUSE_REMOVEXATTR / removexattr).
func (s *StorageService) RemoveXAttr(ctx context.Context, req *bowlv1.RemoveXAttrRequest) (*bowlv1.RemoveXAttrResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "name is required")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	if sess.ReadOnly || !sess.IsWriteLease || sess.Revoked {
		return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only or lease expired")
	}
	if !sess.LeaseExpiresAt.IsZero() && time.Now().After(sess.LeaseExpiresAt) {
		return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: write lease expired")
	}

	if _, err := s.getInode(ctx, schemaName, req.GetIno()); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	if s.db == nil {
		return nil, connect.NewError(connect.CodeInternal, "database not initialized")
	}

	query := fmt.Sprintf(`DELETE FROM "%s".xattrs WHERE ino = $1 AND name = $2;`, schemaName)
	tag, err := s.db.Exec(ctx, query, int64(req.GetIno()), name)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to delete xattr: %s", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, connect.Errorf(connect.CodeNotFound, "attribute '%s' not found for inode %d", name, req.GetIno())
	}

	return &bowlv1.RemoveXAttrResponse{}, nil
}
