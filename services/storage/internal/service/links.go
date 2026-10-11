package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func (s *StorageService) withTx(ctx context.Context, fn func(executor DBExecutor) error) error {
	if tb, ok := s.db.(txBeginner); ok {
		tx, err := tb.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() {
			_ = tx.Rollback(ctx)
		}()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	return fn(s.db)
}

// Link creates a hard link to an existing regular file or special node (FUSE_LINK).
func (s *StorageService) Link(ctx context.Context, req *bowlv1.LinkRequest) (*bowlv1.LinkResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetSrcIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "src_ino must be greater than 0")
	}
	if req.GetNewParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "new_parent_ino must be greater than 0")
	}
	newName := req.GetNewName()
	if err := validateEntryName(newName); err != nil {
		return nil, err
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Find source Inode by src_ino
	srcInode, err := s.getInode(ctx, schemaName, req.GetSrcIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "source inode %d not found", req.GetSrcIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query source inode: %s", err)
	}

	// 2. Reject hard linking directories (POSIX forbids hardlinks to directories)
	if srcInode.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.NewError(connect.CodeInvalidArgument, "cannot create hard link to directory")
	}

	// 3. Check new_parent exists and is a directory
	newParentInode, err := s.getInode(ctx, schemaName, req.GetNewParentIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "new parent inode %d not found", req.GetNewParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query new parent inode: %s", err)
	}
	if newParentInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "new parent inode %d is not a directory", req.GetNewParentIno())
	}

	// 4. Check new_name doesn't already exist under new_parent
	_, err = s.getInodeByParentAndName(ctx, schemaName, req.GetNewParentIno(), newName)
	if err == nil {
		return nil, connect.Errorf(connect.CodeAlreadyExists, "entry %q already exists in parent %d", newName, req.GetNewParentIno())
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check entry existence: %s", err)
	}

	now := time.Now()
	newNlink := srcInode.GetNlink() + 1

	// 5. Update nlink and insert new entry in a transaction
	err = s.withTx(ctx, func(exec DBExecutor) error {
		// Increment nlink on all existing entries pointing to src_ino
		updateSrcSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = $1, ctime = $2 WHERE ino = $3;`, schemaName)
		if _, err := exec.Exec(ctx, updateSrcSQL, int(newNlink), now, int64(req.GetSrcIno())); err != nil {
			return fmt.Errorf("failed to increment source nlink: %w", err)
		}

		// Insert new entry with shared ino
		insertSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);
`, schemaName)
		if _, err := exec.Exec(
			ctx,
			insertSQL,
			int64(req.GetSrcIno()),
			int64(req.GetNewParentIno()),
			newName,
			int(srcInode.GetType()),
			int(srcInode.GetMode()),
			int(newNlink),
			int(srcInode.GetUid()),
			int(srcInode.GetGid()),
			int64(srcInode.GetSize()),
			int64(srcInode.GetBlocks()),
			int64(srcInode.GetRdev()),
			srcInode.GetAtime().AsTime(),
			srcInode.GetMtime().AsTime(),
			now,
			srcInode.GetTarget(),
			int64(srcInode.GetGeneration()),
		); err != nil {
			return fmt.Errorf("failed to insert hardlink entry: %w", err)
		}

		// Update new parent timestamps
		updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
		if _, err := exec.Exec(ctx, updateParentSQL, now, int64(req.GetNewParentIno())); err != nil {
			return fmt.Errorf("failed to update parent directory timestamps: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "%s", err)
	}

	linkedInode := &bowlv1.Inode{
		Ino:         req.GetSrcIno(),
		ParentIno:   0, // As per proto spec, for hard links (nlink > 1), parent_ino is set to 0
		Type:        srcInode.GetType(),
		Mode:        srcInode.GetMode(),
		Uid:         srcInode.GetUid(),
		Gid:         srcInode.GetGid(),
		Size:        srcInode.GetSize(),
		Blocks:      srcInode.GetBlocks(),
		Nlink:       newNlink,
		Rdev:        srcInode.GetRdev(),
		Atime:       srcInode.GetAtime(),
		Mtime:       srcInode.GetMtime(),
		Ctime:       timestamppb.New(now),
		Target:      srcInode.GetTarget(),
		Generation:  srcInode.GetGeneration(),
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.LinkResponse{
		Inode:        linkedInode,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// Symlink creates a symbolic link (FUSE_SYMLINK).
func (s *StorageService) Symlink(ctx context.Context, req *bowlv1.SymlinkRequest) (*bowlv1.SymlinkResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if err := validateEntryName(name); err != nil {
		return nil, err
	}
	target := req.GetTarget()
	if target == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "target is required")
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Check parent directory exists and is a directory
	parentInode, err := s.getInode(ctx, schemaName, req.GetParentIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent inode %d not found", req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent inode: %s", err)
	}
	if parentInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent inode %d is not a directory", req.GetParentIno())
	}

	// 2. Check entry doesn't already exist
	_, err = s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err == nil {
		return nil, connect.Errorf(connect.CodeAlreadyExists, "entry %q already exists in parent %d", name, req.GetParentIno())
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check entry existence: %s", err)
	}

	// 3. Allocate next ino
	newIno, err := s.allocateNextIno(ctx, schemaName)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to allocate inode: %s", err)
	}

	now := time.Now()
	targetSize := int64(len(target))

	// 4. Insert symlink inode (type=SYMLINK, mode=0777, nlink=1, size=len(target), target=target)
	insertSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation)
VALUES ($1, $2, $3, $4, $5, 1, 0, 0, $6, 0, 0, $7, $7, $7, $8, 0);
`, schemaName)
	if _, err := s.db.Exec(
		ctx,
		insertSQL,
		int64(newIno),
		int64(req.GetParentIno()),
		name,
		int(bowlv1.InodeType_INODE_TYPE_SYMLINK),
		0o777,
		targetSize,
		now,
		target,
	); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to insert symlink inode: %s", err)
	}

	// 5. Update parent directory timestamps
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	_, _ = s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno()))

	inode := &bowlv1.Inode{
		Ino:         newIno,
		ParentIno:   req.GetParentIno(),
		Type:        bowlv1.InodeType_INODE_TYPE_SYMLINK,
		Mode:        0o777,
		Uid:         0,
		Gid:         0,
		Size:        uint64(targetSize),
		Blocks:      0,
		Nlink:       1,
		Rdev:        0,
		Atime:       timestamppb.New(now),
		Mtime:       timestamppb.New(now),
		Ctime:       timestamppb.New(now),
		Target:      target,
		Generation:  0,
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.SymlinkResponse{
		Inode:        inode,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// Readlink reads the target of a symbolic link (FUSE_READLINK).
func (s *StorageService) Readlink(ctx context.Context, req *bowlv1.ReadlinkRequest) (*bowlv1.ReadlinkResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	_, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	inode, err := s.getInode(ctx, schemaName, req.GetIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
	}

	if inode.GetType() != bowlv1.InodeType_INODE_TYPE_SYMLINK {
		return nil, connect.NewError(connect.CodeInvalidArgument, "inode is not a symlink")
	}

	return &bowlv1.ReadlinkResponse{
		Target: inode.GetTarget(),
	}, nil
}

// Rename renames or moves a file or directory, supporting NOREPLACE and EXCHANGE flags (FUSE_RENAME / FUSE_RENAME2).
func (s *StorageService) Rename(ctx context.Context, req *bowlv1.RenameRequest) (*bowlv1.RenameResponse, error) {
	if strings.TrimSpace(req.GetSessionId()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetParentSrc() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_src must be greater than 0")
	}
	if err := validateEntryName(req.GetNameSrc()); err != nil {
		return nil, err
	}
	if req.GetParentDst() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_dst must be greater than 0")
	}
	if err := validateEntryName(req.GetNameDst()); err != nil {
		return nil, err
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Check parent_src exists and is a directory
	srcParent, err := s.getInode(ctx, schemaName, req.GetParentSrc())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent_src inode %d not found", req.GetParentSrc())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent_src: %s", err)
	}
	if srcParent.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent_src inode %d is not a directory", req.GetParentSrc())
	}

	// 2. Check parent_dst exists and is a directory
	dstParent, err := s.getInode(ctx, schemaName, req.GetParentDst())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent_dst inode %d not found", req.GetParentDst())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent_dst: %s", err)
	}
	if dstParent.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent_dst inode %d is not a directory", req.GetParentDst())
	}

	// 3. Find source entry
	srcEntry, err := s.getInodeByParentAndName(ctx, schemaName, req.GetParentSrc(), req.GetNameSrc())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "source entry %q not found in parent %d", req.GetNameSrc(), req.GetParentSrc())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query source entry: %s", err)
	}

	flags := req.GetFlags()
	isExchange := (flags & uint32(bowlv1.RenameFlag_RENAME_FLAG_EXCHANGE)) != 0
	isNoReplace := (flags & uint32(bowlv1.RenameFlag_RENAME_FLAG_NO_REPLACE)) != 0

	// 4. Check identical source and destination
	if req.GetParentSrc() == req.GetParentDst() && req.GetNameSrc() == req.GetNameDst() {
		if isExchange {
			return nil, connect.NewError(connect.CodeInvalidArgument, "cannot exchange identical paths")
		}
		return &bowlv1.RenameResponse{}, nil
	}

	// 5. Prevent moving a directory into a subdirectory of itself
	if srcEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY && req.GetParentSrc() != req.GetParentDst() {
		curr := req.GetParentDst()
		for curr > 1 {
			if curr == srcEntry.GetIno() {
				return nil, connect.NewError(connect.CodeInvalidArgument, "cannot move directory into a subdirectory of itself")
			}
			pInode, pErr := s.getInode(ctx, schemaName, curr)
			if pErr != nil || pInode.GetParentIno() == curr || pInode.GetParentIno() == 0 {
				break
			}
			curr = pInode.GetParentIno()
		}
		if curr == srcEntry.GetIno() {
			return nil, connect.NewError(connect.CodeInvalidArgument, "cannot move directory into a subdirectory of itself")
		}
	}

	// 6. Check destination entry
	dstEntry, err := s.getInodeByParentAndName(ctx, schemaName, req.GetParentDst(), req.GetNameDst())
	dstExists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to query destination entry: %s", err)
	}

	now := time.Now()

	// 7. Handle RENAME_EXCHANGE
	if isExchange {
		if !dstExists {
			return nil, connect.Errorf(connect.CodeNotFound, "destination entry %q not found for exchange", req.GetNameDst())
		}

		err = s.withTx(ctx, func(exec DBExecutor) error {
			tmpName := fmt.Sprintf("__bowl_tmp_xchg_%d_%d", now.UnixNano(), srcEntry.GetIno())

			// Step a: Move srcEntry to temporary name
			moveTmpSQL := fmt.Sprintf(`UPDATE "%s".inodes SET name = $1 WHERE parent = $2 AND name = $3;`, schemaName)
			if _, err := exec.Exec(ctx, moveTmpSQL, tmpName, int64(req.GetParentSrc()), req.GetNameSrc()); err != nil {
				return fmt.Errorf("failed to move source to temp name: %w", err)
			}

			// Step b: Move dstEntry to parentSrc, nameSrc
			moveDstToSrcSQL := fmt.Sprintf(`UPDATE "%s".inodes SET parent = $1, name = $2, ctime = $3 WHERE parent = $4 AND name = $5;`, schemaName)
			if _, err := exec.Exec(ctx, moveDstToSrcSQL, int64(req.GetParentSrc()), req.GetNameSrc(), now, int64(req.GetParentDst()), req.GetNameDst()); err != nil {
				return fmt.Errorf("failed to move destination to source: %w", err)
			}

			// Step c: Move srcEntry from temp name to parentDst, nameDst
			moveTmpToDstSQL := fmt.Sprintf(`UPDATE "%s".inodes SET parent = $1, name = $2, ctime = $3 WHERE parent = $4 AND name = $5;`, schemaName)
			if _, err := exec.Exec(ctx, moveTmpToDstSQL, int64(req.GetParentDst()), req.GetNameDst(), now, int64(req.GetParentSrc()), tmpName); err != nil {
				return fmt.Errorf("failed to move temp to destination: %w", err)
			}

			// Step d: Update parent timestamps and directory nlink counts if cross-parent
			if req.GetParentSrc() != req.GetParentDst() {
				updParents := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino IN ($2, $3);`, schemaName)
				if _, err := exec.Exec(ctx, updParents, now, int64(req.GetParentSrc()), int64(req.GetParentDst())); err != nil {
					return fmt.Errorf("failed to update parents timestamps: %w", err)
				}

				if srcEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY && dstEntry.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1) WHERE ino = $1;`, schemaName), int64(req.GetParentSrc()))
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink + 1 WHERE ino = $1;`, schemaName), int64(req.GetParentDst()))
				} else if dstEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY && srcEntry.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink + 1 WHERE ino = $1;`, schemaName), int64(req.GetParentSrc()))
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1) WHERE ino = $1;`, schemaName), int64(req.GetParentDst()))
				}
			} else {
				updParent := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
				if _, err := exec.Exec(ctx, updParent, now, int64(req.GetParentSrc())); err != nil {
					return fmt.Errorf("failed to update parent timestamps: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "%s", err)
		}
		return &bowlv1.RenameResponse{}, nil
	}

	// 8. Handle NO_REPLACE and Overwrite
	if dstExists {
		if isNoReplace {
			return nil, connect.Errorf(connect.CodeAlreadyExists, "destination entry %q already exists in parent %d", req.GetNameDst(), req.GetParentDst())
		}

		// Check type compatibility
		if srcEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
			if dstEntry.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
				return nil, connect.NewError(connect.CodeInvalidArgument, "cannot overwrite non-directory with directory")
			}
			// Destination directory must be empty
			countQuery := fmt.Sprintf(`SELECT count(*) FROM "%s".inodes WHERE parent = $1 AND ino != $1;`, schemaName)
			var childCount int64
			if err := s.db.QueryRow(ctx, countQuery, int64(dstEntry.GetIno())).Scan(&childCount); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to check destination directory: %s", err)
			}
			if childCount > 0 {
				return nil, connect.NewError(connect.CodeFailedPrecondition, "destination directory not empty")
			}
		} else {
			if dstEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
				return nil, connect.NewError(connect.CodeInvalidArgument, "cannot overwrite directory with non-directory")
			}
		}

		// Overwrite in transaction
		err = s.withTx(ctx, func(exec DBExecutor) error {
			// Remove existing destination entry
			if dstEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
				deleteDstDir := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE ino = $1;`, schemaName)
				if _, err := exec.Exec(ctx, deleteDstDir, int64(dstEntry.GetIno())); err != nil {
					return fmt.Errorf("failed to delete destination directory: %w", err)
				}
				decDstParent := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1) WHERE ino = $1;`, schemaName)
				if _, err := exec.Exec(ctx, decDstParent, int64(req.GetParentDst())); err != nil {
					return fmt.Errorf("failed to decrement destination parent nlink: %w", err)
				}
			} else {
				if dstEntry.GetNlink() <= 1 {
					deleteDstSQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE ino = $1;`, schemaName)
					if _, err := exec.Exec(ctx, deleteDstSQL, int64(dstEntry.GetIno())); err != nil {
						return fmt.Errorf("failed to delete destination inode: %w", err)
					}
				} else {
					deleteDstEntrySQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE parent = $1 AND name = $2;`, schemaName)
					if _, err := exec.Exec(ctx, deleteDstEntrySQL, int64(req.GetParentDst()), req.GetNameDst()); err != nil {
						return fmt.Errorf("failed to delete destination entry: %w", err)
					}
					decNlinkSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink - 1, ctime = $1 WHERE ino = $2;`, schemaName)
					if _, err := exec.Exec(ctx, decNlinkSQL, now, int64(dstEntry.GetIno())); err != nil {
						return fmt.Errorf("failed to decrement destination nlink: %w", err)
					}
				}
			}

			// Move source entry to destination
			moveSQL := fmt.Sprintf(`UPDATE "%s".inodes SET parent = $1, name = $2, ctime = $3 WHERE parent = $4 AND name = $5;`, schemaName)
			if _, err := exec.Exec(ctx, moveSQL, int64(req.GetParentDst()), req.GetNameDst(), now, int64(req.GetParentSrc()), req.GetNameSrc()); err != nil {
				return fmt.Errorf("failed to move source entry to destination: %w", err)
			}

			// Update parent timestamps and directory nlink counts
			if req.GetParentSrc() != req.GetParentDst() {
				updParents := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino IN ($2, $3);`, schemaName)
				if _, err := exec.Exec(ctx, updParents, now, int64(req.GetParentSrc()), int64(req.GetParentDst())); err != nil {
					return fmt.Errorf("failed to update parent timestamps: %w", err)
				}

				if srcEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1) WHERE ino = $1;`, schemaName), int64(req.GetParentSrc()))
					_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink + 1 WHERE ino = $1;`, schemaName), int64(req.GetParentDst()))
				}
			} else {
				updParent := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
				if _, err := exec.Exec(ctx, updParent, now, int64(req.GetParentSrc())); err != nil {
					return fmt.Errorf("failed to update parent timestamps: %w", err)
				}
			}

			return nil
		})
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "%s", err)
		}
		return &bowlv1.RenameResponse{}, nil
	}

	// 9. Move into non-existent destination
	err = s.withTx(ctx, func(exec DBExecutor) error {
		moveSQL := fmt.Sprintf(`UPDATE "%s".inodes SET parent = $1, name = $2, ctime = $3 WHERE parent = $4 AND name = $5;`, schemaName)
		if _, err := exec.Exec(ctx, moveSQL, int64(req.GetParentDst()), req.GetNameDst(), now, int64(req.GetParentSrc()), req.GetNameSrc()); err != nil {
			return fmt.Errorf("failed to move entry: %w", err)
		}

		if req.GetParentSrc() != req.GetParentDst() {
			updParents := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino IN ($2, $3);`, schemaName)
			if _, err := exec.Exec(ctx, updParents, now, int64(req.GetParentSrc()), int64(req.GetParentDst())); err != nil {
				return fmt.Errorf("failed to update parent timestamps: %w", err)
			}

			if srcEntry.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
				_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1) WHERE ino = $1;`, schemaName), int64(req.GetParentSrc()))
				_, _ = exec.Exec(ctx, fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink + 1 WHERE ino = $1;`, schemaName), int64(req.GetParentDst()))
			}
		} else {
			updParent := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
			if _, err := exec.Exec(ctx, updParent, now, int64(req.GetParentSrc())); err != nil {
				return fmt.Errorf("failed to update parent timestamps: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "%s", err)
	}

	return &bowlv1.RenameResponse{}, nil
}
