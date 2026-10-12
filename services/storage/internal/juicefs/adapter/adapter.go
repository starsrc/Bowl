package adapter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/meta"
)

// DBExecutor 抽象 PostgreSQL 数据库与事务执行接口
type DBExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// MetaAdapter 定义 JuiceFS 元数据适配层核心切片账本接口
type MetaAdapter interface {
	// NewSlice 分配全局唯一单调递增的切片编号
	NewSlice(ctx context.Context) (uint64, error)

	// WriteSlice 将单切片原子追加到指定 Chunk 元数据行中，并更新切片引用计数
	WriteSlice(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slice Slice) error

	// WriteSlices 将多个切片批量追加到指定 Chunk 元数据行中，并维护切片引用计数
	WriteSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slices []Slice) error

	// ReadSlices 查询指定 Chunk 的所有原始切片记录
	ReadSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]Slice, error)

	// ReadReconstitutedSlices 查询指定 Chunk 并重组图层覆盖视图，返回顺序无重叠的切片视图列表
	ReadReconstitutedSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]meta.Slice, error)

	// LocateSlice 查找引用指定切片编号的 (ino, chunk_indx)
	LocateSlice(ctx context.Context, diskID string, sliceID uint64) (uint64, uint32, bool, error)

	// IncSliceRef 增加指定切片的引用计数（例如用于克隆或写时复制）
	IncSliceRef(ctx context.Context, diskID string, sliceID uint64, count int, size uint32) error

	// DecSliceRef 减少指定切片的引用计数（例如用于截断或删除）
	DecSliceRef(ctx context.Context, diskID string, sliceID uint64, count int) error

	// GetSliceRef 获取指定切片的当前引用计数与物理尺寸
	GetSliceRef(ctx context.Context, diskID string, sliceID uint64) (int, uint32, error)

	// DeleteChunk 删除指定 Chunk 的元数据行，并返回被删除的所有切片（便于联动递减引用计数）
	DeleteChunk(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]Slice, error)

	// DeleteInodeChunks 删除指定 Inode 的所有 Chunk 元数据行，并通过 DecSliceRef 安全递减关联切片的引用计数，返回标记为物理孤儿切片（refs <= 0）的切片编号列表
	DeleteInodeChunks(ctx context.Context, diskID string, ino uint64) ([]uint64, error)

	// TruncateChunkSlices 替换指定 Chunk 的切片列表（用于文件截断保留局部切片）
	TruncateChunkSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slices []Slice) error

	// TruncateSlices 依据截断新尺寸截断/清理超出范围的切片，缩短边界切片，并安全递减淘汰切片的引用计数，返回成为孤儿切片的切片编号列表
	TruncateSlices(ctx context.Context, diskID string, ino uint64, size uint64) ([]uint64, error)

	// PunchHole 向指定区间打洞释放物理存储，向重叠 Chunk 原子追加空洞切片
	PunchHole(ctx context.Context, diskID string, ino uint64, offset uint64, length uint64) error

	// CloneRange 在同一个虚拟磁盘内通过元数据写时复制克隆切片账本记录并原子递增 slice_refs 引用计数
	CloneRange(ctx context.Context, diskID string, srcIno uint64, srcOffset uint64, dstIno uint64, dstOffset uint64, length uint64) (uint64, error)

	// WithExecutor 绑定指定事务执行器，支持事务内原子执行
	WithExecutor(exec DBExecutor) MetaAdapter
}

// metaAdapter 实现 MetaAdapter 接口
type metaAdapter struct {
	db          DBExecutor
	nextSliceID atomic.Uint64
}

// FormatDiskSchema 解析并格式化虚拟磁盘独立的 PostgreSQL Schema 名称 (disk_<uuid-with-underscores>)
func FormatDiskSchema(diskID string) (string, error) {
	if strings.HasPrefix(diskID, "disk_") {
		return diskID, nil
	}
	u, err := uuid.Parse(diskID)
	if err != nil {
		return "", fmt.Errorf("非法虚拟磁盘唯一标识符 %q: %w", diskID, err)
	}
	return "disk_" + strings.ReplaceAll(u.String(), "-", "_"), nil
}

// NewMetaAdapter 构造基于 PostgreSQL Per-Disk Schema 隔离表的 JuiceFS 元数据适配器
func NewMetaAdapter(db DBExecutor) MetaAdapter {
	a := &metaAdapter{
		db: db,
	}
	// 默认以当前毫秒时间戳作为原子计数器初始底数，保障跨重启不发生碰撞
	a.nextSliceID.Store(uint64(time.Now().UnixNano()))
	return a
}

// WithExecutor 创建绑定新 DB 执行器（如事务）的适配器浅拷贝
func (a *metaAdapter) WithExecutor(exec DBExecutor) MetaAdapter {
	return &metaAdapter{
		db:          exec,
		nextSliceID: a.nextSliceID,
	}
}

// NewSlice 分配全局唯一单调递增的切片编号
func (a *metaAdapter) NewSlice(ctx context.Context) (uint64, error) {
	id := a.nextSliceID.Add(1)
	return id, nil
}

// WriteSlice 将单切片原子追加到指定 Chunk 元数据行中，并更新切片引用计数
func (a *metaAdapter) WriteSlice(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slice Slice) error {
	return a.WriteSlices(ctx, diskID, ino, chunkIndx, []Slice{slice})
}

// WriteSlices 将多个切片批量追加到指定 Chunk 元数据行中，并维护切片引用计数
func (a *metaAdapter) WriteSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slices []Slice) error {
	if len(slices) == 0 {
		return nil
	}

	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return err
	}

	for _, s := range slices {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("切片校验失败 (ID=%d): %w", s.ID, err)
		}
	}

	encoded := EncodeSlices(slices)

	// 1. 原子追加至 "<schema>".chunks 表 (BYTEA || BYTEA)
	insertChunkSQL := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES ($1, $2, $3)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = "%s".chunks.slices || EXCLUDED.slices;
`, schemaName, schemaName)

	if _, err := a.db.Exec(ctx, insertChunkSQL, int64(ino), int32(chunkIndx), encoded); err != nil {
		return fmt.Errorf("写入 chunks 表切片元数据失败: %w", err)
	}

	// 2. 维护 "<schema>".slice_refs 表的切片引用记录 (稀疏空洞切片 ID == 0 无物理引用)
	for _, s := range slices {
		if s.ID == 0 {
			continue
		}
		insertRefSQL := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, chunk_indx, size, refs)
VALUES ($1, $2, $3, 1)
ON CONFLICT (id)
DO UPDATE SET refs = "%s".slice_refs.refs + 1;
`, schemaName, schemaName)

		if _, err := a.db.Exec(ctx, insertRefSQL, int64(s.ID), int32(chunkIndx), int32(s.Size)); err != nil {
			return fmt.Errorf("写入 slice_refs 引用账本失败 (ID=%d): %w", s.ID, err)
		}
	}

	return nil
}

// ReadSlices 查询指定 Chunk 的所有原始切片记录
func (a *metaAdapter) ReadSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]Slice, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
	var raw []byte
	err = a.db.QueryRow(ctx, query, int64(ino), int32(chunkIndx)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("查询 chunks 表失败 (ino=%d, indx=%d): %w", ino, chunkIndx, err)
	}

	return DecodeSlices(raw)
}

// ReadReconstitutedSlices 查询指定 Chunk 并重组图层覆盖视图，返回顺序无重叠的切片视图列表
func (a *metaAdapter) ReadReconstitutedSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]meta.Slice, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
	var raw []byte
	err = a.db.QueryRow(ctx, query, int64(ino), int32(chunkIndx)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("查询 chunks 表失败 (ino=%d, indx=%d): %w", ino, chunkIndx, err)
	}
	if len(raw) > 0 && len(raw)%SliceBytes != 0 {
		return nil, fmt.Errorf("%w: chunks 表元数据损坏 (ino=%d, indx=%d, len=%d)", ErrTruncatedBuffer, ino, chunkIndx, len(raw))
	}

	return BuildSliceFromRaw(raw), nil
}

// LocateSlice 查找引用指定切片编号的 (ino, chunk_indx)
func (a *metaAdapter) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (uint64, uint32, bool, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return 0, 0, false, err
	}

	sliceIDBytes := binary.BigEndian.AppendUint64(nil, sliceID)
	query := fmt.Sprintf(`SELECT ino, indx, slices FROM "%s".chunks WHERE position($1 IN slices) > 0`, schemaName)

	rows, err := a.db.Query(ctx, query, sliceIDBytes)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "3F000" || pgErr.Code == "42P01") {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("查找切片引用失败: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ino       int64
			indx      int32
			rawSlices []byte
		)
		if err := rows.Scan(&ino, &indx, &rawSlices); err != nil {
			return 0, 0, false, fmt.Errorf("读取 chunks 结果行失败: %w", err)
		}

		slices, err := DecodeSlices(rawSlices)
		if err != nil {
			continue
		}
		for _, s := range slices {
			if s.ID == sliceID {
				return uint64(ino), uint32(indx), true, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false, fmt.Errorf("迭代 chunks 结果集失败: %w", err)
	}

	return 0, 0, false, nil
}

// IncSliceRef 增加指定切片的引用计数（例如用于克隆或写时复制）
func (a *metaAdapter) IncSliceRef(ctx context.Context, diskID string, sliceID uint64, count int, size uint32) error {
	if count <= 0 {
		return nil
	}
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, size, refs)
VALUES ($1, $2, $3)
ON CONFLICT (id)
DO UPDATE SET refs = "%s".slice_refs.refs + EXCLUDED.refs;
`, schemaName, schemaName)

	if _, err := a.db.Exec(ctx, query, int64(sliceID), int32(size), int32(count)); err != nil {
		return fmt.Errorf("递增切片引用计数失败 (ID=%d): %w", sliceID, err)
	}
	return nil
}

// DecSliceRef 减少指定切片的引用计数（例如用于截断或删除）
func (a *metaAdapter) DecSliceRef(ctx context.Context, diskID string, sliceID uint64, count int) error {
	if count <= 0 {
		return nil
	}
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs - $1 WHERE id = $2`, schemaName)
	if _, err := a.db.Exec(ctx, query, int32(count), int64(sliceID)); err != nil {
		return fmt.Errorf("递减切片引用计数失败 (ID=%d): %w", sliceID, err)
	}
	return nil
}

// GetSliceRef 获取指定切片的当前引用计数与物理尺寸
func (a *metaAdapter) GetSliceRef(ctx context.Context, diskID string, sliceID uint64) (int, uint32, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return 0, 0, err
	}

	query := fmt.Sprintf(`SELECT refs, size FROM "%s".slice_refs WHERE id = $1`, schemaName)
	var (
		refs int32
		size int32
	)
	err = a.db.QueryRow(ctx, query, int64(sliceID)).Scan(&refs, &size)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, fmt.Errorf("切片引用记录不存在 (ID=%d)", sliceID)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("查询切片引用失败 (ID=%d): %w", sliceID, err)
	}
	return int(refs), uint32(size), nil
}

// DeleteChunk 删除指定 Chunk 的元数据行，并返回被删除的所有切片
func (a *metaAdapter) DeleteChunk(ctx context.Context, diskID string, ino uint64, chunkIndx uint32) ([]Slice, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND indx = $2 RETURNING slices`, schemaName)
	var raw []byte
	err = a.db.QueryRow(ctx, query, int64(ino), int32(chunkIndx)).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("删除 chunk 记录失败 (ino=%d, indx=%d): %w", ino, chunkIndx, err)
	}

	return DecodeSlices(raw)
}

// TruncateChunkSlices 替换指定 Chunk 的切片列表（用于文件截断保留局部切片）
func (a *metaAdapter) TruncateChunkSlices(ctx context.Context, diskID string, ino uint64, chunkIndx uint32, slices []Slice) error {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return err
	}

	if len(slices) == 0 {
		deleteSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
		_, err := a.db.Exec(ctx, deleteSQL, int64(ino), int32(chunkIndx))
		return err
	}

	encoded := EncodeSlices(slices)
	updateSQL := fmt.Sprintf(`UPDATE "%s".chunks SET slices = $1 WHERE ino = $2 AND indx = $3`, schemaName)
	_, err = a.db.Exec(ctx, updateSQL, encoded, int64(ino), int32(chunkIndx))
	return err
}

// DeleteInodeChunks 删除指定 Inode 的所有 Chunk 元数据行，并通过 DecSliceRef 安全递减关联切片的引用计数，返回标记为物理孤儿切片（refs <= 0）的切片编号列表
func (a *metaAdapter) DeleteInodeChunks(ctx context.Context, diskID string, ino uint64) ([]uint64, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return nil, err
	}

	// 1. 查询该 Inode 下属的所有 Chunk 行及切片二进制数据
	queryChunks := fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1`, schemaName)
	rows, err := a.db.Query(ctx, queryChunks, int64(ino))
	if err != nil {
		return nil, fmt.Errorf("查询 Inode %d 的切片元数据失败: %w", ino, err)
	}
	defer rows.Close()

	sliceCounts := make(map[uint64]int)
	for rows.Next() {
		var indx int32
		var rawSlices []byte
		if err := rows.Scan(&indx, &rawSlices); err != nil {
			return nil, fmt.Errorf("扫描 Inode %d 的 chunk 记录失败: %w", ino, err)
		}
		if len(rawSlices) > 0 {
			slices, err := DecodeSlices(rawSlices)
			if err != nil {
				return nil, fmt.Errorf("解码 Inode %d 的切片数据失败: %w", ino, err)
			}
			for _, s := range slices {
				if s.ID > 0 {
					sliceCounts[s.ID]++
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 Inode %d 的 chunks 结果集失败: %w", ino, err)
	}
	rows.Close()

	var orphanSliceIDs []uint64

	// 2. 依次通过 DecSliceRef 递减切片引用计数，并查验标记物理孤儿切片 (refs <= 0)
	for sliceID, count := range sliceCounts {
		if err := a.DecSliceRef(ctx, diskID, sliceID, count); err != nil {
			return nil, fmt.Errorf("递减切片 %d 引用计数失败: %w", sliceID, err)
		}

		// 检查更新后的引用计数，若 <= 0 则判定为物理孤儿切片
		var currentRefs int32
		checkRefSQL := fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName)
		err := a.db.QueryRow(ctx, checkRefSQL, int64(sliceID)).Scan(&currentRefs)
		if err == nil && currentRefs <= 0 {
			orphanSliceIDs = append(orphanSliceIDs, sliceID)
		}
	}

	// 3. 删除该 Inode 的全部 chunks 行
	deleteChunksSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1`, schemaName)
	if _, err := a.db.Exec(ctx, deleteChunksSQL, int64(ino)); err != nil {
		return nil, fmt.Errorf("删除 Inode %d 的 chunks 行失败: %w", ino, err)
	}

	return orphanSliceIDs, nil
}

// TruncateSlices 依据截断新尺寸截断/清理超出范围的切片，缩短边界切片，并安全递减淘汰切片的引用计数，返回成为孤儿切片的切片编号列表
func (a *metaAdapter) TruncateSlices(ctx context.Context, diskID string, ino uint64, size uint64) ([]uint64, error) {
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return nil, err
	}

	targetChunkIndx := uint32(size / ChunkSize)
	targetChunkPos := uint32(size % ChunkSize)

	discardedSliceCounts := make(map[uint64]int)

	// 1. 查询并删除完全超出截断范围的全部 Chunk
	var boundaryCondition string
	if targetChunkPos == 0 {
		boundaryCondition = fmt.Sprintf("indx >= %d", targetChunkIndx)
	} else {
		boundaryCondition = fmt.Sprintf("indx > %d", targetChunkIndx)
	}

	queryBeyondChunks := fmt.Sprintf(`SELECT indx, slices FROM "%s".chunks WHERE ino = $1 AND %s`, schemaName, boundaryCondition)
	rows, err := a.db.Query(ctx, queryBeyondChunks, int64(ino))
	if err != nil {
		return nil, fmt.Errorf("查询越界 chunks 失败: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cIndx int32
		var rawSlices []byte
		if err := rows.Scan(&cIndx, &rawSlices); err != nil {
			return nil, fmt.Errorf("扫描越界 chunk 失败: %w", err)
		}
		if len(rawSlices) > 0 {
			slices, err := DecodeSlices(rawSlices)
			if err != nil {
				return nil, fmt.Errorf("解码越界 chunk 切片失败: %w", err)
			}
			for _, s := range slices {
				if s.ID > 0 {
					discardedSliceCounts[s.ID]++
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历越界 chunks 失败: %w", err)
	}
	rows.Close()

	deleteBeyondSQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND %s`, schemaName, boundaryCondition)
	if _, err := a.db.Exec(ctx, deleteBeyondSQL, int64(ino)); err != nil {
		return nil, fmt.Errorf("删除越界 chunks 失败: %w", err)
	}

	// 2. 若 targetChunkPos > 0，检查并裁剪边界 Chunk
	if targetChunkPos > 0 {
		var rawSlices []byte
		queryBoundaryChunk := fmt.Sprintf(`SELECT slices FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
		err := a.db.QueryRow(ctx, queryBoundaryChunk, int64(ino), int32(targetChunkIndx)).Scan(&rawSlices)
		if err == nil && len(rawSlices) > 0 {
			slices, err := DecodeSlices(rawSlices)
			if err != nil {
				return nil, fmt.Errorf("解码边界 chunk 切片失败: %w", err)
			}

			var preservedSlices []Slice
			for _, s := range slices {
				if s.Pos >= targetChunkPos {
					if s.ID > 0 {
						discardedSliceCounts[s.ID]++
					}
				} else if s.Pos+s.Len > targetChunkPos {
					adjusted := Slice{
						Pos:  s.Pos,
						ID:   s.ID,
						Size: s.Size,
						Off:  s.Off,
						Len:  targetChunkPos - s.Pos,
					}
					preservedSlices = append(preservedSlices, adjusted)
				} else {
					preservedSlices = append(preservedSlices, s)
				}
			}

			if len(preservedSlices) == 0 {
				deleteBoundarySQL := fmt.Sprintf(`DELETE FROM "%s".chunks WHERE ino = $1 AND indx = $2`, schemaName)
				if _, err := a.db.Exec(ctx, deleteBoundarySQL, int64(ino), int32(targetChunkIndx)); err != nil {
					return nil, fmt.Errorf("删除空边界 chunk 失败: %w", err)
				}
			} else {
				if err := a.TruncateChunkSlices(ctx, diskID, ino, targetChunkIndx, preservedSlices); err != nil {
					return nil, fmt.Errorf("更新边界 chunk 切片失败: %w", err)
				}
			}
		} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("查询边界 chunk 失败: %w", err)
		}
	}

	// 3. 递减淘汰切片的引用计数并排查孤儿切片
	var orphanSliceIDs []uint64
	for sliceID, count := range discardedSliceCounts {
		if err := a.DecSliceRef(ctx, diskID, sliceID, count); err != nil {
			return nil, fmt.Errorf("递减切片引用计数失败 (ID=%d): %w", sliceID, err)
		}
		var currentRefs int32
		checkRefSQL := fmt.Sprintf(`SELECT refs FROM "%s".slice_refs WHERE id = $1`, schemaName)
		if err := a.db.QueryRow(ctx, checkRefSQL, int64(sliceID)).Scan(&currentRefs); err == nil && currentRefs <= 0 {
			orphanSliceIDs = append(orphanSliceIDs, sliceID)
		}
	}

	return orphanSliceIDs, nil
}

// PunchHole 向指定区间打洞释放物理存储，向重叠 Chunk 原子追加空洞切片
func (a *metaAdapter) PunchHole(ctx context.Context, diskID string, ino uint64, offset uint64, length uint64) error {
	if length == 0 {
		return nil
	}
	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return err
	}

	curOffset := offset
	remainingLen := length

	for remainingLen > 0 {
		curChunk := uint32(curOffset / ChunkSize)
		chunkEnd := uint64(curChunk+1) * ChunkSize
		chunkRemaining := chunkEnd - curOffset

		pieceLen := remainingLen
		if pieceLen > chunkRemaining {
			pieceLen = chunkRemaining
		}
		chunkPos := uint32(curOffset % ChunkSize)

		holeSlice := Slice{
			Pos:  chunkPos,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  uint32(pieceLen),
		}

		encoded := EncodeSlice(holeSlice)

		// 原子追加至 chunks 表 (若 chunk 存在则追加，若不存在则插入)
		insertChunkSQL := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES ($1, $2, $3)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = "%s".chunks.slices || EXCLUDED.slices;
`, schemaName, schemaName)

		if _, err := a.db.Exec(ctx, insertChunkSQL, int64(ino), int32(curChunk), encoded); err != nil {
			return fmt.Errorf("写入打洞空洞切片元数据失败 (ino=%d, chunk=%d): %w", ino, curChunk, err)
		}

		curOffset += pieceLen
		remainingLen -= pieceLen
	}

	return nil
}

// CloneRange 在同一个虚拟磁盘内通过元数据写时复制克隆切片账本记录并原子递增 slice_refs 引用计数
func (a *metaAdapter) CloneRange(ctx context.Context, diskID string, srcIno uint64, srcOffset uint64, dstIno uint64, dstOffset uint64, length uint64) (uint64, error) {
	if length == 0 {
		return 0, nil
	}

	schemaName, err := FormatDiskSchema(diskID)
	if err != nil {
		return 0, err
	}

	// 1. 查询源 Inode 大小
	var srcSize int64
	querySrc := fmt.Sprintf(`SELECT size FROM "%s".inodes WHERE ino = $1`, schemaName)
	if err := a.db.QueryRow(ctx, querySrc, int64(srcIno)).Scan(&srcSize); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrSrcInodeNotFound
		}
		return 0, fmt.Errorf("查询源 Inode 大小失败: %w", err)
	}

	// 2. 检查目标 Inode 是否存在
	var dstExists bool
	queryDst := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM "%s".inodes WHERE ino = $1)`, schemaName)
	if err := a.db.QueryRow(ctx, queryDst, int64(dstIno)).Scan(&dstExists); err != nil {
		return 0, fmt.Errorf("检查目标 Inode 失败: %w", err)
	}
	if !dstExists {
		return 0, ErrDstInodeNotFound
	}

	// 边界检查：若 srcOffset 已到达或超出源文件大小，直接返回 0
	if srcOffset >= uint64(srcSize) {
		return 0, nil
	}

	toCopy := length
	available := uint64(srcSize) - srcOffset
	if toCopy > available {
		toCopy = available
	}
	if toCopy == 0 {
		return 0, nil
	}

	srcStart := srcOffset
	srcEnd := srcOffset + toCopy

	startChunkIndx := int32(srcStart / ChunkSize)
	endChunkIndx := int32((srcEnd - 1) / ChunkSize)

	// 3. 查询与请求区间 [srcStart, srcEnd) 相交的源 Inode Chunk 切片元数据
	queryChunks := fmt.Sprintf(`
SELECT indx, slices FROM "%s".chunks
WHERE ino = $1 AND indx >= $2 AND indx <= $3
ORDER BY indx ASC;
`, schemaName)

	rows, err := a.db.Query(ctx, queryChunks, int64(srcIno), startChunkIndx, endChunkIndx)
	if err != nil {
		return 0, fmt.Errorf("查询源 Inode 切片元数据失败: %w", err)
	}
	defer rows.Close()

	type chunkRow struct {
		indx   uint32
		slices []Slice
	}
	var srcRows []chunkRow

	for rows.Next() {
		var cIndx int32
		var rawSlices []byte
		if err := rows.Scan(&cIndx, &rawSlices); err != nil {
			return 0, fmt.Errorf("扫描源 Inode 切片数据失败: %w", err)
		}
		slices, err := DecodeSlices(rawSlices)
		if err != nil {
			return 0, fmt.Errorf("解码源 Inode 切片数据失败: %w", err)
		}
		srcRows = append(srcRows, chunkRow{
			indx:   uint32(cIndx),
			slices: slices,
		})
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("遍历源 Inode 切片结果集失败: %w", err)
	}
	rows.Close()

	// 4. 将相交的源切片投射映射至目标文件 Chunk 偏移区间
	dstChunks := make(map[uint32][]Slice)
	var dstChunkOrder []uint32

	type sliceRefUpdate struct {
		id   uint64
		size uint32
	}
	var sliceRefsToInc []sliceRefUpdate

	for _, cr := range srcRows {
		chunkBase := uint64(cr.indx) * uint64(ChunkSize)
		for _, s := range cr.slices {
			sliceStart := chunkBase + uint64(s.Pos)
			sliceEnd := sliceStart + uint64(s.Len)

			overlapStart := max(sliceStart, srcStart)
			overlapEnd := min(sliceEnd, srcEnd)
			if overlapStart >= overlapEnd {
				continue
			}

			subLen := overlapEnd - overlapStart
			subOff := s.Off + uint32(overlapStart-sliceStart)
			dstFileStart := dstOffset + (overlapStart - srcStart)

			curDstPos := dstFileStart
			remainingLen := subLen
			curPayloadOff := subOff

			for remainingLen > 0 {
				dstChunkIndx := uint32(curDstPos / ChunkSize)
				dstChunkOffset := uint32(curDstPos % ChunkSize)
				dstChunkRemaining := uint64(ChunkSize - dstChunkOffset)

				pieceLen := remainingLen
				if pieceLen > dstChunkRemaining {
					pieceLen = dstChunkRemaining
				}

				clonedSlice := Slice{
					Pos:  dstChunkOffset,
					ID:   s.ID,
					Size: s.Size,
					Off:  curPayloadOff,
					Len:  uint32(pieceLen),
				}
				if err := clonedSlice.Validate(); err != nil {
					return 0, fmt.Errorf("克隆切片校验失败: %w", err)
				}

				if _, exists := dstChunks[dstChunkIndx]; !exists {
					dstChunkOrder = append(dstChunkOrder, dstChunkIndx)
				}
				dstChunks[dstChunkIndx] = append(dstChunks[dstChunkIndx], clonedSlice)

				curDstPos += pieceLen
				curPayloadOff += uint32(pieceLen)
				remainingLen -= pieceLen
			}

			sliceRefsToInc = append(sliceRefsToInc, sliceRefUpdate{
				id:   s.ID,
				size: s.Size,
			})
		}
	}

	// 5. 开启事务，原子递增 slice_refs、写入目标 chunks、更新目标 Inode 属性
	exec := a.db
	type txBeginner interface {
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	var tx pgx.Tx
	if tb, ok := a.db.(txBeginner); ok {
		var err error
		tx, err = tb.Begin(ctx)
		if err != nil {
			return 0, fmt.Errorf("开启事务失败: %w", err)
		}
		defer func() {
			if tx != nil {
				_ = tx.Rollback(ctx)
			}
		}()
		exec = tx
	}

	// 5.1 原子递增切片引用计数
	if len(sliceRefsToInc) > 0 {
		refCounts := make(map[uint64]int)
		refSizes := make(map[uint64]uint32)
		for _, ref := range sliceRefsToInc {
			refCounts[ref.id]++
			refSizes[ref.id] = ref.size
		}

		countsMap := make(map[int][]int64)
		for id, cnt := range refCounts {
			countsMap[cnt] = append(countsMap[cnt], int64(id))
		}

		for cnt, ids := range countsMap {
			updateRefSQL := fmt.Sprintf(`UPDATE "%s".slice_refs SET refs = refs + $1 WHERE id = ANY($2);`, schemaName)
			res, err := exec.Exec(ctx, updateRefSQL, int32(cnt), ids)
			if err != nil {
				return 0, fmt.Errorf("递增切片引用计数失败: %w", err)
			}
			if res.RowsAffected() < int64(len(ids)) {
				for _, id := range ids {
					insertRefSQL := fmt.Sprintf(`
INSERT INTO "%s".slice_refs (id, size, refs)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET refs = "%s".slice_refs.refs + EXCLUDED.refs;
`, schemaName, schemaName)
					if _, err := exec.Exec(ctx, insertRefSQL, id, int32(refSizes[uint64(id)]), int32(cnt)); err != nil {
						return 0, fmt.Errorf("插入切片引用计数失败: %w", err)
					}
				}
			}
		}
	}

	// 5.2 将克隆切片追加至目标 Inode chunks 表
	if len(dstChunkOrder) > 0 {
		valuePlaceholders := make([]string, len(dstChunkOrder))
		args := make([]any, 0, len(dstChunkOrder)*3)
		for i, indx := range dstChunkOrder {
			p1 := i*3 + 1
			p2 := i*3 + 2
			p3 := i*3 + 3
			valuePlaceholders[i] = fmt.Sprintf("($%d, $%d, $%d)", p1, p2, p3)
			args = append(args, int64(dstIno), int32(indx), EncodeSlices(dstChunks[indx]))
		}

		insertChunksSQL := fmt.Sprintf(`
INSERT INTO "%s".chunks (ino, indx, slices)
VALUES %s
ON CONFLICT (ino, indx)
DO UPDATE SET slices = "%s".chunks.slices || EXCLUDED.slices;
`, schemaName, strings.Join(valuePlaceholders, ", "), schemaName)

		if _, err := exec.Exec(ctx, insertChunksSQL, args...); err != nil {
			return 0, fmt.Errorf("追加目标 Chunk 元数据失败: %w", err)
		}
	}

	// 5.3 更新目标 Inode 大小、块数及修改时间
	now := time.Now()
	newEnd := int64(dstOffset + toCopy)
	updateInodeSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
	if _, err := exec.Exec(ctx, updateInodeSQL, newEnd, now, int64(dstIno)); err != nil {
		return 0, fmt.Errorf("更新目标 Inode 大小失败: %w", err)
	}

	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("提交克隆事务失败: %w", err)
		}
		tx = nil
	}

	return toCopy, nil
}
