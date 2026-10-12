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

	// 2. 维护 "<schema>".slice_refs 表的切片引用记录
	for _, s := range slices {
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
				sliceCounts[s.ID]++
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
