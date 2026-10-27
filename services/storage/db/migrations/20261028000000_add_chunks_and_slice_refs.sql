-- +goose Up
-- +goose StatementBegin
-- 1. 全局唯一递增的 Slice ID 序列（从 1 开始，0 保留给空洞 Hole）
CREATE SEQUENCE IF NOT EXISTS slice_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 64;

-- 2. 文件逻辑分块映射表（不设隐式级联外键，删除 Inode 时显式清理并提取 slices 以扣减引用计数）
CREATE TABLE IF NOT EXISTS chunks (
    ino    BIGINT  NOT NULL,
    indx   INTEGER NOT NULL,
    slices BYTEA   NOT NULL,
    PRIMARY KEY (ino, indx),
    CONSTRAINT chk_chunks_ino CHECK (ino > 0),
    CONSTRAINT chk_chunks_indx CHECK (indx >= 0),
    CONSTRAINT chk_chunks_slices_len CHECK (octet_length(slices) > 0 AND octet_length(slices) % 24 = 0)
);

-- 3. 物理 Slice 引用计数表（支撑异步物理 GC、后台 Compaction 与未来零拷贝克隆/快照）
CREATE TABLE IF NOT EXISTS slice_refs (
    slice_id   BIGINT       PRIMARY KEY,
    size       INTEGER     NOT NULL,
    refs       INTEGER     NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT chk_slice_refs_id CHECK (slice_id > 0),
    CONSTRAINT chk_slice_refs_size CHECK (size > 0)
);

-- 4. 垃圾回收部分索引（refs <= 0 时快速扫描待回收物理切片）
CREATE INDEX IF NOT EXISTS idx_slice_refs_garbage ON slice_refs(slice_id) WHERE refs <= 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_slice_refs_garbage;
DROP TABLE IF EXISTS slice_refs;
DROP TABLE IF EXISTS chunks;
DROP SEQUENCE IF EXISTS slice_id_seq;
-- +goose StatementEnd
