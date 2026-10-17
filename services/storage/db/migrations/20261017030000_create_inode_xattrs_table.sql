-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS inode_xattrs (
    ino BIGINT NOT NULL,
    name BYTEA NOT NULL,
    value BYTEA NOT NULL,
    CONSTRAINT pk_inode_xattrs PRIMARY KEY (ino, name),
    CONSTRAINT fk_inode_xattrs_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE CASCADE,
    CONSTRAINT chk_inode_xattrs_name CHECK (
        octet_length(name) BETWEEN 1 AND 255
        AND position('\x00'::bytea IN name) = 0
    ),
    CONSTRAINT chk_inode_xattrs_value CHECK (
        octet_length(value) <= 65536
    )
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS inode_xattrs;
-- +goose StatementEnd
