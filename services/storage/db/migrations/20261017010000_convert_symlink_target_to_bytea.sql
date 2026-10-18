-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION pg_temp.decode_symlink_target_to_bytea(t text) RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res bytea := ''::bytea;
    i int;
    ch text;
    ch_code int;
BEGIN
    IF t IS NULL THEN
        RETURN NULL;
    END IF;
    IF t !~ '[\uE000-\uE0FF]' THEN
        RETURN convert_to(t, 'UTF8');
    END IF;

    FOR i IN 1 .. char_length(t) LOOP
        ch := substr(t, i, 1);
        ch_code := ascii(ch);
        IF ch_code BETWEEN 57344 AND 57599 THEN
            res := res || set_byte('\x00'::bytea, 0, ch_code - 57344);
        ELSE
            res := res || convert_to(ch, 'UTF8');
        END IF;
    END LOOP;
    RETURN res;
END;
$$;

ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;

ALTER TABLE inodes
    ALTER COLUMN symlink_target TYPE BYTEA USING pg_temp.decode_symlink_target_to_bytea(symlink_target);

-- 将旧版允许的 4096 字节符号链接目标截断收敛至 4095 字节（PATH_MAX - 1），避免存量合法数据导致迁移失败
UPDATE inodes
SET symlink_target = substring(symlink_target FROM 1 FOR 4095),
    size = LEAST(size, 4095)
WHERE (mode & 61440) = 40960
  AND symlink_target IS NOT NULL
  AND octet_length(symlink_target) > 4095;

ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    CASE
        WHEN (mode & 61440) = 40960 THEN
            symlink_target IS NOT NULL
            AND octet_length(symlink_target) BETWEEN 1 AND 4095
            AND position('\x00'::bytea IN symlink_target) = 0
        ELSE
            symlink_target IS NULL
    END
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION pg_temp.encode_symlink_target_to_text(b bytea) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res text;
    i int;
    byte_val int;
BEGIN
    IF b IS NULL THEN
        RETURN NULL;
    END IF;
    BEGIN
        res := convert_from(b, 'UTF8');
        IF res !~ '[\uE000-\uE0FF]' THEN
            RETURN res;
        END IF;
    EXCEPTION WHEN character_not_in_repertoire THEN
        NULL;
    END;

    res := '';
    FOR i IN 0 .. octet_length(b) - 1 LOOP
        byte_val := get_byte(b, i);
        IF byte_val BETWEEN 1 AND 127 THEN
            res := res || chr(byte_val);
        ELSE
            res := res || chr(57344 + byte_val);
        END IF;
    END LOOP;
    WHILE octet_length(res) > 4096 LOOP
        res := left(res, char_length(res) - GREATEST(1, (octet_length(res) - 4096) / 3));
    END LOOP;
    RETURN res;
END;
$$;

ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;

ALTER TABLE inodes
    ALTER COLUMN symlink_target TYPE TEXT USING pg_temp.encode_symlink_target_to_text(symlink_target);

ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
    ((mode & 61440) = 40960 AND symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096 AND octet_length(symlink_target) BETWEEN 1 AND 4096)
    OR
    ((mode & 61440) <> 40960 AND symlink_target IS NULL)
);
-- +goose StatementEnd
