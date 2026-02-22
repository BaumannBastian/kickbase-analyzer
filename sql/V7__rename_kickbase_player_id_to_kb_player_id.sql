-- ------------------------------------
-- V7__rename_kickbase_player_id_to_kb_player_id.sql
--
-- Harmonisiert die Benennung im RAW-Star-Schema:
-- - dim_player.kickbase_player_id -> dim_player.kb_player_id
-- - Indexname auf kb_player_id angepasst
-- ------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'kickbase_player_id'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'kb_player_id'
    ) THEN
        EXECUTE 'ALTER TABLE kickbase_raw.dim_player RENAME COLUMN kickbase_player_id TO kb_player_id';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'kickbase_raw'
          AND tablename = 'dim_player'
          AND indexname = 'idx_dim_player_kickbase_player_id'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'kickbase_raw'
          AND tablename = 'dim_player'
          AND indexname = 'idx_dim_player_kb_player_id'
    ) THEN
        EXECUTE 'ALTER INDEX kickbase_raw.idx_dim_player_kickbase_player_id RENAME TO idx_dim_player_kb_player_id';
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_dim_player_kb_player_id
    ON kickbase_raw.dim_player (kb_player_id);
