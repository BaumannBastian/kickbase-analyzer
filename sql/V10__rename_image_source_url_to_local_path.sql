-- ------------------------------------
-- V10__rename_image_source_url_to_local_path.sql
--
-- Speichert in dim_player keinen Remote-URL-Verweis mehr,
-- sondern einen lokalen Dateipfad zum persistierten Spielerbild.
-- ------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'image_source_url'
    ) THEN
        ALTER TABLE kickbase_raw.dim_player
            RENAME COLUMN image_source_url TO image_local_path;
    END IF;
END $$;

UPDATE kickbase_raw.dim_player
SET image_local_path = NULL
WHERE image_local_path ~* '^https?://';
