-- ------------------------------------
-- V13__normalize_ligainsider_team_urls.sql
--
-- Normalisiert in dim_team LigaInsider-URLs auf Team-Rootseiten
-- (ohne /kader/) fuer konsistente Referenzen.
-- ------------------------------------

UPDATE kickbase_raw.dim_team
SET ligainsider_team_url = regexp_replace(ligainsider_team_url, '/kader/?$', '/')
WHERE ligainsider_team_url IS NOT NULL
  AND ligainsider_team_url ~ '/kader/?$';
