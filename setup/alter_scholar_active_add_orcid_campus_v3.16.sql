-- =============================================================================
-- Migration: scholar_active.orcid + scholar_active.campus (v3.16)
-- =============================================================================
-- Scholars (Scholars-Profile-System) is where ORCID iDs are curated, for WCM today and
-- for Cornell Ithaca next (SPS plan 2026-11-11, section 6). v3.1 deliberately stored no
-- iD here; that plan reverses it, so reciterdb jobs can read the confirmed list from one
-- place and never from DynamoDB Identity, which still holds uncurated values. Adds:
--   orcid    CHAR(19) NULL. The iD Scholars holds as CONFIRMED for this person, bare
--            form 0000-0000-0000-0000. NULL = no confirmed iD. Set exactly where
--            has_confirmed_orcid = 1.
--   campus   VARCHAR(16) NOT NULL DEFAULT 'wcm'. 'wcm' | 'ithaca' (scholar.campus).
--            Every row is 'wcm' until SPS exports Ithaca rows, and SPS must not do that
--            until each reader below filters on it.
--
-- READERS that treat every row as an active WCM scholar (add campus = 'wcm' to each
-- before Ithaca rows are exported): update/authorIdSuggestions.py, update/importHarvest.py,
-- and Publication Manager's manageProfile.controller.ts.
--
-- BEHAVIOUR CHANGE: none by itself. No reciterdb code reads the new columns yet, and
--   SPS's export fills `orcid` only once it sees the column.
-- ORDERING: either order works. The SPS export checks information_schema and writes the
--   v3.1 column list when these columns are absent.
-- GRANTS: none new. The SPS ETL user's SELECT, INSERT, DELETE on the table cover it.
-- Idempotent: each ALTER runs only when its column is missing. Nothing is dropped.
--   scholar_active is ~10k rows, so the ALTERs are instant. Prod only matters: SPS
--   writes this table in prod only.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_scholar_active_add_orcid_campus_v3.16.sql
--
-- Rollback (after reverting the SPS export to the v3.1 column list, or it will keep
-- working unchanged: it re-checks the columns each run):
--   ALTER TABLE scholar_active DROP COLUMN orcid, DROP COLUMN campus;
-- =============================================================================

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'scholar_active') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'scholar_active'
            AND column_name = 'orcid') = 0,
    'ALTER TABLE scholar_active ADD COLUMN `orcid` CHAR(19) NULL DEFAULT NULL AFTER `has_confirmed_orcid`',
    'SELECT ''scholar_active already has orcid (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'scholar_active') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'scholar_active'
            AND column_name = 'campus') = 0,
    'ALTER TABLE scholar_active ADD COLUMN `campus` VARCHAR(16) NOT NULL DEFAULT ''wcm'' AFTER `orcid`',
    'SELECT ''scholar_active already has campus (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'scholar_active'
ORDER BY ordinal_position;
