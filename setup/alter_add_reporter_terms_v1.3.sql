-- =============================================================================
-- Migration: NIH RePORTER project terms (v1.3)
-- =============================================================================
-- Adds two columns to grant_reporter_project for the NIH-curated keyword
-- vocabulary RePORTER returns alongside the abstract:
--   - project_terms — RePORTER `terms`, angle-bracket-wrapped (<a><b><c>)
--   - pref_terms    — RePORTER `pref_terms`, semicolon-delimited (a;b;c)
--
-- Stored raw, verbatim from the API. Parsing into a keyword array happens
-- downstream in the Scholars-Profile-System ETL (issue #291); reciterdb keeps
-- the unparsed strings so a future reciterdb-side consumer can re-parse.
--
-- WHY AN ALTER, NOT THE CREATE TABLE in v1.2:
--   alter_add_reporter_fields_v1.2.sql creates grant_reporter_project with
--   CREATE TABLE IF NOT EXISTS — a no-op once the table exists, so editing its
--   body would not add columns to a live table. This file uses the
--   information_schema-guarded ALTER idiom (cf. v1.1) so it is safe on a
--   populated prod/dev table. The two columns were also added to v1.2's
--   CREATE TABLE so a fresh build matches.
--
-- Safe to run on prod and dev. Idempotent (information_schema guard; no-op on
-- re-run). No AFTER clause — keeps ALGORITHM=INSTANT eligible.
--
-- Run BEFORE deploying the updated retrieveReporter.py, otherwise the project
-- INSERT will fail with "Unknown column" on the 2 new fields.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- grant_reporter_project: + project_terms + pref_terms
-- -----------------------------------------------------------------------------

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'grant_reporter_project'
       AND column_name = 'project_terms') = 0,
    'ALTER TABLE grant_reporter_project ADD COLUMN `project_terms` text DEFAULT NULL',
    'SELECT ''grant_reporter_project.project_terms already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'grant_reporter_project'
       AND column_name = 'pref_terms') = 0,
    'ALTER TABLE grant_reporter_project ADD COLUMN `pref_terms` text DEFAULT NULL',
    'SELECT ''grant_reporter_project.pref_terms already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'grant_reporter_project'
  AND column_name IN ('project_terms', 'pref_terms')
ORDER BY ordinal_position;
