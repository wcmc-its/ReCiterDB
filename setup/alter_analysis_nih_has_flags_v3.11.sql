-- =============================================================================
-- Migration: analysis_nih has_percentile / has_rcr (v3.11, part 1 of 2: plan PR R1)
-- =============================================================================
-- PM #1105 (5h, bibliometric report v2). Adds to analysis_nih, both NULL by default:
--   has_percentile  TINYINT  1 = iCite returned an nih_percentile (0.0 included), 0 = null
--   has_rcr         TINYINT  1 = iCite returned a relative_citation_ratio, 0 = null
--
-- Why: update/retrieveNIH.py writes a missing iCite value as '' and LOAD DATA stores it as 0,
-- so nih_percentile 0.00 cannot tell "missing" from "genuinely zero". The loader keeps writing
-- 0 there (v1 bibliometric report, SPS and every other reader are unchanged) and records which
-- it was in these flags. NULL = a row loaded before the columns existed (unknown).
-- Part 2 of v3.11 (plan PR R2) adds the analysis_summary_* columns that copy the flag.
--
-- ORDERING: retrieveNIH.py builds analysis_nih_new with CREATE TABLE ... LIKE analysis_nih, so
--   the columns must exist on the live table before the nightly that should fill them. Merging
--   the PR first is harmless: without the columns the loader logs a warning and loads the old
--   18 columns. The next successful nightly load fills every row (the table is rebuilt in full
--   each night), so there is no backfill.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: the one ALTER adds only the
--   missing columns (per information_schema), so a re-run, or a run without the table, is a
--   no-op. analysis_nih has no FULLTEXT index, so MariaDB adds trailing nullable columns
--   INSTANT (metadata only); it is still best applied outside the nightly window (14:30 UTC).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_analysis_nih_has_flags_v3.11.sql
--
-- Rollback: not needed (the columns are ignored by every reader). If required:
--   ALTER TABLE analysis_nih DROP COLUMN has_percentile, DROP COLUMN has_rcr;
-- =============================================================================

SET @db = DATABASE();

DROP TEMPORARY TABLE IF EXISTS v311_nih_cols;
CREATE TEMPORARY TABLE v311_nih_cols (ord INT PRIMARY KEY, name VARCHAR(64), ddl VARCHAR(200));
INSERT INTO v311_nih_cols VALUES
  (1, 'has_percentile', 'ADD COLUMN `has_percentile` TINYINT NULL DEFAULT NULL'),
  (2, 'has_rcr',        'ADD COLUMN `has_rcr` TINYINT NULL DEFAULT NULL');

SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v311_nih_cols c
             WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = 'analysis_nih'
                                 AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_nih') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_nih ', @adds),
    'SELECT ''analysis_nih already has has_percentile/has_rcr (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
DROP TEMPORARY TABLE IF EXISTS v311_nih_cols;

-- Verification: both columns on analysis_nih.
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'analysis_nih'
  AND column_name IN ('has_percentile', 'has_rcr')
ORDER BY ordinal_position;
