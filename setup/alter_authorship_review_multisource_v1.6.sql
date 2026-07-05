-- =============================================================================
-- Migration: authorship_review multi-source (v1.6)
-- =============================================================================
-- Extends authorship_review from a PubMed-only queue to a multi-source one so
-- the new Scopus detector lane (ReCiterDB#103 / PM#775) can write authorships
-- found on documents NOT in PubMed. Adds:
--   - source        ENUM('pubmed','scopus') NOT NULL DEFAULT 'pubmed' (+ index)
--   - external_id   VARCHAR(96)  NULL  — DOI-first, numeric Scopus ID fallback
--                                        (EIDs are unstable across record merges)
--   - pub_type      VARCHAR(40)  NULL  — Article / Book Chapter / Conference Paper…
--   - container_id  VARCHAR(96)  NULL  — book base DOI (chapter → book), enables
--                                        the tab's "N chapters in this book" runs
-- and relaxes two existing columns:
--   - pmid          BIGINT NOT NULL → NULL   (Scopus-only rows have no PMID)
--   - author_key    VARCHAR(32) → VARCHAR(160) (Scopus keys are longer:
--                                        `scopus:{doi-or-scopusid}:{position}`)
--
-- WHY THIS MIGRATION EXISTS:
--   The producer (ReCiter Research scripts/aar_db.py) upserts by author_key.
--   PubMed rows keep `{pmid}:{position}`; Scopus rows use
--   `scopus:{doi-or-scopusid}:{position}` — up to ~160 chars, and carry no PMID.
--   Without these changes a Scopus upsert fails on the NOT NULL pmid, the 32-char
--   author_key truncation, and the missing source/external_id/pub_type/container_id
--   columns. author_key stays the UNIQUE upsert identity (uq_author_key).
--
--   The fresh-build schema (setup/table_authorship_review.sql) is updated in the
--   same PR to define the table WITH these columns, so new databases are fine.
--   This migration brings EXISTING databases up to that schema. It must be applied
--   directly to BOTH reciterdb instances — the producer instance and the separate
--   dev instance behind reciter-pm-dev (loaded manually) — before the Scopus
--   detector or the PM Scopus tab runs against them. Merging the PR does NOT run DDL.
--
-- DURABILITY: authorship_review is curator state, not a reporting export. It is NOT
--   in update/updateReciterDB.py's truncate list (`all_tables`) and is not touched
--   by any nightly stored procedure or ETL step, so these columns persist across
--   nightly reload. (Verified: the table name appears only in its own CREATE file.)
--
-- Safe to run on prod and dev. Every statement is guarded by an information_schema
-- check (no-op on re-run). Existing rows get source='pubmed' via the column default;
-- no existing row's data is altered. Additive/relaxing only — nothing is dropped or
-- narrowed. Run BEFORE the Scopus detector or the PM Scopus tab hits this database.
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- source ENUM('pubmed','scopus') NOT NULL DEFAULT 'pubmed'
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'source') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `source` ENUM(''pubmed'',''scopus'') NOT NULL DEFAULT ''pubmed''',
    'SELECT ''authorship_review.source already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- external_id VARCHAR(96) NULL — DOI-first, numeric Scopus ID fallback
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'external_id') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `external_id` VARCHAR(96) NULL',
    'SELECT ''authorship_review.external_id already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- pub_type VARCHAR(40) NULL — Scopus subtypeDescription (drives type filter)
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'pub_type') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `pub_type` VARCHAR(40) NULL',
    'SELECT ''authorship_review.pub_type already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- container_id VARCHAR(96) NULL — book base DOI (enables per-book chapter runs)
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'container_id') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `container_id` VARCHAR(96) NULL',
    'SELECT ''authorship_review.container_id already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- pmid BIGINT NOT NULL -> NULL (Scopus-only rows have no PMID)
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'pmid' AND is_nullable = 'NO') = 1,
    'ALTER TABLE authorship_review MODIFY COLUMN `pmid` BIGINT NULL',
    'SELECT ''authorship_review.pmid already nullable'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- author_key VARCHAR(32) -> VARCHAR(160) (Scopus keys are longer)
--   VARCHAR(160) utf8mb4 = 640 bytes < 3072-byte InnoDB index prefix limit, so
--   the uq_author_key UNIQUE index is unaffected by the widen.
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'author_key' AND character_maximum_length < 160) = 1,
    'ALTER TABLE authorship_review MODIFY COLUMN `author_key` VARCHAR(160) NOT NULL',
    'SELECT ''authorship_review.author_key already >= 160'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- ix_source index on source
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.statistics
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND index_name = 'ix_source') = 0,
    'ALTER TABLE authorship_review ADD INDEX `ix_source` (`source`)',
    'SELECT ''authorship_review.ix_source already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name IN ('source', 'external_id', 'pub_type', 'container_id', 'pmid', 'author_key')
ORDER BY ordinal_position;
