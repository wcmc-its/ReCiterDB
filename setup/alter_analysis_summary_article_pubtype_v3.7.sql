-- =============================================================================
-- Migration: Reports canonical type, normalized dates, book fields (v3.7)
-- =============================================================================
-- Phase 5c (PM #1105). Adds to analysis_summary_article, all NULL:
--   pubType          VARCHAR(40)   one of PM's 18 types (src/lib/pubTypes.ts), via pub_type_map
--   pubTypeGroup     VARCHAR(16)   journal / conf / book / other
--   pubTypeCounted   TINYINT       0 for errata, retractions and out-of-scope types, else 1
--   pubDate          DATE          publication date (YYYY -> Jan 1, YYYY-MM -> the 1st)
--   pubDatePrecision ENUM('year','month','day')
--   dateAdded        DATE          PubMed rows: datePublicationAddedToEntrez; external rows:
--                                  external_article.date_added (when a curator added it)
--   publisher        VARCHAR(255)  external rows: external_article.publisher (v3.5)
--   isbn             VARCHAR(128)  external rows: external_article.isbn (v3.5)
--   editors          TEXT          external rows: external_article.editors, a JSON array
-- and creates pub_type_map (source, source_type) -> pub_type, filled by
-- setup/seed_pub_type_map.sql (generated from pubTypes.ts by update/pub_type_map_seed.py).
--
-- publicationTypeCanonical is NOT changed: Scholars (SPS) reads it, so the new type lives
-- in its own column.
--
-- ORDERING: apply this file, then setup/seed_pub_type_map.sql, then reload
--   setup/populateAnalysisSummaryTables_v2.sql. STEP 1 builds the staging table with
--   CREATE TABLE ... LIKE analysis_summary_article, so the columns must exist on the live
--   table before the nightly that should fill them (same rule as v1.6 / v1.7). Merging the
--   PR first is harmless: STEP 3 and 6b check information_schema and skip the new fills
--   while the columns or pub_type_map are absent. The next nightly fills every row
--   (analysis_summary_* is rebuilt in full each night), so there is no backfill.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: every ALTER is guarded by
--   an information_schema check, so a re-run (or a run on a DB without the table) is a no-op.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_analysis_summary_article_pubtype_v3.7.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/seed_pub_type_map.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/populateAnalysisSummaryTables_v2.sql
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- pub_type_map. Durable lookup, never truncated by the nightly; reseeded from the seed file.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `pub_type_map` (
  `source`         VARCHAR(16) NOT NULL,   -- RECITER (PubMed rows) / SCOPUS / OPENALEX / CROSSREF
  `source_type`    VARCHAR(64) NOT NULL,   -- normalized raw type: trim, lower, [space_]+ -> '-'
  `pub_type`       VARCHAR(40) NULL,       -- NULL = not one of the 18 types (erratum, out of scope)
  `pub_type_group` VARCHAR(16) NULL,
  `counted`        TINYINT     NOT NULL DEFAULT 1,
  PRIMARY KEY (`source`, `source_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- analysis_summary_article columns, one guarded ALTER each.
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'pubType') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `pubType` VARCHAR(40) NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.pubType present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'pubTypeGroup') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `pubTypeGroup` VARCHAR(16) NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.pubTypeGroup present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'pubTypeCounted') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `pubTypeCounted` TINYINT NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.pubTypeCounted present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'pubDate') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `pubDate` DATE NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.pubDate present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'pubDatePrecision') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `pubDatePrecision` ENUM(''year'',''month'',''day'') NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.pubDatePrecision present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'dateAdded') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `dateAdded` DATE NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.dateAdded present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'publisher') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `publisher` VARCHAR(255) NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.publisher present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'isbn') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `isbn` VARCHAR(128) NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.isbn present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @db AND table_name = 'analysis_summary_article' AND column_name = 'editors') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `editors` TEXT NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article.editors present (or table absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification: 9 columns on analysis_summary_article, and the pub_type_map table.
-- -----------------------------------------------------------------------------
SELECT column_name, column_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'analysis_summary_article'
  AND column_name IN ('pubType', 'pubTypeGroup', 'pubTypeCounted', 'pubDate', 'pubDatePrecision',
                      'dateAdded', 'publisher', 'isbn', 'editors')
ORDER BY ordinal_position;

SELECT table_name FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'pub_type_map';
