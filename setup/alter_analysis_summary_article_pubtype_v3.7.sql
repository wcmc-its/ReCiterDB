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
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: the one ALTER adds only missing
--   columns (per information_schema), so a re-run (or a run without the table) is a no-op.
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
-- analysis_summary_article columns: ONE guarded ALTER that adds whichever of the nine
-- are missing, so the table is rebuilt at most once (a rebuild per column took minutes
-- each on the dev instance). No-op when all nine exist or the table is absent.
-- -----------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS v37_cols;
CREATE TEMPORARY TABLE v37_cols (ord INT PRIMARY KEY, name VARCHAR(64), ddl VARCHAR(200));
INSERT INTO v37_cols VALUES
  (1, 'pubType',          'ADD COLUMN `pubType` VARCHAR(40) NULL DEFAULT NULL'),
  (2, 'pubTypeGroup',     'ADD COLUMN `pubTypeGroup` VARCHAR(16) NULL DEFAULT NULL'),
  (3, 'pubTypeCounted',   'ADD COLUMN `pubTypeCounted` TINYINT NULL DEFAULT NULL'),
  (4, 'pubDate',          'ADD COLUMN `pubDate` DATE NULL DEFAULT NULL'),
  (5, 'pubDatePrecision', 'ADD COLUMN `pubDatePrecision` ENUM(''year'',''month'',''day'') NULL DEFAULT NULL'),
  (6, 'dateAdded',        'ADD COLUMN `dateAdded` DATE NULL DEFAULT NULL'),
  (7, 'publisher',        'ADD COLUMN `publisher` VARCHAR(255) NULL DEFAULT NULL'),
  (8, 'isbn',             'ADD COLUMN `isbn` VARCHAR(128) NULL DEFAULT NULL'),
  (9, 'editors',          'ADD COLUMN `editors` TEXT NULL DEFAULT NULL');

SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v37_cols c
             WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = 'analysis_summary_article'
                                 AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_summary_article ', @adds),
    'SELECT ''analysis_summary_article already has the v3.7 columns (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
DROP TEMPORARY TABLE IF EXISTS v37_cols;

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
