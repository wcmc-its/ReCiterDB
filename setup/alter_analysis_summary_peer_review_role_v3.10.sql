-- =============================================================================
-- Migration: Reports peer review and role defaults (v3.10)
-- =============================================================================
-- Phase 5d-1 (PM #1105). Adds, all NULL:
--   analysis_summary_article.peerReviewed        TINYINT  1 / 0, the type default from PM's
--                                                          pubTypes.ts defaultPeerReviewed()
--   analysis_summary_article.peerReviewedSource  ENUM('type-default','curator')
--                                                          'type-default' for every row until 5d-2
--                                                          reads curator overrides from external_record
--   analysis_summary_author.role                 VARCHAR(20)  pubTypes.ts defaultRole(): 'Editor' for
--                                                          Edited Book and Conference Proceedings, else 'Author'
-- Filled by STEP 6c of populateAnalysisSummaryTables_v2 from the v3.7 pubType column.
--
-- Needs v3.7 (pubType) for the fill. Without v3.7 the SP skips STEP 6c and the columns stay NULL.
--
-- ORDERING: apply this file, then reload setup/populateAnalysisSummaryTables_v2.sql. STEP 1
--   builds the staging tables with CREATE TABLE ... LIKE, so the columns must exist on the live
--   tables before the nightly that should fill them (same rule as v1.6 / v1.7 / v3.7). Merging
--   the PR first is harmless: STEP 6c checks information_schema and skips while the columns are
--   absent. The next nightly fills every row (analysis_summary_* is rebuilt in full each night),
--   so there is no backfill.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: each ALTER adds only the
--   missing columns (per information_schema), so a re-run (or a run without the table) is a
--   no-op. Nothing is dropped or rewritten.
--
-- LOCKING: analysis_summary_article carries FULLTEXT indexes (ft_title, ft_journal), so MariaDB
--   rebuilds it INPLACE under LOCK=SHARED (reads keep working, writes wait); v3.7's rebuild took
--   about 4-5 minutes on dev. analysis_summary_author has no FULLTEXT index. Apply OUTSIDE the
--   reciterdb nightly window (cron 14:30 UTC, about 2.5 hours): STEP 7's RENAME would otherwise
--   queue behind the rebuild.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_analysis_summary_peer_review_role_v3.10.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/populateAnalysisSummaryTables_v2.sql
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- analysis_summary_article: ONE guarded ALTER for whichever of the two columns are missing,
-- so the FULLTEXT-bearing table is rebuilt at most once.
-- -----------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS v310_cols;
CREATE TEMPORARY TABLE v310_cols (ord INT PRIMARY KEY, tbl VARCHAR(64), name VARCHAR(64), ddl VARCHAR(200));
INSERT INTO v310_cols VALUES
  (1, 'analysis_summary_article', 'peerReviewed',       'ADD COLUMN `peerReviewed` TINYINT NULL DEFAULT NULL'),
  (2, 'analysis_summary_article', 'peerReviewedSource', 'ADD COLUMN `peerReviewedSource` ENUM(''type-default'',''curator'') NULL DEFAULT NULL'),
  (3, 'analysis_summary_author',  'role',               'ADD COLUMN `role` VARCHAR(20) NULL DEFAULT NULL');

SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v310_cols c
             WHERE c.tbl = 'analysis_summary_article'
               AND NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = c.tbl AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_summary_article ', @adds),
    'SELECT ''analysis_summary_article already has the v3.10 columns (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- analysis_summary_author.role
-- -----------------------------------------------------------------------------
SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v310_cols c
             WHERE c.tbl = 'analysis_summary_author'
               AND NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = c.tbl AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_author') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_summary_author ', @adds),
    'SELECT ''analysis_summary_author already has the v3.10 column (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
DROP TEMPORARY TABLE IF EXISTS v310_cols;

-- -----------------------------------------------------------------------------
-- Verification: 2 columns on analysis_summary_article, 1 on analysis_summary_author.
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, column_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND ((table_name = 'analysis_summary_article' AND column_name IN ('peerReviewed', 'peerReviewedSource'))
    OR (table_name = 'analysis_summary_author' AND column_name = 'role'))
ORDER BY table_name, ordinal_position;
