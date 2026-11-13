-- =============================================================================
-- Migration: bibliometric report v2 flags and cohort cells (v3.11, part 2 of 2: plan PR R2)
-- =============================================================================
-- PM #1105 (5h, bibliometric report v2). Part 1 (R1, setup/alter_analysis_nih_has_flags_v3.11.sql)
-- added analysis_nih.has_percentile / has_rcr. This part adds the columns and table that
-- populateAnalysisSummaryTables_v2 fills in STEPs 2v2, 3v2, 5v2 and 6v2. The rules live here
-- (in the SP), and PM reads them; PM never re-derives them from raw fields.
--
-- analysis_summary_article, all NULL by default:
--   provisionalNIH    TINYINT       iCite provisional: 1 = 'True', 0 = 'False', NULL = no iCite row
--   hasPercentileNIH  TINYINT       1 = iCite returned a percentile (0.0 included), 0 = missing,
--                                   NULL = no iCite row. Falls back to nih_percentile > 0 while
--                                   analysis_nih.has_percentile is absent or NULL.
--   pctExact          DECIMAL(5,2)  ROUND(nih_percentile, 2) as an exact decimal; NULL when missing
--   researchArticleV2 TINYINT       pmid > 0, iCite research article, ReCiter type not a preprint,
--                                   review, letter, comment, editorial, conference paper, erratum
--                                   or retraction, and counted (pubTypeCounted <> 0)
--   ratedV2           TINYINT       research article, year >= 1980, has a percentile, not provisional
--   recentV2          TINYINT       counted, not rated, and provisional or (no percentile and pubDate
--                                   within 24 months before the nightly run date)
-- analysis_summary_author, all NULL by default:
--   authorRoleV2      ENUM('first','senior','middle','not_set')  sole author or last -> senior;
--                                   first -> first; other PubMed rows -> middle; external rows with
--                                   no position -> not_set. The v1 authorPosition is unchanged.
--   soleAuthor        TINYINT       1 when the article has one author (PubMed rows only)
-- analysis_summary_person_peer (new): one row per person x role set x N (5, 10) for everyone
--   with at least one rated research article. Rebuilt and swapped with the other
--   analysis_summary_* tables every night (STEP 7), so subject and cohort share one snapshot.
--
-- ORDERING: apply R1's file and this file, then reload setup/populateAnalysisSummaryTables_v2.sql.
--   STEP 1 builds the staging tables with CREATE TABLE ... LIKE, so the columns must exist on the
--   live tables before the nightly that should fill them. Merging the PR first is harmless: the
--   v2 steps check information_schema and skip while the columns or the table are absent. The
--   next nightly fills every row (analysis_summary_* is rebuilt in full each night): no backfill.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: each ALTER adds only the
--   missing columns (per information_schema), the table is CREATE TABLE IF NOT EXISTS, so a
--   re-run (or a run without the tables) is a no-op. Nothing is dropped or truncated.
--
-- LOCKING: analysis_summary_article carries FULLTEXT indexes, so MariaDB rebuilds it INPLACE
--   under LOCK=SHARED (reads keep working, writes wait); v3.7 took about 4-5 minutes on dev.
--   analysis_summary_author has no FULLTEXT index and takes the columns INSTANT. Apply OUTSIDE
--   the reciterdb nightly window (cron 14:30 UTC, about 2.5 hours).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_analysis_nih_has_flags_v3.11.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_analysis_summary_bibliometric_v2_v3.11.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/populateAnalysisSummaryTables_v2.sql
--
-- Rollback: reapply the previous populateAnalysisSummaryTables_v2.sql to stop writing the v2
--   columns and table. The columns and the table can stay (nothing else reads them).
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- analysis_summary_article: one guarded ALTER (one rebuild at most).
-- -----------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS v311_article_cols;
CREATE TEMPORARY TABLE v311_article_cols (ord INT PRIMARY KEY, name VARCHAR(64), ddl VARCHAR(200));
INSERT INTO v311_article_cols VALUES
  (1, 'provisionalNIH',    'ADD COLUMN `provisionalNIH` TINYINT NULL DEFAULT NULL'),
  (2, 'hasPercentileNIH',  'ADD COLUMN `hasPercentileNIH` TINYINT NULL DEFAULT NULL'),
  (3, 'pctExact',          'ADD COLUMN `pctExact` DECIMAL(5,2) NULL DEFAULT NULL'),
  (4, 'researchArticleV2', 'ADD COLUMN `researchArticleV2` TINYINT NULL DEFAULT NULL'),
  (5, 'ratedV2',           'ADD COLUMN `ratedV2` TINYINT NULL DEFAULT NULL'),
  (6, 'recentV2',          'ADD COLUMN `recentV2` TINYINT NULL DEFAULT NULL');

SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v311_article_cols c
             WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = 'analysis_summary_article'
                                 AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_summary_article ', @adds),
    'SELECT ''analysis_summary_article already has the v3.11 columns (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
DROP TEMPORARY TABLE IF EXISTS v311_article_cols;

-- -----------------------------------------------------------------------------
-- analysis_summary_author: one guarded ALTER.
-- -----------------------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS v311_author_cols;
CREATE TEMPORARY TABLE v311_author_cols (ord INT PRIMARY KEY, name VARCHAR(64), ddl VARCHAR(200));
INSERT INTO v311_author_cols VALUES
  (1, 'authorRoleV2', 'ADD COLUMN `authorRoleV2` ENUM(''first'',''senior'',''middle'',''not_set'') NULL DEFAULT NULL'),
  (2, 'soleAuthor',   'ADD COLUMN `soleAuthor` TINYINT NULL DEFAULT NULL');

SET @adds = (SELECT GROUP_CONCAT(c.ddl ORDER BY c.ord SEPARATOR ', ')
             FROM v311_author_cols c
             WHERE NOT EXISTS (SELECT 1 FROM information_schema.columns k
                               WHERE k.table_schema = @db AND k.table_name = 'analysis_summary_author'
                                 AND k.column_name = c.name));
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @db AND table_name = 'analysis_summary_author') = 1
    AND @adds IS NOT NULL,
    CONCAT('ALTER TABLE analysis_summary_author ', @adds),
    'SELECT ''analysis_summary_author already has the v3.11 columns (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
DROP TEMPORARY TABLE IF EXISTS v311_author_cols;

-- -----------------------------------------------------------------------------
-- analysis_summary_person_peer: the v2 cohort cells. Columns:
--   facultyRank     the rank the cells were ranked in (same labels as analysis_summary_person):
--                   the single rank code; with two codes, the one identity.primaryTitle names,
--                   else the most senior. NULL = no faculty rank code.
--   cohortEligible  1 = academic-faculty-weillfulltime plus a faculty rank code
--   roleSet / topN  any, first_or_senior, first, senior x 5, 10
--   ratedCount      rated research articles in the role set
--   recentCount     research articles in the role set that are recent (too recent to rate)
--   avgTop          exact mean of the topN highest pctExact (ROW_NUMBER over pct, RCR, pmid);
--                   NULL when ratedCount < topN
--   cohortM         eligible members of the same rank with an avgTop in this cell (subject
--                   included); NULL for a non-eligible or non-qualifying row
--   cohortRank      RANK() over avgTop within the cohort; NULL when cohortM < 20
--   runDate         the nightly run date; iciteLoadedAt: analysis_nih CREATE_TIME at the run
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `analysis_summary_person_peer` (
  `personIdentifier` varchar(128) NOT NULL,
  `facultyRank` varchar(200) DEFAULT NULL,
  `cohortEligible` tinyint NOT NULL DEFAULT 0,
  `roleSet` enum('any','first_or_senior','first','senior') NOT NULL,
  `topN` tinyint NOT NULL,
  `ratedCount` int(11) NOT NULL DEFAULT 0,
  `recentCount` int(11) NOT NULL DEFAULT 0,
  `avgTop` decimal(6,3) DEFAULT NULL,
  `cohortRank` int(11) DEFAULT NULL,
  `cohortM` int(11) DEFAULT NULL,
  `runDate` date NOT NULL,
  `iciteLoadedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`personIdentifier`, `roleSet`, `topN`),
  KEY `idx_peer_cohort` (`facultyRank`, `roleSet`, `topN`, `avgTop`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification: 6 + 2 columns, and the table.
-- -----------------------------------------------------------------------------
SELECT table_name, column_name, column_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND ((table_name = 'analysis_summary_article'
        AND column_name IN ('provisionalNIH', 'hasPercentileNIH', 'pctExact', 'researchArticleV2', 'ratedV2', 'recentV2'))
    OR (table_name = 'analysis_summary_author' AND column_name IN ('authorRoleV2', 'soleAuthor')))
ORDER BY table_name, ordinal_position;

SELECT table_name FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'analysis_summary_person_peer';
