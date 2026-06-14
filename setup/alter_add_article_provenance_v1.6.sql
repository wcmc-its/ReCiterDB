-- =============================================================================
-- Migration: Add article_provenance table (v1.6)
-- =============================================================================
-- Creates the article_provenance table on EXISTING databases (dev, prod). Fresh
-- builds already get it from setup/createDatabaseTableReciterDb.sql (ReCiterDB#95).
--
-- WHAT IT IS:
--   First-retrieval provenance per (article, person), loaded nightly by
--   update/retrieveArticleProvenance.py from the ReCiter DynamoDB
--   `ArticleProvenance` table. That source table has a COMPOSITE key
--   uid (HASH, personIdentifier) + articleId (RANGE, PMID), so there is one item
--   per person+article. This table mirrors that key exactly -- PRIMARY KEY
--   (pmid, personIdentifier) -- rather than collapsing to one row per PMID.
--
--   Columns map from DynamoDB attributes:
--     pmid               <- articleId (String PMID -> INT)
--     personIdentifier   <- uid
--     firstRetrievalDate <- frd  (epoch SECONDS, UTC -> DATETIME)
--     retrievalStrategy  <- rs   (PM_UI_SEARCH, PM_AUTHOR, ...)
--     source             <- src  (PM, CTSC, GS, MAN, MAN_FROM_PM, ...)
--
-- WHY THIS MIGRATION EXISTS:
--   Publication Manager #737 displays "date a publication was first retrieved"
--   in /curate and reads this table by PMID. The table must exist before the PM
--   #737 branch is deployed against this database.
--
-- DURABILITY / ETL CONTRACT:
--   Loaded via a staging table (article_provenance_new) + atomic RENAME swap by
--   the nightly ETL, exactly like analysis_nih. It is NOT in
--   update/updateReciterDB.py's truncate list and is not touched by any nightly
--   stored procedure. A failure in the ETL step leaves production untouched and
--   does not block the rest of run_all.py (the step runs as non-fatal).
--
-- Safe to run on prod and dev. CREATE TABLE IF NOT EXISTS is a no-op on re-run
-- and additive only -- no existing table or row is modified.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `article_provenance` (
  `pmid`               int(11)      NOT NULL,
  `personIdentifier`   varchar(128) NOT NULL,
  `firstRetrievalDate` datetime     DEFAULT NULL,
  `retrievalStrategy`  varchar(64)  DEFAULT NULL,
  `source`             varchar(32)  DEFAULT NULL,
  PRIMARY KEY (`pmid`, `personIdentifier`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, column_name, data_type, character_maximum_length, is_nullable, column_key
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'article_provenance'
ORDER BY ordinal_position;
