-- =============================================================================
-- Migration: external_article source_type ENUM gains 'CROSSREF' (v3.4)
-- =============================================================================
-- Extends external_article.source_type from ENUM('SCOPUS','WOS','OPENALEX') to
-- ENUM('SCOPUS','WOS','OPENALEX','CROSSREF') so a DOI a curator adds from Crossref
-- (Publication Manager #1060, a paper Scopus and OpenAlex don't have yet) projects into
-- reciterdb. Those rows have article_id = 'CROSSREF:' + the lowercased DOI, slashes kept
-- (e.g. 'CROSSREF:10.1016/j.surg.2026.110815'), at most 96 chars; ReCiter rejects longer
-- ids, so no column is widened. A CROSSREF: paper stays CROSSREF: (never migrated to
-- OPENALEX:); the add-time duplicate check is what keeps one paper to one id.
--
-- WHY THIS MIGRATION EXISTS:
--   update/retrieveExternalArticles.py runs CREATE TABLE IF NOT EXISTS, which never
--   alters an existing table. Without this file the live ENUM lacks 'CROSSREF': under
--   strict SQL mode the insert would error, and without it MySQL would store '' (ENUM
--   index 0). The same PR makes the ETL read the live ENUM and skip (and log) rows it
--   can't hold, so before this DDL CROSSREF rows are left out rather than emptying the
--   table. After it they load, and STEP 6b carries source_type 'CROSSREF' into
--   analysis_summary_article.source_type (varchar(16), no change needed).
--
--   The fresh-build schema (setup/table_external_article.sql) and the CREATE_SQL string
--   in update/retrieveExternalArticles.py are updated in the same PR. This migration
--   brings EXISTING databases up to that schema. Apply it directly to BOTH reciterdb
--   instances, the producer instance and the separate dev instance behind
--   reciter-pm-dev. Merging the PR does NOT run DDL.
--
-- ORDERING: apply this BEFORE ReCiter accepts CROSSREF: ids and before Publication
--   Manager shows the Crossref Add button, so the first Crossref add projects the
--   next night. Merging this repo's code before the DDL is harmless (the ETL skips
--   CROSSREF rows until then).
--
-- DURABILITY: external_article is a rebuildable projection (TRUNCATE + reload nightly),
--   so a CROSSREF row skipped before this DDL simply loads on the next run.
--
-- Appending a member to the END of an ENUM with the same storage size (still 1 byte)
-- is a metadata-only change in MySQL 8: no table rebuild, no existing row's stored
-- value changes ('SCOPUS'=1, 'WOS'=2, 'OPENALEX'=3 keep their indexes). Guarded by an
-- information_schema check (no-op on re-run, and on an env without the table).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/alter_external_article_add_crossref_v3.4.sql
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- source_type ENUM('SCOPUS','WOS','OPENALEX') -> ENUM('SCOPUS','WOS','OPENALEX','CROSSREF')
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'external_article'
       AND column_name = 'source_type' AND column_type NOT LIKE '%''CROSSREF''%') = 1,
    'ALTER TABLE external_article MODIFY COLUMN `source_type` ENUM(''SCOPUS'',''WOS'',''OPENALEX'',''CROSSREF'') NOT NULL',
    'SELECT ''external_article.source_type already carries CROSSREF (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification: column_type should read enum('SCOPUS','WOS','OPENALEX','CROSSREF'),
-- and the per-source counts should match the pre-migration counts exactly.
-- -----------------------------------------------------------------------------
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'external_article'
  AND column_name = 'source_type';

SELECT source_type, COUNT(*) AS n FROM external_article GROUP BY source_type;
