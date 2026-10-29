-- =============================================================================
-- Migration: authorship_review source ENUM gains 'openalex' (v3.2)
-- =============================================================================
-- Extends authorship_review.source from ENUM('pubmed','scopus') to
-- ENUM('pubmed','scopus','openalex') so the OpenAlex lane
-- (update/aar_universe_openalex.py) can write WCM conference abstracts and
-- preprints that neither PubMed nor the Scopus lane finds. Those rows look like
-- Scopus rows: pmid NULL, external_id = the OpenAlex work id ('W…', which
-- Publication Manager prefixes with OPENALEX: on Accept), pub_type = the OpenAlex
-- type ('conference-abstract' / 'preprint'), author_key =
-- `openalex:{doi-or-workid}:{position}`.
--
-- WHY THIS MIGRATION EXISTS:
--   An INSERT of source='openalex' into the old ENUM does not always fail. Under
--   strict SQL mode it errors; without it MySQL stores the empty string '' (ENUM
--   index 0) with only a warning, and that row then matches neither lane's queries
--   anywhere. aar_universe_openalex.run() refuses --apply until this column carries
--   'openalex', so the lane cannot write before this file is applied.
--
--   The fresh-build schema (setup/table_authorship_review.sql) and the DDL string in
--   update/aar_db.py are updated in the same PR. This migration brings EXISTING
--   databases up to that schema. It must be applied directly to BOTH reciterdb
--   instances, the producer instance and the separate dev instance behind
--   reciter-pm-dev, same as v1.6. Merging the PR does NOT run DDL.
--
-- ORDERING: the lane ships OFF (AAR_OPENALEX_LANE_CADENCE unset), so merging the code
--   before this DDL is harmless. Apply this BEFORE the cadence env is ever set, and
--   before Publication Manager is taught that 'openalex' is an external source (PM
--   today treats anything that is not 'scopus' as PubMed).
--
-- DURABILITY: authorship_review is curator state, not a reporting export. It is NOT
--   in update/updateReciterDB.py's truncate list and is not touched by any nightly ETL.
--
-- Appending a member to the END of an ENUM with the same storage size (still 1 byte)
-- is a metadata-only change in MySQL 8: no table rebuild, no existing row's stored
-- value changes ('pubmed'=1, 'scopus'=2 keep their indexes). Guarded by an
-- information_schema check (no-op on re-run), the v1.6/v2.5 form.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/alter_authorship_review_add_openalex_v3.2.sql
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- source ENUM('pubmed','scopus') -> ENUM('pubmed','scopus','openalex')
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'source' AND column_type LIKE '%''openalex''%') = 0,
    'ALTER TABLE authorship_review MODIFY COLUMN `source` ENUM(''pubmed'',''scopus'',''openalex'') NOT NULL DEFAULT ''pubmed''',
    'SELECT ''authorship_review.source already carries openalex'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification: column_type should read enum('pubmed','scopus','openalex'), and the
-- per-source counts should match the pre-migration counts exactly.
-- -----------------------------------------------------------------------------
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name = 'source';

SELECT source, COUNT(*) AS n FROM authorship_review GROUP BY source;
