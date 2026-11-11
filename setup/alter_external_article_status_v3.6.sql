-- =============================================================================
-- Migration: external_article.status + MANUAL/ORCID sources (v3.6, plan R2)
-- =============================================================================
-- Companion to setup/alter_external_record_v3.6.sql (plan R1). Adds to the
-- external_article reporting projection:
--   status       the external_record lifecycle state of the row (NULL until the
--                nightly ETL rebuilds the projection from external_record)
--   source_type  gains 'MANUAL' and 'ORCID', the two sources external_record holds
--                that DynamoDB never did
--
-- WHY: update/retrieveExternalArticles.py now syncs DynamoDB ExternalArticle into
--   external_record and rebuilds external_article from it (PM #1104, plan R2).
--   `suppressed` keeps its meaning (status <> 'ACCEPTED'), so every existing reader
--   (STEP 6b, AAR duplicate lookups, reporting_external_abstracts) is unchanged.
--
-- ORDERING: harmless either way. The ETL writes only columns the live table has and
--   skips rows whose source_type the live ENUM lacks, so before this DDL it loads
--   exactly what it loads today. Apply after alter_external_record_v3.6.sql.
--
-- IDEMPOTENT: ADD COLUMN IF NOT EXISTS, and the ENUM change runs only while the
--   live ENUM lacks 'MANUAL'. A second run is a no-op.
--
-- Apply directly to BOTH reciterdb instances. Merging the PR does NOT run DDL.
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/alter_external_article_status_v3.6.sql
--
-- ROLLBACK: ALTER TABLE external_article DROP COLUMN status; and the ENUM back to
--   ('SCOPUS','WOS','OPENALEX','CROSSREF') once no MANUAL/ORCID row is loaded.
-- =============================================================================

SET @db = DATABASE();

ALTER TABLE external_article
  ADD COLUMN IF NOT EXISTS `status` ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL AFTER `superseded_by_pmid`;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'external_article'
       AND column_name = 'source_type' AND column_type NOT LIKE '%''MANUAL''%') = 1,
    'ALTER TABLE external_article MODIFY COLUMN `source_type` ENUM(''SCOPUS'',''WOS'',''OPENALEX'',''CROSSREF'',''MANUAL'',''ORCID'') NOT NULL',
    'SELECT ''external_article.source_type already carries MANUAL (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verify:
SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'external_article'
  AND column_name IN ('source_type', 'status');
