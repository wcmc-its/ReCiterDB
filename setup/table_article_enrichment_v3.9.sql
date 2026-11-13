-- =============================================================================
-- Migration: article_enrichment table + analysis_summary_article.citationCountOpenAlex (v3.9)
-- =============================================================================
-- Phase 5e (PM #1105). Enrichment for external-source articles (external_article:
-- SCOPUS:, OPENALEX:, CROSSREF:), which STEP 6b injects with no ISSN, volume/issue/pages,
-- citation count or PMCID.
--
--   article_enrichment   Durable, one row per external article_id. Written only by
--                        update/enrichArticles.py (OpenAlex works, Scopus Search, NCBI PMC
--                        ID converter). ISSN / volume / issue / pages are kept once found;
--                        citation counts are refreshed weekly; the PMCID lookup is retried
--                        monthly while empty.
--   analysis_summary_article.citationCountOpenAlex
--                        OpenAlex cited_by_count for external rows (STEP 6b). PubMed rows
--                        keep citationCountNIH / citationCountScopus; readers fall back to
--                        OpenAlex only when both are NULL.
--
-- STEP 6b joins article_enrichment (both SP copies): issn = COALESCE(review ISSN,
-- enrichment issn, enrichment eissn) before the external Scimago match, volume / issue /
-- pages / pmcid from enrichment, citationCountScopus from the Scopus count on SCOPUS rows
-- (plan D5), citationCountOpenAlex from OpenAlex. Every piece checks information_schema,
-- so the SP runs unchanged before this file is applied.
--
-- article_enrichment is NOT in update/updateReciterDB.py's `all_tables` truncate/swap
-- list and must never be added: truncating it would refetch every article.
--
-- ORDERING: apply this file, then reload setup/populateAnalysisSummaryTables_v2.sql.
--   STEP 1 builds the staging table with CREATE TABLE ... LIKE analysis_summary_article, so
--   citationCountOpenAlex must exist on the live table before the nightly that should fill
--   it (same rule as v1.6 / v1.7 / v3.7). Merging the PR first is harmless: the nightly
--   hook is off unless ARTICLE_ENRICHMENT is set on the CronJob, enrichArticles.py --apply
--   refuses until article_enrichment exists, and STEP 6b skips the join while the table or
--   column is absent.
--
-- LOCKING: analysis_summary_article carries FULLTEXT indexes, so MariaDB rebuilds it INPLACE
--   under LOCK=SHARED to add the column (reads keep working, writes wait; v3.7 took about
--   4-5 minutes on dev). Apply OUTSIDE the reciterdb nightly window (cron 14:30 UTC, about
--   2.5 hours), so STEP 7's RENAME doesn't queue behind the rebuild.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS, and the ALTER runs only when the column is
--   missing (per information_schema). Nothing is dropped or truncated. Apply directly to
--   BOTH reciterdb instances (the producer and the dev instance behind reciter-pm-dev).
--   Merging the PR does NOT run DDL.
--
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/table_article_enrichment_v3.9.sql
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/populateAnalysisSummaryTables_v2.sql
-- =============================================================================

SET @db = DATABASE();

CREATE TABLE IF NOT EXISTS `article_enrichment` (
  `article_id`              VARCHAR(96)  NOT NULL,   -- SCOPUS:… / OPENALEX:W… / CROSSREF:…, as in external_article
  `doi`                     VARCHAR(255) NULL,       -- the DOI the lookup used (lower-cased)
  `issn`                    VARCHAR(9)   NULL,       -- NNNN-NNNN: Scopus prism:issn, else OpenAlex issn_l
  `eissn`                   VARCHAR(9)   NULL,       -- NNNN-NNNN: Scopus prism:eIssn, else OpenAlex's other ISSN
  `volume`                  VARCHAR(100) NULL,
  `issue`                   VARCHAR(100) NULL,
  `pages`                   VARCHAR(100) NULL,       -- "first-last" or "first"
  `openalex_id`             VARCHAR(20)  NULL,       -- W…
  `openalex_created`        DATE         NULL,       -- OpenAlex created_date (plan D2 option b; not read yet)
  `citation_count_scopus`   INT          NULL,       -- Scopus citedby-count (SCOPUS: rows only)
  `citation_count_openalex` INT          NULL,       -- OpenAlex cited_by_count
  `pmcid`                   VARCHAR(20)  NULL,       -- PMC… from the NCBI ID converter (or OpenAlex ids.pmcid)
  `scopus_fetched_at`       DATETIME     NULL,       -- last Scopus lookup (hit or miss)
  `openalex_fetched_at`     DATETIME     NULL,       -- last OpenAlex lookup (hit or miss)
  `idconv_fetched_at`       DATETIME     NULL,       -- last ID-converter lookup (hit or miss)
  PRIMARY KEY (`article_id`),
  KEY `idx_ae_scopus_fetched` (`scopus_fetched_at`),
  KEY `idx_ae_openalex_fetched` (`openalex_fetched_at`)
-- COLLATE is load-bearing: it must match external_article.article_id for the 6b join.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'analysis_summary_article') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'analysis_summary_article'
            AND column_name = 'citationCountOpenAlex') = 0,
    'ALTER TABLE analysis_summary_article ADD COLUMN `citationCountOpenAlex` INT NULL DEFAULT NULL',
    'SELECT ''analysis_summary_article already has citationCountOpenAlex (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verification
SELECT table_name, column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND (table_name = 'article_enrichment'
       OR (table_name = 'analysis_summary_article' AND column_name = 'citationCountOpenAlex'))
ORDER BY table_name, ordinal_position;
