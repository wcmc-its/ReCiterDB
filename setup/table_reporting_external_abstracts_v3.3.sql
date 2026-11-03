-- =============================================================================
-- Migration: reporting_external_abstracts table (v3.3)
-- =============================================================================
-- Abstracts for external-source articles (external_article: OPENALEX:, later
-- SCOPUS:), the counterpart of reporting_abstracts, which is keyed by PubMed pmid
-- and so never covers them. Named to sit next to reporting_abstracts.
--
-- Written only by update/externalAbstractImport.py (nightly, run_all.py). Each
-- article is fetched ONCE: a row with an abstract is never refetched. A row with
-- abstract NULL means the source had none when checked (fetched_at); the importer
-- rechecks those after 30 days.
--
-- NOT in update/updateReciterDB.py's `all_tables` truncate list, and must not be
-- added: truncating it would refetch every abstract.
--
-- Readers join on article_id = external_article.article_id
-- = analysis_summary_article.article_id (e.g. "OPENALEX:W4417001897").
-- Scholars (SPS etl/reciter) reads it with the same SELECT grant it uses for
-- reporting_abstracts; if that grant is per-table, add:
--   GRANT SELECT ON <db>.reporting_external_abstracts TO '<sps user>'@'%';
--
-- Safe to run on prod and dev. Idempotent (CREATE TABLE IF NOT EXISTS).
-- =============================================================================

CREATE TABLE IF NOT EXISTS `reporting_external_abstracts` (
  `article_id`   VARCHAR(96)  NOT NULL,   -- OPENALEX:W… / SCOPUS:…, as in external_article
  `abstract`     TEXT         NULL,       -- NULL = the source had no abstract at fetched_at
  `abstract_len` INT          NULL,
  `source`       VARCHAR(16)  NOT NULL,   -- where the text came from: 'openalex'
  `fetched_at`   DATETIME     NOT NULL,
  PRIMARY KEY (`article_id`),
  KEY `idx_rea_fetched_at` (`fetched_at`)
-- COLLATE is load-bearing: it must match external_article.article_id for the join.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'reporting_external_abstracts'
ORDER BY ordinal_position;
