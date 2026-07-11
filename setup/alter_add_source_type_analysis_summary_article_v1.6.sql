-- =============================================================================
-- v1.6 — external-article reporting union (ReCiterDB #101, Option B)
-- =============================================================================
-- The nightly SP (populateAnalysisSummaryTables_v2, STEP 6b) now unions
-- non-suppressed `external_article` rows (manually-added Scopus/OpenAlex/WOS pubs)
-- into `analysis_summary_article` / `analysis_summary_author` so they surface in
-- the person publication list. This migration prepares the live table so the SP's
-- `CREATE TABLE analysis_summary_article_new LIKE analysis_summary_article` clones
-- the new column into staging.
--
-- Apply BEFORE the next nightly run (the SP references source_type in STEP 6b).
-- Idempotent — safe to re-run.
--
--   1. source_type — discriminator so SPS/reporting can tell PubMed rows (default
--      'PUBMED') from unioned external rows ('SCOPUS'/'OPENALEX'/'WOS'). Excludes
--      external pubs from bibliometric counts is handled by the SP; this column is
--      for display/attribution and filtering.
--   2. doi widened 128 -> 255 to match external_article.doi (VARCHAR(255)). doi is
--      the SPS dedup key for external pubs; a truncated doi could not be matched
--      back to its source or its suppressed PubMed twin.
-- =============================================================================

ALTER TABLE `analysis_summary_article`
  ADD COLUMN IF NOT EXISTS `source_type` varchar(16) DEFAULT 'PUBMED';

ALTER TABLE `analysis_summary_article`
  MODIFY COLUMN `doi` varchar(255) DEFAULT NULL;
