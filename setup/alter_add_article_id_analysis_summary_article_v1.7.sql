-- =============================================================================
-- v1.7 — expose external article_id in analysis_summary_article (ReCiterDB #101 downstream)
-- =============================================================================
-- STEP 6b (populateAnalysisSummaryTables_v2) unions external_article rows into
-- analysis_summary_article keyed on a SYNTHETIC NEGATIVE pmid that is recomputed
-- every nightly run and shifts whenever the curated external set changes. That is
-- fine as an internal join key, but SPS ingests these rows and needs a STABLE
-- identity for external publications. `article_id` (the source-prefixed id, e.g.
-- 'SCOPUS:105037533819' / 'OPENALEX:W2741809807') is stable, unique, and always
-- present, so SPS keys external pubs on it instead of the synthetic pmid.
--
-- This migration adds the column so the SP's
-- `CREATE TABLE analysis_summary_article_new LIKE analysis_summary_article`
-- clones it into staging; STEP 6b then populates it (NULL for PubMed rows).
--
-- Apply BEFORE the next nightly run. Idempotent — safe to re-run.
-- =============================================================================

ALTER TABLE `analysis_summary_article`
  ADD COLUMN IF NOT EXISTS `article_id` varchar(96) DEFAULT NULL;
