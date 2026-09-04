-- =============================================================================
-- v2.8 — replace the literal string default 'NULL' with real SQL NULL
--        (ReCiterDB #197)
-- =============================================================================
-- 24 columns across 6 base tables were declared `DEFAULT 'NULL'` — the
-- four-character STRING, not SQL NULL. Nothing writes it explicitly; it fires
-- only when an INSERT OMITS the column, which makes it dormant until someone
-- adds an INSERT path that doesn't name every column.
--
-- That is exactly what happened in populateAnalysisSummaryTables_v2 STEP 3 vs
-- STEP 6b:
--   * STEP 3  (PubMed)   names issue/volume/pages    -> default never fires.
--   * STEP 6b (external) OMITS issue/volume/pages    -> literal 'NULL' written.
--
-- Prod, measured 2026-09-04 before this migration:
--   analysis_summary_article, pmid < 0 : 4054 / 4054 rows carried 'NULL'
--   analysis_summary_article, pmid > 0 :    0 / 389312
--   all other tables below             :    0 affected rows (prophylactic)
--
-- Downstream this defeats every truthiness guard (`if (volume)`) because a
-- non-empty string is truthy — SPS rendered `2024;NULL(NULL):NULL` in four
-- citation builders (Scholars-Profile-System#2580).
--
-- NO BACKFILL IS NEEDED for the swap-participating tables. Both nightly
-- rebuilds create their staging table with `CREATE TABLE <t>_new LIKE <t>`
-- and promote it with an atomic RENAME:
--   * analysis_summary_article  -> populateAnalysisSummaryTables_v2, STEP 1 / 7
--   * person_article, person_article_department,
--     person_article_scopus_{target,non_target}_author_affiliation
--                               -> prepare_person_shadow_tables() /
--                                  swap_person_tables() (setup/person_table_swap.sql)
-- So an ALTER on the live table is cloned into staging on the next run and the
-- data regenerates correct. auto_accept_log and reporting_author_affiliation do
-- NOT participate in a swap, but both have 0 affected rows today.
--
-- Apply BEFORE the next nightly run. Idempotent — safe to re-run.
--
-- Uses ALTER COLUMN ... SET DEFAULT (metadata-only, instant) rather than
-- MODIFY COLUMN: only the default changes, so there is no table rebuild on
-- person_article (858k rows) or person_article_scopus_target_* (589k).
--
-- NOTE: the analysis_summary_article block was already applied to prod on
-- 2026-09-04; re-running it is a no-op.
-- =============================================================================

ALTER TABLE `analysis_summary_article`
  ALTER COLUMN `volume` SET DEFAULT NULL,
  ALTER COLUMN `issue` SET DEFAULT NULL,
  ALTER COLUMN `pages` SET DEFAULT NULL,
  ALTER COLUMN `journalTitleVerbose` SET DEFAULT NULL;

ALTER TABLE `person_article`
  ALTER COLUMN `volume` SET DEFAULT NULL,
  ALTER COLUMN `issue` SET DEFAULT NULL,
  ALTER COLUMN `pages` SET DEFAULT NULL,
  ALTER COLUMN `journalTitleVerbose` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchFirstType` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchMiddleType` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchLastType` SET DEFAULT NULL,
  ALTER COLUMN `journalSubfieldScienceMetrixLabel` SET DEFAULT NULL,
  ALTER COLUMN `identityBachelorYear` SET DEFAULT NULL;

ALTER TABLE `person_article_department`
  ALTER COLUMN `articleAffiliation` SET DEFAULT NULL;

ALTER TABLE `person_article_scopus_non_target_author_affiliation`
  ALTER COLUMN `nonTargetAuthorInstitutionLabel` SET DEFAULT NULL,
  ALTER COLUMN `nonTargetAuthorInstitutionID` SET DEFAULT NULL;

ALTER TABLE `person_article_scopus_target_author_affiliation`
  ALTER COLUMN `targetAuthorInstitutionalAffiliationSource` SET DEFAULT NULL,
  ALTER COLUMN `targetAuthorInstitutionalAffiliationArticleScopusAffiliationId` SET DEFAULT NULL;

-- auto_accept_log and reporting_author_affiliation have no DDL in this repo —
-- they exist only in prod. Same defect, 0 affected rows; fixed here so a future
-- partial INSERT can't reintroduce it.
ALTER TABLE `auto_accept_log`
  ALTER COLUMN `volume` SET DEFAULT NULL,
  ALTER COLUMN `issue` SET DEFAULT NULL,
  ALTER COLUMN `pages` SET DEFAULT NULL,
  ALTER COLUMN `journalTitleVerbose` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchFirstType` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchMiddleType` SET DEFAULT NULL,
  ALTER COLUMN `nameMatchLastType` SET DEFAULT NULL,
  ALTER COLUMN `journalSubfieldScienceMetrixLabel` SET DEFAULT NULL,
  ALTER COLUMN `identityBachelorYear` SET DEFAULT NULL;

ALTER TABLE `reporting_author_affiliation`
  ALTER COLUMN `affiliationID` SET DEFAULT NULL;

-- Verify (expect 0):
-- SELECT COUNT(*) FROM information_schema.COLUMNS
--  WHERE TABLE_SCHEMA = DATABASE() AND COLUMN_DEFAULT = "'NULL'"
--    AND TABLE_NAME NOT LIKE '%\_backup'
--    AND TABLE_NAME NOT REGEXP '_[0-9]{4}_[0-9]{2}_[0-9]{2}$';
