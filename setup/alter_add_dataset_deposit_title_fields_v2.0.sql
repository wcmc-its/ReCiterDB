-- =============================================================================
-- Migration: dataset_deposit title/metadata fields (v2.0)
-- =============================================================================
-- Adds 7 nullable columns to the existing `dataset_deposit` table (see
-- setup/alter_add_dataset_deposit_v1.9.sql, PR #131/#132) to backfill
-- title/metadata for the #2350 Phase 2 dataset-enrichment feature in
-- Scholars-Profile-System — today a dataset row renders with only
-- repository/year/accessModel/confidence, no indication of what the dataset
-- or trial actually is (e.g. a ClinicalTrials.gov row shows nothing about
-- what the trial studies).
--
-- title             VARCHAR(512) NULL  -- DataCite titles[0].title OR CT.gov briefTitle
-- description       TEXT         NULL  -- DataCite descriptions[0].description
-- creators          JSON         NULL  -- DataCite creators[].name (string[] of names only)
-- publisher         VARCHAR(255) NULL  -- DataCite publisher
-- trial_phase       VARCHAR(32)  NULL  -- ClinicalTrials.gov phases[0]
-- trial_status      VARCHAR(32)  NULL  -- ClinicalTrials.gov overallStatus
-- trial_conditions  JSON         NULL  -- ClinicalTrials.gov conditions (array)
--
-- Populated by a new standalone script (scripts/bulk-data-rule/enrich_titles.py,
-- Scholars-Profile-System repo) that UPDATEs by the existing natural key
-- (repository, accession_or_doi) — this migration does not populate data.
--
-- Additive only: no existing column, index, or constraint on `dataset_deposit`
-- is touched, altered, or dropped. The table's PRIMARY KEY, its
-- uk_cwid_repo_accession_pmid unique key, and all existing ix_* indexes are
-- unchanged.
--
-- These 7 columns are NOT added to any nightly truncate list and are NOT
-- referenced by any stored procedure. Verified by:
--   grep -rn "dataset_deposit" update/*.py setup/createEventsProceduresReciterDb.sql
-- -> no matches in either (dataset_deposit is not in updateReciterDB.py's
-- `all_tables` truncate list, and createEventsProceduresReciterDb.sql has no
-- reference to it at all). Same durable-table pattern as `authorship_review`
-- and `grant_provenance` — this table is written only by
-- scripts/bulk-data-rule/attribute.py and (going forward) enrich_titles.py,
-- never by this repo's nightly cron.
--
-- Safe to run on prod and dev. Idempotent (ADD COLUMN IF NOT EXISTS —
-- supported on MariaDB since 10.0.2), safe to re-run.
-- =============================================================================

ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `title` VARCHAR(512) NULL COMMENT 'DataCite titles[0].title or CT.gov briefTitle';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `description` TEXT NULL COMMENT 'DataCite descriptions[0].description';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `creators` JSON NULL COMMENT 'DataCite creators[].name, string array of names only';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `publisher` VARCHAR(255) NULL COMMENT 'DataCite publisher';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `trial_phase` VARCHAR(32) NULL COMMENT 'ClinicalTrials.gov phases[0]';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `trial_status` VARCHAR(32) NULL COMMENT 'ClinicalTrials.gov overallStatus';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `trial_conditions` JSON NULL COMMENT 'ClinicalTrials.gov conditions array';

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

DESCRIBE `dataset_deposit`;
