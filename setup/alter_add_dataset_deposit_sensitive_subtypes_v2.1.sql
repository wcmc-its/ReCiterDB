-- =============================================================================
-- Migration: dataset_deposit sensitive-subtype fields (v2.1)
-- =============================================================================
-- Adds 2 nullable columns to the existing `dataset_deposit` table (see
-- setup/alter_add_dataset_deposit_v1.9.sql, PR #131/#132, extended by
-- setup/alter_add_dataset_deposit_title_fields_v2.0.sql, PR #133/#134) for
-- the Scholars-Profile-System S-Index v2 "granular sub-types" dashboard
-- section — today a dataset row's sensitivity is a single derived flag, with
-- no coarse category or granular sub-type recorded on the row itself.
--
-- sensitive_cats      VARCHAR(255) NULL  -- '|'-delimited coarse categories
--                                        -- (genomic, omic_other, health,
--                                        -- biometric, geolocation) from
--                                        -- Scholars-Profile-System's
--                                        -- scripts/bulk-data-rule/taxonomy.py
--                                        -- tag(). Null if none detected.
-- sensitive_subtypes  VARCHAR(255) NULL  -- '|'-delimited granular sub-types,
--                                        -- e.g. "genomic:WGS/WES|omic_other:
--                                        -- single-cell", same taxonomy.py
--                                        -- source. Null if none detected.
--
-- Populated later by a rerun of the existing WRITE_DATASET_DEPOSIT path in
-- scripts/bulk-data-rule/attribute.py (Scholars-Profile-System repo) — this
-- migration only adds the columns, no backfill.
--
-- Additive only: no existing column, index, or constraint on `dataset_deposit`
-- is touched, altered, or dropped. The table's PRIMARY KEY, its
-- uk_cwid_repo_accession_pmid unique key, and all existing ix_* indexes are
-- unchanged.
--
-- These 2 columns are NOT added to any nightly truncate list and are NOT
-- referenced by any stored procedure. Verified by:
--   grep -rn "dataset_deposit" update/*.py setup/createEventsProceduresReciterDb.sql
-- -> no matches in either (dataset_deposit is not in updateReciterDB.py's
-- `all_tables` truncate list, and createEventsProceduresReciterDb.sql has no
-- reference to it at all). Same durable-table pattern as `authorship_review`
-- and `grant_provenance` — this table is written only by
-- scripts/bulk-data-rule/attribute.py, never by this repo's nightly cron.
--
-- Safe to run on prod and dev. Idempotent (ADD COLUMN IF NOT EXISTS —
-- supported on MariaDB since 10.0.2), safe to re-run.
-- =============================================================================

ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `sensitive_cats` VARCHAR(255) NULL COMMENT 'Pipe-delimited coarse sensitive-data categories from bulk-data-rule/taxonomy.py tag()';
ALTER TABLE `dataset_deposit` ADD COLUMN IF NOT EXISTS `sensitive_subtypes` VARCHAR(255) NULL COMMENT 'Pipe-delimited granular sensitive-data sub-types from bulk-data-rule/taxonomy.py tag()';

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

DESCRIBE `dataset_deposit`;
