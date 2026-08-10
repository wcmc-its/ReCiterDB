-- =============================================================================
-- Migration: dataset_deposit table (v1.9)
-- =============================================================================
-- Adds the source table for the Scholars-Profile-System "Phase 2 data sharing"
-- feature (Bulk Data Rule / S-Index project) — one row per (cwid, dataset,
-- citing pmid), populated by the standalone scripts/bulk-data-rule/attribute.py
-- pipeline (Scholars-Profile-System repo), NOT by this repo's nightly cron.
--
-- Column names/widths mirror the DatasetDeposit/PersonDatasetDeposit Prisma
-- models already merged in Scholars-Profile-System (prisma/schema.prisma) —
-- Prisma's @map targets these exact snake_case names, so the SPS ETL bridge
-- (etl/data-sharing/shared.ts SourceRow) can SELECT this shape directly.
--
-- Flat/denormalized here on purpose: SPS splits this into two normalized
-- tables (DatasetDeposit + PersonDatasetDeposit) on import; this source table
-- stays one-row-per-citing-pmid so attribute.py can INSERT straight from its
-- existing per-(pmid, repo, person) dataframe, no pre-aggregation required.
--
-- NOT in update/updateReciterDB.py's `all_tables` truncate list and not
-- referenced by any nightly stored procedure — durable like `authorship_review`
-- and `grant_provenance`, not a reload-each-run reporting table. Must not be
-- added to that list.
--
-- Safe to run on prod and dev. Idempotent (CREATE TABLE IF NOT EXISTS).
-- =============================================================================

CREATE TABLE IF NOT EXISTS `dataset_deposit` (
  `id`               INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `cwid`             VARCHAR(32)  DEFAULT NULL,
  `repository`       VARCHAR(64)  NOT NULL,           -- canonical name, from bulk-data-rule/catalog.py
  `accession_or_doi` VARCHAR(255) NOT NULL,
  `resource_type`    VARCHAR(32)  DEFAULT NULL,        -- DataCite resourceTypeGeneral; null for accession-based repos
  `data_type`        VARCHAR(64)  DEFAULT NULL,        -- repo-native bucket, or MeSH-derived
  `access_model`     VARCHAR(16)  DEFAULT NULL,        -- 'open' | 'controlled'
  `deposit_year`     SMALLINT     DEFAULT NULL,        -- repo metadata where available, else pub year
  `provenance`       VARCHAR(16)  NOT NULL,            -- 'databank' | 'fulltext-scan'
  `confidence`       VARCHAR(16)  DEFAULT NULL,        -- 'high' | 'low' | 'unclear'; null for databank rows
  `author_position`  VARCHAR(16)  DEFAULT NULL,        -- 'first' | 'last' | 'middle'
  `pmid`             INT(11)      DEFAULT NULL,        -- citing publication for this (cwid, dataset) pair
  `last_refreshed_at` DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_cwid_repo_accession_pmid` (`cwid`, `repository`, `accession_or_doi`, `pmid`),
  KEY `ix_repository_accession` (`repository`, `accession_or_doi`),
  KEY `ix_cwid` (`cwid`),
  KEY `ix_pmid` (`pmid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, table_rows, create_time
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name = 'dataset_deposit';
