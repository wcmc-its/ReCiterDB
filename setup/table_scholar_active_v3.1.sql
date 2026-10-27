-- =============================================================================
-- Migration: scholar_active table (v3.1)
-- =============================================================================
-- One row per person with an active record in Scholars (Scholars-Profile-System),
-- written by SPS's nightly `etl:scholar-active-export` in prod only. Publication
-- Manager reads it to decide whether a uid's ORCID field is read-only: Scholars
-- is the ORCID authority for these people, and PM links to the Scholars queue
-- instead of saving to admin_orcid.
--
-- No ORCID iD is stored here: PM shows the one in DynamoDB Identity, which SPS's
-- push keeps equal to Scholars. has_confirmed_orcid = 0 means Identity may hold
-- an iD Scholars has not confirmed; PM labels it "not confirmed in Scholars".
--
-- SPS replaces the whole set each run (DELETE + INSERT in one transaction), and
-- every row of a run carries the same refreshed_at. PM treats the table as
-- unavailable when MAX(refreshed_at) is older than 48 hours, and falls back to
-- the editable field.
--
-- NOT in update/updateReciterDB.py's `all_tables` truncate list, and must not be
-- added: SPS is its only writer.
--
-- Safe to run on prod and dev. Idempotent (CREATE TABLE IF NOT EXISTS).
-- Then grant the SPS ETL user (the SCHOLARS_RECITERDB_USERNAME in the SPS secret
-- scholars/prod/etl/reciter) exactly what the export needs:
--   GRANT SELECT, INSERT, DELETE ON <db>.scholar_active TO '<sps user>'@'%';
-- =============================================================================

CREATE TABLE IF NOT EXISTS `scholar_active` (
  `cwid`                VARCHAR(32) NOT NULL,
  `has_confirmed_orcid` TINYINT(1)  NOT NULL DEFAULT 0,
  `refreshed_at`        DATETIME    NOT NULL,
  PRIMARY KEY (`cwid`),
  KEY `idx_scholar_active_refreshed_at` (`refreshed_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'scholar_active'
ORDER BY ordinal_position;
