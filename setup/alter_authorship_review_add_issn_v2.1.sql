-- =============================================================================
-- Migration: authorship_review add issn (v2.1)
-- =============================================================================
-- Adds:
--   - issn   VARCHAR(9)  NULL  — hyphenated NNNN-NNNC (Scopus gives it unhyphenated;
--                                the producer normalizes before storing)
--
-- WHY THIS MIGRATION EXISTS:
--   recheck_open_scopus() (update/aar_universe_scopus.py) can only journal-filter a
--   no-DOI scopus row against PubMed if it has the row's ISSN — but the column never
--   existed, so the 1,393-row no-DOI backlog (99.5% from the one-time 2026-07-05
--   --mode initial backfill) could only be skipped by pub_type ('Book'), not by
--   dead-journal ISSN. This column lets the recheck reach that existing backlog, not
--   just new rows going forward (ingest-time filtering already has the raw Scopus doc
--   in hand and doesn't need this column). See
--   docs/HANDOFF_2026-08-14_authorships_live_session.md thread #4 (ReCiter Research).
--
--   The fresh-build schema (setup/table_authorship_review.sql) is updated in the same
--   PR. This migration brings EXISTING databases up to that schema. It must be applied
--   directly to BOTH reciterdb instances — the producer instance and the separate dev
--   instance behind reciter-pm-dev (loaded manually) — same as v1.6. Merging the PR
--   does NOT run DDL.
--
-- DURABILITY: authorship_review is curator state, not a reporting export — not in
--   update/updateReciterDB.py's truncate list, not touched by any nightly ETL step.
--
-- Additive only, guarded by an information_schema check (no-op on re-run). Existing
-- rows get issn=NULL; the producer backfills it opportunistically as rows are
-- upserted/rechecked going forward (no bulk backfill of historical rows here — the
-- title/DOI recheck path already covers them independent of ISSN).
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- issn VARCHAR(9) NULL — hyphenated NNNN-NNNC
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'issn') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `issn` VARCHAR(9) NULL',
    'SELECT ''authorship_review.issn already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name = 'issn';
