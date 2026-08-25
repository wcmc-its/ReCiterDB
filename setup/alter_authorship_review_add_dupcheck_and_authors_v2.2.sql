-- =============================================================================
-- Migration: authorship_review add authors_json + dup_flag/dup_reason (v2.2)
-- =============================================================================
-- Adds:
--   - authors_json  LONGTEXT     NULL      — Scopus full author list (simplified
--                                            {given,surname} list); PubMed rows leave
--                                            this NULL (PubMed already shows the full
--                                            byline at the PMID link)
--   - dup_flag      TINYINT(1)   NOT NULL DEFAULT 0  — precomputed "matches an
--                                            existing external_article by DOI" signal
--   - dup_reason    VARCHAR(255) NULL      — human-readable note, e.g. "Already added
--                                            as ExternalArticle for <uid> (DOI match)"
--
-- WHY THIS MIGRATION EXISTS:
--   Round 2 of the PM feedback pass (docs/PLAN_2026-08-24_pm-feedback-round2.md,
--   "Round 2 — cross-cutting, needs a human at the DB" in the ReCiter Research
--   project) bundles two producer-side gaps into one migration to minimize how many
--   times anyone touches prod DDL:
--
--   authors_json — aar_universe_scopus.py's wcm_authorships() already parses the FULL
--   per-document author array from the Scopus Search API COMPLETE-view response, but
--   _build_row() only ever stored the one WCM-matched author, discarding every other
--   author. Curators reviewing a Scopus-sourced row had no way to see the paper's full
--   byline (unlike PubMed rows, which link straight to the PMID where PubMed shows it).
--   This is "stop discarding," not "start fetching" — no new Scopus API calls.
--
--   dup_flag/dup_reason — a curator could spend time on Accept/Assign for a row whose
--   underlying paper was ALREADY manually added to that person's record as an
--   ExternalArticle (PM's "Add publication"), with the only present-day check being a
--   live 409 at click time. reciterdb already runs update/retrieveExternalArticles.py
--   nightly, projecting DynamoDB's ExternalArticle table into MySQL `external_article`
--   BEFORE the weekly AAR lane runs — so a plain SQL join against external_article.doi
--   precomputes "already added" for the large majority of cases at producer time. The
--   live 409 check at Accept/Assign time (Publication Manager, out of scope here) stays
--   as a final write-time safety net for same-day races between weekly producer runs;
--   this column is a heads-up, not a replacement.
--
--   The fresh-build schema (setup/table_authorship_review.sql) is updated in the same
--   PR. This migration brings EXISTING databases up to that schema. It must be applied
--   directly to BOTH reciterdb instances — the producer instance and the separate dev
--   instance behind reciter-pm-dev (loaded manually) — same as v1.6 and v2.1. Merging
--   the PR does NOT run DDL.
--
-- DURABILITY: authorship_review is curator state, not a reporting export — not in
--   update/updateReciterDB.py's truncate list, not touched by any nightly ETL step.
--
-- Additive only, guarded by an information_schema check per column (no-op on re-run).
-- Existing rows get authors_json=NULL, dup_flag=0, dup_reason=NULL; the producer
-- backfills authors_json/dup_flag/dup_reason opportunistically as rows are upserted/
-- rechecked going forward (no bulk backfill of historical rows here — the Scopus
-- --mode initial backfill, when re-run per the plan doc's sequencing, covers the
-- historical authors_json backlog; dup_flag/dup_reason likewise refresh on the next
-- producer run for any given row).
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- authors_json LONGTEXT NULL — Scopus full author list, scopus rows only
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'authors_json') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `authors_json` LONGTEXT NULL',
    'SELECT ''authorship_review.authors_json already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- dup_flag TINYINT(1) NOT NULL DEFAULT 0 — precomputed ExternalArticle DOI match
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'dup_flag') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `dup_flag` TINYINT(1) NOT NULL DEFAULT 0',
    'SELECT ''authorship_review.dup_flag already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- dup_reason VARCHAR(255) NULL — human-readable note for dup_flag=1
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'dup_reason') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `dup_reason` VARCHAR(255) NULL',
    'SELECT ''authorship_review.dup_reason already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name IN ('authors_json', 'dup_flag', 'dup_reason')
ORDER BY column_name;
