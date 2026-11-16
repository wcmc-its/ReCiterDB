-- =============================================================================
-- Migration: authorship_review.accept_conflict_matches (v3.12)
-- =============================================================================
-- Publication Manager, /authorships "Duplicate records" queue. Adds to authorship_review:
--   accept_conflict_matches  JSON NULL  what the duplicate check matched when a curator's
--                                       Accept came back 409 (the row was parked in
--                                       accept_conflict). NULL = nothing stored.
--
-- Why: accept_conflict holds only the check's message ("Possible duplicate. ..."), so the
-- duplicates queue cannot show what the paper supposedly duplicates. Prod had 296 parked rows
-- (Cornell Ithaca Scopus conference papers) on 2026-11-16 that curators could not adjudicate.
-- PM now writes the matches together with accept_conflict, and PM's
-- scripts/backfill-accept-conflict-matches.mjs fills the rows parked before this column existed.
--
-- Shape (written only by PM, read only by PM):
--   {"v":1, "uid":"<cwid the accept was for>", "name":"<their name>", "via":"accept"|"backfill",
--    "checked_at":"<ISO time>",
--    "matches":[{"source":"PubMed"|"Scopus"|..., "type":"TITLE_YEAR_MATCH"|..., "id":"<PMID or
--                source id>", "idLabel":"PMID"|"EID"|..., "doi":..., "title":..., "journal":...,
--                "pubYear":..., "where":"record"|"candidates"|"rejected", "detail":...}]}
-- PM-owned like accept_conflict: the AAR producer never writes it (not in aar_db._INSERT_COLS
-- or _REFRESH_COLS), and the table is durable (no nightly truncate), so nothing clears it.
--
-- ORDERING: apply BEFORE deploying the PM change. PM's AuthorshipReview model lists the column,
--   so a PM build that has it SELECTs it on every /authorships load and fails without it.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Idempotent: the ALTER runs only when the
--   column is missing (per information_schema), so a re-run, or a run without the table, is a
--   no-op. MariaDB's JSON is LONGTEXT with a JSON_VALID check; a trailing nullable column is a
--   small ALTER on this table, still best applied outside the nightly window (14:30 UTC).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_authorship_review_add_accept_conflict_matches_v3.12.sql
--
-- Rollback (only after rolling PM back to a build without the column):
--   ALTER TABLE authorship_review DROP COLUMN accept_conflict_matches;
-- =============================================================================

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'authorship_review') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'authorship_review'
            AND column_name = 'accept_conflict_matches') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `accept_conflict_matches` JSON NULL DEFAULT NULL COMMENT ''PM: what the dup check matched when accept_conflict was set; NULL = nothing stored'' AFTER `accept_conflict`',
    'SELECT ''authorship_review already has accept_conflict_matches (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verification: the column on authorship_review, and how many parked rows still lack matches.
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'authorship_review'
  AND column_name IN ('accept_conflict', 'accept_conflict_matches')
ORDER BY ordinal_position;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = @db AND table_name = 'authorship_review'
        AND column_name = 'accept_conflict_matches') = 1,
    'SELECT COUNT(*) AS parked_rows, SUM(accept_conflict_matches IS NULL) AS parked_without_matches FROM authorship_review WHERE accept_conflict IS NOT NULL',
    'SELECT ''no authorship_review.accept_conflict_matches column'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
