-- =============================================================================
-- Migration: authorship_review byline identifiers + top_orcid_match (v3.15)
-- =============================================================================
-- The external lanes (Scopus, OpenAlex) rank a byline on name and affiliation text and
-- store neither identifier the source attaches to the byline author. Adds:
--   byline_orcid           CHAR(19) NULL. ORCID of THIS ROW's byline author, bare form
--                          0000-0000-0000-0000 (no https://orcid.org/ prefix). Scopus
--                          lane: the author entry's own `orcid` where the COMPLETE view
--                          carries one, else OpenAlex's ORCID for the author at the same
--                          byline position of the same DOI. OpenAlex lane: the
--                          authorship's author.orcid. NULL = none found, the lookup
--                          failed, or capture is off.
--   byline_scopus_authid   VARCHAR(20) NULL. Scopus author id of the byline author.
--                          Capture only: nothing reads it yet. NULL on OpenAlex rows.
--   top_orcid_match        TINYINT(1) NULL. 1 when the top pick is the person the byline
--                          ORCID maps to (cornell_pub_orcid_mapping); NULL when the row
--                          has no byline ORCID or the ORCID maps to nobody on the roster.
--                          Each entry of candidate_cwids_json on such a row carries
--                          "orcid_match": true|false (identity_index.candidates).
-- All three are producer-owned (aar_db._REFRESH_COLS), like top_venue_fit. Publication
-- Manager reads them; it never writes them.
--
-- BEHAVIOUR CHANGE: none by itself. Nothing fills the columns until the CronJob env
--   sets AAR_BYLINE_ORCID=on. That one switch turns on both halves: the lanes store the
--   identifiers, and on a Cornell Ithaca byline with no WCM candidate the matcher ranks
--   the person the ORCID is on file for first. It never changes a row's status.
--
-- ORDERING: apply this file BEFORE setting AAR_BYLINE_ORCID. Merging the code first is
--   harmless: the env var ships off, and aar_db.upsert leaves a column the table does
--   not have out of its statement (aar_db._OPTIONAL_COLS).
--   One-off after applying and turning it on (dry run first, then --apply). With
--   AAR_BYLINE_ORCID=on in its own shell it also re-ranks the open rows and reports
--   PICK_CHANGE / ORCID_CONFIRMS / ORCID_NEW_PERSON:
--     python3 update/backfill_byline_orcid.py
--
-- Idempotent: each ALTER runs only when its column is missing (per information_schema).
--   Nothing is dropped or truncated. Apply directly to BOTH reciterdb instances (the
--   producer and the dev instance behind reciter-pm-dev). Merging the PR does NOT run
--   DDL. Nullable columns on authorship_review are small ALTERs, still best applied
--   outside the nightly window (14:30 UTC).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_authorship_review_add_byline_orcid_v3.15.sql
--
-- Rollback (set AAR_BYLINE_ORCID off first):
--   ALTER TABLE authorship_review DROP COLUMN byline_orcid,
--     DROP COLUMN byline_scopus_authid, DROP COLUMN top_orcid_match;
-- =============================================================================

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'authorship_review') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'authorship_review'
            AND column_name = 'byline_orcid') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `byline_orcid` CHAR(19) NULL DEFAULT NULL AFTER `author_affiliation`',
    'SELECT ''authorship_review already has byline_orcid (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'authorship_review') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'authorship_review'
            AND column_name = 'byline_scopus_authid') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `byline_scopus_authid` VARCHAR(20) NULL DEFAULT NULL AFTER `byline_orcid`',
    'SELECT ''authorship_review already has byline_scopus_authid (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'authorship_review') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'authorship_review'
            AND column_name = 'top_orcid_match') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `top_orcid_match` TINYINT(1) NULL DEFAULT NULL AFTER `top_venue_fit`',
    'SELECT ''authorship_review already has top_orcid_match (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verification: the three columns, and how much of each is filled.
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'authorship_review'
  AND column_name IN ('byline_orcid', 'byline_scopus_authid', 'top_orcid_match')
ORDER BY ordinal_position;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = @db AND table_name = 'authorship_review'
        AND column_name IN ('byline_orcid', 'byline_scopus_authid', 'top_orcid_match')) = 3,
    'SELECT source, COUNT(*) AS row_count, SUM(byline_orcid IS NOT NULL) AS with_orcid, SUM(byline_scopus_authid IS NOT NULL) AS with_authid, SUM(top_orcid_match = 1) AS top_orcid_match FROM authorship_review GROUP BY source',
    'SELECT ''authorship_review is missing a v3.15 column'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
