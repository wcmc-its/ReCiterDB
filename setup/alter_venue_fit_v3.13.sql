-- =============================================================================
-- Migration: venue_subfield_inferred table + authorship_review.top_venue_fit (v3.13)
-- =============================================================================
-- Venue fit for the external-lane matcher (Scopus and OpenAlex rows on authorship_review).
-- Adds:
--   venue_subfield_inferred   Durable, one row per container title that has no ISSN
--                             (conference proceedings, books). venue_key is the title as
--                             update/venue_fit.py venue_key() normalises it: lowercased,
--                             accents folded, 4-digit years and edition ordinals removed,
--                             so "CLEO 2023" and "CLEO 2024" share one row. subfield is a
--                             Science-Metrix subfield that exists in journal_science_metrix;
--                             field and domain come from joining on it and are not stored
--                             twice. Written only by update/infer_venue_subfield.py.
--   authorship_review.top_venue_fit
--                             FLOAT NULL. How well the row's venue fits the top candidate's
--                             department: ln of the department's share of its accepted
--                             papers in the venue's subfield over the campus-wide share
--                             (update/venue_fit.py DeptVenueFit). Positive = the department
--                             publishes there more than the campus does. NULL = unknown:
--                             venue has no category, department has under 20 training
--                             rows, or the feature is off for that campus. Producer-owned
--                             (aar_db._REFRESH_COLS), like top_years_after_wcm.
--
-- Venues WITH an ISSN are not stored in the new table: they go into journal_science_metrix
-- with smsid >= 800001, as the earlier person_article augmentation already did.
--
-- BEHAVIOUR CHANGE: none by itself. Nothing reads or writes either object until the
--   CronJob env turns the features on:
--     AAR_VENUE_INFER=on   run_all.py classifies new venues before the AAR lanes
--     AAR_VENUE_FIT=on     the matcher adds the venue term for Cornell Ithaca candidates
--
-- ORDERING: apply this file BEFORE setting either env var. Merging the code first is
--   harmless: both env vars ship off, aar_db.upsert leaves top_venue_fit out of its
--   statement while the column is absent, and infer_venue_subfield.py --apply refuses
--   without the table.
--   One-off loads after applying (dry run first, then --apply):
--     python3 update/infer_venue_subfield.py --import-science-metrix <ReCiter>/src/main/resources/files/ScienceMetrix.json
--     python3 update/infer_venue_subfield.py --import-json <venue_subfield_inferred_2026-11-16.json>
--
-- DURABILITY: neither table is in update/updateReciterDB.py's `all_tables` truncate list,
--   and venue_subfield_inferred must never be added: truncating it would send every
--   container title back to the model.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS, and the ALTER runs only when the column is
--   missing (per information_schema). Nothing is dropped or truncated. Apply directly to
--   BOTH reciterdb instances (the producer and the dev instance behind reciter-pm-dev).
--   Merging the PR does NOT run DDL. A nullable column on authorship_review is a small
--   ALTER, still best applied outside the nightly window (14:30 UTC).
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_venue_fit_v3.13.sql
--
-- Rollback (set AAR_VENUE_FIT and AAR_VENUE_INFER off first):
--   ALTER TABLE authorship_review DROP COLUMN top_venue_fit;
--   DROP TABLE venue_subfield_inferred;
--   Journal rows this work adds to journal_science_metrix can stay: they are ordinary
--   rows, and the earlier augmentation and the ScienceMetrix.json import use
--   smsid >= 800001 too, so do not delete on smsid alone.
-- =============================================================================

SET @db = DATABASE();

-- Same statement as in setup/createDatabaseTableReciterDb.sql.
CREATE TABLE IF NOT EXISTS `venue_subfield_inferred` (
  `venue_key`   VARCHAR(512) NOT NULL,
  `title`       VARCHAR(512) NOT NULL,
  `subfield`    VARCHAR(128) NOT NULL,
  `model`       VARCHAR(64)  NOT NULL,
  `inferred_at` DATETIME     NOT NULL,
  PRIMARY KEY (`venue_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = @db AND table_name = 'authorship_review') = 1
    AND (SELECT COUNT(*) FROM information_schema.columns
          WHERE table_schema = @db AND table_name = 'authorship_review'
            AND column_name = 'top_venue_fit') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `top_venue_fit` FLOAT NULL DEFAULT NULL AFTER `top_years_after_wcm`',
    'SELECT ''authorship_review already has top_venue_fit (or the table is absent)'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- Verification: the table, the column, and how much of each is filled.
SELECT table_name, table_rows
FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'venue_subfield_inferred';

SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'authorship_review'
  AND column_name IN ('top_years_after_wcm', 'top_venue_fit')
ORDER BY ordinal_position;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = @db AND table_name = 'authorship_review'
        AND column_name = 'top_venue_fit') = 1,
    'SELECT source, COUNT(*) AS open_rows, SUM(top_venue_fit IS NOT NULL) AS with_venue_fit FROM authorship_review WHERE status = ''open'' GROUP BY source',
    'SELECT ''no authorship_review.top_venue_fit column'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT COUNT(*) AS inferred_journal_rows FROM journal_science_metrix WHERE smsid >= 800001;
