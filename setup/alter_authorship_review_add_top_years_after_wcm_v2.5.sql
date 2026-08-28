-- =============================================================================
-- Migration: authorship_review add top_years_after_wcm (v2.5)
-- =============================================================================
-- Adds:
--   - top_years_after_wcm  INT  NULL  — paper publication year MINUS the top
--                                       candidate's last WCM appointment year
--                                       (identity.endDateWCMFaculty /
--                                       endDateWCMStudent, whichever is later).
--
--     Signed and three-valued on purpose:
--       NULL      unknown — the candidate has no WCM end year on file, or the
--                 record carries no parseable publication year. NOT the same as 0.
--       negative  the paper predates the departure. This is the LARGEST band
--                 (11,661 of 19,273 open rows with a top candidate, measured
--                 2026-08-28) and it is the legitimate case — exactly the missing
--                 attributions this queue exists to find. It must stay
--                 distinguishable from NULL and from 0.
--       0..5      within the grace period: publication lag, late-career and
--                 posthumous output, consortium reporting.
--       6+        the stale band. 2,771 open rows on the same measurement, of
--                 which 1,604 (58%) have a homonym cohort of exactly ONE person.
--
-- WHY THIS MIGRATION EXISTS:
--   Issue #159: the matcher routinely proposes long-departed faculty as the top
--   candidate for recent papers. The producer now applies a graduated, capped
--   temporal penalty to `confidence` (update/identity_index.py temporal_penalty),
--   but re-ranking can only ever help a row that HAS a rival to promote — and on
--   58% of the stale rows the departed person is the only candidate in the cohort.
--   There is nothing to re-rank. Persisting the gap as a queryable column is the
--   lever that reaches those rows: Publication Manager can filter and sort the
--   queue on the stale band and let curators bulk-dismiss it, which is issue #159's
--   own "third, cheaper option".
--
--   No reader exists yet — the PM-side filter/sort is a separate issue in
--   wcmc-its/ReCiter-Publication-Manager. This ships the producer half so the
--   column is populated by the time that lands.
--
--   The fresh-build schema (setup/table_authorship_review.sql) is updated in the
--   same PR. This migration brings EXISTING databases up to that schema. It must be
--   applied directly to BOTH reciterdb instances — the producer instance and the
--   separate dev instance behind reciter-pm-dev (loaded manually) — same as v1.6,
--   v2.1 and v2.2. Merging the PR does NOT run DDL.
--
--   Apply with:
--     mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--       < setup/alter_authorship_review_add_top_years_after_wcm_v2.5.sql
--
-- DURABILITY: authorship_review is curator state, not a reporting export — not in
--   update/updateReciterDB.py's truncate list, not touched by any nightly ETL step.
--
-- Additive only, guarded by an information_schema check (no-op on re-run).
--
-- THE BACKFILL BELOW IS NOT OPTIONAL. The producer only writes this column when it
-- re-emits a row, and neither recheck path writes it at all: aar_orchestrator._recheck
-- mutates only the CSV ledger, and aar_universe_scopus.recheck_open_scopus issues only
-- `UPDATE … SET status='dismissed'`. Of the 2,771 existing stale open rows, 35 fall in
-- the PubMed recurring window [today-71d, today-40d] and 69 in the Scopus rolling
-- window [today-90d, today-14d] — about 1.3%. The other ~98% would keep NULL
-- indefinitely, which would leave this column unable to do the one job it was added
-- for. (Same lesson as update/targeted_authors_backfill.py: a sweep re-run cannot
-- backfill a new column onto existing rows.)
--
-- The backfill uses YEAR(entrez_date) as the publication year, which the producer
-- deliberately does NOT do — `entrez_date` is the NCBI index date on PubMed rows and a
-- Scopus coverDate on Scopus rows. That substitution is acceptable HERE and nowhere
-- else: on the Scopus lane it is exact (entrez_date IS coverDate,
-- aar_universe_scopus.py:405), and on the PubMed lane, checked against
-- analysis_summary_article.publicationDateStandardized for the 8,104 joinable open
-- rows, it agrees exactly on 93.8%, differs by one year on 6.0% and by two or more on
-- 0.16%. A one-year error cannot move a row across a band that is five years wide, and
-- the producer overwrites the value with the true publication year on the row's next
-- refresh anyway.
--
-- Measured on prod 2026-08-28, immediately before writing this file — open rows with a
-- top candidate carrying a WCM end year:
--   total backfilled            17,702
--     paper predates departure  11,661   (negative — the legitimate case)
--     0-5y   (grace)             3,270
--     6-10y                      1,178
--     11-20y                     1,149
--     20y+                         444
--   stale band (6y+)             2,771
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- top_years_after_wcm INT NULL — signed gap; negative = paper predates departure
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'top_years_after_wcm') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `top_years_after_wcm` INT NULL',
    'SELECT ''authorship_review.top_years_after_wcm already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name = 'top_years_after_wcm';

-- -----------------------------------------------------------------------------
-- Backfill existing rows. Idempotent: re-running recomputes the same values.
--
-- GREATEST over COALESCE(...,0) reproduces the producer's max(facultyEnd, studentEnd)
-- reading, and the > 0 guard keeps rows with neither year at NULL rather than
-- inventing a gap.
--
-- A COLLATE cast is required — identity.cwid is utf8mb4_unicode_ci and
-- authorship_review.top_cwid is utf8mb4_general_ci, so the join throws "Illegal mix of
-- collations" without one. Cast ONLY the authorship_review side, to identity's
-- collation. Casting both sides makes identity.cwid's index unusable and the join
-- degenerates to a full index scan per row: EXPLAIN went from 26,884 estimated row
-- combinations to 880,934,912, and a dev run sat in "Sending data" for 5+ minutes with
-- nothing committed. Leaving i.cwid bare keeps the plan at type=ref, rows=1.
-- -----------------------------------------------------------------------------
UPDATE authorship_review ar
JOIN identity i
  ON i.cwid = ar.top_cwid COLLATE utf8mb4_unicode_ci
SET ar.top_years_after_wcm =
      YEAR(ar.entrez_date) - GREATEST(COALESCE(i.endDateWCMFaculty, 0),
                                      COALESCE(i.endDateWCMStudent, 0))
WHERE ar.top_cwid IS NOT NULL
  AND ar.entrez_date IS NOT NULL
  AND GREATEST(COALESCE(i.endDateWCMFaculty, 0), COALESCE(i.endDateWCMStudent, 0)) > 0;

-- Banded verification: compare against the counts in the header comment.
SELECT COUNT(*)                                   AS backfilled,
       SUM(top_years_after_wcm <  0)              AS before_departure,
       SUM(top_years_after_wcm BETWEEN 0 AND 5)   AS grace_0_5,
       SUM(top_years_after_wcm BETWEEN 6 AND 10)  AS stale_6_10,
       SUM(top_years_after_wcm BETWEEN 11 AND 20) AS stale_11_20,
       SUM(top_years_after_wcm >  20)             AS stale_20_plus
FROM authorship_review
WHERE status = 'open' AND top_years_after_wcm IS NOT NULL;
