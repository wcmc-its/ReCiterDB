-- =============================================================================
-- v2.8 — authorship_review: reopen the pre-#191 title-dismissed backlog
-- =============================================================================
-- WHY: between 2026-08-16 and 2026-08-30, aar_universe_scopus.py's
-- recheck_open_scopus() (update/aar_universe_scopus.py:503-506, pre-#191) auto-
-- DISMISSED 345 open Scopus rows on an unquoted `<title>[ti] AND <surname>[au]`
-- esearch hit, writing status='dismissed', resolved_at=:ts, note='auto: now in
-- PubMed (title match)' -- with no record of which PubMed record it believed
-- matched and no curator ever adjudicating the call. ReCiterDB #191 changes this
-- going forward: a title hit now only sets matched_pmid/matched_pmid_source='title'
-- and leaves status/note untouched, so a curator decides. The 345 rows dismissed
-- before #191 shipped are outside that fix's reach -- this migration is the
-- one-time backlog catch-up.
--
-- PRECONDITIONS:
--   (a) **v2.7 applied** (setup/alter_authorship_review_add_matched_pmid_v2.7.sql --
--       this file assumes the matched_pmid* columns and collation fix already exist).
--   (b) **ReCiterDB #191 merged AND its image live in the `reciterdb` CronJob.**
--       Under the OLD image, the next Sunday recheck_open_scopus() run would simply
--       re-dismiss every row this migration reopens.
--   (c) **PM #956 live** so a flagged row has an adjudication UI for curators to act on.
--
-- WHAT HAPPENS NEXT: once reopened, these rows are ordinary open Scopus rows again
-- (status='open' AND matched_pmid IS NULL) -- the next Sunday recheck_open_scopus()
-- SELECT (update/aar_universe_scopus.py:550 as of #191, no date window) picks every one of them
-- up, stamps matched_pmid/matched_pmid_source='title'/matched_pmid_at, and leaves them
-- open, moving them into PM's duplicates/adjudication view for a curator's verdict.
--
-- Idempotent: the note filter excludes any row already touched by this migration, so
-- a re-run reopens 0 rows.
--
-- Apply with:
--   MYSQL_PWD="$DB_PASSWORD" mysql -h "$DB_HOST" -u "$DB_USERNAME" "$DB_NAME" \
--     < setup/fix_authorship_review_reopen_title_dismissed_v2.8.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Pre-count (expected 344 on prod as of 2026-09-01: 345 rows carry the note; one
-- has reviewer='paa2013' set and is excluded -- a human already looked at it).
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS target_rows
FROM authorship_review
WHERE source='scopus' AND status='dismissed'
  AND reviewer IS NULL AND resolution_cwid IS NULL
  AND note LIKE 'auto: now in PubMed (title match)%'
  AND note NOT LIKE '%reopened by v2.8%';

UPDATE authorship_review
   SET status='open', resolved_at=NULL,
       note=CONCAT(note, ' | reopened by v2.8 for #951 adjudication')
 WHERE source='scopus' AND status='dismissed'
   AND reviewer IS NULL AND resolution_cwid IS NULL
   AND note LIKE 'auto: now in PubMed (title match)%'
   AND note NOT LIKE '%reopened by v2.8%';

-- -----------------------------------------------------------------------------
-- Post-count: target rows remaining -> 0; rows this migration reopened -> 344.
-- -----------------------------------------------------------------------------
SELECT COUNT(*) AS target_rows_remaining
FROM authorship_review
WHERE source='scopus' AND status='dismissed'
  AND reviewer IS NULL AND resolution_cwid IS NULL
  AND note LIKE 'auto: now in PubMed (title match)%'
  AND note NOT LIKE '%reopened by v2.8%';

SELECT COUNT(*) AS rows_reopened
FROM authorship_review
WHERE status='open' AND note LIKE '%reopened by v2.8%';
