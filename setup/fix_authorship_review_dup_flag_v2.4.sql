-- #158 remediation: clear the per-article dup_flag chips already written to
-- authorship_review.
--
-- The code fix makes dup_flag a per-authorship test going forward, but it does
-- NOT heal the rows already in the table. dup_flag/dup_reason are in
-- aar_db._REFRESH_COLS, so they are rewritten whenever the producer re-emits a
-- row -- but the scopus lane's recurring sweep is a rolling ORIG-LOAD-DATE
-- window [today-90d, today-14d], and Scopus load date is immutable per
-- document. Once a document falls out of that window it never re-enters, so its
-- row is never re-emitted. recheck_open_scopus() revisits old rows but only
-- writes status/resolved_at/note, never dup_flag. 98% of the wrong chips were
-- first seen in the 2026-07-04/05 --mode initial backfill, whose load dates run
-- 2021-05..2026-05 -- permanently outside the rolling window.
--
-- So without this UPDATE the reported symptom stays on screen indefinitely.
--
-- Measured on prod 2026-08-28, immediately before writing this file:
--   dup_flag=1 rows                        2,642
--   of those, dup_reason names a uid that
--   is NOT a candidate on its own row      1,349  (51.1%)
--   of those wrong chips, status='open'      735
--   correct chips that are currently open      0
-- i.e. every dup_flag=1 row in a curator's queue today is a false positive.
--
-- The predicate is the SQL twin of aar_db.dup_uid_for_authorship(): a chip
-- survives only if the uid it names is this row's top_cwid or appears in its
-- candidate_cwids_json. Correct chips (1,293 rows, none of them open) are left
-- untouched rather than blanket-cleared.
--
-- Run against dev first, then prod. Idempotent -- re-running matches nothing.

-- Dry run first; expect the same counts as the UPDATE reports.
-- SELECT COUNT(*) AS n, SUM(status='open') AS n_open
-- FROM authorship_review
-- WHERE dup_flag = 1
--   AND JSON_SEARCH(candidate_cwids_json, 'one',
--         SUBSTRING_INDEX(SUBSTRING_INDEX(dup_reason, 'for ', -1), ' (DOI', 1)) IS NULL
--   AND (top_cwid IS NULL
--        OR top_cwid <> SUBSTRING_INDEX(SUBSTRING_INDEX(dup_reason, 'for ', -1), ' (DOI', 1));

UPDATE authorship_review
SET dup_flag = 0,
    dup_reason = NULL
WHERE dup_flag = 1
  AND JSON_SEARCH(candidate_cwids_json, 'one',
        SUBSTRING_INDEX(SUBSTRING_INDEX(dup_reason, 'for ', -1), ' (DOI', 1)) IS NULL
  AND (top_cwid IS NULL
       OR top_cwid <> SUBSTRING_INDEX(SUBSTRING_INDEX(dup_reason, 'for ', -1), ' (DOI', 1));
