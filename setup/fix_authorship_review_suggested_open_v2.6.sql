-- #160 remediation: retire the already-curated ('suggested') rows still sitting
-- open in a curator's queue.
--
-- The code fix stops _db_rows from EMITTING a row for an authorship already at
-- the production final score (>= STORAGE_THRESHOLD), because such an authorship
-- is a resolved attribution, not a curation record. It does NOT heal the rows
-- already in the table, and nothing else will: aar_db.upsert only refreshes rows
-- the producer re-emits, and after this change it never re-emits these. The four
-- other paths that touch authorship_review all miss them too --
--   * run() blocks re-explosion for any logged pmid (`already =
--     store.processed_pmids()` has no status filter),
--   * run_backfill selects last_status == 'attributed', not 'suggested',
--   * recheck_open_scopus is `WHERE source='scopus' AND status='open'`,
--   * targeted_authors_backfill.py is `WHERE source='scopus'` and only COALESCEs
--     NULL issn/isbn,
--   * _recheck mutates the CSV ledger only, never this table.
-- Same non-self-healing property v2.4 and v2.5 document. Without this UPDATE the
-- reported symptom -- a publication already in the person's GoldStandard at
-- fg=100 showing in the queue as if unassigned -- stays on screen indefinitely.
--
-- Measured on prod 2026-08-28, immediately before writing this file:
--   source='pubmed', classification='suggested'   660 rows
--     of those status='accepted'                  642   (terminal, invisible in PM)
--     of those status='assigned'                   15   (terminal)
--     of those status='open'                        3   <- the whole blast radius
--   the three:
--     60859  pmid 42445022  mog4005  fg=100.00
--     60881  pmid 42440345  mog4005  fg=100.00
--     60884  pmid 42438162  koj2001  fg= 63.83
--   all first_seen 2026-08-23 (the last pre-fix weekly sweep)
--
-- Three rows, not a migration-scale repair -- but shipped with the code change
-- rather than left as a manual follow-up, because a remediation nobody runs is
-- the same as no remediation.
--
-- status='open' is the guard: a curator decision (accepted/assigned/rejected/
-- dismissed/snoozed) is never touched. Run against dev first, then prod.
-- Idempotent -- re-running matches nothing.

-- Dry run first; expect the same count the UPDATE reports.
-- SELECT id, pmid, top_cwid, top_fg_score, first_seen
-- FROM authorship_review
-- WHERE source = 'pubmed' AND classification = 'suggested' AND status = 'open';

UPDATE authorship_review
SET status      = 'dismissed',
    resolved_at = NOW(),
    note        = CONCAT(COALESCE(CONCAT(note, ' | '), ''),
                         'auto #160: already attributed in production at ',
                         'Authorship Score ', COALESCE(top_fg_score, '?'),
                         ' -- resolved, not a curation record')
WHERE source = 'pubmed'
  AND classification = 'suggested'
  AND status = 'open';

-- Verify: expect 0 rows.
-- SELECT COUNT(*) FROM authorship_review
-- WHERE source = 'pubmed' AND classification = 'suggested' AND status = 'open';
