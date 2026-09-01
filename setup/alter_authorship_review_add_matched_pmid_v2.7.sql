-- =============================================================================
-- v2.7 — authorship_review: fix collation, persist the matched PMID (ReCiter #951)
-- =============================================================================
-- PART 1 — collation fix (same defect class as v1.8's external_article fix)
--
-- `authorship_review` was created with `DEFAULT CHARSET=utf8mb4` and no COLLATE
-- clause, which resolves to utf8mb4_general_ci rather than inheriting the schema
-- default. Every table the #951 work needs to join against — external_article (fixed
-- to utf8mb4_unicode_ci by v1.8), analysis_summary_article, person_article, person —
-- is utf8mb4_unicode_ci. Reproduced live 2026-09-01:
--     SELECT ... FROM authorship_review ar
--       JOIN external_article ea ON ar.external_id = SUBSTRING(ea.article_id,8);
--     ERROR 1267 (HY000): Illegal mix of collations (utf8mb4_general_ci,IMPLICIT)
--     and (utf8mb4_unicode_ci,IMPLICIT) for operation '='
--
-- authorship_review is durable curator state (see setup/table_authorship_review.sql's
-- header), not a rebuildable projection — CONVERT (not a bare attribute change)
-- rewrites the stored column bytes in place and preserves every row, the same v1.8
-- precedent. Fixing the table, rather than adding COLLATE casts into each query, fixes
-- every present and future join against it, not just the one #951 needs today.
--
-- PART 2 — matched_pmid / matched_pmid_source / matched_pmid_at / matched_pmid_verdict
--
-- Layer 1 of #951: aar_universe_scopus.py's Scopus lane resolves DOI-less documents
-- against PubMed by an unquoted title+author esearch heuristic (resolve_no_doi /
-- title_in_pubmed) when no DOI is available. Until this migration, a heuristic hit
-- silently DISMISSED the row — at ingest the document was dropped before a row was
-- ever written; on the weekly recheck an existing open row was dismissed with
-- note='auto: now in PubMed (title match)' — with no record of WHICH PubMed record the
-- producer believed matched. An unreviewable auto-decision riding on a fuzzy signal.
-- These columns let the producer propose a match without deciding it:
--
--   matched_pmid          BIGINT NULL  — the PubMed record the producer believes is
--                                        the same work as this Scopus authorship.
--   matched_pmid_source   ENUM('scopus','doi','title') NULL — how it was found:
--                            scopus — Scopus's own `pubmed-id` field. RESERVED: not
--                                     yet written by the producer (a doc carrying it is
--                                     dropped in run() step 2, before any row exists).
--                            doi    — exact `"<doi>"[DOI]` esearch.
--                            title  — the unquoted `<title>[ti] AND <surname>[au]`
--                                     heuristic.
--   matched_pmid_at        DATETIME NULL — when matched_pmid was set.
--   matched_pmid_verdict   ENUM('same','distinct') NULL — the CURATOR's answer, set
--                            ONLY by Publication Manager, never by this producer:
--                              same     — confirmed the same work; the row is
--                                         dismissed by PM.
--                              distinct — confirmed a genuinely separate work; the
--                                         producer must never re-flag this row again.
--                            recheck_open_scopus's SELECT excludes any row with
--                            matched_pmid NOT NULL, which covers an unreviewed flag
--                            and a 'distinct' verdict alike — a verdict only ever sits
--                            on a non-NULL matched_pmid.
--
-- A doi-source hit stays reliable enough to auto-resolve — dismissed immediately, same
-- as before this migration, now with matched_pmid/matched_pmid_source='doi' recorded
-- alongside the note. Only the title heuristic stops auto-deciding.
--
-- Measured on prod 2026-09-01, immediately before writing this file:
--   source='scopus' rows                                                    10,527
--   of those status='open'                                                   4,717
--   dismissed by the title heuristic
--     (note LIKE 'auto: now in PubMed (title match)%')                         345
--   dismissed by the DOI path (two note spellings coexist — older jobs wrote
--     'auto: DOI now in PubMed (<doi>)', current jobs write
--     'auto: now in PubMed (<doi>)')                                            21
-- The 345 already-dismissed title-match rows are NOT touched by this migration or by
-- the Layer 1 code change — they are history, not reopened. Only future title-
-- heuristic hits (new ingests and the next recheck sweep) start persisting
-- matched_pmid instead of silently dismissing.
--
-- APPLY BEFORE MERGE. A ReCiterDB merge to master auto-deploys the CronJob image; the
-- producer's aar_db.py._INSERT_COLS gains three new column names the instant that
-- image ships, and an INSERT naming a column the live table doesn't have yet fails
-- outright. This migration must be applied to prod BY HAND (foreman, user-gated)
-- BEFORE the branch merges — same DDL-then-code-deploy ordering v2.5/v2.6 document.
--
-- The fresh-build schema (setup/table_authorship_review.sql) is updated in the same
-- PR; this migration brings the EXISTING prod database up to that schema.
--
-- NOT idempotent, unlike v2.5/v2.6's information_schema-guarded form: these are plain
-- ALTER TABLE statements, so re-running errors with "Duplicate column name". Safe
-- because it is applied exactly once, by hand, before merge.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/alter_authorship_review_add_matched_pmid_v2.7.sql
-- =============================================================================

ALTER TABLE authorship_review
  CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

ALTER TABLE authorship_review
  ADD COLUMN matched_pmid BIGINT NULL AFTER dup_reason,
  ADD COLUMN matched_pmid_source ENUM('scopus','doi','title') NULL AFTER matched_pmid,
  ADD COLUMN matched_pmid_at DATETIME NULL AFTER matched_pmid_source,
  ADD COLUMN matched_pmid_verdict ENUM('same','distinct') NULL AFTER matched_pmid_at,
  ADD KEY ix_matched_pmid (matched_pmid),
  ADD KEY ix_doi (doi);

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SHOW CREATE TABLE authorship_review;

SELECT column_name, column_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name IN ('matched_pmid', 'matched_pmid_source', 'matched_pmid_at',
                       'matched_pmid_verdict')
ORDER BY ordinal_position;
