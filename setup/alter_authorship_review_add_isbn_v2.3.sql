-- =============================================================================
-- Migration: authorship_review add isbn (v2.3)
-- =============================================================================
-- Adds:
--   - isbn   VARCHAR(128) NULL  — comma-joined ISBN list (Scopus returns several per
--                                 book: print/ebook editions, ISBN-10 and ISBN-13).
--                                 The producer joins _extract_isbns() with ','; the
--                                 recheck splits on ',' — no comma inside an ISBN.
--
-- WHY THIS MIGRATION EXISTS:
--   Book-like Scopus documents are ISBN-keyed, not journal-keyed: they carry no
--   prism:issn at all, so the `issn` column added in v2.1 is structurally NULL for
--   them and the dead-journal pre-filter can never reach them. They are most of the
--   queue — measured on prod 2026-08-28, Book Chapter + Book + Editorial are 573 of
--   the 729 open no-DOI scopus rows (79%), and 15/15 sampled documents of each of
--   those three types carried an ISBN.
--
--   resolve_no_doi() has taken an `isbns` argument since v2.1 and the ingest path
--   (run(), which still holds the raw Scopus doc) passes it. recheck_open_scopus()
--   could not: it works from the stored row, and there was no column to store the
--   ISBN in, so it silently passed the default None and every backlog row fell
--   through to the title search. This column closes that ingest/recheck asymmetry —
--   the same asymmetry v2.1 closed for ISSN. See issue #157.
--
--   SIZE THE BENEFIT HONESTLY BEFORE APPLYING. resolve_no_doi consults ISBN only for
--   pub_type='Book' (18 of the 729 rows), and a live probe on 2026-08-28 found 0 of
--   53 sampled ISBN-carrying backlog documents (18 Book, 25 Book Chapter, 10
--   Editorial) hit PubMed on ANY of their ISBNs — the current backlog is the residue
--   the weekly recheck has already failed to resolve, so this column buys ~0
--   dismissals on it TODAY. Its value is forward-looking (new Book rows get the same
--   check at recheck time that ingest already applies — the ingest probe of
--   2026-08-14, n=26, resolved 3.8% on ISBN alone) plus the stored identifier itself,
--   which is the only venue id a book-like row ever has.
--
--   The fresh-build schema (setup/table_authorship_review.sql) is updated in the same
--   PR. This migration brings EXISTING databases up to that schema. It must be applied
--   directly to BOTH reciterdb instances — the producer instance and the separate dev
--   instance behind reciter-pm-dev (loaded manually) — same as v1.6, v2.1 and v2.2.
--   Merging the PR does NOT run DDL.
--
-- DURABILITY: authorship_review is curator state, not a reporting export — not in
--   update/updateReciterDB.py's truncate list, not touched by any nightly ETL step.
--
-- Additive only, guarded by an information_schema check (no-op on re-run). Existing
-- rows get isbn=NULL; the producer fills it on the next upsert of a given row. Rows
-- from the one-time 2026-07-05 --mode initial backfill are never re-swept by the
-- recurring cron (it sweeps a recent ORIG-LOAD-DATE window only), so they need
-- update/targeted_authors_backfill.py --apply, which looks each document up by its
-- stored doi/external_id and fills issn/isbn in place.
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- isbn VARCHAR(128) NULL — comma-joined ISBN list
-- -----------------------------------------------------------------------------
SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'authorship_review'
       AND column_name = 'isbn') = 0,
    'ALTER TABLE authorship_review ADD COLUMN `isbn` VARCHAR(128) NULL',
    'SELECT ''authorship_review.isbn already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
SELECT column_name, data_type, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'authorship_review'
  AND column_name = 'isbn';
