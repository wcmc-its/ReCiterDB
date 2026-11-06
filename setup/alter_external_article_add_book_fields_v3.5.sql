-- =============================================================================
-- Migration: external_article book / CV fields (v3.5)
-- =============================================================================
-- Adds, all NULL:
--   isbn            VARCHAR(128)  comma-joined ISBNs (Crossref rawRecord ISBN, else the
--                                 Scopus authorship_review.isbn for that document)
--   publisher       VARCHAR(255)  Crossref rawRecord publisher
--   editors         TEXT          JSON array of "Given Surname" (Crossref rawRecord editor)
--   byline          VARCHAR(1000) "Surname GA, ((Person GA)), ..." built from the stored
--                                 author list, the person wrapped like PubMed rows
--   author_position VARCHAR(20)   'first' / 'last' / NULL, from the same match
--
-- WHY: a manually added external record (PM Add tab) has no authorship_review row, so
--   STEP 6b gave it no byline and Scholars showed it with no authors. 6b now falls
--   back to byline/author_position. isbn/publisher/editors are what a book citation
--   (CV, bibliography) needs and were only in DynamoDB's rawRecord.
--
-- ORDERING: merging the code first is harmless. update/retrieveExternalArticles.py
--   writes only the columns the live table has, and 6b's fallback reads them through a
--   column check, so before this DDL the nightly behaves exactly as before.
--
-- Apply directly to BOTH reciterdb instances (producer + the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL. Then reload the STEP 6b
--   procedure (populateAnalysisSummaryTables_v2.sql) on both.
-- =============================================================================

ALTER TABLE external_article
  ADD COLUMN IF NOT EXISTS `isbn`            VARCHAR(128)  NULL,
  ADD COLUMN IF NOT EXISTS `publisher`       VARCHAR(255)  NULL,
  ADD COLUMN IF NOT EXISTS `editors`         TEXT          NULL,
  ADD COLUMN IF NOT EXISTS `byline`          VARCHAR(1000) NULL,
  ADD COLUMN IF NOT EXISTS `author_position` VARCHAR(20)   NULL;

-- Verify:
SELECT column_name FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'external_article'
  AND column_name IN ('isbn', 'publisher', 'editors', 'byline', 'author_position');
