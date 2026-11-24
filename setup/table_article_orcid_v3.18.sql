-- =============================================================================
-- Migration: pubsource_article_orcid (v3.18)
-- =============================================================================
-- The ORCID iD Crossref prints next to a person's own name, per paper. Written by
-- update/collectArticleOrcids.py for active scholars' accepted and rejected papers where
-- PubMed prints no iD. Reader: Scholars Profile System etl/orcid-evidence.
--
-- One row per (person, paper, source) ASKED. `orcid` NULL = asked, and that source prints
-- no iD for that author. No row = not asked yet, or the request failed. So the table is
-- its own ledger, per person-slot rather than per DOI: a person who becomes the target of
-- a paper already fetched for a coauthor is asked for too.
--
-- READERS: rows are never deleted. Count a row only while person_article_author still marks
-- the same person and rank as the target (targetAuthor LIKE '1%', rank = author_rank) and
-- person_article.targetAuthorCount <= 1; take the assertion from person_article. A reader
-- that lets an iD RAISE a match must also check that the backfill is complete (row count):
-- a missing row can be the rejected paper that contests the iD.
--
-- Not in updateReciterDB.py's truncate/swap list and must never be added: truncating it
-- would refetch every paper.
--
-- ORDERING: apply before setting ARTICLE_ORCID=apply on the CronJob. Merging the PR first is
--   harmless: the hook is off by default and --apply refuses without the table. Idempotent,
--   CREATE only. Merging the PR does NOT run DDL.
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/table_article_orcid_v3.18.sql
-- Rollback: unset ARTICLE_ORCID, then DROP TABLE pubsource_article_orcid;
-- =============================================================================
CREATE TABLE IF NOT EXISTS `pubsource_article_orcid` (
  `personIdentifier` VARCHAR(128) NOT NULL,
  `pmid`             INT          NOT NULL,
  `source`           VARCHAR(16)  NOT NULL,   -- 'crossref'
  `author_rank`      INT          NOT NULL,   -- person_article_author.rank when asked
  `doi`              VARCHAR(255) NOT NULL,   -- the DOI asked (lower-cased)
  `orcid`            CHAR(19)     NULL,       -- 0000-0000-0000-000X, or NULL
  `authenticated`    TINYINT      NULL,       -- Crossref authenticated-orcid; NULL with no iD
  `found`            TINYINT      NOT NULL,   -- 1 = the source has a record for the DOI
  `fetched_at`       DATETIME     NOT NULL,
  PRIMARY KEY (`personIdentifier`, `pmid`, `source`),
  KEY `idx_pao_orcid` (`orcid`)
-- COLLATE is load-bearing: it must match person_article_author.personIdentifier.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Verification
SELECT column_name, column_type, is_nullable FROM information_schema.columns
 WHERE table_schema = DATABASE() AND table_name = 'pubsource_article_orcid' ORDER BY ordinal_position;
