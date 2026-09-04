-- Byline-slot lookups: Publication Manager's identity-conflicts predicate (PM #986/#990)
-- tests person_article_author by (pmid, rank). Without this index the EXISTS drives from
-- person_article.idx_pmid and costs ~1.2 s per /summary on prod (2026-09-04).
-- person_article_author is rebuilt nightly with CREATE TABLE ... LIKE, which copies indexes,
-- so applying this once on prod is durable; createDatabaseTableReciterDb.sql carries it too.
ALTER TABLE `person_article_author`
  ADD KEY `ix_pmid_rank` (`pmid`,`rank`,`personIdentifier`) USING BTREE;
