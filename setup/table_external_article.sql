-- =============================================================================
-- external_article — reporting projection of external and manual works
-- =============================================================================
-- Manually-added non-PubMed publications (Publication Manager "Add publication" ->
-- OpenAlex / Scopus / Crossref, ReCiter #661/#662, and Authorships accepts). Since v3.6
-- (PM #1104, plan R2) update/retrieveExternalArticles.py first syncs the DynamoDB
-- `ExternalArticle` table into the durable `external_record` table, then rebuilds this
-- table from it (truncate + reload each nightly run) so external pubs surface in
-- reciterdb reporting (ReCiterDB #101). Before v3.6 it was loaded straight from DynamoDB.
--
-- Unlike `authorship_review` and `external_record` (durable curator state), this is a
-- rebuildable projection, so it IS truncated + reloaded nightly. Never add lifecycle
-- state here: it lives in external_record.
--
-- `status` (v3.6) is the external_record lifecycle state. REMOVED rows are left out;
-- every other state is loaded, with `suppressed` = (status <> 'ACCEPTED'). Suppressed
-- rows (e.g. a Scopus/OpenAlex record superseded by its PubMed twin, same DOI) are KEPT
-- with the flag so reporting can include or exclude them; exclude them from person
-- publication counts to avoid double-counting the PubMed version.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `external_article` (
  `uid`                 VARCHAR(64)  NOT NULL,
  `article_id`          VARCHAR(96)  NOT NULL,            -- SCOPUS:/OPENALEX:/WOS:<id>, CROSSREF:<lowercased doi>
  `source_type`         ENUM('SCOPUS','WOS','OPENALEX','CROSSREF','MANUAL','ORCID') NOT NULL,
  `doi`                 VARCHAR(255) NULL,
  `pmid`                BIGINT       NULL,                -- normally absent; blocked at add time
  `title`               TEXT         NULL,
  `journal_or_venue`    VARCHAR(512) NULL,
  `authors`             TEXT         NULL,                -- JSON array (source-provided)
  `pub_date`            VARCHAR(32)  NULL,                -- source-provided (yyyy or yyyy-MM-dd)
  `publication_type`    VARCHAR(64)  NULL,
  `added_by`            VARCHAR(64)  NULL,
  `date_added`          VARCHAR(32)  NULL,
  `method`              VARCHAR(64)  NULL,
  `suppressed`          TINYINT(1)   NOT NULL DEFAULT 0,
  `superseded_by_pmid`  BIGINT       NULL,
  `status`              ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL,  -- v3.6: external_record lifecycle state
  `isbn`                VARCHAR(128) NULL,                -- v3.5: comma-joined ISBNs
  `publisher`           VARCHAR(255) NULL,                -- v3.5
  `editors`             TEXT         NULL,                -- v3.5: JSON array of names
  `byline`              VARCHAR(1000) NULL,               -- v3.5: STEP 6b fallback byline
  `author_position`     VARCHAR(20)  NULL,                -- v3.5: 'first'/'last'/NULL
  PRIMARY KEY (`uid`, `article_id`),
  KEY `ix_source` (`source_type`),
  KEY `ix_doi` (`doi`),
  KEY `ix_suppressed` (`suppressed`)
-- COLLATE is load-bearing: `CHARSET=utf8mb4` alone means general_ci, but every join
-- partner (analysis_summary_*, person) is unicode_ci. STEP 6b's join then raises
-- "Illegal mix of collations" — which 6b swallows, so the nightly reports SUCCESS
-- having injected zero external rows. See alter_fix_external_article_collation_v1.8.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
