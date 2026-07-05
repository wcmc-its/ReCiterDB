-- =============================================================================
-- external_article — reporting projection of the ExternalArticle DynamoDB table
-- =============================================================================
-- Manually-added non-PubMed publications (Publication Manager "Add publication" ->
-- OpenAlex / Scopus, ReCiter #661/#662). This MySQL table is a pure PROJECTION of the
-- DynamoDB `ExternalArticle` table, refreshed by update/retrieveExternalArticles.py
-- (truncate + reload each nightly run) so external pubs surface in reciterdb reporting
-- (ReCiterDB #101).
--
-- Unlike `authorship_review` (durable curator state), this is a rebuildable projection:
-- the source of truth is DynamoDB, so it IS truncated + reloaded nightly.
--
-- `suppressed` rows (a Scopus/OpenAlex record later superseded by its PubMed twin, same
-- DOI) are KEPT here with the flag so reporting can include or exclude them; exclude
-- them from person publication counts to avoid double-counting the PubMed version.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `external_article` (
  `uid`                 VARCHAR(64)  NOT NULL,
  `article_id`          VARCHAR(96)  NOT NULL,            -- SCOPUS:/OPENALEX:/WOS:<id>
  `source_type`         ENUM('SCOPUS','WOS','OPENALEX') NOT NULL,
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
  PRIMARY KEY (`uid`, `article_id`),
  KEY `ix_source` (`source_type`),
  KEY `ix_doi` (`doi`),
  KEY `ix_suppressed` (`suppressed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
