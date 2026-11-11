-- =============================================================================
-- external_record — system of record for external and manual works (v3.6)
-- =============================================================================
-- One row per person per source record (Scopus, OpenAlex, Crossref, WoS, a manual
-- entry, later ORCID). Publication Manager writes it directly (PM #1104, phases
-- 3b-3e). Until the cutover, update/retrieveExternalArticles.py syncs DynamoDB
-- ExternalArticle into it (sync_origin = 'DYNAMO'); a row PM has written
-- (pm_touched = 1) is never overwritten by that sync.
--
-- Unlike external_article (a TRUNCATE-and-reload projection rebuilt every night),
-- this table is durable curator state. It must NEVER be added to a truncate list.
-- external_article is rebuilt FROM it.
--
-- History lives in external_record_event, rejection memory in external_rejection,
-- "Different papers" verdicts in external_dup_verdict.
--
-- Prod runs MariaDB 11.4, the dev instance 10.6: JSON is LONGTEXT + json_valid there,
-- and the STORED generated column below is supported (and indexable) on both.
-- =============================================================================
CREATE TABLE IF NOT EXISTS `external_record` (
  `id`                     BIGINT        NOT NULL AUTO_INCREMENT,  -- API handle; events and merges point here
  `uid`                    VARCHAR(64)   NOT NULL,                 -- person
  `article_id`             VARCHAR(96)   NOT NULL,                 -- SCOPUS:/OPENALEX:/WOS:<id>, CROSSREF:<doi>, MANUAL:<uuid>, ORCID:<put-code>
  `source_type`            ENUM('SCOPUS','WOS','OPENALEX','CROSSREF','MANUAL','ORCID') NOT NULL,

  -- Lifecycle
  `status`                 ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NOT NULL DEFAULT 'PENDING',
  `prev_status`            ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL,  -- one-step Undo
  `status_by`              VARCHAR(64)   NULL,                     -- curator cwid
  `status_impersonated_by` VARCHAR(64)   NULL,                     -- real admin cwid under "View as"
  `status_at`              DATETIME(3)   NULL,
  `added_by`               VARCHAR(64)   NULL,
  `added_at`               DATETIME(3)   NULL,
  `provenance`             ENUM('ORCID_SELF','ORCID_CROSSREF','ORCID_SCOPUS','SCOPUS_AUID','OPENALEX_AUTHOR','MANUAL','ADD_SEARCH','AUTHORSHIPS') NULL,
  `method`                 VARCHAR(64)   NULL,                     -- DynamoDB free-text method, verbatim
  `merged_into_id`         BIGINT        NULL,                     -- another external_record (MERGED / AUTOMERGED)
  `merged_into_pmid`       BIGINT        NULL,                     -- PubMed record it was merged into
  `superseded_by_pmid`     BIGINT        NULL,                     -- PubMed twin (SUPERSEDED)

  -- Identifiers
  `pmid`                   BIGINT        NULL,
  `doi`                    VARCHAR(255)  NULL,
  -- Lower-cased, https://(dx.)doi.org/ and doi: prefixes stripped: the duplicate and
  -- rejection-memory key. Generated, so no writer can store it differently.
  `doi_norm`               VARCHAR(255)  AS (NULLIF(REGEXP_REPLACE(LOWER(TRIM(`doi`)), '^(https?://(dx\\.)?doi\\.org/|doi:[[:space:]]*)', ''), '')) STORED,
  `isbn`                   VARCHAR(128)  NULL,                     -- comma-joined
  `scopus_eid`             VARCHAR(64)   NULL,
  `openalex_id`            VARCHAR(64)   NULL,
  `orcid_put_code`         VARCHAR(32)   NULL,
  `url`                    VARCHAR(1024) NULL,

  -- Citation
  `title`                  TEXT          NULL,
  `title_year_key`         CHAR(40)      NULL,                     -- SHA-1 hex of normTitle(title) + year, written by the writer
  `pub_type`               VARCHAR(40)   NULL,                     -- PM 18-type taxonomy (pubTypes.ts); NULL = unmapped, curator must pick
  `source_pub_type`        VARCHAR(64)   NULL,                     -- source's raw publication_type
  `type_notice`            ENUM('erratum','retraction') NULL,      -- not-counted mappings
  `peer_reviewed`          TINYINT       NULL,                     -- NULL = the type default (peerReviewedOf)
  `peer_reviewed_by_hand`  TINYINT       NOT NULL DEFAULT 0,
  `role`                   ENUM('Author','Editor','Section editor','Translator') NULL,  -- NULL = defaultRole(type)
  `pub_date`               VARCHAR(32)   NULL,
  `date_precision`         ENUM('YEAR','MONTH','DAY') NULL,
  `container_title`        VARCHAR(512)  NULL,                     -- external_article.journal_or_venue
  `publisher`              VARCHAR(255)  NULL,
  `volume`                 VARCHAR(64)   NULL,
  `issue`                  VARCHAR(64)   NULL,
  `pages`                  VARCHAR(64)   NULL,
  `fields_json`            JSON          NULL,                     -- editors, place, edition, conference, report number, version, degree, kind, audience, living, updated
  `authors_json`           JSON          NULL,                     -- [{given, surname}] in byline order
  `target_author_index`    SMALLINT      NULL,                     -- 1-based position of the person; NULL = not set
  `author_count`           SMALLINT      NULL,
  `byline`                 VARCHAR(1000) NULL,                     -- STEP 6b fallback, as external_article.byline
  `raw_record`             LONGTEXT      NULL,                     -- DynamoDB rawRecord, kept for re-mapping

  -- Sync bookkeeping
  `sync_origin`            ENUM('DYNAMO','PM') NOT NULL DEFAULT 'PM',
  `pm_touched`             TINYINT       NOT NULL DEFAULT 0,       -- set by every PM write; the DynamoDB sync never overwrites a touched row
  `created_at`             DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`             DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_er_uid_article` (`uid`, `article_id`),
  KEY `ix_er_uid_status` (`uid`, `status`),
  KEY `ix_er_doi_norm` (`doi_norm`),
  KEY `ix_er_uid_title_year` (`uid`, `title_year_key`),
  KEY `ix_er_pmid` (`pmid`)
-- COLLATE is load-bearing: every join partner (external_article, analysis_summary_*,
-- person) is unicode_ci. See alter_fix_external_article_collation_v1.8.sql.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
