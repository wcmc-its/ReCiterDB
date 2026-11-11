-- =============================================================================
-- Migration: external and manual record lifecycle tables (v3.6)
-- =============================================================================
-- Creates four tables for Publication Manager phases 3b-3e (PM #1104):
--   external_record        system of record for every external/manual work
--   external_record_event  append-only history (Undo, audit)
--   external_rejection     per-person rejection memory
--   external_dup_verdict   "Different papers" verdicts per pair
--
-- WHY: external_article is a TRUNCATE-and-reload projection of DynamoDB, so any
--   lifecycle column added to it would be wiped every night. external_record holds
--   the durable state; a later PR (plan R2) rebuilds external_article from it.
--
-- BEHAVIOUR CHANGE: none. Nothing reads or writes these tables until plan R2
--   (ReCiterDB sync) and P1/P2 (PM). external_article is not touched here; its
--   `status` column and 'MANUAL'/'ORCID' ENUM members come with R2.
--
-- IDEMPOTENT: CREATE TABLE IF NOT EXISTS, so a second run is a no-op. The
--   information_schema checks at the end report what exists. The CREATE statements
--   are byte-identical to setup/table_external_record.sql,
--   table_external_record_event.sql, table_external_rejection.sql and
--   table_external_dup_verdict.sql (update/test_external_record_ddl.py enforces it).
--
-- Requires MariaDB 10.2+ (STORED generated columns, REGEXP_REPLACE, enforced CHECK).
--   Prod is 11.4, the dev instance 10.6.
--
-- Apply directly to BOTH reciterdb instances (the producer and the dev instance
--   behind reciter-pm-dev). Merging the PR does NOT run DDL.
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/alter_external_record_v3.6.sql
--
-- ROLLBACK: DROP TABLE external_dup_verdict, external_rejection,
--   external_record_event, external_record; (safe until R2 ships: nothing else
--   reads them).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- external_record  (same as setup/table_external_record.sql)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- external_record_event  (same as setup/table_external_record_event.sql)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `external_record_event` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT,
  `record_id`        BIGINT       NOT NULL,                  -- external_record.id
  `uid`              VARCHAR(64)  NOT NULL,
  `action`           ENUM('ADD','IMPORT','TRANSITION','EDIT','MERGE','UNMERGE') NOT NULL,
  `from_status`      ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL,
  `to_status`        ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL,
  `actor`            VARCHAR(64)  NULL,                      -- curator cwid, or 'system' for the sync
  `impersonated_by`  VARCHAR(64)  NULL,                      -- real admin cwid under "View as"
  `at`               DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `note`             TEXT         NULL,
  `diff_json`        JSON         NULL,
  PRIMARY KEY (`id`),
  KEY `ix_ere_record_at` (`record_id`, `at`),
  KEY `ix_ere_uid_at` (`uid`, `at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- external_rejection  (same as setup/table_external_rejection.sql)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `external_rejection` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `uid`          VARCHAR(64)  NOT NULL,
  `key_type`     ENUM('DOI','ORCID_PUTCODE','SOURCE_ID','TITLE_YEAR') NOT NULL,
  `key_value`    VARCHAR(255) NOT NULL,
  `record_id`    BIGINT       NULL,                        -- external_record.id that was rejected/removed
  `reason`       ENUM('REJECTED','REMOVED') NOT NULL,
  `rejected_by`  VARCHAR(64)  NULL,
  `rejected_at`  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_xr_uid_key` (`uid`, `key_type`, `key_value`),
  KEY `ix_xr_record` (`record_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- external_dup_verdict  (same as setup/table_external_dup_verdict.sql)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `external_dup_verdict` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `uid`          VARCHAR(64)  NOT NULL,
  `key_a`        VARCHAR(112) NOT NULL,
  `key_b`        VARCHAR(112) NOT NULL,
  `verdict`      ENUM('DIFFERENT') NOT NULL DEFAULT 'DIFFERENT',
  `scope_cwid`   VARCHAR(64)  NULL,
  `scope_key`    VARCHAR(64)  AS (COALESCE(`scope_cwid`, '')) STORED,
  `decided_by`   VARCHAR(64)  NULL,
  `decided_at`   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_edv_pair_scope` (`uid`, `key_a`, `key_b`, `scope_key`),
  -- Binary (code-point) order, not the table's unicode_ci order: unicode_ci sorts '_' and
  -- '(' before digits, so a pair sorted in JS ('10.1/a1b' < '10.1/a_b') would be refused.
  CONSTRAINT `ck_edv_ordered_pair` CHECK (CAST(`key_a` AS BINARY) < CAST(`key_b` AS BINARY))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------
-- Expect 4 rows, all utf8mb4_unicode_ci. A different collation means the table
-- existed before this migration with another definition: fix it before R2, or
-- joins against external_article fail ("Illegal mix of collations").
SELECT table_name, table_collation, table_rows
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name IN ('external_record', 'external_record_event',
                     'external_rejection', 'external_dup_verdict')
ORDER BY table_name;

-- Expect external_record 49, external_record_event 11, external_rejection 8,
-- external_dup_verdict 9.
SELECT table_name, COUNT(*) AS n_columns
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name IN ('external_record', 'external_record_event',
                     'external_rejection', 'external_dup_verdict')
GROUP BY table_name
ORDER BY table_name;
