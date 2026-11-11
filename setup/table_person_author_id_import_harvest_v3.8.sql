-- =============================================================================
-- Migration: author-id linkage + import-harvest bookkeeping tables (v3.8)
-- =============================================================================
-- Five new tables, nothing altered:
--
--   person_author_id       Each person's Scopus Author IDs and OpenAlex author ids,
--                          SUGGESTED / CONFIRMED / REJECTED. Written nightly by
--                          update/authorIdSuggestions.py (SUGGESTED, plus CONFIRMED with
--                          method AUTO for the strong band) and by Publication Manager's
--                          Manage Profile card (curator confirm / reject / add).
--   person_author_id_vote  Per person and id type: the fingerprint of the accepted-PMID
--                          set the last vote was taken over. person_article is rebuilt
--                          every night, so it carries no "changed since" timestamp; this
--                          is what lets the suggester re-vote only people whose accepted
--                          set changed. Suggester-owned; nothing else reads it.
--   import_harvest_run     One row per run of the nightly import harvest (3g), dry-run or
--                          apply, with per-source totals.
--   import_harvest_person  Harvest state per person, source and source id: last visit,
--                          last counts, the item-set hash, and the dry-run measurement
--                          columns (sample_json holds up to 20 would-be rows).
--   doi_pmid_cache         DOI -> PMID lookups against PubMed (pmid NULL = checked, none
--                          found), re-checked after 30 days.
--
-- Curator decisions live in person_author_id, so NONE of these tables may be added to
-- update/updateReciterDB.py's `all_tables` truncate/swap list.
--
-- ORDERING: harmless in any order with the code. authorIdSuggestions.py --dry-run works
--   without these tables (it treats them as empty); --apply refuses until
--   person_author_id and person_author_id_vote exist. The nightly hook is off unless
--   AUTHOR_ID_SUGGESTIONS is set on the CronJob.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS only. Safe to re-run. Apply directly to BOTH
--   reciterdb instances (producer + the dev instance behind reciter-pm-dev). Merging the
--   PR does NOT run DDL.
--
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" \
--     < setup/table_person_author_id_import_harvest_v3.8.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `person_author_id` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `uid`          VARCHAR(64)  NOT NULL COMMENT 'lower-cased cwid',
  `id_type`      ENUM('SCOPUS_AUID','OPENALEX_AUTHOR') NOT NULL,
  `id_value`     VARCHAR(32)  NOT NULL COMMENT 'Scopus: digits. OpenAlex: A... without the URL',
  `status`       ENUM('SUGGESTED','CONFIRMED','REJECTED') NOT NULL DEFAULT 'SUGGESTED',
  `method`       ENUM('PUB_VOTE','ORCID_LOOKUP','AUTO','CURATOR') NOT NULL,
  `votes`        SMALLINT     NOT NULL DEFAULT 0 COMMENT 'accepted papers whose target-author slot carries this id',
  `resolvable`   SMALLINT     NOT NULL DEFAULT 0 COMMENT 'accepted papers whose slot resolved to any id',
  `share`        DECIMAL(4,3) NULL     COMMENT 'votes / resolvable',
  `evidence_json` JSON        NULL     COMMENT 'up to 10 voting PMIDs, competing ids, orcid_lookup flag',
  `conflict_uid` VARCHAR(64)  NULL     COMMENT 'another uid holding this id as SUGGESTED or CONFIRMED',
  `decided_by`   VARCHAR(64)  NULL     COMMENT 'curator uid, or nightly for AUTO',
  `decided_at`   DATETIME     NULL,
  `note`         VARCHAR(500) NULL,
  `first_seen`   DATETIME     NOT NULL,
  `last_seen`    DATETIME     NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_person_author_id` (`uid`, `id_type`, `id_value`),
  KEY `idx_person_author_id_value` (`id_type`, `id_value`),
  KEY `idx_person_author_id_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `person_author_id_vote` (
  `uid`           VARCHAR(64)  NOT NULL,
  `id_type`       ENUM('SCOPUS_AUID','OPENALEX_AUTHOR') NOT NULL,
  `pmid_set_hash` CHAR(40)     NOT NULL COMMENT 'sha1 of the sorted PMIDs the vote was taken over',
  `n_pmids`       SMALLINT     NOT NULL DEFAULT 0,
  `resolvable`    SMALLINT     NOT NULL DEFAULT 0,
  `top_id`        VARCHAR(32)  NULL,
  `voted_at`      DATETIME     NOT NULL,
  PRIMARY KEY (`uid`, `id_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `import_harvest_run` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT,
  `mode`        ENUM('DRY_RUN','APPLY') NOT NULL,
  `status`      ENUM('running','ok','degraded','failed') NOT NULL DEFAULT 'running',
  `sources`     VARCHAR(64)  NULL COMMENT 'e.g. orcid,scopus,openalex',
  `cohort`      VARCHAR(255) NULL COMMENT 'S3 cohort key or all-confirmed',
  `shard`       TINYINT      NULL COMMENT 'crc32(uid) % 7 visited this run',
  `started_at`  DATETIME     NOT NULL,
  `finished_at` DATETIME     NULL,
  `totals`      JSON         NULL COMMENT 'per-source counters',
  `note`        TEXT         NULL COMMENT 'why a run degraded or failed',
  PRIMARY KEY (`id`),
  KEY `idx_import_harvest_run_started` (`started_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `import_harvest_person` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `uid`             VARCHAR(64)  NOT NULL,
  `source`          ENUM('ORCID','SCOPUS_AUID','OPENALEX_AUTHOR') NOT NULL,
  `source_id`       VARCHAR(64)  NOT NULL COMMENT 'ORCID iD, Scopus AU-ID or OpenAlex A-id',
  `last_run_id`     BIGINT       NULL,
  `last_visit_at`   DATETIME     NULL,
  `last_ok_at`      DATETIME     NULL,
  `last_full_at`    DATETIME     NULL COMMENT 'last full (non-incremental) pull. Scopus re-pulls fully every 90 days',
  `last_status`     ENUM('ok','failed','capped','skipped') NULL,
  `item_set_hash`   CHAR(40)     NULL COMMENT 'sha1 of the sorted put-codes / work ids last seen',
  `fetched`                      INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_scope`                INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_pubmed`               INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_pubmed_accepted`      INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_pubmed_rejected`      INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_pubmed_candidate`     INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_pubmed_not_retrieved` INT UNSIGNED NOT NULL DEFAULT 0,
  `skipped_rejected`             INT UNSIGNED NOT NULL DEFAULT 0,
  `existing`                     INT UNSIGNED NOT NULL DEFAULT 0,
  `automerged`                   INT UNSIGNED NOT NULL DEFAULT 0,
  `new_pending`                  INT UNSIGNED NOT NULL DEFAULT 0,
  `hidden_type`                  INT UNSIGNED NOT NULL DEFAULT 0,
  `affil_known`                  INT UNSIGNED NOT NULL DEFAULT 0,
  `affil_unfamiliar`             INT UNSIGNED NOT NULL DEFAULT 0,
  `affil_none`                   INT UNSIGNED NOT NULL DEFAULT 0,
  `capped`          TINYINT(1)   NOT NULL DEFAULT 0,
  `sample_json`     JSON         NULL COMMENT 'up to 20 would-be rows (dry run)',
  `error`           VARCHAR(500) NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_import_harvest_person` (`uid`, `source`, `source_id`),
  KEY `idx_import_harvest_person_run` (`last_run_id`),
  KEY `idx_import_harvest_person_status` (`source`, `last_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `doi_pmid_cache` (
  `doi`        VARCHAR(255) NOT NULL COMMENT 'lower-cased, no https://doi.org/ prefix',
  `pmid`       INT          NULL     COMMENT 'NULL = checked, PubMed has no record for this DOI',
  `checked_at` DATETIME     NOT NULL,
  PRIMARY KEY (`doi`),
  KEY `idx_doi_pmid_cache_checked` (`checked_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Verify: five rows.
SELECT table_name, table_collation
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name IN ('person_author_id', 'person_author_id_vote', 'import_harvest_run',
                     'import_harvest_person', 'doi_pmid_cache')
ORDER BY table_name;
