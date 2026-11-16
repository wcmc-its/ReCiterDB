-- =============================================================================
-- Migration: research-security collection and watch-list match tables (v3.13)
-- =============================================================================
-- Research security dashboard (Projects/Roadmap/research-security-dashboard-plan-2026-11-16.md,
-- steps 1-2). Stores every co-author's affiliation text and each article's funder list,
-- which no other lane keeps, and the watch-list screen built from them. All five tables
-- are written only by update/collectAffiliations.py.
--
--   article_author_affiliation  Durable. One row per author affiliation string. Authors
--                               with no affiliation text have no row; the author count is
--                               article_affiliation_state.n_authors.
--   article_funder              Durable. One row per grant on the article.
--   article_affiliation_state   Durable. One row per article_id ever attempted, including
--                               the ones with no data, so nothing is refetched forever.
--                               A miss (source 'none') is tried again after 30 days, up
--                               to 4 attempts in all (`attempts`).
--   watchlist_match             Rebuilt in full every run from article_author_affiliation
--                               (staging table + RENAME, so readers never see it empty).
--   watchlist_funding_flag      Rebuilt in full every run from article_funder, swapped in
--                               by the same RENAME statement as watchlist_match.
--
-- article_id is the STABLE key: the pmid as a string for PubMed articles, the
-- SCOPUS: / OPENALEX: / CROSSREF: article_id for external articles
-- (analysis_summary_article.article_id). An open authorship_review row with no PMID gets
-- the id it will have once accepted, UPPER(source):external_id. 'DOI:<doi>' is used only
-- for a row with neither. It is never the negative pmid STEP 6b assigns: that number is
-- renumbered every night.
--
-- watchlist_match.is_institution_author is approximate: the matched author's byline name
-- ends with the surname of one of our people on that article and its given names, with
-- trailing middle initials dropped, equal that person's first name. A namesake co-author
-- sets it. An initials-only byline never does.
--
-- None of these tables is in update/updateReciterDB.py's `all_tables` / SWAP_TABLES
-- truncate/swap lists and none may be added: truncating the three durable ones would
-- refetch about 85,000 articles.
--
-- Merging the PR first is harmless: the nightly hook is off unless RESEARCH_SECURITY_MODE
-- is set on the CronJob, and collectAffiliations.py refuses apply until these tables exist.
-- Turn it on with `kubectl set env cronjob/reciterdb -n reciter RESEARCH_SECURITY_MODE=dry-run`
-- (then apply), never `kubectl apply`.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS only. Nothing is altered, dropped or truncated.
--   Apply directly to BOTH reciterdb instances (the producer and the dev instance behind
--   reciter-pm-dev). Merging the PR does NOT run DDL.
--
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/table_research_security_v3.13.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `article_author_affiliation` (
  `article_id`  VARCHAR(96)  NOT NULL,            -- pmid as a string, or SCOPUS:/OPENALEX:/CROSSREF:/DOI: id
  `rank`        INT          NOT NULL,            -- 1-based position in the byline
  `seq`         SMALLINT     NOT NULL DEFAULT 0,  -- 0-based: an author can list several affiliations
  `author_name` VARCHAR(255) NULL,                -- "Fore Last", or the collective name
  `affiliation` TEXT         NULL,                -- as supplied by the source
  `source`      VARCHAR(16)  NOT NULL,            -- dynamodb / efetch / openalex
  `fetched_at`  DATETIME     NOT NULL,
  PRIMARY KEY (`article_id`, `rank`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `article_funder` (
  `article_id` VARCHAR(96)  NOT NULL,
  `seq`        SMALLINT     NOT NULL,             -- 0-based position in the grant list
  `agency`     VARCHAR(500) NULL,
  `grant_id`   VARCHAR(255) NULL,
  `country`    VARCHAR(100) NULL,
  `source`     VARCHAR(16)  NOT NULL,             -- dynamodb / efetch / openalex
  PRIMARY KEY (`article_id`, `seq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `article_affiliation_state` (
  `article_id`     VARCHAR(96) NOT NULL,
  `source`         VARCHAR(16) NOT NULL,          -- dynamodb / efetch / openalex / none (not retrievable)
  `n_authors`      INT         NOT NULL DEFAULT 0,  -- all authors, with or without affiliation text
  `n_affiliations` INT         NOT NULL DEFAULT 0,  -- rows written to article_author_affiliation
  `n_funders`      INT         NOT NULL DEFAULT 0,  -- rows written to article_funder
  `fetched_at`     DATETIME    NOT NULL,          -- the last attempt
  `attempts`       SMALLINT    NOT NULL DEFAULT 1,  -- a 'none' row is tried again, a few times
  PRIMARY KEY (`article_id`),
  KEY `idx_aas_source` (`source`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `watchlist_match` (
  `article_id`            VARCHAR(96)  NOT NULL,
  `list_code`             VARCHAR(40)  NOT NULL,  -- house-1286-nominations / usual-suspects / country-of-concern
  `category`              VARCHAR(64)  NOT NULL,
  `list_no`               INT          NULL,      -- number on the source list, where it has one
  `entity`                VARCHAR(160) NOT NULL,
  `rank`                  INT          NOT NULL,  -- the matched author's position in the byline
  `author_name`           VARCHAR(255) NULL,
  `affiliation`           TEXT         NULL,      -- the exact string that matched
  `is_institution_author` TINYINT(1)   NOT NULL DEFAULT 0,  -- approximate, see the migration header
  PRIMARY KEY (`article_id`, `list_code`, `entity`, `rank`),
  KEY `idx_wm_list_entity` (`list_code`, `entity`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `watchlist_funding_flag` (
  `article_id` VARCHAR(96)  NOT NULL,
  `program`    VARCHAR(160) NOT NULL,
  `agency`     VARCHAR(500) NULL,                 -- the first grant on the article that flagged the program
  `grant_id`   VARCHAR(255) NULL,
  PRIMARY KEY (`article_id`, `program`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Verification
SELECT table_name, column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name IN ('article_author_affiliation', 'article_funder', 'article_affiliation_state',
                     'watchlist_match', 'watchlist_funding_flag')
ORDER BY table_name, ordinal_position;
