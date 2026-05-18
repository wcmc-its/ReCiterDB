-- =============================================================================
-- Migration: NIH RePORTER integration (v1.2)
-- =============================================================================
-- Adds the tables needed to ingest pub-grant linkages and project metadata
-- from NIH RePORTER (https://api.reporter.nih.gov/v2/) and to track per-pair
-- provenance over time.
--
-- WHY SEPARATE TABLES (not columns on person_article_grant):
--   person_article_grant is TRUNCATE-reloaded by updateReciterDB.py every
--   night from ReCiter scoring output (see updateReciterDB.py:241). Any
--   provenance columns added directly to that table would be wiped on each
--   nightly run, defeating the purpose of *_first_seen tracking. The
--   provenance table below is updated incrementally and survives reloads.
--
-- WHAT'S CREATED:
--   1. grant_reporter_project — RePORTER /projects/search results
--   2. grant_reporter_link    — RePORTER /publications/search results
--   3. grant_provenance       — long-lived per-(person, pmid, grant)
--                               source-and-timestamp log
--
-- Safe to run on prod and dev. Idempotent (CREATE TABLE IF NOT EXISTS).
-- Run BEFORE deploying retrieveReporter.py.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- grant_reporter_project — RePORTER project metadata
-- -----------------------------------------------------------------------------
-- One row per RePORTER appl_id returned by /projects/search for the configured
-- WCM org filter. Refreshed each ETL cycle (truncate-reload OK; no historical
-- state to preserve here — RePORTER is the source of truth).
--
-- abstract_text is stored here as a cross-reference. The Funding UI reads
-- abstracts from Postgres (Scholars-Profile-System) where they're joined to
-- InfoEd grant rows; this column exists for ad-hoc analysis and future
-- reciterdb-side consumers.
--
-- project_terms / pref_terms hold the NIH-curated keyword vocabulary RePORTER
-- returns per project, stored raw (project_terms angle-bracket-wrapped,
-- pref_terms semicolon-delimited). Added by alter_add_reporter_terms_v1.3.sql;
-- mirrored into the CREATE TABLE here so a fresh build matches (issue #291).
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `grant_reporter_project` (
  `appl_id` int(11) NOT NULL,
  `core_project_num` varchar(32) DEFAULT NULL,
  `project_title` varchar(512) DEFAULT NULL,
  `org_name` varchar(255) DEFAULT NULL,
  `fiscal_year` smallint(6) DEFAULT NULL,
  `activity_code` varchar(8) DEFAULT NULL,
  `project_start_date` date DEFAULT NULL,
  `project_end_date` date DEFAULT NULL,
  `abstract_text` mediumtext DEFAULT NULL,
  `project_terms` text DEFAULT NULL,
  `pref_terms` text DEFAULT NULL,
  `last_fetched_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`appl_id`),
  KEY `core_project_num` (`core_project_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- grant_reporter_link — RePORTER pub-grant linkages
-- -----------------------------------------------------------------------------
-- One row per (pmid, appl_id) pair returned by /publications/search.
-- Refreshed each ETL cycle (truncate-reload). The grant_provenance table
-- below is what carries history.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `grant_reporter_link` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) NOT NULL,
  `appl_id` int(11) NOT NULL,
  `core_project_num` varchar(32) DEFAULT NULL,
  `last_fetched_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pmid_appl_id` (`pmid`, `appl_id`),
  KEY `pmid` (`pmid`),
  KEY `core_project_num` (`core_project_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- grant_provenance — per-(person, pmid, grant) source and timestamp log
-- -----------------------------------------------------------------------------
-- The audit log that survives nightly truncate-reload of person_article_grant.
-- Keyed by (personIdentifier, pmid, core_project_num) where core_project_num
-- is the normalized NIH grant identifier (e.g. "R01DK127777" — no year suffix,
-- no spaces). For non-NIH grants the original articleGrant string is stored
-- in core_project_num as a fallback so the row is still keyable.
--
-- Update logic (run nightly by retrieveReporter.py after person_article_grant
-- has been refreshed by retrieveArticles.py):
--
--   1. UPSERT from person_article_grant: any (personIdentifier, pmid,
--      normalized_grant) currently in person_article_grant gets
--      source_reciterdb=1 and last_verified=NOW(). reciterdb_first_seen is
--      set on first insert and never overwritten.
--
--   2. UPSERT from grant_reporter_link joined to person_article (where
--      userAssertion='ACCEPTED' to scope to confirmed WCM authors): any
--      (personIdentifier, pmid, core_project_num) seen in RePORTER gets
--      source_reporter=1 and last_verified=NOW(). reporter_first_seen is
--      set on first insert and never overwritten.
--
-- Subaward caution: see retrieveReporter.py — we filter RePORTER projects
-- to org_names=["WEILL MEDICAL COLL OF CORNELL UNIV"] and join PMIDs to
-- person_article ACCEPTED rows. This minimizes false positives at the cost
-- of missing some legitimate WCM-as-subaward linkages.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `grant_provenance` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `personIdentifier` varchar(128) NOT NULL,
  `pmid` int(11) NOT NULL,
  `core_project_num` varchar(64) NOT NULL,
  `appl_id` int(11) DEFAULT NULL,
  `source_reporter` tinyint(1) NOT NULL DEFAULT 0,
  `source_reciterdb` tinyint(1) NOT NULL DEFAULT 0,
  `reporter_first_seen` datetime DEFAULT NULL,
  `reciterdb_first_seen` datetime DEFAULT NULL,
  `last_verified` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_person_pmid_grant` (`personIdentifier`, `pmid`, `core_project_num`),
  KEY `pmid` (`pmid`),
  KEY `appl_id` (`appl_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, table_rows, create_time
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name IN ('grant_reporter_project', 'grant_reporter_link', 'grant_provenance')
ORDER BY table_name;
