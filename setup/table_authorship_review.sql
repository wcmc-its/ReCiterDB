-- -----------------------------------------------------------------------------
-- authorship_review — Publication Manager "Authorships" review queue
-- -----------------------------------------------------------------------------
-- DURABLE TABLE — survives the nightly truncate-reload. Like `grant_provenance`
-- (the (person,pmid,grant) audit log) and the `admin_*` tables, this is curator
-- state, NOT a reporting export. It MUST NOT be added to any truncate list
-- (see update/updateReciterDB.py `all_tables`) and is not touched by any nightly
-- stored procedure or ETL step. CREATE TABLE IF NOT EXISTS so re-applying is safe.
--
-- One row per WCM-affiliated AUTHORSHIP (an author carrying a WCM affiliation on a
-- publication) that is NOT yet assigned to any identity. Powers the Curator_All
-- `/authorships` tab in ReCiter-Publication-Manager (reads this table via Sequelize).
--
-- MULTI-SOURCE (v1.6): `source` distinguishes the PubMed lane from the Scopus lane.
--   pubmed  — PMID-keyed rows; author_key = `{pmid}:{position}`; resolves to gold standard.
--   scopus  — documents NOT in PubMed (no PMID); author_key = `scopus:{doi-or-scopusid}:{position}`;
--             external_id = DOI (else numeric Scopus ID); pub_type = subtypeDescription;
--             container_id = book base DOI where derivable; resolves to ExternalArticle.
--   Existing (pre-v1.6) rows are PubMed via the column default.
--
-- POPULATED EXTERNALLY (this repo's ETL cannot compute the scores). The producer is
-- the adversarial-attribution-review pipeline in the ReCiter Research project
-- (scripts/aar_orchestrator.py -> aar_db.py upsert), which runs the gate (reciterdb
-- analysis_summary_author = accepted set), the identity matcher (reciterdb identity),
-- and the pinned XGBoost 3.2.0 models over the S3 scoring inputs to compute the
-- feedback-identity (FG) and identity-only (IO) scores per authorship. Monthly cron.
--
-- Classification per authorship (the producer sets it):
--   absent     top candidate never scored by production (no person_article row)
--   suggested  top candidate production final (FG) >= 30 — already in a pending queue
--   buried     top candidate FG < 30 (IO can be high) — production buried it
--   assigned   reserved (accepted rows are excluded by the gate, not stored here)
--
-- single_candidate = exactly one WCM identity matches the author's surname +
-- given/initial (cohort_size == 1) — the strongest precision signal; such rows are
-- near-certain and form the high-precision review lane.
--
-- Refresh contract: the producer UPSERTs by author_key, refreshing the scoring/
-- classification columns and `last_refreshed`; it NEVER overwrites a curator-set
-- `status` (assigned/accepted/rejected/dismissed/snoozed) or its resolution_cwid/
-- reviewer/note/snooze_until, and `first_seen` is set once and never overwritten.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS `authorship_review` (
  `id`                     BIGINT       NOT NULL AUTO_INCREMENT,
  `source`                 ENUM('pubmed','scopus') NOT NULL DEFAULT 'pubmed',
  `pmid`                   BIGINT       NULL,                -- NULL for scopus rows
  `external_id`            VARCHAR(96)  NULL,                -- DOI-first, numeric Scopus ID fallback
  `author_key`             VARCHAR(160) NOT NULL,            -- `{pmid}:{position}` or `scopus:{doi-or-scopusid}:{position}`
  `pub_type`               VARCHAR(40)  NULL,                -- Article / Book Chapter / Conference Paper…
  `container_id`           VARCHAR(96)  NULL,                -- book base DOI (chapter → book)
  `author_position`        INT          NULL,
  `author_position_label`  VARCHAR(8)   NULL,                -- first/middle/last
  `wcm_author`             VARCHAR(255) NULL,                -- PubMed author name
  `author_affiliation`     TEXT         NULL,
  `entrez_date`            DATE         NULL,                -- ReCiter entrez add date
  `title`                  TEXT         NULL,
  `journal`                VARCHAR(512) NULL,
  `doi`                    VARCHAR(255) NULL,
  `classification`         ENUM('assigned','suggested','buried','absent') NULL,
  `top_cwid`               VARCHAR(32)  NULL,                -- proposed identity
  `top_name`               VARCHAR(255) NULL,
  `top_person_type`        VARCHAR(64)  NULL,
  `top_dept`               VARCHAR(255) NULL,
  `top_fg_score`           FLOAT        NULL,                -- production final (FG)
  `top_io_score`           FLOAT        NULL,                -- identity-only (IO)
  `top_confidence`         FLOAT        NULL,
  `top_cohort_size`        INT          NULL,                -- homonyms (surname+initial)
  `top_given_match`        VARCHAR(16)  NULL,                -- full|initial
  `top_affil_match`        TINYINT(1)   NULL,
  `n_candidates`           INT          NULL,
  `single_candidate`       TINYINT(1)   NULL,                -- cohort_size == 1
  `candidate_cwids_json`   LONGTEXT     NULL,                -- ranked alternates
  `status`                 ENUM('open','assigned','accepted','rejected','dismissed','snoozed')
                                        NOT NULL DEFAULT 'open',   -- curator state
  `resolution_cwid`        VARCHAR(32)  NULL,
  `reviewer`               VARCHAR(64)  NULL,
  `note`                   TEXT         NULL,
  `snooze_until`           DATE         NULL,
  `resolved_at`            DATETIME     NULL,
  `first_seen`             DATETIME     NULL,                -- set once, never overwritten
  `last_refreshed`         DATETIME     NULL,
  `last_checked`           DATETIME     NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_author_key` (`author_key`),
  KEY `ix_source` (`source`),
  KEY `ix_pmid` (`pmid`),
  KEY `ix_classification` (`classification`),
  KEY `ix_status` (`status`),
  KEY `ix_single_candidate` (`single_candidate`),
  KEY `ix_top_io_score` (`top_io_score`),
  KEY `ix_entrez_date` (`entrez_date`),
  KEY `ix_top_cwid` (`top_cwid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
