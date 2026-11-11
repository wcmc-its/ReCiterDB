-- =============================================================================
-- external_dup_verdict — "Different papers" verdicts, per pair (v3.6)
-- =============================================================================
-- Replaces the FeedbackLog-note markers PM's duplicate banner reads today.
-- key_a / key_b: an ORDERED pair (key_a < key_b) of 'PMID:<n>' or
-- 'EXT:<article_id>', so one pair has one spelling.
-- scope_cwid: the curator the verdict applies to (Curate writes per curator), or
-- NULL for everyone (Authorships writes globally). scope_key folds NULL to '' so
-- the UNIQUE key also holds for global verdicts.
-- Durable: never truncate.
-- =============================================================================
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
  CONSTRAINT `ck_edv_ordered_pair` CHECK (`key_a` < `key_b`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
