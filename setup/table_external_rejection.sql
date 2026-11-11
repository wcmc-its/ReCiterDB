-- =============================================================================
-- external_rejection — per-person rejection memory (v3.6)
-- =============================================================================
-- Reject and Remove write one row per key the record carries, so the same work
-- arriving again (another source, a re-import) is recognised. Matching compares on
-- the highest-precedence key both records carry: DOI, then ORCID put-code, then
-- source id, then title+year; two different DOIs mean two different works.
--   DOI            key_value = external_record.doi_norm
--   ORCID_PUTCODE  key_value = put-code
--   SOURCE_ID      key_value = article_id (SCOPUS:..., OPENALEX:..., ...)
--   TITLE_YEAR     key_value = external_record.title_year_key
-- Undoing the Reject/Remove deletes the record's rows (by record_id).
-- Durable: never truncate.
-- =============================================================================
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
