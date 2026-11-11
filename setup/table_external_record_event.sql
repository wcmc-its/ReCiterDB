-- =============================================================================
-- external_record_event — append-only history of external_record (v3.6)
-- =============================================================================
-- One row per add, import, status transition, edit, merge or unmerge. Writers only
-- INSERT. It is the audit trail behind Undo and the record history in PM.
-- IMPORT rows are written by the DynamoDB sync, one per inserted external_record.
-- diff_json holds {field: [before, after]} for EDIT.
-- Durable: never truncate.
-- =============================================================================
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
