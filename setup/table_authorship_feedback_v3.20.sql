-- =============================================================================
-- Migration: authorship_feedback (v3.20)
-- =============================================================================
-- "Report a problem" reports from Publication Manager's /authorships page: what the
-- curator typed, plus a snapshot of the card and of the rows behind it. A report must
-- outlive the authorship_review row it is about (the producer refreshes that row in
-- place), so the keys are copied and there is no foreign key.
--
-- WRITERS: Publication Manager inserts the report and fills the snapshot once
-- (`client_snapshot` at insert; `server_snapshot`, `snapshot_state` and the row keys in
-- one UPDATE after it). Only the feedback skill writes `status` and the columns after it,
-- through the column-limited login in grants_authorship_feedback_reader_v3.20.sql.template.
-- No producer, nightly job or stored procedure reads or writes this table.
--
-- READERS: no SQL join to authorship_review or the admin_* tables without an explicit
-- COLLATE. This table is utf8mb4_unicode_ci and authorship_review is general_ci, so a
-- join on author_key or a cwid raises error 1267. `created_at`, `claimed_at` and
-- `handled_at` are UTC.
--
-- Snapshots hold HR dates and comments about named people. Check which accounts can read
-- this schema before applying. RETENTION (by hand, 90 days after a report closes): set
-- client_snapshot = '{}' and server_snapshot = NULL; keep the comment, keys and diagnosis.
--
-- Not in updateReciterDB.py's truncate/swap list and must never be added: nothing can
-- rebuild it.
--
-- ORDERING: apply before setting AUTHORSHIP_FEEDBACK_CWIDS on Publication Manager. Merging
--   the PR first is harmless: without the table the endpoint answers 503 and nothing else
--   uses it. Idempotent, CREATE only. Merging the PR does NOT run DDL; apply to BOTH
--   reciterdb instances by hand.
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/table_authorship_feedback_v3.20.sql
-- Rollback: unset AUTHORSHIP_FEEDBACK_CWIDS, then DROP TABLE authorship_feedback;
-- =============================================================================
CREATE TABLE IF NOT EXISTS `authorship_feedback` (
  `id`               BIGINT       NOT NULL AUTO_INCREMENT,
  `client_uuid`      CHAR(36)     NOT NULL,              -- made by the browser; a resend is not a second report
  `created_at`       DATETIME(3)  NOT NULL,              -- UTC, server clock
  `reported_by`      VARCHAR(64)  NOT NULL,              -- real user, from the session
  `viewing_as`       VARCHAR(64)  NULL,
  `host`             VARCHAR(128) NOT NULL,
  -- durable keys; no FK, the report must outlive the row
  `review_id`        BIGINT       NULL,
  `author_key`       VARCHAR(160) NULL,
  `source`           VARCHAR(16)  NULL,
  `pmid`             BIGINT       NULL,
  `external_id`      VARCHAR(96)  NULL,
  `trigger_place`    VARCHAR(32)  NOT NULL,
  `category`         VARCHAR(32)  NOT NULL,
  `about_cwids`      JSON         NULL,
  `expected`         VARCHAR(255) NULL,
  `typed_ref`        VARCHAR(255) NULL,                  -- PMID or DOI, page-level form
  `comment`          TEXT         NOT NULL,
  -- snapshot: written once by PM
  `snapshot_version` SMALLINT     NOT NULL,
  `client_build`     VARCHAR(64)  NULL,
  `server_build`     VARCHAR(64)  NULL,
  `client_snapshot`  LONGTEXT     NOT NULL,
  `server_snapshot`  LONGTEXT     NULL,                  -- raw_row first
  `snapshot_state`   ENUM('pending','complete','partial') NOT NULL DEFAULT 'pending',
  -- written by the skill only
  `status`           ENUM('new','diagnosing','needs_info','awaiting_approval','pr_open','fixed','not_a_bug','duplicate')
                     NOT NULL DEFAULT 'new',
  `claimed_by`       VARCHAR(64)  NULL,
  `claimed_at`       DATETIME(3)  NULL,
  `duplicate_of`     BIGINT       NULL,
  `diagnosis`        TEXT         NULL,
  `resolution_ref`   VARCHAR(255) NULL,
  `handled_at`       DATETIME(3)  NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_client_uuid` (`client_uuid`),
  KEY `ix_review` (`review_id`),
  KEY `ix_author_key` (`author_key`),
  KEY `ix_status` (`status`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Verification
SELECT column_name, column_type, is_nullable FROM information_schema.columns
 WHERE table_schema = DATABASE() AND table_name = 'authorship_feedback' ORDER BY ordinal_position;
