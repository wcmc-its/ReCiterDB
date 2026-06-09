-- =============================================================================
-- Migration: Add admin_users scope/proxy columns (v1.5)
-- =============================================================================
-- Adds the three JSON scope columns the Publication Manager AdminUser model
-- now selects on every login:
--   - scope_person_types   (JSON, nullable) — person-type curation scope
--   - scope_org_units       (JSON, nullable) — org-unit curation scope
--   - proxy_person_ids      (JSON, nullable) — proxied person identifiers
--
-- WHY THIS MIGRATION EXISTS:
--   ReCiter-Publication-Manager (dev branch, model commit 579d32f
--   "extend AdminUser model with scope/proxy JSON columns") issues
--     SELECT userID, personIdentifier, ..., scope_person_types,
--            scope_org_units, proxy_person_ids FROM admin_users
--   inside findOrcreateAdminUser during authentication. If admin_users is
--   missing these columns the SELECT fails with ER_BAD_FIELD_ERROR
--   ("Unknown column 'scope_person_types'"), the authorize() call throws, and
--   login returns 401 for every user. The columns must exist before the PM
--   dev branch is deployed against this database.
--
--   The fresh-build schema (setup/createDatabaseTableReciterDb.sql on master,
--   PR #92) already defines admin_users WITH these columns, so new databases
--   are fine. This migration brings EXISTING databases (e.g. the production
--   reciterdb, which predates #92 and has none of the three) up to that
--   schema. There was no ALTER path for existing DBs until now.
--
-- DURABILITY: admin_users is curator state, not a reporting export. It is NOT
--   in update/updateReciterDB.py's truncate list (`all_tables`) and is not
--   touched by any nightly stored procedure or ETL step, so these columns
--   persist across nightly reload.
--
-- Safe to run on prod and dev. Uses IF NOT EXISTS-style guards via an
-- information_schema check (no-op on re-run). Additive only — no existing
-- column or row is modified. Run BEFORE deploying the PM dev branch.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- admin_users: + scope_person_types + scope_org_units + proxy_person_ids
-- -----------------------------------------------------------------------------

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'admin_users'
       AND column_name = 'scope_person_types') = 0,
    'ALTER TABLE admin_users ADD COLUMN `scope_person_types` JSON DEFAULT NULL',
    'SELECT ''admin_users.scope_person_types already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'admin_users'
       AND column_name = 'scope_org_units') = 0,
    'ALTER TABLE admin_users ADD COLUMN `scope_org_units` JSON DEFAULT NULL',
    'SELECT ''admin_users.scope_org_units already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'admin_users'
       AND column_name = 'proxy_person_ids') = 0,
    'ALTER TABLE admin_users ADD COLUMN `proxy_person_ids` JSON DEFAULT NULL',
    'SELECT ''admin_users.proxy_person_ids already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'admin_users'
  AND column_name IN ('scope_person_types', 'scope_org_units', 'proxy_person_ids')
ORDER BY ordinal_position;
