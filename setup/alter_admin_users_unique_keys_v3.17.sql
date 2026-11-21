-- =============================================================================
-- Migration: UNIQUE keys on admin_users.personIdentifier and
--            admin_users_roles (userID, roleID)  (v3.17)
-- =============================================================================
-- APPLY ONLY AFTER THE REMEDIATION SQL HAS REMOVED THE DUPLICATES
-- (Roadmap/cwid-normalization-plan.md, Phase 2 step 2). On 2026-11-18 prod held:
--   * 15 CWIDs with two PM accounts each (e.g. mrj4001 = userIDs 13 and 40078),
--     plus accounts whose id differs only in case or a stray space from another's.
--   * ~60 duplicate (userID, roleID) rows, and 286 copies each of roles 3 and 4 on
--     userID 46 (blank personIdentifier).
-- Each key is added only when its table has no duplicates left; otherwise this file
-- prints how many remain and changes nothing, so running it early is harmless.
--
-- What the keys mean (the column collation is utf8mb4_unicode_ci, PAD SPACE):
--   uq_admin_users_personIdentifier  `Abl9002`, `abl9002` and `abl9002 ` are ONE key, so
--     PM can no longer create a second account for a person. NULLs are not compared.
--   uq_admin_users_roles_user_role   one row per (userID, roleID).
--
-- ORDER: reload setup/proc_updateCurateSelfRole_v3.17.sql first. The old procedure
--   body can insert rows these keys refuse, and the first refusal would abort it.
--   Publication Manager's account creation must also find the existing account by
--   canonical id before inserting (PM PR), or a login under a miscased id errors.
--
-- Apply with:
--   mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < setup/alter_admin_users_unique_keys_v3.17.sql
-- Rollback:
--   ALTER TABLE admin_users DROP INDEX uq_admin_users_personIdentifier;
--   ALTER TABLE admin_users_roles DROP INDEX uq_admin_users_roles_user_role;
-- Idempotent: each ALTER runs only when its key is absent and its table is clean.
-- =============================================================================

SET @db = DATABASE();

-- admin_users.createTimestamp defaults to '0000-00-00 00:00:00'. Under a strict
-- sql_mode with NO_ZERO_DATE (MySQL 8's default) any ALTER of that table fails with
-- ERROR 1067 "Invalid default value for 'createTimestamp'", even one that only adds a
-- key. Drop the two zero-date flags for this session; restored at the end.
SET @old_sql_mode = @@SESSION.sql_mode;
SET SESSION sql_mode = TRIM(BOTH ',' FROM REPLACE(REPLACE(
    CONCAT(',', @@SESSION.sql_mode, ','), ',NO_ZERO_DATE,', ','), ',NO_ZERO_IN_DATE,', ','));

-- admin_users.personIdentifier ------------------------------------------------
SET @dups = (SELECT COUNT(*) FROM (
    SELECT personIdentifier FROM admin_users
    WHERE personIdentifier IS NOT NULL
    GROUP BY personIdentifier HAVING COUNT(*) > 1) d);
SET @has = (SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = @db AND table_name = 'admin_users'
              AND index_name = 'uq_admin_users_personIdentifier');
SET @sql = (SELECT CASE
    WHEN @has > 0 THEN 'SELECT ''uq_admin_users_personIdentifier already exists'' AS status'
    WHEN @dups > 0 THEN CONCAT('SELECT ''SKIPPED uq_admin_users_personIdentifier: ', @dups,
                               ' personIdentifier values still duplicated; run the remediation first'' AS status')
    ELSE 'ALTER TABLE admin_users ADD UNIQUE KEY `uq_admin_users_personIdentifier` (`personIdentifier`)'
    END);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- admin_users_roles (userID, roleID) ------------------------------------------
SET @dups = (SELECT COUNT(*) FROM (
    SELECT userID, roleID FROM admin_users_roles
    WHERE userID IS NOT NULL AND roleID IS NOT NULL
    GROUP BY userID, roleID HAVING COUNT(*) > 1) d);
SET @has = (SELECT COUNT(*) FROM information_schema.statistics
            WHERE table_schema = @db AND table_name = 'admin_users_roles'
              AND index_name = 'uq_admin_users_roles_user_role');
SET @sql = (SELECT CASE
    WHEN @has > 0 THEN 'SELECT ''uq_admin_users_roles_user_role already exists'' AS status'
    WHEN @dups > 0 THEN CONCAT('SELECT ''SKIPPED uq_admin_users_roles_user_role: ', @dups,
                               ' (userID, roleID) pairs still duplicated; run the remediation first'' AS status')
    ELSE 'ALTER TABLE admin_users_roles ADD UNIQUE KEY `uq_admin_users_roles_user_role` (`userID`, `roleID`)'
    END);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET SESSION sql_mode = @old_sql_mode;

SELECT table_name, index_name, non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS cols
FROM information_schema.statistics
WHERE table_schema = DATABASE()
  AND index_name IN ('uq_admin_users_personIdentifier', 'uq_admin_users_roles_user_role')
GROUP BY table_name, index_name, non_unique;
