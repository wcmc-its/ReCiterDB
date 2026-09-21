-- =============================================================================
-- Migration: Add admin_users.scope_institutions (v3.0)
-- =============================================================================
-- Third Curator_Scoped axis alongside scope_person_types / scope_org_units
-- (v1.5): a JSON array of person.primaryInstitution values the curator may
-- edit, e.g. ["Weill Cornell Medical College in Qatar", "Hamad Medical Corporation"].
-- AND across axes, OR within -- same resolver as the other two.
--
-- Publication Manager selects this column on every login (AdminUser model),
-- so run BEFORE deploying the PM change or authentication returns 401 with
-- ER_BAD_FIELD_ERROR. admin_users is not in the nightly truncate list, so the
-- column and its values persist. Additive, guarded, safe to re-run.
-- =============================================================================

SET @db = DATABASE();

SET @sql = (SELECT IF(
    (SELECT COUNT(*) FROM information_schema.columns
     WHERE table_schema = @db AND table_name = 'admin_users'
       AND column_name = 'scope_institutions') = 0,
    'ALTER TABLE admin_users ADD COLUMN `scope_institutions` JSON DEFAULT NULL AFTER `scope_org_units`',
    'SELECT ''admin_users.scope_institutions already exists'''));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'admin_users'
  AND column_name IN ('scope_person_types', 'scope_org_units', 'scope_institutions', 'proxy_person_ids')
ORDER BY ordinal_position;
