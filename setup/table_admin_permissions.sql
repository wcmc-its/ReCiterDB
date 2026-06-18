-- Seed data for the data-driven RBAC permission tables.
-- Mirror of the SEED section of ReCiter-Publication-Manager
-- scripts/migrations/add-permission-tables.sql (3-places rule).
--
-- Run ONCE per environment, after createDatabaseTableReciterDb.sql has created
-- the tables and table_admin_roles.sql has seeded admin_roles. The role->permission
-- seed joins on admin_roles.roleLabel, so it adapts to whatever roles an
-- environment defines; 'Curator_Scoped' is a harmless no-op where that role
-- does not exist.

-- 1. Permissions (7)
INSERT INTO `admin_permissions` (`permissionKey`, `label`, `description`, `category`) VALUES
  ('canCurate', 'Curate Publications', 'Accept or reject article suggestions for people', 'Curation'),
  ('canSearch', 'Search Identities', 'Search and browse the identity directory', 'Navigation'),
  ('canReport', 'Create Reports', 'Generate publication reports and export data', 'Reporting'),
  ('canManageUsers', 'Manage Users', 'Create, edit, and deactivate user accounts and assign roles', 'Administration'),
  ('canConfigure', 'Configuration', 'Edit application settings, labels, and field visibility', 'Administration'),
  ('canManageNotifications', 'Manage Notifications', 'Configure notification preferences', 'Communication'),
  ('canManageProfile', 'Manage Profile', 'View and edit user profile information', 'Profile');

-- 2. Role -> permission mappings (reproduces current behavior)
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Superuser';                                                   -- all 7
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Curator_All'                 AND ap.permissionKey IN ('canCurate','canSearch');
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Curator_Self'                AND ap.permissionKey IN ('canCurate');
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Curator_Scoped'              AND ap.permissionKey IN ('canCurate','canSearch');
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Curator_Department'          AND ap.permissionKey IN ('canCurate','canSearch');
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Curator_Department_Delegate' AND ap.permissionKey IN ('canCurate','canSearch');
INSERT INTO `admin_role_permissions` (`roleID`, `permissionID`)
  SELECT ar.roleID, ap.permissionID FROM admin_roles ar CROSS JOIN admin_permissions ap
  WHERE ar.roleLabel = 'Reporter_All'                AND ap.permissionKey IN ('canReport','canSearch');

-- 3. Nav resources (sidebar items)
INSERT INTO `admin_permission_resources` (`permissionID`, `resourceType`, `resourceKey`, `displayOrder`, `icon`, `label`, `route`)
  SELECT ap.permissionID, v.resourceType, v.resourceKey, v.displayOrder, v.icon, v.label, v.route
  FROM admin_permissions ap
  JOIN (
    SELECT 'canSearch' AS pk, 'nav' AS resourceType, 'nav_search' AS resourceKey, 1 AS displayOrder, 'Search' AS icon, 'Find People' AS label, '/search' AS route
    UNION ALL SELECT 'canCurate', 'nav', 'nav_curate', 2, 'LocalLibrary', 'Curate Publications', '/curate'
    UNION ALL SELECT 'canReport', 'nav', 'nav_report', 3, 'Assessment', 'Create Reports', '/report'
    UNION ALL SELECT 'canManageNotifications', 'nav', 'nav_notifications', 4, 'NotificationsActive', 'Manage Notifications', '/notifications'
    UNION ALL SELECT 'canManageProfile', 'nav', 'nav_profile', 5, 'AccountCircle', 'Manage Profile', '/manageprofile'
    UNION ALL SELECT 'canManageUsers', 'nav', 'nav_users', 6, 'Group', 'Manage Users', '/manageusers'
    UNION ALL SELECT 'canConfigure', 'nav', 'nav_config', 7, 'Settings', 'Configuration', '/configuration'
  ) v ON ap.permissionKey = v.pk;
