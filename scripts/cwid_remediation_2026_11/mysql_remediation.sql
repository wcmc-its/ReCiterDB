-- =============================================================================
-- CWID remediation, MySQL half (reciterdb, 2026-11)
--
-- One transaction. Ends in ROLLBACK by default: run it once as a dry run, read
-- the counts, then switch the last lines to COMMIT (see the bottom of the file)
-- and run it again. Every statement is idempotent: a second COMMIT run changes 0
-- rows.
--
-- Run (never with --force: the first error must stop the script, and the open
-- transaction is then rolled back when the client disconnects). The script stops
-- itself with "CONSTRAINT `stop_here` failed" when two merged accounts carry different
-- scope_* / proxy_person_ids values, when a GUARD count is not 0, or when an after-check
-- is not 0 (just before ROLLBACK/COMMIT, so a COMMIT run never commits a bad state); the
-- "checks" / "STOP if listed" result just above the error says which:
--   mysql --defaults-file=<prod-creds.cnf> reciterdb < mysql_remediation.sql | tee mysql_run_$(date -u +%Y%m%dT%H%M%SZ).log
--
-- Mapping: the rm_map rows below are the MAP/DELETE rows of mapping.json
-- (dynamo_remediation.py --selftest checks they match). UNDERSCORE ids (legacy
-- PM external logins such as _pah4001 = Paul A. Harris, Vanderbilt) are NOT
-- touched here: their CWID look-alikes in ED are different people.
--
-- Comparisons that must be case- or space-exact use BINARY: every table here is
-- utf8mb4_unicode_ci, where 'Abl9002' = 'abl9002' and 'ggi9001 ' = 'ggi9001'.
--
-- External-cohort ids keep their case (trim only). They are exactly the ids matching
-- BINARY TRIM(id) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)' (the underscore is
-- required: a bare 'uc...' id such as 'Uca2001' or 'ucb9001' is a CWID and is lowercased).
-- Underscore test identities (_pab4001 and the rest) are lowercase already and are
-- never merged.
-- =============================================================================

SELECT @@hostname AS host, DATABASE() AS db, NOW() AS run_at,
       'CHECK THIS IS THE DATABASE YOU MEAN' AS note;

-- ---------------------------------------------------------------- temp tables
DROP TEMPORARY TABLE IF EXISTS rm_map;
CREATE TEMPORARY TABLE rm_map (
  bad    VARCHAR(128) NOT NULL,
  canon  VARCHAR(128) NULL,
  action VARCHAR(8)   NOT NULL,
  KEY (bad)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO rm_map (bad, canon, action) VALUES
-- BEGIN rm_map
  ('Ana2037', 'ana2037', 'MAP'),
  ('Kym9003', 'kym9003', 'MAP'),
  ('ggi9001 ', 'ggi9001', 'MAP'),
  ('Abl9002', 'abl9002', 'MAP'),
  ('Jod2033', 'jod2033', 'MAP'),
  ('ldn9002@nyp.org', 'ldn9002', 'MAP'),
  ('Arn4002', 'arn4002', 'MAP'),
  ('EMH2012', 'emh2012', 'MAP'),
  ('SHP2010', 'shp2010', 'MAP'),
  ('kims7@mskcc.org', 'ski4009', 'MAP'),
  ('Minkowitz', 'bam9012', 'MAP'),
  ('xoprotop ', 'xoprotop', 'MAP'),
  ('jih9026 ', 'jih9026', 'MAP'),
  ('Anc2032', 'anc2032', 'MAP'),
  ('Hlloyd', 'hlloyd', 'MAP'),
  ('Res2011', 'res2011', 'MAP'),
  ('SMB9009', 'smb9009', 'MAP'),
  ('DLAMB', 'djl2008', 'MAP'),
  ('dlamb', 'djl2008', 'MAP'),
  ('tfm9003@nyp.org', 'tfm9003', 'MAP'),
  (' Isk9008', 'isk9008', 'MAP'),
  ('ars2027 ', 'ars2027', 'MAP'),
  ('HAF9018', 'haf9018', 'MAP'),
  ('Nes9050', 'nes9050', 'MAP'),
  ('jason.carmel', 'jbc7001', 'MAP'),
  ('str2025-mi', 'str2025', 'MAP'),
  ('NA', NULL, 'DELETE'),
  ('na', NULL, 'DELETE'),
  ('Hello, world!', NULL, 'DELETE'),
  ('margarita.todor', NULL, 'DELETE'),
  ('rodman.john', NULL, 'DELETE')
-- END rm_map
;

-- A row inserted here stops the script with "CONSTRAINT `stop_here` failed" (the
-- reason is in the INSERT that fired). Without --force the client exits and the
-- open transaction is rolled back.
DROP TEMPORARY TABLE IF EXISTS rm_abort;
CREATE TEMPORARY TABLE rm_abort (reason VARCHAR(255), CONSTRAINT stop_here CHECK (reason IS NULL));

-- Every GUARD and after-check count lands here; any n > 0 aborts the run at the end.
DROP TEMPORARY TABLE IF EXISTS rm_check;
CREATE TEMPORARY TABLE rm_check (phase VARCHAR(8), name VARCHAR(128), n BIGINT NOT NULL)
  DEFAULT CHARSET=utf8mb4;

-- PM accounts that are not CWIDs at all (decision 5, 11-18)
DROP TEMPORARY TABLE IF EXISTS rm_junk_pid;
CREATE TEMPORARY TABLE rm_junk_pid (pid VARCHAR(128) NOT NULL)
  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO rm_junk_pid VALUES ('WCMCWID'), ('Mahender Jangari'), ('NewName123'), ('ReCiterBot'), ('Sbackovic');
-- a DELETE-class person's PM account goes too (NA = Shama Farooq, no CWID)
INSERT INTO rm_junk_pid SELECT bad FROM rm_map WHERE action = 'DELETE';

-- =============================================================================
-- DRY-RUN COUNTS (before)
-- =============================================================================
SELECT 'before' AS phase, 'admin_users junk accounts' AS what, u.userID, u.personIdentifier, u.status,
       (SELECT COUNT(*) FROM admin_users_roles r WHERE r.userID = u.userID) AS n_roles,
       (SELECT COUNT(*) FROM admin_feedback_log f WHERE f.userID = u.userID) AS n_feedback_authored,
       (SELECT COUNT(*) FROM admin_feedback_log f WHERE f.impersonatedByUserID = u.userID) AS n_feedback_impersonated
FROM admin_users u
WHERE BINARY u.personIdentifier IN (SELECT BINARY pid FROM rm_junk_pid)
ORDER BY u.userID;

SELECT 'before' AS phase, 'admin_users mapped/non-canonical' AS what, u.userID, u.personIdentifier
FROM admin_users u
WHERE u.personIdentifier IS NOT NULL AND TRIM(u.personIdentifier) <> ''
  AND BINARY u.personIdentifier NOT IN (SELECT BINARY pid FROM rm_junk_pid)
  AND (BINARY u.personIdentifier IN (SELECT BINARY bad FROM rm_map WHERE action = 'MAP')
       OR BINARY u.personIdentifier <> BINARY LOWER(TRIM(u.personIdentifier)))
ORDER BY u.personIdentifier, u.userID;

SELECT 'before' AS phase, 'admin_users duplicate lowercase accounts' AS what,
       LOWER(TRIM(personIdentifier)) AS canon, COUNT(*) AS n, GROUP_CONCAT(userID ORDER BY userID) AS userIDs
FROM admin_users
WHERE personIdentifier IS NOT NULL AND TRIM(personIdentifier) <> ''
GROUP BY LOWER(TRIM(personIdentifier)) HAVING COUNT(*) > 1;

SELECT 'before' AS phase, 'admin_users_roles duplicate (userID, roleID) rows' AS what,
       COUNT(*) - COUNT(DISTINCT userID, roleID) AS extra_rows FROM admin_users_roles;
SELECT 'before' AS phase, 'userID 46 role rows' AS what, roleID, COUNT(*) AS n
FROM admin_users_roles WHERE userID = 46 GROUP BY roleID;

-- Anything else that points at admin_users.userID must be handled below; if this lists a
-- table other than admin_users_roles, admin_users_departments, admin_feedback_log,
-- admin_notification_preferences, admin_notification_log: STOP and extend the script.
SELECT 'before' AS phase, 'FKs referencing admin_users' AS what, TABLE_NAME, COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE REFERENCED_TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = 'admin_users';

SELECT 'before' AS phase, 'grant_provenance' AS tbl, COUNT(*) AS n FROM grant_provenance g JOIN rm_map m ON BINARY g.personIdentifier = BINARY m.bad;
SELECT 'before' AS phase, 'admin_feedback_log' AS tbl, COUNT(*) AS n FROM admin_feedback_log f JOIN rm_map m ON BINARY f.personIdentifier = BINARY m.bad;
SELECT 'before' AS phase, 'authorship_review.top_cwid' AS tbl, COUNT(*) AS n FROM authorship_review a JOIN rm_map m ON BINARY a.top_cwid = BINARY m.bad;
SELECT 'before' AS phase, 'authorship_review.resolution_cwid' AS tbl, COUNT(*) AS n FROM authorship_review a JOIN rm_map m ON BINARY a.resolution_cwid = BINARY m.bad;
SELECT 'before' AS phase, 'authorship_review.candidate_cwids_json' AS tbl, COUNT(DISTINCT a.id) AS n FROM authorship_review a JOIN rm_map m ON INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0;
SELECT 'before' AS phase, 'reporting_ad_hoc_feature_generator_execution (non-canonical)' AS tbl, COUNT(*) AS n FROM reporting_ad_hoc_feature_generator_execution WHERE BINARY personIdentifier <> BINARY LOWER(TRIM(personIdentifier));
SELECT 'before' AS phase, 'reporting_ad_hoc_identity_creation (non-canonical)' AS tbl, COUNT(*) AS n FROM reporting_ad_hoc_identity_creation WHERE BINARY personIdentifier <> BINARY LOWER(TRIM(personIdentifier));
SELECT 'before' AS phase, 'reporting_ad_hoc_* (mapped)' AS tbl, COUNT(*) AS n FROM reporting_ad_hoc_feature_generator_execution x JOIN rm_map m ON BINARY x.personIdentifier = BINARY m.bad;
SELECT 'before' AS phase, 'authorship_review.reviewer' AS tbl, COUNT(*) AS n FROM authorship_review a JOIN rm_map m ON BINARY a.reviewer = BINARY m.bad;
SELECT 'before' AS phase, 'admin_users.proxy_person_ids (mapped/deleted ids)' AS what, u.userID, u.personIdentifier, m.bad, m.action
FROM admin_users u JOIN rm_map m ON INSTR(CAST(u.proxy_person_ids AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
ORDER BY u.userID;
-- informational: the nightly identity build reloads this table from Jenzabar and
-- lowercases cwid itself (update/buildIdentity.py, same PR), so it is not written here
SELECT 'before' AS phase, 'identity_grad_school_faculty (non-canonical, fixed by the next nightly)' AS tbl, COUNT(*) AS n FROM identity_grad_school_faculty WHERE BINARY cwid <> BINARY LOWER(TRIM(cwid));
-- guards: expected 0 (Phase 0); a non-zero here means extend the script before COMMIT
-- (enforced: a non-zero GUARD aborts the run before ROLLBACK/COMMIT)
INSERT INTO rm_check SELECT 'GUARD', 'admin_orcid', COUNT(*) FROM admin_orcid o JOIN rm_map m ON BINARY o.personIdentifier = BINARY m.bad;
INSERT INTO rm_check SELECT 'GUARD', 'analysis_override_author_position', COUNT(*) FROM analysis_override_author_position o JOIN rm_map m ON BINARY o.personIdentifier = BINARY m.bad;
INSERT INTO rm_check SELECT 'GUARD', 'external_record', COUNT(*) FROM external_record o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;
INSERT INTO rm_check SELECT 'GUARD', 'external_rejection', COUNT(*) FROM external_rejection o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;
INSERT INTO rm_check SELECT 'GUARD', 'person_author_id', COUNT(*) FROM person_author_id o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;
SELECT 'before' AS phase, CONCAT('GUARD ', name) AS tbl, n, 'expect 0' AS expect FROM rm_check WHERE phase = 'GUARD';

-- =============================================================================
-- WRITES
-- =============================================================================
START TRANSACTION;

-- ---------------------------------------------------------------- 1. junk PM accounts
-- Accounts that authored admin_feedback_log rows, or are named in its
-- impersonatedByUserID, are deactivated (status=0, roles and departments removed) instead
-- of deleted, so curator history keeps its userID.
DROP TEMPORARY TABLE IF EXISTS rm_junk_acct;
CREATE TEMPORARY TABLE rm_junk_acct (userID INT PRIMARY KEY, keep_inactive TINYINT NOT NULL DEFAULT 0);
INSERT INTO rm_junk_acct (userID)
SELECT userID FROM admin_users WHERE BINARY personIdentifier IN (SELECT BINARY pid FROM rm_junk_pid);
UPDATE rm_junk_acct j SET keep_inactive = 1
WHERE EXISTS (SELECT 1 FROM admin_feedback_log f
              WHERE f.userID = j.userID OR f.impersonatedByUserID = j.userID);

DELETE r FROM admin_users_roles r JOIN rm_junk_acct j ON r.userID = j.userID;
DELETE d FROM admin_users_departments d JOIN rm_junk_acct j ON d.userID = j.userID;
DELETE p FROM admin_notification_preferences p JOIN rm_junk_acct j ON p.userID = j.userID;
DELETE l FROM admin_notification_log l JOIN rm_junk_acct j ON l.userID = j.userID AND j.keep_inactive = 0;
UPDATE admin_users u JOIN rm_junk_acct j ON u.userID = j.userID AND j.keep_inactive = 1 SET u.status = 0;
DELETE u FROM admin_users u JOIN rm_junk_acct j ON u.userID = j.userID AND j.keep_inactive = 0;
SELECT 'junk accounts' AS step, SUM(keep_inactive = 0) AS deleted, SUM(keep_inactive = 1) AS deactivated_kept FROM rm_junk_acct;

-- ---------------------------------------------------------------- 2. account merge / rename
-- canonical id per account: the mapped canon, else trim + lowercase (external-cohort ids
-- keep their case: trim only).
DROP TEMPORARY TABLE IF EXISTS rm_acct;
CREATE TEMPORARY TABLE rm_acct (
  userID INT PRIMARY KEY,
  cur    VARCHAR(128) COLLATE utf8mb4_bin,
  canon  VARCHAR(128) COLLATE utf8mb4_bin,
  KEY (canon)
) DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_acct (userID, cur, canon)
SELECT u.userID, u.personIdentifier,
       COALESCE(m.canon,
         CASE WHEN BINARY TRIM(u.personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)'
              THEN TRIM(u.personIdentifier)
              ELSE LOWER(TRIM(u.personIdentifier)) END)
FROM admin_users u
LEFT JOIN rm_map m ON BINARY m.bad = BINARY u.personIdentifier AND m.action = 'MAP'
WHERE u.personIdentifier IS NOT NULL AND TRIM(u.personIdentifier) <> ''
  AND u.userID NOT IN (SELECT userID FROM rm_junk_acct);

-- keeper = lowest userID per canonical id, for every group that needs any change
DROP TEMPORARY TABLE IF EXISTS rm_keep;
CREATE TEMPORARY TABLE rm_keep (canon VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY, keeper INT NOT NULL)
  DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_keep
SELECT canon, MIN(userID) FROM rm_acct GROUP BY canon
HAVING COUNT(*) > 1 OR SUM(BINARY cur <> BINARY canon) > 0;  -- BINARY: utf8mb4_bin is PAD SPACE, 'x ' = 'x'

DROP TEMPORARY TABLE IF EXISTS rm_loser;
CREATE TEMPORARY TABLE rm_loser (userID INT PRIMARY KEY, keeper INT NOT NULL, canon VARCHAR(128) COLLATE utf8mb4_bin)
  DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_loser
SELECT a.userID, k.keeper, k.canon FROM rm_acct a JOIN rm_keep k ON a.canon = k.canon WHERE a.userID <> k.keeper;

SELECT 'account merges' AS step, l.canon, l.keeper, ku.status AS keeper_status,
       l.userID AS merged_away, lu.status AS merged_away_status
FROM rm_loser l JOIN admin_users ku ON ku.userID = l.keeper JOIN admin_users lu ON lu.userID = l.userID
ORDER BY l.canon, l.userID;

UPDATE admin_users_roles r JOIN rm_loser l ON r.userID = l.userID SET r.userID = l.keeper;
UPDATE admin_users_departments d JOIN rm_loser l ON d.userID = l.userID SET d.userID = l.keeper;
UPDATE admin_notification_log n JOIN rm_loser l ON n.userID = l.userID SET n.userID = l.keeper;
UPDATE admin_feedback_log f JOIN rm_loser l ON f.userID = l.userID SET f.userID = l.keeper;
UPDATE admin_feedback_log f JOIN rm_loser l ON f.impersonatedByUserID = l.userID SET f.impersonatedByUserID = l.keeper;
-- notification preferences: the keeper's own row wins; move the loser's only when the keeper has none
DELETE p FROM admin_notification_preferences p
  JOIN rm_loser l ON p.userID = l.userID
  JOIN admin_notification_preferences k ON k.userID = l.keeper;
UPDATE admin_notification_preferences p JOIN rm_loser l ON p.userID = l.userID SET p.userID = l.keeper;
-- (a loser may still own several preference rows: keep the lowest id per user)
DELETE p FROM admin_notification_preferences p
  JOIN admin_notification_preferences p2 ON p.userID = p2.userID AND p.id > p2.id
  JOIN rm_keep k ON k.keeper = p.userID;

-- Curator_Scoped scope and proxies: the keeper must end up with what the group had.
-- Per (canonical id, column), collect the non-empty values across the keeper and its
-- losers. One distinct value: the keeper gets it. More than one: STOP (settle by hand,
-- then re-run). Compared byte-for-byte, so differently formatted JSON also stops.
DROP TEMPORARY TABLE IF EXISTS rm_scope;
CREATE TEMPORARY TABLE rm_scope (
  canon  VARCHAR(128) COLLATE utf8mb4_bin, keeper INT, userID INT,
  col    VARCHAR(32) COLLATE utf8mb4_bin, val LONGTEXT COLLATE utf8mb4_bin
) DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_scope
SELECT k.canon, k.keeper, u.userID, 'scope_person_types', u.scope_person_types
  FROM rm_keep k JOIN rm_acct a ON a.canon = k.canon JOIN admin_users u ON u.userID = a.userID
 WHERE u.scope_person_types IS NOT NULL AND JSON_LENGTH(u.scope_person_types) > 0;
INSERT INTO rm_scope
SELECT k.canon, k.keeper, u.userID, 'scope_org_units', u.scope_org_units
  FROM rm_keep k JOIN rm_acct a ON a.canon = k.canon JOIN admin_users u ON u.userID = a.userID
 WHERE u.scope_org_units IS NOT NULL AND JSON_LENGTH(u.scope_org_units) > 0;
INSERT INTO rm_scope
SELECT k.canon, k.keeper, u.userID, 'scope_institutions', u.scope_institutions
  FROM rm_keep k JOIN rm_acct a ON a.canon = k.canon JOIN admin_users u ON u.userID = a.userID
 WHERE u.scope_institutions IS NOT NULL AND JSON_LENGTH(u.scope_institutions) > 0;
INSERT INTO rm_scope
SELECT k.canon, k.keeper, u.userID, 'proxy_person_ids', u.proxy_person_ids
  FROM rm_keep k JOIN rm_acct a ON a.canon = k.canon JOIN admin_users u ON u.userID = a.userID
 WHERE u.proxy_person_ids IS NOT NULL AND JSON_LENGTH(u.proxy_person_ids) > 0;

-- (each temp table is referenced once per statement: MariaDB cannot reopen a temp table)
DROP TEMPORARY TABLE IF EXISTS rm_scope_merge;
CREATE TEMPORARY TABLE rm_scope_merge (canon VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY) DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_scope_merge SELECT DISTINCT canon FROM rm_scope WHERE userID <> keeper;

SELECT 'before' AS phase, 'merge groups with scope/proxy values on a merged-away account' AS what,
       s.canon, s.keeper, s.userID, s.col, s.val
FROM rm_scope s JOIN rm_scope_merge g ON g.canon = s.canon
ORDER BY s.canon, s.col, s.userID;

SELECT 'STOP if listed' AS phase, 'scope/proxy conflicts (settle by hand, then re-run)' AS what,
       canon, col, GROUP_CONCAT(userID ORDER BY userID) AS userIDs
FROM rm_scope GROUP BY canon, col HAVING COUNT(DISTINCT val) > 1;
INSERT INTO rm_abort (reason)
SELECT CONCAT('scope/proxy conflict: ', canon, '.', col, ' differs across userIDs ', GROUP_CONCAT(userID ORDER BY userID))
FROM rm_scope GROUP BY canon, col HAVING COUNT(DISTINCT val) > 1;

DROP TEMPORARY TABLE IF EXISTS rm_scope_pivot;
CREATE TEMPORARY TABLE rm_scope_pivot (
  canon VARCHAR(128) COLLATE utf8mb4_bin PRIMARY KEY,
  spt LONGTEXT, sou LONGTEXT, sin LONGTEXT, ppi LONGTEXT
) DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_scope_pivot
SELECT s.canon,
       MAX(CASE WHEN s.col = 'scope_person_types' THEN s.val END),
       MAX(CASE WHEN s.col = 'scope_org_units'    THEN s.val END),
       MAX(CASE WHEN s.col = 'scope_institutions' THEN s.val END),
       MAX(CASE WHEN s.col = 'proxy_person_ids'   THEN s.val END)
FROM rm_scope s JOIN rm_scope_merge g ON g.canon = s.canon
GROUP BY s.canon;

-- the keeper's own non-empty value is the group's only value (else the abort above fired)
UPDATE admin_users u JOIN rm_keep k ON u.userID = k.keeper JOIN rm_scope_pivot v ON v.canon = k.canon
SET u.scope_person_types = COALESCE(v.spt, u.scope_person_types),
    u.scope_org_units    = COALESCE(v.sou, u.scope_org_units),
    u.scope_institutions = COALESCE(v.sin, u.scope_institutions),
    u.proxy_person_ids   = COALESCE(v.ppi, u.proxy_person_ids);
SELECT 'scope/proxy carried to keeper' AS step, v.canon, k.keeper, v.spt, v.sou, v.sin, v.ppi
FROM rm_scope_pivot v JOIN rm_keep k ON k.canon = v.canon ORDER BY v.canon;

-- Account fields: the keeper gets status = MAX(status) over its group (a merged-away
-- account that is the active one must not leave the curator locked out on an inactive
-- keeper), and its own email / names unless empty, else the group's first non-empty
-- value by userID (COALESCE; the keeper is the lowest userID, so its value comes first).
DROP TEMPORARY TABLE IF EXISTS rm_acct_fields;
CREATE TEMPORARY TABLE rm_acct_fields (
  keeper INT PRIMARY KEY, status_max INT,
  email VARCHAR(128), nameFirst VARCHAR(128), nameMiddle VARCHAR(128), nameLast VARCHAR(128)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
INSERT INTO rm_acct_fields
SELECT k.keeper, MAX(u.status),
       SUBSTRING_INDEX(GROUP_CONCAT(NULLIF(TRIM(u.email), '')      ORDER BY u.userID SEPARATOR x'1F'), x'1F', 1),
       SUBSTRING_INDEX(GROUP_CONCAT(NULLIF(TRIM(u.nameFirst), '')  ORDER BY u.userID SEPARATOR x'1F'), x'1F', 1),
       SUBSTRING_INDEX(GROUP_CONCAT(NULLIF(TRIM(u.nameMiddle), '') ORDER BY u.userID SEPARATOR x'1F'), x'1F', 1),
       SUBSTRING_INDEX(GROUP_CONCAT(NULLIF(TRIM(u.nameLast), '')   ORDER BY u.userID SEPARATOR x'1F'), x'1F', 1)
FROM rm_keep k JOIN rm_acct a ON a.canon = k.canon JOIN admin_users u ON u.userID = a.userID
GROUP BY k.keeper;
UPDATE admin_users u JOIN rm_acct_fields f ON u.userID = f.keeper
SET u.status     = COALESCE(f.status_max, u.status),
    u.email      = CASE WHEN TRIM(COALESCE(u.email, '')) <> ''      THEN u.email      ELSE f.email END,
    u.nameFirst  = CASE WHEN TRIM(COALESCE(u.nameFirst, '')) <> ''  THEN u.nameFirst  ELSE f.nameFirst END,
    u.nameMiddle = CASE WHEN TRIM(COALESCE(u.nameMiddle, '')) <> '' THEN u.nameMiddle ELSE f.nameMiddle END,
    u.nameLast   = CASE WHEN TRIM(COALESCE(u.nameLast, '')) <> ''   THEN u.nameLast   ELSE f.nameLast END;
SELECT 'keeper account after merge' AS step, k.canon, u.userID AS keeper, u.status, u.email, u.nameFirst, u.nameLast
FROM rm_keep k JOIN admin_users u ON u.userID = k.keeper
WHERE k.keeper IN (SELECT keeper FROM rm_loser) ORDER BY k.canon;

DELETE u FROM admin_users u JOIN rm_loser l ON u.userID = l.userID;

UPDATE admin_users u JOIN rm_keep k ON u.userID = k.keeper
SET u.personIdentifier = k.canon
WHERE BINARY u.personIdentifier <> BINARY k.canon;
UPDATE admin_notification_preferences p JOIN rm_keep k ON p.userID = k.keeper
SET p.personIdentifier = k.canon
WHERE p.personIdentifier IS NOT NULL AND BINARY p.personIdentifier <> BINARY k.canon;

-- ---------------------------------------------------------------- 3. role / department dedupe (global)
DELETE r FROM admin_users_roles r
  JOIN admin_users_roles r2 ON r.userID <=> r2.userID AND r.roleID <=> r2.roleID AND r.id > r2.id;
DELETE d FROM admin_users_departments d
  JOIN admin_users_departments d2 ON d.userID <=> d2.userID AND d.departmentID <=> d2.departmentID AND d.id > d2.id;
SELECT 'userID 46 after dedupe' AS step, u.userID, CONCAT('[', COALESCE(u.personIdentifier, 'NULL'), ']') AS pid,
       u.nameFirst, u.nameLast, u.email, u.status, GROUP_CONCAT(r.roleID ORDER BY r.roleID) AS roles
FROM admin_users u LEFT JOIN admin_users_roles r ON r.userID = u.userID
WHERE u.userID = 46 GROUP BY u.userID;

-- ---------------------------------------------------------------- 4. grant_provenance
-- UNIQUE (personIdentifier, pmid, core_project_num) is case-insensitive: UPDATE IGNORE
-- leaves a row that would collide with an existing canonical row, and the DELETE then
-- drops it (the canonical row already records that grant link).
UPDATE IGNORE grant_provenance g JOIN rm_map m ON BINARY g.personIdentifier = BINARY m.bad AND m.action = 'MAP'
SET g.personIdentifier = m.canon;
DELETE g FROM grant_provenance g JOIN rm_map m ON BINARY g.personIdentifier = BINARY m.bad;

-- ---------------------------------------------------------------- 5. admin_feedback_log / authorship_review
UPDATE admin_feedback_log f JOIN rm_map m ON BINARY f.personIdentifier = BINARY m.bad AND m.action = 'MAP'
SET f.personIdentifier = m.canon;
UPDATE authorship_review a JOIN rm_map m ON BINARY a.top_cwid = BINARY m.bad AND m.action = 'MAP'
SET a.top_cwid = m.canon;
UPDATE authorship_review a JOIN rm_map m ON BINARY a.resolution_cwid = BINARY m.bad AND m.action = 'MAP'
SET a.resolution_cwid = m.canon;
-- candidate_cwids_json holds "cwid": "<uid>" entries. REPLACE() is case-sensitive. A
-- multi-table UPDATE changes each row once per statement, so it runs in a loop (with
-- the proxy_person_ids rewrite in 5b) until a pass changes nothing: a row carrying any
-- number of mapped ids converges in one run. The after-count below must be 0.

UPDATE authorship_review a JOIN rm_map m ON BINARY a.reviewer = BINARY m.bad AND m.action = 'MAP'
SET a.reviewer = m.canon;

-- ---------------------------------------------------------------- 5b. admin_users.proxy_person_ids
-- JSON arrays of person ids. A mapped id is rewritten (REPLACE() is case-sensitive);
-- a DELETE-class id is removed. One change per row per statement, so all three rewrites
-- (with candidate_cwids_json above) repeat until a pass changes no row, capped at 100
-- passes; the after-counts must be 0 (enforced below).
DELIMITER //
BEGIN NOT ATOMIC
  DECLARE changed BIGINT DEFAULT 1;
  DECLARE passes INT DEFAULT 0;
  WHILE changed > 0 AND passes < 100 DO
    UPDATE authorship_review a JOIN rm_map m
      ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
    SET a.candidate_cwids_json = REPLACE(a.candidate_cwids_json, CONCAT('"', m.bad, '"'), CONCAT('"', m.canon, '"'));
    SET changed = ROW_COUNT();
    UPDATE admin_users u JOIN rm_map m
      ON m.action = 'MAP' AND INSTR(CAST(u.proxy_person_ids AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
    SET u.proxy_person_ids = REPLACE(u.proxy_person_ids, CONCAT('"', m.bad, '"'), CONCAT('"', m.canon, '"'));
    SET changed = changed + ROW_COUNT();
    UPDATE admin_users u JOIN rm_map m
      ON m.action = 'DELETE' AND INSTR(CAST(u.proxy_person_ids AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
    SET u.proxy_person_ids = COALESCE(JSON_REMOVE(u.proxy_person_ids, JSON_UNQUOTE(JSON_SEARCH(u.proxy_person_ids, 'one', m.bad))), u.proxy_person_ids);
    SET changed = changed + ROW_COUNT();
    SET passes = passes + 1;
  END WHILE;
END//
DELIMITER ;

-- ---------------------------------------------------------------- 6. reporting_ad_hoc_*
-- map, then trim + lowercase (external-cohort ids: trim only), then drop DELETE-class
UPDATE reporting_ad_hoc_feature_generator_execution x JOIN rm_map m
  ON BINARY x.personIdentifier = BINARY m.bad AND m.action = 'MAP' SET x.personIdentifier = m.canon;
UPDATE reporting_ad_hoc_identity_creation x JOIN rm_map m
  ON BINARY x.personIdentifier = BINARY m.bad AND m.action = 'MAP' SET x.personIdentifier = m.canon;
DELETE x FROM reporting_ad_hoc_feature_generator_execution x JOIN rm_map m
  ON BINARY x.personIdentifier = BINARY m.bad AND m.action = 'DELETE';
DELETE x FROM reporting_ad_hoc_identity_creation x JOIN rm_map m
  ON BINARY x.personIdentifier = BINARY m.bad AND m.action = 'DELETE';
UPDATE reporting_ad_hoc_feature_generator_execution
SET personIdentifier = CASE WHEN BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)'
                            THEN TRIM(personIdentifier) ELSE LOWER(TRIM(personIdentifier)) END
WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier)
   OR (BINARY personIdentifier <> BINARY LOWER(personIdentifier)
       AND NOT BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)');
UPDATE reporting_ad_hoc_identity_creation
SET personIdentifier = CASE WHEN BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)'
                            THEN TRIM(personIdentifier) ELSE LOWER(TRIM(personIdentifier)) END
WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier)
   OR (BINARY personIdentifier <> BINARY LOWER(personIdentifier)
       AND NOT BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)');
-- the cron reads every row: one (uid, frequency, type) registration is enough
DELETE x FROM reporting_ad_hoc_feature_generator_execution x
  JOIN reporting_ad_hoc_feature_generator_execution x2
    ON BINARY x.personIdentifier = BINARY x2.personIdentifier
   AND x.frequency <=> x2.frequency AND x.`type` <=> x2.`type` AND x.id > x2.id;

-- =============================================================================
-- AFTER COUNTS (inside the transaction: what COMMIT would leave)
-- =============================================================================
INSERT INTO rm_check SELECT 'after', 'admin_users junk', COUNT(*) FROM admin_users WHERE BINARY personIdentifier IN (SELECT BINARY pid FROM rm_junk_pid) AND status <> 0;
INSERT INTO rm_check SELECT 'after', 'admin_users mapped', COUNT(*) FROM admin_users u JOIN rm_map m ON BINARY u.personIdentifier = BINARY m.bad AND m.action = 'MAP';
INSERT INTO rm_check SELECT 'after', 'admin_users duplicate canon', COUNT(*) FROM (SELECT CAST(CASE WHEN BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)' THEN TRIM(personIdentifier) ELSE LOWER(TRIM(personIdentifier)) END AS BINARY) c FROM admin_users WHERE personIdentifier IS NOT NULL AND TRIM(personIdentifier) <> '' AND BINARY personIdentifier NOT IN (SELECT BINARY pid FROM rm_junk_pid) GROUP BY c HAVING COUNT(*) > 1) d;
INSERT INTO rm_check SELECT 'after', 'admin_users_roles duplicates', COUNT(*) - COUNT(DISTINCT userID, roleID) FROM admin_users_roles;
INSERT INTO rm_check SELECT 'after', 'grant_provenance mapped/deleted', COUNT(*) FROM grant_provenance g JOIN rm_map m ON BINARY g.personIdentifier = BINARY m.bad;
INSERT INTO rm_check SELECT 'after', 'admin_feedback_log mapped', COUNT(*) FROM admin_feedback_log f JOIN rm_map m ON BINARY f.personIdentifier = BINARY m.bad AND m.action = 'MAP';
INSERT INTO rm_check SELECT 'after', 'authorship_review.top_cwid mapped', COUNT(*) FROM authorship_review a JOIN rm_map m ON BINARY a.top_cwid = BINARY m.bad AND m.action = 'MAP';
INSERT INTO rm_check SELECT 'after', 'authorship_review.resolution_cwid mapped', COUNT(*) FROM authorship_review a JOIN rm_map m ON BINARY a.resolution_cwid = BINARY m.bad AND m.action = 'MAP';
INSERT INTO rm_check SELECT 'after', 'authorship_review.candidate_cwids_json mapped', COUNT(DISTINCT a.id) FROM authorship_review a JOIN rm_map m ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0;
INSERT INTO rm_check SELECT 'after', 'reporting_ad_hoc_feature_generator_execution untrimmed', COUNT(*) FROM reporting_ad_hoc_feature_generator_execution WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier);
INSERT INTO rm_check SELECT 'after', 'reporting_ad_hoc_identity_creation untrimmed', COUNT(*) FROM reporting_ad_hoc_identity_creation WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier);
INSERT INTO rm_check SELECT 'after', 'admin_users personIdentifier not byte-equal to its canonical id', COUNT(*) FROM admin_users u JOIN rm_acct a ON a.userID = u.userID WHERE BINARY u.personIdentifier <> BINARY a.canon;
INSERT INTO rm_check SELECT 'after', 'admin_users untrimmed/upper-case (external cohorts excepted)', COUNT(*) FROM admin_users
 WHERE personIdentifier IS NOT NULL AND TRIM(personIdentifier) <> ''
   AND BINARY personIdentifier NOT IN (SELECT BINARY pid FROM rm_junk_pid)
   AND (BINARY personIdentifier <> BINARY TRIM(personIdentifier)
        OR (BINARY personIdentifier <> BINARY LOWER(personIdentifier)
            AND NOT BINARY TRIM(personIdentifier) REGEXP '^(usc_|fh_|fredhutch_|uc[a-z]+_)'));
INSERT INTO rm_check SELECT 'after', 'admin_users.proxy_person_ids mapped/deleted', COUNT(DISTINCT u.userID) FROM admin_users u JOIN rm_map m ON INSTR(CAST(u.proxy_person_ids AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0;
INSERT INTO rm_check SELECT 'after', 'authorship_review.reviewer mapped', COUNT(*) FROM authorship_review a JOIN rm_map m ON BINARY a.reviewer = BINARY m.bad AND m.action = 'MAP';
SELECT 'after' AS phase, name AS tbl, n, 'expect 0' AS expect FROM rm_check WHERE phase = 'after';
SELECT 'after' AS phase, 'admin_users accounts that changed (force these to re-login)' AS what,
       u.userID, u.personIdentifier FROM admin_users u JOIN rm_keep k ON u.userID = k.keeper ORDER BY u.personIdentifier;

-- =============================================================================
-- ENFORCE: every GUARD and after-check must be 0, or the run stops here
-- ("CONSTRAINT `stop_here` failed"), the client exits, and the open transaction is
-- rolled back: neither ROLLBACK nor COMMIT below is reached.
-- =============================================================================
SELECT 'STOP if listed' AS phase, 'checks not 0' AS what, phase AS check_phase, name, n
FROM rm_check WHERE n > 0;
INSERT INTO rm_abort (reason)
SELECT CONCAT(phase, ' check failed: ', name, ' = ', n) FROM rm_check WHERE n > 0;

-- =============================================================================
-- FINISH. Default is ROLLBACK (dry run). To apply: comment out ROLLBACK and
-- uncomment COMMIT, run again, and keep the log.
-- =============================================================================
ROLLBACK;
-- COMMIT;
