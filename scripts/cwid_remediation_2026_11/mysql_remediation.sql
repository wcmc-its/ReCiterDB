-- =============================================================================
-- CWID remediation, MySQL half (reciterdb, 2026-11)
--
-- One transaction. Ends in ROLLBACK by default: run it once as a dry run, read
-- the counts, then switch the last lines to COMMIT (see the bottom of the file)
-- and run it again. Every statement is idempotent: a second COMMIT run changes 0
-- rows.
--
-- Run (never with --force: the first error must stop the script, and the open
-- transaction is then rolled back when the client disconnects):
--   mysql --defaults-file=<prod-creds.cnf> reciterdb < mysql_remediation.sql | tee mysql_run_$(date -u +%Y%m%dT%H%M%SZ).log
--
-- Mapping: the rm_map rows below are the MAP/DELETE rows of mapping.json
-- (dynamo_remediation.py --selftest checks they match). UNDERSCORE ids (legacy
-- PM external logins such as _pah4001 = Paul A. Harris, Vanderbilt) are NOT
-- touched here: their CWID look-alikes in ED are different people.
--
-- Comparisons that must be case- or space-exact use BINARY: every table here is
-- utf8mb4_unicode_ci, where 'Abl9002' = 'abl9002' and 'ggi9001 ' = 'ggi9001'.
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
       (SELECT COUNT(*) FROM admin_feedback_log f WHERE f.userID = u.userID) AS n_feedback_authored
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
-- guards: expected 0 (Phase 0); a non-zero here means extend the script before COMMIT
SELECT 'before' AS phase, 'identity_grad_school_faculty (non-canonical)' AS tbl, COUNT(*) AS n FROM identity_grad_school_faculty WHERE BINARY cwid <> BINARY LOWER(TRIM(cwid));
SELECT 'before' AS phase, 'GUARD admin_orcid' AS tbl, COUNT(*) AS n FROM admin_orcid o JOIN rm_map m ON BINARY o.personIdentifier = BINARY m.bad;
SELECT 'before' AS phase, 'GUARD analysis_override_author_position' AS tbl, COUNT(*) AS n FROM analysis_override_author_position o JOIN rm_map m ON BINARY o.personIdentifier = BINARY m.bad;
SELECT 'before' AS phase, 'GUARD external_record' AS tbl, COUNT(*) AS n FROM external_record o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;
SELECT 'before' AS phase, 'GUARD external_rejection' AS tbl, COUNT(*) AS n FROM external_rejection o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;
SELECT 'before' AS phase, 'GUARD person_author_id' AS tbl, COUNT(*) AS n FROM person_author_id o JOIN rm_map m ON BINARY o.uid = BINARY m.bad;

-- =============================================================================
-- WRITES
-- =============================================================================
START TRANSACTION;

-- ---------------------------------------------------------------- 1. junk PM accounts
-- Accounts that authored admin_feedback_log rows are deactivated (status=0, roles and
-- departments removed) instead of deleted, so curator history keeps its userID.
DROP TEMPORARY TABLE IF EXISTS rm_junk_acct;
CREATE TEMPORARY TABLE rm_junk_acct (userID INT PRIMARY KEY, keep_inactive TINYINT NOT NULL DEFAULT 0);
INSERT INTO rm_junk_acct (userID)
SELECT userID FROM admin_users WHERE BINARY personIdentifier IN (SELECT BINARY pid FROM rm_junk_pid);
UPDATE rm_junk_acct j SET keep_inactive = 1
WHERE EXISTS (SELECT 1 FROM admin_feedback_log f WHERE f.userID = j.userID);

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
         CASE WHEN BINARY LEFT(TRIM(u.personIdentifier), 4) = 'usc_'
                OR BINARY LEFT(TRIM(u.personIdentifier), 3) = 'fh_'
                OR BINARY LEFT(TRIM(u.personIdentifier), 10) = 'fredhutch_'
                OR BINARY LEFT(TRIM(u.personIdentifier), 5) = 'ucsf_'
                OR BINARY LEFT(TRIM(u.personIdentifier), 2) = 'uc'
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
HAVING COUNT(*) > 1 OR SUM(cur <> canon) > 0;

DROP TEMPORARY TABLE IF EXISTS rm_loser;
CREATE TEMPORARY TABLE rm_loser (userID INT PRIMARY KEY, keeper INT NOT NULL, canon VARCHAR(128) COLLATE utf8mb4_bin)
  DEFAULT CHARSET=utf8mb4;
INSERT INTO rm_loser
SELECT a.userID, k.keeper, k.canon FROM rm_acct a JOIN rm_keep k ON a.canon = k.canon WHERE a.userID <> k.keeper;

SELECT 'account merges' AS step, l.canon, l.keeper, l.userID AS merged_away FROM rm_loser l ORDER BY l.canon;

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
-- multi-table UPDATE changes each row once per statement, so repeat it for rows that
-- carry two mapped ids; the after-count below must be 0.
UPDATE authorship_review a JOIN rm_map m
  ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
SET a.candidate_cwids_json = REPLACE(a.candidate_cwids_json, CONCAT('"', m.bad, '"'), CONCAT('"', m.canon, '"'));
UPDATE authorship_review a JOIN rm_map m
  ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
SET a.candidate_cwids_json = REPLACE(a.candidate_cwids_json, CONCAT('"', m.bad, '"'), CONCAT('"', m.canon, '"'));
UPDATE authorship_review a JOIN rm_map m
  ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0
SET a.candidate_cwids_json = REPLACE(a.candidate_cwids_json, CONCAT('"', m.bad, '"'), CONCAT('"', m.canon, '"'));

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
SET personIdentifier = CASE WHEN BINARY LEFT(TRIM(personIdentifier), 4) = 'usc_'
                              OR BINARY LEFT(TRIM(personIdentifier), 3) = 'fh_'
                              OR BINARY LEFT(TRIM(personIdentifier), 10) = 'fredhutch_'
                              OR BINARY LEFT(TRIM(personIdentifier), 5) = 'ucsf_'
                              OR BINARY LEFT(TRIM(personIdentifier), 2) = 'uc'
                            THEN TRIM(personIdentifier) ELSE LOWER(TRIM(personIdentifier)) END
WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier)
   OR (BINARY personIdentifier <> BINARY LOWER(personIdentifier)
       AND NOT (BINARY LEFT(TRIM(personIdentifier), 4) = 'usc_' OR BINARY LEFT(TRIM(personIdentifier), 3) = 'fh_'
                OR BINARY LEFT(TRIM(personIdentifier), 10) = 'fredhutch_' OR BINARY LEFT(TRIM(personIdentifier), 5) = 'ucsf_'
                OR BINARY LEFT(TRIM(personIdentifier), 2) = 'uc'));
UPDATE reporting_ad_hoc_identity_creation
SET personIdentifier = CASE WHEN BINARY LEFT(TRIM(personIdentifier), 4) = 'usc_'
                              OR BINARY LEFT(TRIM(personIdentifier), 3) = 'fh_'
                              OR BINARY LEFT(TRIM(personIdentifier), 10) = 'fredhutch_'
                              OR BINARY LEFT(TRIM(personIdentifier), 5) = 'ucsf_'
                              OR BINARY LEFT(TRIM(personIdentifier), 2) = 'uc'
                            THEN TRIM(personIdentifier) ELSE LOWER(TRIM(personIdentifier)) END
WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier)
   OR (BINARY personIdentifier <> BINARY LOWER(personIdentifier)
       AND NOT (BINARY LEFT(TRIM(personIdentifier), 4) = 'usc_' OR BINARY LEFT(TRIM(personIdentifier), 3) = 'fh_'
                OR BINARY LEFT(TRIM(personIdentifier), 10) = 'fredhutch_' OR BINARY LEFT(TRIM(personIdentifier), 5) = 'ucsf_'
                OR BINARY LEFT(TRIM(personIdentifier), 2) = 'uc'));
-- the cron reads every row: one (uid, frequency, type) registration is enough
DELETE x FROM reporting_ad_hoc_feature_generator_execution x
  JOIN reporting_ad_hoc_feature_generator_execution x2
    ON BINARY x.personIdentifier = BINARY x2.personIdentifier
   AND x.frequency <=> x2.frequency AND x.`type` <=> x2.`type` AND x.id > x2.id;

-- ---------------------------------------------------------------- 7. identity_grad_school_faculty
UPDATE IGNORE identity_grad_school_faculty SET cwid = LOWER(TRIM(cwid))
WHERE BINARY cwid <> BINARY LOWER(TRIM(cwid));

-- =============================================================================
-- AFTER COUNTS (inside the transaction: what COMMIT would leave)
-- =============================================================================
SELECT 'after' AS phase, 'admin_users junk' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM admin_users WHERE BINARY personIdentifier IN (SELECT BINARY pid FROM rm_junk_pid) AND status <> 0;
SELECT 'after' AS phase, 'admin_users mapped' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM admin_users u JOIN rm_map m ON BINARY u.personIdentifier = BINARY m.bad AND m.action = 'MAP';
SELECT 'after' AS phase, 'admin_users duplicate canon' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM (SELECT LOWER(TRIM(personIdentifier)) c FROM admin_users WHERE personIdentifier IS NOT NULL AND TRIM(personIdentifier) <> '' GROUP BY c HAVING COUNT(*) > 1) d;
SELECT 'after' AS phase, 'admin_users_roles duplicates' AS tbl, COUNT(*) - COUNT(DISTINCT userID, roleID) AS n, 'expect 0' AS expect FROM admin_users_roles;
SELECT 'after' AS phase, 'grant_provenance mapped/deleted' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM grant_provenance g JOIN rm_map m ON BINARY g.personIdentifier = BINARY m.bad;
SELECT 'after' AS phase, 'admin_feedback_log mapped' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM admin_feedback_log f JOIN rm_map m ON BINARY f.personIdentifier = BINARY m.bad AND m.action = 'MAP';
SELECT 'after' AS phase, 'authorship_review.top_cwid mapped' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM authorship_review a JOIN rm_map m ON BINARY a.top_cwid = BINARY m.bad AND m.action = 'MAP';
SELECT 'after' AS phase, 'authorship_review.resolution_cwid mapped' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM authorship_review a JOIN rm_map m ON BINARY a.resolution_cwid = BINARY m.bad AND m.action = 'MAP';
SELECT 'after' AS phase, 'authorship_review.candidate_cwids_json mapped' AS tbl, COUNT(DISTINCT a.id) AS n, 'expect 0' AS expect FROM authorship_review a JOIN rm_map m ON m.action = 'MAP' AND INSTR(CAST(a.candidate_cwids_json AS BINARY), CAST(CONCAT('"', m.bad, '"') AS BINARY)) > 0;
SELECT 'after' AS phase, 'reporting_ad_hoc_feature_generator_execution untrimmed' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM reporting_ad_hoc_feature_generator_execution WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier);
SELECT 'after' AS phase, 'reporting_ad_hoc_identity_creation untrimmed' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM reporting_ad_hoc_identity_creation WHERE BINARY personIdentifier <> BINARY TRIM(personIdentifier);
SELECT 'after' AS phase, 'identity_grad_school_faculty non-canonical' AS tbl, COUNT(*) AS n, 'expect 0' AS expect FROM identity_grad_school_faculty WHERE BINARY cwid <> BINARY LOWER(TRIM(cwid));
SELECT 'after' AS phase, 'admin_users accounts that changed (force these to re-login)' AS what,
       u.userID, u.personIdentifier FROM admin_users u JOIN rm_keep k ON u.userID = k.keeper ORDER BY u.personIdentifier;

-- =============================================================================
-- FINISH. Default is ROLLBACK (dry run). To apply: comment out ROLLBACK and
-- uncomment COMMIT, run again, and keep the log.
-- =============================================================================
ROLLBACK;
-- COMMIT;
