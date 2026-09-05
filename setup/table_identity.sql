-- reciterdb.identity -- checked in for the first time on 2026-09-05.
--
-- This table was created by hand through the Splunk DB Connect UI and has never
-- been in this repo. The CREATE below is the live production definition as of
-- 2026-09-05, transcribed verbatim (including the index literally named
-- `dfsdfsdf`, kept so this file matches production rather than improving on it).
--
-- The table is populated by update/buildIdentity.py, which replaced the Splunk
-- saved search "reciter identity update". It is CUMULATIVE: rows are upserted on
-- cwid and never deleted, so department/division survive after someone drops out
-- of ED's ou=canonical. Do NOT add this table to the shadow-build / atomic-swap
-- pattern in setup/person_table_swap.sql -- a swap would delete every person who
-- falls out of the current population.
--
-- `notes` (1,979 rows) and `alumniResidentNYP` (778 rows) are written by
-- something outside buildIdentity.py and are absent from its UPSERT_COLUMNS so
-- they are never clobbered. Their writer is still unidentified.

CREATE TABLE IF NOT EXISTS `identity` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cwid` varchar(128) DEFAULT NULL,
  `surname` varchar(128) DEFAULT NULL,
  `middleName` varchar(128) DEFAULT NULL,
  `givenName` varchar(128) DEFAULT NULL,
  `primaryTitle` varchar(128) DEFAULT NULL,
  `primaryAcademicDepartment` varchar(128) DEFAULT NULL,
  `primaryAcademicDivision` varchar(128) DEFAULT NULL,
  `primaryProgram` varchar(128) DEFAULT NULL,
  `fullTimeFaculty` varchar(128) DEFAULT NULL,
  `postdoc` varchar(11) DEFAULT NULL,
  `studentMDNYC` varchar(12) DEFAULT NULL,
  `studentMDQatar` varchar(12) DEFAULT NULL,
  `studentMDPhD` varchar(12) DEFAULT NULL,
  `studentPhDTriI` varchar(12) DEFAULT NULL,
  `studentPhDWeill` varchar(12) DEFAULT NULL,
  `partTimeFaculty` varchar(11) DEFAULT NULL,
  `voluntaryFaculty` varchar(11) DEFAULT NULL,
  `emeritusFaculty` varchar(11) DEFAULT NULL,
  `adjunctFaculty` varchar(11) DEFAULT NULL,
  `residentNYP` varchar(11) DEFAULT NULL,
  `fellow` varchar(11) DEFAULT NULL,
  `faculty` varchar(11) DEFAULT NULL,
  `nonFaculty` varchar(11) DEFAULT NULL,
  `inactiveFaculty` varchar(11) DEFAULT NULL,
  `alumniMD` varchar(11) DEFAULT NULL,
  `alumniMDPHD` varchar(11) DEFAULT NULL,
  `alumniPHD` varchar(11) DEFAULT NULL,
  `alumniResidentNYP` varchar(11) DEFAULT NULL,
  `inactiveNonAlumniStudent` varchar(128) DEFAULT NULL,
  `startDateWCMFaculty` int(11) DEFAULT NULL,
  `endDateWCMFaculty` int(11) DEFAULT NULL,
  `startDateWCMStudent` int(11) DEFAULT NULL,
  `endDateWCMStudent` int(11) DEFAULT NULL,
  `popsProfile` varchar(128) DEFAULT NULL,
  `directoryProfile` varchar(128) DEFAULT NULL,
  `vivoProfile` varchar(128) DEFAULT NULL,
  `facultyRank` varchar(128) DEFAULT NULL,
  `primaryOrg` varchar(128) DEFAULT NULL,
  `notes` varchar(128) DEFAULT NULL,
  `createTimestamp` timestamp NULL DEFAULT current_timestamp(),
  `modifyTimestamp` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `dfsdfsdf` (`cwid`) USING BTREE,
  KEY `idx_fulltime_cwid` (`fullTimeFaculty`,`cwid`),
  KEY `idx_identity_cwid_ftf` (`cwid`,`fullTimeFaculty`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- REQUIRED before buildIdentity.py can run. Its upsert is
-- INSERT ... ON DUPLICATE KEY UPDATE, which needs a unique key on cwid to fire;
-- without it every run would insert 35k duplicate rows instead of updating.
--
-- Safe to apply: cwid was already unique in production on 2026-09-05
-- (35,448 rows / 35,448 distinct cwids). Verify before applying, then run:
--
--   SELECT COUNT(*) rows_total, COUNT(DISTINCT cwid) cwids FROM identity;
--
-- ALTER TABLE `identity` ADD UNIQUE KEY `uq_identity_cwid` (`cwid`);


-- `modifyTimestamp` has DEFAULT current_timestamp() but no ON UPDATE clause, so
-- it records insert time, not modify time -- identical to createTimestamp. Same
-- trap as person.dateAdded: do not date incidents from it. Left as-is because
-- adding ON UPDATE would silently change the meaning of 35k existing rows.
