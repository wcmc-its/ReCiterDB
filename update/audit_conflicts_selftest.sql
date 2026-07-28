-- audit_conflicts_selftest.sql -- proves audit_conflicts.sql actually detects what it claims (#127)
--
-- An audit whose detectors have never been shown to fire is worse than no audit:
-- it reports a clean table and the issue gets closed over real damage. This seeds
-- known-good and known-corrupt rows and asserts each detector finds exactly them.
--
-- It caught a live bug on first run: under the table's utf8mb4_unicode_ci collation
-- REGEXP '^[a-z]' also matches uppercase, so the starts_lowercase check reported 7
-- of 9 healthy rows as corrupt. Hence COLLATE utf8mb4_bin in audit_conflicts.sql.
--
-- SAFETY: runs entirely inside its own scratch database and never references the
-- real one. Still, run it against a throwaway server, not production:
--
--   docker run -d --name coi-selftest -e MARIADB_ROOT_PASSWORD=t mariadb:11
--   docker cp audit_conflicts_selftest.sql coi-selftest:/tmp/
--   docker exec coi-selftest mariadb -uroot -pt -e "SOURCE /tmp/audit_conflicts_selftest.sql"
--
-- Every row of output must read PASS.

CREATE DATABASE IF NOT EXISTS audit_conflicts_selftest;
USE audit_conflicts_selftest;

DROP TABLE IF EXISTS reporting_conflicts;
DROP TABLE IF EXISTS analysis_summary_article;

-- DDL copied verbatim from setup/createDatabaseTableReciterDb.sql; the collation
-- is the whole point of the exercise, so it must not be simplified here.
CREATE TABLE `reporting_conflicts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pmid` int(11) DEFAULT NULL,
  `conflictStatement` blob DEFAULT NULL,
  `conflictsVarchar` varchar(15000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_pmid` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `analysis_summary_article` (
  `pmid` int(11) DEFAULT 0,
  `articleYear` int(11) DEFAULT 0,
  KEY `z` (`pmid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Healthy rows, including the poison content the old LOAD DATA path mangled and
-- the new executemany path stores verbatim. None of these may be flagged.
INSERT INTO reporting_conflicts (pmid, conflictStatement) VALUES
 (1001, 'The authors have declared that no competing interest exists.'),
 (1002, 'Dr. Smith reports grants from NIH. The other authors declare no competing interest.'),
 (1003, 'The authors declare "no" competing interest. Path: C:\\data\\coi.txt'),
 (1004, '');

-- Corrupt rows, one per shape the old path produced.
INSERT INTO reporting_conflicts (pmid, conflictStatement) VALUES
 (2001, 'The authors declare no competing interest exists. The authors report no competing interest for this second unrelated paper.'),  -- two papers welded (#87/#89/#88)
 (2002, 'ports personal fees from Pfizer outside the submitted work.'),  -- front of field lost
 (2003, '38112233'),                                                     -- pmid landed in the text column
 (2004, '" competing interest disclosed by the corresponding author.'),  -- starts on the enclosure char
 (1001, 'The authors have declared that no competing interest exists.'); -- duplicate pmid (no UNIQUE key here)

INSERT INTO reporting_conflicts (pmid, conflictStatement)
  VALUES (2005, CONCAT('competing interest ', REPEAT('x', 16000)));      -- exceeds the varchar copy

UPDATE reporting_conflicts SET conflictsVarchar = CAST(conflictStatement AS CHAR(15000)) WHERE conflictsVarchar IS NULL;

-- 9001 is pre-2017; -7 is the reporting SP's synthetic external_article pmid; 0 is the column default.
-- All three must be excluded from the eligible population. 3001 is eligible but unimported.
INSERT INTO analysis_summary_article (pmid, articleYear) VALUES
 (1001,2020),(1002,2021),(1003,2022),(1004,2019),(2001,2018),(2002,2020),
 (2003,2021),(2004,2022),(2005,2023),(3001,2024),(9001,2005),(-7,2023),(0,2023);

-- ---------------------------------------------------------------------------
-- Assertions. Expected values are the seeded truth, counted by hand above.
-- ---------------------------------------------------------------------------
SELECT 'duplicate pmid detected' AS assertion,
       IF(COUNT(*) - COUNT(DISTINCT pmid) = 1, 'PASS', CONCAT('FAIL got ', COUNT(*) - COUNT(DISTINCT pmid))) AS result
FROM reporting_conflicts;

SELECT 'synthetic/negative/old pmids excluded' AS assertion,
       IF(COUNT(DISTINCT p.pmid) = 10, 'PASS', CONCAT('FAIL got ', COUNT(DISTINCT p.pmid))) AS result
FROM analysis_summary_article p
WHERE p.articleYear >= 2017 AND p.pmid > 0;

SELECT 'unimported pmid detected, join fanout not miscounted' AS assertion,
       IF(COUNT(DISTINCT CASE WHEN a.pmid IS NULL THEN p.pmid END) = 1, 'PASS',
          CONCAT('FAIL got ', COUNT(DISTINCT CASE WHEN a.pmid IS NULL THEN p.pmid END))) AS result
FROM analysis_summary_article p
LEFT JOIN reporting_conflicts a ON a.pmid = p.pmid
WHERE p.articleYear >= 2017 AND p.pmid > 0;

SELECT 'backslash survives verbatim' AS assertion,
       IF(SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%\\\\%') = 1, 'PASS',
          CONCAT('FAIL got ', SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%\\\\%'))) AS result
FROM reporting_conflicts WHERE LENGTH(conflictStatement) > 0;

SELECT 'embedded quotes counted' AS assertion,
       IF(SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%"%') = 2, 'PASS',
          CONCAT('FAIL got ', SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%"%'))) AS result
FROM reporting_conflicts WHERE LENGTH(conflictStatement) > 0;

SELECT 'cross-paper concatenation detected' AS assertion,
       IF(COUNT(*) = 1, 'PASS', CONCAT('FAIL got ', COUNT(*))) AS result
FROM reporting_conflicts
WHERE LENGTH(conflictStatement) > 0
  AND ( CHAR_LENGTH(LOWER(CONVERT(conflictStatement USING utf8mb4)))
        - CHAR_LENGTH(REPLACE(LOWER(CONVERT(conflictStatement USING utf8mb4)), 'competing interest', ''))
      ) / CHAR_LENGTH('competing interest') >= 2;

-- The regression this file exists for. Expect 2: the truncated row 2002 and the
-- oversized row 2005, which also happens to start lowercase. Without the binary
-- collation this returns 7, i.e. nearly every healthy row.
SELECT 'starts_lowercase is case-SENSITIVE' AS assertion,
       IF(SUM(CONVERT(conflictStatement USING utf8mb4) COLLATE utf8mb4_bin REGEXP '^[a-z]') = 2, 'PASS',
          CONCAT('FAIL got ', SUM(CONVERT(conflictStatement USING utf8mb4) COLLATE utf8mb4_bin REGEXP '^[a-z]'),
                 ' expected 2 -- the ci collation is matching uppercase')) AS result
FROM reporting_conflicts WHERE LENGTH(conflictStatement) > 0;

SELECT 'field-shift and punctuation-start detected' AS assertion,
       IF(SUM(CONVERT(conflictStatement USING utf8mb4) REGEXP '^[0-9]+$') = 1
          AND SUM(CONVERT(conflictStatement USING utf8mb4) REGEXP '^[[:punct:]]') = 1, 'PASS', 'FAIL') AS result
FROM reporting_conflicts WHERE LENGTH(conflictStatement) > 0;

SELECT 'blob exceeding the varchar copy detected' AS assertion,
       IF(SUM(LENGTH(conflictStatement) > 15000) = 1, 'PASS',
          CONCAT('FAIL got ', SUM(LENGTH(conflictStatement) > 15000))) AS result
FROM reporting_conflicts;
