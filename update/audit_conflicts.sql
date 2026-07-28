-- audit_conflicts.sql -- read-only damage assessment for reporting_conflicts (#127)
--
-- PR #128/#129 stops conflictsImport.py from CORRUPTING new rows. It repairs
-- nothing: the backfill is `LEFT JOIN ... WHERE a.pmid IS NULL`, so a row that
-- already holds truncated or concatenated text is never revisited.
--
-- This answers "is there anything to repair?" without touching DynamoDB. Run it
-- first. Only if it shows damage is it worth porting auditAbstracts.py (which
-- compares every row against DynamoDB truth) to conflicts.
--
-- Every query reads the BLOB `conflictStatement`, never `conflictsVarchar` --
-- the varchar truncates at 15000 and will understate the damage. CONVERT(...)
-- is required because string functions treat a blob as case-sensitive bytes.
--
-- Read-only: no INSERT/UPDATE/DELETE/DDL anywhere in this file.
--
-- Run: mysql -h "$DB_HOST" -u "$DB_USERNAME" -p "$DB_NAME" < audit_conflicts.sql

-- ---------------------------------------------------------------------------
-- Q1. Coverage and duplicates.
--
-- Note reporting_conflicts has only KEY idx_pmid, NOT the UNIQUE key that
-- alter_add_uq_pmid_reporting_abstracts_v1.4.sql added to reporting_abstracts
-- after the #87 incident. Duplicate pmids are therefore possible here and are
-- worth knowing about before any repair.
-- ---------------------------------------------------------------------------
SELECT 'Q1 coverage' AS check_name,
       COUNT(*)                                             AS rows_total,
       COUNT(DISTINCT pmid)                                 AS pmids_distinct,
       COUNT(*) - COUNT(DISTINCT pmid)                      AS duplicate_rows,
       SUM(conflictStatement IS NULL OR LENGTH(conflictStatement) = 0) AS empty_statements,
       SUM(LENGTH(conflictStatement) > 0)                   AS nonempty_statements
FROM reporting_conflicts;

-- Eligible population the importer targets, for comparison against rows_total.
-- COUNT(DISTINCT ...) rather than SUM(): a duplicated pmid in reporting_conflicts
-- fans the join out, so a plain row count would misreport.
SELECT 'Q1 eligible' AS check_name,
       COUNT(DISTINCT p.pmid) AS eligible_pmids,
       COUNT(DISTINCT CASE WHEN a.pmid IS NULL THEN p.pmid END) AS still_missing
FROM analysis_summary_article p
LEFT JOIN reporting_conflicts a ON a.pmid = p.pmid
WHERE p.articleYear >= 2017
  AND p.pmid > 0;

-- ---------------------------------------------------------------------------
-- Q2. The escaping skew -- the tell the issue calls out.
--
-- LOAD DATA defaulted to ESCAPED BY '\\', so a backslash in the source text was
-- consumed as an escape character rather than stored as data. If essentially no
-- stored statement contains a backslash while a meaningful share contain other
-- punctuation, the backslashes were eaten. A doubled quote ("") is csv.writer's
-- escaping leaking through as literal data.
-- ---------------------------------------------------------------------------
SELECT 'Q2 escaping skew' AS check_name,
       COUNT(*)                                                                  AS nonempty_rows,
       SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%\\\\%')               AS contains_backslash,
       SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%"%')                  AS contains_quote,
       SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%""%')                 AS contains_doubled_quote,
       SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%\t%')                 AS contains_tab,
       SUM(CONVERT(conflictStatement USING utf8mb4) LIKE '%,%')                  AS contains_comma_control
FROM reporting_conflicts
WHERE LENGTH(conflictStatement) > 0;

-- ---------------------------------------------------------------------------
-- Q3. Cross-paper concatenation -- the #87/#89/#88 shape.
--
-- COI statements are highly stereotyped. A row where the boilerplate opening
-- appears two or more times is two papers' text welded together by a desynced
-- LOAD DATA parse. Occurrence count via the standard length-of-REPLACE idiom.
-- ---------------------------------------------------------------------------
SELECT 'Q3 concatenation' AS check_name,
       COUNT(*) AS suspect_rows
FROM reporting_conflicts
WHERE LENGTH(conflictStatement) > 0
  AND ( CHAR_LENGTH(LOWER(CONVERT(conflictStatement USING utf8mb4)))
        - CHAR_LENGTH(REPLACE(LOWER(CONVERT(conflictStatement USING utf8mb4)),
                              'competing interest', ''))
      ) / CHAR_LENGTH('competing interest') >= 2;

-- The worst offenders, to eyeball. If these read as one coherent statement the
-- detector is just seeing a wordy disclosure; if they read as two papers, it is real.
SELECT pmid,
       LENGTH(conflictStatement) AS blob_bytes,
       LEFT(CONVERT(conflictStatement USING utf8mb4), 300) AS head_300
FROM reporting_conflicts
WHERE LENGTH(conflictStatement) > 0
  AND ( CHAR_LENGTH(LOWER(CONVERT(conflictStatement USING utf8mb4)))
        - CHAR_LENGTH(REPLACE(LOWER(CONVERT(conflictStatement USING utf8mb4)),
                              'competing interest', ''))
      ) / CHAR_LENGTH('competing interest') >= 2
ORDER BY LENGTH(conflictStatement) DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q4. Parse-desync tells that need no DynamoDB comparison.
--
-- A statement starting lowercase or with punctuation began mid-sentence: the
-- reader lost the front of the field. A statement that is exactly a numeric
-- string is a pmid that landed in the text column -- fields shifted by one.
-- ---------------------------------------------------------------------------
-- COLLATE utf8mb4_bin is load-bearing on starts_lowercase: the table collation is
-- utf8mb4_unicode_ci, under which REGEXP '^[a-z]' matches uppercase too and every
-- healthy row is reported as a desync. Verified against a seeded fixture -- without
-- the binary collation this counted 7 of 9 rows instead of the 1 that is real.
SELECT 'Q4 desync tells' AS check_name,
       SUM(CONVERT(conflictStatement USING utf8mb4) COLLATE utf8mb4_bin REGEXP '^[a-z]') AS starts_lowercase,
       SUM(CONVERT(conflictStatement USING utf8mb4) REGEXP '^[[:punct:]]') AS starts_punctuation,
       SUM(CONVERT(conflictStatement USING utf8mb4) REGEXP '^[0-9]+$')     AS is_bare_number,
       MAX(LENGTH(conflictStatement))                                      AS max_blob_bytes,
       SUM(LENGTH(conflictStatement) >= 65535)                             AS at_blob_ceiling
FROM reporting_conflicts
WHERE LENGTH(conflictStatement) > 0;

-- ---------------------------------------------------------------------------
-- Q5. Truncation against the varchar copy.
--
-- conflictsVarchar was written as CAST(conflictStatement AS CHAR(15000)). Rows
-- where the blob exceeds that are the ones any varchar-based analysis silently
-- understates -- which is why every check above reads the blob.
-- ---------------------------------------------------------------------------
SELECT 'Q5 varchar truncation' AS check_name,
       SUM(LENGTH(conflictStatement) > 15000) AS blob_exceeds_varchar,
       SUM(conflictsVarchar IS NULL AND LENGTH(conflictStatement) > 0) AS varchar_never_populated
FROM reporting_conflicts;
