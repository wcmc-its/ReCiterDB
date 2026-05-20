-- =============================================================================
-- Migration: UNIQUE KEY on reporting_abstracts.pmid (v1.4)
-- =============================================================================
-- Replaces the existing non-unique `idx_pmid` index on reporting_abstracts
-- with a UNIQUE KEY so the parser-desync class of failure that corrupted
-- ~3,100 rows historically (issue #87, pre-PR #78 CSV / LOAD DATA path) can
-- no longer silently produce duplicate-pmid rows.
--
-- WHY THIS IS NEEDED:
--   update/abstractImport.py's fetch_missing_pmids() uses
--     LEFT JOIN reporting_abstracts a ON a.pmid = p.pmid WHERE a.pmid IS NULL
--   so the import path *assumes* one-row-per-pmid. The schema never
--   enforced it. This migration codifies the assumption, mirroring the
--   analysis_nih fix from March (PR #71/#72 after the Dec 2025 duplicate
--   loading incident).
--
-- PRECONDITION:
--   reporting_abstracts must contain zero duplicate pmids. The
--   information_schema-guarded block at the top aborts the migration with a
--   readable error if duplicates remain (run update/repairAbstracts.py
--   first; it warns when duplicates are present).
--
-- Safe to run on prod and dev. Idempotent (information_schema guard;
-- re-runs are no-ops once the UNIQUE KEY exists). No AFTER clause; the
-- ALTER converts the existing BTREE index in place.
-- =============================================================================

SET @db = DATABASE();

-- -----------------------------------------------------------------------------
-- Precondition: no duplicate pmids.
-- -----------------------------------------------------------------------------

SET @dup_count = (
    SELECT COUNT(*) FROM (
        SELECT pmid FROM reporting_abstracts
        GROUP BY pmid HAVING COUNT(*) > 1
    ) d
);

SET @sql = IF(
    @dup_count > 0,
    CONCAT(
        'SELECT ',
        '''Migration aborted: reporting_abstracts has ',
        @dup_count,
        ' duplicate pmid value(s). Run update/repairAbstracts.py and resolve ',
        'duplicates before re-running this migration.'' AS error, ',
        '1/0 AS force_error'
    ),
    'SELECT ''No duplicate pmids; precondition satisfied.'' AS status'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- reporting_abstracts.idx_pmid: KEY -> UNIQUE KEY
-- -----------------------------------------------------------------------------

SET @already_unique = (
    SELECT COUNT(*) FROM information_schema.statistics
    WHERE table_schema = @db
      AND table_name = 'reporting_abstracts'
      AND index_name = 'idx_pmid'
      AND non_unique = 0
);

SET @sql = IF(
    @already_unique > 0,
    'SELECT ''reporting_abstracts.idx_pmid is already UNIQUE; no-op.''',
    'ALTER TABLE reporting_abstracts
       DROP INDEX idx_pmid,
       ADD UNIQUE KEY idx_pmid (pmid) USING BTREE'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- -----------------------------------------------------------------------------
-- Verification
-- -----------------------------------------------------------------------------

SELECT table_name, index_name, non_unique, column_name, index_type
FROM information_schema.statistics
WHERE table_schema = DATABASE()
  AND table_name = 'reporting_abstracts'
  AND index_name = 'idx_pmid';
