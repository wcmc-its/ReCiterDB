-- Table-lifecycle control for the nightly loader's shadow-build / atomic-swap
-- pattern, moved out of update/updateReciterDB.py (previously gated behind an
-- ATOMIC_SWAP env var) and into stored procedures, matching how this repo
-- already manages the equivalent lifecycle for the analysis_summary_* tables
-- (setup/populateAnalysisSummaryTables_v2.sql, "1. Create staging tables" and
-- "7. Atomic table swap") and the rollback companion for these same 10 tables
-- (setup/restore_person_tables_from_backup.sql).
--
-- Two procedures, called once per nightly run in this order:
--   1. prepare_person_shadow_tables() -- at the start of the run, before any
--      CSV loads. Creates `<table>_new` for each of the 10 person_* tables.
--   2. swap_person_tables()           -- at the end of the run, after the
--      final identity merge. Promotes every `_new` shadow table into its
--      live name with a single atomic RENAME, moving the current live table
--      to `_backup` in the same statement.
--
-- The `_backup` tables produced by swap_person_tables() are NOT dropped by
-- that procedure -- they are the rollback window consumed by
-- restore_person_tables_from_backup() and are only cleared by the next run's
-- call to swap_person_tables() (which drops the prior run's `_backup` set
-- before renaming). person_temp is internal staging, always truncated and
-- loaded in place, and is deliberately excluded from both procedures here.
--
-- Apply with:  mysql --host=... --user=... --password=... reciterdb < this_file.sql
-- Invoke with: CALL prepare_person_shadow_tables();
--              CALL swap_person_tables();

DELIMITER //

DROP PROCEDURE IF EXISTS `reciterdb`.`prepare_person_shadow_tables`//

CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`prepare_person_shadow_tables`()
BEGIN
    DECLARE v_error INT DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;

    -- Drop any `_new` left by a prior crashed run before recreating it --
    -- CREATE ... IF NOT EXISTS would silently reuse that leftover table,
    -- which may carry a stale schema if a migration has since altered the
    -- live table. Drop-then-CREATE ... LIKE guarantees the shadow table
    -- always matches the live schema exactly.
    DROP TABLE IF EXISTS person_new;
    CREATE TABLE person_new LIKE person;

    DROP TABLE IF EXISTS person_article_new;
    CREATE TABLE person_article_new LIKE person_article;

    DROP TABLE IF EXISTS person_article_author_new;
    CREATE TABLE person_article_author_new LIKE person_article_author;

    DROP TABLE IF EXISTS person_article_department_new;
    CREATE TABLE person_article_department_new LIKE person_article_department;

    DROP TABLE IF EXISTS person_article_grant_new;
    CREATE TABLE person_article_grant_new LIKE person_article_grant;

    DROP TABLE IF EXISTS person_article_keyword_new;
    CREATE TABLE person_article_keyword_new LIKE person_article_keyword;

    DROP TABLE IF EXISTS person_article_relationship_new;
    CREATE TABLE person_article_relationship_new LIKE person_article_relationship;

    DROP TABLE IF EXISTS person_article_scopus_target_author_affiliation_new;
    CREATE TABLE person_article_scopus_target_author_affiliation_new LIKE person_article_scopus_target_author_affiliation;

    DROP TABLE IF EXISTS person_article_scopus_non_target_author_affiliation_new;
    CREATE TABLE person_article_scopus_non_target_author_affiliation_new LIKE person_article_scopus_non_target_author_affiliation;

    DROP TABLE IF EXISTS person_person_type_new;
    CREATE TABLE person_person_type_new LIKE person_person_type;

    IF v_error = 0 THEN
        SELECT 'SUCCESS: Prepared 10 person shadow tables' AS status;
    ELSE
        SELECT 'ERROR: Failed to prepare person shadow tables' AS status;
    END IF;
END//

DROP PROCEDURE IF EXISTS `reciterdb`.`swap_person_tables`//

CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`swap_person_tables`()
BEGIN
    DECLARE v_error INT DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;

    -- Drop stale backup tables left by the prior run. This is what clears
    -- the rollback window that restore_person_tables_from_backup() relies
    -- on -- once this drop runs, the previous run's backups are gone.
    DROP TABLE IF EXISTS person_backup;
    DROP TABLE IF EXISTS person_article_backup;
    DROP TABLE IF EXISTS person_article_author_backup;
    DROP TABLE IF EXISTS person_article_department_backup;
    DROP TABLE IF EXISTS person_article_grant_backup;
    DROP TABLE IF EXISTS person_article_keyword_backup;
    DROP TABLE IF EXISTS person_article_relationship_backup;
    DROP TABLE IF EXISTS person_article_scopus_target_author_affiliation_backup;
    DROP TABLE IF EXISTS person_article_scopus_non_target_author_affiliation_backup;
    DROP TABLE IF EXISTS person_person_type_backup;

    -- Atomic swap: current -> backup, new -> current. MySQL/MariaDB executes
    -- a multi-table RENAME TABLE as a single atomic operation -- either every
    -- listed rename takes effect or none do, so a crash or error here leaves
    -- every live table exactly as it was before this call. The `_backup`
    -- tables this produces are intentionally left in place -- they are the
    -- rollback window consumed by restore_person_tables_from_backup() and are
    -- only cleared by the DROPs above on the next call to this procedure.
    RENAME TABLE
        person TO person_backup,
        person_new TO person,
        person_article TO person_article_backup,
        person_article_new TO person_article,
        person_article_author TO person_article_author_backup,
        person_article_author_new TO person_article_author,
        person_article_department TO person_article_department_backup,
        person_article_department_new TO person_article_department,
        person_article_grant TO person_article_grant_backup,
        person_article_grant_new TO person_article_grant,
        person_article_keyword TO person_article_keyword_backup,
        person_article_keyword_new TO person_article_keyword,
        person_article_relationship TO person_article_relationship_backup,
        person_article_relationship_new TO person_article_relationship,
        person_article_scopus_target_author_affiliation TO person_article_scopus_target_author_affiliation_backup,
        person_article_scopus_target_author_affiliation_new TO person_article_scopus_target_author_affiliation,
        person_article_scopus_non_target_author_affiliation TO person_article_scopus_non_target_author_affiliation_backup,
        person_article_scopus_non_target_author_affiliation_new TO person_article_scopus_non_target_author_affiliation,
        person_person_type TO person_person_type_backup,
        person_person_type_new TO person_person_type;

    IF v_error = 0 THEN
        SELECT 'SUCCESS: Swapped 10 person tables into place' AS status;
    ELSE
        SELECT 'ERROR: Failed to swap person tables into place' AS status;
    END IF;
END//

DELIMITER ;
