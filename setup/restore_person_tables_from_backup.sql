-- Rollback helper for the nightly loader's atomic table swap (ATOMIC_SWAP=1).
-- Mirrors restore_from_backup_v2 (setup/restore_from_backup_v2.sql) but for the
-- 10 person_* tables promoted by swap_new_tables_into_place() in update/updateReciterDB.py.
-- The `_backup` tables persist until the NEXT run's pre-swap drop, so this can restore the
-- previous run's data any time before then.
--
-- Apply with:  mysql --host=... --user=... --password=... reciterdb < this_file.sql
-- Invoke with: CALL restore_person_tables_from_backup();

DELIMITER //

DROP PROCEDURE IF EXISTS `reciterdb`.`restore_person_tables_from_backup`//

CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`restore_person_tables_from_backup`()
BEGIN
    DECLARE v_error INT DEFAULT 0;
    DECLARE v_backup_count INT DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;

    -- Require ALL 10 backup tables before touching anything. The sibling procedure
    -- restore_from_backup_v2 guards on a single table; that is unsafe here because the
    -- DROPs below are unconditional -- a partial backup set would destroy every live
    -- table and then fail the RENAME, leaving nothing to restore.
    SELECT COUNT(*) INTO v_backup_count
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name IN (
        'person_backup','person_article_backup','person_article_author_backup',
        'person_article_department_backup','person_article_grant_backup',
        'person_article_keyword_backup','person_article_relationship_backup',
        'person_article_scopus_target_author_affiliation_backup',
        'person_article_scopus_non_target_author_affiliation_backup',
        'person_person_type_backup');

    IF v_backup_count = 10 THEN

        -- Drop current tables and rename backup to current
        DROP TABLE IF EXISTS person;
        DROP TABLE IF EXISTS person_article;
        DROP TABLE IF EXISTS person_article_author;
        DROP TABLE IF EXISTS person_article_department;
        DROP TABLE IF EXISTS person_article_grant;
        DROP TABLE IF EXISTS person_article_keyword;
        DROP TABLE IF EXISTS person_article_relationship;
        DROP TABLE IF EXISTS person_article_scopus_target_author_affiliation;
        DROP TABLE IF EXISTS person_article_scopus_non_target_author_affiliation;
        DROP TABLE IF EXISTS person_person_type;

        RENAME TABLE
            person_backup TO person,
            person_article_backup TO person_article,
            person_article_author_backup TO person_article_author,
            person_article_department_backup TO person_article_department,
            person_article_grant_backup TO person_article_grant,
            person_article_keyword_backup TO person_article_keyword,
            person_article_relationship_backup TO person_article_relationship,
            person_article_scopus_target_author_affiliation_backup TO person_article_scopus_target_author_affiliation,
            person_article_scopus_non_target_author_affiliation_backup TO person_article_scopus_non_target_author_affiliation,
            person_person_type_backup TO person_person_type;

        IF v_error = 0 THEN
            SELECT 'SUCCESS: Restored from backup tables' AS status;
        ELSE
            SELECT 'ERROR: Failed to restore from backup' AS status;
        END IF;
    ELSE
        SELECT CONCAT('ERROR: expected 10 backup tables, found ', v_backup_count,
                      ' -- refusing to drop live tables') AS status;
    END IF;
END//

DELIMITER ;
