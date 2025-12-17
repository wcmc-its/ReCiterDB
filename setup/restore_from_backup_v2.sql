CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`restore_from_backup_v2`()
BEGIN
    DECLARE v_error INT DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET v_error = 1;

    -- Check if backup tables exist
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'analysis_summary_author_backup' AND table_schema = DATABASE()) THEN

        -- Drop current tables and rename backup to current
        DROP TABLE IF EXISTS analysis_summary_author;
        DROP TABLE IF EXISTS analysis_summary_article;
        DROP TABLE IF EXISTS analysis_summary_person;
        DROP TABLE IF EXISTS analysis_summary_author_list;

        RENAME TABLE
            analysis_summary_author_backup TO analysis_summary_author,
            analysis_summary_article_backup TO analysis_summary_article,
            analysis_summary_person_backup TO analysis_summary_person,
            analysis_summary_author_list_backup TO analysis_summary_author_list;

        IF v_error = 0 THEN
            SELECT 'SUCCESS: Restored from backup tables' AS status;
        ELSE
            SELECT 'ERROR: Failed to restore from backup' AS status;
        END IF;
    ELSE
        SELECT 'ERROR: Backup tables do not exist' AS status;
    END IF;
END