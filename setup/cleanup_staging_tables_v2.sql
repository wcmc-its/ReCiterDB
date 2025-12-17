CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`cleanup_staging_tables_v2`()
BEGIN
    DROP TABLE IF EXISTS analysis_summary_author_new;
    DROP TABLE IF EXISTS analysis_summary_article_new;
    DROP TABLE IF EXISTS analysis_summary_person_new;
    DROP TABLE IF EXISTS analysis_summary_author_list_new;
    DROP TABLE IF EXISTS temp_maxrank;
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    DROP TABLE IF EXISTS analysis_temp_equalcontrib_v2;
    SELECT 'Staging tables cleaned up' AS status;
END