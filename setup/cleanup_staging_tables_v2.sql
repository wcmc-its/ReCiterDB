CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`cleanup_staging_tables_v2`()
BEGIN
    DROP TABLE IF EXISTS analysis_summary_author_new;
    DROP TABLE IF EXISTS analysis_summary_article_new;
    DROP TABLE IF EXISTS analysis_summary_person_new;
    DROP TABLE IF EXISTS analysis_summary_author_list_new;
    DROP TABLE IF EXISTS temp_maxrank;
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    DROP TABLE IF EXISTS analysis_temp_equalcontrib_v2;
    -- >>> R2 (v3.11, PM #1105 5h): bibliometric v2 staging and temp tables
    DROP TABLE IF EXISTS analysis_summary_person_peer_new;
    DROP TABLE IF EXISTS temp_v2_sole_pmid;
    DROP TABLE IF EXISTS temp_v2_rank;
    DROP TABLE IF EXISTS temp_v2_works;
    DROP TABLE IF EXISTS temp_v2_roleset;
    DROP TABLE IF EXISTS temp_v2_cohort;
    -- <<< R2 (v3.11)
    SELECT 'Staging tables cleaned up' AS status;
END