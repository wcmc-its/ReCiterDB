CREATE DEFINER=`admin`@`%` PROCEDURE `reciterdb`.`populateAnalysisSummaryTables_v2`()
proc_main: BEGIN

    -- ========================================================================
    -- DECLARATION SECTION
    -- ========================================================================
    DECLARE v_error_occurred INT DEFAULT 0;
    DECLARE v_error_message VARCHAR(1000) DEFAULT '';
    DECLARE v_start_time DATETIME;
    DECLARE v_step VARCHAR(100);
    DECLARE v_job_id VARCHAR(50);
    DECLARE v_rows INT;

    -- Error handler - captures any SQL exception
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            v_error_message = MESSAGE_TEXT;
        SET v_error_occurred = 1;
    END;

    SET v_start_time = NOW();
    SET v_job_id = CONCAT('job_', DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s'));

    -- Setup logging
    CALL setup_job_logging();

    -- ========================================================================
    -- PRE-FLIGHT CHECK
    -- ========================================================================
    SET v_step = 'Pre-flight check';
    CALL log_progress(v_job_id, v_step, 'Starting job', 'RUNNING', NULL, CONCAT('Job ID: ', v_job_id));

    -- Ensure person_article has data
    SELECT COUNT(*) INTO v_rows FROM person_article;
    CALL log_progress(v_job_id, v_step, 'Checking person_article', 'INFO', v_rows, NULL);

    IF v_rows < 5 THEN
        CALL log_progress(v_job_id, v_step, 'Validation failed', 'ERROR', v_rows, 'Insufficient data in person_article');
        LEAVE proc_main;
    END IF;

    -- ========================================================================
    -- STEP 1: CREATE STAGING TABLES (new_* tables)
    -- These are populated while production tables remain available
    -- ========================================================================
    SET v_step = '1. Create staging tables';
    CALL log_progress(v_job_id, v_step, 'Dropping old staging tables', 'RUNNING', NULL, NULL);

    -- Drop staging tables if they exist from a failed prior run
    DROP TABLE IF EXISTS analysis_summary_author_new;
    DROP TABLE IF EXISTS analysis_summary_article_new;
    DROP TABLE IF EXISTS analysis_summary_person_new;
    DROP TABLE IF EXISTS analysis_summary_author_list_new;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Drop failed', 'ERROR', NULL, v_error_message);
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Creating new staging tables', 'RUNNING', NULL, NULL);

    -- Create staging tables with same structure as production
    CREATE TABLE IF NOT EXISTS analysis_summary_author_new LIKE analysis_summary_author;
    CREATE TABLE IF NOT EXISTS analysis_summary_article_new LIKE analysis_summary_article;
    CREATE TABLE IF NOT EXISTS analysis_summary_person_new LIKE analysis_summary_person;
    CREATE TABLE IF NOT EXISTS analysis_summary_author_list_new LIKE analysis_summary_author_list;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Create failed', 'ERROR', NULL, v_error_message);
        DROP TABLE IF EXISTS analysis_summary_author_new;
        DROP TABLE IF EXISTS analysis_summary_article_new;
        DROP TABLE IF EXISTS analysis_summary_person_new;
        DROP TABLE IF EXISTS analysis_summary_author_list_new;
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 2: POPULATE analysis_summary_author_new
    -- ========================================================================
    SET v_step = '2. Populate analysis_summary_author';
    CALL log_progress(v_job_id, v_step, 'Inserting author records', 'RUNNING', NULL, NULL);

	CREATE TABLE IF NOT EXISTS analysis_summary_author_new LIKE analysis_summary_author;
    INSERT INTO analysis_summary_author_new (pmid, personIdentifier, authorPosition, authors)
    SELECT
        y.pmid,
        y.personIdentifier,
        CASE
            WHEN authors LIKE '((%' THEN 'first'
            WHEN authors LIKE '%))' THEN 'last'
        END AS authorPosition,
        CASE
            WHEN totalAuthorCount < 8 THEN authors
            ELSE CONCAT(
                SUBSTRING_INDEX(authors, ',', 6),
                ' ...',
                SUBSTRING_INDEX(authors, ',', -1)
            )
        END AS authors
    FROM (
        SELECT DISTINCT
            personIdentifier,
            pmid,
            MAX(rank) AS totalAuthorCount,
            GROUP_CONCAT(authorName ORDER BY rank ASC SEPARATOR ', ') AS authors
        FROM (
            SELECT DISTINCT
                aa.personIdentifier,
                aa.pmid,
                rank,
                CONVERT(
                    CASE
                        WHEN targetAuthor = 1 THEN CONCAT('((',authorLastName,' ',REPLACE(CAST(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') AS CHAR),' ',''),'))')
                        ELSE CONCAT(authorLastName,' ',REPLACE(CAST(REGEXP_REPLACE(BINARY authorFirstName,'[a-z]','') AS CHAR),' ',''))
                    END
                USING utf8) AS authorName
            FROM person_article_author aa
            JOIN person_article a ON a.pmid = aa.pmid AND a.personIdentifier = aa.personIdentifier
            WHERE userAssertion = 'ACCEPTED'
        ) x
        GROUP BY pmid, personIdentifier
    ) y;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Inserted author records', 'INFO', v_rows, NULL);

    -- Apply overrides (only if the override table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE()
               AND table_name = 'analysis_override_author_position') THEN
        CALL log_progress(v_job_id, v_step, 'Applying author position overrides', 'RUNNING', NULL, NULL);
        UPDATE analysis_summary_author_new a
        JOIN analysis_override_author_position o ON a.pmid = o.pmid AND a.personIdentifier = o.personIdentifier
        SET a.authorPosition = o.position;
        SET v_rows = ROW_COUNT();
        CALL log_progress(v_job_id, v_step, 'Applied overrides', 'INFO', v_rows, NULL);
    END IF;

    -- Populate authorsRTF (copy from authors, then escape special chars)
    CALL log_progress(v_job_id, v_step, 'Populating authorsRTF', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_author_new SET authorsRTF = authors WHERE authors IS NOT NULL;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Copied authors to RTF', 'INFO', v_rows, NULL);

    -- Apply RTF special character escapes for authors (only if table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_special_characters') THEN
        CALL log_progress(v_job_id, v_step, 'Escaping special chars in authorsRTF', 'RUNNING', NULL, NULL);
        BEGIN
            DECLARE v_done INT DEFAULT FALSE;
            DECLARE v_special_char VARCHAR(10);
            DECLARE v_rtf_escape VARCHAR(50);
            DECLARE cur_special CURSOR FOR SELECT specialCharacter, RTFescape FROM analysis_special_characters;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

            OPEN cur_special;
            read_loop: LOOP
                FETCH cur_special INTO v_special_char, v_rtf_escape;
                IF v_done THEN
                    LEAVE read_loop;
                END IF;
                UPDATE analysis_summary_author_new
                SET authorsRTF = REPLACE(authorsRTF, v_special_char, v_rtf_escape)
                WHERE authorsRTF LIKE CONCAT('%', v_special_char, '%');
            END LOOP;
            CLOSE cur_special;
        END;
        CALL log_progress(v_job_id, v_step, 'Escaped special chars in authorsRTF', 'INFO', NULL, NULL);
    END IF;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 2b: Handle equal contribution authors
    -- Optimized: pre-compute maxRank and equalContribAll into indexed temp tables
    -- ========================================================================
    SET v_step = '2b. Equal contribution authors';

    -- Step 2b.1: Pre-compute maxRank per pmid
    CALL log_progress(v_job_id, v_step, 'Computing maxRank per article', 'RUNNING', NULL, NULL);
    DROP TABLE IF EXISTS temp_maxrank;
    CREATE TABLE IF NOT EXISTS temp_maxrank (
        pmid INT PRIMARY KEY,
        maxRank INT
    ) ENGINE=InnoDB;
    INSERT INTO temp_maxrank SELECT pmid, MAX(rank) FROM person_article_author GROUP BY pmid;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Computed maxRank', 'INFO', v_rows, NULL);

    -- Step 2b.2: Pre-compute equalContribAll per pmid (only for pmids with equal contrib)
    CALL log_progress(v_job_id, v_step, 'Computing equalContribAll per article', 'RUNNING', NULL, NULL);
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    CREATE TABLE IF NOT EXISTS temp_equalcontrib_all (
        pmid INT PRIMARY KEY,
        equalContribAll VARCHAR(500)
    ) ENGINE=InnoDB;
    INSERT INTO temp_equalcontrib_all
    SELECT pmid, GROUP_CONCAT(DISTINCT rank ORDER BY rank ASC SEPARATOR ',')
    FROM person_article_author
    WHERE equalContrib = 'Y'
    GROUP BY pmid;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Computed equalContribAll', 'INFO', v_rows, NULL);

    -- Step 2b.3: Build the equal contrib result table
    CALL log_progress(v_job_id, v_step, 'Building equal contrib positions', 'RUNNING', NULL, NULL);
    DROP TABLE IF EXISTS analysis_temp_equalcontrib_v2;
    CREATE TABLE IF NOT EXISTS analysis_temp_equalcontrib_v2 (
        id INT AUTO_INCREMENT PRIMARY KEY,
        personIdentifier VARCHAR(30),
        pmid INT,
        authorPositionEqualContrib VARCHAR(20),
        KEY idx_person_pmid (personIdentifier, pmid)
    ) ENGINE=InnoDB;

    INSERT INTO analysis_temp_equalcontrib_v2 (personIdentifier, pmid, authorPositionEqualContrib)
    SELECT a.personIdentifier, a.pmid,
           CASE
               -- First author contiguous from rank 1
               WHEN a.rank = 2 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 3 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 4 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 AND FIND_IN_SET(4, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 5 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 AND FIND_IN_SET(4, ec.equalContribAll) > 0 AND FIND_IN_SET(5, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 6 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 AND FIND_IN_SET(4, ec.equalContribAll) > 0 AND FIND_IN_SET(5, ec.equalContribAll) > 0 AND FIND_IN_SET(6, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 7 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 AND FIND_IN_SET(4, ec.equalContribAll) > 0 AND FIND_IN_SET(5, ec.equalContribAll) > 0 AND FIND_IN_SET(6, ec.equalContribAll) > 0 AND FIND_IN_SET(7, ec.equalContribAll) > 0 THEN 'first'
               WHEN a.rank = 8 AND FIND_IN_SET(1, ec.equalContribAll) > 0 AND FIND_IN_SET(2, ec.equalContribAll) > 0 AND FIND_IN_SET(3, ec.equalContribAll) > 0 AND FIND_IN_SET(4, ec.equalContribAll) > 0 AND FIND_IN_SET(5, ec.equalContribAll) > 0 AND FIND_IN_SET(6, ec.equalContribAll) > 0 AND FIND_IN_SET(7, ec.equalContribAll) > 0 AND FIND_IN_SET(8, ec.equalContribAll) > 0 THEN 'first'
               -- Last author contiguous from maxRank
               WHEN a.rank = m.maxRank - 1 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 2 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 3 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 4 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 4, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 5 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 4, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 5, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 6 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 4, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 5, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 6, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 7 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 4, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 5, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 6, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 7, ec.equalContribAll) > 0 THEN 'last'
               WHEN a.rank = m.maxRank - 8 AND FIND_IN_SET(m.maxRank, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 1, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 2, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 3, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 4, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 5, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 6, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 7, ec.equalContribAll) > 0 AND FIND_IN_SET(m.maxRank - 8, ec.equalContribAll) > 0 THEN 'last'
               ELSE NULL
           END AS authorPositionEqualContrib
    FROM person_article_author a
    INNER JOIN person_article p ON p.pmid = a.pmid AND p.personIdentifier = a.personIdentifier
    INNER JOIN temp_maxrank m ON m.pmid = a.pmid
    INNER JOIN temp_equalcontrib_all ec ON ec.pmid = a.pmid
    WHERE a.equalContrib = 'Y'
      AND a.targetAuthor = 1
      AND p.userAssertion = 'ACCEPTED';

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Built equal contrib positions', 'INFO', v_rows, NULL);

    -- Step 2b.4: Apply to main table
    CALL log_progress(v_job_id, v_step, 'Applying equal contrib positions', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_author_new y
    JOIN analysis_temp_equalcontrib_v2 x ON x.pmid = y.pmid AND x.personIdentifier = y.personIdentifier
    SET y.authorPosition = x.authorPositionEqualContrib
    WHERE x.authorPositionEqualContrib IS NOT NULL AND y.authorPosition IS NULL;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Applied equal contrib positions', 'INFO', v_rows, NULL);

    -- Cleanup temp tables
    DROP TABLE IF EXISTS temp_maxrank;
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    DROP TABLE IF EXISTS analysis_temp_equalcontrib_v2;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 2c: Populate analysis_summary_author_list_new
    -- ========================================================================
    SET v_step = '2c. Populate author_list';
    CALL log_progress(v_job_id, v_step, 'Inserting author list records', 'RUNNING', NULL, NULL);

	CREATE TABLE IF NOT EXISTS analysis_summary_author_list_new LIKE analysis_summary_author_list;
    INSERT INTO analysis_summary_author_list_new (pmid, authorFirstName, authorLastName, rank, personIdentifier)
    SELECT
        pmid,
        MAX(authorFirstName) AS authorFirstName,
        MAX(authorLastName) AS authorLastName,
        rank,
        MAX(personIdentifier) AS personIdentifier
    FROM (
        SELECT
            aa.personIdentifier,
            aa.pmid,
            authorFirstName,
            authorLastName,
            rank,
            targetAuthor
        FROM person_article_author aa
        JOIN person_article a ON a.pmid = aa.pmid AND a.personIdentifier = aa.personIdentifier
        WHERE userAssertion = 'ACCEPTED'
          AND targetAuthor = 1
        UNION
        SELECT
            '' AS personIdentifier,
            pmid,
            authorFirstName,
            authorLastName,
            rank,
            targetAuthor
        FROM person_article_author
        WHERE targetAuthor = 0
    ) x
    GROUP BY pmid, rank
    ORDER BY pmid DESC, rank ASC;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Inserted author list records', 'INFO', v_rows, NULL);

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 3: Populate analysis_summary_article_new
    -- ========================================================================
    SET v_step = '3. Populate analysis_summary_article';
    CALL log_progress(v_job_id, v_step, 'Inserting article records', 'RUNNING', NULL, NULL);
	
    CREATE TABLE IF NOT EXISTS analysis_summary_article_new LIKE analysis_summary_article;
    INSERT INTO analysis_summary_article_new (
        pmid, pmcid, publicationTypeCanonical, articleYear,
        publicationDateStandardized, publicationDateDisplay,
        datePublicationAddedToEntrez, articleTitle, journalTitleVerbose,
        issn, doi, issue, volume, pages, citationCountScopus
    )
    SELECT DISTINCT
        pmid,
        MAX(pmcid),
        publicationTypeCanonical,
        articleYear,
        MIN(publicationDateStandardized),
        publicationDateDisplay,
        datePublicationAddedToEntrez,
        articleTitle,
        journalTitleVerbose,
        issn,
        doi,
        issue,
        volume,
        pages,
        MAX(timesCited)
    FROM person_article
    WHERE userAssertion = 'ACCEPTED'
    GROUP BY pmid
    ORDER BY datePublicationAddedToEntrez DESC;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Inserted article records', 'INFO', v_rows, NULL);

    -- Update with journal impact scores (only if table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'journal_impact_scimago') THEN
        CALL log_progress(v_job_id, v_step, 'Updating journal impact scores (Scimago)', 'RUNNING', NULL, NULL);
        UPDATE analysis_summary_article_new a
        JOIN journal_impact_scimago i ON i.issn1 = a.issn
        SET journalImpactScore1 = i.sjr
        WHERE a.journalImpactScore1 IS NULL AND a.issn IS NOT NULL;

        UPDATE analysis_summary_article_new a
        JOIN journal_impact_scimago i ON i.issn2 = a.issn
        SET journalImpactScore1 = i.sjr
        WHERE a.journalImpactScore1 IS NULL AND a.issn IS NOT NULL;

        UPDATE analysis_summary_article_new a
        JOIN journal_impact_scimago i ON i.issn3 = a.issn
        SET journalImpactScore1 = i.sjr
        WHERE a.journalImpactScore1 IS NULL AND a.issn IS NOT NULL;
    ELSE
        CALL log_progress(v_job_id, v_step, 'Skipping Scimago (table not found)', 'INFO', 0, NULL);
    END IF;

    -- Update with alternative journal impact scores (only if table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'journal_impact_alternative') THEN
        UPDATE analysis_summary_article_new a
        JOIN journal_impact_alternative i ON i.issn = a.issn
        SET journalImpactScore2 = i.impactScore1
        WHERE a.journalImpactScore2 IS NULL AND a.issn IS NOT NULL;

        UPDATE analysis_summary_article_new a
        JOIN journal_impact_alternative i ON i.eissn = a.issn
        SET journalImpactScore2 = i.impactScore1
        WHERE a.journalImpactScore2 IS NULL AND a.issn IS NOT NULL;
    END IF;

    -- Update NIH citation data (only if table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_nih') THEN
        CALL log_progress(v_job_id, v_step, 'Updating NIH citation data', 'RUNNING', NULL, NULL);
        UPDATE analysis_summary_article_new a
        JOIN analysis_nih n ON n.pmid = a.pmid
        SET a.citationCountNIH = n.citation_count,
            a.percentileNIH = n.nih_percentile,
            a.relativeCitationRatioNIH = n.relative_citation_ratio,
            a.publicationTypeNIH = CASE WHEN n.is_research_article = 'True' THEN 'Research Article' ELSE NULL END;
        SET v_rows = ROW_COUNT();
        CALL log_progress(v_job_id, v_step, 'Updated NIH citation data', 'INFO', v_rows, NULL);
    ELSE
        CALL log_progress(v_job_id, v_step, 'Skipping NIH data (table not found)', 'INFO', 0, NULL);
    END IF;

    -- Update Mendeley readers (only if analysis_altmetric table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_altmetric') THEN
        CALL log_progress(v_job_id, v_step, 'Updating Mendeley readers', 'RUNNING', NULL, NULL);
        UPDATE analysis_summary_article_new a
        JOIN analysis_altmetric al ON al.doi = a.doi
        SET a.readersMendeley = al.`readers-mendeley`
        WHERE ROUND((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24), 0) < 366;
        SET v_rows = ROW_COUNT();
        CALL log_progress(v_job_id, v_step, 'Updated Mendeley readers', 'INFO', v_rows, NULL);
    ELSE
        CALL log_progress(v_job_id, v_step, 'Skipping Mendeley (table not found)', 'INFO', 0, NULL);
    END IF;

    -- Populate articleTitleRTF (copy from articleTitle, then escape special chars)
    CALL log_progress(v_job_id, v_step, 'Populating articleTitleRTF', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_article_new SET articleTitleRTF = articleTitle WHERE articleTitle IS NOT NULL;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Copied articleTitle to RTF', 'INFO', v_rows, NULL);

    -- Apply RTF special character escapes (only if table exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_special_characters') THEN
        CALL log_progress(v_job_id, v_step, 'Escaping special chars in articleTitleRTF', 'RUNNING', NULL, NULL);
        BEGIN
            DECLARE v_done INT DEFAULT FALSE;
            DECLARE v_special_char VARCHAR(10);
            DECLARE v_rtf_escape VARCHAR(50);
            DECLARE cur_special CURSOR FOR SELECT specialCharacter, RTFescape FROM analysis_special_characters;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

            OPEN cur_special;
            read_loop: LOOP
                FETCH cur_special INTO v_special_char, v_rtf_escape;
                IF v_done THEN
                    LEAVE read_loop;
                END IF;
                UPDATE analysis_summary_article_new
                SET articleTitleRTF = REPLACE(articleTitleRTF, v_special_char, v_rtf_escape)
                WHERE articleTitleRTF LIKE CONCAT('%', v_special_char, '%');
            END LOOP;
            CLOSE cur_special;
        END;
        CALL log_progress(v_job_id, v_step, 'Escaped special chars in articleTitleRTF', 'INFO', NULL, NULL);
    END IF;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 4: Populate analysis_summary_person_new
    -- ========================================================================
    SET v_step = '4. Populate analysis_summary_person';
    CALL log_progress(v_job_id, v_step, 'Inserting person records', 'RUNNING', NULL, NULL);

	 CREATE TABLE IF NOT EXISTS analysis_summary_person_new LIKE analysis_summary_person;
    INSERT INTO analysis_summary_person_new (personIdentifier, nameFirst, nameMiddle, nameLast, facultyRank, department)
    SELECT DISTINCT
        p.personIdentifier,
        p.firstName,
        p.middleName,
        p.lastName,
        p.title,
        p.primaryOrganizationalUnit
    FROM person p
    JOIN analysis_summary_person_scope s ON s.personIdentifier = p.personIdentifier;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Inserted person records', 'INFO', v_rows, NULL);

    -- Update article counts
    CALL log_progress(v_job_id, v_step, 'Updating article counts', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, COUNT(DISTINCT pmid) AS cnt
        FROM analysis_summary_author_new
        GROUP BY personIdentifier
    ) c ON c.personIdentifier = p.personIdentifier
    SET p.countAll = c.cnt;

    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, COUNT(DISTINCT pmid) AS cnt
        FROM analysis_summary_author_new
        WHERE authorPosition = 'first'
        GROUP BY personIdentifier
    ) c ON c.personIdentifier = p.personIdentifier
    SET p.countFirst = c.cnt;

    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, COUNT(DISTINCT pmid) AS cnt
        FROM analysis_summary_author_new
        WHERE authorPosition = 'last'
        GROUP BY personIdentifier
    ) c ON c.personIdentifier = p.personIdentifier
    SET p.countSenior = c.cnt;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 5: Compute percentile rankings (with rank and denominator)
    -- ========================================================================
    SET v_step = '5. Compute percentile rankings';
    CALL log_progress(v_job_id, v_step, 'Computing percentiles (8 metrics with rank/denominator)', 'RUNNING', NULL, NULL);

    -- Top 5 percentile, first/last authored
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition IN ('first', 'last')
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileFirstSenior = x.pct,
        p.top5RankFirstSenior = x.rank_count,
        p.top5DenominatorFirstSenior = x.denominator;

    -- Top 10 percentile, first/last authored
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition IN ('first', 'last')
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileFirstSenior = x.pct,
        p.top10RankFirstSenior = x.rank_count,
        p.top10DenominatorFirstSenior = x.denominator;

    -- Top 5 percentile, first authored only
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition = 'first'
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileFirst = x.pct,
        p.top5RankFirst = x.rank_count,
        p.top5DenominatorFirst = x.denominator;

    -- Top 10 percentile, first authored only
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition = 'first'
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileFirst = x.pct,
        p.top10RankFirst = x.rank_count,
        p.top10DenominatorFirst = x.denominator;

    -- Top 5 percentile, last authored only
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition = 'last'
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileSenior = x.pct,
        p.top5RankSenior = x.rank_count,
        p.top5DenominatorSenior = x.denominator;

    -- Top 10 percentile, last authored only
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
          AND authorPosition = 'last'
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileSenior = x.pct,
        p.top10RankSenior = x.rank_count,
        p.top10DenominatorSenior = x.denominator;

    -- Top 5 percentile, all positions
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 95 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileAll = x.pct,
        p.top5RankAll = x.rank_count,
        p.top5DenominatorAll = x.denominator;

    -- Top 10 percentile, all positions
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            a.personIdentifier,
            ROUND(100 * SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct,
            SUM(CASE WHEN percentileNIH >= 90 THEN 1 ELSE 0 END) AS rank_count,
            COUNT(*) AS denominator
        FROM analysis_summary_author_new a
        JOIN analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE percentileNIH IS NOT NULL
        GROUP BY a.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileAll = x.pct,
        p.top10RankAll = x.rank_count,
        p.top10DenominatorAll = x.denominator;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 6: COMPUTE H-INDEX USING EFFICIENT SET-BASED QUERY
    -- This replaces the slow row-by-row REPEAT loop
    -- ========================================================================
    SET v_step = '6. Compute h-index (set-based)';
    CALL log_progress(v_job_id, v_step, 'Computing h-index NIH (all time)', 'RUNNING', NULL, NULL);

    -- H-index using NIH citation data (all time)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            personIdentifier,
            MAX(h_index) AS h_index
        FROM (
            SELECT
                personIdentifier,
                rn AS h_index
            FROM (
                SELECT
                    a.personIdentifier,
                    r.pmid,
                    r.citationCountNIH,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.personIdentifier
                        ORDER BY r.citationCountNIH DESC
                    ) AS rn
                FROM analysis_summary_author_new a
                JOIN analysis_summary_article_new r ON r.pmid = a.pmid
                WHERE r.citationCountNIH > 0
            ) AS ranked
            WHERE citationCountNIH >= rn
        ) AS h_calc
        GROUP BY personIdentifier
    ) h ON h.personIdentifier = p.personIdentifier
    SET p.hindexNIH = h.h_index;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated h-index NIH', 'INFO', v_rows, NULL);

    -- H5-index using NIH citation data (last 5 years)
    CALL log_progress(v_job_id, v_step, 'Computing h5-index NIH (last 5 years)', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            personIdentifier,
            MAX(h_index) AS h_index
        FROM (
            SELECT
                personIdentifier,
                rn AS h_index
            FROM (
                SELECT
                    a.personIdentifier,
                    r.pmid,
                    r.citationCountNIH,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.personIdentifier
                        ORDER BY r.citationCountNIH DESC
                    ) AS rn
                FROM analysis_summary_author_new a
                JOIN analysis_summary_article_new r ON r.pmid = a.pmid
                WHERE r.citationCountNIH > 0
                  AND r.datePublicationAddedToEntrez > DATE_SUB(CURDATE(), INTERVAL 5 YEAR)
            ) AS ranked
            WHERE citationCountNIH >= rn
        ) AS h_calc
        GROUP BY personIdentifier
    ) h ON h.personIdentifier = p.personIdentifier
    SET p.h5indexNIH = h.h_index;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated h5-index NIH', 'INFO', v_rows, NULL);

    -- H-index using Scopus citation data (all time)
    CALL log_progress(v_job_id, v_step, 'Computing h-index Scopus (all time)', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            personIdentifier,
            MAX(h_index) AS h_index
        FROM (
            SELECT
                personIdentifier,
                rn AS h_index
            FROM (
                SELECT
                    a.personIdentifier,
                    r.pmid,
                    r.citationCountScopus,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.personIdentifier
                        ORDER BY r.citationCountScopus DESC
                    ) AS rn
                FROM analysis_summary_author_new a
                JOIN analysis_summary_article_new r ON r.pmid = a.pmid
                WHERE r.citationCountScopus > 0
            ) AS ranked
            WHERE citationCountScopus >= rn
        ) AS h_calc
        GROUP BY personIdentifier
    ) h ON h.personIdentifier = p.personIdentifier
    SET p.hindexScopus = h.h_index;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated h-index Scopus', 'INFO', v_rows, NULL);

    -- H5-index using Scopus citation data (last 5 years)
    CALL log_progress(v_job_id, v_step, 'Computing h5-index Scopus (last 5 years)', 'RUNNING', NULL, NULL);
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT
            personIdentifier,
            MAX(h_index) AS h_index
        FROM (
            SELECT
                personIdentifier,
                rn AS h_index
            FROM (
                SELECT
                    a.personIdentifier,
                    r.pmid,
                    r.citationCountScopus,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.personIdentifier
                        ORDER BY r.citationCountScopus DESC
                    ) AS rn
                FROM analysis_summary_author_new a
                JOIN analysis_summary_article_new r ON r.pmid = a.pmid
                WHERE r.citationCountScopus > 0
                  AND r.datePublicationAddedToEntrez > DATE_SUB(CURDATE(), INTERVAL 5 YEAR)
            ) AS ranked
            WHERE citationCountScopus >= rn
        ) AS h_calc
        GROUP BY personIdentifier
    ) h ON h.personIdentifier = p.personIdentifier
    SET p.h5indexScopus = h.h_index;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated h5-index Scopus', 'INFO', v_rows, NULL);

    -- Set hindexStatus = 1 for all (completed)
    UPDATE analysis_summary_person_new SET hindexStatus = 1;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 7: ATOMIC TABLE SWAP
    -- This is the key improvement - production tables are only unavailable
    -- for the brief moment of the RENAME operation
    -- ========================================================================
    SET v_step = '7. Atomic table swap';
    CALL log_progress(v_job_id, v_step, 'Dropping old backup tables', 'RUNNING', NULL, NULL);

    -- Drop old backup tables if they exist
    DROP TABLE IF EXISTS analysis_summary_author_backup;
    DROP TABLE IF EXISTS analysis_summary_article_backup;
    DROP TABLE IF EXISTS analysis_summary_person_backup;
    DROP TABLE IF EXISTS analysis_summary_author_list_backup;

    CALL log_progress(v_job_id, v_step, 'Performing atomic RENAME', 'RUNNING', NULL, 'This swaps all 4 tables instantly');

    -- Atomic swap: current -> backup, new -> current
    -- This is a single atomic operation in MySQL/MariaDB
    RENAME TABLE
        analysis_summary_author TO analysis_summary_author_backup,
        analysis_summary_author_new TO analysis_summary_author,
        analysis_summary_article TO analysis_summary_article_backup,
        analysis_summary_article_new TO analysis_summary_article,
        analysis_summary_person TO analysis_summary_person_backup,
        analysis_summary_person_new TO analysis_summary_person,
        analysis_summary_author_list TO analysis_summary_author_list_backup,
        analysis_summary_author_list_new TO analysis_summary_author_list;

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'RENAME failed - CRITICAL', 'ERROR', NULL, v_error_message);
        CALL log_progress(v_job_id, v_step, 'Attempting restore', 'RUNNING', NULL, NULL);
        -- Try to restore if rename failed partway
        CALL restore_from_backup_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, 'Tables swapped successfully');

    -- ========================================================================
    -- SUCCESS
    -- ========================================================================
    CALL log_progress(v_job_id, 'FINISHED', 'Job completed successfully', 'SUCCESS', NULL,
                      CONCAT('Total time: ', TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's'));

    SELECT CONCAT('SUCCESS: populateAnalysisSummaryTables_v2 completed in ',
                  TIMESTAMPDIFF(SECOND, v_start_time, NOW()), ' seconds') AS status;

END proc_main