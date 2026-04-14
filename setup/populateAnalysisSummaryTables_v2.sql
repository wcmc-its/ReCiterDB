-- ============================================================================
-- ReCiter Analysis Summary Tables - Improved Nightly Job
-- Version 2.1
--
-- Key improvements:
-- 1. Atomic table swap pattern - no downtime during updates
-- 2. Set-based h-index computation - much faster than row-by-row
-- 3. Transaction control with rollback capability
-- 4. Backup tables preserved for recovery
-- 5. Granular progress logging via analysis_job_log table
-- ============================================================================

DELIMITER //

-- Create job logging table if it doesn't exist
DROP PROCEDURE IF EXISTS `setup_job_logging`//
CREATE PROCEDURE `setup_job_logging`()
BEGIN
    CREATE TABLE IF NOT EXISTS `analysis_job_log` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `job_id` VARCHAR(50),
        `step` VARCHAR(100),
        `substep` VARCHAR(200),
        `status` VARCHAR(20),
        `rows_affected` INT,
        `message` VARCHAR(500),
        `created_at` TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
    ) ENGINE=InnoDB;

    -- Upgrade existing table to microsecond precision if needed
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
        AND table_name = 'analysis_job_log'
        AND column_name = 'created_at'
        AND datetime_precision = 0
    ) THEN
        ALTER TABLE analysis_job_log
        MODIFY COLUMN created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6);
    END IF;
END//

DROP PROCEDURE IF EXISTS `log_progress`//
CREATE PROCEDURE `log_progress`(
    IN p_job_id VARCHAR(50),
    IN p_step VARCHAR(100),
    IN p_substep VARCHAR(200),
    IN p_status VARCHAR(20),
    IN p_rows INT,
    IN p_message VARCHAR(500)
)
BEGIN
    INSERT INTO analysis_job_log (job_id, step, substep, status, rows_affected, message)
    VALUES (p_job_id, p_step, p_substep, p_status, p_rows, p_message);
    -- Also output to console
    SELECT CONCAT(NOW(), ' | ', p_step, ' | ', p_substep, ' | ', p_status,
                  IFNULL(CONCAT(' | rows: ', p_rows), ''),
                  IFNULL(CONCAT(' | ', p_message), '')) AS progress;
END//

DROP PROCEDURE IF EXISTS `populateAnalysisSummaryTables_v2`//

CREATE PROCEDURE `populateAnalysisSummaryTables_v2`()
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
    -- GLOBAL JOB LOCK - Prevent concurrent execution
    -- ========================================================================
    IF GET_LOCK('populateAnalysisSummaryTables_v2_lock', 0) != 1 THEN
        -- Could not acquire lock immediately - another job is running
        INSERT INTO analysis_job_log (job_id, step, substep, status, message)
        VALUES (v_job_id, 'Pre-flight check', 'Concurrent job detected', 'SKIPPED',
                'Another instance is already running. Exiting to prevent conflicts.');
        SELECT 'SKIPPED: Another job is already running' AS status;
        LEAVE proc_main;
    END IF;

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
    CREATE TABLE analysis_summary_author_new LIKE analysis_summary_author;
    CREATE TABLE analysis_summary_article_new LIKE analysis_summary_article;
    CREATE TABLE analysis_summary_person_new LIKE analysis_summary_person;
    CREATE TABLE analysis_summary_author_list_new LIKE analysis_summary_author_list;

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
    -- Uses advisory lock to prevent deadlocks from concurrent cursor-based updates
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_special_characters') THEN
        CALL log_progress(v_job_id, v_step, 'Escaping special chars in authorsRTF', 'RUNNING', NULL, NULL);

        -- Acquire advisory lock to serialize RTF updates (wait up to 300 seconds)
        IF GET_LOCK('rtf_author_update_lock', 300) = 1 THEN
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
            DO RELEASE_LOCK('rtf_author_update_lock');
        ELSE
            CALL log_progress(v_job_id, v_step, 'Could not acquire RTF lock', 'ERROR', NULL, 'Another job may be running');
            LEAVE proc_main;
        END IF;

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
    CREATE TABLE temp_maxrank (
        pmid INT PRIMARY KEY,
        maxRank INT
    ) ENGINE=InnoDB;
    INSERT INTO temp_maxrank SELECT pmid, MAX(rank) FROM person_article_author GROUP BY pmid;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Computed maxRank', 'INFO', v_rows, NULL);

    -- Step 2b.2: Pre-compute equalContribAll per pmid (only for pmids with equal contrib)
    CALL log_progress(v_job_id, v_step, 'Computing equalContribAll per article', 'RUNNING', NULL, NULL);
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    CREATE TABLE temp_equalcontrib_all (
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
    CREATE TABLE analysis_temp_equalcontrib_v2 (
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

    INSERT INTO analysis_summary_article_new (
        pmid, pmcid, publicationTypeCanonical, articleYear,
        publicationDateStandardized, publicationDateDisplay,
        datePublicationAddedToEntrez, datePublicationAddedToPMC,
        articleTitle, journalTitleVerbose,
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
        MAX(datePublicationAddedToPMC),
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
        WHERE datePublicationAddedtoEntrez != ''
          AND ROUND((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(STR_TO_DATE(datePublicationAddedtoEntrez,'%Y-%m-%d')) ) / (60 * 60 * 24), 0) < 366;
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
    -- Uses advisory lock to prevent deadlocks from concurrent cursor-based updates
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema = DATABASE() AND table_name = 'analysis_special_characters') THEN
        CALL log_progress(v_job_id, v_step, 'Escaping special chars in articleTitleRTF', 'RUNNING', NULL, NULL);

        -- Acquire advisory lock to serialize RTF updates (wait up to 300 seconds)
        IF GET_LOCK('rtf_article_update_lock', 300) = 1 THEN
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
            DO RELEASE_LOCK('rtf_article_update_lock');
        ELSE
            CALL log_progress(v_job_id, v_step, 'Could not acquire RTF lock', 'ERROR', NULL, 'Another job may be running');
            LEAVE proc_main;
        END IF;

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

    -- Populate using person_person_type to derive facultyRank
    INSERT INTO analysis_summary_person_new (personIdentifier, nameFirst, nameMiddle, nameLast, department, facultyRank)
    SELECT * FROM (
        SELECT DISTINCT
            p.personIdentifier,
            p.firstName AS nameFirst,
            p.middleName AS nameMiddle,
            p.lastName AS nameLast,
            p.primaryOrganizationalUnit AS department,
            COALESCE(a.facultyRank, b.facultyRank, c.facultyRank, d.facultyRank) AS facultyRank
        FROM person p

        LEFT JOIN (
            SELECT personIdentifier, 'Full Professor' AS facultyRank
            FROM person_person_type
            WHERE personType = 'academic-faculty-fullprofessor'
        ) a ON a.personIdentifier = p.personIdentifier

        LEFT JOIN (
            SELECT personIdentifier, 'Associate Professor' AS facultyRank
            FROM person_person_type
            WHERE personType = 'academic-faculty-associate'
        ) b ON b.personIdentifier = p.personIdentifier

        LEFT JOIN (
            SELECT personIdentifier, 'Assistant Professor' AS facultyRank
            FROM person_person_type
            WHERE personType = 'academic-faculty-assistant'
        ) c ON c.personIdentifier = p.personIdentifier

        LEFT JOIN (
            SELECT personIdentifier, 'Instructor or Lecturer' AS facultyRank
            FROM person_person_type
            WHERE personType IN ('academic-faculty-instructor', 'academic-faculty-lecturer')
        ) d ON d.personIdentifier = p.personIdentifier

        INNER JOIN analysis_summary_person_scope e ON e.personIdentifier = p.personIdentifier
    ) x
    WHERE facultyRank IS NOT NULL;

    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Inserted person records', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- STEP 4b: Compute article counts
    -- Counts are for articles with publicationTypeNIH = 'Research Article'
    -- and percentileNIH is not null
    -- ========================================================================
    CALL log_progress(v_job_id, v_step, 'Updating article counts', 'RUNNING', NULL, NULL);

    -- countAll: Count of research articles with NIH percentile
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT s.personIdentifier, COUNT(a1.pmid) AS count
        FROM analysis_summary_person_new s
        JOIN analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
        Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE publicationTypeNIH = 'Research Article' AND percentileNIH IS NOT NULL
        GROUP BY s.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.countAll = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated countAll', 'INFO', v_rows, NULL);

    -- countFirst: Count of first-authored research articles with NIH percentile
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT s.personIdentifier, COUNT(a1.pmid) AS count
        FROM analysis_summary_person_new s
        Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
        Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE publicationTypeNIH = 'Research Article' AND percentileNIH IS NOT NULL
          AND a.authorPosition = 'first'
        GROUP BY s.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.countFirst = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated countFirst', 'INFO', v_rows, NULL);

    -- countSenior: Count of senior/last-authored research articles with NIH percentile
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT s.personIdentifier, COUNT(a1.pmid) AS count
        FROM analysis_summary_person_new s
        Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
        Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
        WHERE publicationTypeNIH = 'Research Article' AND percentileNIH IS NOT NULL
          AND a.authorPosition = 'last'
        GROUP BY s.personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.countSenior = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated countSenior', 'INFO', v_rows, NULL);

    IF v_error_occurred THEN
        CALL log_progress(v_job_id, v_step, 'Failed', 'ERROR', NULL, v_error_message);
        CALL cleanup_staging_tables_v2();
        LEAVE proc_main;
    END IF;

    CALL log_progress(v_job_id, v_step, 'Complete', 'DONE', NULL, CONCAT(TIMESTAMPDIFF(SECOND, v_start_time, NOW()), 's elapsed'));

    -- ========================================================================
    -- STEP 5: Compute percentile rankings (peer-based)
    -- Percentile = average of top N articles by percentileNIH
    -- Denominator = count of people with same facultyRank who have the metric
    -- Rank = rank within facultyRank by percentile value
    -- ========================================================================
    SET v_step = '5. Compute percentile rankings';
    CALL log_progress(v_job_id, v_step, 'Computing percentiles (peer-based avg of top N)', 'RUNNING', NULL, NULL);

    -- ========================================================================
    -- 5a. TOP 5 PERCENTILE - ALL POSITIONS
    -- ========================================================================

    -- top5PercentileAll: Average of top 5 percentiles (requires countAll > 4)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            JOIN analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countAll > 4
        ) y
        WHERE article_rank < 6
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileAll = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5PercentileAll', 'INFO', v_rows, NULL);

    -- top5DenominatorAll: Count of people in same facultyRank with valid top5PercentileAll
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top5PercentileAll IS NOT NULL AND countAll > 4
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top5DenominatorAll = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5DenominatorAll', 'INFO', v_rows, NULL);

    -- top5RankAll: Rank within facultyRank by top5PercentileAll
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top5PercentileAll DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countAll > 4
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5RankAll = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5RankAll', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5b. TOP 10 PERCENTILE - ALL POSITIONS
    -- ========================================================================

    -- top10PercentileAll: Average of top 10 percentiles (requires countAll > 9)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            JOIN analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countAll > 9
        ) y
        WHERE article_rank < 11
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileAll = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10PercentileAll', 'INFO', v_rows, NULL);

    -- top10DenominatorAll: Count of people in same facultyRank with valid top10PercentileAll
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top10PercentileAll IS NOT NULL AND countAll > 9
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top10DenominatorAll = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10DenominatorAll', 'INFO', v_rows, NULL);

    -- top10RankAll: Rank within facultyRank by top10PercentileAll
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top10PercentileAll DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countAll > 9
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10RankAll = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10RankAll', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5c. TOP 5 PERCENTILE - FIRST AUTHOR ONLY
    -- ========================================================================

    -- top5PercentileFirst: Average of top 5 percentiles for first-authored (requires countFirst > 4)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countFirst > 4
              AND a.authorPosition = 'first'
        ) y
        WHERE article_rank < 6
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileFirst = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5PercentileFirst', 'INFO', v_rows, NULL);

    -- top5DenominatorFirst
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top5PercentileFirst IS NOT NULL AND countFirst > 4
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top5DenominatorFirst = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5DenominatorFirst', 'INFO', v_rows, NULL);

    -- top5RankFirst
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top5PercentileFirst DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countFirst > 4
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5RankFirst = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5RankFirst', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5d. TOP 10 PERCENTILE - FIRST AUTHOR ONLY
    -- ========================================================================

    -- top10PercentileFirst: Average of top 10 percentiles for first-authored (requires countFirst > 9)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countFirst > 9
              AND a.authorPosition = 'first'
        ) y
        WHERE article_rank < 11
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileFirst = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10PercentileFirst', 'INFO', v_rows, NULL);

    -- top10DenominatorFirst
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top10PercentileFirst IS NOT NULL AND countFirst > 9
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top10DenominatorFirst = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10DenominatorFirst', 'INFO', v_rows, NULL);

    -- top10RankFirst
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top10PercentileFirst DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countFirst > 9
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10RankFirst = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10RankFirst', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5e. TOP 5 PERCENTILE - SENIOR/LAST AUTHOR ONLY
    -- ========================================================================

    -- top5PercentileSenior: Average of top 5 percentiles for last-authored (requires countSenior > 4)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countSenior > 4
              AND a.authorPosition = 'last'
        ) y
        WHERE article_rank < 6
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileSenior = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5PercentileSenior', 'INFO', v_rows, NULL);

    -- top5DenominatorSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top5PercentileSenior IS NOT NULL AND countSenior > 4
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top5DenominatorSenior = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5DenominatorSenior', 'INFO', v_rows, NULL);

    -- top5RankSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top5PercentileSenior DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countSenior > 4
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5RankSenior = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5RankSenior', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5f. TOP 10 PERCENTILE - SENIOR/LAST AUTHOR ONLY
    -- ========================================================================

    -- top10PercentileSenior: Average of top 10 percentiles for last-authored (requires countSenior > 9)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL AND s.countSenior > 9
              AND a.authorPosition = 'last'
        ) y
        WHERE article_rank < 11
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileSenior = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10PercentileSenior', 'INFO', v_rows, NULL);

    -- top10DenominatorSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top10PercentileSenior IS NOT NULL AND countSenior > 9
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top10DenominatorSenior = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10DenominatorSenior', 'INFO', v_rows, NULL);

    -- top10RankSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top10PercentileSenior DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE countSenior > 9
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10RankSenior = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10RankSenior', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5g. TOP 5 PERCENTILE - FIRST OR SENIOR (combined)
    -- Note: countFirstSenior is computed inline since column doesn't exist
    -- ========================================================================

    -- top5PercentileFirstSenior: Average of top 5 percentiles for first/last authored
    -- Requires at least 5 first+last authored articles with percentileNIH
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL
              AND a.authorPosition IN ('first', 'last')
              AND s.personIdentifier IN (
                  -- Only include people with > 4 first/last articles
                  SELECT s2.personIdentifier
                  FROM analysis_summary_person_new s2
                  Join analysis_summary_author_new a2 ON a2.personIdentifier = s2.personIdentifier
                  Join analysis_summary_article_new a12 ON a12.pmid = a2.pmid
                  WHERE a12.publicationTypeNIH = 'Research Article' AND a12.percentileNIH IS NOT NULL
                    AND a2.authorPosition IN ('first', 'last')
                  GROUP BY s2.personIdentifier
                  HAVING COUNT(*) > 4
              )
        ) y
        WHERE article_rank < 6
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5PercentileFirstSenior = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5PercentileFirstSenior', 'INFO', v_rows, NULL);

    -- top5DenominatorFirstSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top5PercentileFirstSenior IS NOT NULL
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top5DenominatorFirstSenior = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5DenominatorFirstSenior', 'INFO', v_rows, NULL);

    -- top5RankFirstSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top5PercentileFirstSenior DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE top5PercentileFirstSenior IS NOT NULL
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top5RankFirstSenior = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top5RankFirstSenior', 'INFO', v_rows, NULL);

    -- ========================================================================
    -- 5h. TOP 10 PERCENTILE - FIRST OR SENIOR (combined)
    -- ========================================================================

    -- top10PercentileFirstSenior (requires > 9 first/last articles)
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier, ROUND(AVG(percentileNIH), 3) AS percentileNIH
        FROM (
            SELECT s.personIdentifier, a1.pmid, a1.percentileNIH,
                   RANK() OVER (PARTITION BY s.personIdentifier ORDER BY a1.percentileNIH DESC) AS article_rank
            FROM analysis_summary_person_new s
            Join analysis_summary_author_new a ON a.personIdentifier = s.personIdentifier
            Join analysis_summary_article_new a1 ON a1.pmid = a.pmid
            WHERE a1.percentileNIH IS NOT NULL
              AND a.authorPosition IN ('first', 'last')
              AND s.personIdentifier IN (
                  -- Only include people with > 9 first/last articles
                  SELECT s2.personIdentifier
                  FROM analysis_summary_person_new s2
                  Join analysis_summary_author_new a2 ON a2.personIdentifier = s2.personIdentifier
                  Join analysis_summary_article_new a12 ON a12.pmid = a2.pmid
                  WHERE a12.publicationTypeNIH = 'Research Article' AND a12.percentileNIH IS NOT NULL
                    AND a2.authorPosition IN ('first', 'last')
                  GROUP BY s2.personIdentifier
                  HAVING COUNT(*) > 9
              )
        ) y
        WHERE article_rank < 11
        GROUP BY personIdentifier
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10PercentileFirstSenior = x.percentileNIH;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10PercentileFirstSenior', 'INFO', v_rows, NULL);

    -- top10DenominatorFirstSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT COUNT(*) AS count, facultyRank
        FROM analysis_summary_person_new
        WHERE top10PercentileFirstSenior IS NOT NULL
        GROUP BY facultyRank
    ) x ON x.facultyRank = p.facultyRank
    SET p.top10DenominatorFirstSenior = x.count;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10DenominatorFirstSenior', 'INFO', v_rows, NULL);

    -- top10RankFirstSenior
    UPDATE analysis_summary_person_new p
    JOIN (
        SELECT personIdentifier,
               RANK() OVER (PARTITION BY facultyRank ORDER BY top10PercentileFirstSenior DESC) AS personRank
        FROM analysis_summary_person_new
        WHERE top10PercentileFirstSenior IS NOT NULL
    ) x ON x.personIdentifier = p.personIdentifier
    SET p.top10RankFirstSenior = x.personRank;
    SET v_rows = ROW_COUNT();
    CALL log_progress(v_job_id, v_step, 'Updated top10RankFirstSenior', 'INFO', v_rows, NULL);

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
                  AND r.datePublicationAddedToEntrez != ''
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
                  AND r.datePublicationAddedToEntrez != ''
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

    -- Release global job lock
    DO RELEASE_LOCK('populateAnalysisSummaryTables_v2_lock');

END proc_main//

-- ============================================================================
-- HELPER PROCEDURE: Cleanup staging tables on error
-- ============================================================================
DROP PROCEDURE IF EXISTS `cleanup_staging_tables_v2`//

CREATE PROCEDURE `cleanup_staging_tables_v2`()
BEGIN
    DROP TABLE IF EXISTS analysis_summary_author_new;
    DROP TABLE IF EXISTS analysis_summary_article_new;
    DROP TABLE IF EXISTS analysis_summary_person_new;
    DROP TABLE IF EXISTS analysis_summary_author_list_new;
    DROP TABLE IF EXISTS temp_maxrank;
    DROP TABLE IF EXISTS temp_equalcontrib_all;
    DROP TABLE IF EXISTS analysis_temp_equalcontrib_v2;
    SELECT 'Staging tables cleaned up' AS status;
END//

-- ============================================================================
-- HELPER PROCEDURE: Restore from backup tables
-- Call this if you need to revert to the previous day's data
-- ============================================================================
DROP PROCEDURE IF EXISTS `restore_from_backup_v2`//

CREATE PROCEDURE `restore_from_backup_v2`()
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
END//

-- ============================================================================
-- HELPER PROCEDURE: Check job status
-- ============================================================================
DROP PROCEDURE IF EXISTS `check_analysis_summary_status`//

CREATE PROCEDURE `check_analysis_summary_status`()
BEGIN
    SELECT
        'analysis_summary_author' AS table_name,
        COUNT(*) AS row_count,
        (SELECT MAX(id) FROM analysis_summary_author) AS max_id
    UNION ALL
    SELECT
        'analysis_summary_article',
        COUNT(*),
        (SELECT MAX(id) FROM analysis_summary_article)
    FROM analysis_summary_article
    UNION ALL
    SELECT
        'analysis_summary_person',
        COUNT(*),
        (SELECT MAX(id) FROM analysis_summary_person)
    FROM analysis_summary_person
    UNION ALL
    SELECT
        'analysis_summary_author_list',
        COUNT(*),
        (SELECT MAX(id) FROM analysis_summary_author_list)
    FROM analysis_summary_author_list;

    -- Check if backup tables exist
    SELECT
        table_name,
        CASE WHEN table_rows > 0 THEN 'EXISTS' ELSE 'EMPTY' END AS backup_status,
        table_rows
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
      AND table_name LIKE 'analysis_summary_%_backup';
END//

-- ============================================================================
-- HELPER PROCEDURE: View job progress logs
-- ============================================================================
DROP PROCEDURE IF EXISTS `view_job_progress`//

CREATE PROCEDURE `view_job_progress`(IN p_job_id VARCHAR(50))
BEGIN
    IF p_job_id IS NULL THEN
        -- Show last job
        SELECT * FROM analysis_job_log
        WHERE job_id = (SELECT MAX(job_id) FROM analysis_job_log)
        ORDER BY id;
    ELSE
        SELECT * FROM analysis_job_log
        WHERE job_id = p_job_id
        ORDER BY id;
    END IF;
END//

-- ============================================================================
-- HELPER PROCEDURE: View latest progress (for polling)
-- ============================================================================
DROP PROCEDURE IF EXISTS `get_latest_progress`//

CREATE PROCEDURE `get_latest_progress`()
BEGIN
    SELECT
        job_id,
        step,
        substep,
        status,
        rows_affected,
        message,
        created_at,
        TIMESTAMPDIFF(SECOND, (SELECT MIN(created_at) FROM analysis_job_log WHERE job_id = l.job_id), created_at) AS elapsed_seconds
    FROM analysis_job_log l
    WHERE id = (SELECT MAX(id) FROM analysis_job_log);
END//

DELIMITER ;
