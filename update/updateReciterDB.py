# updateReciterDB.py

import pymysql
import os
import time
import logging
import pymysql.err
import signal  # Only needed if you ever apply timeouts here; otherwise may omit

# ------------------------------------------------------------------------------
# LOGGING WITH TIMESTAMPS
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 10
RETRY_WAIT_MAX = 300

READ_TIMEOUT = 500     # seconds
WRITE_TIMEOUT = 500    # seconds
CONNECT_TIMEOUT = 10   # seconds

connection = None

# ------------------------------------------------------------------------------
#                     TABLE-LIFECYCLE (SHADOW BUILD / ATOMIC SWAP)
# ------------------------------------------------------------------------------
# The nightly load builds into `<table>_new` shadow tables instead of
# truncating the live tables and rebuilding them in place, then promotes the
# shadow tables into their live names with a single atomic RENAME TABLE at
# the very end (swap_new_tables_into_place(), called from retrieveArticles.py
# after the final identity merge). This removes the daily window where the
# live tables are empty/partial while being read for reporting. This is
# unconditional -- there is no flag to opt out.
#
# Table lifecycle -- creating the `_new` shadow tables, dropping stale
# `_backup` tables, and the atomic RENAME itself -- lives in the stored
# procedures `prepare_person_shadow_tables()` and `swap_person_tables()`
# (setup/person_table_swap.sql), matching the pattern already used for the
# analysis_summary_* tables in setup/populateAnalysisSummaryTables_v2.sql
# ("1. Create staging tables" and "7. Atomic table swap").

# The 10 tables that participate in the atomic swap. person_temp is
# deliberately excluded -- it is internal staging, always truncated and
# loaded in place, never swapped.
SWAP_TABLES = [
    'person', 'person_article', 'person_article_author',
    'person_article_department', 'person_article_grant',
    'person_article_keyword', 'person_article_relationship',
    'person_article_scopus_target_author_affiliation',
    'person_article_scopus_non_target_author_affiliation',
    'person_person_type',
]


class ShadowTableProcedureMissing(RuntimeError):
    """Raised when prepare_person_shadow_tables() or swap_person_tables() is
    not installed in the target database (setup/person_table_swap.sql has not
    been applied). Always propagates out of main() -- never swallowed like
    other errors there -- because it means the load already wrote to shadow
    tables that will never be promoted."""
    pass


# RENAME TABLE safety / disk-space analysis (verified against this repo):
# - FOREIGN KEYs: none of the 10 SWAP_TABLES is referenced by a FOREIGN KEY
#   anywhere in setup/createDatabaseTableReciterDb.sql -- the only FKs in that
#   file are on the admin_* auth tables, which are untouched by this swap.
# - TRIGGERs / VIEWs: setup/ contains no SQL TRIGGERs and no SQL VIEWs. The
#   only "view" hit is a stored procedure named `view_job_progress`, not a
#   database VIEW.
# - Stored procedures: table names inside them resolve at execution time, so
#   a RENAME TABLE between runs is safe -- no procedure can be left pointing
#   at a stale/renamed table.
# - Disk space: every swap table exists twice (live + `_new`) for the full
#   build window, so peak usage is roughly 2x the combined footprint of the
#   10 SWAP_TABLES. person_article_author (~3.9M rows) dominates that total.
#   Confirm headroom on the target volume before running the nightly loader
#   in production.

def shadow_table_name(table_name):
    """Return the table this run should actually load/update: `<table_name>_new`
    when the table participates in the swap, otherwise `table_name` unchanged
    (person_temp always passes through unchanged). Pure string logic, no DB
    access."""
    if table_name in SWAP_TABLES:
        return f"{table_name}_new"
    return table_name


def _call_swap_procedure(cursor, procedure_name):
    """CALL a table-lifecycle stored procedure from setup/person_table_swap.sql
    (prepare_person_shadow_tables or swap_person_tables) and consume its
    `SELECT ... AS status` result set so the connection is left usable for the
    next statement. Raises ShadowTableProcedureMissing with an actionable
    message if the procedure is not installed in this database -- this must
    never be silently swallowed: a missing procedure means the load wrote (or
    is about to write) to shadow tables that will never be promoted.
    """
    try:
        cursor.execute(f"CALL {procedure_name}();")
        try:
            cursor.fetchall()
        except pymysql.err.ProgrammingError:
            pass  # no result set to consume
        return cursor
    except pymysql.err.MySQLError as e:
        error_code = e.args[0] if e.args else None
        if error_code == 1305:  # ER_SP_DOES_NOT_EXIST
            raise ShadowTableProcedureMissing(
                f"Stored procedure `{procedure_name}` is not installed in this "
                "database. Apply setup/person_table_swap.sql "
                "(mysql --host=... --user=... --password=... reciterdb < "
                "setup/person_table_swap.sql) before running the nightly "
                "loader -- otherwise the load writes to shadow tables that "
                "are never promoted."
            ) from e
        raise

# ------------------------------------------------------------------------------
#                     EXECUTE WITH RECONNECT
# ------------------------------------------------------------------------------
def execute_with_reconnect(cursor, sql):
    """
    Execute SQL with automatic reconnection on certain errors.
    Includes handling of various MySQL errors and generic Python exceptions.
    """
    retries = 0
    while retries < MAX_RETRIES:
        start_time = time.time()
        try:
            logger.debug(f"Executing SQL: {sql[:500]}...")  # Log partial SQL to avoid huge logs
            cursor.execute(sql)
            logger.debug(f"SQL executed successfully in {time.time() - start_time:.2f} seconds.")
            return cursor

        except (pymysql.err.OperationalError,
                pymysql.err.InternalError,
                pymysql.err.InterfaceError,
                pymysql.err.MySQLError,
                BrokenPipeError,
                TimeoutError) as e:
            # Handle specific error codes for lost connection or server timeout
            if isinstance(e, pymysql.err.OperationalError) and e.args and e.args[0] in (2006, 2013):
                retries += 1
                wait_time = min(2 ** retries, RETRY_WAIT_MAX)
                logger.warning(
                    f"Connection lost during query (Error {e.args[0]}). "
                    f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s."
                )
                time.sleep(wait_time)

                # Attempt to reconnect
                try:
                    connection.ping(reconnect=True)
                    cursor = connection.cursor()
                    logger.info("Reconnected to the database successfully.")
                except Exception as reconnect_error:
                    logger.error(f"Error reconnecting after connection loss: {reconnect_error}")
                    continue
            elif isinstance(e, pymysql.err.MySQLError) or isinstance(e, TimeoutError):
                # Generic MySQL or timeout error
                retries += 1
                wait_time = min(2 ** retries, RETRY_WAIT_MAX)
                logger.warning(
                    f"MySQL error encountered: {e}. "
                    f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s."
                )
                time.sleep(wait_time)
            else:
                # Reraise unexpected exceptions
                raise

        except Exception as e:
            # Catch any other unexpected exceptions
            retries += 1
            wait_time = min(2 ** retries, RETRY_WAIT_MAX)
            logger.error(
                f"Unexpected error executing SQL: {e}. "
                f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s."
            )
            time.sleep(wait_time)
            try:
                connection.ping(reconnect=True)
                cursor = connection.cursor()
                logger.info("Reconnected to the database after unexpected error.")
            except Exception as reconnect_error:
                logger.error(f"Error reconnecting after unexpected exception: {reconnect_error}")
                continue

    raise Exception("Failed to execute SQL after several retries.")


# ------------------------------------------------------------------------------
#                     ESTABLISH CONNECTION
# ------------------------------------------------------------------------------
def establish_connection():
    global connection
    retries = 0
    while retries < MAX_RETRIES:
        try:
            # Added read_timeout, write_timeout, and connect_timeout to avoid hangs
            connection = pymysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"),
                local_infile=True,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=CONNECT_TIMEOUT,
                read_timeout=READ_TIMEOUT,
                write_timeout=WRITE_TIMEOUT
            )
            logger.info("Database connection established successfully.")
            return connection
        except pymysql.err.OperationalError as e:
            retries += 1
            wait_time = min(2 ** retries, RETRY_WAIT_MAX)
            logger.warning(
                f"Database connection failed: {e}. "
                f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time} seconds..."
            )
            time.sleep(wait_time)

    raise Exception("Failed to establish database connection after several retries.")


# ------------------------------------------------------------------------------
#                    LOADING person_temp AND Other Tables
# ------------------------------------------------------------------------------
def load_person_temp(cursor, csv_file_path):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{current_time} -- Loading data into person_temp from {csv_file_path}.")
    sql = f"""
    LOAD DATA LOCAL INFILE '{csv_file_path}'
    INTO TABLE person_temp
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (lastName, title, firstName, middleName, primaryEmail,
    primaryOrganizationalUnit, primaryInstitution, personIdentifier, relationshipIdentityCount);
    """
    cursor = execute_with_reconnect(cursor, sql)
    cursor = execute_with_reconnect(cursor, "SELECT COUNT(*) AS row_count FROM person_temp;")
    row_count = cursor.fetchone()['row_count']
    logger.info(f"{current_time} -- Loaded {row_count} rows into person_temp successfully.")
    return cursor

def update_person(cursor):
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    target_table = shadow_table_name('person')
    logger.info(f"{current_time} -- Starting update_person (target: `{target_table}`).")
    update_query = f"""
    UPDATE `{target_table}` p
    JOIN person_temp i ON i.personIdentifier = p.personIdentifier
    SET p.firstName = i.firstName,
        p.middleName = i.middleName,
        p.lastName = i.lastName,
        p.title = i.title,
        p.primaryEmail = i.primaryEmail,
        p.primaryOrganizationalUnit = i.primaryOrganizationalUnit,
        p.primaryInstitution = i.primaryInstitution,
        p.relationshipIdentityCount = i.relationshipIdentityCount;
    """
    cursor = execute_with_reconnect(cursor, update_query)
    logger.info(f"{current_time} -- `{target_table}` table updated with data from person_temp table.")
    return cursor

def load_table_once(cursor, csv_file_path, table_name, columns, already_loaded_tables):
    if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
        logger.warning(f"CSV file {csv_file_path} is missing or empty for {table_name}. Skipping.")
        return cursor

    if table_name in already_loaded_tables:
        logger.info(f"Table {table_name} already loaded in this run. Skipping.")
        return cursor

    target_table = shadow_table_name(table_name)
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{current_time} -- Loading {table_name} from {csv_file_path} into `{target_table}`.")
    columns_str = ', '.join(f'`{col}`' for col in columns)
    csv_file_path_escaped = csv_file_path.replace("\\", "\\\\")  # Escape for Windows if needed

    sql = (
        f"LOAD DATA LOCAL INFILE '{csv_file_path_escaped}' "
        f"INTO TABLE `{target_table}` "
        "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES "
        f"({columns_str});"
    )

    cursor = execute_with_reconnect(cursor, sql)
    cursor = execute_with_reconnect(cursor, f"SELECT COUNT(*) AS row_count FROM `{target_table}`;")
    row_count = cursor.fetchone()['row_count']
    logger.info(f"{current_time} -- Data successfully loaded into `{target_table}`. Row count: {row_count}")

    already_loaded_tables.add(table_name)
    return cursor


# ------------------------------------------------------------------------------
#                               MAIN FUNCTION
# ------------------------------------------------------------------------------
def main(truncate_tables=True, skip_person_temp=False):
    """
    Main entry point for loading CSV data into MariaDB.

    :param truncate_tables: If True, truncates all relevant tables, disables keys once,
                           loads data, then re-enables keys at the end.
    :param skip_person_temp: If True, skip loading person_temp (and thus skip update_person).
    """
    global connection
    connection = establish_connection()
    cursor = connection.cursor()

    # The set of all relevant tables
    all_tables = [
        'person', 'person_article', 'person_article_author',
        'person_article_department', 'person_article_grant',
        'person_article_keyword', 'person_article_relationship',
        'person_article_scopus_target_author_affiliation',
        'person_article_scopus_non_target_author_affiliation',
        'person_person_type', 'person_temp'
    ]

    try:
        logger.info(f"Inside main(): truncate_tables={truncate_tables}, skip_person_temp={skip_person_temp}")

        # ------------------------------------------------------------------------------
        # (1) Optional: TRUNCATE TABLES if requested
        # ------------------------------------------------------------------------------
        if truncate_tables:
            # person_temp is internal staging -- always truncated and loaded
            # in place, never swapped.
            truncate_sql = "TRUNCATE TABLE `person_temp`;"
            cursor = execute_with_reconnect(cursor, truncate_sql)
            connection.commit()

            # The 10 swap-managed tables are rebuilt as fresh `_new` shadow
            # tables by the stored procedure in setup/person_table_swap.sql --
            # table lifecycle (dropping any `_new` left by a prior crashed
            # run, then CREATE ... LIKE) is managed in SQL, not Python, so it
            # can't silently drift from setup/populateAnalysisSummaryTables_v2.sql's
            # equivalent "1. Create staging tables" step.
            cursor = _call_swap_procedure(cursor, "prepare_person_shadow_tables")
            connection.commit()

            # Disable keys once at the outset (on the shadow table when swapping)
            for table in all_tables:
                target = shadow_table_name(table)
                disable_sql = f"ALTER TABLE `{target}` DISABLE KEYS;"
                try:
                    cursor = execute_with_reconnect(cursor, disable_sql)
                except Exception as e:
                    logger.warning(f"Could not disable keys on {target}: {e}")

        already_loaded_tables = set()

        # ------------------------------------------------------------------------------
        # (2) Load CSVs (Except person_temp/person_person_type initially)
        # ------------------------------------------------------------------------------
        csv_files = {
            'person2.csv': 'person',
            'person_article2.csv': 'person_article',
            'person_article_author2.csv': 'person_article_author',
            'person_article_department2.csv': 'person_article_department',
            'person_article_grant2.csv': 'person_article_grant',
            'person_article_keyword2.csv': 'person_article_keyword',
            'person_article_relationship2.csv': 'person_article_relationship',
            'person_article_scopus_target_author_affiliation2.csv': 'person_article_scopus_target_author_affiliation',
            'person_article_scopus_non_target_author_affiliation2.csv': 'person_article_scopus_non_target_author_affiliation',
        }

        table_columns = {
            'person': [
                'personIdentifier', 'dateAdded', 'dateUpdated', 'precision', 'recall',
                'countSuggestedArticles', 'countPendingArticles', 'overallAccuracy', 'mode'
            ],
            'person_article': [
                "personIdentifier", "pmid", "authorshipLikelihoodScore", "pmcid",
                "userAssertion", "publicationDateDisplay", "publicationDateStandardized",
                "publicationTypeCanonical", "scopusDocID", "journalTitleVerbose", "articleTitle",
                "articleAuthorNameFirstName", "articleAuthorNameLastName",
                "institutionalAuthorNameFirstName", "institutionalAuthorNameMiddleName",
                "institutionalAuthorNameLastName", "nameMatchFirstScore", "nameMatchFirstType",
                "nameMatchMiddleScore", "nameMatchMiddleType", "nameMatchLastScore",
                "nameMatchLastType", "nameMatchModifierScore", "nameScoreTotal", "emailMatch",
                "emailMatchScore", "journalSubfieldScienceMetrixLabel",
                "journalSubfieldScienceMetrixID", "journalSubfieldDepartment",
                "journalSubfieldScore", "relationshipEvidenceTotalScore",
                "relationshipPositiveMatchScore",
                "relationshipNegativeMatchScore",
                "relationshipIdentityCount",
                "relationshipMinimumTotalScore", "relationshipNonMatchCount",
                "relationshipNonMatchScore", "articleYear",
                "identityBachelorYear", "discrepancyDegreeYearBachelor", "discrepancyDegreeYearBachelorScore",
                "identityDoctoralYear", "discrepancyDegreeYearDoctoral", "discrepancyDegreeYearDoctoralScore",
                "genderScoreArticle", "genderScoreIdentity", "genderScoreIdentityArticleDiscrepancy",
                "personType", "personTypeScore", "countArticlesRetrieved", "articleCountScore",
                "countAuthors",
                "authorCountScore",
                "targetAuthorCount",        
                "targetAuthorCountPenalty",   
                "targetAuthorInstitutionalAffiliationArticlePubmedLabel",
                "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore",
                "scopusNonTargetAuthorInstitutionalAffiliationSource",
                "scopusNonTargetAuthorInstitutionalAffiliationScore",
                "datePublicationAddedToEntrez", "datePublicationAddedToPMC", "doi",
                "issn", "issue", "journalTitleISOabbreviation", "pages", "timesCited", "volume",
                "feedbackScoreCites", "feedbackScoreCoAuthorName", "feedbackScoreEmail",
                "feedbackScoreInstitution", "feedbackScoreJournal", "feedbackScoreJournalSubField",
                "feedbackScoreKeyword", "feedbackScoreOrcid", "feedbackScoreOrcidCoAuthor",
                "feedbackScoreOrganization", "feedbackScoreTargetAuthorName", "feedbackScoreYear",
                "feedbackScoreTextSimilarity", "feedbackScoreJournalTitleSimilarity",
                "feedbackScoreBibliographicCoupling",
                "totalArticleScoreStandardized", "totalArticleScoreNonStandardized"
            ],
            'person_article_author': [
                'personIdentifier', 'pmid', 'authorFirstName', 'authorLastName', 'equalContrib', 'rank', 'orcid', 'targetAuthor'
            ],
            'person_article_department': [
                'personIdentifier', 'pmid', 'identityOrganizationalUnit', 'articleAffiliation',
                'organizationalUnitType', 'organizationalUnitMatchingScore', 'organizationalUnitModifier',
                'organizationalUnitModifierScore'
            ],
            'person_article_grant': [
                'personIdentifier', 'pmid', 'articleGrant', 'grantMatchScore', 'institutionGrant'
            ],
            'person_article_keyword': [
                'personIdentifier', 'keyword', 'pmid'
            ],
            'person_article_relationship': [
                'personIdentifier', 'pmid', 'relationshipNameArticleFirstName', 'relationshipNameArticleLastName',
                'relationshipNameIdentityFirstName', 'relationshipNameIdentityLastName', 'relationshipType',
                'relationshipMatchType', 'relationshipMatchingScore', 'relationshipVerboseMatchModifierScore',
                'relationshipMatchModifierMentor', 'relationshipMatchModifierMentorSeniorAuthor',
                'relationshipMatchModifierManager', 'relationshipMatchModifierManagerSeniorAuthor'
            ],
            'person_article_scopus_target_author_affiliation': [
                'personIdentifier', 'pmid', 'targetAuthorInstitutionalAffiliationSource',
                'scopusTargetAuthorInstitutionalAffiliationIdentity',
                'targetAuthorInstitutionalAffiliationArticleScopusLabel',
                'targetAuthorInstitutionalAffiliationArticleScopusAffiliationId',
                'targetAuthorInstitutionalAffiliationMatchType',
                'targetAuthorInstitutionalAffiliationMatchTypeScore'
            ],
            'person_article_scopus_non_target_author_affiliation': [
                'personIdentifier', 'pmid', 'nonTargetAuthorInstitutionLabel',
                'nonTargetAuthorInstitutionID', 'nonTargetAuthorInstitutionCount'
            ],
        }

        # Load all CSVs except person_temp and person_person_type
        for csv_file, table_name in csv_files.items():
            csv_file_path = os.path.join('temp', 'parsedOutput', csv_file)
            if table_name not in table_columns:
                logger.warning(f"No columns defined for {table_name}. Skipping load.")
                continue
            cursor = load_table_once(cursor, csv_file_path, table_name, table_columns[table_name], already_loaded_tables)

        # ------------------------------------------------------------------------------
        # (3) Load person_temp and person_person_type if needed
        # ------------------------------------------------------------------------------
        if not skip_person_temp:
            temp_csv_path = os.path.join("temp", "parsedOutput", "person_temp.csv")
            cursor = load_person_temp(cursor, temp_csv_path)

            person_person_type_path = os.path.join("temp", "parsedOutput", "person_person_type.csv")
            if os.path.exists(person_person_type_path):
                columns = ["personIdentifier", "personType"]
                # Using a new set here for the sake of clarity, so it doesn't conflict
                # with other loaded tables. If you want to unify, you can pass in `already_loaded_tables`.
                cursor = load_table_once(
                    cursor,
                    person_person_type_path,
                    "person_person_type",
                    columns,
                    already_loaded_tables=set()
                )

        # ------------------------------------------------------------------------------
        # (4) Re-enable keys (only once at the end, if we disabled them above)
        # ------------------------------------------------------------------------------
        if truncate_tables:
            for table in all_tables:
                target = shadow_table_name(table)
                enable_sql = f"ALTER TABLE `{target}` ENABLE KEYS;"
                try:
                    cursor = execute_with_reconnect(cursor, enable_sql)
                except Exception as e:
                    logger.warning(f"Could not enable keys on {target}: {e}")

        # ------------------------------------------------------------------------------
        # (5) If we have person_temp, run update_person
        # ------------------------------------------------------------------------------
        if not skip_person_temp:
            cursor = update_person(cursor)

        connection.commit()

    except ShadowTableProcedureMissing:
        # A missing swap procedure means this run wrote to shadow tables that
        # will never be promoted -- never swallow this like the generic
        # handler below does for other errors.
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            logger.info("Database connection closed after main().")
            connection = None


# ------------------------------------------------------------------------------
#                call_update_person_only (For Overwrite Scenarios)
# ------------------------------------------------------------------------------
def call_update_person_only():
    global connection
    connection = establish_connection()
    cursor = connection.cursor()
    try:
        logger.info("Calling update_person ONLY, without loading person_temp...")
        cursor = update_person(cursor)  # uses the existing person_temp table
        connection.commit()
    except Exception as e:
        logger.error(f"Error in call_update_person_only: {e}")
        raise
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            connection = None
            logger.info("Database connection closed after call_update_person_only().")


# ------------------------------------------------------------------------------
#                swap_new_tables_into_place (Atomic Table Swap)
# ------------------------------------------------------------------------------
def swap_new_tables_into_place():
    """Promote every `<table>_new` shadow table into its live name by calling
    the swap_person_tables() stored procedure (setup/person_table_swap.sql),
    which performs the drop-stale-backups-then-single-atomic-RENAME sequence
    mirroring the "7. Atomic table swap" step in
    setup/populateAnalysisSummaryTables_v2.sql. Table lifecycle lives in that
    stored procedure now, not here.

    Always runs -- there is no flag to disable it. Must be called after the
    final identity merge (call_update_person_only(), which updates
    person_new) has completed, so the person table swapped into place
    already has firstName/lastName/etc. populated -- call this from
    retrieveArticles.py's Step 6, after that call.

    MySQL executes a multi-table RENAME TABLE atomically: either every listed
    rename takes effect or none do. So a crash or error during the RENAME
    itself leaves every live table exactly as it was before this call --
    still serving the prior run's complete data. Everything before the
    RENAME only ever touches `_new` shadow tables (or person_temp, which was
    never live-reporting data), so a crash anywhere earlier in the nightly
    run is equally harmless to the live tables.

    The `_backup` tables the procedure produces are NOT dropped here -- they
    are the rollback window consumed by
    setup/restore_person_tables_from_backup.sql, cleared only by the next
    run's call to swap_person_tables().
    """
    global connection
    connection = establish_connection()
    cursor = connection.cursor()
    try:
        logger.info("7. Atomic table swap -- calling swap_person_tables().")
        cursor = _call_swap_procedure(cursor, "swap_person_tables")
        connection.commit()
        logger.info("Atomic table swap complete -- live tables now reflect tonight's rebuild.")

    except Exception as e:
        logger.error(f"Atomic table swap FAILED -- live tables left untouched (prior run's data still serving): {e}")
        raise
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            connection = None
            logger.info("Database connection closed after swap_new_tables_into_place().")
