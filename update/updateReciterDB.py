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
#                     ATOMIC TABLE SWAP CONFIG
# ------------------------------------------------------------------------------
# When ATOMIC_SWAP=1, the nightly load builds into `<table>_new` shadow tables
# instead of truncating the live tables and rebuilding them in place, then
# promotes the shadow tables into their live names with a single atomic
# RENAME TABLE at the very end (swap_new_tables_into_place(), called from
# retrieveArticles.py after the final identity merge). This removes the daily
# window where the live tables are empty/partial while being read for
# reporting. Mirrors the pattern in setup/populateAnalysisSummaryTables_v2.sql
# ("7. Atomic table swap"), adapted to Python/pymysql and to `_old`/immediate
# drop instead of that script's `_backup`/next-run drop.
#
# Default OFF: this is the core nightly loader and this branch has not been
# exercised against a real database. Opt in explicitly with ATOMIC_SWAP=1
# (env var on the reciterdb CronJob) once verified on dev.
ATOMIC_SWAP = os.getenv("ATOMIC_SWAP", "0") == "1"

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


def shadow_table_name(table_name):
    """Return the table this run should actually load/update: `<table_name>_new`
    when atomic-swap mode is on and the table participates in the swap,
    otherwise `table_name` unchanged. Pure string logic, no DB access."""
    if ATOMIC_SWAP and table_name in SWAP_TABLES:
        return f"{table_name}_new"
    return table_name


def build_swap_rename_sql():
    """Build the single atomic RENAME TABLE statement that promotes every
    `_new` shadow table into its live name, moving the current live table to
    `_old` in the same statement. Pure string construction, no DB access."""
    clauses = []
    for t in SWAP_TABLES:
        clauses.append(f"`{t}` TO `{t}_old`")
        clauses.append(f"`{t}_new` TO `{t}`")
    return "RENAME TABLE " + ", ".join(clauses) + ";"

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
            for table in all_tables:
                if ATOMIC_SWAP and table in SWAP_TABLES:
                    # Build into a shadow table instead of truncating the live one.
                    # CREATE ... LIKE copies structure/indexes (no data); TRUNCATE
                    # clears out any partial data left by a prior crashed run.
                    create_sql = f"CREATE TABLE IF NOT EXISTS `{table}_new` LIKE `{table}`;"
                    cursor = execute_with_reconnect(cursor, create_sql)
                    truncate_sql = f"TRUNCATE TABLE `{table}_new`;"
                    cursor = execute_with_reconnect(cursor, truncate_sql)
                else:
                    sql = f"TRUNCATE TABLE `{table}`;"
                    cursor = execute_with_reconnect(cursor, sql)
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
    """Promote every `<table>_new` shadow table into its live name with a
    single atomic RENAME TABLE statement, mirroring the "7. Atomic table
    swap" step in setup/populateAnalysisSummaryTables_v2.sql.

    No-op unless ATOMIC_SWAP=1. Must be called after the final identity merge
    (call_update_person_only(), which updates person_new) has completed, so
    the person table swapped into place already has firstName/lastName/etc.
    populated -- call this from retrieveArticles.py's Step 6, after that call.

    MySQL executes a multi-table RENAME TABLE atomically: either every listed
    rename takes effect or none do. So a crash or error during the RENAME
    itself leaves every live table exactly as it was before this call --
    still serving the prior run's complete data. Everything before the
    RENAME only ever touches `_new` shadow tables (or person_temp, which was
    never live-reporting data), so a crash anywhere earlier in the nightly
    run is equally harmless to the live tables.
    """
    if not ATOMIC_SWAP:
        logger.info("ATOMIC_SWAP not enabled; skipping table swap (legacy truncate-in-place mode).")
        return

    global connection
    connection = establish_connection()
    cursor = connection.cursor()
    try:
        logger.info("7. Atomic table swap -- dropping any stale `_old` tables from a prior run.")
        for t in SWAP_TABLES:
            drop_sql = f"DROP TABLE IF EXISTS `{t}_old`;"
            cursor = execute_with_reconnect(cursor, drop_sql)
        connection.commit()

        rename_sql = build_swap_rename_sql()
        logger.info(f"Performing atomic RENAME to swap {len(SWAP_TABLES)} tables into place: {rename_sql}")
        cursor = execute_with_reconnect(cursor, rename_sql)
        connection.commit()
        logger.info("Atomic table swap complete -- live tables now reflect tonight's rebuild.")

        # The RENAME above already succeeded at this point: live tables are
        # the new data. Dropping the now-stale `_old` tables is best-effort
        # cleanup, not part of the atomicity guarantee -- a failure here is
        # non-fatal and swept up by next run's pre-swap drop.
        try:
            for t in SWAP_TABLES:
                drop_sql = f"DROP TABLE IF EXISTS `{t}_old`;"
                cursor = execute_with_reconnect(cursor, drop_sql)
            connection.commit()
            logger.info("Dropped `_old` tables from this run.")
        except Exception as cleanup_err:
            logger.warning(f"Swap succeeded but failed to drop `_old` backup tables (non-fatal, cleaned up next run): {cleanup_err}")

    except Exception as e:
        logger.error(f"Atomic table swap FAILED -- live tables left untouched (prior run's data still serving): {e}")
        raise
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            connection = None
            logger.info("Database connection closed after swap_new_tables_into_place().")
