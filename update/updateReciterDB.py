# updateReCiterDB.py  

import pymysql
import os
import time
import logging
import pymysql.err

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_RETRIES = 10
RETRY_WAIT_MAX = 300

READ_TIMEOUT = 500     # seconds
WRITE_TIMEOUT = 500    # seconds
CONNECT_TIMEOUT = 10   # seconds

connection = None

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
            if isinstance(e, pymysql.err.OperationalError) and e.args[0] in (2006, 2013):
                retries += 1
                wait_time = min(2 ** retries, RETRY_WAIT_MAX)
                logger.warning(f"Connection lost during query (Error {e.args[0]}). "
                               f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s.")
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
                logger.warning(f"MySQL error encountered: {e}. Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s.")
                time.sleep(wait_time)
            else:
                # Reraise unexpected exceptions
                raise
        except Exception as e:
            # Catch any other unexpected exceptions
            retries += 1
            wait_time = min(2 ** retries, RETRY_WAIT_MAX)
            logger.error(f"Unexpected error executing SQL: {e}. Retrying ({retries}/{MAX_RETRIES}) in {wait_time}s.")
            time.sleep(wait_time)
            try:
                connection.ping(reconnect=True)
                cursor = connection.cursor()
                logger.info("Reconnected to the database after unexpected error.")
            except Exception as reconnect_error:
                logger.error(f"Error reconnecting after unexpected exception: {reconnect_error}")
                continue

    raise Exception("Failed to execute SQL after several retries.")

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
            logger.warning(f"Database connection failed: {e}. "
                           f"Retrying ({retries}/{MAX_RETRIES}) in {wait_time} seconds...")
            time.sleep(wait_time)
    raise Exception("Failed to establish database connection after several retries.")

def load_person_temp(cursor, csv_file_path):
    logger.info(f"{time.ctime()} -- Loading data into person_temp from {csv_file_path}.")
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
    logger.info(f"{time.ctime()} -- Loaded {row_count} rows into person_temp successfully.")
    return cursor

def update_person(cursor):
    logger.info(f"{time.ctime()} -- Starting update_person.")
    update_query = """
    UPDATE person p
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
    logger.info(f"{time.ctime()} -- person table updated with data from person_temp table.")
    return cursor

def load_table_once(cursor, csv_file_path, table_name, columns, already_loaded_tables):
    if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
        logger.warning(f"CSV file {csv_file_path} is missing or empty for {table_name}. Skipping.")
        return cursor

    if table_name in already_loaded_tables:
        logger.info(f"Table {table_name} already loaded in this run. Skipping.")
        return cursor

    logger.info(f"Loading {table_name} from {csv_file_path}.")
    columns_str = ', '.join(f'`{col}`' for col in columns)
    csv_file_path_escaped = csv_file_path.replace("\\", "\\\\")  # Escape for Windows if needed

    sql = (
        f"LOAD DATA LOCAL INFILE '{csv_file_path_escaped}' "
        f"INTO TABLE `{table_name}` "
        "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES "
        f"({columns_str});"
    )

    cursor = execute_with_reconnect(cursor, sql)
    cursor = execute_with_reconnect(cursor, f"SELECT COUNT(*) AS row_count FROM {table_name};")
    row_count = cursor.fetchone()['row_count']
    logger.info(f"Data successfully loaded into {table_name}. Row count: {row_count}")

    already_loaded_tables.add(table_name)
    return cursor

def main(truncate_tables=True, skip_person_temp=False):
    global connection
    connection = establish_connection()
    cursor = connection.cursor()

    try:
        logger.info(f"Inside main(): truncate_tables={truncate_tables}, skip_person_temp={skip_person_temp}")

        # Define all tables to be loaded for index disable/enable
        all_tables = [
            'person', 'person_article', 'person_article_author',
            'person_article_department', 'person_article_grant',
            'person_article_keyword', 'person_article_relationship',
            'person_article_scopus_target_author_affiliation',
            'person_article_scopus_non_target_author_affiliation',
            'person_person_type', 'person_temp'
        ]

        if truncate_tables:
            for table in all_tables:
                sql = f"TRUNCATE TABLE {table};"
                cursor = execute_with_reconnect(cursor, sql)
            connection.commit()

        # Disable keys before loading
        for table in all_tables:
            sql = f"ALTER TABLE `{table}` DISABLE KEYS;"
            try:
                cursor = execute_with_reconnect(cursor, sql)
            except Exception as e:
                logger.warning(f"Could not disable keys on {table}: {e}")

        already_loaded_tables = set()

        # ---------------------------------------------------------
        # (A) Load all other CSVs except person_temp/person_person_type
        # ---------------------------------------------------------
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
            'person': ['personIdentifier', 'dateAdded', 'dateUpdated', 'precision', 'recall', 'countSuggestedArticles', 'countPendingArticles', 'overallAccuracy', 'mode'],
            'person_article': ["personIdentifier", "pmid", "authorshipLikelihoodScore", "pmcid",
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
                               "relationshipMinimumTotalScore", "relationshipNonMatchCount",
                               "relationshipNonMatchScore", "articleYear",
                               "identityBachelorYear", "discrepancyDegreeYearBachelor", "discrepancyDegreeYearBachelorScore",
                               "identityDoctoralYear", "discrepancyDegreeYearDoctoral", "discrepancyDegreeYearDoctoralScore",
                               "genderScoreArticle", "genderScoreIdentity", "genderScoreIdentityArticleDiscrepancy",
                               "personType", "personTypeScore", "countArticlesRetrieved", "articleCountScore",
                               "targetAuthorInstitutionalAffiliationArticlePubmedLabel",
                               "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore",
                               "scopusNonTargetAuthorInstitutionalAffiliationSource",
                               "scopusNonTargetAuthorInstitutionalAffiliationScore",
                               "datePublicationAddedToEntrez", "doi",
                               "issn", "issue", "journalTitleISOabbreviation", "pages", "timesCited", "volume",
                               "feedbackScoreCites", "feedbackScoreCoAuthorName", "feedbackScoreEmail",
                               "feedbackScoreInstitution", "feedbackScoreJournal", "feedbackScoreJournalSubField",
                               "feedbackScoreKeyword", "feedbackScoreOrcid", "feedbackScoreOrcidCoAuthor",
                               "feedbackScoreOrganization", "feedbackScoreTargetAuthorName", "feedbackScoreYear",
                               "totalArticleScoreStandardized", "totalArticleScoreNonStandardized"],
            'person_article_author': ['personIdentifier', 'pmid', 'authorFirstName', 'authorLastName', 'equalContrib', 'rank', 'orcid', 'targetAuthor'],
            'person_article_department': ['personIdentifier', 'pmid', 'identityOrganizationalUnit', 'articleAffiliation', 'organizationalUnitType', 'organizationalUnitMatchingScore', 'organizationalUnitModifier', 'organizationalUnitModifierScore'],
            'person_article_grant': ['personIdentifier', 'pmid', 'articleGrant', 'grantMatchScore', 'institutionGrant'],
            'person_article_keyword': ['personIdentifier', 'keyword', 'pmid'],
            'person_article_relationship': ['personIdentifier', 'pmid', 'relationshipNameArticleFirstName', 'relationshipNameArticleLastName', 'relationshipNameIdentityFirstName', 'relationshipNameIdentityLastName', 'relationshipType', 'relationshipMatchType', 'relationshipMatchingScore', 'relationshipVerboseMatchModifierScore', 'relationshipMatchModifierMentor', 'relationshipMatchModifierMentorSeniorAuthor', 'relationshipMatchModifierManager', 'relationshipMatchModifierManagerSeniorAuthor'],
            'person_article_scopus_target_author_affiliation': ['personIdentifier', 'pmid', 'targetAuthorInstitutionalAffiliationSource', 'scopusTargetAuthorInstitutionalAffiliationIdentity', 'targetAuthorInstitutionalAffiliationArticleScopusLabel', 'targetAuthorInstitutionalAffiliationArticleScopusAffiliationId', 'targetAuthorInstitutionalAffiliationMatchType', 'targetAuthorInstitutionalAffiliationMatchTypeScore'],
            'person_article_scopus_non_target_author_affiliation': ['personIdentifier', 'pmid', 'nonTargetAuthorInstitutionLabel', 'nonTargetAuthorInstitutionID', 'nonTargetAuthorInstitutionCount'],
        }

        # Load all tables except person_temp and person_person_type
        for csv_file, table_name in csv_files.items():
            csv_file_path = os.path.join('temp', 'parsedOutput', csv_file)
            if table_name not in table_columns:
                logger.warning(f"No columns defined for {table_name}. Skipping load.")
                continue
            cursor = load_table_once(cursor, csv_file_path, table_name, table_columns[table_name], already_loaded_tables)

        # (B) Load person_temp and person_person_type if needed
        # NOTE: We do NOT run update_person here anymore.
        if not skip_person_temp:
            cursor = load_person_temp(cursor, "temp/parsedOutput/person_temp.csv")
            person_person_type_path = os.path.join("temp", "parsedOutput", "person_person_type.csv")
            if os.path.exists(person_person_type_path):
                columns = ["personIdentifier","personType"]  
                cursor = load_table_once(
                    cursor,
                    person_person_type_path,
                    "person_person_type",
                    columns,
                    already_loaded_tables=set()
                )

        # Enable keys after loading is completed
        for table in all_tables:
            sql = f"ALTER TABLE `{table}` ENABLE KEYS;"
            try:
                cursor = execute_with_reconnect(cursor, sql)
            except Exception as e:
                logger.warning(f"Could not enable keys on {table}: {e}")

        # Now that all data is loaded, if we have person_temp, we can finally update_person
        if not skip_person_temp:
            cursor = update_person(cursor)

        connection.commit()

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            connection = None
            logger.info("Database connection closed after main().")


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