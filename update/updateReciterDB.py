# updateReCiterDB.py  

import pymysql
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry parameters
MAX_RETRIES = 10
RETRY_WAIT_MAX = 300  # Maximum wait time in seconds for exponential backoff

def execute_with_reconnect(cursor, sql):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            cursor.execute(sql)
            return cursor
        except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.InterfaceError, BrokenPipeError) as e:
            if (isinstance(e, pymysql.err.OperationalError) and e.args[0] in (2006, 2013)) \
               or isinstance(e, (pymysql.err.InterfaceError, BrokenPipeError)):
                # Connection lost, attempt reconnect
                retries += 1
                wait_time = min(2 ** retries, RETRY_WAIT_MAX)
                logger.warning(f"Connection lost. Retrying ({retries}/{MAX_RETRIES}) in {wait_time} seconds...")
                time.sleep(wait_time)
                try:
                    connection.ping(reconnect=True)
                    cursor = connection.cursor()
                except Exception as reconnect_error:
                    logger.error(f"Error reconnecting: {reconnect_error}")
                    continue
            else:
                raise
    raise Exception("Failed to execute SQL after several retries.")

def establish_connection():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            conn = pymysql.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                db=os.getenv("DB_NAME"),
                local_infile=True,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            logger.info("Database connection established successfully.")
            return conn
        except pymysql.err.OperationalError as e:
            retries += 1
            wait_time = min(2 ** retries, RETRY_WAIT_MAX)
            logger.warning(f"Database connection failed: {e}. Retrying ({retries}/{MAX_RETRIES}) in {wait_time} seconds...")
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
    cursor.execute("SELECT COUNT(*) AS row_count FROM person_temp;")
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

    try:
        cursor = execute_with_reconnect(cursor, sql)
        cursor.execute(f"SELECT COUNT(*) AS row_count FROM {table_name};")
        row_count = cursor.fetchone()['row_count']
        logger.info(f"Data successfully loaded into {table_name}. Row count: {row_count}")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}")
        raise

    already_loaded_tables.add(table_name)
    return cursor

def main(truncate_tables=True, skip_person_temp=False):
    global connection
    connection = establish_connection()
    cursor = connection.cursor()

    try:
        logger.info(f"Inside main(): truncate_tables={truncate_tables}, skip_person_temp={skip_person_temp}")
        if truncate_tables:
            tables_to_truncate = [
                'person', 'person_article', 'person_article_author',
                'person_article_department', 'person_article_grant',
                'person_article_keyword', 'person_article_relationship',
                'person_article_scopus_target_author_affiliation',
                'person_article_scopus_non_target_author_affiliation',
                'person_person_type'
            ]
            if not skip_person_temp:
                tables_to_truncate.append('person_temp')

            for table in tables_to_truncate:
                sql = f"TRUNCATE TABLE {table};"
                cursor = execute_with_reconnect(cursor, sql)
            connection.commit()

        # ---------------------------------------------------------
        # (A) Load *all the other CSVs except* person_temp/person_person_type
        # ---------------------------------------------------------
        already_loaded_tables = set()
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
            # 'person_person_type.csv': 'person_person_type'  <--- REMOVE THIS so it's not loaded multiple times
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
            'person_person_type': ['personIdentifier', 'personType']
        }

        for csv_file, table_name in csv_files.items():
            csv_file_path = os.path.join('temp', 'parsedOutput', csv_file)
            if table_name not in table_columns:
                logger.warning(f"No columns defined for {table_name}. Skipping load.")
                continue
            cursor = load_table_once(cursor, csv_file_path, table_name, table_columns[table_name], already_loaded_tables)

        # ---------------------------------------------------------
        # (B) Load person_temp and person_person_type *once*, if needed
        # ---------------------------------------------------------
        if not skip_person_temp:
            # TRUNCATE person_temp, person_person_type once if needed
            # Load person_temp.csv once
            cursor = load_person_temp(cursor, "temp/parsedOutput/person_temp.csv")

            # Load person_person_type.csv once
            # if not in the dictionary, you can manually do:
            if os.path.exists("temp/parsedOutput/person_person_type.csv"):
                columns = ["personIdentifier","personType"]  # adjust as needed
                cursor = load_table_once(
                    cursor,
                    os.path.join("temp", "parsedOutput", "person_person_type.csv"),
                    "person_person_type",
                    columns,
                    already_loaded_tables=set()
                )

            # Call update_person() last
            cursor = update_person(cursor)

        connection.commit()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()

def call_update_person_only():
    """
    Connect to the DB, then call update_person().
    This does NOT load person_temp or person_person_type.
    """
    global connection
    connection = establish_connection()
    cursor = connection.cursor()
    try:
        logger.info("Calling update_person ONLY, without loading person_temp...")
        cursor = update_person(cursor)  # uses the existing person_temp table (whatever is currently in it)
        connection.commit()
    except Exception as e:
        logger.error(f"Error in call_update_person_only: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
        logger.info("Finished update_person only.")

if __name__ == '__main__':
    main(truncate_tables=True, skip_person_temp=False)