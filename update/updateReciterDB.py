# updateReCiterDB.py

import pymysql
import os
import time

def execute_with_reconnect(cursor, sql):
    max_retries = 10  # Maximum number of retries
    retries = 0
    while retries < max_retries:
        try:
            cursor.execute(sql)
            return cursor  # Return the cursor if execution is successful
        except (pymysql.err.OperationalError, pymysql.err.InternalError, pymysql.err.InterfaceError, BrokenPipeError) as e:
            # Handle specific errors that indicate a lost connection
            if (isinstance(e, pymysql.err.OperationalError) and e.args[0] in (2006, 2013)) \
               or isinstance(e, (pymysql.err.InterfaceError, BrokenPipeError)):
                # 2006: MySQL server has gone away, 2013: Lost connection to MySQL server
                retries += 1
                wait_time = min(2 ** retries, 300)  # Exponential backoff up to 5 minutes
                print(f"Connection lost. Retrying ({retries}/{max_retries}) after {wait_time} seconds...")
                time.sleep(wait_time)
                try:
                    # Reconnect the connection
                    connection.ping(reconnect=True)
                    cursor = connection.cursor()
                except Exception as conn_e:
                    print(f"Error reconnecting: {conn_e}")
                    continue
            else:
                # For other exceptions, re-raise
                raise
    raise Exception("Failed to execute SQL after several retries.")

def establish_connection():
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            connection = pymysql.connect(
                host=DB_HOST,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                db=DB_NAME,
                local_infile=True,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10  # Optional: set a timeout for the connection attempt
            )
            print(f"Connected to database: {DB_NAME} at {DB_HOST}")
            return connection
        except pymysql.err.OperationalError as e:
            print(f"Error connecting to database: {e}, retrying ({retries + 1}/{max_retries})...")
            retries += 1
            wait_time = min(2 ** retries, 300)
            time.sleep(wait_time)
    raise Exception("Failed to establish database connection after several retries.")

def load_identity_temp(cursor, csv_file_path):
    sql = f"""
    LOAD DATA LOCAL INFILE '{csv_file_path}'
    INTO TABLE identity_temp
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    (personIdentifier, title, firstName, middleName, lastName, primaryEmail,
     primaryOrganizationalUnit, primaryInstitution);
    """
    print(f"Executing SQL:\n{sql}")
    cursor = execute_with_reconnect(cursor, sql)
    print(f"{time.ctime()} -- Loaded data into identity_temp from {csv_file_path}")
    return cursor

def update_person(cursor):
    update_query = """
    UPDATE person p
    JOIN identity_temp i ON i.personIdentifier = p.personIdentifier
    SET p.firstName = i.firstName,
        p.middleName = i.middleName,
        p.lastName = i.lastName,
        p.title = i.title,
        p.primaryEmail = i.primaryEmail,
        p.primaryOrganizationalUnit = i.primaryOrganizationalUnit,
        p.primaryInstitution = i.primaryInstitution;
    """
    cursor = execute_with_reconnect(cursor, update_query)
    print(f"{time.ctime()} -- person table updated with data from identity_temp table")
    return cursor

def load_csv_into_table(cursor, csv_file_path, table_name, columns):
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        columns_str = ', '.join(f'`{col}`' for col in columns)
        sql = f"""
        LOAD DATA LOCAL INFILE '{csv_file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        ({columns_str});
        """
        print(f"Executing SQL:\n{sql}")
        cursor = execute_with_reconnect(cursor, sql)
        print(f"Data loaded into {table_name} from {csv_file_path}")
    else:
        print(f"CSV file {csv_file_path} does not exist or is empty.")
    return cursor

def main(truncate_tables=True, skip_identity_temp=False):
    global connection  # Needed for execute_with_reconnect
    global DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME  # Needed for establish_connection

    # Retrieve environment variables
    DB_HOST = os.getenv("DB_HOST")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")

    if not all([DB_HOST, DB_USERNAME, DB_PASSWORD, DB_NAME]):
        raise EnvironmentError("One or more required database environment variables are missing.")

    # Establish database connection with retries
    connection = establish_connection()
    cursor = connection.cursor()

    # Truncate tables if required
    if truncate_tables:
        print("Truncating relevant tables...")
        tables_to_truncate = [
            'person',
            'person_article',
            'person_article_author',
            'person_article_department',
            'person_article_grant',
            'person_article_keyword',
            'person_article_relationship',
            'person_article_scopus_target_author_affiliation',
            'person_article_scopus_non_target_author_affiliation',
            'person_person_type'
        ]
        if not skip_identity_temp:
            tables_to_truncate.append('identity_temp')

        for table in tables_to_truncate:
            sql = f"TRUNCATE TABLE {table};"
            print(f"Executing: {sql}")
            cursor = execute_with_reconnect(cursor, sql)
        connection.commit()
        print("Tables truncated successfully.")

    # Load identity_temp if not skipped
    if not skip_identity_temp:
        # Ensure 'identity.csv' exists
        output_file = os.path.join('temp', 'parsedOutput', 'identity.csv')
        if not os.path.exists(output_file):
            raise FileNotFoundError(f"{output_file} not found. Ensure 'process_identity' has been run to generate it.")

        # Load CSV into identity_temp table
        cursor = load_identity_temp(cursor, output_file)

        # Update the person table with data from the temp table
        cursor = update_person(cursor)

    # Load additional tables incrementally
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
        'person_person_type.csv': 'person_person_type'
    }

    table_columns = {
        'person': ['personIdentifier', 'dateAdded', 'dateUpdated', 'precision', 'recall', 'countSuggestedArticles', 'countPendingArticles', 'overallAccuracy', 'mode'],
        'person_article': ['personIdentifier', 'pmid', 'authorshipLikelihoodScore', 'pmcid', 'totalArticleScoreStandardized', 'totalArticleScoreNonStandardized', 'userAssertion', 'publicationDateDisplay', 'publicationDateStandardized', 'publicationTypeCanonical', 'scopusDocID', 'journalTitleVerbose', 'articleTitle', 'articleAuthorNameFirstName', 'articleAuthorNameLastName', 'institutionalAuthorNameFirstName', 'institutionalAuthorNameMiddleName', 'institutionalAuthorNameLastName', 'nameMatchFirstScore', 'nameMatchFirstType', 'nameMatchMiddleScore', 'nameMatchMiddleType', 'nameMatchLastScore', 'nameMatchLastType', 'nameMatchModifierScore', 'nameScoreTotal', 'emailMatch', 'emailMatchScore', 'journalSubfieldScienceMetrixLabel', 'journalSubfieldScienceMetrixID', 'journalSubfieldDepartment', 'journalSubfieldScore', 'relationshipEvidenceTotalScore', 'relationshipMinimumTotalScore', 'relationshipNonMatchCount', 'relationshipNonMatchScore', 'articleYear', 'datePublicationAddedToEntrez', 'doi', 'issn', 'issue', 'journalTitleISOabbreviation', 'pages', 'timesCited', 'volume', 'feedbackScoreCites', 'feedbackScoreCoAuthorName', 'feedbackScoreEmail', 'feedbackScoreInstitution', 'feedbackScoreJournal', 'feedbackScoreJournalSubField', 'feedbackScoreKeyword', 'feedbackScoreOrcid', 'feedbackScoreOrcidCoAuthor', 'feedbackScoreOrganization', 'feedbackScoreTargetAuthorName', 'feedbackScoreYear'],
        'person_article_author': ['personIdentifier', 'pmid', 'authorFirstName', 'authorLastName', 'equalContrib', 'rank', 'orcid', 'targetAuthor'],
        'person_article_department': ['personIdentifier', 'pmid', 'identityOrganizationalUnit', 'articleAffiliation', 'organizationalUnitType', 'organizationalUnitMatchingScore', 'organizationalUnitModifier', 'organizationalUnitModifierScore'],
        'person_article_grant': ['personIdentifier', 'pmid', 'articleGrant', 'grantMatchScore', 'institutionGrant'],
        'person_article_keyword': ['personIdentifier', 'pmid', 'keyword'],
        'person_article_relationship': ['personIdentifier', 'pmid', 'relationshipNameArticleFirstName', 'relationshipNameArticleLastName', 'relationshipNameIdentityFirstName', 'relationshipNameIdentityLastName', 'relationshipType', 'relationshipMatchType', 'relationshipMatchingScore', 'relationshipVerboseMatchModifierScore', 'relationshipMatchModifierMentor', 'relationshipMatchModifierMentorSeniorAuthor', 'relationshipMatchModifierManager', 'relationshipMatchModifierManagerSeniorAuthor'],
        'person_article_scopus_target_author_affiliation': ['personIdentifier', 'pmid', 'targetAuthorInstitutionalAffiliationSource', 'scopusTargetAuthorInstitutionalAffiliationIdentity', 'targetAuthorInstitutionalAffiliationArticleScopusLabel', 'targetAuthorInstitutionalAffiliationArticleScopusAffiliationId', 'targetAuthorInstitutionalAffiliationMatchType', 'targetAuthorInstitutionalAffiliationMatchTypeScore'],
        'person_article_scopus_non_target_author_affiliation': ['personIdentifier', 'pmid', 'nonTargetAuthorInstitutionLabel', 'nonTargetAuthorInstitutionID', 'nonTargetAuthorInstitutionCount'],
        'person_person_type': ['personIdentifier', 'personType']
    }




    # Process each CSV file and load into the corresponding table
    for csv_file, table_name in csv_files.items():
        csv_file_path = os.path.join('temp', 'parsedOutput', csv_file)
        columns = table_columns.get(table_name)
        if columns is None:
            print(f"No columns defined for table {table_name}. Skipping.")
            continue
        try:
            cursor = load_csv_into_table(cursor, csv_file_path, table_name, columns)
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")
            log_error('N/A', f"Error loading data into {table_name}: {e}")
            continue

    # Commit and close
    connection.commit()
    cursor.close()
    connection.close()
    print(f"{time.ctime()} -- Database update completed successfully.")

def log_error(person_identifier, error_message):
    error_log_file = 'error.txt'
    with open(error_log_file, 'a') as f:
        f.write(f"PersonIdentifier: {person_identifier}, Error: {error_message}\n")

if __name__ == '__main__':
    # Explicitly call main with truncate_tables=True and skip_identity_temp=False
    main(truncate_tables=True, skip_identity_temp=False)
