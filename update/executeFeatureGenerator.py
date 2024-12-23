# executeFeatureGenerator.py

import json
import os
import time
import requests
import logging
import pymysql
from datetime import datetime
import concurrent.futures  # <--- for concurrency

# ------------------------------------------------------------------------------
#  ENVIRONMENT VARIABLES
# ------------------------------------------------------------------------------
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
URL = os.environ['URL']
API_KEY = os.environ['API_KEY']

# ------------------------------------------------------------------------------
#  LOGGING
# ------------------------------------------------------------------------------
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
#  DATABASE CONNECTION
# ------------------------------------------------------------------------------
def connect_mysql_server(username, db_password, db_hostname, database_name):
    """
    Function to connect to MySQL database
    """
    try:
        mysql_db = pymysql.connect(
            user=username,
            password=db_password,
            database=database_name,
            host=db_hostname
        )
        logger.info(f"Connected to database server: {db_hostname}, database: {database_name}, user: {username}")
        return mysql_db
    except pymysql.err.MySQLError as err:
        logger.error(f"{time.ctime()} -- Error connecting to the database: {err}")

# ------------------------------------------------------------------------------
#  FETCH PERSON IDENTIFIERS
# ------------------------------------------------------------------------------
def get_person_identifier(mysql_cursor):
    """
    Get personIdentifiers from MySQL database based on frequency rules:
      - daily
      - weekly (only on Sunday)
      - monthly (only on day 7)
    """
    get_metadata_query = f"""
        SELECT DISTINCT personIdentifier
        FROM {DB_NAME}.reporting_ad_hoc_feature_generator_execution
        WHERE 
            (frequency = 'daily')
            OR (frequency = 'weekly' AND DAYOFWEEK(CURRENT_DATE) = 7)
            OR (frequency = 'monthly' AND DAY(CURRENT_DATE) = 7);
    """
    try:
        mysql_cursor.execute(get_metadata_query)
        person_identifiers = [rec[0] for rec in mysql_cursor]
        return person_identifiers
    except Exception as e:
        logger.exception(f"An error occurred while fetching person identifiers: {e}")
        return []

# ------------------------------------------------------------------------------
#  MAKE FEATURE-GENERATOR REQUEST
# ------------------------------------------------------------------------------
def make_curl_request(person_identifier):
    """
    Make a GET request to ReCiter's Feature Generator service for a given personIdentifier.
    
    - On the 1st day of the month, we request ALL_PUBLICATIONS.
    - On other days, ONLY_NEWLY_ADDED_PUBLICATIONS are requested.
    """
    retrieval_flag = "ONLY_NEWLY_ADDED_PUBLICATIONS" if datetime.now().day != 1 else "ALL_PUBLICATIONS"

    curl_url = (
        f"{URL}?uid={person_identifier}"
        f"&useGoldStandard=AS_EVIDENCE"
        f"&fields=reCiterArticleFeatures.pmid,personIdentifier,reCiterArticleFeatures.publicationDateStandardized"
        f"&analysisRefreshFlag=true&retrievalRefreshFlag={retrieval_flag}"
    )
    headers = {"accept": "application/json", "api-key": API_KEY}

    try:
        response = requests.get(curl_url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Response for {person_identifier}: {response.text}")
        else:
            logger.error(
                f"Failed to retrieve data for {person_identifier}. "
                f"HTTP Status: {response.status_code}. Response: {response.text}"
            )
    except Exception as e:
        logger.exception(
            f"An error occurred while making the request for {person_identifier}: {e}"
        )
    finally:
        logger.info("")  # Blank line in logs for readability

# ------------------------------------------------------------------------------
#  MAIN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Connect to the database
        mysql_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
        mysql_cursor = mysql_db.cursor()

        # Fetch all relevant personIdentifiers
        person_identifiers = get_person_identifier(mysql_cursor)
        logger.info(f"Total personIdentifiers to process: {len(person_identifiers)}")

        # Number of concurrent threads
        max_concurrency = 3

        # Submit requests to the thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            future_to_pid = {
                executor.submit(make_curl_request, pid): pid for pid in person_identifiers
            }

            # As each future completes, log any exceptions
            for future in concurrent.futures.as_completed(future_to_pid):
                pid = future_to_pid[future]
                try:
                    future.result()  # If there's an exception inside make_curl_request, re-raise it here
                except Exception as exc:
                    logger.exception(f"Error during request for {pid}: {exc}")

        logger.info("All requests have been processed.")
        
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
    finally:
        if mysql_db and mysql_db.open:
            mysql_db.close()
            logger.info("MySQL connection closed.")