import json
import os
import time
import requests
import logging
import pymysql
from datetime import datetime

## This script is a lightweight way for using person ID's from ReCiterDB to run Feature Generator (which suggests new 
## publications). This way you don't have to bother your developer if you want to add some new people.
## Here's how it works. To do so, it first retrieves personIdentifiers from the "reporting_ad_hoc_feature_execution" table 
## in ReCiterDB.  Depending on the value of the frequency attribute ("daily", "weekly", "monthly"), the tool will make a call 
## to ReCiter Feature Generator. Note that on a monthly basis, this script asks Feature Generator to retrieve
## updated versions of publications.

## Note that this script expects two values as environmental variables that 
## aren't used by other scripts: URL and API_KEY. 
## The URL attribute has the form: https://myDomain.edu/reciter/feature-generator/by/uid?uid
## The API_KEY attribute is what you would use in ReCiter Swagger to run Feature Generator.


# Database and API credentials from environment variables
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
URL = os.environ['URL']
API_KEY = os.environ['API_KEY']

# Configure logging to output to command line
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_mysql_server(username, db_password, db_hostname, database_name):
    """Function to connect to MySQL database"""
    try:
        mysql_db = pymysql.connect(user=DB_USERNAME,
                                   password=DB_PASSWORD,
                                   database=DB_NAME,
                                   host=DB_HOST)
        logger.info(f"Connected to database server: {DB_HOST}, database: {DB_NAME}, with user: {DB_USERNAME}")
        return mysql_db
    except pymysql.err.MySQLError as err:
        logger.error(f"{time.ctime()} -- Error connecting to the database: {err}")

def get_person_identifier(mysql_cursor):
    """Get personIdentifiers from MySQL database"""
    get_metadata_query = (
        """
        SELECT DISTINCT personIdentifier
        FROM """ + DB_NAME + """.reporting_ad_hoc_feature_generator_execution
        WHERE (frequency = 'daily') OR (frequency = 'weekly' AND DAYOFWEEK(CURRENT_DATE) = 7) OR (frequency = 'monthly' AND DAY(CURRENT_DATE) = 7);
        """
    )
    try:
        mysql_cursor.execute(get_metadata_query)
        person_identifier = list()
        for rec in mysql_cursor:
            person_identifier.append(rec[0])
        return person_identifier
    except Exception as e:
        logger.exception(f"An error occurred while fetching person identifiers: {e}")

def make_curl_request(person_identifier):
    """Make curl request for each personIdentifier"""
    retrieval_flag = "ONLY_NEWLY_ADDED_PUBLICATIONS" if datetime.now().day != 1 else "ALL_PUBLICATIONS"
    curl_url = f"{URL}?uid={person_identifier}&useGoldStandard=AS_EVIDENCE&fields=reCiterArticleFeatures.pmid,personIdentifier,reCiterArticleFeatures.publicationDateStandardized&analysisRefreshFlag=true&retrievalRefreshFlag={retrieval_flag}"
    headers = {"accept": "application/json", "api-key": API_KEY}
    try:
        response = requests.get(curl_url, headers=headers)
        if response.status_code == 200:
            logger.info(f"Response for person identifier {person_identifier}: {response.text}")
        else:
            logger.error(f"Failed to retrieve data for person identifier {person_identifier}. HTTP Status Code: {response.status_code}. Response: {response.text}")
    except Exception as e:
        logger.exception(f"An error occurred while making the curl request for person identifier {person_identifier}: {e}")
    logger.info('')  # Output an empty line to the log

if __name__ == "__main__":
    try:
        mysql_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
        mysql_cursor = mysql_db.cursor()
        person_identifiers = get_person_identifier(mysql_cursor)
        for person_identifier in person_identifiers:
            make_curl_request(person_identifier)
            time.sleep(2)
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
