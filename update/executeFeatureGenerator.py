import json
import os
import time
import requests
import logging
import pymysql
import psutil
import boto3			 
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread


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

# ------------------------------
# Configuration
# ------------------------------
MAX_WORKERS = 10                 # Safe level of concurrency per pod
REQUESTS_PER_SECOND = 3          # Global rate limit

# S3 logging configuration
S3_BUCKET = "reciterdbcrondevlogs"
S3_LOG_PREFIX = "logs/featuregeneratorapi-job/"       # Folder path in bucket
LOCAL_LOG_FILE = "/tmp/featuregeneratorapi.log"  # Local log file path

# Metrics settings
METRIC_INTERVAL = 10  # seconds between CPU/mem reports

# AWS clients
s3_client = boto3.client("s3")


# Rate limiter state
_last_request_time = 0
_rate_lock = Lock()


# Database and API credentials from environment variables
DB_USERNAME = os.environ['DB_USERNAME']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
URL = os.environ['URL']
API_KEY = os.environ['API_KEY']

# Configure logging to output to command line
#logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
#logger = logging.getLogger(__name__)

# ------------------------------
# Logging Setup (writes to file)
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOCAL_LOG_FILE),  # Write logs to file
        logging.StreamHandler()               # Also print to console
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------
# Upload Logs to S3
# ------------------------------
def upload_log_to_s3():
    """Upload log file to S3."""
    if not os.path.exists(LOCAL_LOG_FILE):
        logger.warning("Local log file does not exist; skipping S3 upload.")
        return

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"{S3_LOG_PREFIX}{timestamp}.log"

    try:
        s3_client.upload_file(LOCAL_LOG_FILE, S3_BUCKET, s3_key)
        logger.info(f"Uploaded logs to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload logs to S3: {e}")


# ------------------------------
# CPU & Memory Metrics Thread
# ------------------------------
def metrics_loop():
    """Runs in background, logs CPU & memory every METRIC_INTERVAL seconds."""
    while True:
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()

        logger.info(
            f"[METRICS] CPU={cpu}% | Memory={mem.percent}% "
            f"({mem.used/1024/1024:.1f} MB used)"
        )

        time.sleep(METRIC_INTERVAL)

# ------------------------------
# start_metrics_collector function
# ------------------------------

def start_metrics_collector():
    t = Thread(target=metrics_loop, daemon=True)
    t.start()


# ------------------------------
# Rate Limiter
# ------------------------------
def rate_limited():
    """Ensures only REQUESTS_PER_SECOND outbound API calls."""
    global _last_request_time
    with _rate_lock:
        gap = 1 / REQUESTS_PER_SECOND
        now = time.time()
        elapsed = now - _last_request_time

        if elapsed < gap:
            time.sleep(gap - elapsed)

        _last_request_time = time.time()
		
# ------------------------------
# Connect to MySQL
# ------------------------------
		
def connect_mysql_server(username, db_password, db_hostname, database_name):
    """Function to connect to MySQL database"""
    try:
        mysql_db = pymysql.connect(user=username,
                                   password=db_password,
                                   database=database_name,
                                   host=db_hostname)
        logger.info(f"Connected to database server: {db_hostname}, database: {database_name}, with user: {username}")
        return mysql_db
    except pymysql.err.MySQLError as err:
        logger.error(f"{time.ctime()} -- Error connecting to the database: {err}")

# ------------------------------
# Fecthing PersonIdentifier
# ------------------------------

def get_person_identifier(mysql_cursor):
    """Get personIdentifiers from MySQL database"""
    get_metadata_query = (
        """
        SELECT DISTINCT personIdentifier
        FROM """ + DB_NAME + """.reporting_ad_hoc_feature_generator_execution limit 2000;
        #WHERE (frequency = 'daily') OR (frequency = 'weekly' AND DAYOFWEEK(CURRENT_DATE) = 1) OR (frequency = 'monthly' AND DAY(CURRENT_DATE) = 1);
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
		

# ------------------------------
# API Request Function
# ------------------------------
def make_curl_request(person_identifier):
    """Make a safe API request for each person_identifier."""
    rate_limited()

    retrieval_flag = (
        "ONLY_NEWLY_ADDED_PUBLICATIONS"
        if datetime.now().day != 1
        else "ALL_PUBLICATIONS"
    )

    curl_url = (
        f"{URL}?uid={person_identifier}"
        f"&useGoldStandard=AS_EVIDENCE"
        f"&fields=reCiterArticleFeatures.pmid,personIdentifier,"
        f"reCiterArticleFeatures.publicationDateStandardized"
        f"&analysisRefreshFlag=true"
        f"&retrievalRefreshFlag={retrieval_flag}"
    )

    headers = {
        "accept": "application/json",
        "api-key": API_KEY
    }

    try:
        response = requests.get(curl_url, headers=headers, timeout=60)

        if response.status_code == 200:
            logger.info(
                f"[{person_identifier}] Success: {len(response.text)} bytes received"
            )
        else:
            logger.error(
                f"[{person_identifier}] Failed with status {response.status_code}: {response.text}"
            )

    except Exception as e:
        logger.exception(
            f"[{person_identifier}] Exception occurred: {e}"
        )

    return True

# ------------------------------
# Main Execution
# ------------------------------
def main():
    logger.info("Starting Feature Generator Script.")
    start_metrics_collector()  # start CPU/memory monitoring

    try:
        mysql_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
        mysql_cursor = mysql_db.cursor()
        person_identifiers = get_person_identifier(mysql_cursor)

        logger.info(f"Total person identifiers: {len(person_identifiers)}")
        logger.info(f"Using {MAX_WORKERS} workers, rate limit {REQUESTS_PER_SECOND} req/sec")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(make_curl_request, person_identifiers)

        logger.info("Processing complete for Feature Generator.")

    except Exception as e:
        logger.exception(f"Unexpected error in main(): {e}")

    finally:
        upload_log_to_s3()
        logger.info("Uploaded logs to S3.")


if __name__ == "__main__":
    main()