import os
import sys
import time
import json
import boto3
from dynamodb_json import json_util as dynamodb_json
import logging
from botocore.exceptions import ClientError, EndpointConnectionError
import pymysql
import signal
from functools import wraps

from dataTransformer import (
    process_person,
    process_person_article,
    process_person_article_author,
    process_person_article_department,
    process_person_article_grant,
    process_person_article_keyword,
    process_person_article_relationship,
    process_person_article_scopus_target_author_affiliation,
    process_person_article_scopus_non_target_author_affiliation,
)
import updateReciterDB

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

outputPath = 'temp/parsedOutput/'
default_batch_size = 500
delete_csv_after_processing = True
os.makedirs(outputPath, exist_ok=True)

# --------------------- Timeout Decorator ---------------------
def timeout(seconds=300, error_message="Operation timed out"):
    """
    Decorator to enforce a timeout on a function call using signal.alarm().
    Only works on Unix-like systems.

    Usage:
        @timeout(300)
        def long_running_operation():
            # ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def handler(signum, frame):
                raise TimeoutError(error_message)
            original_handler = signal.signal(signal.SIGALRM, handler)
            signal.alarm(seconds)
            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, original_handler)
        return wrapper
    return decorator

def check_for_duplicates():
    """
    Connect to the database and run queries to check for duplicates in person
    and person_article tables. If duplicates are found, log the occurrences and
    raise an exception to halt the script.
    """
    connection = None
    try:
        connection = pymysql.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        cursor = connection.cursor()

        duplicate_check_queries = {
            'person': """
                SELECT BINARY personIdentifier, COUNT(*) as count
                FROM person
                GROUP BY BINARY personIdentifier
                HAVING count > 1
            """,
            'person_article': """
                SELECT BINARY personIdentifier, pmid, COUNT(*) as count
                FROM person_article
                GROUP BY BINARY personIdentifier, pmid
                HAVING count > 1
            """
        }

        duplicates_found = False

        for table_name, query in duplicate_check_queries.items():
            cursor.execute(query)
            results = cursor.fetchall()
            if results:
                duplicates_found = True
                logger.error(f"Duplicate entries found in {table_name} table:")
                for row in results:
                    logger.error(row)

        cursor.close()

        if duplicates_found:
            raise Exception("Duplicate entries detected. Halting script for investigation.")

    except Exception as e:
        logger.error(f"Error while checking for duplicates: {e}")
        raise
    finally:
        if connection:
            connection.close()

@timeout(600, "Processing batch took too long and timed out")
def process_batch(batch, batch_number):
    """
    Given a list of items from DynamoDB, extract 'reCiterFeature' from each,
    then run them through various dataTransformer functions and update the DB.
    Check for duplicates after updating.
    This function is now wrapped with a timeout.
    """
    logger.info(f"Processing batch {batch_number} with {len(batch)} total records.")

    extracted_records = []
    for record in batch:
        reCiterFeature = record.get('reCiterFeature')
        if reCiterFeature:
            extracted_records.append(reCiterFeature)
        else:
            logger.debug(f"Record without 'reCiterFeature': {record}")

    if not extracted_records:
        logger.warning(f"No valid 'reCiterFeature' found in batch {batch_number}. Skipping.")
        return

    # Run the transformations to CSV
    process_person(extracted_records, outputPath)
    process_person_article(extracted_records, outputPath)
    process_person_article_author(extracted_records, outputPath)
    process_person_article_department(extracted_records, outputPath)
    process_person_article_grant(extracted_records, outputPath)
    process_person_article_keyword(extracted_records, outputPath)
    process_person_article_relationship(extracted_records, outputPath)
    process_person_article_scopus_target_author_affiliation(extracted_records, outputPath)
    process_person_article_scopus_non_target_author_affiliation(extracted_records, outputPath)

    # After CSV files are created, load them into DB (skip loading person_temp)
    retries = 0
    while retries < 5:
        try:
            updateReciterDB.main(truncate_tables=False, skip_person_temp=True)
            updateReciterDB.call_update_person_only()
            break
        except Exception as e:
            retries += 1
            logger.warning(f"Database update failed: {e}. Retrying {retries}/5...")
            time.sleep(2 ** retries)

    # Check for duplicates
    check_for_duplicates()

    # Clean up CSV files
    if delete_csv_after_processing:
        cleanup_csv_files()

@timeout(300, "CSV cleanup operation timed out")        
def cleanup_csv_files():
    """
    Cleanup CSV files in the output directory with a timeout.
    """
    for csv_file in os.listdir(outputPath):
        if csv_file != "identity.csv":
            csv_file_path = os.path.join(outputPath, csv_file)
            if os.path.isfile(csv_file_path):
                os.remove(csv_file_path)
    logger.info(f"Cleaned up generated CSV files in {outputPath}.")


def fetch_records_in_batches(table_name, batch_size=500, max_retries=5):
    """
    Generator that scans DynamoDB table 'Analysis' for items with usingS3=0.
    Yields a batch of 'batch_size' items each time, then continues scanning until no more items.

    No checkpoints. If the process is interrupted, it starts from the beginning next time.
    Includes a timeout for each scan operation.
    """
    dynamodb = boto3.client("dynamodb")
    last_evaluated_key = None

    while True:
        scan_kwargs = {
            "TableName": table_name,
            "Limit": batch_size,
            "FilterExpression": "usingS3 = :val",
            "ExpressionAttributeValues": {":val": {"N": "0"}},
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        attempt = 0
        while attempt < max_retries:
            try:
                # Wrap the scan in a timeout as well
                response = timed_scan(dynamodb, scan_kwargs)
                raw_items = response.get("Items", [])
                if not raw_items:
                    logger.info("No more items to process. Ending scan.")
                    return

                transformed_items = [dynamodb_json.loads(item) for item in raw_items]
                logger.info(f"Fetched and transformed {len(transformed_items)} records.")

                yield transformed_items

                last_evaluated_key = response.get("LastEvaluatedKey", None)
                if not last_evaluated_key:
                    logger.info("All items have been processed. Scan complete.")
                    return

                break  # Break out of retry loop on success

            except (ClientError, EndpointConnectionError, TimeoutError) as e:
                attempt += 1
                wait_time = min(2 ** attempt, 300)
                logger.warning(f"Error scanning table: {e}. Attempt {attempt}/{max_retries}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            except Exception as e:
                logger.error(f"Unexpected error scanning table: {e}")
                return  # Stop scanning if unknown error

@timeout(300, "DynamoDB scan operation timed out")
def timed_scan(dynamodb, scan_kwargs):
    """
    Perform the DynamoDB scan with a timeout enforced.
    """
    return dynamodb.scan(**scan_kwargs)

def main():
    table_name = "Analysis"
    total_items_processed = 0
    batch_number = 0

    batch_size = default_batch_size
    if len(sys.argv) > 1:
        try:
            batch_size = int(sys.argv[1])
        except ValueError:
            logger.error("Please provide an integer value for batch_size.")
            sys.exit(1)

    logger.info("Starting scan from the beginning of the table (no checkpoints).")

    for batch in fetch_records_in_batches(table_name, batch_size=batch_size):
        batch_number += 1
        try:
            process_batch(batch, batch_number)
            total_items_processed += len(batch)
        except TimeoutError as te:
            logger.error(f"Timeout occurred: {te} for batch {batch_number}. Skipping this batch.")
            continue
        except Exception as e:
            logger.error(f"Error processing batch {batch_number}: {e}. Skipping this batch.")
            continue

    logger.info(f"Completed processing. Total items processed: {total_items_processed}")

if __name__ == '__main__':
    main()