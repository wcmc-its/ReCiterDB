#!/usr/bin/env python3

import os
import sys
import time
import json
import signal
import logging
import boto3
import pymysql

from functools import wraps
from botocore.exceptions import ClientError, EndpointConnectionError
from dynamodb_json import json_util as dynamodb_json

# Import your transformation and DB-update modules
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

# ------------------------------------------------------------------------------
#                              LOGGING SETUP
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
#                              GLOBAL VARIABLES
# ------------------------------------------------------------------------------
OUTPUT_PATH = 'temp/parsedOutput/'
DEFAULT_SEGMENT_COUNT = 20  # Can override via command line
DELETE_CSV_AFTER_PROCESSING = True

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ------------------------------------------------------------------------------
#                         TIMEOUT DECORATOR
# ------------------------------------------------------------------------------
def timeout(seconds=300, error_message="Operation timed out"):
    """
    Decorator to enforce a timeout on a function call using signal.alarm().
    Only works on Unix-like systems.
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

# ------------------------------------------------------------------------------
#                              SCAN + PROCESS
# ------------------------------------------------------------------------------
def scan_and_process_segment(segment, total_segments, filter_expr, expr_vals):
    """
    Scans and processes a single segment of the 'Analysis' table.
    1) Scans all items for this segment.
    2) Processes them immediately (CSV transform + upload).
    """
    dynamodb = boto3.client("dynamodb")
    logger.info(f"Starting scan for segment {segment}/{total_segments - 1}")
    
    items = []
    last_evaluated_key = None
    
    while True:
        scan_kwargs = {
            "TableName": "Analysis",
            "Segment": segment,
            "TotalSegments": total_segments,
            "FilterExpression": filter_expr,
            "ExpressionAttributeValues": expr_vals,
        }
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        try:
            response = dynamodb.scan(**scan_kwargs)
            raw_items = response.get("Items", [])
            items.extend(raw_items)
            last_evaluated_key = response.get("LastEvaluatedKey", None)

            # If there's no more data to scan, break
            if not last_evaluated_key:
                logger.info(f"Segment {segment} scan complete. Fetched {len(items)} items.")
                break

        except (ClientError, EndpointConnectionError) as e:
            logger.error(f"Error scanning segment {segment}: {e}")
            return  # Return without processing further
        except Exception as e:
            logger.error(f"Unexpected error scanning segment {segment}: {e}")
            return

    # Now that we have this segment's items, process them
    process_records(items)

@timeout(300, "CSV cleanup operation timed out")
def cleanup_csv_files():
    """
    Cleanup CSV files in the output directory. Skips identity.csv if present.
    """
    for csv_file in os.listdir(OUTPUT_PATH):
        if csv_file != "identity.csv":
            csv_file_path = os.path.join(OUTPUT_PATH, csv_file)
            if os.path.isfile(csv_file_path):
                os.remove(csv_file_path)
    logger.info(f"Cleaned up generated CSV files in {OUTPUT_PATH}.")

def process_records(records):
    """
    Processes the scanned items for a single segment:
    1) Convert to Python dict
    2) Extract 'reCiterFeature'
    3) Run transformations
    4) Load into DB
    """
    logger.info(f"Processing {len(records)} records.")
    extracted_records = []

    for record in records:
        python_record = dynamodb_json.loads(record)
        reCiterFeature = python_record.get("reCiterFeature")
        if reCiterFeature:
            extracted_records.append(reCiterFeature)
    
    if not extracted_records:
        logger.warning("No valid 'reCiterFeature' found in this segment. Skipping.")
        return

    # ------------------- Transform to CSVs -------------------
    process_person(extracted_records, OUTPUT_PATH)
    process_person_article(extracted_records, OUTPUT_PATH)
    process_person_article_author(extracted_records, OUTPUT_PATH)
    process_person_article_department(extracted_records, OUTPUT_PATH)
    process_person_article_grant(extracted_records, OUTPUT_PATH)
    process_person_article_keyword(extracted_records, OUTPUT_PATH)
    process_person_article_relationship(extracted_records, OUTPUT_PATH)
    process_person_article_scopus_target_author_affiliation(extracted_records, OUTPUT_PATH)
    process_person_article_scopus_non_target_author_affiliation(extracted_records, OUTPUT_PATH)

    # ------------------- Load CSVs into DB -------------------
    # We do NOT truncate each time, so data accumulates. If you need a full rebuild,
    # run updateReciterDB.main(truncate_tables=True) once at the start or end.
    retries = 0
    while retries < 5:
        try:
            logger.info("Uploading CSV data to the DB...")
            updateReciterDB.main(truncate_tables=False, skip_person_temp=True)
            updateReciterDB.call_update_person_only()
            break
        except Exception as e:
            retries += 1
            logger.warning(f"Database update failed: {e}. Retrying {retries}/5...")
            time.sleep(2 ** retries)

    # ------------------- Cleanup CSV files -------------------
    if DELETE_CSV_AFTER_PROCESSING:
        cleanup_csv_files()

# ------------------------------------------------------------------------------
#                                MAIN
# ------------------------------------------------------------------------------
def main():
    """
    Main driver:
    1) Determines how many segments to process (default 8, or from CLI).
    2) Iterates over each segment, scanning DynamoDB for 'usingS3=0'.
    3) Processes each segment's items into CSV -> Loads into DB -> Cleans up CSV.
    """
    segment_count = DEFAULT_SEGMENT_COUNT
    if len(sys.argv) > 1:
        try:
            segment_count = int(sys.argv[1])
        except ValueError:
            logger.error("Please provide an integer value for segment_count.")
            sys.exit(1)

    logger.info(f"Starting segmented scan of 'Analysis' table with {segment_count} segments.")
    filter_expr = "usingS3 = :val"
    expr_vals = {":val": {"N": "0"}}

    for segment in range(segment_count):
        logger.info(f"Processing segment {segment}/{segment_count - 1}")
        scan_and_process_segment(segment, segment_count, filter_expr, expr_vals)

    logger.info("All segments processed. Script completed.")

if __name__ == "__main__":
    main()
