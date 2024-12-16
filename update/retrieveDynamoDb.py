# retrieveDynamoDB.py

"""

Rewritten to use DynamoDB's LastEvaluatedKey approach.
This script scans the "Analysis" table for items where usingS3=0,
in increments of 'max_objects_per_batch' items. It processes them
in batches, writes out CSV files via dataTransformer, then updates
the Reciter DB.

We store the actual DynamoDB LastEvaluatedKey in 'checkpoint.json'
to avoid re-scanning items on re-runs.

Usage:
    python3 retrieveDynamoDB.py [optional_batch_size]

Example:
    python3 retrieveDynamoDB.py        # uses default batch size (500)
    python3 retrieveDynamoDB.py 1000   # override batch size to 1000
"""

import os
import sys
import time
import json
import boto3
from dynamodb_json import json_util as dynamodb_json
import logging
from botocore.exceptions import ClientError, EndpointConnectionError

# Import your data transformation and DB update modules
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

# Paths and Global Config
outputPath = 'temp/parsedOutput/'
checkpoint_file = "checkpoint.json"
# We used to do 10 items at a time; let's default to something larger, e.g. 500
# so the script doesn't take hours for bigger datasets.
default_batch_size = 500

delete_csv_after_processing = True
os.makedirs(outputPath, exist_ok=True)

# ------------------------------------------------------------------------
# Checkpoint Handling: store entire LastEvaluatedKey, not just "last_uid"
# ------------------------------------------------------------------------
def save_checkpoint(last_evaluated_key):
    """
    Save the DynamoDB 'LastEvaluatedKey' dict to a local JSON file so that
    we can resume scanning exactly where we left off.
    """
    with open(checkpoint_file, "w") as f:
        json.dump({"LastEvaluatedKey": last_evaluated_key}, f)
    logger.info(f"Checkpoint saved: LastEvaluatedKey={last_evaluated_key}")

def load_checkpoint():
    """
    Load the checkpoint file to retrieve the stored LastEvaluatedKey.
    Returns None if no checkpoint file exists, meaning start from the beginning.
    """
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            data = json.load(f)
        last_key = data.get("LastEvaluatedKey", None)
        return last_key
    return None

# ------------------------------------------------------------------------
# DynamoDB Scanning Logic
# ------------------------------------------------------------------------
def fetch_records_in_batches(table_name, batch_size=500, max_retries=5):
    """
    Generator that scans DynamoDB table 'Analysis' for items with usingS3=0.
    Yields a batch of 'batch_size' items each time. Uses LastEvaluatedKey
    checkpoint logic so we don't re-scan duplicates across runs.
    """
    dynamodb = boto3.client("dynamodb")
    last_evaluated_key = load_checkpoint()  # Start from previous checkpoint if exists

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
                response = dynamodb.scan(**scan_kwargs)
                raw_items = response.get("Items", [])
                if not raw_items:
                    logger.info("No more items to process. Ending scan.")
                    return

                # Transform the raw DynamoDB JSON items into normal dicts
                transformed_items = [dynamodb_json.loads(item) for item in raw_items]
                logger.info(f"Fetched and transformed {len(transformed_items)} records.")

                # Yield this batch to the caller
                yield transformed_items

                # Update LastEvaluatedKey from response
                last_evaluated_key = response.get("LastEvaluatedKey", None)
                save_checkpoint(last_evaluated_key)

                if not last_evaluated_key:
                    logger.info("All items have been processed. Scan complete.")
                    return

                # Break out of the retry loop if success
                break

            except (ClientError, EndpointConnectionError) as e:
                attempt += 1
                logger.warning(f"Error scanning table: {e}. Attempt {attempt}/{max_retries}. Retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error scanning table: {e}")
                return  # Stop scanning if unknown error

# ------------------------------------------------------------------------
# Processing Logic
# ------------------------------------------------------------------------
def process_batch(batch, batch_number):
    """
    Given a list of items from DynamoDB, extract 'reCiterFeature' from each,
    then run them through various dataTransformer functions and update the DB.
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

    # Optionally delete the CSVs to avoid clutter
    if delete_csv_after_processing:
        for csv_file in os.listdir(outputPath):
            if csv_file != "identity.csv":
                csv_file_path = os.path.join(outputPath, csv_file)
                if os.path.isfile(csv_file_path):
                    os.remove(csv_file_path)
        logger.info(f"Cleaned up generated CSV files in {outputPath}.")

# ------------------------------------------------------------------------
# Main Script
# ------------------------------------------------------------------------
def main():
    table_name = "Analysis"
    total_items_processed = 0
    batch_number = 0

    # Determine batch size from sys.argv or fallback to our default
    batch_size = default_batch_size
    if len(sys.argv) > 1:
        try:
            batch_size = int(sys.argv[1])
        except ValueError:
            logger.error("Please provide an integer value for batch_size.")
            sys.exit(1)

    # If you want to start fresh from the beginning, simply delete checkpoint.json
    if os.path.exists(checkpoint_file):
        logger.info(f"Checkpoint file {checkpoint_file} found. Resuming from stored LastEvaluatedKey.")
    else:
        logger.info("No checkpoint file found. Starting scan from the beginning of the table.")

    # Fetch and process the data in chunks
    for batch in fetch_records_in_batches(table_name, batch_size=batch_size):
        batch_number += 1
        try:
            process_batch(batch, batch_number)
            total_items_processed += len(batch)
        except Exception as e:
            logger.error(f"Error processing batch {batch_number}: {e}. Continuing anyway.")

    logger.info(f"Completed processing. Total items processed: {total_items_processed}")

if __name__ == '__main__':
    main()