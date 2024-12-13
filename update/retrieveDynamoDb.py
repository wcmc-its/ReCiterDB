# retrieveDynamoDB.py

import os
import sys
import time
import json
import boto3
from dynamodb_json import json_util as dynamodb_json
import logging
from botocore.exceptions import ClientError, EndpointConnectionError
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

outputPath = 'temp/parsedOutput/'
checkpoint_file = "checkpoint.json"
max_objects_per_batch = 100
delete_csv_after_processing = True

os.makedirs(outputPath, exist_ok=True)

def save_checkpoint(last_uid):
    with open(checkpoint_file, "w") as f:
        json.dump({"last_uid": last_uid}, f)

def load_checkpoint():
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return json.load(f).get("last_uid", None)
    return None

def fetch_records_in_batches(table_name, batch_size=100, max_retries=5):
    dynamodb = boto3.client("dynamodb")
    last_evaluated_key = None

    while True:
        attempt = 0
        while attempt < max_retries:
            try:
                scan_kwargs = {
                    "TableName": table_name,
                    "Limit": batch_size,
                    "FilterExpression": "usingS3 = :val",
                    "ExpressionAttributeValues": {":val": {"N": "0"}},
                }
                if last_evaluated_key:
                    scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

                response = dynamodb.scan(**scan_kwargs)
                raw_items = response.get("Items", [])
                transformed_items = [dynamodb_json.loads(item) for item in raw_items]

                if not transformed_items:
                    logger.info("No more items to process.")
                    return

                logger.info(f"Fetched and transformed {len(transformed_items)} records.")
                yield transformed_items

                last_evaluated_key = response.get("LastEvaluatedKey", None)
                if not last_evaluated_key:
                    logger.info("All items have been processed.")
                    return
                break
            except (ClientError, EndpointConnectionError) as e:
                attempt += 1
                logger.warning(f"Error fetching records: {e}. Attempt {attempt} of {max_retries}. Retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return

def process_batch(batch, batch_number):
    logger.info(f"Processing batch {batch_number} with {len(batch)} records.")
    extracted_records = []
    for record in batch:
        reCiterFeature = record.get('reCiterFeature')
        if reCiterFeature:
            extracted_records.append(reCiterFeature)

    if not extracted_records:
        logger.warning(f"No valid 'reCiterFeature' records found in batch {batch_number}. Skipping.")
        return

    process_person(extracted_records, outputPath)
    process_person_article(extracted_records, outputPath)
    process_person_article_author(extracted_records, outputPath)
    process_person_article_department(extracted_records, outputPath)
    process_person_article_grant(extracted_records, outputPath)
    process_person_article_keyword(extracted_records, outputPath)
    process_person_article_relationship(extracted_records, outputPath)
    process_person_article_scopus_target_author_affiliation(extracted_records, outputPath)
    process_person_article_scopus_non_target_author_affiliation(extracted_records, outputPath)

    retries = 0
    while retries < 5:
        try:
            updateReciterDB.main(truncate_tables=False, skip_person_temp=True)
            break
        except Exception as e:
            retries += 1
            logger.warning(f"Database update failed: {e}. Retrying ({retries}/5)...")
            time.sleep(2 ** retries)

    if delete_csv_after_processing:
        for csv_file in os.listdir(outputPath):
            if csv_file != "identity.csv":
                csv_file_path = os.path.join(outputPath, csv_file)
                if os.path.isfile(csv_file_path):
                    os.remove(csv_file_path)

def main():
    table_name = "Analysis"
    total_items_processed = 0
    batch_number = 0
    last_processed_uid = load_checkpoint()
    logger.info(f"Resuming from UID: {last_processed_uid}" if last_processed_uid else "Starting from the beginning.")

    for batch in fetch_records_in_batches(table_name, batch_size=max_objects_per_batch):
        batch_number += 1
        try:
            process_batch(batch, batch_number)
            total_items_processed += len(batch)
            if batch:
                last_uid = batch[-1].get("uid")
                if last_uid:
                    save_checkpoint(last_uid)
                    logger.info(f"Checkpoint saved: Last UID processed is {last_uid}.")
        except Exception as e:
            logger.error(f"Error processing batch {batch_number}: {e}. Continuing.")

    logger.info(f"Total items processed: {total_items_processed}")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            max_objects_per_batch = int(sys.argv[1])
        except ValueError:
            logger.error("Please provide an integer value for max_objects_per_batch.")
            sys.exit(1)
    main()
