# retrieveDynamoDB.py

import os
import sys
import time
import boto3
from dynamodb_json import json_util as dynamodb_json
import logging
import pprint
from botocore.exceptions import ClientError, EndpointConnectionError
from data_transformer import (
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
import updateReCiterDb

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize paths and settings
outputPath = 'temp/parsedOutput/'
max_objects_per_batch = 100  # Number of items to process per batch
delete_csv_after_processing = True  # Set to True to delete CSV files after processing

# Ensure directories exist
os.makedirs(outputPath, exist_ok=True)

def fetch_records_in_batches(table_name, batch_size=100, max_retries=5):
    """
    Generator function to fetch and transform records from a DynamoDB table in batches,
    using the dynamodb-json library. Filters records where `usingS3 = 0`.

    Args:
        table_name (str): The name of the DynamoDB table.
        batch_size (int): Number of items to fetch per scan operation.
        max_retries (int): Maximum number of retry attempts for DynamoDB scan operations.

    Yields:
        list: A batch of transformed records.
    """
    dynamodb = boto3.client("dynamodb")
    last_evaluated_key = None

    while True:
        attempt = 0
        while attempt < max_retries:
            try:
                # Build the scan request with the filter expression
                scan_kwargs = {
                    "TableName": table_name,
                    "Limit": batch_size,
                    "FilterExpression": "usingS3 = :val",
                    "ExpressionAttributeValues": {":val": {"N": "0"}},
                }
                if last_evaluated_key:
                    scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

                # Perform the scan
                response = dynamodb.scan(**scan_kwargs)

                # Transform the items using dynamodb-json
                raw_items = response.get("Items", [])
                transformed_items = [dynamodb_json.loads(item) for item in raw_items]

                if not transformed_items:
                    logger.info("No more items to process.")
                    return  # Exit if no items are returned

                logger.info(f"Fetched and transformed {len(transformed_items)} records.")

                # Yield the batch of transformed items
                yield transformed_items

                last_evaluated_key = response.get("LastEvaluatedKey", None)
                if not last_evaluated_key:
                    logger.info("All items have been processed.")
                    break  # Exit the loop if there are no more items

                break  # Exit retry loop if successful
            except (ClientError, EndpointConnectionError) as e:
                attempt += 1
                logger.warning(f"Error fetching records: {e}. Attempt {attempt} of {max_retries}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return  # Exit function on unexpected errors

def process_batch(batch, batch_number):
    """
    Processes a single batch of records.

    Args:
        batch (list): A batch of transformed records.
        batch_number (int): The batch number for logging purposes.
    """
    logger.info(f"Processing batch {batch_number} with {len(batch)} records.")

    # Extract 'reCiterFeature' from each record
    extracted_records = []
    for record in batch:
        reCiterFeature = record.get('reCiterFeature')
        if reCiterFeature:
            extracted_records.append(reCiterFeature)
        else:
            logger.warning(f"No 'reCiterFeature' key in record: {record}")

    if not extracted_records:
        logger.warning(f"No valid 'reCiterFeature' records found in batch {batch_number}. Skipping.")
        return

    # Process the extracted records and generate CSV files
    process_person(extracted_records, outputPath)
    process_person_article(extracted_records, outputPath)
    process_person_article_author(extracted_records, outputPath)
    process_person_article_department(extracted_records, outputPath)
    process_person_article_grant(extracted_records, outputPath)
    process_person_article_keyword(extracted_records, outputPath)
    process_person_article_relationship(extracted_records, outputPath)
    process_person_article_scopus_target_author_affiliation(extracted_records, outputPath)
    process_person_article_scopus_non_target_author_affiliation(extracted_records, outputPath)

    logger.info(f"Batch {batch_number}: Updating the database...")
    updateReCiterDB.main(truncate_tables=False, skip_identity_temp=True)

    if delete_csv_after_processing:
        logger.info(f"Batch {batch_number}: Deleting CSV files...")
        for csv_file in os.listdir(outputPath):
            if csv_file != "identity.csv":  # Retain `identity.csv`
                csv_file_path = os.path.join(outputPath, csv_file)
                if os.path.isfile(csv_file_path):
                    os.remove(csv_file_path)
        logger.info(f"Batch {batch_number}: CSV files deleted.")

def main():
    table_name = "Analysis"
    total_items_processed = 0
    batch_number = 0

    logger.info("Fetching and processing records from DynamoDB in batches.")

    for batch in fetch_records_in_batches(table_name, batch_size=max_objects_per_batch):
        batch_number += 1
        process_batch(batch, batch_number)
        total_items_processed += len(batch)

        # For testing purposes, you can break after processing the first batch
        # Uncomment the following line if you want to process only one batch
        # break

    logger.info(f"Total items processed: {total_items_processed}")
    logger.info("Script execution completed.")

if __name__ == '__main__':
    # Allow passing the max_objects_per_batch as a command-line argument
    if len(sys.argv) > 1:
        try:
            max_objects_per_batch = int(sys.argv[1])
        except ValueError:
            logger.error("Please provide an integer value for max_objects_per_batch.")
            sys.exit(1)
    else:
        max_objects_per_batch = 100  # Default value

    main()
