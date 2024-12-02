# retrieveS3.py

import json
import os
import sys
import time
import boto3
import botocore
import logging
from botocore.exceptions import ClientError, EndpointConnectionError, SSLError
from data_transformer import (
    process_identity,
    process_person,
    process_person_article,
    process_person_article_author,
    process_person_article_department,
    process_person_article_grant,
    process_person_article_keyword,
    process_person_article_relationship,
    process_person_article_scopus_target_author_affiliation,
    process_person_article_scopus_non_target_author_affiliation,
    process_person_person_type
)
import updateReCiterDB

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize paths and settings
originalDataPath = 'temp/s3Output/'
outputPath = 'temp/parsedOutput/'
download_from_s3 = True  # Set to True to download data from S3
max_files_per_download_batch = 100  # Number of files to download per batch
max_objects_per_chunk = 100  # Number of objects to process per batch
max_retry_attempts = 5  # Number of retry attempts for downloading a file
delete_json_after_processing = True  # Set to True to delete JSON files after processing

# Ensure directories exist
os.makedirs(originalDataPath, exist_ok=True)
os.makedirs(outputPath, exist_ok=True)

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def scan_table(table_name):
    start = time.time()
    table = dynamodb.Table(table_name)

    response = table.scan()
    items = response['Items']

    while 'LastEvaluatedKey' in response:
        logger.info(f"Retrieved {len(response['Items'])} items")
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    logger.info('Execution time for scanning table %s: %.2f seconds', table_name, time.time() - start)
    return items

def download_files_from_s3(bucket_name, keys, prefix, local_path, max_retries=5):
    s3 = boto3.client('s3')
    successfully_downloaded = []  # Keep track of successfully downloaded files
    for s3_key in keys:
        local_file_path = os.path.join(local_path, os.path.relpath(s3_key, prefix))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        logger.info(f"Downloading {s3_key} to {local_file_path}")

        attempt = 0
        while attempt < max_retries:
            try:
                s3.download_file(bucket_name, s3_key, local_file_path)
                successfully_downloaded.append(s3_key)  # Add to the list of successful downloads
                break  # Download succeeded
            except (ClientError, EndpointConnectionError, SSLError) as e:
                attempt += 1
                logger.warning(f"Error downloading {s3_key}: {e}. Attempt {attempt} of {max_retries}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error downloading {s3_key}: {e}")
                break  # Exit on unexpected errors
        else:
            logger.error(f"Failed to download {s3_key} after {max_retries} attempts.")
    return successfully_downloaded  # Return the list of successfully downloaded files

def main():
    # Process identities once
    identities = scan_table('Identity')
    logger.info(f"Count of items from DynamoDB Identity table: {len(identities)}")
    process_identity(identities, outputPath)
    process_person_person_type(identities, outputPath)

    # Verify `identity.csv` exists
    identity_csv_path = os.path.join(outputPath, 'identity.csv')
    if not os.path.exists(identity_csv_path) or os.path.getsize(identity_csv_path) == 0:
        logger.error("identity.csv missing or empty. Aborting.")
        return

    # Truncate tables one time before processing batches and load identity_temp
    try:
        updateReCiterDB.main(truncate_tables=True, skip_identity_temp=False)
    except Exception as e:
        logger.error(f"Error during initial database update: {e}")
        return  # Exit or handle accordingly

    # Initialize S3 client and get list of all files
    s3 = boto3.client('s3')
    bucket_name = 'reciter-dynamodb'
    prefix = 'AnalysisOutput/'
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    all_keys = []

    # Collect all the keys
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                all_keys.append(obj['Key'])

    total_files = len(all_keys)
    logger.info(f"Total files to process: {total_files}")

    current_index = 0

    while current_index < total_files:
        # Determine the keys for the current batch
        batch_keys = all_keys[current_index:current_index + max_files_per_download_batch]
        logger.info(f"Processing batch {current_index // max_files_per_download_batch + 1}: files {current_index + 1} to {current_index + len(batch_keys)}")

        # Download the batch of files and get successfully downloaded keys
        if download_from_s3:
            successfully_downloaded_keys = download_files_from_s3(
                bucket_name=bucket_name,
                keys=batch_keys,
                prefix=prefix,
                local_path=originalDataPath,
                max_retries=max_retry_attempts
            )
        else:
            logger.info("Using existing data in local directory.")
            successfully_downloaded_keys = batch_keys  # Assume all keys are available locally

        if not successfully_downloaded_keys:
            logger.warning("No files were successfully downloaded in this batch. Skipping processing.")
            current_index += max_files_per_download_batch
            continue

        # Get list of downloaded files
        person_list = [os.path.relpath(key, prefix) for key in successfully_downloaded_keys]
        person_list = [f for f in person_list if f not in [".DS_Store", ".gitkeep"]]
        person_list.sort()
        logger.info(f"Processing {len(person_list)} files in the batch.")

        # Process each file in the batch
        items = []
        for filename in person_list:
            file_path = os.path.join(originalDataPath, filename)
            if not os.path.exists(file_path):
                logger.error(f"File {file_path} does not exist. Skipping.")
                continue
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        items.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON in {filename}: {e}")

        if not items:
            logger.warning("No items to process in this batch.")
        else:
            # Process items and generate CSV files for the batch
            process_person(items, outputPath)
            process_person_article(items, outputPath)
            process_person_article_author(items, outputPath)
            process_person_article_department(items, outputPath)
            process_person_article_grant(items, outputPath)
            process_person_article_keyword(items, outputPath)
            process_person_article_relationship(items, outputPath)
            process_person_article_scopus_target_author_affiliation(items, outputPath)
            process_person_article_scopus_non_target_author_affiliation(items, outputPath)

            # Update the database without truncating tables again
            updateReCiterDB.main(truncate_tables=False, skip_identity_temp=True)

            # Clean up batch-specific CSV files
            for csv_file in os.listdir(outputPath):
                if csv_file != "identity.csv":  # Retain `identity.csv`
                    csv_file_path = os.path.join(outputPath, csv_file)
                    if os.path.isfile(csv_file_path):
                        os.remove(csv_file_path)

            # Conditionally delete processed JSON files
            if delete_json_after_processing:
                for filename in person_list:
                    file_path = os.path.join(originalDataPath, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
            logger.info(f"Batch {current_index // max_files_per_download_batch + 1} processed and cleaned up.")

        # Clear items to free memory
        items.clear()

        # Update current_index for the next batch
        current_index += max_files_per_download_batch

    logger.info("All batches processed successfully.")

if __name__ == '__main__':
    main()
