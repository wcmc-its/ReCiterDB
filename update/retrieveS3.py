# retrieveS3.py

import json
import os
import sys
import time
import boto3
import botocore
import logging
from botocore.exceptions import ClientError, EndpointConnectionError, SSLError
from dataTransformer import (
    process_person_temp,
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

originalDataPath = 'temp/s3Output/'
outputPath = 'temp/parsedOutput/'
download_from_s3 = True
max_files_per_download_batch = 100
max_objects_per_chunk = 100
max_retry_attempts = 5
delete_json_after_processing = False

os.makedirs(originalDataPath, exist_ok=True)
os.makedirs(outputPath, exist_ok=True)

dynamodb = boto3.resource('dynamodb')

def scan_table(table_name):
    start = time.time()
    table = dynamodb.Table(table_name)
    response = table.scan(ConsistentRead=True)
    items = response['Items']

    while 'LastEvaluatedKey' in response:
        logger.info(f"Retrieved {len(response['Items'])} items")
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ConsistentRead=True
        )
        items.extend(response['Items'])

    logger.info('Execution time for scanning table %s: %.2f seconds', table_name, time.time() - start)
    return items

def download_files_from_s3(bucket_name, keys, prefix, local_path, max_retries=5):
    s3 = boto3.client('s3')
    successfully_downloaded = []
    for s3_key in keys:
        local_file_path = os.path.join(local_path, os.path.relpath(s3_key, prefix))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        logger.info(f"Downloading {s3_key} to {local_file_path}")

        attempt = 0
        while attempt < max_retries:
            try:
                s3.download_file(bucket_name, s3_key, local_file_path)
                successfully_downloaded.append(s3_key)
                break
            except (ClientError, EndpointConnectionError, SSLError) as e:
                attempt += 1
                logger.warning(f"Error downloading {s3_key}: {e}. Attempt {attempt} of {max_retries}. Retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error downloading {s3_key}: {e}")
                break
        else:
            logger.error(f"Failed to download {s3_key} after {max_retries} attempts.")
    return successfully_downloaded

def main():
    # Process identities once at the beginning
    identities = scan_table('Identity')
    logger.info(f"Count of items from DynamoDB Identity table: {len(identities)}")
    process_person_temp(identities, outputPath)
    process_person_person_type(identities, outputPath)

    identity_csv_path = os.path.join(outputPath, 'person_temp.csv')
    if not os.path.exists(identity_csv_path) or os.path.getsize(identity_csv_path) == 0:
        logger.error("person_temp.csv missing or empty. Aborting.")
        return

    # First run: truncate all tables and load person_temp + others
    try:
        updateReCiterDB.main(truncate_tables=True, skip_person_temp=False)
    except Exception as e:
        logger.error(f"Error during initial database update: {e}")
        return

    s3 = boto3.client('s3')
    bucket_name = 'reciter-dynamodb'
    prefix = 'AnalysisOutput/'
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    all_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                all_keys.append(obj['Key'])

    all_keys = list(set(all_keys))
    all_keys.sort()
    total_files = len(all_keys)
    logger.info(f"Total files to process after deduplication: {total_files}")

    current_index = 0
    processed_uids = set()
    skipped_uids = set()

    # Subsequent runs: do not truncate again, do not load person_temp again
    while current_index < total_files:
        batch_keys = all_keys[current_index:current_index + max_files_per_download_batch]
        logger.info(f"Processing batch {current_index // max_files_per_download_batch + 1}: files {current_index + 1} to {current_index + len(batch_keys)}")

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
            successfully_downloaded_keys = batch_keys

        if not successfully_downloaded_keys:
            logger.warning("No files were successfully downloaded in this batch. Skipping processing.")
            current_index += max_files_per_download_batch
            continue

        person_list = [os.path.relpath(key, prefix) for key in successfully_downloaded_keys]
        person_list = [f for f in person_list if f not in [".DS_Store", ".gitkeep"]]
        person_list.sort()
        logger.info(f"Processing {len(person_list)} files in the batch.")

        items = []
        for filename in person_list:
            file_path = os.path.join(originalDataPath, filename)
            if not os.path.exists(file_path):
                logger.error(f"File {file_path} does not exist. Skipping.")
                skipped_uids.add(filename)
                continue
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        items.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON in {filename}: {e}")
                        logger.debug(f"Raw content: {line.strip()}")
                        skipped_uids.add(filename)

        if not items:
            logger.warning("No items to process in this batch.")
        else:
            for item in items:
                person_identifier = item.get('personIdentifier', None)
                if person_identifier is None:
                    logger.warning(f"personIdentifier missing in item: {item}")
                    skipped_uids.add(item)
                else:
                    processed_uids.add(person_identifier)
                    logger.info(f"Processed personIdentifier: {person_identifier}")

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

            # Now that we have new CSVs for these items, load them without truncation or person_temp reload
            updateReCiterDB.main(truncate_tables=False, skip_person_temp=True)

        items.clear()
        current_index += max_files_per_download_batch

    logger.info(f"Total processed personIdentifiers: {len(processed_uids)}")
    logger.info(f"Total skipped personIdentifiers: {len(skipped_uids)}")
    if skipped_uids:
        logger.warning(f"Skipped personIdentifiers: {skipped_uids}")

    logger.info("All batches processed successfully.")

if __name__ == '__main__':
    main()
