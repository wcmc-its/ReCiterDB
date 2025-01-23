#!/usr/bin/env python3

import json
import os
import sys
import time
import signal
import logging
import boto3
from boto3.s3.transfer import TransferConfig
from functools import wraps
from botocore.exceptions import ClientError, EndpointConnectionError, SSLError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Data transform + DB update modules
from dataTransformer import (
    process_person_temp,
    process_person_person_type,
    process_person,
    process_person_article,
    process_person_article_author,
    process_person_article_department,
    process_person_article_grant,
    process_person_article_keyword,
    process_person_article_relationship,
    process_person_article_scopus_target_author_affiliation,
    process_person_article_scopus_non_target_author_affiliation
)
import updateReciterDB

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Global paths and constants
OUTPUT_PATH = 'temp/parsedOutput/'
S3_OUTPUT_PATH = 'temp/s3Output/'
BUCKET_NAME = 'reciter-dynamodb'
S3_PREFIX = 'AnalysisOutput/'
CHUNK_SIZE = 1000           # DynamoDB chunk size
BATCH_THRESHOLD = 100        # Count to accumulate before processing
MAX_FILES_PER_DOWNLOAD_BATCH = 100
MAX_RETRY_ATTEMPTS = 5
MAX_WORKERS = 5

DOWNLOAD_FROM_S3 = True
os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(S3_OUTPUT_PATH, exist_ok=True)

dynamodb_resource = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

def scan_identity_table() -> list:
    """Scan Identity fully; returns all items in memory (assumed OK)."""
    table = dynamodb_resource.Table("Identity")
    items = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = table.scan(ConsistentRead=True, ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan(ConsistentRead=True)

        items.extend(response.get("Items", []))
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    logger.info(f"Scanned Identity table; retrieved {len(items)} items.")
    return items

def yield_analysis_items_in_chunks(table_name="Analysis", page_size=1000):
    """
    Generator that scans the Analysis table in chunks of 'page_size',
    yielding each chunk as a Python list of dicts.
    """
    table = dynamodb_resource.Table(table_name)
    last_evaluated_key = None
    total_count = 0

    while True:
        scan_kwargs = {"ConsistentRead": True, "Limit": page_size}
        if last_evaluated_key:
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])
        count_this_batch = len(items)
        total_count += count_this_batch

        if count_this_batch:
            logger.info(f"Retrieved {count_this_batch} items from Analysis (running total: {total_count}).")
            yield items

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            logger.info(f"No more items to scan in Analysis. Final total: {total_count} items.")
            break

#
# S3 Download Logic (unchanged)
#

# For larger files, use a TransferConfig
transfer_config = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,
    max_concurrency=10,  # number of threads *per* file
    multipart_chunksize=5 * 1024 * 1024
)

def _download_single_object(bucket_name, s3_key, local_file_path, max_retries=5):
    attempt = 0
    while attempt < max_retries:
        try:
            # Use TransferConfig to enable parallel multi-part downloads
            s3_client.download_file(
                bucket_name, 
                s3_key, 
                local_file_path, 
                Config=transfer_config
            )
            logger.info(f"Successfully downloaded '{s3_key}'")
            return True
        except (ClientError, EndpointConnectionError, SSLError) as e:
            attempt += 1
            # Possibly reduce exponential backoff
            backoff = 1.5 ** attempt
            logger.warning(
                f"Error downloading {s3_key}: {type(e).__name__}: {e}. "
                f"Attempt {attempt}/{max_retries}. Retrying in {backoff} secs..."
            )
            time.sleep(backoff)
        except Exception as e:
            logger.error(f"Unexpected error downloading {s3_key}: {e}")
            return False
    else:
        logger.error(f"Failed to download {s3_key} after {max_retries} attempts.")
        return False

def download_files_from_s3(bucket_name, keys, prefix, local_path, max_retries=5, max_workers=5):
    successfully_downloaded = []
    if not keys:
        return successfully_downloaded

    logger.info(f"Starting concurrent download of {len(keys)} objects with up to {max_workers} threads...")
    download_tasks = []
    for s3_key in keys:
        local_file_path = os.path.join(local_path, os.path.relpath(s3_key, prefix))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        download_tasks.append((s3_key, local_file_path))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(_download_single_object, bucket_name, s3_key, local_file_path, max_retries): s3_key
            for (s3_key, local_file_path) in download_tasks
        }

        from concurrent.futures import as_completed
        for future in as_completed(future_to_key):
            s3_key = future_to_key[future]
            try:
                success = future.result()
                if success:
                    successfully_downloaded.append(s3_key)
                else:
                    logger.error(f"Failed to download S3 object '{s3_key}'.")
            except Exception as e:
                logger.error(f"Unexpected error in download for '{s3_key}': {type(e).__name__}: {e}", exc_info=True)

    logger.info(f"Completed parallel downloads. Successfully downloaded {len(successfully_downloaded)}/{len(keys)} objects.")
    return successfully_downloaded

#
# Processing logic for usingS3=0 (convert sets->lists, load DB)
#
def process_direct_records_for_analysis(raw_items):
    if not raw_items:
        return

    logger.info(f"Converting {len(raw_items)} analysis items from DynamoDB resource scan.")

    def walk_obj(obj):
        if isinstance(obj, dict):
            return {k: walk_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [walk_obj(x) for x in obj]
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj

    python_records = []
    for item in raw_items:
        item = walk_obj(item)  # convert sets => lists
        reCiter = item.get("reCiterFeature")
        if reCiter:
            python_records.append(reCiter)

    if not python_records:
        logger.warning("No 'reCiterFeature' found in these usingS3=0 items.")
        return

    logger.info(f"Transforming {len(python_records)} direct items into CSV...")
    process_person(python_records, OUTPUT_PATH)
    process_person_article(python_records, OUTPUT_PATH)
    process_person_article_author(python_records, OUTPUT_PATH)
    process_person_article_department(python_records, OUTPUT_PATH)
    process_person_article_grant(python_records, OUTPUT_PATH)
    process_person_article_keyword(python_records, OUTPUT_PATH)
    process_person_article_relationship(python_records, OUTPUT_PATH)
    process_person_article_scopus_target_author_affiliation(python_records, OUTPUT_PATH)
    process_person_article_scopus_non_target_author_affiliation(python_records, OUTPUT_PATH)

    logger.info("Loading CSV results for usingS3=0 items into DB (no truncate)...")
    updateReciterDB.main(truncate_tables=False, skip_person_temp=True)

    del python_records
    logger.info("Released memory allocated to python_records (usingS3=0).")

#
# Processing logic for usingS3=1 => S3 downloads => transform => DB
#
def process_s3_batch(s3_items, s3_filenames_set):
    """
    Given a list of items that have 'uid' (usingS3=1),
    find corresponding S3 keys, download, transform => DB.
    """
    if not s3_items:
        return

    logger.info(f"Preparing to download & process {len(s3_items)} items with usingS3=1...")

    # Build a list of S3 keys to download
    s3_keys = []
    for itm in s3_items:
        uid = itm.get("uid")
        if uid and uid in s3_filenames_set:
            full_key = os.path.join(S3_PREFIX, uid)
            s3_keys.append(full_key)
        else:
            logger.debug(f"UID '{uid}' is not in S3 set or missing. Skipping download.")

    if not s3_keys:
        logger.warning("No valid S3 keys found among usingS3=1 items in this batch.")
        return

    # Download in sub-batches if needed
    current_idx = 0
    total = len(s3_keys)
    while current_idx < total:
        sub_batch = s3_keys[current_idx : current_idx + MAX_FILES_PER_DOWNLOAD_BATCH]
        logger.info(f"Downloading sub-batch of {len(sub_batch)} S3 objects for usingS3=1 items...")
        downloaded = []
        if DOWNLOAD_FROM_S3:
            downloaded = download_files_from_s3(
                bucket_name=BUCKET_NAME,
                keys=sub_batch,
                prefix=S3_PREFIX,
                local_path=S3_OUTPUT_PATH,
                max_retries=MAX_RETRY_ATTEMPTS,
                max_workers=MAX_WORKERS
            )
        else:
            logger.info("Skipping actual S3 download because DOWNLOAD_FROM_S3=False.")
            downloaded = sub_batch

        if downloaded:
            # Now parse & load them into DB
            process_s3_files(downloaded, S3_PREFIX)
        current_idx += MAX_FILES_PER_DOWNLOAD_BATCH

def process_s3_files(downloaded_keys, prefix):
    """
    For each local file corresponding to the downloaded S3 keys,
    parse JSON lines, load data into the DB, and then delete the local file.
    """
    person_list = [os.path.relpath(k, prefix) for k in downloaded_keys]
    person_list = [f for f in person_list if f not in [".DS_Store", ".gitkeep"]]
    person_list.sort()
    logger.info(f"Processing {len(person_list)} local files from S3...")

    all_items = []
    for filename in person_list:
        local_path = os.path.join(S3_OUTPUT_PATH, filename)
        if not os.path.exists(local_path):
            logger.error(f"File not found: {local_path}")
            continue

        # Read the file contents into 'all_items'
        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        all_items.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error in {filename}: {e}")
                        logger.debug(f"Raw line: {line.strip()}")
        finally:
            # Delete file whether parsing succeeded or not
            try:
                os.remove(local_path)
                logger.debug(f"Deleted local file: {local_path}")
            except OSError as e:
                logger.warning(f"Failed to delete {local_path}: {e}")

    if not all_items:
        logger.warning("No items extracted from downloaded S3 files.")
        return

    logger.info(f"Transforming {len(all_items)} items from S3 into CSV...")
    process_person(all_items, OUTPUT_PATH)
    process_person_article(all_items, OUTPUT_PATH)
    process_person_article_author(all_items, OUTPUT_PATH)
    process_person_article_department(all_items, OUTPUT_PATH)
    process_person_article_grant(all_items, OUTPUT_PATH)
    process_person_article_keyword(all_items, OUTPUT_PATH)
    process_person_article_relationship(all_items, OUTPUT_PATH)
    process_person_article_scopus_target_author_affiliation(all_items, OUTPUT_PATH)
    process_person_article_scopus_non_target_author_affiliation(all_items, OUTPUT_PATH)

    logger.info("Loading CSV results from S3 items into DB (no truncate)...")
    updateReciterDB.main(truncate_tables=False, skip_person_temp=True)

    del all_items
    logger.info("Released memory allocated to all_items (usingS3=1).")


def main():
    # Step 1: Identity => build & load person_temp, etc
    identity_items = scan_identity_table()
    process_person_temp(identity_items, OUTPUT_PATH)
    process_person_person_type(identity_items, OUTPUT_PATH)
    del identity_items[:]  # free memory

    # Make sure person_temp.csv isn't empty
    person_temp_path = os.path.join(OUTPUT_PATH, 'person_temp.csv')
    if not os.path.exists(person_temp_path) or os.path.getsize(person_temp_path) == 0:
        logger.error("person_temp.csv is missing or empty. Aborting.")
        return

    logger.info("Loading Identity-based CSV data into DB (truncate).")
    updateReciterDB.main(truncate_tables=True, skip_person_temp=False)

    # Step 2: list all S3 keys => store in set for membership
    logger.info(f"Listing all S3 objects under '{S3_PREFIX}' from bucket '{BUCKET_NAME}'...")
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX)

    all_keys = []
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                all_keys.append(obj['Key'])
    all_keys = sorted(set(all_keys))
    logger.info(f"Found {len(all_keys)} total S3 objects under prefix {S3_PREFIX}")
    s3_filenames_set = set(os.path.relpath(k, S3_PREFIX) for k in all_keys)  # e.g. "dis4002"

    # Step 3: Stream-scan Analysis in chunks, accumulate in buffers
    direct_buffer = []  # usingS3=0
    s3_buffer = []      # usingS3=1

    logger.info(f"Scanning Analysis table in chunks of {CHUNK_SIZE} to avoid high memory usage...")

    for chunk in yield_analysis_items_in_chunks(table_name="Analysis", page_size=CHUNK_SIZE):
        logger.info(f"Processing chunk of {len(chunk)} items from Analysis.")

        # Distribute items to the appropriate buffer
        for item in chunk:
            using_s3_val = item.get("usingS3", 0)
            if using_s3_val == 1:
                s3_buffer.append(item)
            else:
                direct_buffer.append(item)

        # If either buffer reaches the threshold, process it
        if len(direct_buffer) >= BATCH_THRESHOLD:
            logger.info(f"Reached direct_buffer threshold of {BATCH_THRESHOLD}, processing now...")
            process_direct_records_for_analysis(direct_buffer)
            direct_buffer.clear()

        if len(s3_buffer) >= BATCH_THRESHOLD:
            logger.info(f"Reached s3_buffer threshold of {BATCH_THRESHOLD}, processing now...")
            process_s3_batch(s3_buffer, s3_filenames_set)
            s3_buffer.clear()

        del chunk[:]  # free memory from this chunk
        logger.info("Finished processing this chunk. Moving on to next chunk...")

    # After the scan ends, process any leftover items in buffers
    if direct_buffer:
        logger.info(f"Processing final {len(direct_buffer)} items in direct_buffer.")
        process_direct_records_for_analysis(direct_buffer)
        direct_buffer.clear()

    if s3_buffer:
        logger.info(f"Processing final {len(s3_buffer)} items in s3_buffer.")
        process_s3_batch(s3_buffer, s3_filenames_set)
        s3_buffer.clear()

    logger.info("All chunks from Analysis table processed successfully. Done.")

if __name__ == '__main__':
    main()