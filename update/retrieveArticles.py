#!/usr/bin/env python3

import json
import os
import sys
import time
import signal
import logging
import boto3
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
CHUNK_SIZE = 1000           # DynamoDB chunk size for scanning
BATCH_THRESHOLD = 200       # Accumulate this many items before processing
MAX_FILES_PER_DOWNLOAD_BATCH = 150
MAX_RETRY_ATTEMPTS = 5
MAX_WORKERS = 8

# Toggle to skip actual S3 downloads (for debugging)
DOWNLOAD_FROM_S3 = True

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(S3_OUTPUT_PATH, exist_ok=True)

dynamodb_resource = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

# For debugging: track processed and skipped UIDs
processed_uids = set()
skipped_uids = set()

# Track final S3 download failures (even after the built-in 5 retries)
final_s3_download_failures = []

def scan_identity_table() -> list:
    """
    Scan the entire Identity table in a single pass (paginated),
    accumulating items in-memory. Returns all items as a list.
    """
    table = dynamodb_resource.Table("Identity")
    items = []
    last_evaluated_key = None
    while True:
        if last_evaluated_key:
            response = table.scan(ConsistentRead=True, ExclusiveStartKey=last_evaluated_key)
        else:
            response = table.scan(ConsistentRead=True)

        batch = response.get("Items", [])
        items.extend(batch)

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    logger.info(f"Scanned Identity table; retrieved {len(items)} items.")
    return items

def yield_analysis_items_in_chunks(table_name="Analysis", page_size=1000):
    """
    Generator that scans the Analysis table in chunks of 'page_size',
    yielding each chunk as a Python list of dicts.
    This is a single-segment, single-pass approach (no skipping).
    """
    table = dynamodb_resource.Table(table_name)
    last_evaluated_key = None
    total_count = 0

    while True:
        kwargs = {"ConsistentRead": True, "Limit": page_size}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key

        response = table.scan(**kwargs)
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

def _download_single_object(bucket_name, s3_key, local_file_path, max_retries=5):
    """
    Helper function to download a single S3 object with retry logic.
    Returns True if successful, False if exhausted retries or unexpected error.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            s3_client.download_file(bucket_name, s3_key, local_file_path)
            logger.info(f"Successfully downloaded '{s3_key}' to '{local_file_path}'")
            return True
        except (ClientError, EndpointConnectionError, SSLError) as e:
            attempt += 1
            logger.warning(
                f"Error downloading {s3_key}: {type(e).__name__}: {e}. "
                f"Attempt {attempt}/{max_retries}. Retrying..."
            )
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Unexpected error downloading {s3_key}: {e}", exc_info=True)
            return False

    logger.error(f"Failed to download {s3_key} after {max_retries} attempts.")
    return False

def download_files_from_s3(bucket_name, keys, prefix, local_path, max_retries=5, max_workers=5):
    """
    Downloads multiple S3 objects concurrently using up to `max_workers` threads.
    Returns list of successfully downloaded s3_keys.
    """
    successfully_downloaded = []
    if not keys:
        return successfully_downloaded

    logger.info(f"Starting concurrent download of {len(keys)} objects with up to {max_workers} threads...")

    download_tasks = []
    for s3_key in keys:
        local_file_path = os.path.join(local_path, os.path.relpath(s3_key, prefix))
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        download_tasks.append((s3_key, local_file_path))

    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(_download_single_object, bucket_name, s3_key, local_file_path, max_retries): s3_key
            for (s3_key, local_file_path) in download_tasks
        }

        for future in as_completed(future_to_key):
            s3_key = future_to_key[future]
            try:
                success = future.result()
                if success:
                    successfully_downloaded.append(s3_key)
                else:
                    # Keep track of keys that never downloaded successfully
                    logger.error(f"Final failure to download '{s3_key}'.")
                    final_s3_download_failures.append(s3_key)
            except Exception as e:
                logger.error(
                    f"Unexpected exception in download for '{s3_key}': {type(e).__name__}: {e}",
                    exc_info=True
                )
                final_s3_download_failures.append(s3_key)

    logger.info(
        f"Completed parallel downloads. Successfully downloaded "
        f"{len(successfully_downloaded)}/{len(keys)} objects."
    )
    return successfully_downloaded

def process_direct_records_for_analysis(raw_items):
    """
    Takes items that have usingS3=0 (no S3 data). Convert sets->lists,
    ensure we have a personIdentifier, then run dataTransformer => DB.
    """
    if not raw_items:
        return

    logger.info(f"Converting {len(raw_items)} analysis items from DynamoDB resource scan (usingS3=0).")

    def walk_obj(obj):
        if isinstance(obj, dict):
            return {k: walk_obj(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [walk_obj(x) for x in obj]
        elif isinstance(obj, set):
            # Convert sets to lists to be JSON-serializable
            return list(obj)
        else:
            return obj

    python_records = []

    for item in raw_items:
        # Flatten sets->lists
        item = walk_obj(item)

        # Try 'reCiterFeature' if present, else fallback to item
        reciter_data = item.get("reCiterFeature")
        if not (reciter_data and isinstance(reciter_data, dict)):
            reciter_data = item

        # Ensure personIdentifier
        person_identifier = reciter_data.get("personIdentifier") or item.get("personIdentifier") or item.get("uid")
        if not person_identifier:
            logger.warning(f"Skipping item with no personIdentifier or uid: {item}")
            skipped_uids.add("no-personIdentifier-and-uid")
            continue

        reciter_data["personIdentifier"] = person_identifier
        python_records.append(reciter_data)
        processed_uids.add(str(person_identifier))

    if not python_records:
        logger.warning("No valid records found in these usingS3=0 items after checks.")
        return

    logger.info(f"Transforming {len(python_records)} direct items into CSV (usingS3=0).")

    # DataTransformer calls
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

def process_s3_batch(s3_items, s3_filenames_set):
    """
    For items that have usingS3=1, find their S3 object, download, transform => DB.
    """
    if not s3_items:
        return

    logger.info(f"Preparing to download & process {len(s3_items)} items with usingS3=1...")

    s3_keys = []
    for itm in s3_items:
        uid = itm.get("uid")
        if not uid:
            logger.warning(f"S3 item has no uid: {itm}. Skipping.")
            skipped_uids.add("no-uid-for-s3")
            continue

        # Check if this exact S3 key is known to exist
        if uid in s3_filenames_set:
            full_key = os.path.join(S3_PREFIX, uid)
            s3_keys.append(full_key)
        else:
            # If it doesn't match exactly, skip
            logger.debug(f"UID '{uid}' not in s3_filenames_set or missing. Skipping.")
            skipped_uids.add(str(uid) + "-s3missing")

    if not s3_keys:
        logger.warning("No valid S3 keys found among usingS3=1 items in this batch.")
        return

    # Download in sub-batches if needed
    current_idx = 0
    total = len(s3_keys)
    while current_idx < total:
        sub_batch = s3_keys[current_idx:current_idx + MAX_FILES_PER_DOWNLOAD_BATCH]
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
            # If we have successfully downloaded any files, parse them
            process_s3_files(downloaded, S3_PREFIX)
        current_idx += MAX_FILES_PER_DOWNLOAD_BATCH

def process_s3_files(downloaded_keys, prefix):
    """
    For each local file from S3, parse either single-JSON or line-delimited JSON => transform => load => then delete file.
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
            skipped_uids.add(filename + "-filemissing")
            continue

        try:
            with open(local_path, 'r', encoding='utf-8') as f:
                file_content = f.read().strip()

            if not file_content:
                logger.warning(f"File {filename} is empty.")
                continue

            # Attempt single-object/array parse first
            try:
                data = json.loads(file_content)
                if isinstance(data, list):
                    all_items.extend(data)
                else:
                    all_items.append(data)
            except json.JSONDecodeError:
                # Fallback: parse line-by-line
                lines = file_content.splitlines()
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        subobj = json.loads(line)
                        # if a line has a JSON array, handle that
                        if isinstance(subobj, list):
                            all_items.extend(subobj)
                        else:
                            all_items.append(subobj)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error in {filename}, line='{line}': {e}")
                        skipped_uids.add(filename + "-jsonErrorLine")

        finally:
            # Always delete the local file after reading
            try:
                os.remove(local_path)
                logger.debug(f"Deleted local file: {local_path}")
            except OSError as e:
                logger.warning(f"Failed to delete {local_path}: {e}")

    if not all_items:
        logger.warning("No items extracted from downloaded S3 files.")
        return

    logger.info(f"Transforming {len(all_items)} items from S3 into CSV...")

    # DataTransformer calls
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

    # Mark them processed by adding their personIdentifiers to processed_uids
    #   (because each JSON might have multiple records)
    # We do that inside process_person(...) if you prefer. Or do it manually here.
    # For example:
    for record in all_items:
        pid = record.get("personIdentifier")
        if pid:
            processed_uids.add(str(pid))

    del all_items
    logger.info("Released memory allocated to all_items (usingS3=1).")

def main():
    # Step 1: Process Identity => build person_temp + person_person_type => DB
    identity_items = scan_identity_table()
    process_person_temp(identity_items, OUTPUT_PATH)
    process_person_person_type(identity_items, OUTPUT_PATH)
    del identity_items[:]
    logger.info("Loading Identity-based CSV data into DB (truncate).")

    updateReciterDB.main(truncate_tables=True, skip_person_temp=False)

    # Step 2: List all S3 keys => store in set
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
    s3_filenames_set = set(os.path.relpath(k, S3_PREFIX) for k in all_keys)

    # Step 3: Single-pass chunked scan of Analysis => separate buffers
    direct_buffer = []
    s3_buffer = []

    logger.info(f"Scanning Analysis table in chunks of {CHUNK_SIZE}...")

    for chunk in yield_analysis_items_in_chunks(table_name="Analysis", page_size=CHUNK_SIZE):
        logger.info(f"Processing chunk of {len(chunk)} items from Analysis.")
        for item in chunk:
            using_s3_val = item.get("usingS3", 0)
            if using_s3_val == 1:
                s3_buffer.append(item)
            else:
                direct_buffer.append(item)

        # If either buffer reaches threshold, process it
        if len(direct_buffer) >= BATCH_THRESHOLD:
            logger.info(f"Reached direct_buffer threshold {BATCH_THRESHOLD}; processing now.")
            process_direct_records_for_analysis(direct_buffer)
            direct_buffer.clear()

        if len(s3_buffer) >= BATCH_THRESHOLD:
            logger.info(f"Reached s3_buffer threshold {BATCH_THRESHOLD}; processing now.")
            process_s3_batch(s3_buffer, s3_filenames_set)
            s3_buffer.clear()

        # Clear out the chunk to reduce memory usage
        del chunk[:]
        logger.info("Finished processing this chunk. Moving on to next chunk...")

    # Process leftover buffers
    if direct_buffer:
        logger.info(f"Processing final {len(direct_buffer)} items in direct_buffer.")
        process_direct_records_for_analysis(direct_buffer)
        direct_buffer.clear()

    if s3_buffer:
        logger.info(f"Processing final {len(s3_buffer)} items in s3_buffer.")
        process_s3_batch(s3_buffer, s3_filenames_set)
        s3_buffer.clear()

    # Step 4: Second pass for final_s3_download_failures (if any)
    if final_s3_download_failures:
        logger.warning(f"Attempting second-pass downloads for {len(final_s3_download_failures)} S3 keys that failed all retries.")
        # Single-thread or smaller concurrency might help. Let's do single-thread for clarity:
        for s3_key in final_s3_download_failures:
            local_file_path = os.path.join(S3_OUTPUT_PATH, os.path.relpath(s3_key, S3_PREFIX))
            success = _download_single_object(BUCKET_NAME, s3_key, local_file_path, max_retries=3)
            if success:
                logger.info(f"Second-pass download succeeded for '{s3_key}'.")
                # parse it
                process_s3_files([s3_key], S3_PREFIX)
            else:
                logger.error(f"Second-pass download STILL failed for '{s3_key}'.")
                # Optionally fallback: we can parse from Analysis if it's in DynamoDB
                #   if the record had enough reCiterFeature data. That means we need
                #   to find the 'uid' from the s3_key. For instance:
                uid_only = os.path.relpath(s3_key, S3_PREFIX)
                # Try retrieving item from Analysis directly
                table = dynamodb_resource.Table("Analysis")
                # Minimal approach:
                #   If your table's primary key is "uid", you can do table.get_item(Key={"uid": uid_only})
                #   Else do a query or scan to find it. Example:
                response = table.get_item(Key={"uid": uid_only})
                db_item = response.get("Item")
                if db_item:
                    logger.info(f"Fallback: processing item from Analysis directly for uid '{uid_only}'.")
                    process_direct_records_for_analysis([db_item])
                else:
                    logger.warning(f"Fallback not possible; item not found in Analysis for uid '{uid_only}'.")

    # Step 5: Final pass to catch any items we never processed (belt-and-suspenders)
    logger.info("Performing final pass to find unprocessed items in Analysis.")
    missing_in_processed_uids = []
    # We'll do a full scan of Analysis again, but we can limit to smaller page size
    # since it's just a check
    table = dynamodb_resource.Table("Analysis")
    last_key = None
    while True:
        scan_kwargs = {"ConsistentRead": True, "Limit": 1000}
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key

        resp = table.scan(**scan_kwargs)
        batch_items = resp.get("Items", [])
        for itm in batch_items:
            uid = itm.get("uid")
            if uid and (uid not in processed_uids):
                # We found an item never processed
                missing_in_processed_uids.append(itm)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if missing_in_processed_uids:
        logger.warning(f"Found {len(missing_in_processed_uids)} items that were never processed. Attempting fallback.")
        # We'll just process them direct, ignoring S3
        process_direct_records_for_analysis(missing_in_processed_uids)

    # Step 6: After everything, call UPDATE_PERSON one more time
    logger.info("All chunks from Analysis table processed. Now calling UPDATE_PERSON.")
    updateReciterDB.call_update_person_only()
    logger.info("Final UPDATE_PERSON completed.")

    # Log final debugging info about processed/skipped UIDs
    logger.info(f"Processed UIDs count: {len(processed_uids)}")
    logger.info(f"Skipped UIDs count: {len(skipped_uids)}")
    if skipped_uids:
        logger.warning(f"Skipped UIDs: {skipped_uids}")

    logger.info("Done.")

if __name__ == '__main__':
    main()