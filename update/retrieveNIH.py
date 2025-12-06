# retrieveNIH.py

import json
import os
import time
import requests
import logging
import random
import csv

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

import pymysql.cursors
import pymysql.err

def connect_mysql_server(username, db_password, db_hostname, database_name, max_retries=5, backoff_factor=1):
    """Establish a connection to MySQL or MariaDB server with retry logic."""
    for retry in range(max_retries):
        try:
            mysql_db = pymysql.connect(user=username,
                                       password=db_password,
                                       database=database_name,
                                       host=db_hostname,
                                       local_infile=True)
            logger.info("Connected to database server: %s; database: %s; with user: %s",
                        db_hostname, database_name, username)
            return mysql_db
        except pymysql.err.MySQLError as err:
            logger.error(f"Error connecting to the database on attempt {retry+1}: {err}")
            sleep_time = backoff_factor * (2 ** retry) + random.uniform(0, 1)
            time.sleep(sleep_time)
    logger.error("Exceeded maximum retries for database connection.")
    raise Exception("Could not connect to the database after multiple attempts.")

def get_person_article_pmid(mysql_cursor):
    """Fetch PMIDs from the person_article table where userAssertion = 'ACCEPTED'."""
    get_metadata_query = (
        """
        SELECT DISTINCT
            CAST(pmid AS CHAR) AS pmid
        FROM person_article
        WHERE userAssertion = 'ACCEPTED'
        """
    )
    mysql_cursor.execute(get_metadata_query)
    pmid = [rec['pmid'] for rec in mysql_cursor.fetchall()]
    logger.info(f"Retrieved {len(pmid)} PMIDs from person_article table.")
    return pmid

def create_nih_API_url(article_pmid_list):
    """Create NIH RCR API URL."""
    API_BASE_URL = "https://icite.od.nih.gov/api/pubs?pmids="
    combined_pmids = ",".join(article_pmid_list)
    full_api_url = API_BASE_URL + combined_pmids
    return full_api_url

def get_dict_value(dict_obj, *keys):
    """Safely get nested dictionary values."""
    for key in keys:
        dict_obj = dict_obj.get(key)
        if dict_obj is None:
            return None
    return dict_obj

def get_nih_records(nih_api_url, max_retries=5, backoff_factor=1):
    """Fetch NIH records with retry logic."""
    for retry in range(max_retries):
        try:
            response = requests.get(nih_api_url, timeout=30)
            response.raise_for_status()
            nih_record = response.json()

            if isinstance(nih_record, dict):
                process_records = get_dict_value(nih_record, "data")
                if not process_records:
                    logger.warning(f"No data found in API response for URL: {nih_api_url}")
                    return []
                logger.debug(f"Retrieved {len(process_records)} records from API.")
                return process_records
            else:
                logger.error(f"Invalid data format received from API URL: {nih_api_url}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"API request error on attempt {retry+1} for URL {nih_api_url}: {e}")
            sleep_time = backoff_factor * (2 ** retry) + random.uniform(0, 1)
            time.sleep(sleep_time)
        except Exception as e:
            logger.error(f"Unexpected error on attempt {retry+1} for URL {nih_api_url}: {e}")
            sleep_time = backoff_factor * (2 ** retry) + random.uniform(0, 1)
            time.sleep(sleep_time)

    logger.error(f"Failed to retrieve data after {max_retries} attempts for URL {nih_api_url}")
    return []

def write_records_to_csv(records, csv_files):
    """Write records to CSV files for analysis_nih, analysis_nih_cites, and analysis_nih_cites_clin."""
    nih_writer, cites_writer, cites_clin_writer = csv_files
    nih_count = 0
    cites_count = 0
    cites_clin_count = 0

    for record in records:
        try:
            # Write to analysis_nih
            nih_record = [
                get_dict_value(record, "pmid"),
                get_dict_value(record, "year"),
                get_dict_value(record, "is_research_article"),
                get_dict_value(record, "is_clinical"),
                get_dict_value(record, "relative_citation_ratio"),
                get_dict_value(record, "nih_percentile"),
                get_dict_value(record, "citation_count"),
                get_dict_value(record, "citations_per_year"),
                get_dict_value(record, "expected_citations_per_year"),
                get_dict_value(record, "field_citation_rate"),
                get_dict_value(record, "provisional"),
                get_dict_value(record, "doi"),
                get_dict_value(record, "human"),
                get_dict_value(record, "animal"),
                get_dict_value(record, "molecular_cellular"),
                get_dict_value(record, "apt"),
                get_dict_value(record, "x_coord"),
                get_dict_value(record, "y_coord")
            ]
            nih_writer.writerow(nih_record)
            nih_count += 1

            citing_pmid = get_dict_value(record, "pmid")

            # Write to analysis_nih_cites
            if record.get("cited_by"):
                for cited_by in record["cited_by"]:
                    cites_writer.writerow([cited_by, citing_pmid])
                    cites_count += 1
            if record.get("references"):
                for ref in record["references"]:
                    cites_writer.writerow([ref, citing_pmid])
                    cites_count += 1

            # Write to analysis_nih_cites_clin
            if record.get("cited_by_clin"):
                for cited_by_clin in record["cited_by_clin"]:
                    cites_clin_writer.writerow([cited_by_clin, citing_pmid])
                    cites_clin_count +=1

        except Exception as e:
            logger.error(f"Error writing record {get_dict_value(record, 'pmid')}: {e}")
            continue  # Skip this record and continue with the next

    logger.debug(f"Wrote {nih_count} records to analysis_nih CSV.")
    logger.debug(f"Wrote {cites_count} records to analysis_nih_cites CSV.")
    logger.debug(f"Wrote {cites_clin_count} records to analysis_nih_cites_clin CSV.")

def load_data_into_db(mysql_db, mysql_cursor, table_name, csv_file_path, columns):
    """Use LOAD DATA LOCAL INFILE to bulk load data into the database."""
    columns_str = ', '.join(f'`{col}`' for col in columns)
    sql = f"""
    LOAD DATA LOCAL INFILE '{csv_file_path}'
    INTO TABLE {table_name}
    FIELDS TERMINATED BY '\\t'
    LINES TERMINATED BY '\\n'
    ({columns_str});
    """
    try:
        mysql_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_before = mysql_cursor.fetchone()['COUNT(*)']

        mysql_cursor.execute(sql)
        mysql_db.commit()

        mysql_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = mysql_cursor.fetchone()['COUNT(*)']

        logger.info(f"Data loaded into {table_name} from {csv_file_path}")
        logger.info(f"Rows before: {count_before}, Rows after: {count_after}, Rows inserted: {count_after - count_before}")

    except pymysql.err.MySQLError as e:
        logger.error(f"Error loading data into {table_name}: {e}")
        raise

def truncate_table(mysql_cursor, table_name):
    """Truncate a table."""
    truncate_query = f"TRUNCATE TABLE {table_name};"
    mysql_cursor.execute(truncate_query)
    logger.info(f"Existing {table_name} table truncated.")

def create_staging_tables(mysql_cursor, tables):
    """Create staging tables (table_new) with same structure as production tables."""
    for table_name in tables:
        staging_table = f"{table_name}_new"
        # Drop if exists from failed prior run
        mysql_cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        # Create staging table with same structure
        mysql_cursor.execute(f"CREATE TABLE {staging_table} LIKE {table_name}")
        logger.info(f"Created staging table: {staging_table}")

def atomic_table_swap(mysql_db, mysql_cursor, tables):
    """
    Atomically swap staging tables with production tables.
    Production tables become backup, staging becomes production.
    """
    # Build the RENAME statement for all tables at once (atomic operation)
    rename_parts = []
    for table_name in tables:
        staging_table = f"{table_name}_new"
        backup_table = f"{table_name}_backup"
        # Drop old backup tables first
        mysql_cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
        rename_parts.append(f"{table_name} TO {backup_table}")
        rename_parts.append(f"{staging_table} TO {table_name}")

    rename_sql = "RENAME TABLE " + ", ".join(rename_parts)
    logger.info(f"Executing atomic table swap: {rename_sql}")

    try:
        mysql_cursor.execute(rename_sql)
        mysql_db.commit()
        logger.info("Atomic table swap completed successfully")
        return True
    except Exception as e:
        logger.error(f"Atomic table swap failed: {e}")
        return False

def restore_from_backup(mysql_db, mysql_cursor, tables):
    """Restore production tables from backup tables."""
    logger.warning("Attempting to restore from backup tables...")
    try:
        rename_parts = []
        for table_name in tables:
            backup_table = f"{table_name}_backup"
            # Check if backup exists
            mysql_cursor.execute(f"SHOW TABLES LIKE '{backup_table}'")
            if mysql_cursor.fetchone():
                # Drop current table if it exists
                mysql_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                rename_parts.append(f"{backup_table} TO {table_name}")

        if rename_parts:
            rename_sql = "RENAME TABLE " + ", ".join(rename_parts)
            mysql_cursor.execute(rename_sql)
            mysql_db.commit()
            logger.info("Restored from backup tables successfully")
            return True
        else:
            logger.error("No backup tables found to restore from")
            return False
    except Exception as e:
        logger.error(f"Failed to restore from backup: {e}")
        return False

def cleanup_staging_tables(mysql_cursor, tables):
    """Clean up staging tables after a failed run."""
    for table_name in tables:
        staging_table = f"{table_name}_new"
        mysql_cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
        logger.info(f"Cleaned up staging table: {staging_table}")

def validate_data(mysql_cursor, staging_table, production_table, min_rows=100, min_percentage=80):
    """
    Validate staging table has sufficient data before swap.
    - Must have at least min_rows
    - Must have at least min_percentage of production table's row count
    """
    mysql_cursor.execute(f"SELECT COUNT(*) as cnt FROM {staging_table}")
    staging_count = mysql_cursor.fetchone()['cnt']

    mysql_cursor.execute(f"SELECT COUNT(*) as cnt FROM {production_table}")
    production_count = mysql_cursor.fetchone()['cnt']

    logger.info(f"Validation: {staging_table} has {staging_count} rows, "
                f"{production_table} has {production_count} rows")

    # Check minimum rows
    if staging_count < min_rows:
        logger.error(f"Validation FAILED: {staging_table} has {staging_count} rows "
                     f"(minimum required: {min_rows})")
        return False

    # Check percentage of production (only if production has data)
    if production_count > 0:
        percentage = (staging_count / production_count) * 100
        logger.info(f"Staging data is {percentage:.1f}% of production data")
        if percentage < min_percentage:
            logger.error(f"Validation FAILED: staging data ({staging_count} rows) is only "
                         f"{percentage:.1f}% of production ({production_count} rows). "
                         f"Minimum required: {min_percentage}%")
            return False

    logger.info(f"Validation PASSED for {staging_table}")
    return True

#########

if __name__ == '__main__':
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

    if not all([DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME]):
        raise EnvironmentError("Database credentials are not fully set in environment variables.")

    # Create a MySQL connection to the database
    reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    reciter_db_cursor = reciter_db.cursor(pymysql.cursors.DictCursor)

    # Define tables to manage
    NIH_TABLES = ["analysis_nih", "analysis_nih_cites", "analysis_nih_cites_clin"]

    # Create staging tables (instead of truncating production tables)
    logger.info("Creating staging tables for zero-downtime update...")
    create_staging_tables(reciter_db_cursor, NIH_TABLES)
    reciter_db.commit()

    # Get PMIDs
    person_article_pmid = get_person_article_pmid(reciter_db_cursor)

    if not person_article_pmid:
        logger.error("No PMIDs retrieved from the database. Cleaning up and exiting.")
        cleanup_staging_tables(reciter_db_cursor, NIH_TABLES)
        reciter_db.commit()
        reciter_db_cursor.close()
        reciter_db.close()
        exit(1)

    # Prepare columns for analysis_nih table
    analysis_nih_columns = [
        'pmid',
        'year',
        'is_research_article',
        'is_clinical',
        'relative_citation_ratio',
        'nih_percentile',
        'citation_count',
        'citations_per_year',
        'expected_citations_per_year',
        'field_citation_rate',
        'provisional',
        'doi',
        'human',
        'animal',
        'molecular_cellular',
        'apt',
        'x_coord',
        'y_coord'
    ]

    # File paths for CSV files
    nih_csv_path = 'analysis_nih.csv'
    cites_csv_path = 'analysis_nih_cites.csv'
    cites_clin_csv_path = 'analysis_nih_cites_clin.csv'

    # Open CSV files
    nih_csv = open(nih_csv_path, mode='w+', newline='', encoding='utf-8')
    cites_csv = open(cites_csv_path, mode='w+', newline='', encoding='utf-8')
    cites_clin_csv = open(cites_clin_csv_path, mode='w+', newline='', encoding='utf-8')

    # Log file paths
    logger.info(f"NIH CSV file path: {nih_csv_path}")
    logger.info(f"Cites CSV file path: {cites_csv_path}")
    logger.info(f"Cites Clin CSV file path: {cites_clin_csv_path}")

    # Prepare CSV writers
    nih_writer = csv.writer(nih_csv, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    cites_writer = csv.writer(cites_csv, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    cites_clin_writer = csv.writer(cites_clin_csv, delimiter='\t', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

    # Fibonacci sequence for retries
    fibonacci_sequence = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]

    total_records_retrieved = 0

    # Process each batch
    for i in range(0, len(person_article_pmid), 150):
        # Get current batch of PMIDs
        pmid_batch = person_article_pmid[i:i+150]
        logger.info(f"Processing batch {i//150 +1}: PMIDs {i+1} to {i+len(pmid_batch)}")

        # Make API call
        api_url = create_nih_API_url(pmid_batch)

        retries = 0
        success = False
        while retries < len(fibonacci_sequence):
            # Wait for the appropriate amount of time before making the API call
            time.sleep(1)
            nih_records = get_nih_records(api_url)
            if nih_records:
                success = True
                total_records_retrieved += len(nih_records)
                # Write records to CSV files
                write_records_to_csv(nih_records, (nih_writer, cites_writer, cites_clin_writer))
                break
            else:
                retries += 1
                if retries < len(fibonacci_sequence):
                    sleep_time = fibonacci_sequence[retries]
                else:
                    sleep_time = fibonacci_sequence[-1]
                logger.warning(f'API call failed. Retry attempt {retries} after {sleep_time} seconds.')
                time.sleep(sleep_time)

        if not success:
            logger.error('Max retry attempts exceeded for this batch. Moving to the next batch.')
            continue  # Skip to the next batch

    logger.info(f"Total records retrieved from API: {total_records_retrieved}")

    # Close CSV files
    nih_csv.close()
    cites_csv.close()
    cites_clin_csv.close()

    # Track if data load was successful
    load_success = True

    # Check if CSV files are not empty before loading into STAGING tables
    if os.path.getsize(nih_csv_path) > 0:
        # Load data into the STAGING table (not production)
        load_data_into_db(reciter_db, reciter_db_cursor, "analysis_nih_new", nih_csv_path, analysis_nih_columns)
    else:
        logger.error(f"No data in {nih_csv_path}. Skipping data load for analysis_nih.")
        load_success = False

    if os.path.getsize(cites_csv_path) > 0:
        load_data_into_db(reciter_db, reciter_db_cursor, "analysis_nih_cites_new", cites_csv_path, ["cited_pmid", "citing_pmid"])
    else:
        logger.error(f"No data in {cites_csv_path}. Skipping data load for analysis_nih_cites.")
        load_success = False

    if os.path.getsize(cites_clin_csv_path) > 0:
        load_data_into_db(reciter_db, reciter_db_cursor, "analysis_nih_cites_clin_new", cites_clin_csv_path, ["cited_pmid", "citing_pmid"])
    else:
        logger.error(f"No data in {cites_clin_csv_path}. Skipping data load for analysis_nih_cites_clin.")
        load_success = False

    # Validate staging data before swap
    if load_success:
        logger.info("Validating staging table data...")
        # Check that staging has at least 80% of production data and minimum 100 rows
        if not validate_data(reciter_db_cursor, "analysis_nih_new", "analysis_nih",
                             min_rows=100, min_percentage=80):
            logger.error("Validation failed: aborting table swap to protect production data")
            load_success = False

    # Perform atomic table swap if data load was successful
    if load_success:
        logger.info("Data load successful. Performing atomic table swap...")
        if atomic_table_swap(reciter_db, reciter_db_cursor, NIH_TABLES):
            logger.info("SUCCESS: NIH data updated with zero downtime")
        else:
            logger.error("Atomic table swap failed. Attempting to restore from backup...")
            restore_from_backup(reciter_db, reciter_db_cursor, NIH_TABLES)
    else:
        logger.error("Data load failed. Cleaning up staging tables...")
        cleanup_staging_tables(reciter_db_cursor, NIH_TABLES)
        reciter_db.commit()
        logger.error("Production tables remain unchanged.")

    # Do not delete temporary files to allow inspection
    # os.remove(nih_csv_path)
    # os.remove(cites_csv_path)
    # os.remove(cites_clin_csv_path)
    # logger.info("Temporary files deleted.")

    # Close DB connection
    reciter_db_cursor.close()
    reciter_db.close()
    logger.info("Database connection closed.")
