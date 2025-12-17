# abstractImport.py

import boto3
import csv
import logging
import pymysql.cursors
import pymysql.err
import sys
import time
import os
import concurrent.futures

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------------------
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# DynamoDB concurrency settings
CHUNK_SIZE = 100       # Max items per batch_get_item call
MAX_WORKERS = 5        # Number of threads for parallel fetching

# ------------------------------------------------------------------------------
# Database Connection
# ------------------------------------------------------------------------------
def connect_mysql_server(db_user, db_pass, db_host, db_name):
    """Function to connect to MySQL database"""
    try:
        mysql_db = pymysql.connect(
            user=db_user,
            password=db_pass,
            database=db_name,
            host=db_host,
            autocommit=True,
            local_infile=True,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info(f"Connected to database server: {db_host}, database: {db_name}, user: {db_user}")
        return mysql_db
    except pymysql.err.MySQLError as err:
        logger.error(f"{time.ctime()} -- Error connecting to the database: {err}")
        sys.exit(1)

# ------------------------------------------------------------------------------
# Fetch All Missing PMIDs
# ------------------------------------------------------------------------------
def fetch_missing_pmids(mysql_conn):
    """
    Returns a list of all PMIDs that exist in analysis_summary_article
    but do NOT exist in reporting_abstracts.
    """
    sql = """
        SELECT DISTINCT p.pmid AS pmid
        FROM analysis_summary_article p
        LEFT JOIN reporting_abstracts a ON a.pmid = p.pmid
        WHERE a.pmid IS NULL
    """
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        return [row["pmid"] for row in rows]

# ------------------------------------------------------------------------------
# Extract Abstract Text
# ------------------------------------------------------------------------------
def get_abstract(item):
    """
    Extracts the abstract text from a DynamoDB item representing a PubMed article.
    Handles labeled abstract segments if present.
    """
    medline_citation = item.get("pubmedarticle", {}).get("medlinecitation")
    if not medline_citation:
        return ""

    article = medline_citation.get("article")
    if not article:
        return ""

    publication_abstract = article.get("publicationAbstract")
    if not publication_abstract:
        return ""

    abstract_texts = []
    for abstract_part in publication_abstract.get("abstractTexts", []):
        label = abstract_part.get("abstractTextLabel")
        text = abstract_part.get("abstractText")
        if text:
            label_text = f"{label}: " if label else ""
            abstract_texts.append(label_text + text)

    return " ".join(abstract_texts) if abstract_texts else ""

# ------------------------------------------------------------------------------
# Batch Fetch Abstracts from DynamoDB
# ------------------------------------------------------------------------------
def fetch_abstracts_for_chunk(chunk_pmids):
    """
    Performs a single batch_get_item call for the given chunk of PMIDs.
    Returns a list of (pmid, abstract_text) pairs.
    """
    dynamodb = boto3.resource("dynamodb")
    client = dynamodb.meta.client

    # Prepare Keys for batch_get_item
    keys = [{"pmid": pmid} for pmid in chunk_pmids]

    # Perform batch_get_item
    response = client.batch_get_item(
        RequestItems={
            "PubMedArticle": {"Keys": keys}
        }
    )

    items = response["Responses"].get("PubMedArticle", [])
    results = []
    for item in items:
        pmid = item.get("pmid")
        if pmid:
            abstract_text = get_abstract(item)
            results.append((pmid, abstract_text))

    return results

# ------------------------------------------------------------------------------
# Bulk-Load a Single CSV into reporting_abstracts
# ------------------------------------------------------------------------------
def load_csv_into_reporting_abstracts(mysql_conn, csv_path):
    with mysql_conn.cursor() as cursor:
        cwd = os.getcwd()
        full_csv_path = os.path.join(cwd, csv_path).replace("\\", "/")  # Ensure correct path format

        load_query = (
            "LOAD DATA LOCAL INFILE '{path}' "
            "INTO TABLE reporting_abstracts "
            "FIELDS TERMINATED BY '\t' ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\n' "
            "IGNORE 1 LINES (pmid, abstract);"
        ).format(path=full_csv_path)

        cursor.execute(load_query)
        logger.info(f"{time.ctime()} -- {csv_path} loaded into reporting_abstracts.")

        update_query = (
            "UPDATE reporting_abstracts "
            "SET abstractVarchar = CAST(abstract AS CHAR(15000)) "
            "WHERE abstractVarchar IS NULL;"
        )
        cursor.execute(update_query)
        logger.info(f"{time.ctime()} -- reporting_abstracts updated with varchar equivalents.")

# ------------------------------------------------------------------------------
# Main Script Logic
# ------------------------------------------------------------------------------
def main():
    # 1) Connect to MySQL
    mysql_conn = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)

    while True:
        # 2) Fetch all missing PMIDs
        all_pmids = fetch_missing_pmids(mysql_conn)
        if not all_pmids:
            logger.info("No more missing abstracts. We are done.")
            break

        logger.info(f"Found {len(all_pmids)} PMIDs needing abstracts.")

        # 3) Remove any existing abstract.csv
        csv_path = "abstract.csv"
        if os.path.exists(csv_path):
            os.remove(csv_path)

        # 4) Chunk the PMIDs
        chunks = [
            all_pmids[i : i + CHUNK_SIZE]
            for i in range(0, len(all_pmids), CHUNK_SIZE)
        ]
        logger.info(f"Created {len(chunks)} chunk(s). Each chunk up to {CHUNK_SIZE} PMIDs.")

        # Accumulate all results in memory for this iteration
        all_results = []

        # 5) Parallel fetch from DynamoDB
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_chunk = {
                executor.submit(fetch_abstracts_for_chunk, c): c for c in chunks
            }
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    chunk_result = future.result()
                    all_results.extend(chunk_result)
                except Exception as e:
                    logger.exception(f"Error fetching chunk: {e}")

        logger.info(f"Fetched abstracts for {len(all_results)} PMIDs in this cycle.")

        # 6) Write to CSV
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["pmid", "abstract"])
            for pmid, abstract_text in all_results:
                writer.writerow([pmid, abstract_text])

        # 7) Load CSV into DB
        load_csv_into_reporting_abstracts(mysql_conn, csv_path)

        # We then loop again in case there are additional PMIDs that
        # appeared or newly became missing. Usually, you won't see more,
        # but if your data is updated behind the scenes, it handles that too.

    mysql_conn.close()
    logger.info("All missing abstracts have now been imported.")


if __name__ == "__main__":
    main()
