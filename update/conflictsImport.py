# conflictsImport.py

import boto3
import logging
import pymysql.cursors
import pymysql.err
import random
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

# Quiet botocore's per-call credential/endpoint chatter so pipeline logs stay readable.
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

# ------------------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------------------
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# ------------------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------------------
# DynamoDB fetch
CHUNK_SIZE = 100              # Max keys per batch_get_item call (DynamoDB hard limit)
MAX_WORKERS = 5               # Threads for parallel fetching
MAX_UNPROCESSED_RETRIES = 8   # Backoff retries for keys DynamoDB reports as unprocessed

# Insert
INSERT_BATCH_SIZE = 200       # Rows per executemany batch (kept well under max_allowed_packet)

# Scope. COI statements are only collected for recent articles; this preserves
# the filter the importer has always used. Widening it would backfill decades
# of articles on the next nightly run.
MIN_ARTICLE_YEAR = 2017

# Loop safety
MAX_CYCLES = 25               # Hard cap on fetch/insert cycles; a healthy run needs 1-2

# Dry run
DRY_RUN = "--dry-run" in sys.argv
DRY_RUN_SAMPLE = 500          # PMIDs processed when --dry-run is passed
DRY_RUN_TABLE = "reporting_conflicts_dryrun"


# ------------------------------------------------------------------------------
# Database Connection
# ------------------------------------------------------------------------------
def connect_mysql_server(db_user, db_pass, db_host, db_name):
    """Connect to the MariaDB database."""
    try:
        mysql_db = pymysql.connect(
            user=db_user,
            password=db_pass,
            database=db_name,
            host=db_host,
            autocommit=True,
            charset="utf8mb4",
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
    Returns every PMID that exists in analysis_summary_article but has no
    matching row in reporting_conflicts.

    `p.pmid > 0` is load-bearing: STEP 6b of the reporting SP injects one row per
    external_article (Scopus/WOS/OpenAlex) under a synthetic NEGATIVE pmid, and
    analysis_summary_article.pmid is itself nullable with DEFAULT 0. Neither has a
    PubMedArticle record, so without this guard they are reported as missing on
    every run forever and the no-progress warning below fires unconditionally.
    """
    sql = """
        SELECT DISTINCT p.pmid AS pmid
        FROM analysis_summary_article p
        LEFT JOIN reporting_conflicts a ON a.pmid = p.pmid
        WHERE a.pmid IS NULL
          AND p.articleYear >= %s
          AND p.pmid > 0
    """
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql, (MIN_ARTICLE_YEAR,))
        return [row["pmid"] for row in cursor.fetchall()]


# ------------------------------------------------------------------------------
# Extract COI Statement
# ------------------------------------------------------------------------------
def get_conflicts(item):
    """
    Extracts the conflict-of-interest statement from a DynamoDB item
    representing a PubMed article. Returns "" when none is present -- an empty
    row still has to be written, otherwise the PMID stays "missing" forever and
    every subsequent run re-fetches it.
    """
    medline_citation = item.get("pubmedarticle", {}).get("medlinecitation")
    if not medline_citation:
        return ""
    return medline_citation.get("coiStatement") or ""


# ------------------------------------------------------------------------------
# Fetch COI Statements from DynamoDB
# ------------------------------------------------------------------------------
def fetch_conflicts_for_chunk(chunk_pmids):
    """
    Fetches one chunk of PMIDs from DynamoDB via batch_get_item. Any keys that
    DynamoDB reports as unprocessed (throttling) are retried with exponential
    backoff so they are not silently lost. Returns (pmid, conflictStatement) pairs.
    """
    client = boto3.resource("dynamodb").meta.client

    request_keys = [{"pmid": pmid} for pmid in chunk_pmids]
    results = []
    attempt = 0

    while request_keys:
        response = client.batch_get_item(
            RequestItems={"PubMedArticle": {"Keys": request_keys}}
        )

        for item in response["Responses"].get("PubMedArticle", []):
            pmid = item.get("pmid")
            if pmid is not None:
                results.append((pmid, get_conflicts(item)))

        request_keys = (
            response.get("UnprocessedKeys", {})
            .get("PubMedArticle", {})
            .get("Keys", [])
        )
        if request_keys:
            attempt += 1
            if attempt > MAX_UNPROCESSED_RETRIES:
                logger.warning(
                    f"{len(request_keys)} key(s) still unprocessed after "
                    f"{MAX_UNPROCESSED_RETRIES} retries; skipping this chunk's remainder."
                )
                break
            time.sleep(min(0.1 * (2 ** attempt), 5.0))

    return results


def fetch_all_conflicts(pmids):
    """Fetches COI statements for all given PMIDs from DynamoDB in parallel."""
    chunks = [pmids[i:i + CHUNK_SIZE] for i in range(0, len(pmids), CHUNK_SIZE)]
    logger.info(f"Created {len(chunks)} chunk(s). Each chunk up to {CHUNK_SIZE} PMIDs.")

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_conflicts_for_chunk, c): c for c in chunks}
        for future in concurrent.futures.as_completed(futures):
            try:
                all_results.extend(future.result())
            except Exception as e:
                logger.exception(f"Error fetching chunk: {e}")
    return all_results


# ------------------------------------------------------------------------------
# Insert COI Statements
# ------------------------------------------------------------------------------
def insert_conflicts(mysql_conn, results, target_table="reporting_conflicts"):
    """
    Inserts (pmid, conflictStatement) pairs with a parameterized, batched INSERT.

    pymysql binds every value as a query parameter, so COI statements containing
    double quotes, tabs, newlines or backslashes are stored verbatim. The
    previous CSV + LOAD DATA INFILE path could not parse such content and
    silently dropped or concatenated the affected rows -- the same defect that
    was removed from abstractImport.py.
    """
    if not results:
        logger.info("No conflict statements to insert.")
        return 0

    insert_sql = f"INSERT INTO {target_table} (pmid, conflictStatement) VALUES (%s, %s)"
    inserted = 0
    with mysql_conn.cursor() as cursor:
        for i in range(0, len(results), INSERT_BATCH_SIZE):
            batch = results[i:i + INSERT_BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            inserted += len(batch)
        logger.info(f"{time.ctime()} -- Inserted {inserted} row(s) into {target_table}.")

        cursor.execute(
            f"UPDATE {target_table} "
            f"SET conflictsVarchar = CAST(conflictStatement AS CHAR(15000)) "
            f"WHERE conflictsVarchar IS NULL"
        )
        logger.info(f"{time.ctime()} -- {target_table} updated with varchar equivalents.")
    return inserted


# ------------------------------------------------------------------------------
# Dry Run
# ------------------------------------------------------------------------------
def run_dry_run(mysql_conn):
    """
    Verifies the fetch -> insert path end to end without modifying
    reporting_conflicts: a random sample of missing PMIDs is processed into a
    session-private TEMPORARY table, then verified and discarded.
    """
    logger.info("=== DRY RUN === reporting_conflicts will NOT be modified.")

    all_pmids = fetch_missing_pmids(mysql_conn)
    logger.info(f"{len(all_pmids)} PMID(s) currently missing conflict statements in production.")
    if not all_pmids:
        logger.info("Nothing missing; no sample to process.")
        mysql_conn.close()
        return

    sample = random.sample(all_pmids, min(DRY_RUN_SAMPLE, len(all_pmids)))
    logger.info(f"Processing a random sample of {len(sample)} PMID(s) through the new insert path.")

    try:
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"CREATE TEMPORARY TABLE {DRY_RUN_TABLE} LIKE reporting_conflicts")

        all_results = fetch_all_conflicts(sample)
        logger.info(f"Fetched {len(all_results)} item(s) from DynamoDB (requested {len(sample)}).")
        if not all_results:
            logger.error("DRY RUN FAILED: DynamoDB returned nothing for the sample.")
            return

        poison = [
            (p, c) for p, c in all_results
            if c and any(ch in c for ch in ('"', '\t', '\n', '\r', '\\'))
        ]
        logger.info(
            f"{len(poison)} of {len(all_results)} fetched statements contain "
            f"quotes/tabs/newlines/backslashes -- the content the old LOAD DATA "
            f"path silently dropped."
        )
        if poison:
            logger.info(f"Example poison statement (PMID {poison[0][0]}): {poison[0][1][:160]!r}")

        inserted = insert_conflicts(mysql_conn, all_results, target_table=DRY_RUN_TABLE)

        with mysql_conn.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) c, COUNT(DISTINCT pmid) d, "
                f"SUM(pmid IS NULL) nullp, SUM(conflictsVarchar IS NULL) nullv "
                f"FROM {DRY_RUN_TABLE}"
            )
            stats = cursor.fetchone()

        # An empty COI statement casts to '' rather than NULL, so nullv should be
        # 0 even for the many articles that carry no statement at all.
        counts_ok = (
            stats["c"] == len(all_results)
            and not stats["nullp"]
            and not stats["nullv"]
        )

        # Content-integrity check: re-read the longest poison statement verbatim.
        integrity_ok = True
        if poison:
            worst_pmid, worst_text = max(poison, key=lambda x: len(x[1]))
            with mysql_conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT conflictStatement FROM {DRY_RUN_TABLE} WHERE pmid = %s", (worst_pmid,)
                )
                stored = cursor.fetchone()["conflictStatement"]
            if isinstance(stored, bytes):
                stored = stored.decode("utf-8")
            integrity_ok = (stored == worst_text)
            logger.info(
                f"Content-integrity check on PMID {worst_pmid} "
                f"({len(worst_text)} chars, contains poison characters): "
                f"{'MATCH' if integrity_ok else 'MISMATCH'}"
            )

        if counts_ok and integrity_ok:
            logger.info(
                f"DRY RUN PASSED -- {inserted} row(s) inserted; {stats['c']} present; "
                f"{stats['d']} distinct PMIDs; 0 NULL pmids; 0 NULL conflictsVarchar; "
                f"content stored verbatim."
            )
        else:
            logger.error(
                f"DRY RUN FAILED -- rows={stats['c']} (expected {len(all_results)}); "
                f"null_pmid={stats['nullp']}; null_varchar={stats['nullv']}; "
                f"integrity_ok={integrity_ok}"
            )
    finally:
        with mysql_conn.cursor() as cursor:
            cursor.execute(f"DROP TEMPORARY TABLE IF EXISTS {DRY_RUN_TABLE}")
        logger.info(f"Scratch table {DRY_RUN_TABLE} dropped.")
        mysql_conn.close()


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    mysql_conn = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)

    if DRY_RUN:
        run_dry_run(mysql_conn)
        return

    prev_missing = None
    for cycle in range(1, MAX_CYCLES + 1):
        all_pmids = fetch_missing_pmids(mysql_conn)
        if not all_pmids:
            logger.info("No more missing conflict statements. We are done.")
            break

        logger.info(f"Cycle {cycle}: found {len(all_pmids)} PMID(s) needing conflict statements.")

        # Safety net: if a cycle does not reduce the missing count, the
        # remaining PMIDs cannot be resolved (no DynamoDB record). Stop rather
        # than loop forever -- the failure mode that hung the nightly pipeline.
        if prev_missing is not None and len(all_pmids) >= prev_missing:
            logger.warning(
                f"No progress since the previous cycle ({len(all_pmids)} PMID(s) "
                f"still missing); stopping. These PMIDs have no retrievable record."
            )
            break
        prev_missing = len(all_pmids)

        all_results = fetch_all_conflicts(all_pmids)
        logger.info(f"Fetched conflict statements for {len(all_results)} PMID(s) from DynamoDB.")
        insert_conflicts(mysql_conn, all_results)
    else:
        logger.warning(
            f"Reached the {MAX_CYCLES}-cycle safety limit with conflict statements "
            f"still missing; stopping. A healthy run converges in 1-2 cycles -- investigate."
        )

    mysql_conn.close()
    logger.info("Conflict statement import complete.")


if __name__ == "__main__":
    main()
