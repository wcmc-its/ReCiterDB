# retrieveArticleProvenance.py
#
# Nightly ETL step that scans the ReCiter DynamoDB `ArticleProvenance` table and
# loads it into the reciterdb `article_provenance` table. Powers the Publication
# Manager "date a publication was first retrieved" display in /curate
# (wcmc-its/ReCiter-Publication-Manager#737); backend half of ReCiterDB#95.
#
# Source table (reciter.service.dynamo.ArticleProvenanceServiceImpl):
#   - COMPOSITE key: `uid` (HASH, the personIdentifier/CWID) + `articleId` (RANGE,
#     the PMID as a String). One item per (person, article) pair.
#   - `frd` = first retrieval date, epoch SECONDS, written with if_not_exists so it
#     is immutable once set (the first time that person retrieved that article).
#   - `rs`  = first retrieval strategy (PM_UI_SEARCH, PM_AUTHOR, ...).
#   - `src` = source (PM, CTSC, GS, MAN, MAN_FROM_PM, ...).
#   - `ads` = String Set of all strategies seen (not loaded here).
#
# reciterdb target is keyed on (pmid, personIdentifier) -- it mirrors the DynamoDB
# composite key exactly (one row per person+article), so no cross-person collapse
# is performed. frd (epoch seconds, UTC) is converted to a DATETIME on load to
# match the rest of reciterdb; PM formats it for display.
#
# Memory: rows are streamed into the staging table one scan page at a time
# (INSERT IGNORE), so peak RSS is bounded to one page regardless of corpus size.
# INSERT IGNORE also collapses any (pmid, personIdentifier) collision (e.g. a
# case-variant uid under the utf8mb4_unicode_ci PK) rather than aborting the load.
#
# Atomicity: mirrors retrieveNIH.py -- load into a `article_provenance_new`
# staging table, validate it against production, then RENAME-swap. A failure here
# leaves production untouched. run_all.py runs this step as NON-FATAL so a hiccup
# does not block the nightly indexing SP (PM reads this table directly; nothing
# downstream depends on it).

import os
import sys
import time
import random
import logging
import faulthandler
import signal
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import pymysql.cursors
import pymysql.err

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('retrieveArticleProvenance.log', mode='w'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

faulthandler.enable(file=sys.stderr, all_threads=True)
faulthandler.register(signal.SIGUSR1, file=sys.stderr, all_threads=True)

DYNAMO_TABLE = 'ArticleProvenance'
TARGET_TABLE = 'article_provenance'
STAGING_TABLE = 'article_provenance_new'
BACKUP_TABLE = 'article_provenance_backup'
SCAN_PAGE_SIZE = 1000

# Validation floor: reject a partial/empty scan that would otherwise seed or
# replace production with too little data (matches retrieveNIH's min_rows).
MIN_STAGING_ROWS = 100
# Warn if more than this fraction of scanned items are skipped (data-quality signal).
SKIP_RATIO_WARN = 0.10

# Plausible bounds for frd (epoch seconds). Anything outside is treated as corrupt
# and stored NULL rather than a bogus DATETIME. Lower bound = 2000-01-01 UTC.
MIN_EPOCH_SECONDS = 946684800

# DDL kept in sync with setup/createDatabaseTableReciterDb.sql and
# setup/alter_add_article_provenance_v1.6.sql. Created defensively so a fresh
# environment that has not yet run the migration still works.
CREATE_TARGET_SQL = f"""
CREATE TABLE IF NOT EXISTS `{TARGET_TABLE}` (
  `pmid`               int(11)      NOT NULL,
  `personIdentifier`   varchar(128) NOT NULL,
  `firstRetrievalDate` datetime     DEFAULT NULL,
  `retrievalStrategy`  varchar(64)  DEFAULT NULL,
  `source`             varchar(32)  DEFAULT NULL,
  PRIMARY KEY (`pmid`, `personIdentifier`),
  KEY `idx_personIdentifier` (`personIdentifier`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

LOAD_COLUMNS = ['pmid', 'personIdentifier', 'firstRetrievalDate',
                'retrievalStrategy', 'source']


def connect_db(max_retries=5, backoff_factor=1):
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASSWORD']
    hostname = os.environ['DB_HOST']
    database = os.environ['DB_NAME']
    for retry in range(max_retries):
        try:
            conn = pymysql.connect(
                user=username,
                password=password,
                database=database,
                host=hostname,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
            )
            logger.info('Connected to database %s on %s', database, hostname)
            return conn
        except pymysql.err.MySQLError as err:
            logger.error('DB connect attempt %d failed: %s', retry + 1, err)
            time.sleep(backoff_factor * (2 ** retry) + random.uniform(0, 1))
    raise RuntimeError('Could not connect to database after retries')


def epoch_to_datetime_str(frd):
    """Convert an epoch-seconds value (DynamoDB Decimal/int/str) to a UTC
    'YYYY-MM-DD HH:MM:SS' string, or None if absent/invalid/out-of-range. frd is
    stored as UTC; Publication Manager formats it for display."""
    if frd is None:
        return None
    try:
        secs = int(frd)
    except (TypeError, ValueError):
        logger.warning('Unparseable frd value: %r; storing NULL', frd)
        return None
    # Reject implausible timestamps (corrupt data) rather than store a bogus year.
    upper = int(datetime.now(tz=timezone.utc).timestamp()) + 86400  # now + 1 day skew
    if secs < MIN_EPOCH_SECONDS or secs > upper:
        logger.warning('Out-of-range frd value: %r; storing NULL', frd)
        return None
    try:
        return datetime.fromtimestamp(secs, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    except (OverflowError, OSError, ValueError):
        logger.warning('Unconvertible frd value: %r; storing NULL', frd)
        return None


def scan_article_provenance(dynamo_table):
    """Generator that yields pages (lists of items) from a full scan of the
    ArticleProvenance table. Eventually-consistent read is fine for a nightly
    snapshot (frd is immutable once written)."""
    total = 0
    last_key = None
    while True:
        kwargs = {'Limit': SCAN_PAGE_SIZE}
        if last_key:
            kwargs['ExclusiveStartKey'] = last_key
        response = dynamo_table.scan(**kwargs)
        items = response.get('Items', [])
        total += len(items)
        if items:
            logger.info('Scanned %d items from %s (running total: %d).',
                        len(items), DYNAMO_TABLE, total)
            yield items
        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
    logger.info('Finished scanning %s. Total items: %d.', DYNAMO_TABLE, total)


def item_to_row(item):
    """Map one ArticleProvenance item to a row tuple matching LOAD_COLUMNS, or
    return None to skip (missing uid / non-numeric articleId)."""
    uid = item.get('uid')
    if not uid:
        return None
    try:
        pmid = int(item.get('articleId'))
    except (TypeError, ValueError):
        return False  # distinguishes bad-pmid from no-uid for counting
    first_retrieval = epoch_to_datetime_str(item.get('frd'))
    rs = item.get('rs')
    src = item.get('src')
    rs = str(rs)[:64] if rs is not None else None
    src = str(src)[:32] if src is not None else None
    return (pmid, str(uid)[:128], first_retrieval, rs, src)


def stream_into_staging(conn, cursor, dynamo_table):
    """Scan ArticleProvenance and INSERT IGNORE each page into the staging table.
    Streaming keeps peak memory to one page; INSERT IGNORE collapses any
    (pmid, personIdentifier) duplicate (incl. case-variant uids under the
    case-insensitive PK) instead of aborting. Returns scan/skip stats."""
    col_list = ', '.join(f'`{c}`' for c in LOAD_COLUMNS)
    placeholders = ', '.join(['%s'] * len(LOAD_COLUMNS))
    sql = f"INSERT IGNORE INTO `{STAGING_TABLE}` ({col_list}) VALUES ({placeholders})"

    scanned = skipped_no_uid = skipped_bad_pmid = 0
    for page in scan_article_provenance(dynamo_table):
        scanned += len(page)
        page_rows = []
        for item in page:
            row = item_to_row(item)
            if row is None:
                skipped_no_uid += 1
            elif row is False:
                skipped_bad_pmid += 1
            else:
                page_rows.append(row)
        if page_rows:
            cursor.executemany(sql, page_rows)
            conn.commit()

    cursor.execute(f"SELECT COUNT(*) AS c FROM `{STAGING_TABLE}`")
    staged = cursor.fetchone()['c']
    skipped = skipped_no_uid + skipped_bad_pmid
    logger.info('Scanned %d items; staged %d rows (skipped %d no-uid, %d bad-pmid).',
                scanned, staged, skipped_no_uid, skipped_bad_pmid)
    if scanned and (skipped / scanned) > SKIP_RATIO_WARN:
        logger.warning('High skip ratio: %d/%d (%.1f%%) of scanned items were '
                       'dropped; staged table may be partial.',
                       skipped, scanned, 100.0 * skipped / scanned)
    return {'scanned': scanned, 'staged': staged}


def create_staging_table(cursor):
    """(Re)create the staging table as an empty clone of production."""
    cursor.execute(CREATE_TARGET_SQL)  # ensure production exists (fresh env)
    cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE}`")
    cursor.execute(f"CREATE TABLE `{STAGING_TABLE}` LIKE `{TARGET_TABLE}`")
    logger.info('Created staging table %s', STAGING_TABLE)


def recover_orphaned_backup(conn, cursor):
    """Self-heal from a prior run that died after RENAMEing production away but
    before the swap completed: if production is gone but a backup exists, restore
    it so we never fabricate an empty production table over a good backup."""
    cursor.execute(f"SHOW TABLES LIKE '{TARGET_TABLE}'")
    if cursor.fetchone():
        return
    cursor.execute(f"SHOW TABLES LIKE '{BACKUP_TABLE}'")
    if cursor.fetchone():
        logger.warning('Production %s missing but %s present (orphaned prior run); '
                       'restoring backup before proceeding.', TARGET_TABLE, BACKUP_TABLE)
        cursor.execute(f"RENAME TABLE `{BACKUP_TABLE}` TO `{TARGET_TABLE}`")
        conn.commit()


def validate_staging(cursor, min_rows=MIN_STAGING_ROWS, min_percentage=80):
    """Guard against replacing a healthy production table with a partial/empty
    scan. Requires the staging table to meet a row floor and (when production
    already has data) to be at least min_percentage of the production row count."""
    cursor.execute(f"SELECT COUNT(*) AS c FROM `{STAGING_TABLE}`")
    staging_count = cursor.fetchone()['c']
    cursor.execute(f"SELECT COUNT(*) AS c FROM `{TARGET_TABLE}`")
    production_count = cursor.fetchone()['c']
    logger.info('Validation: %s has %d rows, %s has %d rows',
                STAGING_TABLE, staging_count, TARGET_TABLE, production_count)

    if staging_count < min_rows:
        logger.error('Validation FAILED: %s has %d rows (minimum %d)',
                     STAGING_TABLE, staging_count, min_rows)
        return False
    if production_count > 0:
        percentage = (staging_count / production_count) * 100
        logger.info('Staging is %.1f%% of production', percentage)
        if percentage < min_percentage:
            logger.error('Validation FAILED: staging (%d) is only %.1f%% of '
                         'production (%d); minimum %d%%',
                         staging_count, percentage, production_count, min_percentage)
            return False
    logger.info('Validation PASSED for %s', STAGING_TABLE)
    return True


def atomic_swap(conn, cursor):
    """Atomically swap staging into production: production -> backup, staging ->
    production, in a single RENAME TABLE (atomic in MariaDB/InnoDB)."""
    cursor.execute(f"DROP TABLE IF EXISTS `{BACKUP_TABLE}`")
    rename_sql = (f"RENAME TABLE `{TARGET_TABLE}` TO `{BACKUP_TABLE}`, "
                  f"`{STAGING_TABLE}` TO `{TARGET_TABLE}`")
    logger.info('Executing atomic swap: %s', rename_sql)
    cursor.execute(rename_sql)
    conn.commit()
    logger.info('Atomic swap completed for %s', TARGET_TABLE)


def restore_from_backup(conn, cursor):
    """Rename the backup table back to production if a swap failed mid-flight.
    Because the swap is a single atomic RENAME, a raised swap means NEITHER table
    was renamed and the backup was already dropped -- so 'no backup found' here is
    the expected, SAFE outcome (production was never moved), not a data-loss event."""
    cursor.execute(f"SHOW TABLES LIKE '{BACKUP_TABLE}'")
    if not cursor.fetchone():
        logger.info('No backup table %s to restore (production left untouched).',
                    BACKUP_TABLE)
        return
    cursor.execute(f"SHOW TABLES LIKE '{TARGET_TABLE}'")
    if cursor.fetchone():
        cursor.execute(f"DROP TABLE IF EXISTS `{TARGET_TABLE}`")
    cursor.execute(f"RENAME TABLE `{BACKUP_TABLE}` TO `{TARGET_TABLE}`")
    conn.commit()
    logger.info('Restored %s from backup', TARGET_TABLE)


def cleanup_staging(conn, cursor):
    cursor.execute(f"DROP TABLE IF EXISTS `{STAGING_TABLE}`")
    conn.commit()
    logger.info('Cleaned up staging table %s', STAGING_TABLE)


def main():
    conn = connect_db()
    cursor = conn.cursor()

    cfg = Config(retries={'max_attempts': 10, 'mode': 'standard'})
    dynamo_table = boto3.resource('dynamodb', config=cfg).Table(DYNAMO_TABLE)

    try:
        recover_orphaned_backup(conn, cursor)
        create_staging_table(cursor)
        conn.commit()

        stream_into_staging(conn, cursor, dynamo_table)

        if not validate_staging(cursor):
            logger.error('Validation failed; aborting swap to protect production.')
            cleanup_staging(conn, cursor)
            sys.exit(1)

        try:
            atomic_swap(conn, cursor)
        except Exception as swap_err:
            logger.error('Atomic swap failed: %s; attempting restore.', swap_err)
            restore_from_backup(conn, cursor)
            cleanup_staging(conn, cursor)
            sys.exit(1)

        logger.info('SUCCESS: %s updated with zero downtime.', TARGET_TABLE)

    except (BotoCoreError, ClientError) as e:
        logger.error('DynamoDB error during %s scan: %s', DYNAMO_TABLE, e)
        cleanup_staging(conn, cursor)
        sys.exit(1)
    except pymysql.err.MySQLError as e:
        logger.error('Database error: %s', e)
        try:
            cleanup_staging(conn, cursor)
        except Exception:
            pass
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()
        logger.info('Database connection closed.')


if __name__ == '__main__':
    main()
