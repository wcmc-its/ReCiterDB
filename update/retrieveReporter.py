# retrieveReporter.py
#
# Pulls grant metadata and pub-grant linkages from NIH RePORTER
# (https://api.reporter.nih.gov/v2/) and reconciles them against the
# ReCiter-derived person_article_grant table.
#
# Two API loops:
#   1. POST /projects/search filtered by WCM org name → grant_reporter_project
#   2. POST /publications/search keyed by appl_ids from step 1 → grant_reporter_link
#
# Then a SQL reconciliation step populates grant_provenance, the long-lived
# (person, pmid, grant)-keyed audit log that survives the nightly truncate-
# reload of person_article_grant. See setup/alter_add_reporter_fields_v1.2.sql
# for the full design rationale.
#
# Why we filter by org_name rather than fetching everything:
#   RePORTER returns ~thousands of WCM-attributed projects. Pulling the full
#   corpus would require partitioning by FY (offset cap is 9,999) and gives
#   no benefit for our use case. Subaward caveat: WCM-as-sub may not appear
#   under this org filter — accepted as a false-negative tradeoff to keep
#   false positives near zero.

import os
import sys
import csv
import time
import random
import re
import logging
import faulthandler
import signal
import requests
import pymysql.cursors
import pymysql.err

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('retrieveReporter.log', mode='w'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

faulthandler.enable(file=sys.stderr, all_threads=True)
faulthandler.register(signal.SIGUSR1, file=sys.stderr, all_threads=True)

REPORTER_BASE_URL = 'https://api.reporter.nih.gov/v2'
WCM_ORG_NAME = 'WEILL MEDICAL COLL OF CORNELL UNIV'
PAGE_LIMIT = 500
OFFSET_CAP = 9999
REQUEST_INTERVAL_SEC = 1.0  # NIH guidance: 1 req/sec
PUBS_BATCH_SIZE = 50  # appl_ids per /publications/search call

# core_project_num pattern, e.g. "R01DK127777", "U01AI189285", "K23MH112873".
# Prefix is 1-3 alphanumeric (activity code) + 2 letters (IC) + 5-7 digits.
CORE_PROJECT_RE = re.compile(r'\b([A-Z]\d{1,2}[A-Z]{2}\d{5,7})\b')


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
                local_infile=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            logger.info('Connected to database %s on %s', database, hostname)
            return conn
        except pymysql.err.MySQLError as err:
            logger.error('DB connect attempt %d failed: %s', retry + 1, err)
            time.sleep(backoff_factor * (2 ** retry) + random.uniform(0, 1))
    raise RuntimeError('Could not connect to database after retries')


def post_with_retry(url, payload, max_retries=5, backoff_factor=1):
    """POST with exponential backoff. Honors NIH's 1 req/sec rate limit
    by sleeping between successful calls in the caller."""
    for retry in range(max_retries):
        try:
            r = requests.post(url, json=payload, timeout=(10, 90))
            if r.status_code == 429:
                wait = backoff_factor * (2 ** retry) + random.uniform(0, 5)
                logger.warning('429 from RePORTER; sleeping %.1fs', wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            wait = backoff_factor * (2 ** retry) + random.uniform(0, 1)
            logger.error('RePORTER request failed (attempt %d): %s; sleep %.1fs',
                         retry + 1, e, wait)
            time.sleep(wait)
    raise RuntimeError(f'RePORTER request failed after {max_retries} retries: {url}')


def _fetch_projects_page(criteria):
    """Yield project dicts for a single criteria block. Caller must ensure
    the result set fits under OFFSET_CAP; we log and stop if it doesn't."""
    url = f'{REPORTER_BASE_URL}/projects/search'
    offset = 0
    while offset <= OFFSET_CAP:
        payload = {
            'criteria': criteria,
            'limit': PAGE_LIMIT,
            'offset': offset,
        }
        data = post_with_retry(url, payload)
        results = data.get('results', []) or []
        if not results:
            return
        for row in results:
            yield row
        meta = data.get('meta', {}) or {}
        total = meta.get('total', 0)
        offset += PAGE_LIMIT
        if offset >= total:
            return
        if offset > OFFSET_CAP:
            logger.warning(
                'Result set has %d records but offset cap is %d; truncating. '
                'Caller should partition further (e.g. by activity_code).',
                total, OFFSET_CAP)
            return
        time.sleep(REQUEST_INTERVAL_SEC)


def fetch_projects(base_criteria):
    """Yield project dicts, partitioning by fiscal year when needed to stay
    under the offset cap. WCM has ~15K projects historically, which exceeds
    the 9,999 offset limit on a single criteria block.

    Strategy: probe total once with the base criteria. If under the cap,
    return all in one stream. Otherwise iterate fiscal years from the
    earliest NIH grant year (1985) through next year, requesting
    fiscal_years=[FY] for each."""
    probe = post_with_retry(
        f'{REPORTER_BASE_URL}/projects/search',
        {'criteria': base_criteria, 'limit': 1, 'offset': 0},
    )
    total = (probe.get('meta', {}) or {}).get('total', 0)
    logger.info('RePORTER /projects/search reports %d total matches for base criteria', total)

    if total <= OFFSET_CAP:
        yield from _fetch_projects_page(base_criteria)
        return

    import datetime
    end_fy = datetime.date.today().year + 1
    for fy in range(1985, end_fy + 1):
        criteria = dict(base_criteria)
        criteria['fiscal_years'] = [fy]
        yielded_this_fy = 0
        for row in _fetch_projects_page(criteria):
            yielded_this_fy += 1
            yield row
        if yielded_this_fy:
            logger.info('FY %d: yielded %d projects', fy, yielded_this_fy)
        time.sleep(REQUEST_INTERVAL_SEC)


def fetch_publications_for_appl_ids(appl_ids):
    """Yield (pmid, appl_id, core_project_num) tuples from /publications/search
    in batches of PUBS_BATCH_SIZE."""
    url = f'{REPORTER_BASE_URL}/publications/search'
    appl_ids = list({int(x) for x in appl_ids if x is not None})
    for i in range(0, len(appl_ids), PUBS_BATCH_SIZE):
        batch = appl_ids[i:i + PUBS_BATCH_SIZE]
        offset = 0
        while offset <= OFFSET_CAP:
            payload = {
                'criteria': {'appl_ids': batch},
                'limit': PAGE_LIMIT,
                'offset': offset,
            }
            data = post_with_retry(url, payload)
            results = data.get('results', []) or []
            if not results:
                break
            for row in results:
                pmid = row.get('pmid')
                appl_id = row.get('applid') or row.get('appl_id')
                core = row.get('coreproject') or row.get('core_project_num')
                if pmid and appl_id:
                    yield int(pmid), int(appl_id), core
            meta = data.get('meta', {}) or {}
            total = meta.get('total', 0)
            offset += PAGE_LIMIT
            if offset >= total:
                break
            time.sleep(REQUEST_INTERVAL_SEC)
        time.sleep(REQUEST_INTERVAL_SEC)


def reload_table(conn, table, rows, columns):
    """Truncate `table` and insert `rows` (list of tuples matching `columns`).
    Used for the staging tables grant_reporter_project and grant_reporter_link.
    grant_provenance is upserted, not reloaded."""
    placeholders = ', '.join(['%s'] * len(columns))
    col_list = ', '.join(f'`{c}`' for c in columns)
    cur = conn.cursor()
    cur.execute(f'TRUNCATE TABLE `{table}`')
    if rows:
        sql = f'INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})'
        cur.executemany(sql, rows)
    conn.commit()
    cur.execute(f'SELECT COUNT(*) AS c FROM `{table}`')
    count = cur.fetchone()['c']
    logger.info('Reloaded %s: %d rows', table, count)


def normalize_grant_string(raw):
    """Extract a core project number (e.g. R01DK127777) from a free-text
    NIH grant string. Returns None if no match — caller decides whether to
    fall back to the raw string."""
    if not raw:
        return None
    upper = re.sub(r'[\s\-\/]', '', raw.upper())
    m = CORE_PROJECT_RE.search(upper)
    return m.group(1) if m else None


def reconcile_provenance(conn):
    """Populate grant_provenance from person_article_grant and grant_reporter_link.

    Bulk pattern: each side does a single INSERT...SELECT with ON DUPLICATE
    KEY UPDATE so we make one round trip per side instead of one per row.
    First_seen timestamps stick because they're only in the INSERT clause,
    not the UPDATE clause."""
    import tempfile
    cur = conn.cursor()

    # ----- (1) reciterdb side -----
    # Normalization (free-text articleGrant → core_project_num) happens in
    # Python, so we stage the normalized rows in a temp table first via
    # LOAD DATA LOCAL INFILE, then do a single bulk upsert.
    logger.info('Reading person_article_grant for reconciliation')
    cur.execute("""
        SELECT personIdentifier, pmid, articleGrant
        FROM person_article_grant
        WHERE personIdentifier IS NOT NULL
          AND pmid > 0
          AND articleGrant IS NOT NULL
          AND articleGrant <> ''
    """)
    pag_rows = cur.fetchall()
    logger.info('person_article_grant rows considered: %d', len(pag_rows))

    # Normalize + dedupe in Python (the temp table's PK enforces uniqueness
    # but deduping here avoids LOAD DATA INFILE warnings on duplicate rows).
    seen = set()
    normalized = []
    for row in pag_rows:
        n = normalize_grant_string(row['articleGrant'])
        if not n:
            # Non-NIH fallback: sanitize control chars (CSV uses TAB delim)
            n = re.sub(r'[\t\n\r]', ' ', row['articleGrant'])[:64]
        key = (row['personIdentifier'], row['pmid'], n)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)
    logger.info('Normalized + deduped to %d distinct (person, pmid, grant) rows',
                len(normalized))

    csv_file = tempfile.NamedTemporaryFile(
        delete=False, mode='w', suffix='.csv', newline='', encoding='utf-8')
    try:
        writer = csv.writer(csv_file, delimiter='\t', lineterminator='\n',
                            quoting=csv.QUOTE_NONE, escapechar='\\')
        for r in normalized:
            writer.writerow(r)
        csv_file.close()

        cur.execute("DROP TEMPORARY TABLE IF EXISTS _reciter_grant_staging")
        cur.execute("""
            CREATE TEMPORARY TABLE _reciter_grant_staging (
                personIdentifier VARCHAR(128) NOT NULL,
                pmid INT NOT NULL,
                core_project_num VARCHAR(64) NOT NULL,
                PRIMARY KEY (personIdentifier, pmid, core_project_num)
            ) ENGINE=InnoDB
        """)
        load_sql = (
            f"LOAD DATA LOCAL INFILE '{csv_file.name}' "
            "INTO TABLE _reciter_grant_staging "
            "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' "
            "(personIdentifier, pmid, core_project_num)"
        )
        cur.execute(load_sql)
        cur.execute("SELECT COUNT(*) AS c FROM _reciter_grant_staging")
        logger.info('Loaded %d rows into reciterdb staging table',
                    cur.fetchone()['c'])

        cur.execute("""
            INSERT INTO grant_provenance
                (personIdentifier, pmid, core_project_num,
                 source_reciterdb, reciterdb_first_seen, last_verified)
            SELECT personIdentifier, pmid, core_project_num,
                   1, NOW(), NOW()
            FROM _reciter_grant_staging
            ON DUPLICATE KEY UPDATE
                source_reciterdb = 1,
                last_verified = NOW()
        """)
        # rowcount on bulk upsert is "1 per insert + 2 per update" in MariaDB
        # — informative, not exact
        logger.info('Reciterdb-side bulk upsert: %d rowcount', cur.rowcount)
        cur.execute("DROP TEMPORARY TABLE _reciter_grant_staging")
        conn.commit()
    finally:
        try:
            os.unlink(csv_file.name)
        except OSError:
            pass

    # ----- (2) RePORTER side -----
    # Pure SQL — no Python iteration. The JOIN to person_article enforces
    # the false-positive guard (only ACCEPTED PMIDs credit a person).
    # GROUP BY collapses cases where one (person, pmid, core_project) has
    # multiple appl_ids (different fiscal years of the same grant); MAX
    # picks the most recent appl_id deterministically.
    logger.info('Running RePORTER-side bulk upsert')
    cur.execute("""
        INSERT INTO grant_provenance
            (personIdentifier, pmid, core_project_num, appl_id,
             source_reporter, reporter_first_seen, last_verified)
        SELECT pa.personIdentifier, grl.pmid, grl.core_project_num,
               MAX(grl.appl_id), 1, NOW(), NOW()
        FROM grant_reporter_link grl
        JOIN person_article pa
          ON pa.pmid = grl.pmid
         AND pa.userAssertion = 'ACCEPTED'
        WHERE grl.core_project_num IS NOT NULL
        GROUP BY pa.personIdentifier, grl.pmid, grl.core_project_num
        ON DUPLICATE KEY UPDATE
            source_reporter = 1,
            appl_id = COALESCE(VALUES(appl_id), grant_provenance.appl_id),
            last_verified = NOW()
    """)
    logger.info('RePORTER-side bulk upsert: %d rowcount', cur.rowcount)
    conn.commit()

    # Summary
    cur.execute("SELECT COUNT(*) AS c FROM grant_provenance")
    total = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) AS c FROM grant_provenance WHERE source_reporter = 1 AND source_reciterdb = 1")
    both = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) AS c FROM grant_provenance WHERE source_reporter = 1 AND source_reciterdb = 0")
    rep_only = cur.fetchone()['c']
    cur.execute("SELECT COUNT(*) AS c FROM grant_provenance WHERE source_reporter = 0 AND source_reciterdb = 1")
    reciter_only = cur.fetchone()['c']
    logger.info('Provenance totals: %d rows | both=%d | reporter-only=%d | reciter-only=%d',
                total, both, rep_only, reciter_only)


def main():
    org_name = os.environ.get('REPORTER_ORG_NAME', WCM_ORG_NAME)
    logger.info('Starting RePORTER ETL for org: %s', org_name)

    conn = connect_db()

    # ----- Loop A: projects -----
    # No include_fields — the API expects CamelCase there ('ApplId') but
    # response field names are snake_case ('appl_id'). Easier to take all
    # fields back than maintain two name conventions.
    project_rows = []
    appl_ids = []
    for proj in fetch_projects(base_criteria={'org_names': [org_name]}):
        appl_id = proj.get('appl_id')
        if not appl_id:
            continue
        appl_ids.append(appl_id)
        org = (proj.get('organization') or {}).get('org_name')
        project_rows.append((
            int(appl_id),
            proj.get('core_project_num'),
            (proj.get('project_title') or '')[:512],
            (org or '')[:255],
            proj.get('fiscal_year'),
            proj.get('activity_code'),
            proj.get('project_start_date'),
            proj.get('project_end_date'),
            proj.get('abstract_text'),
        ))
    logger.info('Fetched %d RePORTER projects', len(project_rows))
    reload_table(
        conn,
        'grant_reporter_project',
        project_rows,
        ['appl_id', 'core_project_num', 'project_title', 'org_name',
         'fiscal_year', 'activity_code', 'project_start_date',
         'project_end_date', 'abstract_text'],
    )

    # ----- Loop B: publications -----
    link_rows = []
    seen_pairs = set()
    for pmid, appl_id, core in fetch_publications_for_appl_ids(appl_ids):
        key = (pmid, appl_id)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        link_rows.append((pmid, appl_id, core))
    logger.info('Fetched %d unique (pmid, appl_id) pairs', len(link_rows))
    reload_table(
        conn,
        'grant_reporter_link',
        link_rows,
        ['pmid', 'appl_id', 'core_project_num'],
    )

    # ----- Reconciliation -----
    reconcile_provenance(conn)

    conn.close()
    logger.info('RePORTER ETL complete')


if __name__ == '__main__':
    main()
