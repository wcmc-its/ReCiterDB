"""
auditAbstracts.py -- one-shot forensic audit of reporting_abstracts.

Pulls rows where LENGTH(abstract) >= AUDIT_LENGTH_THRESHOLD, fetches the
DynamoDB ground truth for each PMID via the same path abstractImport.py
uses, and classifies each row:

  CLEAN              DB matches Dynamo (long but legitimate abstract).
  PREFIX_CORRUPTED   First ~150 chars of the Dynamo abstract appear near
                     the start of the DB blob and DB is substantially
                     longer than Dynamo -- the cross-paper concatenation
                     pattern produced by the old CSV / LOAD DATA path.
  DISJOINT           DB front does not match Dynamo front; needs manual
                     review.
  MISSING_IN_DYNAMO  DynamoDB has no PubMedArticle record for the PMID.
  EMPTY_IN_DYNAMO    Record present but yields empty abstract.

Outputs:
  - audit_abstracts.csv     one row per PMID examined
  - audit_abstracts_dump.txt full text dump of the top N corrupted rows
  - per-verdict counters and worst-offender summary to stdout

Read-only. Does not modify reporting_abstracts.

Env:
  DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
  AUDIT_LENGTH_THRESHOLD (default 4000)
  AUDIT_MAX_CANDIDATES   (default 1000)
"""

import concurrent.futures
import csv
import logging
import os
import sys
import time

import boto3
import pymysql.cursors
import pymysql.err


DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

LENGTH_THRESHOLD = int(os.getenv("AUDIT_LENGTH_THRESHOLD", "4000"))
MAX_CANDIDATES = int(os.getenv("AUDIT_MAX_CANDIDATES", "1000"))

CHUNK_SIZE = 100
MAX_WORKERS = 5
MAX_UNPROCESSED_RETRIES = 8

OUTPUT_CSV = "audit_abstracts.csv"
DUMP_FILE = "audit_abstracts_dump.txt"
DUMP_TOP_N = 5

# Compare on the first HEAD_SAMPLE chars of the Dynamo abstract; require
# it to be found within the first HEAD_SEARCH_WINDOW chars of the DB blob.
# Short enough to tolerate leading-character noise (the orphan `"` and
# similar CSV artifacts), long enough to be specific.
HEAD_SAMPLE = 150
HEAD_SEARCH_WINDOW = 400
# A DB blob this much longer than Dynamo is the concatenation signal.
LENGTH_INFLATION_RATIO = 1.3
# BLOB-cap rule: a row right at the column cap with a Dynamo abstract many
# times smaller is the parser-desync fingerprint regardless of whether the
# first 150 chars happen to match (PubMed sometimes updated section labels
# between the original CSV load and now, which can defeat the head-string
# match).
BLOB_CAP_THRESHOLD = 60000
BLOB_CAP_INFLATION_RATIO = 5

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)


def connect_mysql():
    try:
        return pymysql.connect(
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_NAME,
            host=DB_HOST,
            autocommit=True,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    except pymysql.err.MySQLError as err:
        logger.error(f"DB connection failed: {err}")
        sys.exit(1)


def fetch_candidates(conn, threshold, max_rows):
    sql = """
        SELECT pmid, LENGTH(abstract) AS db_len, abstract
        FROM reporting_abstracts
        WHERE LENGTH(abstract) >= %s
        ORDER BY LENGTH(abstract) DESC
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (threshold, max_rows))
        rows = cur.fetchall()
    for r in rows:
        if isinstance(r["abstract"], (bytes, bytearray)):
            r["abstract"] = r["abstract"].decode("utf-8", errors="replace")
        r["abstract"] = r["abstract"].replace("\r\n", "\n")
    return rows


def get_abstract(item):
    """Same extraction logic as update/abstractImport.py:99."""
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


def fetch_abstracts_from_dynamo(pmids):
    client = boto3.resource("dynamodb").meta.client

    def fetch_chunk(chunk):
        request_keys = [{"pmid": p} for p in chunk]
        results = {}
        present = set()
        attempt = 0
        while request_keys:
            response = client.batch_get_item(
                RequestItems={"PubMedArticle": {"Keys": request_keys}}
            )
            for item in response["Responses"].get("PubMedArticle", []):
                pmid = item.get("pmid")
                if pmid is not None:
                    present.add(pmid)
                    results[pmid] = get_abstract(item)
            request_keys = (
                response.get("UnprocessedKeys", {})
                .get("PubMedArticle", {})
                .get("Keys", [])
            )
            if request_keys:
                attempt += 1
                if attempt > MAX_UNPROCESSED_RETRIES:
                    logger.warning(
                        f"{len(request_keys)} keys still unprocessed after "
                        f"{MAX_UNPROCESSED_RETRIES} retries; skipping remainder."
                    )
                    break
                time.sleep(min(0.1 * (2 ** attempt), 5.0))
        return results, present

    chunks = [pmids[i:i + CHUNK_SIZE] for i in range(0, len(pmids), CHUNK_SIZE)]
    all_results = {}
    found = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(fetch_chunk, c) for c in chunks]
        for f in concurrent.futures.as_completed(futures):
            res, present = f.result()
            all_results.update(res)
            found.update(present)
    return all_results, found


def classify(db_abs, dyn_abs, dyn_present):
    if not dyn_present:
        return "MISSING_IN_DYNAMO"
    if not dyn_abs:
        return "EMPTY_IN_DYNAMO"

    db_norm = db_abs.strip()
    dyn_norm = dyn_abs.strip()
    if db_norm == dyn_norm:
        return "CLEAN"

    db_len = len(db_norm)
    dyn_len = len(dyn_norm)

    # Allow tiny tail differences (trailing whitespace/punctuation, an
    # extra character or two) without flagging as corruption.
    if abs(db_len - dyn_len) <= 5 and db_norm[: min(db_len, dyn_len) - 5 if db_len > 5 else db_len].lstrip('"') == dyn_norm[: min(db_len, dyn_len) - 5 if db_len > 5 else dyn_len].lstrip('"'):
        return "CLEAN"

    head_sample = dyn_norm[:HEAD_SAMPLE]
    if head_sample and head_sample in db_norm[:HEAD_SEARCH_WINDOW]:
        if db_len > dyn_len * LENGTH_INFLATION_RATIO:
            return "PREFIX_CORRUPTED"
        return "CLEAN"

    if db_len >= BLOB_CAP_THRESHOLD and db_len > dyn_len * BLOB_CAP_INFLATION_RATIO:
        return "PREFIX_CORRUPTED"

    return "DISJOINT"


def safe_oneline(s, n):
    return s[:n].replace("\n", " ").replace("\t", " ").replace("\r", " ")


def main():
    logger.info(
        f"Audit: LENGTH(abstract) >= {LENGTH_THRESHOLD}; "
        f"max candidates: {MAX_CANDIDATES}"
    )
    conn = connect_mysql()
    try:
        candidates = fetch_candidates(conn, LENGTH_THRESHOLD, MAX_CANDIDATES)
    finally:
        conn.close()

    logger.info(f"Candidates from reporting_abstracts: {len(candidates)}")
    if not candidates:
        logger.info("Nothing above threshold; exiting.")
        return

    lens = sorted(c["db_len"] for c in candidates)
    logger.info(
        f"DB length distribution: min={lens[0]} "
        f"p50={lens[len(lens) // 2]} p95={lens[int(len(lens) * 0.95)]} "
        f"max={lens[-1]}"
    )

    pmids = [c["pmid"] for c in candidates]
    dyn_abstracts, dyn_present = fetch_abstracts_from_dynamo(pmids)
    logger.info(
        f"DynamoDB returned records for {len(dyn_present)} / {len(pmids)} PMIDs"
    )

    rows = []
    counters = {
        "CLEAN": 0,
        "PREFIX_CORRUPTED": 0,
        "DISJOINT": 0,
        "MISSING_IN_DYNAMO": 0,
        "EMPTY_IN_DYNAMO": 0,
    }
    for c in candidates:
        pmid = c["pmid"]
        db_abs = c["abstract"]
        present = pmid in dyn_present
        dyn_abs = dyn_abstracts.get(pmid, "")
        verdict = classify(db_abs, dyn_abs, present)
        counters[verdict] += 1
        rows.append({
            "pmid": pmid,
            "db_len": c["db_len"],
            "dyn_len": len(dyn_abs) if present else "",
            "verdict": verdict,
            "db_head": safe_oneline(db_abs, 80),
            "db_tail": safe_oneline(db_abs[-80:] if len(db_abs) >= 80 else db_abs, 80),
            "dyn_head": safe_oneline(dyn_abs, 80) if present else "",
        })

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Per-row audit written to {OUTPUT_CSV}")

    logger.info("Verdict counts:")
    for k in ("CLEAN", "PREFIX_CORRUPTED", "DISJOINT",
              "MISSING_IN_DYNAMO", "EMPTY_IN_DYNAMO"):
        logger.info(f"  {k:18s} {counters[k]}")

    suspect = [r for r in rows if r["verdict"] in ("PREFIX_CORRUPTED", "DISJOINT")]
    suspect.sort(key=lambda r: r["db_len"], reverse=True)

    if suspect:
        with open(DUMP_FILE, "w", encoding="utf-8") as f:
            for r in suspect[:DUMP_TOP_N]:
                pmid = r["pmid"]
                db_abs = next(c["abstract"] for c in candidates if c["pmid"] == pmid)
                dyn_abs = dyn_abstracts.get(pmid, "")
                f.write("=" * 80 + "\n")
                f.write(
                    f"pmid={pmid} verdict={r['verdict']} "
                    f"db_len={r['db_len']} dyn_len={r['dyn_len']}\n"
                )
                f.write("--- DB (full) ---\n")
                f.write(db_abs + "\n")
                f.write("--- Dynamo (full) ---\n")
                f.write(dyn_abs + "\n\n")
        logger.info(f"Top {DUMP_TOP_N} suspects dumped to {DUMP_FILE}")

        logger.info(f"Top {min(DUMP_TOP_N, len(suspect))} suspects (summary):")
        for r in suspect[:DUMP_TOP_N]:
            logger.info(
                f"  pmid={r['pmid']:>9} verdict={r['verdict']:17s} "
                f"db_len={r['db_len']:>6} dyn_len={r['dyn_len']}"
            )
            logger.info(f"    db_head  : {r['db_head']!r}")
            logger.info(f"    db_tail  : {r['db_tail']!r}")
            logger.info(f"    dyn_head : {r['dyn_head']!r}")


if __name__ == "__main__":
    main()
