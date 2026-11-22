#!/usr/bin/env python3
"""Rollback for the external-records cutover (plan 3b-3e): copy PM's MySQL writes back to
DynamoDB ExternalArticle, so PM can return to EXTERNAL_STORE=reciter without losing them.

After the cutover PM writes only `external_record` (EXTERNAL_STORE=mysql), and the nightly
no longer scans DynamoDB (EXTERNAL_DYNAMO_SYNC=off). To go back, run this, then set
EXTERNAL_STORE=reciter on PM and EXTERNAL_DYNAMO_SYNC=on on the reciterdb CronJob.

It is the inverse of retrieveExternalArticles.py's mapping (legacy_status):
  ACCEPTED   -> suppressed=false, no supersededByPmid
  REJECTED   -> suppressed=true
  SUPERSEDED -> suppressed=true, supersededByPmid=<pmid>
  REMOVED    -> suppressed=true if the item exists (never deleted); skipped if it never did
Rows read: every row PM wrote or touched (sync_origin='PM' OR pm_touched=1), optionally
only those updated since --since, or for one --uid.
  - An item already in DynamoDB gets only its suppressed/supersededByPmid changed
    (UpdateItem, conditional on the item existing); nothing else on it is touched.
  - A row PM created gets a full item (PutItem, conditional on no item existing).
  - Skipped, with counts: MANUAL rows (ReCiter's ExternalArticle API only accepts SCOPUS,
    WOS, OPENALEX and CROSSREF ids, so PM in reciter mode cannot show them; they stay in
    MySQL and in reporting), and statuses with no DynamoDB form (PENDING, MERGED, AUTOMERGED).
Nothing is ever deleted, in either store.

Dry run by default: reads both stores and prints the plan. --apply writes.

Runs from a workstation with the operator's AWS session: the reciterdb pod role can read
ExternalArticle but not write it (PutItem/UpdateItem implicitDeny, checked 2026-11-21).
MySQL: DB_HOST/DB_USERNAME/DB_PASSWORD/DB_NAME, or ~/.my.cnf when DB_HOST is unset. The
target host and database are printed first; check them before --apply.

Usage:
  python3 update/export_external_record_to_dynamo.py [--since 2026-11-23T17:00] [--uid X] [--apply]
"""
import argparse
import json
import logging
import os
import sys
from collections import Counter

import boto3
import pymysql
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("export_external_record_to_dynamo")

DDB_TABLE = "ExternalArticle"
ER_TABLE = "external_record"
# ReCiter's ExternalArticle API allowlist (ExternalArticleController, ^(SCOPUS|WOS|OPENALEX|CROSSREF):).
DYNAMO_SOURCES = {"SCOPUS", "WOS", "OPENALEX", "CROSSREF"}
EXPORTED_STATUSES = {"ACCEPTED", "REJECTED", "SUPERSEDED", "REMOVED"}


def fmt_dt(dt):
    """MySQL DATETIME(3) -> DynamoDB dateAdded form (retrieveExternalArticles._fmt_dt)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z" if dt else None


def status_attrs(rec):
    """(suppressed, supersededByPmid) for a record's status; supersededByPmid None = absent."""
    st = rec["status"]
    if st == "ACCEPTED":
        return False, None
    if st == "SUPERSEDED":
        return True, int(rec["superseded_by_pmid"]) if rec.get("superseded_by_pmid") is not None else None
    return True, None   # REJECTED, REMOVED


def _json(v):
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return None
    return v


def author_names(rec):
    """authors_json [{given, surname}] -> the flat name strings ExternalArticle carries."""
    names = []
    for a in _json(rec.get("authors_json")) or []:
        if isinstance(a, dict):
            n = " ".join(x for x in (a.get("given"), a.get("surname")) if x).strip()
            if n:
                names.append(n)
    return names


def new_item(rec):
    """A full ExternalArticle item for a record PM created, in the attribute names the sync reads."""
    suppressed, superseded = status_attrs(rec)
    item = {
        "uid": rec["uid"],
        "articleId": rec["article_id"],
        "sourceType": rec["source_type"],
        "title": rec.get("title"),
        "doi": rec.get("doi"),
        "pmid": int(rec["pmid"]) if rec.get("pmid") is not None else None,
        "authors": author_names(rec) or None,
        "journalOrVenue": rec.get("container_title"),
        "pubDate": rec.get("pub_date"),
        "publicationType": rec.get("source_pub_type"),
        "method": rec.get("method"),
        "addedBy": rec.get("added_by"),
        "dateAdded": fmt_dt(rec.get("added_at")),
        "rawRecord": rec.get("raw_record"),
        "suppressed": suppressed,
        "supersededByPmid": superseded,
    }
    # DynamoDB rejects empty strings in key-less attributes on older SDKs and stores nothing
    # useful for None: leave both out.
    return {k: v for k, v in item.items() if v is not None and v != ""}


def plan_export(records, existing):
    """One action per record. existing: {(uid, articleId): item or None}, looked up by exact key.
    Actions: ("update", rec, suppressed, superseded), ("put", rec, item), ("unchanged", rec),
    ("skip", rec, reason)."""
    plan = []
    for rec in records:
        key = (rec["uid"], rec["article_id"])
        if rec["source_type"] not in DYNAMO_SOURCES:
            plan.append(("skip", rec, f"source {rec['source_type']} has no DynamoDB form"))
            continue
        if rec["status"] not in EXPORTED_STATUSES:
            plan.append(("skip", rec, f"status {rec['status']} has no DynamoDB form"))
            continue
        item = existing.get(key)
        if item is None:
            if rec["status"] == "REMOVED":
                plan.append(("skip", rec, "removed, and never in DynamoDB"))
                continue
            plan.append(("put", rec, new_item(rec)))
            continue
        suppressed, superseded = status_attrs(rec)
        have_sup = bool(item.get("suppressed"))
        have_pmid = int(item["supersededByPmid"]) if item.get("supersededByPmid") is not None else None
        if have_sup == suppressed and have_pmid == superseded:
            plan.append(("unchanged", rec))
        else:
            plan.append(("update", rec, suppressed, superseded))
    return plan


def select_sql(since=None, uid=None):
    sql = f"SELECT * FROM `{ER_TABLE}` WHERE (sync_origin = 'PM' OR pm_touched = 1)"
    args = []
    if since:
        sql += " AND updated_at >= %s"
        args.append(since)
    if uid:
        sql += " AND uid = %s"
        args.append(uid)
    return sql + " ORDER BY uid, article_id", args


def _conn():
    if os.getenv("DB_HOST"):
        return pymysql.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USERNAME"),
                               password=os.getenv("DB_PASSWORD"), db=os.getenv("DB_NAME"), charset="utf8mb4")
    return pymysql.connect(read_default_file=os.path.expanduser("~/.my.cnf"), charset="utf8mb4")


def read_records(conn, since, uid):
    sql, args = select_sql(since, uid)
    with conn.cursor() as c:
        c.execute(sql, args)
        cols = [d[0] for d in c.description]
        return [dict(zip(cols, t)) for t in c.fetchall()]


def read_existing(table, records):
    """Exact-key GetItem per record (the export volume is curator writes, not thousands)."""
    out = {}
    for rec in records:
        key = {"uid": rec["uid"], "articleId": rec["article_id"]}
        out[(rec["uid"], rec["article_id"])] = table.get_item(Key=key, ConsistentRead=True).get("Item")
    return out


def apply(table, plan):
    """Execute the plan; returns (done Counter, failures list)."""
    done, failed = Counter(), []
    for a in plan:
        kind, rec = a[0], a[1]
        key = {"uid": rec["uid"], "articleId": rec["article_id"]}
        try:
            if kind == "put":
                table.put_item(Item=a[2], ConditionExpression="attribute_not_exists(uid)")
            elif kind == "update":
                suppressed, superseded = a[2], a[3]
                if superseded is None:
                    table.update_item(Key=key, UpdateExpression="SET suppressed = :s REMOVE supersededByPmid",
                                      ConditionExpression="attribute_exists(uid)",
                                      ExpressionAttributeValues={":s": suppressed})
                else:
                    table.update_item(Key=key, UpdateExpression="SET suppressed = :s, supersededByPmid = :p",
                                      ConditionExpression="attribute_exists(uid)",
                                      ExpressionAttributeValues={":s": suppressed, ":p": superseded})
            else:
                continue
            done[kind] += 1
        except ClientError as e:
            failed.append((kind, rec["uid"], rec["article_id"], e.response["Error"]["Code"]))
    return done, failed


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", help="only records updated at or after this time (ISO, UTC), e.g. the cutover start")
    ap.add_argument("--uid", help="only this person (a canary run)")
    ap.add_argument("--apply", action="store_true", help="write DynamoDB (default: dry run)")
    args = ap.parse_args(argv)

    conn = _conn()
    with conn.cursor() as c:
        c.execute("SELECT @@hostname, DATABASE()")
        host, db = c.fetchone()
    logger.info(f"MySQL {host} / {db}; DynamoDB {DDB_TABLE}; mode {'APPLY' if args.apply else 'dry run'}")
    if not db:
        sys.exit("no database selected: set DB_NAME, or database= in ~/.my.cnf")

    records = read_records(conn, args.since, args.uid)
    if records:
        logger.info(f"{len(records)} PM-written or PM-touched records, updated "
                    f"{min(r['updated_at'] for r in records)} .. {max(r['updated_at'] for r in records)}")
    else:
        logger.info("0 PM-written or PM-touched records: nothing to export")
        return 0
    table = boto3.resource("dynamodb", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")).Table(DDB_TABLE)
    plan = plan_export(records, read_existing(table, records))

    counts = Counter(a[0] for a in plan)
    reasons = Counter(a[2] for a in plan if a[0] == "skip")
    logger.info("plan: " + ", ".join(f"{k}={counts[k]}" for k in ("put", "update", "unchanged", "skip")))
    for reason, n in reasons.most_common():
        logger.info(f"  skip {n}: {reason}")
    for a in [a for a in plan if a[0] in ("put", "update")][:25]:
        rec = a[1]
        what = f"suppressed={a[2]} supersededByPmid={a[3]}" if a[0] == "update" else f"new item, status {rec['status']}"
        logger.info(f"  {a[0]:6} {rec['uid']} {rec['article_id']}  {what}")

    if not args.apply:
        logger.info("dry run: nothing written (add --apply)")
        return 0
    done, failed = apply(table, plan)
    logger.info("applied: " + ", ".join(f"{k}={v}" for k, v in sorted(done.items())) + f", failed={len(failed)}")
    for f in failed:
        logger.error(f"  failed {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
