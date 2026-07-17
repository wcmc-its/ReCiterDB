#!/usr/bin/env python3
"""One-shot repair for SCOPUS ExternalArticle rows written during the 2026-07-16 prod
cutover with journalOrVenue / authors attributes MISSING. The source of truth is the
DynamoDB "ExternalArticle" table; reciterdb.external_article is a TRUNCATE+RELOAD
projection, so this backfills DynamoDB and reciterdb reflects on the next nightly run.
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DDB_TABLE = "ExternalArticle"
SEARCH_PATH = "/scopus/search/query"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CALLS = 0.2


def _is_empty(v):
    """True if a DynamoDB value is absent, an empty/whitespace string, or an empty collection."""
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, (list, tuple, dict, set)):
        return len(v) == 0
    return False


def parse_scopus_id(article_id):
    """'SCOPUS:<id>' -> '<id>'. Returns None if there is no ':' or the id is empty."""
    if not article_id or ":" not in article_id:
        return None
    scopus_id = article_id.split(":", 1)[1].strip()
    return scopus_id or None


def build_eid(scopus_id):
    return f"EID(2-s2.0-{scopus_id})"


def scopus_lookup(search_url, scopus_id):
    """Query the Scopus search proxy for one EID. Returns (status, publication_name, creator)
    where status is 'ok' | 'not_found' | 'failed'. Never raises."""
    url = search_url.rstrip("/") + SEARCH_PATH
    body = json.dumps({"query": build_eid(scopus_id), "count": 1}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError) as e:
        logger.error(f"    Scopus lookup FAILED for {scopus_id}: {e}")
        return ("failed", None, None)
    entries = payload.get("search-results", {}).get("entry", [])
    # No-match: Elsevier returns an entry carrying an "error" field and no dc:identifier.
    if not entries or "dc:identifier" not in entries[0]:
        return ("not_found", None, None)
    e0 = entries[0]
    return ("ok", e0.get("prism:publicationName"), e0.get("dc:creator"))


def compute_updates(item, publication_name, creator):
    """Fill ONLY the field(s) currently null/empty on the item; never overwrite an existing
    non-empty value. Returns a dict of {ddb_attr: value} to SET (may be empty). authors is a
    Python list of strings; journalOrVenue is a Python str -- matching the projection reader."""
    updates = {}
    if _is_empty(item.get("journalOrVenue")) and not _is_empty(publication_name):
        updates["journalOrVenue"] = str(publication_name)
    if _is_empty(item.get("authors")) and not _is_empty(creator):
        updates["authors"] = [str(creator)]
    return updates


def apply_update(table, key, updates):
    """update_item SET of only the missing field(s). Each attribute is guarded by
    "(attribute_not_exists OR size == 0)" so a concurrent curator/Java write of a real (non-empty)
    value is never clobbered, while an ABSENT attribute OR a present-but-empty String/List is still
    fillable -- matching the fill-only-missing eligibility in compute_updates/_is_empty. Re-runs are
    safe. Both target attributes are only ever String or List, so size() is always valid. Raises on
    failure."""
    names, values, set_parts, cond_parts = {}, {}, [], []
    for i, (attr, val) in enumerate(updates.items()):
        n, v = f"#f{i}", f":v{i}"
        names[n] = attr
        values[v] = val
        set_parts.append(f"{n} = {v}")
        cond_parts.append(f"(attribute_not_exists({n}) OR size({n}) = :zero)")
    values[":zero"] = 0
    table.update_item(
        Key=key,
        UpdateExpression="SET " + ", ".join(set_parts),
        ConditionExpression=" AND ".join(cond_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def scan_all(table):
    """Full paginated scan of the whole table."""
    kwargs = {}
    while True:
        resp = table.scan(**kwargs)
        for it in resp.get("Items", []):
            yield it
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            return
        kwargs["ExclusiveStartKey"] = lek


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write to DynamoDB. Without this flag, dry-run only (no writes).")
    args = parser.parse_args()

    search_url = os.getenv("RECITER_SCOPUS_SEARCH_URL")
    if not search_url:
        logger.error("RECITER_SCOPUS_SEARCH_URL is not set (base URL of the Scopus tool, e.g. "
                     "http://localhost:8082 locally or http://reciter-scopus-prod in-cluster).")
        sys.exit(2)
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    mode = "APPLY" if args.apply else "DRY-RUN"
    logger.info(f"backfill_external_article_fields starting in {mode} mode "
                f"(scopus={search_url}, region={region}, table={DDB_TABLE})")

    table = boto3.resource("dynamodb", region_name=region).Table(DDB_TABLE)
    key_names = [s["AttributeName"] for s in table.key_schema]

    counts = {
        "fixed": 0,
        "skipped_non_scopus": 0,
        "skipped_already_filled": 0,
        "not_found": 0,
        "failed": 0,
        "skipped_bad_article_id": 0,
        "skipped_nothing_to_write": 0,
        "skipped_race": 0,
    }

    for item in scan_all(table):
        article_id = item.get("articleId")
        tag = f"uid={item.get('uid')} articleId={article_id}"

        if item.get("sourceType") != "SCOPUS":
            logger.info(f"  skipped (non-scopus): {tag}")
            counts["skipped_non_scopus"] += 1
            continue

        if not (_is_empty(item.get("journalOrVenue")) or _is_empty(item.get("authors"))):
            logger.info(f"  skipped (already filled): {tag}")
            counts["skipped_already_filled"] += 1
            continue

        scopus_id = parse_scopus_id(article_id)
        if scopus_id is None:
            logger.warning(f"  skipped (unparseable articleId): {tag}")
            counts["skipped_bad_article_id"] += 1
            continue

        status, publication_name, creator = scopus_lookup(search_url, scopus_id)
        time.sleep(SLEEP_BETWEEN_CALLS)
        if status == "failed":
            counts["failed"] += 1
            continue
        if status == "not_found":
            logger.warning(f"  not found in Scopus (no match for {scopus_id}): {tag}")
            counts["not_found"] += 1
            continue

        updates = compute_updates(item, publication_name, creator)
        if not updates:
            logger.info(f"  skipped (nothing to write; lookup empty for missing fields): {tag}")
            counts["skipped_nothing_to_write"] += 1
            continue

        preview = ", ".join(f"{k}={v!r}" for k, v in updates.items())
        if not args.apply:
            logger.info(f"  WOULD set [{preview}] on {tag}")
            counts["fixed"] += 1
            continue

        key = {k: item[k] for k in key_names}
        try:
            apply_update(table, key, updates)
            logger.info(f"  set [{preview}] on {tag}")
            counts["fixed"] += 1
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info(f"  skipped (already set by someone else): {tag}")
                counts["skipped_race"] += 1
            else:
                logger.error(f"  update FAILED: {tag}: {e}")
                counts["failed"] += 1
        except BotoCoreError as e:
            logger.error(f"  update FAILED: {tag}: {e}")
            counts["failed"] += 1

    logger.info("==== summary ====")
    logger.info(f"  fixed{' (would fix)' if not args.apply else ''}: {counts['fixed']}")
    logger.info(f"  skipped (non-scopus): {counts['skipped_non_scopus']}")
    logger.info(f"  skipped (already filled): {counts['skipped_already_filled']}")
    logger.info(f"  not found: {counts['not_found']}")
    logger.info(f"  failed: {counts['failed']}")
    if counts["skipped_bad_article_id"]:
        logger.info(f"  skipped (unparseable articleId): {counts['skipped_bad_article_id']}")
    if counts["skipped_nothing_to_write"]:
        logger.info(f"  skipped (nothing to write): {counts['skipped_nothing_to_write']}")
    if counts["skipped_race"]:
        logger.info(f"  skipped (already set by someone else): {counts['skipped_race']}")

    if args.apply:
        logger.info("reciterdb.external_article reflects these fixes on the next nightly projection "
                    "(or after running update/retrieveExternalArticles.py in-cluster).")
    else:
        logger.info("DRY RUN -- no changes made. Re-run with --apply to perform the repair.")

    # Surface failures in the exit code so a wrapping script/operator is never told a run that
    # wrote nothing due to lookup/write failures "succeeded".
    if counts["failed"]:
        logger.error(f"exiting non-zero: {counts['failed']} row(s) failed (lookup or write).")
        sys.exit(1)


def _selfcheck():
    # (a) EID is built as "EID(2-s2.0-<id>)" from "SCOPUS:<id>".
    assert parse_scopus_id("SCOPUS:85012345678") == "85012345678"
    assert build_eid(parse_scopus_id("SCOPUS:85012345678")) == "EID(2-s2.0-85012345678)"
    assert parse_scopus_id("SCOPUS:") is None
    assert parse_scopus_id("85012345678") is None
    # (b) fill-only-missing merge: sets authors when absent, leaves an existing
    #     journalOrVenue untouched:
    u = compute_updates({"journalOrVenue": "Nature"}, "Nature", "Doe J")
    assert u == {"authors": ["Doe J"]}, u
    # ... produces an empty update when both are already present:
    u2 = compute_updates({"journalOrVenue": "Nature", "authors": ["Doe J"]}, "Nature", "Doe J")
    assert u2 == {}, u2
    # ... and fills journalOrVenue when it is the missing one:
    u3 = compute_updates({"authors": ["Doe J"]}, "Nature", "Doe J")
    assert u3 == {"journalOrVenue": "Nature"}, u3
    print("selfcheck OK")


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        _selfcheck()
        sys.exit(0)
    main()