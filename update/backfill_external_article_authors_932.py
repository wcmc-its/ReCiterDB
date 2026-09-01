#!/usr/bin/env python3
"""PM #932 -- backfill missing/incomplete `authors` on SCOPUS ExternalArticle rows.

WHAT / WHY
----------
Rows added before 2026-08-25 were written with NO `authors` attribute at all;
some later rows carry `[]` or a single-element list even though the underlying
Scopus document has multiple authors. This script fills both cases from two
sources, preferring the one that costs no network call:

  1. `reciterdb.authorship_review` (source='scopus') already carries the
     producer's `authors_json` (`[{"given":..,"surname":..}, ...]`) built from a
     COMPLETE-view Scopus fetch for every WCM-authored document the AAR pipeline
     has scored. When that list has >= 2 names, use it directly -- no network.
  2. Otherwise, live Scopus lookup (EID, falling back to DOI) via the Scopus
     retrieval tool's search proxy, `view=COMPLETE` (required -- the `author[]`
     array is entirely absent without it).

`reciterdb.external_article` is a nightly TRUNCATE+RELOAD projection of the
DynamoDB `ExternalArticle` table and is used ONLY to enumerate candidate rows;
it is up to one cycle stale and is never the write-time source of truth. Every
write is a conditional DynamoDB `update_item` guarded against clobbering a
concurrent write, so staleness in the enumeration source is safe.

COHORTS (measured 2026-09-01 against prod reciterdb)
------------------------------------------------------
  Cohort A -- old_len == 0 (no `authors` attribute, or an empty list): 2,446 rows.
    2,187 have an `authorship_review` row with authors_json length >= 2 (source
    the DB, no network); 258 have length 1 (still routed to live lookup -- see
    "Otherwise" above); 1 has length 0.
  Cohort B -- old_len == 1 (exactly one author already stored): 429 rows.
    114 have authors_json length >= 2 (fixed from the DB); 314 have length 1
    (routed to live lookup to VERIFY the single author against Scopus, not
    overwrite it -- see `verified_single_author` below); 1 has length 0.
  Net: 2,301 rows resolved from authorship_review with zero network calls;
  ~574 rows require a live Scopus lookup (matches the day's measured count).

RUN PLAN
--------
  1. Dry run (this script's default; no --apply) over --cohort all -- produces
     the per-cohort x per-source x per-outcome counts and a ledger for review.
  2. `--cohort A --apply` -- writes only the zero-author rows first (lowest risk:
     nothing existing can be clobbered, `size(authors) <= 0` is unconditionally
     true against attribute-not-exists/NULL/empty-list starting states).
  3. `--cohort B --apply` -- writes the one-author rows once A is verified clean.
  4. Next-morning recount: re-run the SAME enumeration SELECT below (unchanged)
     against the now-refreshed nightly projection to confirm old_len counts
     dropped as expected. This script performs no counting beyond a single run;
     the recount is just re-running enumeration and reading the new totals.

CREDENTIALS
-----------
  --apply requires `dynamodb:UpdateItem` on the `ExternalArticle` table (us-east-1,
  account 665083158573) -- a table SHARED between dev and prod (no key-space
  separation). A dry run needs no AWS credentials at all: the boto3 DynamoDB
  resource/table is constructed only when --apply is passed. reciterdb access
  needs the standard DB_HOST/DB_USERNAME/DB_PASSWORD/DB_NAME env vars (read-only
  SELECT); the Scopus lookup needs RECITER_SCOPUS_SEARCH_URL (or --scopus-url).

Usage:
  python3 update/backfill_external_article_authors_932.py --selfcheck
  python3 update/backfill_external_article_authors_932.py --ledger PATH [--cohort A|B|all] [--limit N] [--scopus-url URL]
  python3 update/backfill_external_article_authors_932.py --apply --cohort A --ledger PATH   # foreman-run only, after sign-off
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import create_engine, text

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

# Enumeration SELECT. Uses MAX() rather than ANY_VALUE() for the two
# single-value-per-group columns: tested directly against prod reciterdb
# 2026-09-01, this MariaDB build raises "FUNCTION reciterdb.ANY_VALUE does not
# exist" (1305) -- MAX() over a per-(uid,article_id) group is equivalent here
# since every row in the group shares the same ea.doi/old_len by construction.
# ponytail: skip a try-ANY_VALUE-then-fall-back-to-MAX dance -- confirmed once,
# empirically, that this server needs MAX; no runtime feature-detection needed.
ENUMERATION_SQL = text("""
    SELECT ea.uid, ea.article_id, SUBSTRING(ea.article_id, 8) AS external_id,
           MAX(ea.doi) AS doi,
           MAX(CASE WHEN ea.authors IS NULL OR JSON_LENGTH(ea.authors) = 0 THEN 0
                    ELSE JSON_LENGTH(ea.authors) END) AS old_len,
           MAX(ar.authors_json) AS authors_json
    FROM external_article ea
    LEFT JOIN authorship_review ar
      ON ar.source = 'scopus'
     AND ar.external_id COLLATE utf8mb4_unicode_ci = SUBSTRING(ea.article_id, 8)
    WHERE ea.source_type = 'SCOPUS' AND (ea.authors IS NULL OR JSON_LENGTH(ea.authors) <= 1)
    GROUP BY ea.uid, ea.article_id
""")


def db_engine():
    """SQLAlchemy engine against reciterdb -- same pattern as update/aar_db.py::engine()."""
    return create_engine(
        f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
        connect_args={"connect_timeout": 15}, pool_pre_ping=True,
    )


def enumerate_rows(engine):
    """Run the enumeration SELECT once; return a list of row mappings."""
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(ENUMERATION_SQL)]


def row_cohort(old_len):
    """old_len is always 0 or 1 by construction of ENUMERATION_SQL's WHERE clause."""
    return "A" if old_len == 0 else "B"


# --------------------------------------------------------------------------
# Pure name-shaping logic (covered by --selfcheck; no network, no DB, no AWS).
# --------------------------------------------------------------------------

def _name_from_given_surname(given, surname):
    """PM rule (scopusAuthorsFromRow): "given surname" when both non-empty, else
    whichever is non-empty, else None (caller decides the empty-both fallback)."""
    given = (given or "").strip()
    surname = (surname or "").strip()
    if given and surname:
        return f"{given} {surname}"
    if given:
        return given
    if surname:
        return surname
    return None


def names_from_authors_json(authors_json_text):
    """Parse authorship_review.authors_json ([{"given":..,"surname":..}, ...]) into
    display names via the PM rule verbatim (drop entries where both are empty --
    no authname fallback exists in this source, unlike the live Scopus path)."""
    if not authors_json_text:
        return []
    try:
        entries = json.loads(authors_json_text)
    except (TypeError, ValueError):
        return []
    if not isinstance(entries, list):
        return []
    names = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        n = _name_from_given_surname(e.get("given"), e.get("surname"))
        if n:
            names.append(n)
    return names


def names_from_scopus_authors(author_list):
    """Same given/surname rule as above, but with an authname fallback when both
    given-name and surname are empty (live Scopus `author[]` carries `authname`;
    authorship_review's authors_json does not). NEVER falls back to dc:creator --
    a document whose author[] entries are all empty is `no_authors`, not a
    single-string guess from the entry-level creator field."""
    if not author_list:
        return []
    if isinstance(author_list, dict):
        author_list = [author_list]  # tolerate a single-author dict, not a 1-element list
    names = []
    for a in author_list:
        if not isinstance(a, dict):
            continue
        n = _name_from_given_surname(a.get("given-name"), a.get("surname"))
        if n is None:
            authname = (a.get("authname") or "").strip()
            n = authname or None
        if n:
            names.append(n)
    return names


def classify_write_outcome(old_len, new_len):
    """old_len in {0,1} always (enumeration WHERE clause). new_len >= 1 always
    (caller routes new_len == 0 to 'no_authors' before reaching here)."""
    if old_len == 1 and new_len == 1:
        return "verified_single_author"
    return "fixed"


def plan_row(row, live_lookup_fn):
    """Pure orchestration of the per-row decision, given an injectable
    live_lookup_fn(external_id, doi) -> (status, author_list) so this is fully
    testable under --selfcheck without any network call. status is one of
    'ok' | 'not_found' | 'failed'.

    Returns dict: {source, new_len, outcome, names}
      source: 'authors_json' | 'scopus'
      outcome: 'fixed' | 'verified_single_author' | 'not_found' | 'no_authors' | 'lookup_failed'
      names: list[str], only meaningful when outcome == 'fixed'
    """
    old_len = row["old_len"]
    db_names = names_from_authors_json(row.get("authors_json"))
    if len(db_names) >= 2:
        return {
            "source": "authors_json",
            "new_len": len(db_names),
            "outcome": classify_write_outcome(old_len, len(db_names)),
            "names": db_names,
        }

    status, author_list = live_lookup_fn(row["external_id"], row.get("doi"))
    if status == "failed":
        return {"source": "scopus", "new_len": None, "outcome": "lookup_failed", "names": []}
    if status == "not_found":
        return {"source": "scopus", "new_len": None, "outcome": "not_found", "names": []}

    live_names = names_from_scopus_authors(author_list)
    if not live_names:
        return {"source": "scopus", "new_len": 0, "outcome": "no_authors", "names": []}
    return {
        "source": "scopus",
        "new_len": len(live_names),
        "outcome": classify_write_outcome(old_len, len(live_names)),
        "names": live_names,
    }


# --------------------------------------------------------------------------
# Live Scopus lookup (network I/O -- not covered by --selfcheck).
# --------------------------------------------------------------------------

def _scopus_search(session, base_url, query):
    """POST one search query (EID(...) or DOI(...)) with view=COMPLETE, count=1.
    Returns (status, entry) where status is 'ok' | 'not_found' | 'failed', entry
    is the search-results.entry[0] dict on 'ok' else None. Never raises; throttles
    SLEEP_BETWEEN_CALLS after every actual HTTP call (this function is the only
    place an HTTP request happens, so throttling here covers EID + DOI-fallback
    calls uniformly)."""
    import requests  # local import: keeps --selfcheck import-light and network-free

    url = base_url.rstrip("/") + SEARCH_PATH
    body = {"query": query, "count": 1, "view": "COMPLETE"}
    try:
        resp = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        logger.error(f"    Scopus lookup FAILED for query={query!r}: {e}")
        return ("failed", None)
    except ValueError as e:  # non-JSON body
        logger.error(f"    Scopus lookup FAILED (bad JSON) for query={query!r}: {e}")
        return ("failed", None)
    finally:
        time.sleep(SLEEP_BETWEEN_CALLS)

    results = payload.get("search-results", {})
    total = results.get("opensearch:totalResults")
    entries = results.get("entry") or []
    if total == "0" or not entries or "dc:identifier" not in entries[0]:
        return ("not_found", None)
    return ("ok", entries[0])


def make_live_lookup_fn(session, base_url):
    """Bind session/base_url into a live_lookup_fn(external_id, doi) for plan_row.
    EID first; DOI fallback ONLY when EID comes back not_found AND a DOI is present
    (never on a network failure -- that stays 'failed' so it surfaces as lookup_failed,
    not a silently-different not_found)."""

    def _lookup(external_id, doi):
        status, entry = _scopus_search(session, base_url, f"EID(2-s2.0-{external_id})")
        if status == "ok":
            return ("ok", entry.get("author"))
        if status == "failed":
            return ("failed", None)
        # not_found -- try DOI fallback if we have one
        doi = (doi or "").strip()
        if not doi:
            return ("not_found", None)
        status2, entry2 = _scopus_search(session, base_url, f"DOI({doi})")
        if status2 == "ok":
            return ("ok", entry2.get("author"))
        return (status2, None)  # 'not_found' or 'failed'

    return _lookup


# --------------------------------------------------------------------------
# DynamoDB write (apply-only; never constructed/imported for a dry run).
# --------------------------------------------------------------------------

def apply_update(table, key, names, old_len):
    """Conditional SET of the `authors` List<String>. The condition covers all three
    starting states this backfill targets -- attribute entirely absent, present as
    DynamoDB NULL, or present as a list whose size hasn't grown past old_len since
    enumeration -- so a concurrent write of a REAL (bigger) authors list is never
    clobbered. Raises botocore.exceptions.ClientError (including
    ConditionalCheckFailedException) or BotoCoreError on failure; caller classifies."""
    table.update_item(
        Key=key,
        UpdateExpression="SET authors = :a",
        ConditionExpression=(
            "attribute_not_exists(authors) OR attribute_type(authors, :nul) "
            "OR size(authors) <= :n"
        ),
        ExpressionAttributeValues={":a": names, ":nul": "NULL", ":n": old_len},
    )


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="Write to DynamoDB. Without this flag, dry-run only (no writes, no AWS creds needed).")
    parser.add_argument("--cohort", choices=["A", "B", "all"], default="all",
                        help="A = old_len 0 (no authors), B = old_len 1 (single author). Default: all.")
    parser.add_argument("--limit", type=int, default=None, help="Cap the number of rows processed (post cohort filter).")
    parser.add_argument("--ledger", required=True, help="Path to append the JSONL ledger to.")
    parser.add_argument("--scopus-url", default=os.getenv("RECITER_SCOPUS_SEARCH_URL"),
                        help="Base URL of the Scopus retrieval tool (default: env RECITER_SCOPUS_SEARCH_URL).")
    parser.add_argument("--selfcheck", action="store_true", help="Run pure-function selfcheck and exit (handled before this parser runs).")
    args = parser.parse_args()

    mode = "apply" if args.apply else "dry-run"
    logger.info(f"backfill_external_article_authors_932 starting in {mode.upper()} mode "
                f"(cohort={args.cohort}, limit={args.limit}, scopus={args.scopus_url})")

    engine = db_engine()
    rows = enumerate_rows(engine)
    logger.info(f"enumerated {len(rows)} candidate rows from reciterdb")

    if args.cohort != "all":
        rows = [r for r in rows if row_cohort(r["old_len"]) == args.cohort]
        logger.info(f"filtered to cohort {args.cohort}: {len(rows)} rows")
    if args.limit is not None:
        rows = rows[:args.limit]
        logger.info(f"limited to first {len(rows)} rows")

    needs_network = any(len(names_from_authors_json(r.get("authors_json"))) < 2 for r in rows)
    if needs_network and not args.scopus_url:
        logger.error("No Scopus URL available (--scopus-url / RECITER_SCOPUS_SEARCH_URL) and at "
                     "least one row requires a live lookup.")
        sys.exit(2)

    table = None
    key_names = None
    if args.apply:
        import boto3
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        table = boto3.resource("dynamodb", region_name=region).Table(DDB_TABLE)
        key_names = [s["AttributeName"] for s in table.key_schema]
        logger.info(f"DynamoDB key schema: {key_names}")

    session = None
    live_lookup_fn = None
    if needs_network:
        import requests
        session = requests.Session()
        live_lookup_fn = make_live_lookup_fn(session, args.scopus_url)
    else:
        def live_lookup_fn(external_id, doi):  # pragma: no cover -- never called when needs_network is False
            raise AssertionError("live_lookup_fn invoked but no row required network access")

    counts = {}  # (cohort, source, outcome) -> int
    fixed_preview = []
    problem_rows = []
    lookup_failed_count = 0
    write_failed_count = 0

    with open(args.ledger, "a", encoding="utf-8") as ledger_f:
        for row in rows:
            cohort = row_cohort(row["old_len"])
            plan = plan_row(row, live_lookup_fn)
            source, new_len, outcome, names = plan["source"], plan["new_len"], plan["outcome"], plan["names"]

            # ponytail: one ledger line per row, written once the row's final outcome
            # is known, rather than a separate "attempt intent" line before every
            # DynamoDB call plus a second "result" line after it. A row that never
            # reaches a write (verified_single_author / not_found / no_authors /
            # lookup_failed) has nothing for a second line to add; a row that DOES
            # write is logged right after that single update_item call resolves, so
            # the line is written "before" any further processing and IS the "after
            # the write attempt" record for that write -- one line satisfies both
            # halves of the spec without a redundant duplicate per row.
            final_outcome = outcome
            if outcome == "fixed":
                if not args.apply:
                    final_outcome = "would_fix"
                else:
                    key = {name: (row["uid"] if name == "uid" else row["article_id"]) for name in key_names}
                    try:
                        apply_update(table, key, names, row["old_len"])
                        final_outcome = "fixed"
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                            final_outcome = "skipped_race"
                            logger.info(f"  skipped_race (benign): uid={row['uid']} articleId={row['article_id']}")
                        else:
                            final_outcome = "failed"
                            write_failed_count += 1
                            logger.error(f"  update FAILED: uid={row['uid']} articleId={row['article_id']}: {e}")
                    except BotoCoreError as e:
                        final_outcome = "failed"
                        write_failed_count += 1
                        logger.error(f"  update FAILED: uid={row['uid']} articleId={row['article_id']}: {e}")

            key3 = (cohort, source, final_outcome)
            counts[key3] = counts.get(key3, 0) + 1

            ledger_entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "mode": mode,
                "uid": row["uid"],
                "articleId": row["article_id"],
                "cohort": cohort,
                "source": source,
                "old_len": row["old_len"],
                "new_len": new_len,
                "outcome": final_outcome,
            }
            if final_outcome in ("fixed", "would_fix"):
                ledger_entry["names"] = names
            ledger_f.write(json.dumps(ledger_entry) + "\n")

            if final_outcome in ("fixed", "would_fix") and len(fixed_preview) < 10:
                fixed_preview.append((row["uid"], row["article_id"], row["old_len"], new_len, names[:3]))
            if final_outcome in ("not_found", "no_authors", "lookup_failed"):
                problem_rows.append((final_outcome, row["uid"], row["article_id"], row["external_id"], row.get("doi")))
                if final_outcome == "lookup_failed":
                    lookup_failed_count += 1

    logger.info("==== per-cohort x per-source x per-outcome counts ====")
    for (cohort, source, outcome), n in sorted(counts.items()):
        logger.info(f"  cohort={cohort} source={source:<12} outcome={outcome:<22} count={n}")

    logger.info(f"==== first {min(10, len(fixed_preview))} planned writes ====")
    for uid, article_id, old_len, new_len, sample_names in fixed_preview:
        logger.info(f"  uid={uid} articleId={article_id} {old_len}->{new_len} names={sample_names}")

    logger.info(f"==== every not_found / no_authors / lookup_failed row ({len(problem_rows)}) ====")
    for outcome, uid, article_id, external_id, doi in problem_rows:
        logger.info(f"  {outcome}: uid={uid} articleId={article_id} external_id={external_id} doi={doi}")

    logger.info("==== summary ====")
    logger.info(f"  rows processed: {len(rows)}")
    logger.info(f"  lookup_failed: {lookup_failed_count}")
    if args.apply:
        logger.info(f"  write failed: {write_failed_count}")
        logger.info("reciterdb.external_article reflects these writes on the next nightly projection.")
    else:
        logger.info("DRY RUN -- no DynamoDB writes made. Re-run with --apply (foreman-run only) to write.")

    if lookup_failed_count or write_failed_count:
        logger.error(f"exiting non-zero: {lookup_failed_count} lookup failure(s), {write_failed_count} write failure(s).")
        sys.exit(1)


def _selfcheck():
    # -- name shaping (PM rule: "given surname", else whichever is non-empty) --
    assert _name_from_given_surname("Alaa", "Abd-Alrazaq") == "Alaa Abd-Alrazaq"
    assert _name_from_given_surname("Alaa", "") == "Alaa"
    assert _name_from_given_surname("", "Abd-Alrazaq") == "Abd-Alrazaq"
    assert _name_from_given_surname("", "") is None
    assert _name_from_given_surname(None, None) is None

    # -- authors_json (DB source, no authname fallback) --
    assert names_from_authors_json(None) == []
    assert names_from_authors_json("") == []
    assert names_from_authors_json("not json") == []
    assert names_from_authors_json('[{"given":"Dari","surname":"Alhuwail"},{"given":"Alaa","surname":"Abd-Alrazaq"}]') == \
        ["Dari Alhuwail", "Alaa Abd-Alrazaq"]
    assert names_from_authors_json('[{"given":"","surname":""}]') == []

    # -- live Scopus authors (authname fallback when both given/surname empty) --
    assert names_from_scopus_authors([{"given-name": "Alaa", "surname": "Abd-Alrazaq"}]) == ["Alaa Abd-Alrazaq"]
    assert names_from_scopus_authors([{"given-name": "", "surname": "", "authname": "Abd-Alrazaq A."}]) == ["Abd-Alrazaq A."]
    assert names_from_scopus_authors([{"given-name": "", "surname": "", "authname": ""}]) == []  # no dc:creator fallback
    assert names_from_scopus_authors([]) == []
    assert names_from_scopus_authors(None) == []
    # tolerate a bare dict (single author, not wrapped in a list)
    assert names_from_scopus_authors({"given-name": "Alaa", "surname": "Abd-Alrazaq"}) == ["Alaa Abd-Alrazaq"]

    # -- outcome classification --
    assert row_cohort(0) == "A"
    assert row_cohort(1) == "B"
    assert classify_write_outcome(0, 1) == "fixed"
    assert classify_write_outcome(0, 3) == "fixed"
    assert classify_write_outcome(1, 1) == "verified_single_author"
    assert classify_write_outcome(1, 2) == "fixed"

    # -- plan_row, cohort A: authors_json alone has >= 2 names -> no network --
    def _boom(*_a, **_k):
        raise AssertionError("live_lookup_fn should not be called")
    row_a1 = {"old_len": 0, "authors_json": '[{"given":"A","surname":"B"},{"given":"C","surname":"D"}]',
              "external_id": "1", "doi": None}
    p = plan_row(row_a1, _boom)
    assert p == {"source": "authors_json", "new_len": 2, "outcome": "fixed", "names": ["A B", "C D"]}, p

    # -- plan_row, cohort A: authors_json has 1 name -> live lookup used, 3 authors found -> fixed --
    row_a2 = {"old_len": 0, "authors_json": '[{"given":"A","surname":"B"}]', "external_id": "1", "doi": None}
    p = plan_row(row_a2, lambda ext, doi: ("ok", [{"given-name": "X", "surname": "Y"},
                                                    {"given-name": "Z", "surname": "W"}]))
    assert p == {"source": "scopus", "new_len": 2, "outcome": "fixed", "names": ["X Y", "Z W"]}, p

    # -- plan_row, cohort B: old_len 1, live lookup confirms 1 author -> verified_single_author, untouched --
    row_b1 = {"old_len": 1, "authors_json": None, "external_id": "1", "doi": None}
    p = plan_row(row_b1, lambda ext, doi: ("ok", [{"given-name": "X", "surname": "Y"}]))
    assert p["outcome"] == "verified_single_author" and p["new_len"] == 1, p

    # -- plan_row: live lookup not_found --
    p = plan_row(row_b1, lambda ext, doi: ("not_found", None))
    assert p == {"source": "scopus", "new_len": None, "outcome": "not_found", "names": []}, p

    # -- plan_row: live lookup failed (network) --
    p = plan_row(row_b1, lambda ext, doi: ("failed", None))
    assert p == {"source": "scopus", "new_len": None, "outcome": "lookup_failed", "names": []}, p

    # -- plan_row: document found but zero author names -- no_authors, never falls back --
    p = plan_row(row_b1, lambda ext, doi: ("ok", []))
    assert p == {"source": "scopus", "new_len": 0, "outcome": "no_authors", "names": []}, p

    print("selfcheck OK")


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        _selfcheck()
        sys.exit(0)
    main()
