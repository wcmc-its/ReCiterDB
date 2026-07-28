#!/usr/bin/env python3
"""Compare stored reporting_conflicts text against DynamoDB truth (#127). READ-ONLY.

audit_conflicts.sql can only ask "does this table look internally odd?". It cannot
tell whether a stored statement matches its source, and it cannot tell an escaping
bug apart from ordinary staleness. This can: it samples rows, refetches each pmid
from DynamoDB, and classifies stored-vs-source.

Reads a stratified sample rather than the whole table -- the high-risk classes are
taken exhaustively, plus a random baseline for an unbiased rate.

The discriminator that matters: if statements are missing because the old
CSV/LOAD DATA path mis-escaped them, essentially ALL of the missing ones must
contain a quote, tab, newline or backslash, because that is the only mechanism by
which that bug drops content. If the poison rate among the missing matches the
population base rate instead, the cause is staleness, not corruption -- rows are
written once and `LEFT JOIN ... WHERE a.pmid IS NULL` never revisits them, so a
statement PubMed adds after first import stays invisible forever.

Measured 2026-07-28 against reciterdb: 96.9% MATCH over 1,660 sampled rows, zero
escaping damage, and 5.9% of empty rows (~5,100 table-wide) carrying a statement in
DynamoDB of which only 4% held poison characters -- i.e. staleness, not #127.

Run: python3 auditConflictsVsDynamo.py [--sample-empty N]
"""
import argparse, collections, concurrent.futures, math, os, sys
import boto3
import pymysql.cursors

CHUNK_SIZE = 100
MAX_WORKERS = 5
MAX_UNPROCESSED_RETRIES = 8
POISON = ('"', '\t', '\n', '\r', '\\')


def connect():
    return pymysql.connect(
        user=os.environ["DB_USERNAME"], password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"], database=os.environ["DB_NAME"],
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor)


def strata(n_empty):
    # RAND(<seed>) so a re-run after a fix samples the same rows and the before/after
    # numbers are comparable rather than two different random draws.
    return {
        "has_quote":    ("SELECT pmid, conflictStatement b FROM reporting_conflicts "
                         "WHERE LENGTH(conflictStatement)>0 AND CONVERT(conflictStatement USING utf8mb4) LIKE '%\"%'"),
        "longest":      ("SELECT pmid, conflictStatement b FROM reporting_conflicts "
                         "WHERE LENGTH(conflictStatement)>5000 ORDER BY LENGTH(conflictStatement) DESC LIMIT 300"),
        "random_filled":("SELECT pmid, conflictStatement b FROM reporting_conflicts "
                         "WHERE LENGTH(conflictStatement)>0 ORDER BY RAND(11) LIMIT 600"),
        "random_empty": ("SELECT pmid, conflictStatement b FROM reporting_conflicts "
                         f"WHERE LENGTH(conflictStatement)=0 ORDER BY RAND(42) LIMIT {int(n_empty)}"),
    }


def get_coi(item):
    mc = item.get("pubmedarticle", {}).get("medlinecitation")
    return (mc.get("coiStatement") or "") if mc else ""


def fetch_chunk(chunk):
    client = boto3.resource("dynamodb").meta.client
    keys, out, attempt = [{"pmid": p} for p in chunk], {}, 0
    while keys:
        r = client.batch_get_item(RequestItems={"PubMedArticle": {"Keys": keys}})
        for it in r["Responses"].get("PubMedArticle", []):
            out[int(it["pmid"])] = get_coi(it)
        keys = r.get("UnprocessedKeys", {}).get("PubMedArticle", {}).get("Keys", [])
        attempt += 1
        if attempt > MAX_UNPROCESSED_RETRIES:
            print(f"  warning: {len(keys)} key(s) unprocessed after retries", file=sys.stderr)
            break
    return out


def classify(db, dyn, present):
    if not present:                 return "NOT_IN_DYNAMO"
    if db == dyn:                   return "MATCH"
    if db.strip() == dyn.strip():   return "MATCH_WHITESPACE"
    if not db and dyn:              return "MISSING_FROM_DB"
    if db and not dyn:              return "EXTRA_IN_DB"
    if dyn.startswith(db):          return "TRUNCATED"
    if db.startswith(dyn):          return "APPENDED"
    return "MISMATCH_OTHER"


def wilson(k, n):
    if not n:
        return 0.0, 0.0
    p, z = k / n, 1.96
    den = 1 + z * z / n
    ctr = (p + z * z / (2 * n)) / den
    mrg = z / den * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return max(0.0, ctr - mrg), min(1.0, ctr + mrg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-empty", type=int, default=3000,
                    help="how many empty rows to sample (drives the staleness estimate)")
    args = ap.parse_args()

    conn = connect()
    stored, member_of = {}, collections.defaultdict(set)
    for name, sql in strata(args.sample_empty).items():
        with conn.cursor() as c:
            c.execute(sql)
            rows = c.fetchall()
        for r in rows:
            b = r["b"]
            stored[r["pmid"]] = b.decode("utf-8", "replace") if isinstance(b, bytes) else (b or "")
            member_of[name].add(r["pmid"])
        print(f"  stratum {name}: {len(rows)} rows", file=sys.stderr)
    with conn.cursor() as c:
        c.execute("SELECT COUNT(*) n FROM reporting_conflicts WHERE LENGTH(conflictStatement)=0")
        total_empty = c.fetchone()["n"]
    conn.close()

    pmids = list(stored)
    truth = {}
    chunks = [pmids[i:i + CHUNK_SIZE] for i in range(0, len(pmids), CHUNK_SIZE)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for f in concurrent.futures.as_completed([ex.submit(fetch_chunk, c) for c in chunks]):
            truth.update(f.result())
    print(f"sampled {len(pmids)} pmids; DynamoDB answered {len(truth)}\n", file=sys.stderr)

    tally = collections.Counter()
    for p in pmids:
        tally[classify(stored[p], truth.get(p, ""), p in truth)] += 1
    print("=== stored vs DynamoDB ===")
    for k, v in tally.most_common():
        print(f"  {k:20s} {v:6d}  ({100*v/len(pmids):5.2f}%)")

    # The staleness estimate and the blame probe.
    empties = member_of["random_empty"]
    missing = [p for p in empties if truth.get(p, "")]
    lo, hi = wilson(len(missing), len(empties))
    print(f"\n=== empty rows that DO have a statement upstream ===")
    print(f"  {len(missing)} of {len(empties)} sampled = {100*len(missing)/max(1,len(empties)):.2f}% "
          f"(95% CI {100*lo:.2f}%..{100*hi:.2f}%)")
    print(f"  {total_empty} empty rows in table => approx {int(len(missing)/max(1,len(empties))*total_empty)} "
          f"affected (range {int(lo*total_empty)}..{int(hi*total_empty)})")

    poisoned = [p for p in missing if any(c in truth[p] for c in POISON)]
    base = [p for p in pmids if truth.get(p, "") and any(c in truth[p] for c in POISON)]
    have = [p for p in pmids if truth.get(p, "")]
    print(f"\n=== blame probe ===")
    print(f"  poison chars among the MISSING   : {len(poisoned)}/{len(missing)} "
          f"({100*len(poisoned)/max(1,len(missing)):.1f}%)")
    print(f"  poison chars in the POPULATION   : {len(base)}/{len(have)} "
          f"({100*len(base)/max(1,len(have)):.1f}%)")
    print("  near 100% among the missing => CSV/LOAD DATA escaping damage.")
    print("  comparable to the population => staleness: the backfill never revisits an existing row.")


if __name__ == "__main__":
    main()
