"""externalAbstractImport.py — abstracts for external-source articles.

reporting_abstracts is keyed by PubMed pmid, so OPENALEX: external articles (conference
abstracts and preprints from the AAR OpenAlex lane, or added by a curator) never get one.
This fills reporting_external_abstracts (setup/table_reporting_external_abstracts_v3.3.sql)
from OpenAlex's abstract_inverted_index.

Fetch-once, like abstractImport.py: only article_ids with no row are fetched. A work with
no abstract gets a row with abstract NULL and is rechecked after RECHECK_DAYS. A failed
OpenAlex batch writes nothing, so it is simply retried next run. Rows are never deleted.

SCOPUS: ids are not fetched yet (that needs the Scopus abstract API); the table and the
`source` column are ready for them.

  python3 externalAbstractImport.py            # dry run: count what would be fetched
  python3 externalAbstractImport.py --apply    # fetch and write
  python3 externalAbstractImport.py --selftest # offline checks, no DB or network
"""
import argparse, os, re, sys, time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TABLE = "reporting_external_abstracts"
OPENALEX_WORKS = "https://api.openalex.org/works"
BATCH = 50              # OpenAlex caps an `openalex:` OR-filter at 50 ids
SLEEP = 0.2             # polite throttle between calls
RECHECK_DAYS = 30       # recheck "no abstract" rows after this long
MAX_CHARS = 15000       # same cap as reporting_abstracts.abstractVarchar
# Well-formed OpenAlex work ids only: one malformed id (e.g. "W1") makes OpenAlex 400 the
# whole 50-id batch, while a well-formed id that doesn't exist is just absent from results.
WORK_ID = re.compile(r"^W\d{4,15}$")


def reconstruct(index):
    """OpenAlex ships abstracts as {word: [positions]}; put the words back in order."""
    if not index:
        return None
    words = {}
    for word, positions in index.items():
        for p in positions:
            words[p] = word
    text = " ".join(words[p] for p in sorted(words)).strip()
    return text[:MAX_CHARS] or None


def rows_for_batch(work_ids, results, now):
    """One row per requested id: the abstract if OpenAlex returned one, else NULL."""
    found = {}
    for w in results:
        wid = w["id"].rsplit("/", 1)[-1]
        found[wid] = reconstruct(w.get("abstract_inverted_index"))
    out = []
    for wid in work_ids:
        text = found.get(wid)
        out.append({"article_id": f"OPENALEX:{wid}", "abstract": text,
                    "abstract_len": len(text) if text else None,
                    "source": "openalex", "fetched_at": now})
    return out


def _get(params):
    mailto = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mailto:
        params = {**params, "mailto": mailto}
    for attempt in range(5):
        try:
            r = requests.get(OPENALEX_WORKS, params=params, timeout=60,
                             headers={"Accept": "application/json"})
        except (requests.ConnectionError, requests.Timeout):
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
    raise RuntimeError("OpenAlex fetch failed after retries")


def pending_ids(conn, text):
    """OPENALEX work ids with no row, or a NULL row older than RECHECK_DAYS."""
    rows = conn.execute(text(f"""
        SELECT DISTINCT SUBSTRING(e.article_id, 10) AS wid
          FROM external_article e
          LEFT JOIN {TABLE} a ON a.article_id = e.article_id
         WHERE e.article_id LIKE 'OPENALEX:W%'
           AND (a.article_id IS NULL
                OR (a.abstract IS NULL AND a.fetched_at < NOW() - INTERVAL {RECHECK_DAYS} DAY))
    """)).fetchall()
    ids = [r[0] for r in rows]
    good = [i for i in ids if WORK_ID.match(i)]
    if len(good) != len(ids):
        print(f"skipping {len(ids) - len(good)} malformed OpenAlex ids: "
              f"{[i for i in ids if not WORK_ID.match(i)][:5]}")
    return sorted(good)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=5000, help="max ids per run")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    from sqlalchemy import text
    import aar_db
    eng = aar_db.engine()
    with eng.connect() as conn:
        exists = conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = :t"), {"t": TABLE}).first()
        if not exists:
            print(f"{TABLE} does not exist yet (apply setup/"
                  f"table_reporting_external_abstracts_v3.3.sql) — skipped")
            return
        ids = pending_ids(conn, text)[: args.limit]
    print(f"{len(ids)} OpenAlex works to fetch ({'apply' if args.apply else 'dry run'})")
    if not args.apply or not ids:
        return

    upsert = text(f"""
        INSERT INTO {TABLE} (article_id, abstract, abstract_len, source, fetched_at)
        VALUES (:article_id, :abstract, :abstract_len, :source, :fetched_at)
        ON DUPLICATE KEY UPDATE abstract = VALUES(abstract),
            abstract_len = VALUES(abstract_len), source = VALUES(source),
            fetched_at = VALUES(fetched_at)""")
    written = with_text = failed = 0
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i + BATCH]
        try:
            body = _get({"filter": "openalex:" + "|".join(batch),
                         "select": "id,abstract_inverted_index", "per-page": BATCH})
        except Exception as e:  # noqa: BLE001 — a failed batch is retried next run
            failed += 1
            print(f"batch {i // BATCH} failed, retried next run: {e}")
            continue
        rows = rows_for_batch(batch, body.get("results", []), datetime.now(timezone.utc).replace(tzinfo=None))
        with eng.begin() as conn:
            conn.execute(upsert, rows)
        written += len(rows)
        with_text += sum(1 for r in rows if r["abstract"])
        time.sleep(SLEEP)
    print(f"wrote {written} rows ({with_text} with an abstract, "
          f"{written - with_text} without); {failed} failed batches")


def _selftest():
    ok = True

    def check(name, cond):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name)
        ok = ok and bool(cond)

    check("reconstruct orders words by position",
          reconstruct({"world": [1], "hello": [0], "again": [3], "hi": [2]})
          == "hello world hi again")
    check("reconstruct: none/empty -> None", reconstruct(None) is None and reconstruct({}) is None)
    check("reconstruct caps at MAX_CHARS",
          len(reconstruct({"x" * 20000: [0]})) == MAX_CHARS)
    now = datetime(2026, 11, 3)
    rows = rows_for_batch(
        ["W100001", "W100002", "W100003"],
        [{"id": "https://openalex.org/W100001", "abstract_inverted_index": {"a": [0]}},
         {"id": "https://openalex.org/W100002", "abstract_inverted_index": None}], now)
    check("one row per requested id, prefixed",
          [r["article_id"] for r in rows]
          == ["OPENALEX:W100001", "OPENALEX:W100002", "OPENALEX:W100003"])
    check("returned with abstract -> text + len",
          rows[0]["abstract"] == "a" and rows[0]["abstract_len"] == 1)
    check("returned without abstract / not returned -> NULL row",
          rows[1]["abstract"] is None and rows[2]["abstract"] is None
          and rows[2]["abstract_len"] is None)
    check("WORK_ID accepts real ids, rejects malformed",
          WORK_ID.match("W4417001897") and not WORK_ID.match("W1")
          and not WORK_ID.match("W44;drop"))
    return ok


if __name__ == "__main__":
    main()
