"""enrichArticles.py — journal, citation and PMCID data for external-source articles (5e).

STEP 6b of the reporting SP injects external_article rows (SCOPUS:, OPENALEX:, CROSSREF:)
with no ISSN, volume, issue, pages, citation count or PMCID: the Authorships-tab add never
stored the source record. This fills the durable table article_enrichment
(setup/table_article_enrichment_v3.9.sql), keyed by article_id, and 6b joins it.

Three passes, each over the non-suppressed external articles that are due:

  openalex  OpenAlex /works, 50 per call: OPENALEX: rows by work id, the rest by DOI.
            cited_by_count, primary_location.source ISSNs, biblio volume/issue/pages,
            ids.pmcid, created_date.
  scopus    Scopus Search (STANDARD view), EID(...) OR ... 25 per call, SCOPUS: rows only.
            citedby-count, prism:issn / prism:eIssn, volume, issue, page range. Needs
            SCOPUS_API_KEY and SCOPUS_INST_TOKEN; skipped without them.
  idconv    NCBI PMC ID converter, 200 DOIs per call, rows with a DOI and no PMCID yet.
            External rows only (plan D6): PubMed rows keep #141's PMCID rule.

Due: never fetched, or (citation counts) last fetched ENRICH_REFRESH_DAYS ago (default 7)
on the article's crc32(article_id) % 7 weekday, so the weekly refresh is spread over seven
nights instead of spending every call on one. --all ignores the weekday. The ID converter
retries an empty PMCID after IDCONV_RECHECK_DAYS (30). A miss still stamps *_fetched_at,
so it isn't retried every night; a failed call stamps nothing and is retried next run.

Which values win: ISSN, volume, issue and pages from Scopus replace OpenAlex's (Scopus is
the source of record for SCOPUS: rows); OpenAlex fills them only while empty. A count from a
later lookup replaces the older one; a lookup that misses keeps the older one.

  python3 enrichArticles.py                          # dry run: count due articles per pass
  python3 enrichArticles.py --fetch --limit 200      # dry run that calls the APIs, writes nothing
  python3 enrichArticles.py --apply                  # fetch and write
  python3 enrichArticles.py --apply --all            # backfill: also refresh every stale count
  python3 enrichArticles.py --apply --max-seconds 1140  # start no batch after 19 min
  python3 enrichArticles.py --selftest               # offline checks, no DB or network

Env: DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD; SCOPUS_API_KEY, SCOPUS_INST_TOKEN;
PUBMED_API_KEY, NCBI_EMAIL; OPENALEX_MAILTO, OPENALEX_API_KEY (both optional);
ENRICH_REFRESH_DAYS.
"""
import argparse, os, re, sys, time, zlib
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

TABLE = "article_enrichment"
OPENALEX_WORKS = "https://api.openalex.org/works"
SCOPUS_SEARCH = "https://api.elsevier.com/content/search/scopus"
IDCONV = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
OA_BATCH = 50           # OpenAlex caps an OR-filter at 50 values
SCOPUS_BATCH = 25       # Scopus Search page size the AAR lane already respects
IDCONV_BATCH = 200      # ID converter accepts up to 200 ids per call
OA_SLEEP = 0.2
SCOPUS_SLEEP = 0.3
NCBI_SLEEP = 0.12 if os.environ.get("PUBMED_API_KEY") else 0.34
IDCONV_RECHECK_DAYS = 30
SPREAD = 7              # weekly refresh spread over 7 nights by crc32(article_id) % 7
PASSES = ("openalex", "scopus", "idconv")

ISSN = re.compile(r"^(\d{4})-?(\d{3}[\dXx])$")
DOI = re.compile(r"^10\.\d{4,9}/\S+$")
WORK_ID = re.compile(r"^W\d{4,15}$")
SCOPUS_ID = re.compile(r"^\d{5,20}$")
PMCID = re.compile(r"PMC\d+")


# ---- pure helpers (selftested) ------------------------------------------------
def norm_issn(s):
    """'00029394' / '0002-9394' -> '0002-9394'; anything else -> None."""
    m = ISSN.match((s or "").strip())
    return f"{m.group(1)}-{m.group(2).upper()}" if m else None


def norm_doi(s):
    """Bare, lower-cased DOI ('https://doi.org/10.1000/X' -> '10.1000/x'), or None.
    DOIs with ',' or '|' are dropped: they would split the OR-filter / id list."""
    s = (s or "").strip()
    s = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", s, flags=re.I).strip().lower()
    if not DOI.match(s) or "," in s or "|" in s:
        return None
    return s


def clip(s, n=100):
    s = (str(s).strip() if s is not None else "")
    return s[:n] or None


def pages_of(first, last):
    first, last = clip(first, 49), clip(last, 49)
    if first and last and last != first:
        return f"{first}-{last}"
    return first


def openalex_key(article_id, doi):
    """('id', 'W…') for OPENALEX: rows, ('doi', doi) for others with a DOI, else None.
    A CROSSREF: id is the DOI itself."""
    if article_id.startswith("OPENALEX:"):
        wid = article_id[len("OPENALEX:"):]
        return ("id", wid) if WORK_ID.match(wid) else None
    if not doi and article_id.startswith("CROSSREF:"):
        doi = norm_doi(article_id[len("CROSSREF:"):])
    return ("doi", doi) if doi else None


def scopus_eid(article_id):
    """'SCOPUS:85134358470' -> '2-s2.0-85134358470'; None for anything else."""
    if not article_id.startswith("SCOPUS:"):
        return None
    sid = article_id[len("SCOPUS:"):]
    return f"2-s2.0-{sid}" if SCOPUS_ID.match(sid) else None


def parse_openalex(w):
    """OpenAlex work -> article_enrichment fields (without article_id / fetched_at)."""
    src = ((w.get("primary_location") or {}).get("source") or {})
    issn_l = norm_issn(src.get("issn_l"))
    others = [i for i in (norm_issn(x) for x in (src.get("issn") or [])) if i and i != issn_l]
    issn = issn_l or (others.pop(0) if others else None)
    bib = w.get("biblio") or {}
    pmcid = PMCID.search((w.get("ids") or {}).get("pmcid") or "")
    created = (w.get("created_date") or "")[:10]
    return {
        "doi": norm_doi(w.get("doi")),
        "openalex_id": (w.get("id") or "").rsplit("/", 1)[-1] or None,
        "issn": issn,
        "eissn": others[0] if others else None,
        "volume": clip(bib.get("volume")),
        "issue": clip(bib.get("issue")),
        "pages": pages_of(bib.get("first_page"), bib.get("last_page")),
        "openalex_created": created if re.match(r"^\d{4}-\d{2}-\d{2}$", created) else None,
        "citation_count_openalex": w.get("cited_by_count"),
        "pmcid": pmcid.group(0) if pmcid else None,
    }


def parse_scopus(e):
    """Scopus Search entry -> (eid, fields)."""
    try:
        cites = int(e.get("citedby-count"))
    except (TypeError, ValueError):
        cites = None
    issn, eissn = norm_issn(e.get("prism:issn")), norm_issn(e.get("prism:eIssn"))
    return e.get("eid"), {
        "issn": issn or eissn,
        "eissn": eissn if issn else None,
        "volume": clip(e.get("prism:volume")),
        "issue": clip(e.get("prism:issueIdentifier")),
        "pages": clip(e.get("prism:pageRange")),
        "citation_count_scopus": cites,
    }


def parse_idconv(body):
    """ID-converter JSON -> {requested doi (lower): 'PMC…'} for the hits."""
    out = {}
    for r in (body or {}).get("records", []) or []:
        m = PMCID.search(r.get("pmcid") or "")
        req = (r.get("requested-id") or r.get("doi") or "").strip().lower()
        if m and req:
            out[req] = m.group(0)
    return out


def is_due(fetched_at, article_id, now, refresh_days, spread=True):
    """Never fetched, or stale. With spread, a stale row is refreshed only on its own
    weekday (crc32 % 7), and 'stale' is refresh_days - 1 so a row fetched a week ago at a
    slightly later hour is not pushed back another week."""
    if fetched_at is None:
        return True
    if refresh_days <= 0:
        return True
    if not spread:
        return now - fetched_at >= timedelta(days=refresh_days)
    if zlib.crc32(article_id.encode()) % SPREAD != now.weekday():
        return False
    return now - fetched_at >= timedelta(days=max(refresh_days - 1, 0))


def idconv_due(row, now):
    if not row["doi"] or row["pmcid"]:
        return False
    f = row["idconv_fetched_at"]
    return f is None or now - f >= timedelta(days=IDCONV_RECHECK_DAYS)


def sample(rows, limit):
    """At most `limit` rows, spread evenly over the list (a cap that still mixes SCOPUS:,
    OPENALEX: and CROSSREF: ids, which sort into separate blocks). 0 = all."""
    if not limit or len(rows) <= limit:
        return rows
    step = -(-len(rows) // limit)
    return rows[::step][:limit]


def chunks(seq, n):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ---- HTTP -------------------------------------------------------------------
DEADLINE = None         # time.time() after which no new batch starts (--max-seconds)


def out_of_time(pass_name):
    """True once --max-seconds has passed. Checked before every batch, because run_all's
    ENRICH_TIMEOUT_SECONDS is only checked when this script prints a line, and a pass
    prints nothing until it ends. Rows not reached are left unstamped: next run retries."""
    if DEADLINE is not None and time.time() >= DEADLINE:
        print(f"  {pass_name}: --max-seconds reached, stopping; the rest is retried next run")
        return True
    return False


class BadBatch(Exception):
    """HTTP 400 for a batch: one value in it is malformed for the API."""


class Stats:
    def __init__(self):
        self.calls = 0
        self.seconds = 0.0
        self.failed = 0

    def per_call(self):
        return self.seconds / self.calls if self.calls else 0.0


def _http_get(url, params, headers, stats, tries=5):
    for attempt in range(tries):
        t0 = time.time()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
        except (requests.ConnectionError, requests.Timeout):
            stats.seconds += time.time() - t0
            time.sleep(2 * (attempt + 1))
            continue
        stats.calls += 1
        stats.seconds += time.time() - t0
        if r.status_code == 200:
            return r.json()
        if r.status_code == 400:
            raise BadBatch(r.text[:200])
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"{url} failed after {tries} tries")


def openalex_get(params, stats):
    mailto = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    key = (os.environ.get("OPENALEX_API_KEY") or "").strip()
    if mailto:
        params = {**params, "mailto": mailto}
    if key:
        params = {**params, "api_key": key}
    return _http_get(OPENALEX_WORKS, params, {"Accept": "application/json"}, stats)


def fetch_openalex(kind, values, stats):
    """{value: parsed} for one batch. A 400 splits the batch in halves until the bad value
    is alone; that value comes back as a miss (so it is stamped, not retried forever)."""
    field = "openalex" if kind == "id" else "doi"
    sel = "id,doi,cited_by_count,primary_location,biblio,ids,created_date"
    try:
        body = openalex_get({"filter": f"{field}:" + "|".join(values), "select": sel,
                             "per-page": OA_BATCH}, stats)
    except BadBatch:
        if len(values) == 1:
            return {}
        mid = len(values) // 2
        return {**fetch_openalex(kind, values[:mid], stats), **fetch_openalex(kind, values[mid:], stats)}
    out = {}
    for w in body.get("results", []) or []:
        p = parse_openalex(w)
        k = p["openalex_id"] if kind == "id" else p["doi"]
        if k and k not in out:      # several works per DOI: OpenAlex returns the best first
            out[k] = p
    return out


def fetch_scopus(eids, stats):
    headers = {"X-ELS-APIKey": os.environ["SCOPUS_API_KEY"],
               "X-ELS-Insttoken": os.environ["SCOPUS_INST_TOKEN"],
               "Accept": "application/json"}
    params = {"query": " OR ".join(f"EID({e})" for e in eids), "view": "STANDARD",
              "count": SCOPUS_BATCH, "field": "eid,citedby-count,prism:issn,prism:eIssn,"
              "prism:volume,prism:issueIdentifier,prism:pageRange"}
    try:
        body = _http_get(SCOPUS_SEARCH, params, headers, stats)
    except BadBatch:
        if len(eids) == 1:
            return {}
        mid = len(eids) // 2
        return {**fetch_scopus(eids[:mid], stats), **fetch_scopus(eids[mid:], stats)}
    out = {}
    for e in (body.get("search-results", {}).get("entry") or []):
        if "error" in e:
            continue
        eid, fields = parse_scopus(e)
        if eid:
            out[eid] = fields
    return out


def fetch_idconv(dois, stats):
    params = {"ids": ",".join(dois), "idtype": "doi", "format": "json",
              "tool": "reciterdb-enrich", "email": os.environ.get("NCBI_EMAIL", "")}
    if os.environ.get("PUBMED_API_KEY"):
        params["api_key"] = os.environ["PUBMED_API_KEY"]
    try:
        return parse_idconv(_http_get(IDCONV, params, {"Accept": "application/json"}, stats))
    except BadBatch:
        if len(dois) == 1:
            return {}
        mid = len(dois) // 2
        return {**fetch_idconv(dois[:mid], stats), **fetch_idconv(dois[mid:], stats)}


# ---- DB ---------------------------------------------------------------------
OA_COLS = ["doi", "issn", "eissn", "volume", "issue", "pages", "openalex_id", "openalex_created",
           "citation_count_openalex", "pmcid"]
SCOPUS_COLS = ["issn", "eissn", "volume", "issue", "pages", "citation_count_scopus"]


def upsert_sql(cols, stamp, override):
    """INSERT … ON DUPLICATE KEY UPDATE. `override` columns take the new value when it is
    non-NULL; the rest keep an existing value and only fill a NULL."""
    names = ["article_id"] + cols + [stamp]
    sets = []
    for c in cols:
        if c in override:
            sets.append(f"{c} = COALESCE(VALUES({c}), {c})")
        else:
            sets.append(f"{c} = COALESCE({c}, VALUES({c}))")
    sets.append(f"{stamp} = VALUES({stamp})")
    return (f"INSERT INTO {TABLE} ({', '.join(names)}) VALUES "
            f"({', '.join(':' + n for n in names)}) ON DUPLICATE KEY UPDATE {', '.join(sets)}")


OA_UPSERT = upsert_sql(OA_COLS, "openalex_fetched_at",
                       override={"doi", "openalex_id", "openalex_created", "citation_count_openalex"})
SCOPUS_UPSERT = upsert_sql(SCOPUS_COLS, "scopus_fetched_at",
                           override={"issn", "eissn", "volume", "issue", "pages", "citation_count_scopus"})
IDCONV_UPSERT = upsert_sql(["pmcid"], "idconv_fetched_at", override={"pmcid"})


def table_exists(conn, text, name):
    return conn.execute(text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"),
        {"t": name}).first() is not None


def load_articles(conn, text, have_table):
    """One row per non-suppressed external article_id, with its enrichment state."""
    if have_table:
        sql = f"""
            SELECT e.article_id, MIN(NULLIF(TRIM(e.doi), '')) AS doi,
                   MAX(x.openalex_fetched_at) AS openalex_fetched_at,
                   MAX(x.scopus_fetched_at) AS scopus_fetched_at,
                   MAX(x.idconv_fetched_at) AS idconv_fetched_at,
                   MAX(x.pmcid) AS pmcid
              FROM external_article e
              LEFT JOIN {TABLE} x ON x.article_id = e.article_id
             WHERE e.suppressed = 0
             GROUP BY e.article_id
             ORDER BY e.article_id"""
    else:
        sql = """
            SELECT article_id, MIN(NULLIF(TRIM(doi), '')) AS doi,
                   NULL AS openalex_fetched_at, NULL AS scopus_fetched_at,
                   NULL AS idconv_fetched_at, NULL AS pmcid
              FROM external_article WHERE suppressed = 0
             GROUP BY article_id ORDER BY article_id"""
    rows = []
    for r in conn.execute(text(sql)).mappings():
        d = dict(r)
        d["doi"] = norm_doi(d["doi"])
        if not d["doi"] and d["article_id"].startswith("CROSSREF:"):
            d["doi"] = norm_doi(d["article_id"][len("CROSSREF:"):])
        rows.append(d)
    return rows


def plan(rows, now, refresh_days, spread):
    """Due rows per pass."""
    return {
        "openalex": [r for r in rows if openalex_key(r["article_id"], r["doi"])
                     and is_due(r["openalex_fetched_at"], r["article_id"], now, refresh_days, spread)],
        "scopus": [r for r in rows if scopus_eid(r["article_id"])
                   and is_due(r["scopus_fetched_at"], r["article_id"], now, refresh_days, spread)],
        "idconv": [r for r in rows if idconv_due(r, now)],
    }


# ---- passes -----------------------------------------------------------------
def _write(eng, text, sql, batch_rows, write):
    if write and batch_rows:
        with eng.begin() as conn:
            conn.execute(text(sql), batch_rows)


def run_openalex(due, eng, text, write, now):
    stats, hits, n = Stats(), 0, 0
    by_kind = {"id": [], "doi": []}
    for r in due:
        kind, val = openalex_key(r["article_id"], r["doi"])
        by_kind[kind].append((r["article_id"], val))
    for kind, items in by_kind.items():
        for batch in chunks(items, OA_BATCH):
            if out_of_time("openalex"):
                return {"looked_up": n, "hits": hits, "stats": stats}
            try:
                found = fetch_openalex(kind, [v for _, v in batch], stats)
            except Exception as e:  # noqa: BLE001 — failed batch: nothing stamped, retried next run
                stats.failed += 1
                print(f"  openalex batch failed, retried next run: {e}")
                continue
            out = []
            for aid, val in batch:
                p = found.get(val) or {c: None for c in OA_COLS}
                hits += bool(found.get(val))
                out.append({"article_id": aid, **p, "openalex_fetched_at": now})
            n += len(out)
            _write(eng, text, OA_UPSERT, out, write)
            time.sleep(OA_SLEEP)
    return {"looked_up": n, "hits": hits, "stats": stats}


def run_scopus(due, eng, text, write, now):
    stats, hits, n = Stats(), 0, 0
    for batch in chunks([(r["article_id"], scopus_eid(r["article_id"])) for r in due], SCOPUS_BATCH):
        if out_of_time("scopus"):
            break
        try:
            found = fetch_scopus([e for _, e in batch], stats)
        except Exception as e:  # noqa: BLE001
            stats.failed += 1
            print(f"  scopus batch failed, retried next run: {e}")
            continue
        out = []
        for aid, eid in batch:
            f = found.get(eid) or {c: None for c in SCOPUS_COLS}
            hits += bool(found.get(eid))
            out.append({"article_id": aid, **f, "scopus_fetched_at": now})
        n += len(out)
        _write(eng, text, SCOPUS_UPSERT, out, write)
        time.sleep(SCOPUS_SLEEP)
    return {"looked_up": n, "hits": hits, "stats": stats}


def run_idconv(due, eng, text, write, now):
    stats, hits, n = Stats(), 0, 0
    for batch in chunks([(r["article_id"], r["doi"]) for r in due], IDCONV_BATCH):
        if out_of_time("idconv"):
            break
        try:
            found = fetch_idconv([d for _, d in batch], stats)
        except Exception as e:  # noqa: BLE001
            stats.failed += 1
            print(f"  idconv batch failed, retried next run: {e}")
            continue
        out = [{"article_id": aid, "pmcid": found.get(doi), "idconv_fetched_at": now}
               for aid, doi in batch]
        hits += sum(1 for o in out if o["pmcid"])
        n += len(out)
        _write(eng, text, IDCONV_UPSERT, out, write)
        time.sleep(NCBI_SLEEP)
    return {"looked_up": n, "hits": hits, "stats": stats}


RUNNERS = {"openalex": run_openalex, "scopus": run_scopus, "idconv": run_idconv}
BATCH_OF = {"openalex": OA_BATCH, "scopus": SCOPUS_BATCH, "idconv": IDCONV_BATCH}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="fetch and write")
    mode.add_argument("--dry-run", action="store_true", help="count due articles (default)")
    ap.add_argument("--fetch", action="store_true",
                    help="with a dry run: call the APIs and report hit rates, write nothing")
    ap.add_argument("--limit", type=int, default=0, help="max articles per pass, spread evenly over the due list (0 = no cap)")
    ap.add_argument("--all", action="store_true",
                    help="refresh every stale count tonight, not only this weekday's seventh")
    ap.add_argument("--passes", default=",".join(PASSES),
                    help=f"comma list, subset of {','.join(PASSES)}")
    ap.add_argument("--max-seconds", type=int, default=0,
                    help="stop starting new batches after this many seconds (0 = no limit)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    passes = [p.strip() for p in args.passes.split(",") if p.strip()]
    bad = [p for p in passes if p not in PASSES]
    if bad:
        ap.error(f"unknown pass(es): {bad}")
    global DEADLINE
    if args.max_seconds > 0:
        DEADLINE = time.time() + args.max_seconds
    refresh_days = int(os.environ.get("ENRICH_REFRESH_DAYS", "7"))
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    from sqlalchemy import text
    import aar_db
    eng = aar_db.engine()
    with eng.connect() as conn:
        host = conn.execute(text("SELECT @@hostname")).scalar()
        if not table_exists(conn, text, "external_article"):
            print("external_article is absent — nothing to enrich")
            return
        have_table = table_exists(conn, text, TABLE)
        rows = load_articles(conn, text, have_table)
    if args.apply and not have_table:
        print(f"{TABLE} does not exist yet (apply setup/table_article_enrichment_v3.9.sql) — skipped")
        return
    write = args.apply
    call_apis = args.apply or args.fetch
    label = "apply" if write else ("dry run, fetching" if call_apis else "dry run")
    print(f"enrichArticles ({label}) on {host}: {len(rows)} external articles; "
          f"refresh {refresh_days}d{' (all)' if args.all else ' (weekday seventh)'}"
          f"{'' if have_table else f'; {TABLE} absent, so everything counts as never fetched'}")

    due = plan(rows, now, refresh_days, spread=not args.all)
    if "scopus" in passes and not (os.environ.get("SCOPUS_API_KEY") and os.environ.get("SCOPUS_INST_TOKEN")):
        print("  scopus: SCOPUS_API_KEY / SCOPUS_INST_TOKEN unset — pass skipped")
        passes.remove("scopus")
    for p in PASSES:
        todo = sample(due[p], args.limit)
        if p not in passes:
            continue
        calls = -(-len(todo) // BATCH_OF[p])
        print(f"  {p}: {len(due[p])} due; this run {len(todo)} (about {calls} calls)")
        if not call_apis or not todo:
            continue
        t0 = time.time()
        res = RUNNERS[p](todo, eng, text, write, now)
        st = res["stats"]
        rate = 100.0 * res["hits"] / res["looked_up"] if res["looked_up"] else 0.0
        full_calls = -(-len(due[p]) // BATCH_OF[p])
        print(f"  {p}: looked up {res['looked_up']}, hits {res['hits']} ({rate:.1f}%), "
              f"{st.calls} calls, {st.per_call():.2f}s/call, {st.failed} failed batches, "
              f"{time.time() - t0:.1f}s; all {len(due[p])} due ≈ {full_calls} calls ≈ "
              f"{full_calls * (st.per_call() + {'openalex': OA_SLEEP, 'scopus': SCOPUS_SLEEP, 'idconv': NCBI_SLEEP}[p]):.0f}s"
              f"{'' if write else ' (nothing written)'}")


# ---- selftest ---------------------------------------------------------------
def _selftest():
    ok = True

    def check(name, cond):
        nonlocal ok
        print(("PASS " if cond else "FAIL ") + name)
        ok = ok and bool(cond)

    check("norm_issn hyphenates and upper-cases X",
          norm_issn("00029394") == "0002-9394" and norm_issn("0732-183x") == "0732-183X")
    check("norm_issn rejects junk", norm_issn("") is None and norm_issn("1234") is None
          and norm_issn(None) is None and norm_issn("12345-678") is None)
    check("norm_doi strips resolver prefix and lower-cases",
          norm_doi("https://doi.org/10.1109/MPRV.2022.3162900") == "10.1109/mprv.2022.3162900"
          and norm_doi("doi:10.1000/AB") == "10.1000/ab")
    check("norm_doi drops non-DOIs and filter-breaking characters",
          norm_doi("not a doi") is None and norm_doi("10.1000/a,b") is None
          and norm_doi("10.1000/a|b") is None and norm_doi(None) is None)
    check("pages_of", pages_of("6181", "6197") == "6181-6197" and pages_of("e12", None) == "e12"
          and pages_of("5", "5") == "5" and pages_of(None, None) is None)
    check("openalex_key: work id, DOI, CROSSREF id, none",
          openalex_key("OPENALEX:W4385294264", None) == ("id", "W4385294264")
          and openalex_key("SCOPUS:85168733980", "10.1093/mnras/stad2281") == ("doi", "10.1093/mnras/stad2281")
          and openalex_key("CROSSREF:10.1016/J.X.1", None) == ("doi", "10.1016/j.x.1")
          and openalex_key("SCOPUS:1", None) is None and openalex_key("OPENALEX:W1", None) is None)
    check("scopus_eid", scopus_eid("SCOPUS:85134358470") == "2-s2.0-85134358470"
          and scopus_eid("OPENALEX:W1234") is None and scopus_eid("SCOPUS:abc") is None)

    w = {"id": "https://openalex.org/W4385294264", "doi": "https://doi.org/10.1093/mnras/stad2281",
         "cited_by_count": 23,
         "primary_location": {"source": {"issn_l": "0035-8711",
                                         "issn": ["0035-8711", "1365-2966", "1365-8711"]}},
         "biblio": {"volume": "524", "issue": "4", "first_page": "6181", "last_page": "6197"},
         "ids": {"pmcid": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1234567"},
         "created_date": "2023-07-28T00:00:00"}
    p = parse_openalex(w)
    check("parse_openalex: ids, ISSNs, biblio, counts, pmcid, created",
          p["openalex_id"] == "W4385294264" and p["doi"] == "10.1093/mnras/stad2281"
          and p["issn"] == "0035-8711" and p["eissn"] == "1365-2966"
          and p["volume"] == "524" and p["issue"] == "4" and p["pages"] == "6181-6197"
          and p["citation_count_openalex"] == 23 and p["pmcid"] == "PMC1234567"
          and p["openalex_created"] == "2023-07-28")
    p = parse_openalex({"id": "https://openalex.org/W1", "primary_location": None, "biblio": None,
                        "ids": {}, "cited_by_count": 0})
    check("parse_openalex: no source / biblio -> NULLs, count 0 kept",
          p["issn"] is None and p["eissn"] is None and p["pages"] is None
          and p["citation_count_openalex"] == 0 and p["pmcid"] is None and p["openalex_created"] is None)

    eid, f = parse_scopus({"eid": "2-s2.0-85168733980", "citedby-count": "20",
                           "prism:issn": "00358711", "prism:eIssn": "13652966",
                           "prism:volume": "524", "prism:issueIdentifier": "4",
                           "prism:pageRange": "6181-6197"})
    check("parse_scopus", eid == "2-s2.0-85168733980" and f["citation_count_scopus"] == 20
          and f["issn"] == "0035-8711" and f["eissn"] == "1365-2966" and f["pages"] == "6181-6197")
    _, f = parse_scopus({"eid": "x", "prism:eIssn": "13652966"})
    check("parse_scopus: e-ISSN only becomes issn; missing count -> None",
          f["issn"] == "1365-2966" and f["eissn"] is None and f["citation_count_scopus"] is None)

    got = parse_idconv({"records": [
        {"doi": "10.1093/nar/gkab1112", "pmcid": "PMC8728269", "requested-id": "10.1093/NAR/gkab1112"},
        {"requested-id": "10.1109/x", "status": "error", "errmsg": "Identifier not found in PMC"}]})
    check("parse_idconv keeps hits keyed by lower-cased requested DOI",
          got == {"10.1093/nar/gkab1112": "PMC8728269"})

    now = datetime(2026, 11, 11, 15, 0)          # a Wednesday (weekday 2)
    aid_today = next(a for a in (f"SCOPUS:{i}" for i in range(1000))
                     if zlib.crc32(a.encode()) % SPREAD == now.weekday())
    aid_other = next(a for a in (f"SCOPUS:{i}" for i in range(1000))
                     if zlib.crc32(a.encode()) % SPREAD != now.weekday())
    check("is_due: never fetched", is_due(None, aid_other, now, 7))
    check("is_due: stale on its weekday (fetched 7 days ago, a little later in the day)",
          is_due(now - timedelta(days=7) + timedelta(hours=1), aid_today, now, 7))
    check("is_due: stale but not its weekday -> waits",
          not is_due(now - timedelta(days=30), aid_other, now, 7))
    check("is_due: fresh on its weekday -> waits", not is_due(now - timedelta(days=2), aid_today, now, 7))
    check("is_due: --all ignores the weekday", is_due(now - timedelta(days=8), aid_other, now, 7, spread=False)
          and not is_due(now - timedelta(days=3), aid_other, now, 7, spread=False))

    check("sample: cap, spread, 0 = all",
          sample(list(range(10)), 0) == list(range(10)) and sample(list(range(10)), 3) == [0, 4, 8]
          and sample(list(range(3)), 5) == [0, 1, 2])

    row = {"doi": "10.1000/a", "pmcid": None, "idconv_fetched_at": None}
    check("idconv_due: DOI, no PMCID, never tried", idconv_due(row, now))
    check("idconv_due: tried recently / has PMCID / no DOI -> no",
          not idconv_due({**row, "idconv_fetched_at": now - timedelta(days=3)}, now)
          and not idconv_due({**row, "pmcid": "PMC1"}, now)
          and not idconv_due({**row, "doi": None}, now))
    check("idconv_due: empty PMCID retried after IDCONV_RECHECK_DAYS",
          idconv_due({**row, "idconv_fetched_at": now - timedelta(days=IDCONV_RECHECK_DAYS)}, now))

    check("upsert: Scopus overrides journal fields",
          "issn = COALESCE(VALUES(issn), issn)" in SCOPUS_UPSERT
          and "citation_count_scopus = COALESCE(VALUES(citation_count_scopus), citation_count_scopus)" in SCOPUS_UPSERT)
    check("upsert: OpenAlex only fills empty journal fields, refreshes its count",
          "issn = COALESCE(issn, VALUES(issn))" in OA_UPSERT
          and "citation_count_openalex = COALESCE(VALUES(citation_count_openalex), citation_count_openalex)" in OA_UPSERT
          and "openalex_fetched_at = VALUES(openalex_fetched_at)" in OA_UPSERT)
    check("upsert: every bind name is a column", all(
        f":{c}" in OA_UPSERT for c in ["article_id"] + OA_COLS + ["openalex_fetched_at"]))

    # 400 splitting: a single bad value is isolated and the rest still come back.
    calls = []

    def fake(params, stats):
        vals = params["filter"].split(":", 1)[1].split("|")
        calls.append(len(vals))
        if "10.1000/bad" in vals:
            raise BadBatch("bad")
        return {"results": [{"id": f"https://openalex.org/W{9000 + i}", "doi": f"https://doi.org/{v}",
                             "cited_by_count": i} for i, v in enumerate(vals)]}
    global openalex_get
    real, openalex_get = openalex_get, fake
    try:
        got = fetch_openalex("doi", ["10.1000/a", "10.1000/bad", "10.1000/c", "10.1000/d"], Stats())
    finally:
        openalex_get = real
    check("fetch_openalex: a 400 batch is split until the bad DOI is alone",
          set(got) == {"10.1000/a", "10.1000/c", "10.1000/d"} and calls[0] == 4)

    global DEADLINE
    saved, DEADLINE = DEADLINE, time.time() - 1
    try:
        res = run_scopus([{"article_id": "SCOPUS:85134358470"}], None, None, False, now)
    finally:
        DEADLINE = saved
    check("--max-seconds: a pass past the deadline starts no batch and stamps nothing",
          res["looked_up"] == 0 and res["stats"].calls == 0)
    return ok


if __name__ == "__main__":
    main()
