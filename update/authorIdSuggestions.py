"""Nightly author-id suggestions (3f): each person's Scopus Author IDs and OpenAlex author
ids, voted from their ACCEPTED papers, into reciterdb.person_author_id.

Votes (one per accepted paper, newest 50 PMIDs):
  Scopus   - DynamoDB ScopusArticle (key = PMID) already stores scopusarticle.authors[] with
             authid, seq and surname, so the vote costs DynamoDB reads, not Scopus quota.
  OpenAlex - /works?filter=pmid:a|b|..., 50 PMIDs per call.
  A paper votes for the id at the person's target-author rank when the surname agrees there,
  else for the one author with that surname; anything else is unresolvable.
ORCID lookups (trusted iDs only): OpenAlex /authors/orcid:<iD>, and Scopus Author Search
  ORCID(<iD>) for people with fewer than 3 accepted papers (capped per run). Trusted means
  Scholars-confirmed (scholar_active.has_confirmed_orcid = 1, iD from DynamoDB Identity) or
  saved by a PM curator for a non-scholar (admin_orcid).

Writes (person_author_id):
  - SUGGESTED rows with votes, resolvable, share and evidence, refreshed when re-voted.
  - CONFIRMED with method AUTO when votes >= 5, share >= 0.8, no other uid holds the id as
    SUGGESTED/CONFIRMED, and this person has not REJECTED it.
  - Never touched: rows whose status is CONFIRMED or REJECTED, or whose method is CURATOR.
  - A re-voted person's SUGGESTED PUB_VOTE rows that no longer get a vote are deleted.
Cadence: a person is re-voted when crc32(uid) % 7 equals today's weekday (UTC) and they have
  never been voted, or whenever their newest-50 accepted-PMID set changed since the last vote
  (person_author_id_vote). The first run is spread over 7 nights.
Not written: external cohorts (uid containing "_").

  python3 authorIdSuggestions.py [--dry-run]        (default; reads only)
  python3 authorIdSuggestions.py --apply
  python3 authorIdSuggestions.py --selftest         (no network, no DB)
  options: --all (ignore the weekday shard), --uids a,b, --limit N, --no-openalex, --no-scopus
"""
import argparse, datetime, hashlib, json, os, re, sys, time, unicodedata, zlib
from collections import Counter, defaultdict

MAX_PMIDS = 50                 # newest accepted PMIDs voted per person
AUTO_MIN_VOTES = 5
AUTO_MIN_SHARE = 0.8
KEEP_MIN_VOTES = 2             # a non-top id needs this many votes to be suggested at all
EVIDENCE_PMIDS = 10
ORCID_SCOPUS_MAX_ACCEPTED = 3  # Scopus Author Search fallback only below this many accepted papers
COMMIT_EVERY = 200             # people per transaction, so a timeout keeps finished work
TYPES = ("SCOPUS_AUID", "OPENALEX_AUTHOR")
ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")

OPENALEX = "https://api.openalex.org"
OPENALEX_SLEEP = 0.2
SCOPUS_AUTHOR_SEARCH = "https://api.elsevier.com/content/search/author"
SCOPUS_SLEEP = 0.3


def log(msg):
    print(msg, flush=True)


def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return "".join(c for c in s if c.isalpha())


def shard_of(uid):
    return zlib.crc32(uid.encode()) % 7


def pmid_set_hash(pmids):
    return hashlib.sha1(",".join(str(p) for p in sorted(pmids)).encode()).hexdigest()


# ---- voting (pure) -----------------------------------------------------------
def pick_slot(authors, rank, last, surname, ident, pos=None):
    """The id of the author at 1-based `rank` if the surname agrees, else of the one author
    with that surname; None when neither resolves. `surname(a, want)`, `ident(a)` and
    `pos(a)` read an author record (Scopus and OpenAlex shape them differently); `pos`
    defaults to byline order."""
    want = norm(last)
    if not want:
        return None
    pos = pos or (lambda a, i: i)
    hits = [a for i, a in enumerate(authors, 1) if pos(a, i) == rank and surname(a, want)]
    if not hits:
        hits = [a for a in authors if surname(a, want)]
    if len(hits) != 1:
        return None
    return ident(hits[0])


def _authid(v):
    """DynamoDB numbers arrive as Decimal; an AU-ID is an integer."""
    if v in (None, ""):
        return None
    s = str(v)
    return s[:-2] if s.endswith(".0") else s


def scopus_slot(item, rank, last):
    """Scopus: authors carry seq (1-based), surname and authid (the AU-ID)."""
    au = ((item or {}).get("scopusarticle") or {}).get("authors") or []
    return pick_slot(au, rank, last,
                     surname=lambda a, want: norm(a.get("surname")) == want,
                     ident=lambda a: _authid(a.get("authid")),
                     pos=lambda a, i: int(a.get("seq") or 0))


def openalex_slot(work, rank, last):
    """OpenAlex: authorships are in byline order; display_name holds the whole name, so the
    surname check is containment (the probe's rule)."""
    au = (work or {}).get("authorships") or []
    return pick_slot(au, rank, last,
                     surname=lambda a, want: want in norm((a.get("author") or {}).get("display_name")),
                     ident=lambda a: ((a.get("author") or {}).get("id") or "").rsplit("/", 1)[-1] or None)


def tally(pubs, slot):
    """pubs: [(pmid, rank, last)]; slot(pmid, rank, last) -> id | None.
    Returns (votes Counter, {id: [pmids]}, resolvable)."""
    votes, voters, resolvable = Counter(), defaultdict(list), 0
    for pmid, rank, last in pubs:
        i = slot(pmid, rank, last)
        if i:
            votes[i] += 1
            voters[i].append(pmid)
            resolvable += 1
    return votes, voters, resolvable


def candidates(votes, voters, resolvable):
    """Ids worth suggesting: the top id, plus any other id with KEEP_MIN_VOTES or more.
    Returns {id: {"votes", "share", "pmids", "competing"}}."""
    if not votes:
        return {}
    top = votes.most_common(1)[0][0]
    out = {}
    for i, n in votes.items():
        if i != top and n < KEEP_MIN_VOTES:
            continue
        out[i] = {"votes": n, "share": round(n / resolvable, 3) if resolvable else 0.0,
                  "pmids": sorted(voters[i], reverse=True)[:EVIDENCE_PMIDS],
                  "competing": {k: v for k, v in votes.most_common(6) if k != i}}
    return out


def is_strong(c):
    return c["votes"] >= AUTO_MIN_VOTES and c["share"] >= AUTO_MIN_SHARE


def frozen(v):
    """Rows the nightly never writes: a decision (CONFIRMED/REJECTED) or a curator's row."""
    return v["status"] in ("CONFIRMED", "REJECTED") or v["method"] == "CURATOR"


def holders_of(have):
    """{(type, id): {uids holding it as SUGGESTED or CONFIRMED}}."""
    h = defaultdict(set)
    for (uid, t, i), v in have.items():
        if v["status"] in ("SUGGESTED", "CONFIRMED"):
            h[(t, i)].add(uid)
    return h


def plan_writes(have, fresh, now):
    """Decide tonight's writes for the people in `fresh`.

    have:  {(uid, type, id): {"status", "method", "row_id", "conflict_uid"}}, every row.
    fresh: {(uid, type): {"resolvable": n, "cands": {id: cand}, "lookup": set(ids)}} for the
           people (and types) re-voted tonight. A lookup id with no vote becomes an
           ORCID_LOOKUP row; with a vote it stays PUB_VOTE and is flagged in evidence.
    Returns (upserts, deletes). upserts carry row_id (None = insert); deletes are row ids of
    a re-voted person's SUGGESTED PUB_VOTE rows that no longer get a vote."""
    planned, deletes = {}, []
    by_pair = defaultdict(list)
    for key, v in have.items():
        by_pair[key[:2]].append((key, v))
    for (uid, t), f in fresh.items():
        lookup = set(f.get("lookup") or ())
        for i in set(f["cands"]) | lookup:
            key = (uid, t, i)
            if key in have and frozen(have[key]):
                continue
            c = f["cands"].get(i)
            ev = {"pmids": c["pmids"], "competing": c["competing"]} if c else {"pmids": [], "competing": {}}
            if i in lookup:
                ev["orcid_lookup"] = True
            planned[key] = {"uid": uid, "id_type": t, "id_value": i,
                            "row_id": have[key]["row_id"] if key in have else None,
                            "method": "PUB_VOTE" if c else "ORCID_LOOKUP",
                            "votes": c["votes"] if c else 0, "resolvable": f["resolvable"],
                            "share": c["share"] if c else None, "evidence": ev,
                            "strong": bool(c) and is_strong(c)}
        for key, v in by_pair[(uid, t)]:
            if key not in planned and not frozen(v) and v["method"] == "PUB_VOTE" and v["row_id"] is not None:
                deletes.append(v["row_id"])
    gone = set(deletes)
    holders = holders_of({k: v for k, v in have.items() if v["row_id"] not in gone})
    for (uid, t, i) in planned:
        holders[(t, i)].add(uid)
    upserts = []
    for key, r in planned.items():
        others = sorted(holders[key[1:]] - {key[0]})
        r["conflict_uid"] = others[0] if others else None
        rejected = key in have and have[key]["status"] == "REJECTED"   # frozen anyway; kept explicit
        auto = r.pop("strong") and r["conflict_uid"] is None and not rejected
        r["status"] = "CONFIRMED" if auto else "SUGGESTED"
        if auto:
            r["method"] = "AUTO"
        upserts.append(r)
    return upserts, deletes


def conflict_refresh(have):
    """conflict_uid for every row the nightly maintains it on (SUGGESTED non-curator rows and
    AUTO-confirmed rows), returning only the ones whose stored value differs. Run once after
    all of tonight's writes so people voted earlier in the run see people voted later."""
    holders = holders_of(have)
    out = {}
    for key, v in have.items():
        if (v["status"] == "SUGGESTED" and v["method"] != "CURATOR") or \
                (v["status"] == "CONFIRMED" and v["method"] == "AUTO"):
            others = sorted(holders[key[1:]] - {key[0]})
            c = others[0] if others else None
            if c != v.get("conflict_uid"):
                out[key] = c
    return out


def due_people(pubs_by_uid, state, weekday, everyone=False):
    """Which (uid, type) pairs to re-vote tonight. state: {(uid, type): pmid_set_hash}."""
    out = []
    for uid, pubs in pubs_by_uid.items():
        h = pmid_set_hash(p for p, _, _ in pubs)
        for t in TYPES:
            prev = state.get((uid, t))
            if everyone or (prev is None and shard_of(uid) == weekday) or (prev is not None and prev != h):
                out.append((uid, t))
    return out


# ---- I/O ---------------------------------------------------------------------
def db():
    import pymysql
    return pymysql.connect(host=os.environ["DB_HOST"], user=os.environ["DB_USERNAME"],
                           password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"],
                           connect_timeout=15, autocommit=False, charset="utf8mb4")


def table_exists(cur, name):
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s", (name,))
    return cur.fetchone()[0] > 0


def load_accepted(cur, uids=None):
    """{uid: [(pmid, rank, last)]} newest MAX_PMIDS accepted papers, external cohorts out.
    The surname comes from the target-author byline row, else ReCiter's matched name."""
    only = ""
    if uids:
        only = "AND LOWER(p.personIdentifier) IN (" + ",".join(["%s"] * len(uids)) + ")"
    cur.execute(f"""SELECT LOWER(p.personIdentifier), p.pmid, MIN(a.`rank`),
                           COALESCE(MIN(a.authorLastName), MIN(p.articleAuthorNameLastName))
                    FROM person_article p
                    LEFT JOIN person_article_author a
                      ON a.personIdentifier = p.personIdentifier AND a.pmid = p.pmid AND a.targetAuthor LIKE '1%%'
                    WHERE p.userAssertion = 'ACCEPTED' AND p.pmid > 0
                      AND p.personIdentifier NOT LIKE '%%\\_%%' {only}
                    GROUP BY LOWER(p.personIdentifier), p.pmid""", sorted(uids) if uids else None)
    out = defaultdict(list)
    for uid, pmid, rank, last in cur.fetchall():
        if last in (None, "", "'NULL'", "NULL"):
            last = None
        out[uid].append((int(pmid), int(rank or 0), last))
    return {u: sorted(v, reverse=True)[:MAX_PMIDS] for u, v in out.items()}


def load_existing(cur, have_tables):
    have, state = {}, {}
    if have_tables["person_author_id"]:
        cur.execute("SELECT id, uid, id_type, id_value, status, method, conflict_uid FROM person_author_id")
        for rid, uid, t, i, st, m, cu in cur.fetchall():
            have[(uid.lower(), t, i)] = {"row_id": rid, "status": st, "method": m, "conflict_uid": cu}
    if have_tables["person_author_id_vote"]:
        cur.execute("SELECT uid, id_type, pmid_set_hash FROM person_author_id_vote")
        state = {(u.lower(), t): h for u, t, h in cur.fetchall()}
    return have, state


def trusted_orcids(cur, uids):
    """{uid: iD} for T1 (Scholars-confirmed, iD from DynamoDB Identity) and T2 (admin_orcid
    for people not in scholar_active)."""
    out, scholars, t1 = {}, set(), []
    if table_exists(cur, "scholar_active"):
        cur.execute("SELECT LOWER(cwid), has_confirmed_orcid FROM scholar_active")
        for u, c in cur.fetchall():
            scholars.add(u)
            if c and u in uids:
                t1.append(u)
    if table_exists(cur, "admin_orcid"):
        cur.execute("SELECT LOWER(personIdentifier), orcid FROM admin_orcid")
        for u, o in cur.fetchall():
            o = (o or "").strip().rsplit("/", 1)[-1].upper()
            if u in uids and u not in scholars and ORCID_RE.match(o):
                out[u] = o
    if t1:
        import boto3
        ddb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1"))
        for item in batch_get(ddb, "Identity", "uid", t1, "#u, #i.orcid", {"#u": "uid", "#i": "identity"}):
            o = str(((item.get("identity") or {}).get("orcid")) or "").strip().rsplit("/", 1)[-1].upper()
            if ORCID_RE.match(o):
                out[item["uid"].lower()] = o
    return out


def batch_get(ddb, table, key, values, projection, names):
    """BatchGetItem in 100-key pages, retrying UnprocessedKeys with backoff."""
    vals = list(dict.fromkeys(values))
    for i in range(0, len(vals), 100):
        req = {table: {"Keys": [{key: v} for v in vals[i:i + 100]], "ProjectionExpression": projection,
                       "ExpressionAttributeNames": names}}
        tries = 0
        while req:
            r = ddb.batch_get_item(RequestItems=req)
            yield from r["Responses"].get(table, [])
            req = r.get("UnprocessedKeys") or None
            if req:
                tries += 1
                time.sleep(min(0.1 * 2 ** tries, 5))


def http_json(url, params=None, headers=None, tries=5):
    import requests
    for attempt in range(tries):
        try:
            r = requests.get(url, params=params, headers=headers or {"Accept": "application/json"}, timeout=60)
        except (requests.ConnectionError, requests.Timeout):
            if attempt == tries - 1:
                raise
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 404:
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 * (attempt + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"GET {url} failed after retries")


def openalex_params(p):
    p = dict(p)
    mailto = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mailto:
        p["mailto"] = mailto
    key = (os.environ.get("OPENALEX_API_KEY") or "").strip()
    if key:
        p["api_key"] = key
    return p


def openalex_works_by_pmid(pmids):
    j = http_json(f"{OPENALEX}/works", openalex_params(
        {"filter": "pmid:" + "|".join(str(p) for p in pmids), "per_page": 50, "select": "ids,authorships"}))
    time.sleep(OPENALEX_SLEEP)
    out = {}
    for w in (j or {}).get("results") or []:
        pm = str((w.get("ids") or {}).get("pmid") or "").rstrip("/").rsplit("/", 1)[-1]
        if pm.isdigit():
            out[int(pm)] = w
    return out


def openalex_author_by_orcid(orcid):
    j = http_json(f"{OPENALEX}/authors/orcid:{orcid}", openalex_params({"select": "id"}))
    time.sleep(OPENALEX_SLEEP)
    return ((j or {}).get("id") or "").rsplit("/", 1)[-1] or None


def scopus_author_by_orcid(orcid):
    j = http_json(SCOPUS_AUTHOR_SEARCH, {"query": f"ORCID({orcid})", "count": 5},
                  headers={"X-ELS-APIKey": os.environ["SCOPUS_API_KEY"],
                           "X-ELS-Insttoken": os.environ["SCOPUS_INST_TOKEN"],
                           "Accept": "application/json"})
    time.sleep(SCOPUS_SLEEP)
    ids = [str(e.get("dc:identifier") or "").split(":")[-1]
           for e in ((j or {}).get("search-results") or {}).get("entry") or [] if not e.get("error")]
    ids = [i for i in ids if i.isdigit()]
    return ids[0] if len(ids) == 1 else None   # several profiles on one iD: let the vote decide


# ---- SQL writes --------------------------------------------------------------
# Every UPDATE re-checks the guard in SQL, so a curator decision saved in PM while the run is
# in flight is never overwritten by a plan made from the run's earlier snapshot.
GUARD = "status = 'SUGGESTED' AND method <> 'CURATOR'"


def write(cur, upserts, deletes, votes_state, now):
    if deletes:
        cur.executemany(f"DELETE FROM person_author_id WHERE id = %s AND {GUARD} AND method = 'PUB_VOTE'",
                        [(d,) for d in deletes])
    ins, upd = [], []
    for r in upserts:
        auto = r["status"] == "CONFIRMED"
        vals = (r["status"], r["method"], r["votes"], r["resolvable"], r["share"],
                json.dumps(r["evidence"]), r["conflict_uid"], "nightly" if auto else None, now if auto else None)
        if r["row_id"] is None:
            ins.append((r["uid"], r["id_type"], r["id_value"]) + vals + (now, now))
        else:
            upd.append(vals + (now, r["row_id"]))
    if ins:
        # IGNORE: a row PM inserted for the same (uid, type, id) since the snapshot wins.
        cur.executemany("""INSERT IGNORE INTO person_author_id
            (uid, id_type, id_value, status, method, votes, resolvable, share, evidence_json, conflict_uid,
             decided_by, decided_at, first_seen, last_seen)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", ins)
    if upd:
        cur.executemany(f"""UPDATE person_author_id SET status=%s, method=%s, votes=%s, resolvable=%s, share=%s,
            evidence_json=%s, conflict_uid=%s, decided_by=%s, decided_at=%s, last_seen=%s
            WHERE id = %s AND {GUARD}""", upd)
    if votes_state:
        cur.executemany("""INSERT INTO person_author_id_vote (uid, id_type, pmid_set_hash, n_pmids, resolvable, top_id, voted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE pmid_set_hash=VALUES(pmid_set_hash), n_pmids=VALUES(n_pmids),
              resolvable=VALUES(resolvable), top_id=VALUES(top_id), voted_at=VALUES(voted_at)""", votes_state)


def write_conflicts(cur, conflicts):
    if conflicts:
        cur.executemany(f"""UPDATE person_author_id SET conflict_uid = %s
            WHERE uid = %s AND id_type = %s AND id_value = %s
              AND (({GUARD}) OR (status = 'CONFIRMED' AND method = 'AUTO'))""",
                        [(c, u, t, i) for (u, t, i), c in conflicts.items()])


# ---- main --------------------------------------------------------------------
def vote_one(uid, t, ps, scopus_items, orcid, ctx, stats):
    """Vote one person for one id type. Returns fresh entry, or None when the source failed."""
    if t == "SCOPUS_AUID":
        stats["scopus_missing_pmids"] += sum(1 for p, _, _ in ps if p not in scopus_items)

        def slot(p, r, l):
            return scopus_slot(scopus_items.get(p), r, l)
    else:
        try:
            works = openalex_works_by_pmid([p for p, _, _ in ps])
            stats["openalex_calls"] += 1
        except Exception as e:
            stats["openalex_error"] += 1
            log(f"authorIdSuggestions: OpenAlex vote failed for {uid}: {e}")
            return None

        def slot(p, r, l):
            return openalex_slot(works.get(p), r, l)
    votes, voters, res = tally(ps, slot)
    lookup = set()
    if orcid:
        try:
            i = None
            if t == "OPENALEX_AUTHOR":
                i = openalex_author_by_orcid(orcid)
                stats["orcid_openalex_calls"] += 1
            elif ctx["scopus_keys"] and len(ps) < ORCID_SCOPUS_MAX_ACCEPTED \
                    and stats["orcid_scopus_calls"] < ctx["scopus_budget"]:
                i = scopus_author_by_orcid(orcid)
                stats["orcid_scopus_calls"] += 1
            if i:
                lookup.add(i)
        except Exception as e:
            stats["orcid_lookup_error"] += 1
            log(f"authorIdSuggestions: ORCID lookup failed for {uid}/{t}: {e}")
    return {"resolvable": res, "cands": candidates(votes, voters, res), "lookup": lookup, "votes": votes}


def band_of(f):
    votes = f["votes"]
    if not votes:
        return "none"
    top, n = votes.most_common(1)[0]
    return "strong" if is_strong(f["cands"][top]) else "moderate" if n >= 3 else "weak"


def run(a):
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0, tzinfo=None)
    mode = "apply" if a.apply else "dry-run"
    cx = db()
    cur = cx.cursor()
    tables = {t: table_exists(cur, t) for t in ("person_author_id", "person_author_id_vote")}
    if a.apply and not all(tables.values()):
        log("authorIdSuggestions: REFUSING --apply: person_author_id / person_author_id_vote missing; "
            "apply setup/table_person_author_id_import_harvest_v3.8.sql first")
        return 2
    uids = {u.strip().lower() for u in a.uids.split(",") if u.strip()} if a.uids else None
    pubs = load_accepted(cur, uids)
    have, state = load_existing(cur, tables)
    types = [t for t, off in (("SCOPUS_AUID", a.no_scopus), ("OPENALEX_AUTHOR", a.no_openalex)) if not off]
    weekday = now.weekday()
    due = [d for d in due_people(pubs, state, weekday, everyone=bool(a.all or uids)) if d[1] in types]
    want = defaultdict(set)
    for u, t in due:
        want[u].add(t)
    due_uids = sorted(want)[:a.limit] if a.limit else sorted(want)
    log(f"authorIdSuggestions: mode={mode} people_with_accepted={len(pubs)} existing_rows={len(have)} "
        f"voted_before={len(state)} weekday={weekday} due_people={len(due_uids)} "
        f"due_pairs={sum(len(want[u]) for u in due_uids)}")

    orcids = {}
    try:
        orcids = trusted_orcids(cur, set(due_uids))
    except Exception as e:
        log(f"authorIdSuggestions: trusted ORCID read failed, lookups skipped: {e}")
    ctx = {"scopus_keys": bool(os.getenv("SCOPUS_API_KEY") and os.getenv("SCOPUS_INST_TOKEN")),
           "scopus_budget": int(os.getenv("SCOPUS_AUTHOR_SEARCH_MAX_CALLS", "200"))}
    ddb = None
    if "SCOPUS_AUID" in types and due_uids:
        import boto3
        ddb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "us-east-1"))

    stats, bands, sample = Counter(), {t: Counter() for t in TYPES}, []
    stats["trusted_orcids"] = len(orcids)
    for start in range(0, len(due_uids), COMMIT_EVERY):
        chunk = due_uids[start:start + COMMIT_EVERY]
        scopus_items, scopus_failed = {}, False
        keys = [str(p) for u in chunk if "SCOPUS_AUID" in want[u] for p, _, _ in pubs[u]]
        if ddb is not None and keys:
            try:
                for it in batch_get(ddb, "ScopusArticle", "id", keys, "#k, #s.authors", {"#k": "id", "#s": "scopusarticle"}):
                    scopus_items[int(it["id"])] = it
            except Exception as e:
                scopus_failed = True
                stats["scopus_ddb_error"] += 1
                log(f"authorIdSuggestions: ScopusArticle read failed ({e}); Scopus votes skipped for this chunk")
        fresh, votes_state = {}, []
        for u in chunk:
            ps = pubs[u]
            for t in sorted(want[u]):
                if t == "SCOPUS_AUID" and scopus_failed:
                    continue
                f = vote_one(u, t, ps, scopus_items, orcids.get(u), ctx, stats)
                if f is None:
                    continue
                fresh[(u, t)] = f
                bands[t][band_of(f)] += 1
                if sum(1 for n in f["votes"].values() if n >= 2) >= 2:
                    bands[t]["split"] += 1
                top = f["votes"].most_common(1)[0][0] if f["votes"] else None
                votes_state.append((u, t, pmid_set_hash(p for p, _, _ in ps), len(ps), f["resolvable"], top, now))
        upserts, deletes = plan_writes(have, fresh, now)
        stats["insert"] += sum(1 for r in upserts if r["row_id"] is None)
        stats["update"] += sum(1 for r in upserts if r["row_id"] is not None)
        stats["auto_confirm"] += sum(1 for r in upserts if r["status"] == "CONFIRMED")
        stats["conflicted"] += sum(1 for r in upserts if r["conflict_uid"])
        stats["delete"] += len(deletes)
        sample.extend(upserts[:max(0, 5 - len(sample))])
        if a.apply:
            write(cur, upserts, deletes, votes_state, now)
            cx.commit()
        # carry tonight's plan into the snapshot (dry-run too), so later chunks and the final
        # conflict pass see it
        gone = set(deletes)
        have = {k: v for k, v in have.items() if v["row_id"] not in gone}
        for r in upserts:
            k = (r["uid"], r["id_type"], r["id_value"])
            have[k] = {"row_id": have.get(k, {}).get("row_id"), "status": r["status"],
                       "method": r["method"], "conflict_uid": r["conflict_uid"]}
        log(f"authorIdSuggestions: {start + len(chunk)}/{len(due_uids)} people")
    conflicts = conflict_refresh(have)
    stats["conflict_refresh"] = len(conflicts)
    stats["auto_now_conflicted"] = sum(1 for k, c in conflicts.items() if c and have[k]["method"] == "AUTO")
    if a.apply:
        write_conflicts(cur, conflicts)
        cx.commit()
    for t in TYPES:
        log(f"authorIdSuggestions: {t} bands {dict(bands[t])}")
    for r in sample:
        log(f"  sample: {r['uid']} {r['id_type']} {r['id_value']} {r['status']}/{r['method']} "
            f"votes={r['votes']}/{r['resolvable']} conflict={r['conflict_uid']}")
    log(f"authorIdSuggestions: done mode={mode} {dict(sorted(stats.items()))}")
    cx.close()
    return 0


def selftest():
    from decimal import Decimal
    D = Decimal  # DynamoDB numbers arrive as Decimal
    sc = lambda *aus: {"scopusarticle": {"authors": [
        {"seq": D(i + 1), "surname": s, "authid": D(a)} for i, (s, a) in enumerate(aus)]}}
    # Scopus slot: rank + surname, else unique surname, else None
    item = sc(("Smith", 111), ("Chiang", 36472973800), ("Lee", 222))
    assert scopus_slot(item, 2, "Chiang") == "36472973800"
    assert scopus_slot(item, 3, "Chiang") == "36472973800"            # wrong rank, unique surname
    assert scopus_slot(sc(("Chiang", 1), ("Chiang", 2)), 2, "Chiang") == "2"  # rank breaks the tie
    assert scopus_slot(sc(("Chiang", 1), ("Chiang", 2)), 0, "Chiang") is None  # ambiguous
    assert scopus_slot(None, 1, "Chiang") is None
    assert scopus_slot(item, 2, None) is None
    assert scopus_slot(sc(("Chiang", 56663200300)), 1, "Chiang") == "56663200300"
    assert scopus_slot({"scopusarticle": {"authors": [{"seq": D(1), "surname": "Chiang"}]}}, 1, "Chiang") is None  # no authid
    # OpenAlex slot: containment on display_name
    w = {"authorships": [{"author": {"id": "https://openalex.org/A1", "display_name": "Gloria C. Chiang"}},
                         {"author": {"id": "https://openalex.org/A2", "display_name": "M Lee"}}]}
    assert openalex_slot(w, 1, "Chiang") == "A1"
    assert openalex_slot(w, 2, "Chiang") == "A1"
    assert openalex_slot(w, 2, "Müller") is None

    # The Chiang split profile: a strong main id plus a second id on a misspelt byline.
    pubs = [(100 + k, 2, "Chiang") for k in range(10)]
    ids = {100 + k: "36472973800" for k in range(8)}
    ids.update({108: "56663200300", 109: "56663200300"})
    votes, voters, res = tally(pubs, lambda p, r, l: ids.get(p))
    c = candidates(votes, voters, res)
    assert set(c) == {"36472973800", "56663200300"}, c
    assert c["36472973800"]["votes"] == 8 and c["36472973800"]["share"] == 0.8 and is_strong(c["36472973800"])
    assert not is_strong(c["56663200300"])
    assert c["36472973800"]["competing"] == {"56663200300": 2}
    # a one-vote stray is dropped unless it is the top id
    votes, voters, res = tally([(1, 1, "X"), (2, 1, "X"), (3, 1, "X")], lambda p, r, l: {1: "A", 2: "A", 3: "B"}[p])
    assert set(candidates(votes, voters, res)) == {"A"}
    votes, voters, res = tally([(1, 1, "X")], lambda p, r, l: "A")
    assert set(candidates(votes, voters, res)) == {"A"}
    assert candidates(Counter(), {}, 0) == {}

    now = datetime.datetime(2026, 11, 11)
    strong = {"votes": 8, "share": 0.8, "pmids": [1], "competing": {}}
    weak = {"votes": 2, "share": 0.2, "pmids": [2], "competing": {}}
    # 1. strong, unheld -> AUTO CONFIRMED; weak -> SUGGESTED
    up, de = plan_writes({}, {("gcc9004", "SCOPUS_AUID"): {"resolvable": 10, "cands": {"S1": strong, "S2": weak}, "lookup": set()}}, now)
    st = {r["id_value"]: (r["status"], r["method"]) for r in up}
    assert st == {"S1": ("CONFIRMED", "AUTO"), "S2": ("SUGGESTED", "PUB_VOTE")}, st
    # 2. another uid already holds S1 -> conflict, no auto-confirm
    have = {("dup1", "SCOPUS_AUID", "S1"): {"row_id": 7, "status": "CONFIRMED", "method": "CURATOR"}}
    up, de = plan_writes(have, {("gcc9004", "SCOPUS_AUID"): {"resolvable": 10, "cands": {"S1": strong}, "lookup": set()}}, now)
    assert [(r["status"], r["conflict_uid"]) for r in up] == [("SUGGESTED", "dup1")]
    # 3. two people strong on the same id tonight -> both conflicted, neither confirmed
    up, _ = plan_writes({}, {("a", "SCOPUS_AUID"): {"resolvable": 10, "cands": {"S1": strong}, "lookup": set()},
                                ("b", "SCOPUS_AUID"): {"resolvable": 10, "cands": {"S1": strong}, "lookup": set()}}, now)
    assert sorted((r["uid"], r["status"], r["conflict_uid"]) for r in up) == [("a", "SUGGESTED", "b"), ("b", "SUGGESTED", "a")]
    # 4. REJECTED / CONFIRMED / CURATOR rows are never written; stale PUB_VOTE SUGGESTED deleted
    have = {("u", "OPENALEX_AUTHOR", "A1"): {"row_id": 1, "status": "REJECTED", "method": "PUB_VOTE"},
            ("u", "OPENALEX_AUTHOR", "A2"): {"row_id": 2, "status": "SUGGESTED", "method": "CURATOR"},
            ("u", "OPENALEX_AUTHOR", "A3"): {"row_id": 3, "status": "SUGGESTED", "method": "PUB_VOTE"},
            ("u", "OPENALEX_AUTHOR", "A4"): {"row_id": 4, "status": "CONFIRMED", "method": "AUTO"},
            ("u", "SCOPUS_AUID", "S9"): {"row_id": 5, "status": "SUGGESTED", "method": "PUB_VOTE"}}
    up, de = plan_writes(have, {("u", "OPENALEX_AUTHOR"): {"resolvable": 9, "cands": {"A1": strong, "A2": weak, "A4": strong}, "lookup": set()}}, now)
    assert up == [] and de == [3], (up, de)
    assert ("u", "SCOPUS_AUID", "S9") in have         # untouched type keeps its row
    # conflict pass: SUGGESTED and AUTO rows get the other holder; curator rows are left alone
    have = {("a", "SCOPUS_AUID", "S1"): {"row_id": 1, "status": "CONFIRMED", "method": "AUTO", "conflict_uid": None},
            ("b", "SCOPUS_AUID", "S1"): {"row_id": 2, "status": "SUGGESTED", "method": "PUB_VOTE", "conflict_uid": None},
            ("c", "SCOPUS_AUID", "S1"): {"row_id": 3, "status": "CONFIRMED", "method": "CURATOR", "conflict_uid": None},
            ("d", "SCOPUS_AUID", "S2"): {"row_id": 4, "status": "SUGGESTED", "method": "PUB_VOTE", "conflict_uid": "x"},
            ("e", "SCOPUS_AUID", "S1"): {"row_id": 5, "status": "REJECTED", "method": "CURATOR", "conflict_uid": None}}
    co = conflict_refresh(have)
    assert co == {("a", "SCOPUS_AUID", "S1"): "b", ("b", "SCOPUS_AUID", "S1"): "a",
                  ("d", "SCOPUS_AUID", "S2"): None}, co
    # 5. ORCID lookup: matching a voted id flags it; a new id becomes ORCID_LOOKUP, never auto
    up, _ = plan_writes({}, {("t1", "OPENALEX_AUTHOR"): {"resolvable": 3, "cands": {"A1": weak}, "lookup": {"A1", "A9"}}}, now)
    by = {r["id_value"]: r for r in up}
    assert by["A1"]["method"] == "PUB_VOTE" and by["A1"]["evidence"]["orcid_lookup"] is True
    assert by["A9"]["method"] == "ORCID_LOOKUP" and by["A9"]["status"] == "SUGGESTED" and by["A9"]["votes"] == 0
    # 6. cadence: never-voted only on its shard day; changed set any day; unchanged never
    pubs = {"aaa": [(1, 1, "X")], "bbb": [(2, 1, "Y")]}
    d = shard_of("aaa")
    assert ("aaa", "SCOPUS_AUID") in due_people(pubs, {}, d)
    assert ("aaa", "SCOPUS_AUID") not in due_people(pubs, {}, (d + 1) % 7)
    h = pmid_set_hash([1])
    st = {("aaa", "SCOPUS_AUID"): h, ("aaa", "OPENALEX_AUTHOR"): "stale"}
    got = due_people({"aaa": pubs["aaa"]}, st, (d + 1) % 7)
    assert got == [("aaa", "OPENALEX_AUTHOR")], got
    assert len(due_people(pubs, st, 0, everyone=True)) == 4
    assert pmid_set_hash([2, 1]) == pmid_set_hash([1, 2])
    print("ok")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="default: compute and log, write nothing")
    g.add_argument("--apply", action="store_true")
    g.add_argument("--selftest", action="store_true")
    ap.add_argument("--all", action="store_true", help="ignore the weekday shard and re-vote everyone")
    ap.add_argument("--uids", help="comma-separated uids (implies --all for them)")
    ap.add_argument("--limit", type=int, help="at most N due people")
    ap.add_argument("--no-openalex", action="store_true")
    ap.add_argument("--no-scopus", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
