"""collectAffiliations.py — co-author affiliations, funders and the watch-list screen.

Research security dashboard, steps 1-2 (setup/table_research_security_v3.13.sql). Stores
every author's affiliation text and each article's grant list, then screens them against
the lists in watchlist_patterns.py. Results are screening results, not determinations.

Mode comes from env RESEARCH_SECURITY_MODE = off | dry-run | apply. Unset, or anything
else, is off: a typo in a CronJob env patch must not start writing.

  off      does nothing.
  dry-run  reads reciterdb and DynamoDB, reports what it would collect and what the lists
           match, and writes NOTHING. Makes no efetch or OpenAlex call.
  apply    collects, writes, and rebuilds the two match tables.

Scope: every article with an accepted WCM or Cornell (Ithaca) author
(analysis_summary_author), plus the not-yet-attributed tier (person_article rows with an
empty or PENDING userAssertion, authorship_review rows with status = 'open'). External
validation cohorts (fh_ / fredhutch_ / uc*_ / usc_) are excluded.

article_id is the stable key: the pmid as a string, or the SCOPUS: / OPENALEX: / CROSSREF: id
of an external article. An open review row with no PMID gets the id it will have once
accepted (UPPER(source):external_id); DOI:<doi> is used only when it has neither, and is
folded onto an in-scope article with the same DOI. Never the negative pmid STEP 6b
assigns, which is renumbered every night.

Collect (incremental: an article_id already in article_affiliation_state is skipped):
  1. DynamoDB PubMedArticle, BatchGetItem, for PMIDs. No PubMed traffic.
  2. PubMed efetch for PMIDs DynamoDB does not have, or has with no author list (which
     also covers a record stored without its body), and for PMIDs whose DynamoDB strings
     are guard-sensitive: DynamoDB holds all of an author's affiliations joined into one
     string, and a whole-string guard (Taiwan, a required city) is wrong across two
     institutions. efetch keeps them apart.
  3. OpenAlex /works for the rest: OPENALEX: ids by work id, the others by DOI.
  Every article attempted gets a state row, with or without data. A failed call stamps
  nothing and is retried next run. A miss (source 'none') is tried again after
  NONE_RETRY_DAYS, up to NONE_MAX_ATTEMPTS times in all. Steps 2-3 stop after
  RESEARCH_SECURITY_MAX_HTTP_CALLS batches a run (default 500); the rest waits for the
  next run. A batch that fails to write is retried one article at a time, so one bad
  row costs one article and the match rebuild still runs.

Match: watchlist_match and watchlist_funding_flag are rebuilt in full from the stored
strings on every apply run (into staging tables, then one RENAME for both), so a new list
or a fixed pattern needs no refetch and readers never see an empty table or the two a day
apart. The RENAME waits at most SWAP_LOCK_WAIT seconds for readers; if it cannot get the
lock, yesterday's tables stay and the script exits 1. They cover every stored article,
including one whose authorship was later rejected: readers join to the current authorship
tables.

is_institution_author is approximate: the matched author's byline name ends with the
surname of one of our people on the article (accepted authors and pending candidates) and
its given names, with trailing middle initials dropped, equal that person's first name.
A namesake co-author sets it; an initials-only byline never does.

A paper that puts one "A is with X. B is with Y." blob on every author is reduced to each
author's own clause (aar_universe.own_segment) before it is stored.

  RESEARCH_SECURITY_MODE=dry-run python3 collectAffiliations.py
  RESEARCH_SECURITY_MODE=apply python3 collectAffiliations.py --max-seconds 3000

Env: DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD; AWS credentials and region (DynamoDB);
PUBMED_API_KEY, NCBI_EMAIL (set it: efetch goes through aar_universe, whose fallback
address is a personal one); OPENALEX_MAILTO, OPENALEX_API_KEY (both optional);
RESEARCH_SECURITY_MODE, RESEARCH_SECURITY_MAX_HTTP_CALLS.
"""
import argparse
import concurrent.futures
import functools
import itertools
import os
import re
import sys
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import watchlist_patterns as wp                                   # noqa: E402
from enrichArticles import chunks, norm_doi, openalex_key          # noqa: E402

T_AFF = "article_author_affiliation"
T_FUNDER = "article_funder"
T_STATE = "article_affiliation_state"
T_MATCH = "watchlist_match"
T_FLAG = "watchlist_funding_flag"
TABLES = (T_AFF, T_FUNDER, T_STATE, T_MATCH, T_FLAG)

EXT_COHORTS = r"^(fh|fredhutch|uc[a-z]*|usc)_"   # external validation cohorts share prod reciterdb
DDB_CHUNK = 100               # BatchGetItem hard limit
DDB_WORKERS = 5
DDB_UNPROCESSED_RETRIES = 8
EFETCH_BATCH = 200
OA_BATCH = 50                 # OpenAlex caps an OR-filter at 50 values
OA_SLEEP = 0.2
WRITE_BATCH = 1000
NAME_MAX, AFF_MAX = 255, 15000
ID_MAX = 96                   # article_id VARCHAR(96) in all five tables
NONE_RETRY_DAYS = 30          # a miss is looked up again after this long ...
NONE_MAX_ATTEMPTS = 4         # ... until it has been tried this many times
SWAP_LOCK_WAIT = 60           # seconds the RENAME may wait for readers of the match tables
DB_IO_TIMEOUT = 900           # seconds one statement may block the socket
SCREEN_PROGRESS = 10000       # articles between progress lines (run_all checks its timeout per line)

DEADLINE = None               # time.time() after which no new batch starts (--max-seconds)


def resolve_mode(value):
    mode = (value or "off").strip().lower()
    return mode if mode in ("dry-run", "apply") else "off"


def out_of_time(what):
    if DEADLINE is not None and time.time() >= DEADLINE:
        print(f"  {what}: --max-seconds reached, stopping; the rest is collected next run")
        return True
    return False


# ---- scope (pure) -----------------------------------------------------------------
def _ci(article_id):
    """How the utf8mb4_unicode_ci primary key sees an article_id: case, accents and
    trailing spaces do not distinguish two ids."""
    t = unicodedata.normalize("NFKD", str(article_id))
    return "".join(ch for ch in t if not unicodedata.combining(ch)).casefold().rstrip()


def article_key(pmid, article_id=None, doi=None, source=None, external_id=None):
    """The stable article_id, or None when a row has nothing to build one from. A review
    row (source, external_id) gets the id the Publication Manager accept will give it,
    UPPER(source):external_id, so the paper keeps one key before and after."""
    try:
        p = int(pmid) if pmid is not None else 0
    except (TypeError, ValueError):
        p = 0
    if p > 0:
        return str(p)
    aid = (article_id or "").strip()
    if aid:
        return aid
    src, ext = str(source or "").strip().upper(), str(external_id or "").strip()
    if ext and src and src != "PUBMED":
        return f"{src}:{ext}"
    d = norm_doi(doi)
    return f"DOI:{d}" if d else None


def build_scope(accepted, pending, review):
    """({article_id: doi or None}, {article_id: {cwid, ...}}, skipped) from the three scope
    queries (any iterables of rows; they are folded in as they stream). cwids are
    lower-cased: the tables do not share a collation, so they are compared here.
    skipped counts rows with no usable key ('no_key'), ids too long for the column
    ('too_long') and DOI: keys folded onto an article with the same DOI ('folded')."""
    articles, people, spelled, skipped = {}, {}, {}, Counter()
    for r in itertools.chain(accepted, pending, review):
        aid = article_key(r.get("pmid"), r.get("article_id"), r.get("doi"),
                          r.get("source"), r.get("external_id"))
        if not aid:
            skipped["no_key"] += 1
            continue
        if len(aid) > ID_MAX:
            skipped["too_long"] += 1
            continue
        aid = spelled.setdefault(_ci(aid), aid)      # one spelling per primary-key value
        articles[aid] = articles.get(aid) or norm_doi(r.get("doi"))
        if r.get("cwid"):
            people.setdefault(aid, set()).add(str(r["cwid"]).strip().lower())
    by_doi = {}
    for aid, doi in articles.items():
        if doi and not aid.startswith("DOI:"):
            by_doi.setdefault(doi, aid)
    for aid in [a for a in articles if a.startswith("DOI:")]:
        target = by_doi.get(aid[len("DOI:"):])
        if target:
            if aid in people:
                people.setdefault(target, set()).update(people.pop(aid))
            del articles[aid]
            skipped["folded"] += 1
    return articles, people, skipped


def name_keys(rows):
    """{cwid: {name_key, ...}} from (cwid, first, last) rows. Keys without both a surname
    and a given name are dropped: they could only match by accident."""
    out = {}
    for cwid, first, last in rows:
        k = wp.name_key(first or "", last or "")
        if cwid and k[0] and k[1]:
            out.setdefault(str(cwid).strip().lower(), set()).add(k)
    return out


# ---- source parsers (pure) ----------------------------------------------------------
def _s(v):
    return v.strip() if isinstance(v, str) else ""


def _record(article_id, source, authors, funders, n_authors):
    return {"article_id": str(article_id), "source": source, "n_authors": n_authors,
            "authors": authors,      # [(rank, name, [affiliation, ...])], only authors with one
            "funders": funders}      # [(agency, grant_id, country)]


def empty_record(article_id):
    """Attempted, nothing retrievable: stamped so it is not refetched forever."""
    return _record(article_id, "none", [], [], 0)


_IS_WITH = re.compile(r"\b(?:is|are|was|were) with\b", re.I)


def _unblob(entries):
    """[(rank, name, [affiliation], surname)] -> [(rank, name, [affiliation])]. A string
    that sits on two or more authors and reads "A is with X. B is with Y." is one blob
    naming everybody's affiliation: each author keeps only their own clause
    (aar_universe.own_segment), or the blob unchanged when the surname is not in exactly
    one clause."""
    seen = Counter(a for _, _, affs, _ in entries for a in set(affs))
    shared = {a for a, n in seen.items() if n > 1 and len(_IS_WITH.findall(a)) >= 2}
    if not shared:
        return [(rank, name, affs) for rank, name, affs, _ in entries]
    from aar_universe import own_segment
    return [(rank, name, [own_segment(a, last) if a in shared and last else a for a in affs])
            for rank, name, affs, last in entries]


def parse_dynamo_item(item):
    """PubMedArticle item -> record, or None when it carries no author list (the caller
    then tries efetch). rank is the position in authorlist, so all-null entries leave a gap
    rather than shifting everyone after them."""
    art = (((item.get("pubmedarticle") or {}).get("medlinecitation") or {}).get("article") or {})
    authors, n_authors = [], 0
    for rank, a in enumerate(art.get("authorlist") or [], 1):
        a = {str(k).lower(): v for k, v in (a or {}).items()}
        name = _s(a.get("collectivename")) or " ".join(
            x for x in (_s(a.get("forename")), _s(a.get("lastname"))) if x)
        aff = a.get("affiliation")
        affs = [x for x in (_s(v) for v in (aff if isinstance(aff, (list, tuple)) else [aff])) if x]
        if not name and not affs:
            continue                 # all-null entry
        n_authors += 1
        if affs:
            authors.append((rank, name, affs, _s(a.get("lastname"))))
    if not n_authors:
        return None
    funders = []
    for g in art.get("grantlist") or []:
        g = {str(k).lower(): v for k, v in (g or {}).items()}
        agency, gid, country = _s(g.get("agency")), _s(g.get("grantid")), _s(g.get("country"))
        if agency or gid:
            funders.append((agency, gid, country))
    return _record(int(item["pmid"]), "dynamodb", _unblob(authors), funders, n_authors)


def parse_efetch(xml_bytes):
    """efetch XML -> {pmid: record}. aar_universe.parse_articles is not reused for this: it
    drops collective names and the grant list."""
    out = {}
    for art in ET.fromstring(xml_bytes).iter("PubmedArticle"):
        pmid = art.findtext("./MedlineCitation/PMID")
        if not pmid:
            continue
        authors, n_authors = [], 0
        for rank, au in enumerate(art.iterfind("./MedlineCitation/Article/AuthorList/Author"), 1):
            name = _s(au.findtext("CollectiveName")) or " ".join(
                x for x in (_s(au.findtext("ForeName")), _s(au.findtext("LastName"))) if x)
            affs = [x for x in (_s(e.text) for e in au.iterfind("AffiliationInfo/Affiliation")) if x]
            if not name and not affs:
                continue
            n_authors += 1
            if affs:
                authors.append((rank, name, affs, _s(au.findtext("LastName"))))
        funders = []
        for g in art.iterfind("./MedlineCitation/Article/GrantList/Grant"):
            agency, gid = _s(g.findtext("Agency")), _s(g.findtext("GrantID"))
            if agency or gid:
                funders.append((agency, gid, _s(g.findtext("Country"))))
        out[int(pmid)] = _record(int(pmid), "efetch", _unblob(authors), funders, n_authors)
    return out


def parse_openalex_work(article_id, w):
    """OpenAlex work -> record. Affiliations are the raw strings plus OpenAlex's own
    institution names, as in the original screen."""
    authors, ships = [], w.get("authorships") or []
    for rank, a in enumerate(ships, 1):
        name = _s((a.get("author") or {}).get("display_name"))
        affs = [x for x in (_s(v) for v in list(a.get("raw_affiliation_strings") or [])
                            + [(i or {}).get("display_name") for i in a.get("institutions") or []]) if x]
        if affs:
            authors.append((rank, name, affs, name.rsplit(" ", 1)[-1]))
    funders = []
    for g in w.get("awards") or []:
        agency, gid = _s(g.get("funder_display_name")), _s(g.get("funder_award_id"))
        if agency or gid:
            funders.append((agency, gid, ""))
    return _record(article_id, "openalex", _unblob(authors), funders, len(ships))


# ---- matching (pure) ----------------------------------------------------------------
@functools.lru_cache(maxsize=200_000)
def _match(aff):
    return tuple(wp.match_affiliation(aff))


def is_our_author(name, our_keys):
    """True when a byline name ("Bruce R Schackman", "Jan van der Berg") is one of our
    people, given their (surname, first name) keys. The name must END with the surname
    (so a multi-word surname works) and the given names before it must equal the first
    name, either as written or with trailing single-letter tokens (middle initials)
    dropped. "Yi Ming Wang" is not "Yi Wang", and an initials-only byline never matches."""
    toks = [t for t in (re.sub(r"[^a-z]", "", x) for x in wp.norm(name).lower().split()) if t]
    for last, first in our_keys:
        for i in range(1, len(toks)):
            if "".join(toks[i:]) != last:
                continue
            given = toks[:i]
            if "".join(given) == first:
                return True
            while given and len(given[-1]) == 1:
                given = given[:-1]
            if given and "".join(given) == first:
                return True
    return False


def match_article(article_id, rows, our_keys):
    """watchlist_match rows for one article from (rank, author_name, affiliation) rows: one
    per (list, entity, author), keeping the first affiliation string that matched."""
    out = {}
    for rank, name, aff in rows:
        if not aff:
            continue
        for list_code, category, list_no, entity in _match(aff):
            k = (list_code, entity, rank)
            if k not in out:
                out[k] = {"article_id": article_id, "list_code": list_code, "category": category,
                          "list_no": list_no, "entity": entity, "rank": rank,
                          "author_name": name, "affiliation": aff,
                          "is_institution_author": int(is_our_author(name, our_keys))}
    return list(out.values())


def flag_article(article_id, rows):
    """watchlist_funding_flag rows for one article from (agency, grant_id) rows: one per
    program, keeping the first grant that flagged it."""
    out = {}
    for agency, grant_id in rows:
        for program in wp.match_funder(agency, grant_id):
            out.setdefault(program, {"article_id": article_id, "program": program,
                                     "agency": (agency or "")[:500] or None,
                                     "grant_id": (grant_id or "")[:255] or None})
    return list(out.values())


class Tally:
    def __init__(self):
        self.rows, self.articles, self.inst = Counter(), {}, Counter()

    def add(self, rows):
        for r in rows:
            code = r.get("list_code", wp.FUNDING)
            self.rows[code] += 1
            self.articles.setdefault(code, set()).add(r["article_id"])
            self.inst[code] += r.get("is_institution_author", 0)

    def report(self, label):
        for code in (wp.HOUSE, wp.USUAL, wp.COUNTRY, wp.FUNDING):
            extra = "" if code == wp.FUNDING else f", {self.inst[code]} where our author lists the entity"
            print(f"  {label} {code}: {self.rows[code]} rows on "
                  f"{len(self.articles.get(code, ()))} articles{extra}")


# ---- fetchers (network; replaced in tests) -----------------------------------------
def dynamo_fetcher():
    """fetch(pmids) -> (items, pmids given up on). Unprocessed keys are retried with
    backoff, as abstractImport.py does; a key still unprocessed after that is left for the
    next run rather than treated as absent."""
    import boto3
    client = boto3.resource("dynamodb").meta.client
    names = {"#p": "pmid", "#a": "pubmedarticle", "#m": "medlinecitation", "#r": "article",
             "#al": "authorlist", "#gl": "grantlist"}

    def fetch(pmids):
        keys, items, attempt = [{"pmid": p} for p in pmids], [], 0
        while keys:
            resp = client.batch_get_item(RequestItems={"PubMedArticle": {
                "Keys": keys, "ProjectionExpression": "#p, #a.#m.#r.#al, #a.#m.#r.#gl",
                "ExpressionAttributeNames": names}})
            items.extend(resp["Responses"].get("PubMedArticle", []))
            keys = resp.get("UnprocessedKeys", {}).get("PubMedArticle", {}).get("Keys", [])
            if keys:
                attempt += 1
                if attempt > DDB_UNPROCESSED_RETRIES:
                    break
                time.sleep(min(0.1 * (2 ** attempt), 5.0))
        return items, {int(k["pmid"]) for k in keys}
    return fetch


def efetch_records(pmids):
    """{pmid: record} for one efetch batch, through aar_universe's keyed, retrying client."""
    import aar_universe as u
    r = u._req("POST", "efetch.fcgi", db="pubmed", id=",".join(str(p) for p in pmids), retmode="xml")
    time.sleep(u.SLEEP)
    return parse_efetch(r.content)


def openalex_works(kind, values):
    """{work id or DOI: work} for one batch. A 400 splits the batch until the bad value is
    alone; that value comes back as a miss."""
    from enrichArticles import BadBatch, Stats, openalex_get
    field = "openalex" if kind == "id" else "doi"
    try:
        body = openalex_get({"filter": f"{field}:" + "|".join(values), "per-page": 100,
                             "select": "id,doi,authorships,awards"}, Stats())
    except BadBatch:
        if len(values) == 1:
            return {}
        mid = len(values) // 2
        return {**openalex_works(kind, values[:mid]), **openalex_works(kind, values[mid:])}
    out = {}
    for w in body.get("results", []) or []:
        k = (w.get("id") or "").rsplit("/", 1)[-1] if kind == "id" else norm_doi(w.get("doi"))
        if k and k not in out:      # several works per DOI: OpenAlex returns the best first
            out[k] = w
    time.sleep(OA_SLEEP)
    return out


# ---- DB -----------------------------------------------------------------------------
SQL_ACCEPTED = """
    SELECT a.personIdentifier AS cwid, r.pmid, r.article_id, r.doi
      FROM analysis_summary_author a
      JOIN analysis_summary_article r ON r.pmid = a.pmid
     WHERE a.personIdentifier NOT REGEXP :ext"""
SQL_PENDING = """
    SELECT personIdentifier AS cwid, pmid, doi
      FROM person_article
     WHERE COALESCE(userAssertion, '') IN ('', 'PENDING') AND pmid > 0
       AND personIdentifier NOT REGEXP :ext"""
SQL_REVIEW = """
    SELECT top_cwid AS cwid, pmid, doi, source, external_id
      FROM authorship_review
     WHERE status = 'open' AND (top_cwid IS NULL OR top_cwid NOT REGEXP :ext)"""

INS_AFF = (f"INSERT INTO {T_AFF} (article_id, `rank`, seq, author_name, affiliation, source, fetched_at) "
           "VALUES (:article_id, :rank, :seq, :author_name, :affiliation, :source, :fetched_at)")
INS_FUNDER = (f"INSERT INTO {T_FUNDER} (article_id, seq, agency, grant_id, country, source) "
              "VALUES (:article_id, :seq, :agency, :grant_id, :country, :source)")
INS_STATE = (f"INSERT INTO {T_STATE} (article_id, source, n_authors, n_affiliations, n_funders, fetched_at) "
             "VALUES (:article_id, :source, :n_authors, :n_affiliations, :n_funders, :fetched_at) "
             # only a 'none' row is ever written twice: the retry of a miss
             "ON DUPLICATE KEY UPDATE source = VALUES(source), n_authors = VALUES(n_authors), "
             "n_affiliations = VALUES(n_affiliations), n_funders = VALUES(n_funders), "
             "fetched_at = VALUES(fetched_at), attempts = attempts + 1")
INS_MATCH = (f"INSERT IGNORE INTO {T_MATCH}_new (article_id, list_code, category, list_no, entity, `rank`, "
             "author_name, affiliation, is_institution_author) VALUES (:article_id, :list_code, :category, "
             ":list_no, :entity, :rank, :author_name, :affiliation, :is_institution_author)")
INS_FLAG = (f"INSERT IGNORE INTO {T_FLAG}_new (article_id, program, agency, grant_id) "
            "VALUES (:article_id, :program, :agency, :grant_id)")
SEL_AFF = f"SELECT article_id, `rank`, author_name, affiliation FROM {T_AFF} ORDER BY article_id, `rank`, seq"
SEL_FUNDER = f"SELECT article_id, agency, grant_id FROM {T_FUNDER} ORDER BY article_id, seq"
SEL_STATE = f"SELECT article_id, source, attempts, fetched_at FROM {T_STATE}"
SQL_PERSON = "SELECT personIdentifier AS cwid, firstName, lastName FROM person"
SQL_IDENTITY = "SELECT cwid, givenName, surname FROM identity"
SQL_SWAP = (f"RENAME TABLE {T_MATCH} TO {T_MATCH}_old, {T_MATCH}_new TO {T_MATCH}, "
            f"{T_FLAG} TO {T_FLAG}_old, {T_FLAG}_new TO {T_FLAG}")


def table_exists(conn, text, name):
    return conn.execute(text(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"),
        {"t": name}).first() is not None


def _stream(conn, text, sql, params=None, counts=None, label=None):
    """Rows of one SELECT, one at a time (server-side cursor), so a scope query is folded
    into the scope dicts without ever being held as a list."""
    for r in conn.execution_options(stream_results=True).execute(text(sql), params or {}).mappings():
        if counts is not None:
            counts[label] += 1
        yield r


def collected(state_rows, now):
    """(done, retry) from article_affiliation_state rows, both as _ci() keys. A miss
    (source 'none') older than NONE_RETRY_DAYS with fewer than NONE_MAX_ATTEMPTS tries
    is not done: an article OpenAlex or PubMed indexes later is picked up."""
    done, retry = set(), set()
    cutoff = now - timedelta(days=NONE_RETRY_DAYS)
    for r in state_rows:
        fetched = r.get("fetched_at")
        if (r.get("source") == "none" and int(r.get("attempts") or 1) < NONE_MAX_ATTEMPTS
                and isinstance(fetched, datetime) and fetched <= cutoff):
            retry.add(_ci(r["article_id"]))
        else:
            done.add(_ci(r["article_id"]))
    return done, retry


def rows_for(records, now):
    """(affiliation rows, funder rows, state rows) for the three durable tables."""
    aff, fund, state = [], [], []
    for rec in records:
        n_aff = 0
        for rank, name, affs in rec["authors"]:
            for seq, a in enumerate(affs):
                aff.append({"article_id": rec["article_id"], "rank": rank, "seq": seq,
                            "author_name": name[:NAME_MAX] or None, "affiliation": a[:AFF_MAX],
                            "source": rec["source"], "fetched_at": now})
                n_aff += 1
        for seq, (agency, gid, country) in enumerate(rec["funders"]):
            fund.append({"article_id": rec["article_id"], "seq": seq, "agency": agency[:500] or None,
                         "grant_id": gid[:255] or None, "country": country[:100] or None,
                         "source": rec["source"]})
        state.append({"article_id": rec["article_id"], "source": rec["source"],
                      "n_authors": rec["n_authors"], "n_affiliations": n_aff,
                      "n_funders": len(rec["funders"]), "fetched_at": now})
    return aff, fund, state


def write_records(eng, text, records, now):
    """One transaction per batch: an article's strings and its state row land together."""
    aff, fund, state = rows_for(records, now)
    with eng.begin() as conn:
        for sql, rows in ((INS_AFF, aff), (INS_FUNDER, fund), (INS_STATE, state)):
            for part in chunks(rows, WRITE_BATCH):
                conn.execute(text(sql), part)


def screen_stored(eng, text, keys_for, tally, write):
    """Screen every stored string. With write, fill both <table>_new and swap them in with
    ONE RENAME, so readers see yesterday's pair until today's is complete. Returns False
    when the swap could not be made (yesterday's tables then stay in place)."""
    if write:
        with eng.begin() as conn:
            for t in (T_MATCH, T_FLAG):
                conn.execute(text(f"CREATE TABLE IF NOT EXISTS {t}_new LIKE {t}"))
                conn.execute(text(f"TRUNCATE TABLE {t}_new"))

    def flush(sql, buf):
        if write and buf:
            with eng.begin() as conn:
                conn.execute(text(sql), buf)

    with eng.connect() as conn:
        for what, sel, ins, make in (
                ("affiliations", SEL_AFF, INS_MATCH,
                 lambda aid, g: match_article(aid, ((r[1], r[2], r[3]) for r in g), keys_for(aid))),
                ("funders", SEL_FUNDER, INS_FLAG,
                 lambda aid, g: flag_article(aid, ((r[1], r[2]) for r in g)))):
            buf, n = [], 0
            stream = conn.execution_options(stream_results=True).execute(text(sel))
            for aid, grp in itertools.groupby(stream, key=lambda r: r[0]):
                rows = make(aid, grp)
                tally.add(rows)
                buf.extend(rows)
                n += 1
                if n % SCREEN_PROGRESS == 0:     # a line of output lets run_all check its timeout
                    print(f"  screening {what}: {n} articles", flush=True)
                if len(buf) >= WRITE_BATCH:
                    flush(ins, buf)
                    buf = []
            flush(ins, buf)
            print(f"  screened {what}: {n} articles", flush=True)

    if not write:
        return True
    # RENAME needs an exclusive metadata lock: a reader holding a transaction open on
    # either table would make it wait for lock_wait_timeout, a year by default.
    try:
        with eng.begin() as conn:
            conn.execute(text(f"SET SESSION lock_wait_timeout = {SWAP_LOCK_WAIT}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {T_MATCH}_old, {T_FLAG}_old"))
            conn.execute(text(SQL_SWAP))
    except Exception as e:  # noqa: BLE001 — keep yesterday's tables, say so, exit 1
        print(f"  SWAP FAILED: {T_MATCH} and {T_FLAG} keep yesterday's rows "
              f"(no lock within {SWAP_LOCK_WAIT}s, or another error): {e}", flush=True)
        return False
    try:
        with eng.begin() as conn:
            conn.execute(text(f"SET SESSION lock_wait_timeout = {SWAP_LOCK_WAIT}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {T_MATCH}_old, {T_FLAG}_old"))
    except Exception as e:  # noqa: BLE001 — the swap is done; the next run drops them
        print(f"  could not drop the _old tables (dropped next run): {e}", flush=True)
    return True


# ---- run ----------------------------------------------------------------------------
def run(mode, eng, text, ddb_fetch=None, efetch=efetch_records, openalex=openalex_works,
        max_http_calls=500, now=None):
    """Collect and screen. Returns a stats dict, or None when apply is refused."""
    write = mode == "apply"
    now = now or datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    n_rows = Counter()
    with eng.connect() as conn:
        host = conn.execute(text("SELECT @@hostname")).scalar()
        have = all(table_exists(conn, text, t) for t in TABLES)
        if write and not have:
            print("research-security tables are absent (apply setup/table_research_security_v3.13.sql) — skipped")
            return None
        has_review = table_exists(conn, text, "authorship_review")
        has_identity = table_exists(conn, text, "identity")
        ext = {"ext": EXT_COHORTS}
        # the three scope queries are streamed and folded one after the other
        articles, people, skipped = build_scope(
            _stream(conn, text, SQL_ACCEPTED, ext, n_rows, "accepted"),
            _stream(conn, text, SQL_PENDING, ext, n_rows, "pending"),
            _stream(conn, text, SQL_REVIEW, ext, n_rows, "review") if has_review else ())
        in_scope = {c for cwids in people.values() for c in cwids}
        people_rows = [(r["cwid"], r["firstName"], r["lastName"]) for r in _stream(conn, text, SQL_PERSON)
                       if str(r["cwid"] or "").strip().lower() in in_scope]
        if has_identity:
            people_rows += [(r["cwid"], r["givenName"], r["surname"]) for r in _stream(conn, text, SQL_IDENTITY)
                            if str(r["cwid"] or "").strip().lower() in in_scope]
        done, retry = collected(_stream(conn, text, SEL_STATE), now) if have else (set(), set())

    names = name_keys(people_rows)

    def keys_for(aid):
        return {k for c in people.get(aid, ()) for k in names.get(c, ())}

    todo = sorted(a for a in articles if _ci(a) not in done)
    n_retry = sum(1 for a in todo if _ci(a) in retry)
    pmids = sorted((int(a) for a in todo if a.isdigit()), reverse=True)   # newest first
    externals = [a for a in todo if not a.isdigit()]
    print(f"collectAffiliations ({mode}) on {host}: {len(articles)} articles in scope "
          f"({n_rows['accepted']} accepted authorships, {n_rows['pending']} pending suggestions, "
          f"{n_rows['review']} open review rows); {len(done)} already collected; to collect: "
          f"{len(pmids)} PMIDs, {len(externals)} external"
          f"{f' ({n_retry} are earlier misses tried again)' if n_retry else ''}"
          f"{'' if have else '; tables absent, so everything counts as not collected'}", flush=True)
    if skipped["no_key"] or skipped["too_long"] or skipped["folded"]:
        print(f"  scope rows left out: {skipped['no_key']} with no PMID, external id or DOI; "
              f"{skipped['too_long']} with an id over {ID_MAX} characters; "
              f"{skipped['folded']} DOI keys folded onto an article with the same DOI")

    by_source, strings, tally = Counter(), Counter(), Tally()
    write_failed = []

    def written(records):
        """The records that reached the database. A batch that fails is retried one
        article at a time: one bad row must not stop the run or the match rebuild."""
        if not records:
            return records
        try:
            write_records(eng, text, records, now)
            return records
        except Exception as e:  # noqa: BLE001
            print(f"  write of {len(records)} articles failed, retrying one at a time: {e}")
        ok = []
        for rec in records:
            try:
                write_records(eng, text, [rec], now)
                ok.append(rec)
            except Exception as e:  # noqa: BLE001 — not stamped, so it is tried again next run
                write_failed.append(rec["article_id"])
                if len(write_failed) <= 5:
                    print(f"  could not write article {rec['article_id']}: {e}")
        return ok

    def handle(records):
        if write:
            records = written(records)
        for rec in records:
            by_source[rec["source"]] += 1
            strings["affiliations"] += sum(len(a[2]) for a in rec["authors"])
            strings["funders"] += len(rec["funders"])
            if not write:
                tally.add(match_article(rec["article_id"], ((rank, name, a) for rank, name, affs in rec["authors"]
                                                            for a in affs), keys_for(rec["article_id"])))
                tally.add(flag_article(rec["article_id"], ((ag, gid) for ag, gid, _ in rec["funders"])))

    # 1. DynamoDB
    fallback, held, ddb_failed, ddb_left = [], {}, 0, 0
    if pmids:
        ddb_fetch = ddb_fetch or dynamo_fetcher()

        def safe(chunk):
            try:
                return ddb_fetch(chunk)
            except Exception as e:  # noqa: BLE001 — failed chunk: nothing stamped, retried next run
                print(f"  dynamodb chunk failed, retried next run: {e}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=DDB_WORKERS) as pool:
            for group in chunks(list(chunks(pmids, DDB_CHUNK)), DDB_WORKERS):
                if out_of_time("dynamodb"):
                    break
                records = []
                for chunk, res in zip(group, list(pool.map(safe, group))):
                    if res is None:
                        ddb_failed += len(chunk)
                        continue
                    items, unprocessed = res
                    ddb_left += len(unprocessed)
                    seen = set()
                    for item in items:
                        pmid = int(item["pmid"])
                        seen.add(pmid)
                        rec = parse_dynamo_item(item)
                        if rec is None:
                            fallback.append(pmid)
                        elif any(wp.guard_sensitive(a) for _, _, affs in rec["authors"] for a in affs):
                            # joined affiliations + a whole-string guard: efetch keeps them apart.
                            # The DynamoDB record is kept in case efetch does not return the PMID.
                            held[pmid] = rec
                            fallback.append(pmid)
                            if not write:
                                records.append(rec)       # a dry run screens what it has
                        else:
                            records.append(rec)
                    fallback.extend(p for p in chunk if p not in seen and p not in unprocessed)
                handle(records)
    print(f"  dynamodb: {by_source['dynamodb']} articles with an author list; {len(fallback) - len(held)} absent "
          f"or without one and {len(held)} guard-sensitive (efetch); {ddb_failed + ddb_left} not read "
          f"(failed or unprocessed, retried next run)", flush=True)

    # 2-3. efetch, then OpenAlex — never in a dry run
    by_kind, no_key = {"id": {}, "doi": {}}, 0
    for aid in externals:
        key = openalex_key(aid, articles[aid])
        if key:
            by_kind[key[0]].setdefault(key[1], []).append(aid)
        else:
            no_key += 1
    n_oa = sum(len(v) for v in by_kind.values())
    if no_key:
        print(f"  {no_key} external articles have no DOI or OpenAlex id: not retrievable, not stamped")
    calls = http_failed = cut = 0
    if not write:
        print(f"  dry run: would fetch {len(fallback)} PMIDs by efetch and {n_oa} external "
              f"articles from OpenAlex; no call made")
    else:
        for batch in chunks(fallback, EFETCH_BATCH):
            if calls >= max_http_calls or out_of_time("efetch"):
                cut += len(batch)
                continue
            calls += 1
            try:
                found = efetch(batch)
            except Exception as e:  # noqa: BLE001 — failed batch: nothing stamped, retried next run
                http_failed += 1
                print(f"  efetch batch failed, retried next run: {e}")
                continue
            handle([found.get(p) or held.get(p) or empty_record(p) for p in batch])
        for kind, by_value in by_kind.items():
            for values in chunks(sorted(by_value), OA_BATCH):
                if calls >= max_http_calls or out_of_time("openalex"):
                    cut += sum(len(by_value[v]) for v in values)
                    continue
                calls += 1
                try:
                    found = openalex(kind, values)
                except Exception as e:  # noqa: BLE001
                    http_failed += 1
                    print(f"  openalex batch failed, retried next run: {e}")
                    continue
                handle([parse_openalex_work(aid, found[v]) if v in found else empty_record(aid)
                        for v in values for aid in by_value[v]])
        if cut:
            print(f"  TRUNCATED: {cut} articles left for the next run "
                  f"(RESEARCH_SECURITY_MAX_HTTP_CALLS={max_http_calls}, or --max-seconds)")
        if write_failed:
            print(f"  WRITE FAILED for {len(write_failed)} articles (not stamped, tried again next run); "
                  f"first: {', '.join(write_failed[:5])}")
    print(f"  collected by source: dynamodb {by_source['dynamodb']}, efetch {by_source['efetch']}, "
          f"openalex {by_source['openalex']}, not retrievable {by_source['none']}; "
          f"{strings['affiliations']} affiliation strings, {strings['funders']} grants; "
          f"{calls} HTTP batches, {http_failed} failed{'' if write else ' (nothing written)'}", flush=True)

    # screen — always reached: nothing above raises for one bad batch
    swapped = True
    if write:
        swapped = screen_stored(eng, text, keys_for, tally, True)
        tally.report("rebuilt" if swapped else "NOT swapped in (yesterday's tables kept); would be")
    else:
        if have:
            screen_stored(eng, text, keys_for, tally, False)
        tally.report("would match (stored + read tonight)")
    return {"mode": mode, "by_source": dict(by_source), "todo": len(todo), "http_calls": calls,
            "truncated": cut, "matches": dict(tally.rows), "write_failed": len(write_failed),
            "retried_misses": n_retry, "swapped": swapped}


def make_engine():
    """Its own engine, not aar_db.engine(): that one has no read or write timeout, so a
    statement stuck on the server would hang the nightly pod with no line of output for
    run_all's timeout check to act on."""
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    return create_engine(
        URL.create("mysql+pymysql", username=os.environ["DB_USERNAME"], password=os.environ["DB_PASSWORD"],
                   host=os.environ["DB_HOST"], database=os.environ["DB_NAME"]),
        connect_args={"connect_timeout": 15, "read_timeout": DB_IO_TIMEOUT, "write_timeout": DB_IO_TIMEOUT},
        pool_pre_ping=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--max-seconds", type=int, default=0,
                    help="stop starting collection batches after this many seconds (0 = no limit)")
    args = ap.parse_args(argv)
    mode = resolve_mode(os.environ.get("RESEARCH_SECURITY_MODE"))
    if mode == "off":
        print(f"collectAffiliations: RESEARCH_SECURITY_MODE={os.environ.get('RESEARCH_SECURITY_MODE') or 'off'}"
              " — off, nothing done")
        return 0
    global DEADLINE
    if args.max_seconds > 0:
        DEADLINE = time.time() + args.max_seconds
    from sqlalchemy import text
    stats = run(mode, make_engine(), text,
                max_http_calls=int(os.environ.get("RESEARCH_SECURITY_MAX_HTTP_CALLS") or "500"))
    return 0 if stats is None or stats["swapped"] else 1


if __name__ == "__main__":
    sys.exit(main())
