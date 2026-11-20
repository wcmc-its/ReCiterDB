#!/usr/bin/env python3
"""
Cornell Ithaca web-page seeds, step 4: citations -> DOIs -> author-confirmed seeds.

Reads person pages kept by webPageSeeds.py, splits their publication lists into
citation blocks, parses each NEW block once (cached by block hash, shared across pages),
resolves it to a DOI, and keeps a DOI as a seed for a NetID only when that person's
name is on the DOI's author list.

Trust boundaries, deliberately:
  * The model only splits/reads citation text into fields. It never decides authorship
    and its DOIs are discarded unless the DOI appears verbatim in the block.
  * DOI resolution is deterministic: an inline DOI, else Crossref with a near-exact title
    match (and year within 1 when both are known). A bad parse can only fail to resolve.
  * Seeds need the person's name on the resolved work's author list (lab pages and CVs
    list students' papers).

Model: Claude on Amazon Bedrock via the CronJob's IRSA role (no stored key), the same
pattern as infer_venue_subfield.py. Default Haiku 5.5, the model that role is granted.
"""

import collections
import datetime
import difflib
import hashlib
import html as htmllib
import json
import logging
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request

log = logging.getLogger("webPageCitations")

MODEL = os.environ.get("WEB_SEEDS_MODEL", "us.anthropic.claude-haiku-5-5")
PARSE_BATCH = 25
MAX_TOKENS = 8000
MAX_BLOCKS_PER_PAGE = 400
TITLE_MATCH = 0.90
UA = "Mozilla/5.0 (compatible; Cornell-WCM-Scholars-research-indexer)"
EXTRACTOR_VERSION = "blocks-v1+" + MODEL

BLOCK_TAGS = re.compile(r"(?i)<\s*/?\s*(li|p|br|div|tr|td|h[1-6]|dd|dt|article|section|blockquote)\b[^>]*>")
DROP = re.compile(r"(?is)<(script|style|noscript|svg|nav|header|footer|form)\b.*?</\1>")
YEAR = re.compile(r"\b(19[5-9]\d|20[0-3]\d)\b")
DOI_INLINE = re.compile(r"\b(10\.\d{4,9}/[^\s\"<>]{1,250})", re.I)


# ---------------------------------------------------------------------------
#                        pure functions (unit-tested)
# ---------------------------------------------------------------------------

def citation_blocks(html):
    """Candidate citation strings from a page, in page order: block-level elements split,
    tags stripped, entities decoded. Kept if 40-2000 chars, mention a year, and look like
    prose (>= 6 words). ponytail: heuristic pre-split; the model drops non-publications."""
    html = DROP.sub(" ", html)
    text = BLOCK_TAGS.sub("\n¶\n", html)
    text = htmllib.unescape(re.sub(r"<[^>]+>", " ", text))
    out, seen = [], set()
    for raw in text.split("¶"):
        b = re.sub(r"\s+", " ", raw).strip()
        if 40 <= len(b) <= 2000 and YEAR.search(b) and len(b.split()) >= 6:
            k = block_hash(b)
            if k not in seen:
                seen.add(k)
                out.append(b)
    return out[:MAX_BLOCKS_PER_PAGE]


def block_hash(b):
    norm = re.sub(r"[^a-z0-9]+", "", unicodedata.normalize("NFKD", b).encode("ascii", "ignore").decode().lower())
    return hashlib.sha256(norm.encode()).hexdigest()


def clean_doi(d):
    """Trim what citation punctuation glues onto a DOI: '.PMID: 1', a trailing '.,;:', and a
    closing ')' or ']' with no opening partner inside the DOI."""
    d = re.split(r"\.?(?:pmid|pmcid)\b", d, flags=re.I)[0]
    while d and d[-1] in ".,;:)]}":
        if d[-1] == ")" and d.count("(") >= d.count(")"):
            break
        if d[-1] == "]" and d.count("[") >= d.count("]"):
            break
        d = d[:-1]
    return d.lower() or None


def inline_doi(block):
    m = DOI_INLINE.search(block)
    return clean_doi(m.group(1)) if m else None


SYSTEM_PROMPT = """You read lines copied from a university researcher's web page or CV.
Each item is one line of text that MAY be a publication citation, or may be something
else (a talk, a grant, an award, a course, a news item, a job, "in preparation" work).

For every item return one result:
  {"i": <index>, "is_publication": true|false, "title": <string or null>,
   "authors": [<author names exactly as written, in order>], "venue": <journal, book,
   conference or publisher, or null>, "year": <integer or null>,
   "type": "article"|"book"|"chapter"|"conference"|"preprint"|"other",
   "doi": <DOI string only if it appears verbatim in the item, else null>}

Rules:
- is_publication is true only for published or accepted scholarly work (journal article,
  book, chapter, conference paper, preprint). Talks, posters, grants, awards, patents,
  media coverage, theses in progress and "submitted"/"in preparation" are false.
- Copy the title as written; do not translate, correct or complete it.
- Never invent a DOI. If none is written in the item, doi is null.
Return only JSON: {"results": [ ... ]}"""


def parse_answer(answer, n):
    """Model text -> {index: parsed dict}, validated. Unparseable -> {} (retried next run)."""
    start, end = (answer or "").find("{"), (answer or "").rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        results = json.loads(answer[start:end + 1]).get("results", [])
    except (ValueError, AttributeError):
        return {}
    out = {}
    for o in results if isinstance(results, list) else []:
        if not isinstance(o, dict):
            continue
        i = o.get("i")
        if not (isinstance(i, int) and not isinstance(i, bool) and 0 <= i < n) or i in out:
            continue
        title = o.get("title") if isinstance(o.get("title"), str) else None
        year = o.get("year") if isinstance(o.get("year"), int) and 1900 <= o.get("year") <= 2100 else None
        authors = [a for a in (o.get("authors") or []) if isinstance(a, str)][:100]
        out[i] = {"is_publication": o.get("is_publication") is True and bool(title and len(title) >= 10),
                  "title": title, "authors": authors, "year": year,
                  "venue": o.get("venue") if isinstance(o.get("venue"), str) else None,
                  "type": o.get("type") if o.get("type") in ("article", "book", "chapter", "conference", "preprint", "other") else "other",
                  "doi": o.get("doi") if isinstance(o.get("doi"), str) else None}
    return out


def title_in_block(title, block):
    """>= 80% of the title's words appear in the block. A model answer keyed to the wrong index
    (results shifted by one) carries another block's title and fails this."""
    tw = norm_title(title).split()
    if not tw:
        return False
    bw = set(norm_title(block).split())
    return sum(w in bw for w in tw) / len(tw) >= 0.8


def verbatim_doi(parsed_doi, block):
    """A model DOI survives only if it is literally in the block."""
    if not parsed_doi:
        return None
    d = clean_doi(parsed_doi.strip().lower().removeprefix("https://doi.org/").removeprefix("doi:").strip() or "")
    return d if d and d in block.lower() else None


def norm_title(t):
    t = unicodedata.normalize("NFKD", htmllib.unescape(t or "")).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"<[^>]+>", " ", t)
    return " ".join(re.findall(r"[a-z0-9]+", t))


def title_similarity(a, b):
    a, b = norm_title(a), norm_title(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()


def pick_crossref(items, title, year):
    """Best Crossref item whose title matches near-exactly (and year within 1 when both known)."""
    best, best_sim = None, 0.0
    for it in items or []:
        ct = (it.get("title") or [""])[0]
        sim = title_similarity(title, ct)
        dp = (it.get("issued") or {}).get("date-parts") or [[None]]
        cy = dp[0][0] if dp and dp[0] else None
        if year and cy and abs(int(cy) - int(year)) > 1:
            continue
        if sim >= TITLE_MATCH and sim > best_sim:
            best, best_sim = it, sim
    return best, best_sim


def tokens(s):
    return [t for t in re.split(r"[^a-z]+", unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()) if t]


PARTICLES = {"de", "la", "le", "van", "von", "der", "den", "del", "della", "da", "di", "do", "dos", "das",
             "du", "des", "ten", "ter", "y", "e", "al", "el", "bin", "ibn", "st"}


def name_on_authors(first, last, authors):
    """True if the person (roster first/last) is on a Crossref author list: an author family
    token is one of the person's surname tokens (compound surnames) and a given initial is
    shared ("Chris" ~ "J. Christopher"). ponytail: misses changed surnames until identity
    facts (alternate names) feed in."""
    lt = {t for t in tokens(last) if len(t) > 1 and t not in PARTICLES}
    fi = {t[0] for t in tokens(first)}
    for a in authors or []:
        fam, giv = tokens(a.get("family")), tokens(a.get("given"))
        if not fam and a.get("name"):
            parts = tokens(a["name"])
            fam, giv = parts[-1:], parts[:-1]
        if lt.intersection(t for t in fam if t not in PARTICLES) and fi.intersection(g[0] for g in giv):
            return True
    return False


# ---------------------------------------------------------------------------
#                              external calls
# ---------------------------------------------------------------------------

def bedrock_client():
    import boto3
    from botocore.config import Config
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    return boto3.client("bedrock-runtime", region_name=region, config=Config(
        connect_timeout=10, read_timeout=180, retries={"max_attempts": 3, "mode": "standard"}))


def _parse_call(client, part, model):
    try:
        resp = client.converse(
            modelId=model, system=[{"text": SYSTEM_PROMPT}],
            messages=[{"role": "user", "content": [{"text": json.dumps(
                [{"i": i, "text": b} for i, b in enumerate(part)], ensure_ascii=False)}]}],
            inferenceConfig={"maxTokens": MAX_TOKENS})
    except Exception as e:
        log.warning("parse batch failed: %s", e)
        return {}
    content = ((resp.get("output") or {}).get("message") or {}).get("content") or []
    return parse_answer("".join(c.get("text", "") for c in content if isinstance(c, dict)), len(part))


def _accept(parsed, block):
    """A publication parse must carry a title from its own block (see title_in_block)."""
    return not parsed["is_publication"] or title_in_block(parsed["title"], block)


def parse_blocks(client, blocks, model=MODEL, stop=lambda: False):
    """[block] -> {index: parsed}. One converse() call per PARSE_BATCH blocks; blocks a batch
    returns nothing for (an error, a truncated or unreadable answer) are retried once in
    half-size batches. Still-missing blocks stay unparsed and are retried next run.
    (Eval 2026-11-20: one lost 25-block batch cost Haiku 23 seeds on a single page.)"""
    out = {}
    for s0 in range(0, len(blocks), PARSE_BATCH):
        if stop():
            break
        idx = list(range(s0, min(s0 + PARSE_BATCH, len(blocks))))
        got = _parse_call(client, [blocks[i] for i in idx], model)
        out.update({idx[i]: p for i, p in got.items() if _accept(p, blocks[idx[i]])})
        missing = [i for i in idx if i not in out]
        half = max(1, PARSE_BATCH // 2)
        for m0 in range(0, len(missing), half):
            if stop():
                break
            sub = missing[m0:m0 + half]
            got = _parse_call(client, [blocks[i] for i in sub], model)
            out.update({sub[i]: p for i, p in got.items() if _accept(p, blocks[sub[i]])})
    return out


class Transient(Exception):
    """Crossref did not answer (429/5xx/timeout). Never cached: the block is retried next run."""


class Crossref:
    """Crossref REST, ~1 req/s per client (locked, so threads sharing one client still pace),
    bounded retries. No contact details are sent (privacy rule). Raises Transient when it
    could not get an answer; a 404 is a real answer (None)."""

    def __init__(self):
        import threading
        self._last, self._lock = 0.0, threading.Lock()

    def _get(self, url):
        for attempt in range(3):
            with self._lock:
                wait = self._last + 1.0 - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                self._last = time.monotonic()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=30) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None
                time.sleep(2 * (attempt + 1))
            except Exception:
                time.sleep(2 * (attempt + 1))
        raise Transient(url)

    def work(self, doi):
        d = self._get("https://api.crossref.org/works/" + urllib.parse.quote(doi, safe="/"))
        return (d or {}).get("message")

    def search(self, title, first_author, year):
        q = " ".join(x for x in (title, first_author or "", str(year or "")) if x)
        d = self._get("https://api.crossref.org/works?rows=5&select=DOI,title,author,issued,type&query.bibliographic="
                      + urllib.parse.quote(q[:500]))
        return ((d or {}).get("message") or {}).get("items") or []


def resolve(cr, block, parsed):
    """(doi, method, score, crossref authors) or (None, ...) -- deterministic given Crossref."""
    d = inline_doi(block) or verbatim_doi(parsed.get("doi"), block)
    if d:
        w = cr.work(d)  # Transient propagates: not an answer
        if w:
            return d, "inline", 1.0, w.get("author") or []
    if not parsed.get("is_publication") or not parsed.get("title"):
        return None, "inline_unresolvable" if d else "not_publication", 0.0, []
    first = (parsed.get("authors") or [""])[0]
    first_surname = (tokens(first) or [""])[-1] if first else ""
    it, sim = pick_crossref(cr.search(parsed["title"], first_surname, parsed.get("year")), parsed["title"], parsed.get("year"))
    if not it:
        return None, "no_match", 0.0, []
    return it["DOI"].lower(), "crossref", round(sim, 3), it.get("author") or []


# ---------------------------------------------------------------------------
#                       store + incremental pass (cron)
# ---------------------------------------------------------------------------

DDL = [
    """CREATE TABLE IF NOT EXISTS pubsource_web_page_snapshot (
  url VARCHAR(600) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  content_hash CHAR(64) NOT NULL,
  main_text MEDIUMTEXT,
  blocks_json MEDIUMTEXT,
  captured_at DATETIME NOT NULL,
  PRIMARY KEY (url, content_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_citation_parse (
  citation_hash CHAR(64) NOT NULL PRIMARY KEY,
  block_text TEXT NOT NULL,
  parsed_json TEXT NOT NULL,
  extractor_version VARCHAR(100) NOT NULL,
  parsed_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_citation_resolve (
  citation_hash CHAR(64) NOT NULL PRIMARY KEY,
  doi VARCHAR(255),
  method VARCHAR(30) NOT NULL,
  score DECIMAL(4,3),
  authors_json MEDIUMTEXT,
  resolved_at DATETIME NOT NULL,
  KEY idx_doi (doi)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_citation (
  url VARCHAR(600) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  citation_hash CHAR(64) NOT NULL,
  content_hash CHAR(64) NOT NULL,
  first_seen DATETIME NOT NULL,
  last_seen DATETIME NOT NULL,
  removed_at DATETIME,
  PRIMARY KEY (url, citation_hash),
  KEY idx_hash (citation_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_seed (
  netid VARCHAR(100) NOT NULL,
  doi VARCHAR(255) NOT NULL,
  urls_json TEXT NOT NULL,
  citation_hashes_json TEXT NOT NULL,
  method VARCHAR(30) NOT NULL,
  extractor_version VARCHAR(100) NOT NULL,
  first_seen DATETIME NOT NULL,
  last_seen DATETIME NOT NULL,
  removed_at DATETIME,
  PRIMARY KEY (netid, doi),
  KEY idx_doi (doi)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    "ALTER TABLE pubsource_web_page ADD COLUMN IF NOT EXISTS citations_hash CHAR(64)",
]

UNRESOLVED_RETRY = datetime.timedelta(days=30)  # Crossref coverage grows; ask again later
CROSSREF_WORKERS = 3

# Academics only: staff and students are out of Ithaca scope and would multiply model/Crossref volume.
SCOPE_SQL = """(r.is_faculty = 1 OR r.is_rte_faculty = 1 OR r.is_postdoc = 1 OR r.is_researcher = 1
                OR r.is_proj_researcher = 1 OR IFNULL(r.affiliations_all, '') LIKE '%emerit%')"""


def has_pub_signal(doi_count, year_count):
    return (doi_count or 0) >= 2 or (year_count or 0) >= 8


def seed_rows(person_citations, resolved, names):
    """{netid: {doi: (urls, hashes, method)}} from (netid, url, citation_hash) rows, resolutions
    {hash: (doi, method, authors)} and roster names {netid: (first, last)}. A DOI is a seed only
    if the person's name is on its author list."""
    out = {}
    for netid, url, h in person_citations:
        r = resolved.get(h)
        if not r or not r[0]:
            continue
        first, last = names.get(netid, ("", ""))
        if not name_on_authors(first, last, r[2]):
            continue
        d = out.setdefault(netid, {}).setdefault(r[0], [set(), set(), r[1]])
        d[0].add(url)
        d[1].add(h)
    return out


def _in(n):
    return ",".join(["%s"] * n)


def _load(c, sql, keys):
    """Run sql (with one IN placeholder group) over keys in chunks of 500."""
    out = []
    for i in range(0, len(keys), 500):
        part = keys[i:i + 500]
        c.execute(sql.format(_in(len(part))), part)
        out.extend(c.fetchall())
    return out


def run_pass(db_conn, expired, now, model=MODEL):
    """Incremental citation pass; `expired()` is the job's deadline/SIGTERM check. Returns stats.

    Fail-soft by construction: a person's seeds are only RETRACTED when every live citation on
    every live page of theirs is parsed and resolved this run; with work outstanding (Bedrock
    down, Crossref throttled, deadline) seeds are only added. Crossref non-answers are never
    cached."""
    stats = collections.Counter()
    db = db_conn()
    try:
        with db.cursor() as c:
            for ddl in DDL:
                c.execute(ddl)
            # pages to (re)process: in-scope sole person, live, snapshot at the current hash, not yet done
            c.execute(f"""SELECT w.url, w.content_hash, s.blocks_json, pp.netid
                FROM pubsource_web_page w
                JOIN pubsource_web_page_snapshot s ON s.url = w.url AND s.content_hash = w.content_hash
                JOIN pubsource_web_page_person pp ON pp.url = w.url AND pp.removed_at IS NULL AND pp.is_sole_person = 1
                JOIN identity_cornell_current r ON r.netid = pp.netid COLLATE utf8mb4_unicode_ci
                WHERE w.gone_at IS NULL AND (w.citations_hash IS NULL OR w.citations_hash <> w.content_hash)
                  AND {SCOPE_SQL}""")
            todo = c.fetchall()
            # stale non-answers to ask Crossref again (coverage grows), on citations still live
            c.execute("""SELECT DISTINCT r.citation_hash, p.block_text FROM pubsource_web_citation_resolve r
                JOIN pubsource_web_citation_parse p ON p.citation_hash = r.citation_hash
                JOIN pubsource_web_citation c ON c.citation_hash = r.citation_hash AND c.removed_at IS NULL
                WHERE r.doi IS NULL AND r.method <> 'not_publication' AND r.resolved_at < %s""", (now - UNRESOLVED_RETRY,))
            stale = {r["citation_hash"]: r["block_text"] for r in c.fetchall()}
        db.commit()
        stats["pages_todo"], stats["stale_retry"] = len(todo), len(stale)

        # 1. page membership for (re)processed pages
        blocks_by_hash, page_hashes = dict(stale), {}
        for p in todo:
            hs = []
            for b in json.loads(p["blocks_json"] or "[]"):
                h = block_hash(b)
                blocks_by_hash.setdefault(h, b)
                hs.append(h)
            page_hashes[p["url"]] = (p["content_hash"], hs)
        with db.cursor() as c:
            for url, (chash, hs) in page_hashes.items():
                if hs:
                    c.executemany("""INSERT INTO pubsource_web_citation (url, citation_hash, content_hash, first_seen, last_seen, removed_at)
                                     VALUES (%s,%s,%s,%s,%s,NULL) ON DUPLICATE KEY UPDATE content_hash=VALUES(content_hash),
                                     last_seen=VALUES(last_seen), removed_at=NULL""", [(url, h, chash, now, now) for h in hs])
                keep = hs or [""]
                c.execute(f"UPDATE pubsource_web_citation SET removed_at=%s WHERE url=%s AND removed_at IS NULL "
                          f"AND citation_hash NOT IN ({_in(len(keep))})", (now, url, *keep))
        db.commit()

        # 2. parse blocks never parsed before (shared across pages)
        all_h = list(blocks_by_hash)
        with db.cursor() as c:
            parsed = {r["citation_hash"]: json.loads(r["parsed_json"]) for r in _load(
                c, "SELECT citation_hash, parsed_json FROM pubsource_web_citation_parse WHERE citation_hash IN ({})", all_h)}
        need = [h for h in all_h if h not in parsed]
        stats["blocks"], stats["blocks_to_parse"] = len(all_h), len(need)
        client = bedrock_client() if need else None
        empty_rounds = 0
        for i in range(0, len(need), PARSE_BATCH * 8):
            if expired():
                stats["stopped_at_deadline"] = 1
                break
            part = need[i:i + PARSE_BATCH * 8]
            got = parse_blocks(client, [blocks_by_hash[h] for h in part], model, expired)
            empty_rounds = 0 if got else empty_rounds + 1
            rows = [(part[j], blocks_by_hash[part[j]], json.dumps(p), EXTRACTOR_VERSION, now) for j, p in got.items()]
            with db.cursor() as c:
                c.executemany("""INSERT INTO pubsource_web_citation_parse (citation_hash, block_text, parsed_json, extractor_version, parsed_at)
                                 VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE citation_hash=citation_hash""", rows)
            db.commit()
            parsed.update({part[j]: p for j, p in got.items()})
            stats["blocks_parsed"] += len(got)
            if empty_rounds >= 3:  # Bedrock unreachable / not permitted: stop, retry next run
                log.warning("citation pass: 3 parse rounds returned nothing; stopping parse for this run")
                stats["parse_gave_up"] = 1
                break

        # 3. resolve parsed blocks with no fresh answer (stale non-answers included)
        resolved = {}
        with db.cursor() as c:
            for r in _load(c, "SELECT citation_hash, doi, method, authors_json FROM pubsource_web_citation_resolve WHERE citation_hash IN ({})", all_h):
                if r["citation_hash"] not in stale:
                    resolved[r["citation_hash"]] = (r["doi"], r["method"], json.loads(r["authors_json"] or "[]"))
        need = [h for h in all_h if h in parsed and h not in resolved]
        stats["to_resolve"] = len(need)
        crs = [Crossref() for _ in range(CROSSREF_WORKERS)]

        def one(k):
            h = need[k]
            if expired():
                return h, "deadline"
            try:
                return h, resolve(crs[k % CROSSREF_WORKERS], blocks_by_hash[h], parsed[h])
            except Transient:
                return h, "transient"
            except Exception as e:  # one odd record must not abort the pass
                log.warning("resolve %s: %s", h[:12], e)
                return h, "error"

        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(CROSSREF_WORKERS) as ex:
            for h, res in ex.map(one, range(len(need))):
                if isinstance(res, str):  # no answer: nothing cached, retried next run
                    stats["resolve_" + res] += 1
                    continue
                doi, method, score, authors = res
                with db.cursor() as c:
                    c.execute("""INSERT INTO pubsource_web_citation_resolve (citation_hash, doi, method, score, authors_json, resolved_at)
                                 VALUES (%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE doi=VALUES(doi), method=VALUES(method),
                                 score=VALUES(score), authors_json=VALUES(authors_json), resolved_at=VALUES(resolved_at)""",
                              (h, doi, method, score, json.dumps(authors), now))
                db.commit()
                resolved[h] = (doi, method, authors)
                stats["resolved_" + ("doi" if doi else method)] += 1

        # 4. seeds: everyone touched this run AND everyone holding live seeds (pages can die, change
        #    owner or drop their list without ever re-entering `todo`). DB-only, so cheap.
        with db.cursor() as c:
            c.execute("SELECT DISTINCT netid FROM pubsource_web_seed WHERE removed_at IS NULL")
            netids = sorted({r["netid"] for r in c.fetchall()} | {p["netid"] for p in todo})
        if netids:
            with db.cursor() as c:
                # only citations of each page's CURRENT content, on live sole-person pages
                pc = [(r["netid"], r["url"], r["citation_hash"]) for r in _load(c, """SELECT pp.netid, c.url, c.citation_hash
                    FROM pubsource_web_page_person pp
                    JOIN pubsource_web_page w ON w.url = pp.url AND w.gone_at IS NULL
                    JOIN pubsource_web_citation c ON c.url = pp.url AND c.removed_at IS NULL AND c.content_hash = w.content_hash
                    WHERE pp.removed_at IS NULL AND pp.is_sole_person = 1 AND pp.netid IN ({})""", netids)]
                extra = sorted({h for _, _, h in pc if h not in resolved})
                for r in _load(c, "SELECT citation_hash, doi, method, authors_json FROM pubsource_web_citation_resolve WHERE citation_hash IN ({})", extra):
                    if r["doi"] or r["citation_hash"] not in stale:
                        resolved[r["citation_hash"]] = (r["doi"], r["method"], json.loads(r["authors_json"] or "[]"))
                names = {r["netid"].lower(): (r["name_first"], r["name_last"]) for r in _load(
                    c, "SELECT netid, name_first, name_last FROM identity_cornell_current WHERE netid IN ({})", netids)}
            seeds = seed_rows(pc, resolved, names)
            complete = {n: True for n in netids}
            for n, _, h in pc:
                if h not in resolved:
                    complete[n] = False
            with db.cursor() as c:
                for netid in netids:
                    mine = seeds.get(netid, {})
                    if mine:
                        c.executemany("""INSERT INTO pubsource_web_seed (netid, doi, urls_json, citation_hashes_json, method, extractor_version, first_seen, last_seen, removed_at)
                                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL) ON DUPLICATE KEY UPDATE urls_json=VALUES(urls_json),
                                         citation_hashes_json=VALUES(citation_hashes_json), method=VALUES(method),
                                         extractor_version=VALUES(extractor_version), last_seen=VALUES(last_seen), removed_at=NULL""",
                                      [(netid, d, json.dumps(sorted(v[0])), json.dumps(sorted(v[1])), v[2], EXTRACTOR_VERSION, now, now)
                                       for d, v in mine.items()])
                        stats["seeds"] += len(mine)
                    if complete[netid]:  # full picture: anything not re-derived is retracted
                        keep = list(mine) or [""]
                        c.execute(f"UPDATE pubsource_web_seed SET removed_at=%s WHERE netid=%s AND removed_at IS NULL "
                                  f"AND doi NOT IN ({_in(len(keep))})", (now, netid, *keep))
                        stats["seeds_retracted"] += c.rowcount
                    else:
                        stats["people_incomplete"] += 1
                # a page is done when every block on it is parsed and resolved
                for url, (chash, hs) in page_hashes.items():
                    if all(h in parsed and h in resolved for h in hs):
                        c.execute("UPDATE pubsource_web_page SET citations_hash=%s WHERE url=%s", (chash, url))
                        stats["pages_done"] += 1
            db.commit()
        stats["people"] = len(netids)
        return dict(stats)
    except Exception:
        db.rollback()
        log.exception("citation pass failed; stored seeds untouched")
        stats["failed"] = 1
        return dict(stats)
    finally:
        db.close()
