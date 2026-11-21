#!/usr/bin/env python3
"""
Cornell Ithaca web-page seeds, step 1: incremental page store + NetID linkage.

Crawls the people pages of the cornell.edu sites in web_seed_hosts.txt (via each
site's sitemap), keeps one row per URL in pubsource_web_page, and links each page
to the Ithaca roster person whose email it shows (pubsource_web_page_person).
Later steps (one-hop lab sites, citation extraction, ORCID inference, expansion)
read this store; see the plan in the Scholars-Profile-System Projects folder,
2026-11-20-PLAN-ithaca-web-page-seeds.md.

Incremental by design:
  * sitemap lastmod <= last_fetched, fetched within REFETCH_DAYS, last status OK -> no request
  * otherwise a conditional GET (If-None-Match / If-Modified-Since); 304 -> touch
  * a 200 whose main-text hash is unchanged -> touch
  * person links are recomputed from the stored emails on EVERY pass (no fetch needed), so a
    roster change reaches pages whose text did not change
  * a URL absent from a COMPLETE sitemap in GONE_AFTER_RUNS weekly runs, or answering 404/410,
    gets gone_at; rows are never deleted
All state lives in reciterdb, so an interrupted run resumes where it stopped and a
rerun is always safe.

Polite: honours robots.txt, identifies itself, at most 1 request/second per host.

Own CronJob (kubernetes/k8-cronjob-web-seeds.yaml), NOT in run_all.py: a first full
crawl takes hours. WEB_SEEDS_MAX_SECONDS stops all work cleanly, every network read has
a total wall-clock cap, and the run row is always closed on a fresh connection, so the
job can never hang into the next schedule (concurrencyPolicy: Forbid).

Usage:
  python3 webPageSeeds.py                    # crawl + link, all hosts
  python3 webPageSeeds.py --hosts nbb.cornell.edu --max-pages 50
  python3 webPageSeeds.py --report           # coverage of active Ithaca professors
"""

import argparse
import datetime
import gzip
import hashlib
import json
import logging
import os
import re
import signal
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor

import webPageCitations

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("webPageSeeds")

HERE = os.path.dirname(os.path.abspath(__file__))
HOSTS_FILE = os.path.join(HERE, "web_seed_hosts.txt")
UA = "Mozilla/5.0 (compatible; Cornell-WCM-Scholars-research-indexer)"
NOT_PEOPLE = re.compile(r"/(news|events?|stories|blog|calendar|press|media|announcements?)/", re.I)
PEOPLE_PATH = re.compile(r"/(people|person|faculty|directory|profiles?|staff|our-team|team|bio)(/|$|-)", re.I)
MAX_PER_HOST = int(os.environ.get("WEB_SEEDS_MAX_PER_HOST", "6000"))
MAX_SECONDS = int(os.environ.get("WEB_SEEDS_MAX_SECONDS", str(10 * 3600)))
REFETCH_DAYS = int(os.environ.get("WEB_SEEDS_REFETCH_DAYS", "30"))
GONE_AFTER_RUNS = 2
MISSING_MIN_GAP = datetime.timedelta(days=5)  # count at most one absence per weekly run, even across restarts
HOST_WORKERS = 12
REQUEST_GAP = 1.0      # seconds between requests to one host (raised by robots Crawl-delay or throttling)
MAX_GAP = 60.0         # ceiling for a host's request gap
# Hosts that rate-limit the cluster's egress IP well below 1 req/s: start slow instead of
# tripping the limit and backing off. Politeness only; a host that still throttles is left alone.
# business.cornell.edu answered 429 after 9 requests at 1/s (run 5, 11-20) and kept refusing at 60s.
HOST_GAP = {"business.cornell.edu": 15.0}
THROTTLE_STATUSES = {429, 503, 403, 405, 999}  # observed rate-limit / WAF answers; back off on these
THROTTLE_STOP = 10     # consecutive throttle answers -> leave the host for this run
ALIAS_TTL = datetime.timedelta(days=30)  # re-ask the directory about an address after this
LDAP_BATCH = 40
READ_TIMEOUT = 25      # per socket operation
FETCH_BUDGET = 60      # total seconds for one response body
MAX_BYTES = 3_000_000
MAX_SITEMAPS = 60
DB_WRITE = threading.Lock()
STOP = threading.Event()  # set on SIGTERM; every network call and page loop checks it
MAX_URL = 600  # (url, netid) PK must fit InnoDB's 3072-byte key limit in utf8mb4
SKIP_EMAIL_DOMAINS = {"med.cornell.edu", "weill.cornell.edu", "qatar-med.cornell.edu"}  # WCM namespace, not Ithaca

URL_COL = "VARCHAR(600) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"  # URLs are case-sensitive
DDL = [
    f"""CREATE TABLE IF NOT EXISTS pubsource_web_page (
  url {URL_COL} NOT NULL PRIMARY KEY,
  host VARCHAR(200) NOT NULL,
  kind VARCHAR(20) NOT NULL DEFAULT 'profile',
  parent_url {URL_COL},
  sitemap_lastmod DATETIME,
  etag VARCHAR(300),
  last_modified VARCHAR(100),
  content_hash CHAR(64),
  http_status SMALLINT,
  emails_json MEDIUMTEXT,
  links_json MEDIUMTEXT,
  doi_count INT,
  year_count INT,
  missing_runs INT NOT NULL DEFAULT 0,
  last_missing_at DATETIME,
  first_seen DATETIME NOT NULL,
  last_attempt DATETIME,
  last_fetched DATETIME,
  last_changed DATETIME,
  gone_at DATETIME,
  KEY idx_host (host),
  KEY idx_gone (gone_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    f"""CREATE TABLE IF NOT EXISTS pubsource_web_page_person (
  url {URL_COL} NOT NULL,
  netid VARCHAR(100) NOT NULL,
  link_method VARCHAR(30) NOT NULL,
  is_sole_person TINYINT(1) NOT NULL,
  content_hash CHAR(64),
  first_seen DATETIME NOT NULL,
  last_seen DATETIME NOT NULL,
  removed_at DATETIME,
  PRIMARY KEY (url, netid),
  KEY idx_netid (netid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_email_alias (
  email VARCHAR(254) NOT NULL PRIMARY KEY,
  netid VARCHAR(100),
  source VARCHAR(20) NOT NULL,
  resolved_at DATETIME NOT NULL,
  KEY idx_netid (netid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
    """CREATE TABLE IF NOT EXISTS pubsource_web_run (
  run_id INT AUTO_INCREMENT PRIMARY KEY,
  started_at DATETIME NOT NULL,
  finished_at DATETIME,
  status VARCHAR(20) NOT NULL,
  stats_json MEDIUMTEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci""",
]

# ---------------------------------------------------------------------------
#                        pure functions (unit-tested)
# ---------------------------------------------------------------------------

# Local part bounded and anchored: unanchored, a long run of [A-Za-z0-9._-] (inline SVG,
# hashes) made this quadratic -- 40k chars took 37s.
EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._-])([A-Za-z0-9._-]{1,64})\s*(?:@|\[at\]|\(at\)|\bat\b)\s*"
    r"((?:[a-z0-9-]{1,63}\.)?)cornell\s*(?:\.|\[dot\]|\(dot\)|\bdot\b)\s*edu", re.I)
LINK_RE = re.compile(r'href="(https?://[^"#\s]{1,2000})"', re.I)
DOI_RE = re.compile(r"10\.\d{4,9}/[^\s\"<>]{1,300}")
YEAR_RE = re.compile(r"\((?:19|20)\d\d\)|\b(?:19|20)\d\d\.")


def tokens(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return [t for t in re.split(r"[^a-z]+", s) if t]


def main_text(html):
    """Visible text, whitespace-collapsed. Scripts/styles dropped so tracking noise doesn't change the hash."""
    html = re.sub(r"(?is)<(script|style|noscript|svg)\b.*?</\1>", " ", html)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()


def page_signals(html):
    """Everything later steps need from one fetch. Emails are matched on tag-stripped text too,
    because Drupal spamspan splits them across tags: <span class="u">x1</span> [at] <span ...>cornell.edu.
    Emails come back as full lowercase addresses (local@[sub.]cornell.edu)."""
    text = main_text(html)
    emails = {f"{m.group(1)}@{m.group(2)}cornell.edu".lower() for m in EMAIL_RE.finditer(text + " " + html)}
    return {
        "hash": hashlib.sha256(text.encode()).hexdigest(),
        "emails": sorted(emails),
        "links": sorted(set(LINK_RE.findall(html)))[:500],
        "dois": len(set(DOI_RE.findall(html))),
        "years": len(YEAR_RE.findall(text)),
    }


def surname_index(people):
    """{surname token: {given initials}} from roster (netid, first, last) rows."""
    idx = {}
    for _, first, last in people:
        for t in tokens(last):
            if len(t) > 2:
                idx.setdefault(t, set()).update(x[0] for x in tokens(first))
    return idx


def looks_like_person_url(url, idx):
    """A people-section path, or a last segment like 'quentin-zarnowski' (a roster surname plus a
    token sharing that person's given initial). ponytail: name heuristic; over-includes some
    slugs, which then simply link to no one."""
    if NOT_PEOPLE.search(url):
        return False
    if PEOPLE_PATH.search(url):
        return True
    seg = tokens(url.rstrip("/").rsplit("/", 1)[-1])
    return 2 <= len(seg) <= 4 and any(
        t in idx and any(o[0] in idx[t] for o in seg if o != t) for t in seg)


def alias_map(roster):
    """Full address -> netid, from roster (netid, email, email_netid, email_alt_cu) rows.
    netid@cornell.edu always maps to its own netid and wins over any alias."""
    m = {}
    for netid, *emails in roster:
        for e in emails:
            if e and "@" in e:
                m.setdefault(e.strip().lower(), netid.lower())
    for netid, *_ in roster:
        m[f"{netid.lower()}@cornell.edu"] = netid.lower()
    return m


def link_people(emails, aliases, directory=None):
    """[(netid, link_method)] for a page's full email addresses. Only exact addresses link (a bare
    local part from another cornell.edu namespace, e.g. WCM, must not): first the roster's own
    addresses, then Cornell directory aliases (mail / publishedemail / mailAlternateAddress) that
    resolve to a roster NetID. A page counts toward a person downstream only if it names exactly
    one roster person."""
    hits = {}
    for e in emails:
        if e.split("@", 1)[-1] in SKIP_EMAIL_DOMAINS:
            continue
        n = aliases.get(e)
        if n:
            hits.setdefault(n, "email" if e == f"{n}@cornell.edu" else "roster_alias")
        elif directory and directory.get(e):
            hits.setdefault(directory[e], "ldap_alias")
    return sorted(hits.items())


def own_host_urls(entries, host, base):
    """Sitemap entries resolved against the sitemap URL, kept only if they belong to THIS host.
    Several host names serve one site (duffield / www.duffield / engineering list the same
    1,215 pages); without this each name crawled them all, tripling the load on one server."""
    out = []
    for u, lm in entries:
        u = urllib.parse.urljoin(base, u)
        if urllib.parse.urlsplit(u).netloc.lower() == host:
            out.append((u, lm))
    return out


def ldap_filter(emails):
    from ldap3.utils.conv import escape_filter_chars
    parts = [f"({a}={escape_filter_chars(e)})" for e in emails
             for a in ("mail", "cornelledupublishedemail", "mailAlternateAddress")]
    return "(|" + "".join(parts) + ")"


def match_directory_entries(emails, entries):
    """{email: uid} for queried addresses found on exactly one directory entry."""
    owners = {}
    for uid, addrs in entries:
        for a in addrs:
            if a in emails:
                owners.setdefault(a, set()).add(uid)
    return {e: next(iter(u)) for e, u in owners.items() if len(u) == 1}


def parse_lastmod(s):
    if not s:
        return None
    try:
        d = datetime.datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        return d.astimezone(datetime.timezone.utc).replace(tzinfo=None) if d.tzinfo else d
    except ValueError:
        return None


def needs_fetch(row, sitemap_lastmod, now):
    """row = existing pubsource_web_page row (dict) or None."""
    if row is None or row["last_fetched"] is None or row["gone_at"] is not None:
        return True
    if row.get("http_status") not in (200, 304):  # a transient error must not buy 30 days of silence
        return True
    if now - row["last_fetched"] >= datetime.timedelta(days=REFETCH_DAYS):
        return True
    return sitemap_lastmod is not None and sitemap_lastmod > row["last_fetched"]


def count_missing(row, now):
    """(new missing_runs, mark gone?) for a known page absent from a complete sitemap, or None
    if this absence was already counted within MISSING_MIN_GAP (restart in the same week)."""
    if row["last_missing_at"] and now - row["last_missing_at"] < MISSING_MIN_GAP:
        return None
    n = row["missing_runs"] + 1
    return n, n >= GONE_AFTER_RUNS


# ---------------------------------------------------------------------------
#                                 network
# ---------------------------------------------------------------------------

class Deadline(Exception):
    pass


def expired(deadline):
    return STOP.is_set() or time.monotonic() > deadline


class HostThrottled(Exception):
    pass


class Host:
    """One host's robots.txt + a request gate (1 req/s, or the site's Crawl-delay, raised further
    when the site throttles). Every read is bounded in total wall-clock time."""

    def __init__(self, name, deadline):
        self.name, self.deadline, self._last, self._lock = name, deadline, 0.0, threading.Lock()
        self.gap, self.throttled_in_a_row, self.throttle_events = HOST_GAP.get(name, REQUEST_GAP), 0, 0
        self.robots = urllib.robotparser.RobotFileParser(f"https://{name}/robots.txt")
        self.robots_ok = True
        try:
            status, _, _, body = self.get(f"https://{name}/robots.txt")
        except Deadline:
            raise
        except Exception:
            status, body = None, ""
        if status == 200:
            self.robots.parse(body.splitlines())
            delay = self.robots.crawl_delay(UA)
            if delay:
                self.gap = min(max(self.gap, float(delay)), MAX_GAP)
        elif status is not None and 400 <= status < 500 and status not in THROTTLE_STATUSES:
            self.robots.allow_all = True       # no robots.txt = no restrictions (RFC 9309)
        else:
            self.robots_ok = False             # 5xx / throttled / unreachable: crawl nothing this run

    def get(self, url, headers=None):
        if expired(self.deadline):
            raise Deadline()
        with self._lock:
            wait = self._last + self.gap - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
        req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
        try:
            r = urllib.request.urlopen(req, timeout=READ_TIMEOUT)
        except urllib.error.HTTPError as e:
            self._throttle_check(e.code, e.headers)
            return e.code, url, dict(e.headers or {}), ""
        self.throttled_in_a_row = 0
        with r:
            stop, chunks, size = time.monotonic() + FETCH_BUDGET, [], 0
            while size < MAX_BYTES:
                if time.monotonic() > stop:
                    raise TimeoutError(f"body exceeded {FETCH_BUDGET}s")
                b = r.read(65536)
                if not b:
                    break
                chunks.append(b)
                size += len(b)
            body = b"".join(chunks)
            truncated = size >= MAX_BYTES
            if url.endswith(".gz") or body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            hdr = dict(r.headers)
            hdr["X-Truncated"] = truncated
            return r.status, r.geturl(), hdr, body.decode("utf-8", "replace")

    def _throttle_check(self, status, headers):
        """Back off on rate-limit / WAF answers: double the gap (or honour Retry-After), and give up
        on the host for this run after THROTTLE_STOP in a row. Pages left behind are retried next run."""
        if status not in THROTTLE_STATUSES:
            self.throttled_in_a_row = 0
            return
        self.throttled_in_a_row += 1
        self.throttle_events += 1
        try:
            retry = float((headers or {}).get("Retry-After") or 0)
        except ValueError:
            retry = 0.0
        self.gap = min(max(self.gap * 2, retry, 2.0), MAX_GAP)
        if self.throttled_in_a_row >= THROTTLE_STOP:
            raise HostThrottled(f"{self.name}: {self.throttled_in_a_row} throttled answers in a row (last {status})")

    def sitemap(self):
        """([(url, lastmod)], complete). complete is False if any sitemap failed, was truncated,
        or the sitemap cap was hit -- callers must then not treat absence as removal."""
        queue = list(self.robots.site_maps() or []) or [f"https://{self.name}/sitemap.xml"]
        seen, out, complete = set(), [], True
        while queue:
            if len(seen) >= MAX_SITEMAPS:
                complete = False
                break
            sm = queue.pop(0)
            if sm in seen:
                continue
            seen.add(sm)
            try:
                status, _, hdr, x = self.get(sm)
            except (Deadline, HostThrottled):
                raise
            except Exception as e:
                log.warning("%s sitemap %s: %s", self.name, sm, e)
                complete = False
                continue
            if status != 200 or hdr.get("X-Truncated"):
                complete = False
                continue
            is_index = "<sitemapindex" in x
            for block in re.findall(r"(?s)<(?:url|sitemap)>(.*?)</(?:url|sitemap)>", x):
                loc = re.search(r"<loc>\s*([^<\s]+)\s*</loc>", block)
                if not loc:
                    continue
                u = urllib.parse.urljoin(sm, loc.group(1).replace("&amp;", "&"))  # some sitemaps list relative paths
                if is_index:
                    queue.append(u)
                else:
                    lm = re.search(r"<lastmod>\s*([^<\s]+)\s*</lastmod>", block)
                    out.append((u, parse_lastmod(lm.group(1) if lm else None)))
        return own_host_urls(out, self.name, f"https://{self.name}/"), complete


# ---------------------------------------------------------------------------
#                                 database
# ---------------------------------------------------------------------------

def utcnow():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None, microsecond=0)


def db_conn():
    import pymysql
    import pymysql.cursors
    return pymysql.connect(
        host=os.environ["DB_HOST"], user=os.environ["DB_USERNAME"], password=os.environ["DB_PASSWORD"],
        database=os.environ.get("DB_NAME", "reciterdb"), charset="utf8mb4", autocommit=False,
        connect_timeout=10, read_timeout=500, write_timeout=500,
        cursorclass=pymysql.cursors.DictCursor)


def load_roster(db):
    with db.cursor() as c:
        c.execute("SELECT netid, email, email_netid, email_alt_cu, name_first, name_last FROM identity_cornell_current")
        rows = c.fetchall()
    aliases = alias_map([(r["netid"], r["email"], r["email_netid"], r["email_alt_cu"]) for r in rows])
    idx = surname_index([(r["netid"], r["name_first"], r["name_last"]) for r in rows])
    return aliases, idx, {r["netid"].lower() for r in rows}


def load_directory(c, roster):
    """Cached directory answers -> {email: roster netid}; addresses owned by non-roster people drop out."""
    c.execute("SELECT email, netid FROM pubsource_web_email_alias WHERE netid IS NOT NULL")
    return {r["email"]: r["netid"] for r in c.fetchall() if r["netid"] in roster}


def relink(c, url, emails, content_hash, aliases, now, directory=None):
    """Make a page's person links match its emails; dropped links are soft-removed. Idempotent."""
    links = link_people(emails, aliases, directory)
    sole = len(links) == 1
    for netid, method in links:
        c.execute("""INSERT INTO pubsource_web_page_person (url, netid, link_method, is_sole_person, content_hash, first_seen, last_seen, removed_at)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,NULL)
                     ON DUPLICATE KEY UPDATE link_method=VALUES(link_method), is_sole_person=VALUES(is_sole_person),
                      content_hash=VALUES(content_hash), last_seen=VALUES(last_seen), removed_at=NULL""",
                  (url, netid, method, sole, content_hash, now, now))
    keep = [n for n, _ in links] or [""]
    c.execute(f"""UPDATE pubsource_web_page_person SET removed_at=%s
                 WHERE url=%s AND removed_at IS NULL AND netid NOT IN ({",".join(["%s"] * len(keep))})""",
              (now, url, *keep))


def load_known(c, host, urls):
    """Existing rows for this host's pages AND for any listed URL (a URL can appear in another host's sitemap)."""
    c.execute("SELECT * FROM pubsource_web_page WHERE host=%s AND kind='profile'", (host,))
    known = {r["url"]: r for r in c.fetchall()}
    rest = [u for u in urls if u not in known]
    for i in range(0, len(rest), 500):
        part = rest[i:i + 500]
        c.execute(f"SELECT * FROM pubsource_web_page WHERE url IN ({','.join(['%s'] * len(part))})", part)
        known.update({r["url"]: r for r in c.fetchall()})
    return known


def crawl_host(name, aliases, directory, idx, deadline, max_pages, stats, lock):
    s = dict(host=name, listed=0, sitemap_complete=None, robots_ok=None, capped=False, fetched=0, not_modified=0,
             unchanged=0, changed=0, new=0, errors=0, page_errors=0, gone=0, skipped=0, relinked=0,
             stopped_at_deadline=False, throttled_stop=False, throttle_events=0, gap=REQUEST_GAP)
    conn = {"db": None}
    h = None
    try:
        if expired(deadline):
            raise Deadline()
        h = Host(name, deadline)
        s["robots_ok"] = h.robots_ok
        if not h.robots_ok:
            log.warning("%s: robots.txt unavailable, skipping host this run", name)
            return
        entries, complete = h.sitemap()
        listed = {u: lm for u, lm in entries
                  if len(u) <= MAX_URL and looks_like_person_url(u, idx) and h.robots.can_fetch(UA, u)}
        s.update(listed=len(listed), sitemap_complete=complete, capped=len(listed) > max_pages)
        conn["db"] = db_conn()
        with conn["db"].cursor() as c:
            known = load_known(c, name, list(listed))
            # pages whose CURRENT version already has a citation snapshot (step 4)
            c.execute("""SELECT w.url FROM pubsource_web_page w JOIN pubsource_web_page_snapshot s
                         ON s.url = w.url AND s.content_hash = w.content_hash WHERE w.host = %s""", (name,))
            snapped = {r["url"] for r in c.fetchall()}
        now = utcnow()

        def write(fn):
            # One writer at a time across host threads (concurrent page/page_person writes from different
            # hosts deadlocked on gap locks). Each page's writes commit together or not at all.
            with DB_WRITE:
                try:
                    conn["db"].ping()
                except Exception:  # dropped by wait_timeout between slow pages: open a fresh one
                    try:
                        conn["db"].close()
                    except Exception:
                        pass
                    conn["db"] = db_conn()
                db = conn["db"]
                with db.cursor() as c:
                    try:
                        fn(c)
                        db.commit()
                    except Exception:
                        db.rollback()
                        raise

        # Absence counts only against a COMPLETE sitemap (a failed or truncated child sitemap
        # would otherwise retire live pages), at most once per weekly run.
        if complete and listed:
            for u, r in known.items():
                if r["host"] == name and u not in listed and r["gone_at"] is None:
                    m = count_missing(r, now)
                    if m:
                        write(lambda c, u=u, m=m: c.execute(
                            "UPDATE pubsource_web_page SET missing_runs=%s, last_missing_at=%s, gone_at=IF(%s,%s,NULL) WHERE url=%s",
                            (m[0], now, m[1], now, u)))
                        s["gone"] += m[1]
            back = [u for u in listed if u in known and known[u]["missing_runs"]]
            for i in range(0, len(back), 500):
                part = back[i:i + 500]
                write(lambda c, part=part: c.execute(
                    f"UPDATE pubsource_web_page SET missing_runs=0, last_missing_at=NULL WHERE url IN ({','.join(['%s'] * len(part))})", part))

        # people-section URLs first, name-slug guesses after, so a cap never spends its budget on guesses
        order = sorted(listed, key=lambda u: not PEOPLE_PATH.search(u))
        for u in order[:max_pages]:
            if expired(deadline):
                s["stopped_at_deadline"] = True
                break
            try:
                crawl_page(h, write, u, listed[u], known.get(u), name, aliases, directory, now, s, snapped)
            except Deadline:
                s["stopped_at_deadline"] = True
                break
            except HostThrottled as e:
                s["throttled_stop"] = True
                log.warning("%s", e)
                break
            except Exception as e:  # one bad page must not fail the host every week
                s["page_errors"] += 1
                log.warning("%s: %s", u, e)
    except Deadline:
        s["stopped_at_deadline"] = True
    except HostThrottled as e:
        s["throttled_stop"] = True
        log.warning("%s", e)
    except Exception:
        log.exception("host %s failed", name)
        s["failed"] = True
    finally:
        if h:
            s.update(throttle_events=h.throttle_events, gap=h.gap)
        if conn["db"]:
            conn["db"].close()
        with lock:
            stats.append(s)
        log.info("%s", s)


def wants_snapshot(emails, doi_count, year_count, aliases, directory):
    """A page worth keeping citation blocks for: it shows pubs and names at least one roster person."""
    return webPageCitations.has_pub_signal(doi_count, year_count) and bool(link_people(emails, aliases, directory))


def save_snapshot(c, url, content_hash, html, text, now):
    c.execute("""INSERT INTO pubsource_web_page_snapshot (url, content_hash, main_text, blocks_json, captured_at)
                 VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE url=url""",
              (url, content_hash, text, json.dumps(webPageCitations.citation_blocks(html)), now))


def crawl_page(h, write, u, lm, row, host, aliases, directory, now, s, snapped=frozenset()):
    # A person page with pubs that predates step 4 has no snapshot: fetch it once, unconditionally.
    backfill = bool(row and row["content_hash"] and u not in snapped and wants_snapshot(
        json.loads(row["emails_json"] or "[]"), row["doi_count"], row["year_count"], aliases, directory))
    if not backfill and not needs_fetch(row, lm, now):
        s["skipped"] += 1
        # links follow the roster even when the page is untouched; no fetch needed
        write(lambda c: relink(c, u, json.loads(row["emails_json"] or "[]"), row["content_hash"], aliases, now, directory))
        s["relinked"] += 1
        return
    cond = {}
    if row and row["http_status"] in (200, 304) and row["content_hash"] and not backfill:
        if row["etag"]:
            cond["If-None-Match"] = row["etag"]
        if row["last_modified"]:
            cond["If-Modified-Since"] = row["last_modified"]
    try:
        status, _, hdr, html = h.get(u, cond)
    except (Deadline, HostThrottled):
        raise
    except Exception as e:
        s["errors"] += 1
        write(lambda c: c.execute(
            """INSERT INTO pubsource_web_page (url, host, sitemap_lastmod, first_seen, last_attempt)
               VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE last_attempt=VALUES(last_attempt)""",
            (u, host, lm, now, now)))
        log.info("%s: %s", u, e)
        return
    s["fetched"] += 1
    if status == 304:
        s["not_modified"] += 1

        def touch(c):
            c.execute("UPDATE pubsource_web_page SET last_attempt=%s, last_fetched=%s, http_status=304, gone_at=NULL, sitemap_lastmod=%s WHERE url=%s",
                      (now, now, lm, u))
            relink(c, u, json.loads(row["emails_json"] or "[]"), row["content_hash"], aliases, now, directory)
        write(touch)
        return
    if status != 200:
        s["errors"] += 1
        gone = status in (404, 410)

        def err(c):
            # last_fetched untouched: needs_fetch retries non-200 next run
            c.execute("""INSERT INTO pubsource_web_page (url, host, sitemap_lastmod, http_status, first_seen, last_attempt, gone_at)
                         VALUES (%s,%s,%s,%s,%s,%s,IF(%s,%s,NULL))
                         ON DUPLICATE KEY UPDATE http_status=VALUES(http_status), last_attempt=VALUES(last_attempt),
                          gone_at=IF(%s, COALESCE(gone_at, %s), gone_at)""",
                      (u, host, lm, status, now, now, gone, now, gone, now))
            if gone:
                c.execute("UPDATE pubsource_web_page_person SET removed_at=%s WHERE url=%s AND removed_at IS NULL", (now, u))
        write(err)
        s["gone"] += gone
        return
    sig = page_signals(html)
    snap = wants_snapshot(sig["emails"], sig["dois"], sig["years"], aliases, directory) and u not in snapped
    etag = hdr.get("ETag") or hdr.get("Etag")
    lmod = hdr.get("Last-Modified")
    if row and row["content_hash"] == sig["hash"]:
        s["unchanged"] += 1

        def same(c):
            # text unchanged, but markup-only signals (DOI hrefs, mailto) can change: keep them current,
            # or a stale "wants snapshot" forces an unconditional refetch every run
            c.execute("""UPDATE pubsource_web_page SET last_attempt=%s, last_fetched=%s, http_status=200, etag=%s, last_modified=%s,
                         sitemap_lastmod=%s, gone_at=NULL, emails_json=%s, links_json=%s, doi_count=%s, year_count=%s
                         WHERE url=%s""", (now, now, etag, lmod, lm, json.dumps(sig["emails"]), json.dumps(sig["links"]),
                                          sig["dois"], sig["years"], u))
            relink(c, u, sig["emails"], sig["hash"], aliases, now, directory)
            if snap:
                save_snapshot(c, u, sig["hash"], html, main_text(html), now)
        write(same)
        s["snapshots"] = s.get("snapshots", 0) + snap
        return
    s["changed" if row and row["content_hash"] else "new"] += 1

    def upsert(c):
        c.execute("""INSERT INTO pubsource_web_page
            (url, host, kind, sitemap_lastmod, etag, last_modified, content_hash, http_status, emails_json, links_json,
             doi_count, year_count, first_seen, last_attempt, last_fetched, last_changed, gone_at)
            VALUES (%s,%s,'profile',%s,%s,%s,%s,200,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            ON DUPLICATE KEY UPDATE sitemap_lastmod=VALUES(sitemap_lastmod), etag=VALUES(etag), last_modified=VALUES(last_modified),
             content_hash=VALUES(content_hash), http_status=200, emails_json=VALUES(emails_json), links_json=VALUES(links_json),
             doi_count=VALUES(doi_count), year_count=VALUES(year_count), last_attempt=VALUES(last_attempt),
             last_fetched=VALUES(last_fetched), last_changed=VALUES(last_changed), gone_at=NULL""",
                  (u, host, lm, etag, lmod, sig["hash"], json.dumps(sig["emails"]), json.dumps(sig["links"]),
                   sig["dois"], sig["years"], now, now, now, now))
        relink(c, u, sig["emails"], sig["hash"], aliases, now, directory)  # same transaction as the page row
        if snap:
            save_snapshot(c, u, sig["hash"], html, main_text(html), now)
    write(upsert)
    s["snapshots"] = s.get("snapshots", 0) + snap


REPORT_SQL = """
SELECT
  COUNT(*) AS active_professors,
  SUM(p.netid IS NOT NULL) AS page_found,
  SUM(p.pubs = 1) AS with_pubs_on_page
FROM identity_cornell_current r
LEFT JOIN (
  SELECT pp.netid, MAX(w.doi_count >= 2 OR w.year_count >= 8) AS pubs
  FROM pubsource_web_page_person pp JOIN pubsource_web_page w ON w.url = pp.url
  WHERE pp.removed_at IS NULL AND pp.is_sole_person = 1 AND w.gone_at IS NULL
  GROUP BY pp.netid) p ON p.netid = r.netid COLLATE utf8mb4_unicode_ci
WHERE r.is_faculty = 1
  AND COALESCE(r.primary_job_title, r.primary_business_title) LIKE '%Prof%'
  AND COALESCE(r.primary_job_title, r.primary_business_title) NOT LIKE '%Emerit%'
  AND IFNULL(r.affiliations_all, '') NOT LIKE '%emerit%'
"""


def directory_pass(roster, aliases, now):
    """Resolve page addresses the roster can't, via the Cornell directory, cache every answer
    (misses too) in pubsource_web_email_alias, and relink the pages that gained a link.
    Off (logged, not an error) when the directory bind secret is absent."""
    dn, pw = os.environ.get("CORNELL_ITHACA_ED_DN"), os.environ.get("CORNELL_ITHACA_ED_PASS")
    if not (dn and pw):
        log.info("directory pass: CORNELL_ITHACA_ED_DN/PASS unset, skipped")
        return {"directory": "off"}
    from ldap3 import Connection, Server, SUBTREE
    db = db_conn()
    try:
        with db.cursor() as c:
            c.execute("SELECT url, emails_json, content_hash FROM pubsource_web_page WHERE gone_at IS NULL AND emails_json IS NOT NULL")
            pages = c.fetchall()
            c.execute("SELECT email FROM pubsource_web_email_alias WHERE resolved_at >= %s", (now - ALIAS_TTL,))
            fresh = {r["email"] for r in c.fetchall()}
        wanted = sorted({e for p in pages for e in json.loads(p["emails_json"] or "[]")
                         if e not in aliases and e not in fresh and e.split("@", 1)[-1] not in SKIP_EMAIL_DOMAINS})
        conn = Connection(Server("query.directory.cornell.edu", port=636, use_ssl=True, get_info=None),
                          user=dn, password=pw, auto_bind=True)
        answered, found = 0, {}
        try:
            for i in range(0, len(wanted), LDAP_BATCH):
                batch = wanted[i:i + LDAP_BATCH]
                conn.search("ou=People,o=Cornell University,c=us", ldap_filter(batch), SUBTREE,
                            attributes=["uid", "mail", "cornelledupublishedemail", "mailAlternateAddress"], time_limit=60)
                if (conn.result or {}).get("description") != "success":
                    log.warning("directory pass: batch %d failed (%s); not cached", i // LDAP_BATCH, conn.result)
                    continue
                entries = []
                for e in conn.entries:
                    d = e.entry_attributes_as_dict
                    uid = str((d.get("uid") or [""])[0]).strip().lower()
                    addrs = {str(v).strip().lower() for a in ("mail", "cornelledupublishedemail", "mailAlternateAddress")
                             for v in (d.get(a) or [])}
                    if uid:
                        entries.append((uid, addrs))
                hit = match_directory_entries(set(batch), entries)
                found.update(hit)
                answered += len(batch)
                with db.cursor() as c:
                    c.executemany("""INSERT INTO pubsource_web_email_alias (email, netid, source, resolved_at) VALUES (%s,%s,'ldap',%s)
                                     ON DUPLICATE KEY UPDATE netid=VALUES(netid), resolved_at=VALUES(resolved_at)""",
                                  [(e, hit.get(e), now) for e in batch])
                db.commit()
        finally:
            conn.unbind()
        with db.cursor() as c:
            directory = load_directory(c, roster)
        new_links = {e for e, n in found.items() if n in roster}
        relinked = 0
        for p in pages:
            emails = json.loads(p["emails_json"] or "[]")
            if new_links.intersection(emails):
                with db.cursor() as c:
                    relink(c, p["url"], emails, p["content_hash"], aliases, now, directory)
                db.commit()
                relinked += 1
        out = {"directory_asked": len(wanted), "directory_answered": answered,
               "directory_resolved": len(found), "directory_to_roster": len(new_links), "directory_relinked_pages": relinked}
        log.info("directory pass: %s", out)
        return out
    except Exception:
        log.exception("directory pass failed; crawl results stand")
        return {"directory": "failed"}
    finally:
        db.close()


def close_run(run_id, status, stats_json=None):
    """Always on a FRESH connection: the startup connection sits idle for hours and is dropped by wait_timeout."""
    db = db_conn()
    try:
        with db.cursor() as c:
            c.execute("UPDATE pubsource_web_run SET finished_at=%s, status=%s, stats_json=COALESCE(%s, stats_json) WHERE run_id=%s",
                      (utcnow(), status, stats_json, run_id))
            c.execute(REPORT_SQL)
            cov = c.fetchone()
        db.commit()
        return cov
    finally:
        db.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hosts", nargs="*", help="limit to these hosts (default: web_seed_hosts.txt)")
    ap.add_argument("--max-pages", type=int, default=MAX_PER_HOST, help="per-host page cap")
    ap.add_argument("--report", action="store_true", help="print coverage of active Ithaca professors and exit")
    a = ap.parse_args()

    db = db_conn()
    try:
        with db.cursor() as c:
            for ddl in DDL + webPageCitations.DDL:
                c.execute(ddl)
            if a.report:
                c.execute(REPORT_SQL)
                log.info("coverage: %s", c.fetchone())
                return 0
            # A run killed hard (activeDeadline, OOM) never closed its row.
            c.execute("UPDATE pubsource_web_run SET status='abandoned', finished_at=%s WHERE status='running'", (utcnow(),))
            c.execute("INSERT INTO pubsource_web_run (started_at, status) VALUES (%s, 'running')", (utcnow(),))
            run_id = c.lastrowid
        db.commit()
        hosts = list(dict.fromkeys(h.strip().lower() for h in (a.hosts or open(HOSTS_FILE))
                                   if h.strip() and not h.startswith("#")))
        aliases, idx, roster = load_roster(db)
        with db.cursor() as c:
            directory = load_directory(c, roster)
            # Rows written by another host name's crawl before own-host filtering: give each its own host.
            c.execute("""UPDATE pubsource_web_page SET host = LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', 3), '/', -1))
                         WHERE host <> LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', 3), '/', -1))""")
        db.commit()
    finally:
        db.close()
    log.info("run %s: roster %d addresses, %d directory aliases, %d surnames; %d hosts",
             run_id, len(aliases), len(directory), len(idx), len(hosts))

    # SIGTERM (pod eviction, activeDeadline): stop every thread at its next request, let the pool
    # drain (a request is bounded by FETCH_BUDGET), then the finally below closes the run row.
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    deadline = time.monotonic() + MAX_SECONDS
    stats, lock, status, dir_stats = [], threading.Lock(), "failed", {}
    try:
        with ThreadPoolExecutor(HOST_WORKERS) as ex:
            list(ex.map(lambda h: crawl_host(h, aliases, directory, idx, deadline, a.max_pages, stats, lock), hosts))
        if STOP.is_set():
            status = "stopped"
            raise SystemExit(143)
        dir_stats = directory_pass(roster, aliases, utcnow()) if not expired(deadline) else {"directory": "skipped_deadline"}
        if os.environ.get("WEB_SEEDS_CITATIONS", "").lower() == "on" and not expired(deadline):
            cit = webPageCitations.run_pass(db_conn, lambda: expired(deadline), utcnow())
            log.info("citation pass: %s", cit)
            dir_stats.update({"citations_" + k: v for k, v in cit.items()})
        incomplete = any(s.get("failed") or s.get("stopped_at_deadline") or s.get("throttled_stop")
                         or s.get("robots_ok") is False for s in stats)
        status = "partial" if incomplete else "ok"
    finally:
        keys = ("listed", "fetched", "new", "changed", "unchanged", "not_modified", "skipped", "relinked",
                "errors", "page_errors", "gone", "snapshots")
        total = {k: sum(s.get(k, 0) for s in stats) for k in keys}
        total.update(hosts=len(hosts), hosts_done=len(stats), hosts_failed=sum(1 for s in stats if s.get("failed")),
                     hosts_robots_unavailable=sum(1 for s in stats if s.get("robots_ok") is False),
                     hosts_sitemap_incomplete=sum(1 for s in stats if s.get("sitemap_complete") is False),
                     hosts_capped=sum(1 for s in stats if s.get("capped")),
                     hosts_stopped_at_deadline=sum(1 for s in stats if s.get("stopped_at_deadline")),
                     hosts_throttled_stop=sum(1 for s in stats if s.get("throttled_stop")),
                     throttle_events=sum(s.get("throttle_events", 0) for s in stats), **dir_stats)
        cov = close_run(run_id, status, json.dumps({"total": total, "hosts": stats}, default=str))
        log.info("run %s %s: %s", run_id, status, total)
        log.info("coverage: %s", cov)
    # partial (deadline / a host failed) still exits 0: state is saved and the next run resumes.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
