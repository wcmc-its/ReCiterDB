#!/usr/bin/env python3
"""ETL: DynamoDB ExternalArticle -> reciterdb.external_record -> reciterdb.external_article.

Manually-added external-source publications (Publication Manager "Add publication" ->
OpenAlex / Scopus / Crossref, ReCiter #661/#662, and Authorships accepts) reach the
reporting DB here so they surface in reciterdb reporting (ReCiterDB #101).

Two steps (PM #1104, plan 3b-3e R2):

1. SYNC. Scan DynamoDB ExternalArticle and upsert it into `external_record`, the durable
   system of record (setup/table_external_record.sql, v3.6):
     - a (uid, article_id) not in external_record is INSERTed, with one IMPORT event;
     - an existing row is UPDATEd only while sync_origin='DYNAMO' and pm_touched=0, so
       a row Publication Manager has written is never overwritten (shadow mode);
     - a status change on an update writes a TRANSITION event (actor 'system');
     - nothing is ever deleted. On a full scan, an untouched DYNAMO row whose item is
       gone from DynamoDB (today's hard delete, e.g. an Authorships reopen) becomes
       REMOVED, guarded against an empty or suspiciously small scan.
   Status: suppressed + supersededByPmid -> SUPERSEDED, suppressed alone -> REJECTED,
   else ACCEPTED. target_author_index comes from the accepted/assigned authorship_review
   row (as STEP 6b reads it), else the surname match the byline uses.
   EXTERNAL_DYNAMO_SYNC=off (the cutover switch) skips the scan and the sync.

2. PROJECTION. Rebuild `external_article` (TRUNCATE + reload, that table only) from the
   synced records: every status but REMOVED, which is today's "deleted", with
   suppressed = (status <> 'ACCEPTED').
   For an untouched synced row the projection uses the DynamoDB values kept verbatim in
   fields_json.dynamo, so it is byte-for-byte what the DynamoDB-only load produced.
   Shadow-mode self-check: the rebuilt rows are compared with the DynamoDB-only rows
   from the same scan; while no row is PM-touched, any difference loads the DynamoDB-only
   rows instead (today's behaviour) and logs an ERROR.

Before the v3.6 DDL (no external_record table) it runs the old DynamoDB-only load.

Flags:
  --dry-run      scan, plan the sync and the projection, print counts and checksums;
                 write nothing
  --sync-only    sync external_record and stop (the cutover catch-up); runs the sync
                 even when EXTERNAL_DYNAMO_SYNC=off
  --since T      sync only items with dateAdded >= T (ISO, e.g. 2026-11-10T14:30);
                 disables the REMOVED pass. A full sync is as cheap (~41k items) and
                 also picks up suppressed flips on older items.
  --count-dup-markers  with --dry-run: count FeedbackLog "Not a duplicate of PMID"
                 notes (the 3e marker migration's size); a full read-only scan

Robust: a missing or empty ExternalArticle table loads 0 rows and exits 0 (the external
feature is not live on every env). Only a genuine MySQL/AWS error exits non-zero.

Env: DB_HOST/DB_USERNAME/DB_PASSWORD/DB_NAME (reciterdb), AWS creds + AWS_DEFAULT_REGION,
EXTERNAL_DYNAMO_SYNC (default on).
"""
import argparse
import hashlib
import os
import sys
import json
import logging
import re
import html
from datetime import datetime, timezone

import boto3
import pymysql
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("retrieveExternalArticles")

DDB_TABLE = "ExternalArticle"
MYSQL_TABLE = "external_article"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS `external_article` (
  `uid`                 VARCHAR(64)  NOT NULL,
  `article_id`          VARCHAR(96)  NOT NULL,
  `source_type`         ENUM('SCOPUS','WOS','OPENALEX','CROSSREF','MANUAL','ORCID') NOT NULL,
  `doi`                 VARCHAR(255) NULL,
  `pmid`                BIGINT       NULL,
  `title`               TEXT         NULL,
  `journal_or_venue`    VARCHAR(512) NULL,
  `authors`             TEXT         NULL,
  `pub_date`            VARCHAR(32)  NULL,
  `publication_type`    VARCHAR(64)  NULL,
  `added_by`            VARCHAR(64)  NULL,
  `date_added`          VARCHAR(32)  NULL,
  `method`              VARCHAR(64)  NULL,
  `suppressed`          TINYINT(1)   NOT NULL DEFAULT 0,
  `superseded_by_pmid`  BIGINT       NULL,
  `status`              ENUM('PENDING','ACCEPTED','REJECTED','REMOVED','MERGED','AUTOMERGED','SUPERSEDED') NULL,
  `isbn`                VARCHAR(128) NULL,
  `publisher`           VARCHAR(255) NULL,
  `editors`             TEXT         NULL,
  `byline`              VARCHAR(1000) NULL,
  `author_position`     VARCHAR(20)  NULL,
  PRIMARY KEY (`uid`, `article_id`),
  KEY `ix_source` (`source_type`),
  KEY `ix_doi` (`doi`),
  KEY `ix_suppressed` (`suppressed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""
# Keep this DDL in sync with setup/table_external_article.sql. COLLATE is load-bearing:
# without it utf8mb4 means general_ci, and STEP 6b's join against unicode_ci tables
# fails silently (6b swallows the error; the nightly still reports SUCCESS).

COLS = ["uid", "article_id", "source_type", "doi", "pmid", "title", "journal_or_venue",
        "authors", "pub_date", "publication_type", "added_by", "date_added", "method",
        "suppressed", "superseded_by_pmid",
        "status",   # v3.6 (alter_external_article_status_v3.6.sql); written once the column exists
        # v3.5 book/CV fields. Written only once the live table has them (see load()).
        "isbn", "publisher", "editors", "byline", "author_position"]


def _conn():
    return pymysql.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USERNAME"),
                           password=os.getenv("DB_PASSWORD"), db=os.getenv("DB_NAME"),
                           charset="utf8mb4")


def scan_external_articles():
    """Full paginated scan of the ExternalArticle table. Missing table -> []."""
    table = boto3.resource("dynamodb",
                           region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")).Table(DDB_TABLE)
    items, kwargs = [], {}
    try:
        while True:
            resp = table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.warning(f"{DDB_TABLE} DynamoDB table not found — loading 0 rows.")
            return []
        raise
    return items


def _to_int(v):
    return int(v) if v is not None else None   # DynamoDB numbers arrive as Decimal


def _clean(v):
    """Source text with HTML entities decoded ("&amp;" -> "&") and tags dropped."""
    if not isinstance(v, str):
        return v
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html.unescape(v))).strip() or None


def _raw(it):
    try:
        r = json.loads(it.get("rawRecord") or "{}")
    except ValueError:
        return {}
    return r if isinstance(r, dict) else {}


SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


def _split_name(n):
    """(surname, given) from a Crossref {given, family} dict or a "Given M. Surname" string.
    ponytail: the last token is taken as the surname, so a free-text "Anna van der Berg" gives
    "Berg"; Crossref names are structured and unaffected."""
    if isinstance(n, dict):
        return (n.get("family") or n.get("name") or "").strip(), (n.get("given") or "").strip()
    toks = [t for t in str(n or "").replace(",", " ").split() if t.strip(".").lower() not in SUFFIXES]
    return (toks[-1], " ".join(toks[:-1])) if toks else ("", "")


def _short(surname, given):
    """PubMed-style "Surname GA"."""
    initials = "".join(w[0].upper() for w in re.split(r"[\s.\-]+", given) if w and w[0].isalpha())
    return f"{surname} {initials}".strip()


def byline_and_position(names, last_name):
    """("Surname GA, ((Koretzky GA)), ...", 'first'|'last'|None). The person is wrapped in
    ((..)) like PubMed rows, found by surname; None/None when there are no names."""
    parts = [_split_name(n) for n in names or []]
    parts = [p for p in parts if p[0]]
    if not parts:
        return None, None
    target = (last_name or "").strip().lower()
    hit = next((i for i, (sur, _) in enumerate(parts) if target and sur.lower() == target), None)
    shorts = [f"(({_short(*p)}))" if i == hit else _short(*p) for i, p in enumerate(parts)]
    pos = None if hit is None else "first" if hit == 0 else "last" if hit == len(parts) - 1 else None
    return _fit(shorts, hit), pos


BYLINE_MAX = 1000   # external_article.byline / temp_external_position.authors


def _fit(shorts, hit):
    """Join names, cutting at a name boundary with "et al." when over BYLINE_MAX; a cut
    keeps the person's own ((name)) so Scholars can still highlight it."""
    out = ", ".join(shorts)
    if len(out) <= BYLINE_MAX:
        return out
    tail = f", ..., {shorts[hit]}, et al." if hit is not None else ", et al."
    keep = []
    for i, n in enumerate(shorts):
        if i == hit or len(", ".join(keep + [n])) + len(tail) > BYLINE_MAX:
            break
        keep.append(n)
    if hit is not None and hit == len(keep):
        tail = f", {shorts[hit]}, et al."   # person is next in line: no gap to mark
    return ", ".join(keep) + tail


def to_row(it, last_names=None, review_isbns=None):
    authors = it.get("authors")
    raw = _raw(it)
    # Crossref's structured author list beats the flat strings; editors only come from there.
    last_name = (last_names or {}).get(it.get("uid"))
    byline, position = byline_and_position(raw.get("author") or authors, last_name)
    if byline is None and raw.get("editor"):
        # An edited book listing only editors: cite them Vancouver-style, "Smith A, Lee B, editors".
        byline, position = byline_and_position(raw["editor"], last_name)
        byline = f"{byline}, editors" if len(byline) <= BYLINE_MAX - 9 else byline
    editors = [" ".join(_split_name(e)[::-1]).strip() for e in raw.get("editor") or []]
    isbn = ",".join(raw.get("ISBN") or []) or (review_isbns or {}).get(it.get("articleId"))
    return {
        "uid": it.get("uid"),
        "article_id": it.get("articleId"),
        "source_type": it.get("sourceType"),
        "doi": it.get("doi"),
        "pmid": _to_int(it.get("pmid")),
        "title": _clean(it.get("title")),
        "journal_or_venue": _clean(it.get("journalOrVenue")),
        "authors": json.dumps(authors) if authors is not None else None,
        "pub_date": it.get("pubDate"),
        "publication_type": it.get("publicationType"),
        "added_by": it.get("addedBy"),
        "date_added": it.get("dateAdded"),
        "method": it.get("method"),
        "suppressed": 1 if it.get("suppressed") else 0,
        "superseded_by_pmid": _to_int(it.get("supersededByPmid")),
        "isbn": (isbn or None) and isbn[:128],
        "publisher": (_clean(raw.get("publisher")) or None) and _clean(raw.get("publisher"))[:255],
        "editors": json.dumps(editors) if editors else None,
        "byline": byline,
        "author_position": position,
    }


ARTICLE_ID_MAX = 96   # external_article.article_id VARCHAR(96)


def loadable(rows, sources):
    """Rows the table can hold. A sourceType the live ENUM lacks (e.g. CROSSREF before
    alter_external_article_add_crossref_v3.4.sql is applied) or an over-long id would fail
    the whole batch under strict SQL mode AFTER the TRUNCATE (an implicit commit), leaving
    the table empty and dropping every external pub from STEP 6b. Skip and log those."""
    keep, skipped = [], []
    for r in rows:
        ok = (r["source_type"] in sources and r["article_id"]
              and len(r["article_id"]) <= ARTICLE_ID_MAX)
        (keep if ok else skipped).append(r)
    for r in skipped[:20]:
        logger.warning(f"skipping unloadable ExternalArticle {r['uid']} {r['article_id']!r} "
                       f"sourceType={r['source_type']!r}")
    if skipped:
        logger.warning(f"skipped {len(skipped)} ExternalArticle rows (allowed sources: {sorted(sources)})")
    return keep


def _enum_members(c, table=MYSQL_TABLE):
    c.execute("SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() "
              "AND table_name = %s AND column_name = 'source_type'", (table,))
    return set(re.findall(r"'([^']*)'", c.fetchone()[0]))


def _live_columns(c, table=MYSQL_TABLE):
    c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() "
              "AND table_name = %s", (table,))
    return {r[0] for r in c.fetchall()}


def lookups():
    """(uid -> person.lastName, 'SCOPUS:<id>' -> authorship_review.isbn). Both optional: a
    missing table gives {} so the load itself never fails on them."""
    last_names, isbns = {}, {}
    conn = _conn()
    try:
        with conn.cursor() as c:
            for sql, target, key in (
                ("SELECT personIdentifier, lastName FROM person WHERE lastName IS NOT NULL", last_names, None),
                ("SELECT external_id, MAX(isbn) FROM authorship_review WHERE source = 'scopus' "
                 "AND isbn IS NOT NULL AND isbn <> '' GROUP BY external_id", isbns, "SCOPUS:"),
            ):
                try:
                    c.execute(sql)
                    target.update(((key or "") + str(k), v) for k, v in c.fetchall())
                except pymysql.err.ProgrammingError as e:
                    logger.warning(f"lookup skipped ({e})")
    finally:
        conn.close()
    return last_names, isbns


def load(rows):
    conn = _conn()
    try:
        with conn.cursor() as c:
            c.execute(CREATE_SQL)
            rows = loadable(rows, _enum_members(c))
            for r in rows:
                r.setdefault("status", legacy_status(r))
            # A column the live table lacks (v3.5 not yet applied) is left out, never sent:
            # a failed INSERT after the TRUNCATE would empty the table.
            live = _live_columns(c)
            cols = [col for col in COLS if col in live]
            if len(cols) < len(COLS):
                logger.warning(f"{MYSQL_TABLE} lacks {sorted(set(COLS) - live)}; apply "
                               "setup/alter_external_article_add_book_fields_v3.5.sql / "
                               "alter_external_article_status_v3.6.sql")
            c.execute(f"TRUNCATE TABLE `{MYSQL_TABLE}`")
            if rows:
                collist = ", ".join(f"`{col}`" for col in cols)
                placeholders = ", ".join(f"%({col})s" for col in cols)
                sql = f"INSERT INTO `{MYSQL_TABLE}` ({collist}) VALUES ({placeholders})"
                for i in range(0, len(rows), 500):
                    c.executemany(sql, rows[i:i + 500])
        conn.commit()
    finally:
        conn.close()
    return rows


def legacy_status(row):
    """external_record status for a DynamoDB item, from its suppressed flag (plan backfill)."""
    if not row.get("suppressed"):
        return "ACCEPTED"
    return "SUPERSEDED" if row.get("superseded_by_pmid") is not None else "REJECTED"


# ------------------- external_record sync + projection (v3.6, plan R2) -------------------
ER_TABLE = "external_record"
EVENT_TABLE = "external_record_event"
SYSTEM_ACTOR = "system"

PROVENANCE_BY_METHOD = {
    "scopus-authorships-tab": "AUTHORSHIPS",
    "openalex-authorships-tab": "AUTHORSHIPS",
    "dropdown-search": "ADD_SEARCH",
    "api-isbn": "ADD_SEARCH",
}
# Any other method (prod: the one 'openalex-verification-test' row, plan decision 10)
# gets provenance NULL and keeps its method verbatim; the dry run lists them.

# Columns the sync owns. pub_type, type_notice, peer_reviewed, role, merges and the PM-only
# fields are left alone (PM, and scripts/backfill-external-type.mjs in plan P3).
SYNC_COLS = ["uid", "article_id", "source_type", "status", "prev_status", "status_by", "status_at",
             "added_by", "added_at", "provenance", "method", "superseded_by_pmid", "pmid", "doi",
             "isbn", "scopus_eid", "openalex_id", "title", "title_year_key", "source_pub_type",
             "pub_date", "date_precision", "container_title", "publisher", "fields_json",
             "authors_json", "target_author_index", "author_count", "byline", "raw_record",
             "sync_origin"]
# A difference in any of these makes an untouched row worth an UPDATE. status_by/at and
# prev_status move only with a status change.
COMPARE_COLS = [c for c in SYNC_COLS if c not in ("prev_status", "status_by", "status_at", "sync_origin")]
JSON_COLS = {"fields_json", "authors_json"}
REMOVE_MAX_ABS, REMOVE_MAX_FRAC = 50, 0.05   # REMOVED-pass guard: more than this is a bad scan


def _parse_dt(s):
    """DynamoDB dateAdded ("2026-10-20T01:34:24.368715376Z") -> naive UTC datetime, ms precision."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})(?:[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?)?", str(s or ""))
    if not m:
        return None
    try:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2) or '00:00:00'}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return dt.replace(microsecond=int((m.group(3) or "0")[:3].ljust(3, "0")) * 1000)


def _fmt_dt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z" if dt else None


def date_precision(pub_date):
    d = str(pub_date or "")
    return ("DAY" if re.match(r"^\d{4}-\d{2}-\d{2}", d) else "MONTH" if re.match(r"^\d{4}-\d{2}$", d)
            else "YEAR" if re.match(r"^\d{4}$", d) else None)


def norm_title(x):
    """PM src/lib/pubTypes.ts normTitle, ported: lower-case, non [a-z0-9 ] -> space, squeeze."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", str(x or "").lower())).strip()


def title_year_key(title, pub_date):
    """SHA-1 hex of "<normTitle(title)>|<year>" (year = first 4 chars of the date, as PM's
    yearOf); NULL without a title or a year. PM's writer must build it the same way."""
    t, y = norm_title(title), str(pub_date or "")[:4].strip()
    return hashlib.sha1(f"{t}|{y}".encode("utf-8")).hexdigest() if t and y else None


def _position(index, count):
    """'first'/'last'/None, external_article.author_position's convention, from a 1-based index."""
    if not index:
        return None
    return "first" if index == 1 else "last" if count and index == count else None


def review_positions():
    """(uid, ARTICLE_ID) lower-cased -> (author_position, author count) from the accepted or
    assigned authorship_review row, the earliest slot when one person holds two (as STEP 6b).
    A missing table gives {}."""
    out = {}
    conn = _conn()
    try:
        with conn.cursor() as c:
            try:
                c.execute("SELECT resolution_cwid, source, external_id, author_position, "
                          "IF(JSON_VALID(authors_json), JSON_LENGTH(authors_json), NULL) "
                          "FROM authorship_review WHERE source IN ('scopus', 'openalex') "
                          "AND status IN ('accepted', 'assigned') AND resolution_cwid IS NOT NULL "
                          "AND author_position IS NOT NULL")
            except pymysql.err.ProgrammingError as e:
                logger.warning(f"authorship_review lookup skipped ({e})")
                return out
            for uid, source, ext_id, pos, n in c.fetchall():
                k = (str(uid).lower(), f"{source}:{ext_id}".lower())
                if k not in out or pos < out[k][0]:
                    out[k] = (int(pos), int(n) if n else None)
    finally:
        conn.close()
    return out


def _key(uid, article_id):
    # The unique key is utf8mb4_unicode_ci (case-insensitive); ids are ASCII.
    return (str(uid or "").lower(), str(article_id or "").lower())


def to_record(it, row, positions=None, last_names=None):
    """external_record row for one DynamoDB item, built from its legacy projection row
    (to_row) so the projection can be rebuilt from it exactly. Values the record has no
    column for (the verbatim authors text, the nanosecond dateAdded, the byline's
    first/last) are kept in fields_json.dynamo."""
    raw = _raw(it)
    parts = [p for p in (_split_name(n) for n in (raw.get("author") or it.get("authors") or [])) if p[0]]
    target = ((last_names or {}).get(row["uid"]) or "").strip().lower()
    hit = next((i for i, (sur, _) in enumerate(parts) if target and sur.lower() == target), None)
    review = (positions or {}).get(_key(row["uid"], row["article_id"]))
    if review:
        index, count = review[0], review[1] or len(parts) or None
    else:
        index, count = (hit + 1 if hit is not None else None), len(parts) or None
    fields = {}
    if row.get("editors"):
        fields["editors"] = json.loads(row["editors"])
    fields["dynamo"] = {"authors": row["authors"], "date_added": row["date_added"],
                        "author_position": row["author_position"]}
    source, _, sid = str(row["article_id"] or "").partition(":")
    added_at = _parse_dt(row["date_added"])
    return {
        "uid": row["uid"], "article_id": row["article_id"], "source_type": row["source_type"],
        "status": legacy_status(row), "prev_status": None,
        "status_by": row["added_by"], "status_at": added_at,
        "added_by": row["added_by"], "added_at": added_at,
        "provenance": PROVENANCE_BY_METHOD.get(row["method"]), "method": row["method"],
        "superseded_by_pmid": row["superseded_by_pmid"], "pmid": row["pmid"], "doi": row["doi"],
        "isbn": row["isbn"],
        "scopus_eid": sid[:64] if source == "SCOPUS" and sid else None,
        "openalex_id": sid[:64] if source == "OPENALEX" and sid else None,
        "title": row["title"], "title_year_key": title_year_key(row["title"], row["pub_date"]),
        "source_pub_type": row["publication_type"], "pub_date": row["pub_date"],
        "date_precision": date_precision(row["pub_date"]),
        "container_title": row["journal_or_venue"], "publisher": row["publisher"],
        "fields_json": fields,
        "authors_json": [{"given": g, "surname": s} for s, g in parts] or None,
        "target_author_index": index, "author_count": count,
        "byline": row["byline"], "raw_record": it.get("rawRecord"),
        "sync_origin": "DYNAMO", "pm_touched": 0,
    }


def to_projection(rec):
    """external_article row for one external_record row. An untouched synced row takes the
    verbatim DynamoDB values from fields_json.dynamo; a PM-written or PM-edited row derives
    them from its columns."""
    f = rec.get("fields_json") or {}
    dyn = f.get("dynamo") if rec.get("sync_origin") == "DYNAMO" and not rec.get("pm_touched") else None
    if isinstance(dyn, dict):
        authors, date_added, position = dyn.get("authors"), dyn.get("date_added"), dyn.get("author_position")
    else:
        names = [" ".join(x for x in (a.get("given"), a.get("surname")) if x)
                 for a in rec.get("authors_json") or [] if isinstance(a, dict)]
        authors = json.dumps(names) if names else None
        date_added = _fmt_dt(rec.get("added_at"))
        position = _position(rec.get("target_author_index"), rec.get("author_count"))
    editors = f.get("editors")
    return {
        "uid": rec["uid"], "article_id": rec["article_id"], "source_type": rec["source_type"],
        "doi": rec.get("doi"), "pmid": rec.get("pmid"), "title": rec.get("title"),
        "journal_or_venue": rec.get("container_title"), "authors": authors,
        "pub_date": rec.get("pub_date"), "publication_type": rec.get("source_pub_type"),
        "added_by": rec.get("added_by"), "date_added": date_added, "method": rec.get("method"),
        "suppressed": 0 if rec["status"] == "ACCEPTED" else 1,
        "superseded_by_pmid": rec.get("superseded_by_pmid"), "status": rec["status"],
        "isbn": rec.get("isbn"), "publisher": rec.get("publisher"),
        "editors": json.dumps(editors) if editors else None,
        "byline": rec.get("byline"), "author_position": position,
    }


def projection_rows(records):
    """REMOVED is today's hard delete (an Authorships reopen), so it stays out, as a deleted
    DynamoDB item does; every other status is loaded, flagged suppressed unless ACCEPTED."""
    return [to_projection(r) for r in records if r["status"] != "REMOVED"]


def _canon(v, col=None):
    if col in JSON_COLS and v is not None:
        return json.dumps(v if not isinstance(v, str) else json.loads(v), sort_keys=True)
    if isinstance(v, datetime):
        return v.replace(microsecond=v.microsecond // 1000 * 1000).isoformat()
    return None if v is None else str(v)


def plan_sync(existing, records, full_scan):
    """What the sync will do, without doing it. existing: external_record rows (dicts);
    records: to_record() rows. Returns inserts, updates [(old, new)], removals [old],
    skipped_touched, unchanged and dup_keys."""
    by_key = {_key(r["uid"], r["article_id"]): r for r in existing}
    seen, out = set(), {"inserts": [], "updates": [], "removals": [], "skipped_touched": 0,
                         "unchanged": 0, "dup_keys": 0}
    for rec in records:
        k = _key(rec["uid"], rec["article_id"])
        if k in seen:
            out["dup_keys"] += 1
            continue
        seen.add(k)
        old = by_key.get(k)
        if old is None:
            out["inserts"].append(rec)
        elif old.get("sync_origin") != "DYNAMO" or old.get("pm_touched"):
            out["skipped_touched"] += 1
        elif any(_canon(old.get(c), c) != _canon(rec.get(c), c) for c in COMPARE_COLS):
            out["updates"].append((old, rec))
        else:
            out["unchanged"] += 1
    if full_scan:
        out["removals"] = [r for k, r in by_key.items()
                           if k not in seen and r.get("sync_origin") == "DYNAMO"
                           and not r.get("pm_touched") and r["status"] != "REMOVED"]
    return out


def removal_guard(plan, existing, scanned):
    """Drop the REMOVED pass when the scan looks incomplete: an empty scan, or more rows
    vanishing than a day of reopens could explain. Returns a reason or None."""
    n = len(plan["removals"])
    if not n:
        return None
    synced = sum(1 for r in existing if r.get("sync_origin") == "DYNAMO")
    if not scanned:
        return f"scan returned 0 items but {synced} synced rows exist"
    if n > max(REMOVE_MAX_ABS, REMOVE_MAX_FRAC * synced):
        return f"{n} of {synced} synced rows would become REMOVED (limit {REMOVE_MAX_ABS} or {REMOVE_MAX_FRAC:.0%})"
    return None


def apply_plan(existing, plan, now):
    """The external_record rows after the plan, in memory (dry run; also what apply_sync writes)."""
    by_key = {_key(r["uid"], r["article_id"]): dict(r) for r in existing}
    for rec in plan["inserts"]:
        by_key[_key(rec["uid"], rec["article_id"])] = dict(rec)
    for old, rec in plan["updates"]:
        by_key[_key(rec["uid"], rec["article_id"])] = _updated(old, rec, now)
    for old in plan["removals"]:
        by_key[_key(old["uid"], old["article_id"])] = dict(old, status="REMOVED", prev_status=old["status"],
                                                          status_by=SYSTEM_ACTOR, status_at=now)
    return list(by_key.values())


def _updated(old, rec, now):
    new = dict(old, **{c: rec[c] for c in COMPARE_COLS})
    if old["status"] != rec["status"]:
        new.update(prev_status=old["status"], status_by=SYSTEM_ACTOR, status_at=now)
    return new


def _db_val(col, v):
    return json.dumps(v, ensure_ascii=False) if col in JSON_COLS and v is not None and not isinstance(v, str) else v


def _table_exists(c, table):
    c.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() "
              "AND table_name = %s", (table,))
    return c.fetchone()[0] > 0


def read_records(c):
    c.execute(f"SELECT * FROM `{ER_TABLE}`")
    cols = [d[0] for d in c.description]
    out = []
    for t in c.fetchall():
        r = dict(zip(cols, t))
        for col in JSON_COLS:
            if isinstance(r.get(col), str):
                try:
                    r[col] = json.loads(r[col])
                except ValueError:
                    pass
        out.append(r)
    return out


def apply_sync(conn, plan, now):
    """Write the plan in one transaction. Updates and removals re-check sync_origin and
    pm_touched in the WHERE, so a PM write that lands mid-run wins."""
    counts = {"inserted": 0, "updated": 0, "transitions": 0, "removed": 0}
    with conn.cursor() as c:
        if plan["inserts"]:
            cols = SYNC_COLS + ["pm_touched"]
            sql = (f"INSERT INTO `{ER_TABLE}` ({', '.join(f'`{x}`' for x in cols)}) "
                   f"VALUES ({', '.join(['%s'] * len(cols))})")
            vals = [[_db_val(x, r.get(x)) for x in cols] for r in plan["inserts"]]
            for i in range(0, len(vals), 500):
                c.executemany(sql, vals[i:i + 500])
            c.execute(f"SELECT id, uid, article_id FROM `{ER_TABLE}`")
            ids = {_key(u, a): i for i, u, a in c.fetchall()}
            ev = [(ids[_key(r["uid"], r["article_id"])], r["uid"], "IMPORT", None, r["status"],
                   SYSTEM_ACTOR, now, f"DynamoDB ExternalArticle sync (method {r['method']})")
                  for r in plan["inserts"]]
            _events(c, ev)
            counts["inserted"] = len(plan["inserts"])
        guard = " AND sync_origin = 'DYNAMO' AND pm_touched = 0"
        ev = []
        for old, rec in plan["updates"]:
            new = _updated(old, rec, now)
            cols = COMPARE_COLS + ["prev_status", "status_by", "status_at"]
            c.execute(f"UPDATE `{ER_TABLE}` SET {', '.join(f'`{x}` = %s' for x in cols)} "
                      f"WHERE id = %s{guard}", [_db_val(x, new.get(x)) for x in cols] + [old["id"]])
            if c.rowcount:
                counts["updated"] += 1
                if old["status"] != new["status"]:
                    counts["transitions"] += 1
                    ev.append((old["id"], old["uid"], "TRANSITION", old["status"], new["status"],
                               SYSTEM_ACTOR, now, "DynamoDB ExternalArticle sync: suppressed changed"))
        for old in plan["removals"]:
            c.execute(f"UPDATE `{ER_TABLE}` SET status = 'REMOVED', prev_status = %s, status_by = %s, "
                      f"status_at = %s WHERE id = %s AND status = %s{guard}",
                      (old["status"], SYSTEM_ACTOR, now, old["id"], old["status"]))
            if c.rowcount:
                counts["removed"] += 1
                ev.append((old["id"], old["uid"], "TRANSITION", old["status"], "REMOVED", SYSTEM_ACTOR,
                           now, "DynamoDB ExternalArticle sync: item no longer in DynamoDB"))
        _events(c, ev)
    conn.commit()
    return counts


def _events(c, rows):
    sql = (f"INSERT INTO `{EVENT_TABLE}` (record_id, uid, action, from_status, to_status, actor, at, note) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    for i in range(0, len(rows), 500):
        c.executemany(sql, rows[i:i + 500])


def checksum(rows, cols=COLS):
    """MD5 over the rows in (uid, article_id) order, every column, NULL as \\N."""
    lines = sorted("\t".join("\\N" if r.get(c) is None else str(r.get(c)) for c in cols) for r in rows)
    return hashlib.md5("\n".join(lines).encode("utf-8")).hexdigest()


def self_check(legacy, rebuilt, records):
    """Compare the rebuilt projection with the DynamoDB-only rows from the same scan, on
    the rows PM has not touched. Returns (ok, touched_count, sample of differences)."""
    touched = {_key(r["uid"], r["article_id"]) for r in records
               if r.get("sync_origin") != "DYNAMO" or r.get("pm_touched")}
    # Ids the load would skip anyway (no uid/id, over-long id) are not differences.
    leg = {_key(r["uid"], r["article_id"]): r for r in legacy
           if r["uid"] and r["article_id"] and len(r["article_id"]) <= ARTICLE_ID_MAX}
    new = {k: r for k, r in ((_key(r["uid"], r["article_id"]), r) for r in rebuilt) if k not in touched}
    for k in touched:
        leg.pop(k, None)
    diffs = [f"only in DynamoDB load: {k}" for k in leg.keys() - new.keys()]
    diffs += [f"only in rebuild: {k}" for k in new.keys() - leg.keys()]
    for k in leg.keys() & new.keys():
        bad = [c for c in COLS if _canon(leg[k].get(c)) != _canon(new[k].get(c))]
        if bad:
            diffs.append(f"{k}: {', '.join(bad)}")
    return not diffs, len(touched), diffs[:20]


def count_dup_markers():
    """Count FeedbackLog "Different papers" markers (PM 2h notes) for the 3e migration."""
    table = boto3.resource("dynamodb",
                           region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")).Table("FeedbackLog")
    from boto3.dynamodb.conditions import Attr
    counts = {}
    for label, prefix in (("not_dup", "Not a duplicate of PMID"), ("reopened", "Duplicate check reopened for PMID")):
        n, kwargs = 0, {"FilterExpression": Attr("note").begins_with(prefix), "Select": "COUNT"}
        while True:
            resp = table.scan(**kwargs)
            n += resp.get("Count", 0)
            if not resp.get("LastEvaluatedKey"):
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        counts[label] = n
    return counts


def _counter(values):
    out = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


def _sync_enabled(args):
    return args.sync_only or os.getenv("EXTERNAL_DYNAMO_SYNC", "on").strip().lower() not in ("off", "0", "false", "no")


def legacy_main(dry_run):
    """The DynamoDB-only load, used until external_record exists (v3.6 not applied)."""
    items = scan_external_articles()
    last_names, review_isbns = lookups()
    rows = [to_row(it, last_names, review_isbns) for it in items]
    logger.info(f"Scanned {len(items)} ExternalArticle items")
    if dry_run:
        logger.info(f"[dry-run] would load {len(rows)} rows into {MYSQL_TABLE}; checksum {checksum(rows)}")
        return
    rows = load(rows)
    suppressed = sum(r["suppressed"] for r in rows)
    logger.info(f"Loaded {len(rows)} rows into {MYSQL_TABLE} ({suppressed} suppressed)")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="plan and report; write nothing")
    ap.add_argument("--sync-only", action="store_true", help="sync external_record, skip the projection")
    ap.add_argument("--since", help="sync only items with dateAdded >= this ISO time; no REMOVED pass")
    ap.add_argument("--count-dup-markers", action="store_true",
                    help="with --dry-run: count FeedbackLog 'Different papers' markers")
    args = ap.parse_args(argv)
    t = datetime.now(timezone.utc).replace(tzinfo=None)
    now = t.replace(microsecond=t.microsecond // 1000 * 1000)

    conn = _conn()
    try:
        with conn.cursor() as c:
            has_er = _table_exists(c, ER_TABLE) and _table_exists(c, EVENT_TABLE)
    finally:
        conn.close()
    if not has_er:
        logger.warning(f"{ER_TABLE} absent: apply setup/alter_external_record_v3.6.sql. "
                       "Running the DynamoDB-only load.")
        if args.sync_only:
            raise SystemExit("--sync-only needs the external_record table")
        return legacy_main(args.dry_run)

    sync = _sync_enabled(args)
    items, legacy, records = [], [], []
    if sync:
        items = scan_external_articles()
        last_names, review_isbns = lookups()
        legacy = [to_row(it, last_names, review_isbns) for it in items]
        for r in legacy:
            r["status"] = legacy_status(r)
        positions = review_positions()
        records = [to_record(it, row, positions, last_names) for it, row in zip(items, legacy)]
        if args.since:
            records = [r for r in records if not r["fields_json"]["dynamo"]["date_added"]
                       or r["fields_json"]["dynamo"]["date_added"] >= args.since]
        logger.info(f"Scanned {len(items)} ExternalArticle items"
                    + (f"; {len(records)} with dateAdded >= {args.since}" if args.since else ""))
    else:
        logger.info("EXTERNAL_DYNAMO_SYNC=off: no DynamoDB scan; projection from external_record only")

    conn = _conn()
    try:
        with conn.cursor() as c:
            records = loadable(records, _enum_members(c, ER_TABLE)) if records else records
            existing = read_records(c)
            ea_sources = _enum_members(c, MYSQL_TABLE) if _table_exists(c, MYSQL_TABLE) else None
        plan = plan_sync(existing, records, full_scan=sync and not args.since)
        why = removal_guard(plan, existing, len(items))
        if why:
            logger.error(f"REMOVED pass skipped: {why}")
            plan["removals"] = []
        logger.info(f"sync plan: {len(plan['inserts'])} insert, {len(plan['updates'])} update "
                    f"({sum(o['status'] != r['status'] for o, r in plan['updates'])} status changes), "
                    f"{len(plan['removals'])} to REMOVED, {plan['skipped_touched']} PM-owned skipped, "
                    f"{plan['unchanged']} unchanged, {plan['dup_keys']} duplicate keys")

        if args.dry_run:
            final = apply_plan(existing, plan, now) if sync else existing
            _report(records, legacy, final, ea_sources, sync and not args.since)
            if args.count_dup_markers:
                logger.info(f"[dry-run] FeedbackLog duplicate markers: {count_dup_markers()}")
            return

        if sync:
            counts = apply_sync(conn, plan, now)
            logger.info(f"synced {ER_TABLE}: {counts}")
        if args.sync_only:
            return
        with conn.cursor() as c:
            final = read_records(c)
    finally:
        conn.close()

    rows = projection_rows(final)
    if sync and not args.since:
        ok, touched, diffs = self_check(legacy, rows, final)
        if ok:
            logger.info(f"self-check: rebuilt projection matches the DynamoDB-only load "
                        f"({touched} PM-owned rows excluded from the comparison)")
        elif touched == 0:
            logger.error(f"self-check FAILED with no PM-owned rows; loading the DynamoDB-only rows "
                         f"instead. First differences: {diffs}")
            rows = legacy
        else:
            logger.warning(f"self-check: {len(diffs)}+ differences on untouched rows: {diffs}")
    rows = load(rows)
    suppressed = sum(r["suppressed"] for r in rows)
    logger.info(f"Loaded {len(rows)} rows into {MYSQL_TABLE} ({suppressed} suppressed) "
                f"from {ER_TABLE}; status {_counter(r['status'] for r in rows)}")


def _report(records, legacy, final, ea_sources, full):
    rebuilt = projection_rows(final)
    if ea_sources is not None:
        rebuilt = [r for r in rebuilt if r["source_type"] in ea_sources]
        legacy = [r for r in legacy if r["source_type"] in ea_sources]
    logger.info(f"[dry-run] status {_counter(r['status'] for r in records)}")
    logger.info(f"[dry-run] provenance {_counter(r['provenance'] for r in records)}")
    logger.info(f"[dry-run] methods without a provenance "
                f"{_counter(r['method'] for r in records if not r['provenance'])}")
    logger.info(f"[dry-run] target_author_index set {sum(1 for r in records if r['target_author_index'])} "
                f"of {len(records)}")
    logger.info(f"[dry-run] projection: {len(rebuilt)} rows, checksum {checksum(rebuilt)}")
    if full:
        logger.info(f"[dry-run] DynamoDB-only load: {len(legacy)} rows, checksum {checksum(legacy)}")
        ok, touched, diffs = self_check(legacy, rebuilt, final)
        logger.info(f"[dry-run] self-check {'MATCH' if ok else 'DIFF'} "
                    f"({touched} PM-owned rows excluded){'' if ok else ': ' + str(diffs)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"retrieveExternalArticles failed: {e}")
        sys.exit(1)
