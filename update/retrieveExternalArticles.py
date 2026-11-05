#!/usr/bin/env python3
"""ETL: scan the ExternalArticle DynamoDB table -> reciterdb.external_article (MySQL).

Projects the manually-added external-source publications (Publication Manager
"Add publication" -> OpenAlex / Scopus / Crossref, ReCiter #661/#662) into the reporting DB so
external pubs surface in reciterdb reporting (ReCiterDB #101).

Pure projection: TRUNCATE + reload each run (source of truth is DynamoDB). Suppressed
rows (superseded by a PubMed twin) are loaded WITH the flag so reporting can include or
exclude them. The table is small (manually-added pubs), so a full scan + executemany is
plenty -- no segmented/parallel scan needed.

Robust: a missing or empty ExternalArticle table loads 0 rows and exits 0 (the external
feature is not live on every env). Only a genuine MySQL/AWS error exits non-zero.

Env: DB_HOST/DB_USERNAME/DB_PASSWORD/DB_NAME (reciterdb), AWS creds + AWS_DEFAULT_REGION.
"""
import os
import sys
import json
import logging
import re
import html

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
  `source_type`         ENUM('SCOPUS','WOS','OPENALEX','CROSSREF') NOT NULL,
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


def _enum_members(c):
    c.execute("SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() "
              "AND table_name = %s AND column_name = 'source_type'", (MYSQL_TABLE,))
    return set(re.findall(r"'([^']*)'", c.fetchone()[0]))


def _live_columns(c):
    c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() "
              "AND table_name = %s", (MYSQL_TABLE,))
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
            # A column the live table lacks (v3.5 not yet applied) is left out, never sent:
            # a failed INSERT after the TRUNCATE would empty the table.
            live = _live_columns(c)
            cols = [col for col in COLS if col in live]
            if len(cols) < len(COLS):
                logger.warning(f"{MYSQL_TABLE} lacks {sorted(set(COLS) - live)}; apply "
                               "setup/alter_external_article_add_book_fields_v3.5.sql")
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


def main():
    items = scan_external_articles()
    last_names, review_isbns = lookups()
    rows = [to_row(it, last_names, review_isbns) for it in items]
    logger.info(f"Scanned {len(items)} ExternalArticle items")
    rows = load(rows)
    suppressed = sum(r["suppressed"] for r in rows)
    logger.info(f"Loaded {len(rows)} rows into {MYSQL_TABLE} ({suppressed} suppressed)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"retrieveExternalArticles failed: {e}")
        sys.exit(1)
