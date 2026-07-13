#!/usr/bin/env python3
"""ETL: scan the ExternalArticle DynamoDB table -> reciterdb.external_article (MySQL).

Projects the manually-added external-source publications (Publication Manager
"Add publication" -> OpenAlex / Scopus, ReCiter #661/#662) into the reporting DB so
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
  `source_type`         ENUM('SCOPUS','WOS','OPENALEX') NOT NULL,
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
        "suppressed", "superseded_by_pmid"]


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


def to_row(it):
    authors = it.get("authors")
    return {
        "uid": it.get("uid"),
        "article_id": it.get("articleId"),
        "source_type": it.get("sourceType"),
        "doi": it.get("doi"),
        "pmid": _to_int(it.get("pmid")),
        "title": it.get("title"),
        "journal_or_venue": it.get("journalOrVenue"),
        "authors": json.dumps(authors) if authors is not None else None,
        "pub_date": it.get("pubDate"),
        "publication_type": it.get("publicationType"),
        "added_by": it.get("addedBy"),
        "date_added": it.get("dateAdded"),
        "method": it.get("method"),
        "suppressed": 1 if it.get("suppressed") else 0,
        "superseded_by_pmid": _to_int(it.get("supersededByPmid")),
    }


def load(rows):
    conn = _conn()
    try:
        with conn.cursor() as c:
            c.execute(CREATE_SQL)
            c.execute(f"TRUNCATE TABLE `{MYSQL_TABLE}`")
            if rows:
                collist = ", ".join(f"`{col}`" for col in COLS)
                placeholders = ", ".join(f"%({col})s" for col in COLS)
                sql = f"INSERT INTO `{MYSQL_TABLE}` ({collist}) VALUES ({placeholders})"
                for i in range(0, len(rows), 500):
                    c.executemany(sql, rows[i:i + 500])
        conn.commit()
    finally:
        conn.close()


def main():
    items = scan_external_articles()
    rows = [to_row(it) for it in items]
    logger.info(f"Scanned {len(items)} ExternalArticle items")
    load(rows)
    suppressed = sum(r["suppressed"] for r in rows)
    logger.info(f"Loaded {len(rows)} rows into {MYSQL_TABLE} ({suppressed} suppressed)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"retrieveExternalArticles failed: {e}")
        sys.exit(1)
