#!/usr/bin/env python3
"""
Authorship Review — reciterdb sink (Phase A of AUTHORSHIP_REVIEW_PM_PLAN.md).

Creates and writes the `authorship_review` table in the reciterdb that Publication
Manager reads (env DB_*). PM gets a Curator_All `/authorships` tab that reads this
table; the AAR pipeline (gate/matcher/scorer) is the producer.

The table is the durable, curator-facing store: the producer owns the scoring/
classification columns (refreshed each run via UPSERT) and NEVER overwrites a
curator-set `status` (assigned/accepted/rejected/dismissed/snoozed) or its
resolution/reviewer/note. New authorships are inserted `status='open'`.

NOTE (persistence): this is a PM-app-side table like `admin_users`, NOT a nightly
reporting export — it must be excluded from the reciterdb nightly-rebuild drop scope
so curator decisions survive. `create_table()` is CREATE TABLE IF NOT EXISTS, so a
re-run is a no-op if it already exists.

Usage:
  python aar_db.py --create        # create the table + indexes (idempotent)
  python aar_db.py --describe      # show columns + row count
"""
import argparse, json, os

from sqlalchemy import create_engine, text

TABLE = "authorship_review"

# producer-owned columns refreshed on every UPSERT (curator columns are preserved)
_REFRESH_COLS = [
    "source", "external_id", "pub_type", "container_id",
    "author_position", "author_position_label", "wcm_author", "author_affiliation",
    "entrez_date", "title", "journal", "doi", "classification",
    "top_cwid", "top_name", "top_person_type", "top_dept",
    "top_fg_score", "top_io_score", "top_confidence", "top_cohort_size",
    "top_given_match", "top_affil_match", "n_candidates", "single_candidate",
    "candidate_cwids_json", "last_refreshed",
]
# full insert column set (curator columns default on first insert)
_INSERT_COLS = ["pmid", "author_key"] + _REFRESH_COLS + [
    "status", "first_seen", "last_checked"]

_ENGINE = None


def engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 15}, pool_pre_ping=True)
    return _ENGINE


DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  id                     BIGINT       NOT NULL AUTO_INCREMENT,
  source                 ENUM('pubmed','scopus') NOT NULL DEFAULT 'pubmed',
  pmid                   BIGINT       NULL,
  external_id            VARCHAR(96)  NULL,
  author_key             VARCHAR(160) NOT NULL,
  pub_type               VARCHAR(40)  NULL,
  container_id           VARCHAR(96)  NULL,
  author_position        INT          NULL,
  author_position_label  VARCHAR(8)   NULL,
  wcm_author             VARCHAR(255) NULL,
  author_affiliation     TEXT         NULL,
  entrez_date            DATE         NULL,
  title                  TEXT         NULL,
  journal                VARCHAR(512) NULL,
  doi                    VARCHAR(255) NULL,
  classification         ENUM('assigned','suggested','buried','absent') NULL,
  top_cwid               VARCHAR(32)  NULL,
  top_name               VARCHAR(255) NULL,
  top_person_type        VARCHAR(64)  NULL,
  top_dept               VARCHAR(255) NULL,
  top_fg_score           FLOAT        NULL,
  top_io_score           FLOAT        NULL,
  top_confidence         FLOAT        NULL,
  top_cohort_size        INT          NULL,
  top_given_match        VARCHAR(16)  NULL,
  top_affil_match        TINYINT(1)   NULL,
  n_candidates           INT          NULL,
  single_candidate       TINYINT(1)   NULL,
  candidate_cwids_json   LONGTEXT     NULL,
  status                 ENUM('open','assigned','accepted','rejected','dismissed','snoozed')
                                      NOT NULL DEFAULT 'open',
  resolution_cwid        VARCHAR(32)  NULL,
  reviewer               VARCHAR(64)  NULL,
  note                   TEXT         NULL,
  snooze_until           DATE         NULL,
  resolved_at            DATETIME     NULL,
  first_seen             DATETIME     NULL,
  last_refreshed         DATETIME     NULL,
  last_checked           DATETIME     NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_author_key (author_key),
  KEY ix_source (source),
  KEY ix_pmid (pmid),
  KEY ix_classification (classification),
  KEY ix_status (status),
  KEY ix_single_candidate (single_candidate),
  KEY ix_top_io_score (top_io_score),
  KEY ix_entrez_date (entrez_date),
  KEY ix_top_cwid (top_cwid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def create_table():
    with engine().begin() as c:
        c.execute(text(DDL))


def upsert(rows):
    """Insert/update authorship rows by author_key. Refreshes producer-owned columns;
    PRESERVES curator status/resolution/reviewer/note/snooze on existing rows."""
    if not rows:
        return 0
    cols = ", ".join(_INSERT_COLS)
    placeholders = ", ".join(f":{c}" for c in _INSERT_COLS)
    updates = ", ".join(f"{c}=VALUES({c})" for c in _REFRESH_COLS)
    # also advance last_checked on refresh; never touch status/resolution_*/reviewer/note/snooze
    stmt = text(f"INSERT INTO {TABLE} ({cols}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {updates}, last_checked=VALUES(last_checked)")
    n = 0
    with engine().begin() as c:
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            c.execute(stmt, [{k: r.get(k) for k in _INSERT_COLS} for r in chunk])
            n += len(chunk)
    return n


def _describe():
    with engine().connect() as c:
        cols = c.execute(text(
            "SELECT column_name, column_type FROM information_schema.columns "
            "WHERE table_schema=:d AND table_name=:t ORDER BY ordinal_position"),
            {"d": os.environ["DB_NAME"], "t": TABLE}).fetchall()
        if not cols:
            print(f"{TABLE} does not exist")
            return
        print(f"{TABLE} — {len(cols)} columns:")
        for name, typ in cols:
            print(f"  {name:24} {typ}")
        n = c.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        print(f"rows: {n}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true")
    ap.add_argument("--describe", action="store_true")
    args = ap.parse_args()
    if args.create:
        create_table()
        print(f"{TABLE} ready")
    if args.describe:
        _describe()
    if not (args.create or args.describe):
        ap.error("use --create and/or --describe")


if __name__ == "__main__":
    main()
