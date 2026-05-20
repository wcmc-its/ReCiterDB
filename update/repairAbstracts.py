"""
repairAbstracts.py -- one-shot cleanup of reporting_abstracts rows flagged
as corrupted by update/auditAbstracts.py.

Reads audit_abstracts.csv (the audit output) and:
  1. Backs up the affected rows to reporting_abstracts_corrupt_backup_<ts>.
  2. Deletes the corrupted rows from reporting_abstracts in batches.
  3. Dedupes any remaining pmids that have multiple rows by keeping the
     row with MIN(id) and backing up the rest to the same backup table.
     (Precondition for the v1.4 UNIQUE KEY migration.)
  4. Verifies post-state row counts and confirms no duplicate pmids remain.

After this script runs, the next nightly update/abstractImport.py will
re-fetch the deleted PMIDs cleanly via the parameterized executemany
path introduced in PR #78.

Destructive. Requires --apply to perform the delete; without --apply it
runs in dry-run mode (counts only, no writes).

Env:
  DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME
"""

import argparse
import csv
import datetime
import logging
import os
import re
import sys

import pymysql.cursors
import pymysql.err


INVALID_VERDICTS = {"PREFIX_CORRUPTED", "DISJOINT", "EMPTY_IN_DYNAMO"}
DEFAULT_AUDIT_CSV = "audit_abstracts.csv"
DEFAULT_BATCH_SIZE = 500

# Identifier safety: the backup-table suffix is timestamp-derived, but
# allow callers to override with --backup-table; whitelist the shape to
# refuse anything that would require quoting.
SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def connect_mysql():
    try:
        return pymysql.connect(
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST"),
            autocommit=True,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
    except pymysql.err.MySQLError as err:
        logger.error(f"DB connection failed: {err}")
        sys.exit(1)


def read_invalid_pmids(audit_csv):
    if not os.path.exists(audit_csv):
        logger.error(f"Audit CSV not found: {audit_csv}")
        logger.error("Run update/auditAbstracts.py first.")
        sys.exit(1)
    with open(audit_csv) as f:
        rows = list(csv.DictReader(f))
    if not rows or "verdict" not in rows[0] or "pmid" not in rows[0]:
        logger.error(f"{audit_csv} is missing required columns (pmid, verdict).")
        sys.exit(1)
    return sorted({
        int(r["pmid"]) for r in rows if r["verdict"] in INVALID_VERDICTS
    })


def count_matching(cur, pmids, batch=5000):
    """COUNT(*) of rows whose pmid is in `pmids`, batched to avoid
    oversized IN-lists. Returns the sum across batches."""
    total = 0
    for i in range(0, len(pmids), batch):
        chunk = pmids[i:i + batch]
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"SELECT COUNT(*) AS c FROM reporting_abstracts "
            f"WHERE pmid IN ({placeholders})",
            chunk,
        )
        total += cur.fetchone()["c"]
    return total


def writable_columns(cur, table="reporting_abstracts"):
    """Return the list of non-generated columns (those that accept INSERT).
    Prod has a STORED generated column abstract_len that cannot be assigned;
    INSERT must enumerate the real columns explicitly."""
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s "
        "  AND (extra IS NULL OR extra NOT LIKE '%%GENERATED%%') "
        "ORDER BY ordinal_position",
        (table,),
    )
    return [r["column_name"] for r in cur.fetchall()]


def backup_rows(cur, pmids, backup_table, batch):
    cur.execute(f"CREATE TABLE `{backup_table}` LIKE reporting_abstracts")
    cols = writable_columns(cur)
    col_list = ", ".join(f"`{c}`" for c in cols)
    inserted = 0
    for i in range(0, len(pmids), batch):
        chunk = pmids[i:i + batch]
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"INSERT INTO `{backup_table}` ({col_list}) "
            f"SELECT {col_list} FROM reporting_abstracts WHERE pmid IN ({placeholders})",
            chunk,
        )
        inserted += cur.rowcount
        if (i // batch) % 5 == 0:
            logger.info(f"  ... backed up {inserted:,} rows")
    return inserted


def delete_rows(cur, pmids, batch):
    deleted = 0
    for i in range(0, len(pmids), batch):
        chunk = pmids[i:i + batch]
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"DELETE FROM reporting_abstracts WHERE pmid IN ({placeholders})",
            chunk,
        )
        deleted += cur.rowcount
        if (i // batch) % 5 == 0:
            logger.info(f"  ... deleted {deleted:,} rows")
    return deleted


def find_duplicate_pmids(cur, limit=10):
    cur.execute(
        "SELECT pmid, COUNT(*) AS c FROM reporting_abstracts "
        "GROUP BY pmid HAVING c > 1 LIMIT %s",
        (limit,),
    )
    return cur.fetchall()


def count_duplicate_extras(cur):
    """Returns (group_count, extra_row_count). extra_row_count is the number
    of rows that would need to be deleted to leave one row per pmid."""
    cur.execute(
        "SELECT COUNT(*) AS groups, COALESCE(SUM(c - 1), 0) AS extras FROM ("
        "  SELECT COUNT(*) AS c FROM reporting_abstracts GROUP BY pmid HAVING c > 1"
        ") d"
    )
    r = cur.fetchone()
    return r["groups"], r["extras"]


def backup_duplicate_extras(cur, backup_table):
    """Insert into the backup table every duplicate row except the MIN(id)
    keeper for each pmid. Returns the number of rows backed up."""
    cols = writable_columns(cur)
    col_list = ", ".join(f"`{c}`" for c in cols)
    select_list = ", ".join(f"ra.`{c}`" for c in cols)
    cur.execute(
        f"INSERT INTO `{backup_table}` ({col_list}) "
        f"SELECT {select_list} FROM reporting_abstracts ra "
        "JOIN ("
        "  SELECT pmid, MIN(id) AS keep_id FROM reporting_abstracts "
        "  GROUP BY pmid HAVING COUNT(*) > 1"
        ") k ON k.pmid = ra.pmid AND ra.id <> k.keep_id"
    )
    return cur.rowcount


def delete_duplicate_extras(cur):
    """Delete every duplicate row except the MIN(id) keeper for each pmid.
    Returns the number of rows deleted."""
    cur.execute(
        "DELETE ra FROM reporting_abstracts ra "
        "JOIN ("
        "  SELECT pmid, MIN(id) AS keep_id FROM reporting_abstracts "
        "  GROUP BY pmid HAVING COUNT(*) > 1"
        ") k ON k.pmid = ra.pmid AND ra.id <> k.keep_id"
    )
    return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-csv", default=DEFAULT_AUDIT_CSV,
                        help=f"Audit CSV path (default {DEFAULT_AUDIT_CSV})")
    parser.add_argument("--apply", action="store_true",
                        help="Perform the delete. Without this flag, dry-run only.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"PMIDs per statement (default {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--backup-table", default=None,
                        help="Backup table name (default: reporting_abstracts_corrupt_backup_<ts>)")
    args = parser.parse_args()

    pmids = read_invalid_pmids(args.audit_csv)
    logger.info(f"Read {len(pmids):,} invalid PMIDs from {args.audit_csv}")
    if not pmids:
        logger.info("Nothing to repair.")
        return

    backup_table = args.backup_table or (
        f"reporting_abstracts_corrupt_backup_"
        f"{datetime.datetime.now():%Y%m%d_%H%M%S}"
    )
    if not SAFE_IDENT.match(backup_table):
        logger.error(f"Refusing unsafe backup-table identifier: {backup_table!r}")
        sys.exit(1)

    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM reporting_abstracts")
            before_total = cur.fetchone()["c"]

            matching = count_matching(cur, pmids)
            logger.info(
                f"reporting_abstracts: {before_total:,} rows total; "
                f"{matching:,} rows match the invalid-PMID list."
            )

            if matching > len(pmids):
                logger.info(
                    f"Live matches ({matching:,}) > unique PMIDs ({len(pmids):,}): "
                    f"{matching - len(pmids):,} of the audited PMIDs have multiple "
                    "rows in the live table (all of which will be deleted by the IN clause)."
                )
            elif matching < len(pmids):
                logger.warning(
                    f"Live matches ({matching:,}) < unique PMIDs ({len(pmids):,}): "
                    f"{len(pmids) - matching:,} audited PMIDs no longer present "
                    "(already deleted or table changed). Proceeding with what is live."
                )

            dupe_groups, dupe_extras = count_duplicate_extras(cur)
            logger.info(
                f"Duplicate-pmid groups: {dupe_groups:,} "
                f"({dupe_extras:,} extra rows would be deduped after the corruption delete)."
            )

            if not args.apply:
                cur.execute(
                    "SELECT pmid, LENGTH(abstract) AS db_len FROM reporting_abstracts "
                    "WHERE LENGTH(abstract) >= 4000 ORDER BY LENGTH(abstract) DESC LIMIT 3"
                )
                samples = cur.fetchall()
                logger.info("Sample of longest current rows (pre-repair):")
                for s in samples:
                    logger.info(f"  pmid={s['pmid']:>9} db_len={s['db_len']}")
                logger.info(f"Would back up to: `{backup_table}`")
                logger.info(
                    f"Would delete {matching:,} corrupted rows + dedupe "
                    f"{dupe_extras:,} duplicate-extras (keep MIN(id) per pmid)."
                )
                logger.info("DRY RUN -- no changes made. Re-run with --apply to perform the repair.")
                return

            logger.info(f"Creating backup table `{backup_table}` ...")
            backed_up = backup_rows(cur, pmids, backup_table, args.batch_size)
            logger.info(f"Backed up {backed_up:,} rows to `{backup_table}`.")
            if backed_up != matching:
                logger.error(
                    f"Backup row count {backed_up:,} != expected {matching:,}. Aborting."
                )
                sys.exit(1)

            logger.info("Deleting corrupted rows from reporting_abstracts ...")
            deleted = delete_rows(cur, pmids, args.batch_size)

            cur.execute("SELECT COUNT(*) AS c FROM reporting_abstracts")
            after_total = cur.fetchone()["c"]
            logger.info(
                f"Deleted {deleted:,} rows; reporting_abstracts now has "
                f"{after_total:,} rows (was {before_total:,})."
            )
            if before_total - after_total != deleted:
                logger.error(
                    f"Row-count delta mismatch: before-after={before_total - after_total}, "
                    f"deleted={deleted}. Backup table `{backup_table}` is intact."
                )
                sys.exit(1)

            cur.execute(
                "SELECT COUNT(*) AS c FROM reporting_abstracts WHERE LENGTH(abstract) >= 4000"
            )
            long_remaining = cur.fetchone()["c"]
            logger.info(
                f"Rows with LENGTH(abstract) >= 4000 remaining: {long_remaining:,} "
                "(should approximately equal the CLEAN count from the audit)."
            )

            cur.execute(
                "SELECT COUNT(*) AS c FROM reporting_abstracts WHERE LENGTH(abstract) >= 60000"
            )
            cap_remaining = cur.fetchone()["c"]
            logger.info(
                f"Rows at/above 60K (BLOB-cap region) remaining: {cap_remaining:,} "
                "(should be 0 if repair caught all corruption)."
            )

            dupe_groups_after, dupe_extras_after = count_duplicate_extras(cur)
            if dupe_extras_after > 0:
                logger.info(
                    f"Phase 2: deduping {dupe_extras_after:,} extra rows across "
                    f"{dupe_groups_after:,} pmid groups (keeping MIN(id) per pmid)..."
                )
                backed_up_dupes = backup_duplicate_extras(cur, backup_table)
                logger.info(f"  ... backed up {backed_up_dupes:,} duplicate rows to `{backup_table}`.")
                if backed_up_dupes != dupe_extras_after:
                    logger.error(
                        f"Dedup backup count {backed_up_dupes:,} != expected {dupe_extras_after:,}. "
                        "Aborting before delete."
                    )
                    sys.exit(1)
                deleted_dupes = delete_duplicate_extras(cur)
                logger.info(f"  ... deleted {deleted_dupes:,} duplicate rows.")
                if deleted_dupes != dupe_extras_after:
                    logger.error(
                        f"Dedup delete count {deleted_dupes:,} != expected {dupe_extras_after:,}. "
                        f"Backup table `{backup_table}` is intact."
                    )
                    sys.exit(1)
            else:
                logger.info("Phase 2: no duplicates to dedupe.")

            dupes = find_duplicate_pmids(cur)
            if dupes:
                logger.error(
                    f"{len(dupes)} duplicate pmid(s) still present after dedup (sample): "
                    f"{[(d['pmid'], d['c']) for d in dupes]}"
                )
                sys.exit(1)
            else:
                logger.info(
                    "No duplicate pmids remain. Safe to apply "
                    "setup/alter_add_uq_pmid_reporting_abstracts_v1.4.sql."
                )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
