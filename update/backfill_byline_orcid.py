#!/usr/bin/env python3
"""
One-off backfill of authorship_review.byline_orcid onto the Scopus and OpenAlex rows
already in the queue whose top pick is a Cornell person (v3.15).

WHY THIS EXISTS. Producer columns do not self-heal. With AAR_BYLINE_ORCID=on the lanes
store the byline author's ORCID on the rows they write from then on, and a Scopus sweep
only reaches documents loaded in the last 90 days. Every older row keeps NULL. This pass
fills them from OpenAlex.

WHAT IT DOES, per row that has a DOI and no byline_orcid:
  1. looks the DOI up in OpenAlex, 50 DOIs a call (aar_universe_scopus.openalex_authorships:
     filter=doi:a|b|..., select=doi,authorships). Only DOIs leave the machine;
  2. takes the ORCID of the author at the row's byline position when that author carries
     the row's surname, else of the one author with the surname and first initial, else
     nothing (aar_universe_scopus.pick_byline_orcid, the Scopus lane's own rule);
  3. writes it to byline_orcid.

WHAT IT CANNOT DO:
  * It cannot recover byline_scopus_authid. The Scopus author id comes from the Scopus
    search response and OpenAlex does not carry it, so that column stays NULL on every
    row older than the capture and is only ever filled by a Scopus sweep.
  * It cannot tell Scopus's own ORCID from OpenAlex's. The lane prefers the ORCID on the
    Scopus author entry and falls back to OpenAlex; this pass has only OpenAlex. OpenAlex
    assigns some ORCIDs through its own author disambiguation, not from the paper.
  * A row with no DOI gets nothing, and neither does one whose DOI carries a comma, pipe,
    ampersand or whitespace (it is never sent).

Rows of every status are filled, resolved ones included: the ORCID describes the byline,
not a decision, and no pick, status or candidate list is read or written here.

SAFETY, the same rules as backfill_venue_fit.py:
  * Dry run by default. It reads, looks up, counts, prints a sample and writes a JSONL
    ledger with the before and after of every row it would touch.
  * The ledger is on disk BEFORE the first UPDATE and rewritten after it with each row's
    `applied`. A ledger that cannot be written stops the run with nothing changed. Every
    run writes its own file and an existing file is never overwritten.
  * --apply writes byline_orcid and nothing else (WRITE_COLS, checked at import against
    aar_db._REFRESH_COLS).
  * The UPDATE re-checks byline_orcid IS NULL, so a row the producer filled between the
    SELECT and the write keeps the producer's value. `applied` in the ledger is the
    UPDATE's own row count, per row.
  * --apply refuses until setup/alter_authorship_review_add_byline_orcid_v3.15.sql is
    applied.
  * OpenAlex failing is not an error: the lookup stops, the rows it did not reach are
    counted as not found, and a later run picks them up.

The --apply run is a prod write; it is handed to the owner to run, out of hours and not
alongside the nightly (14:30 UTC).

ORDER. Dry-run at any time. Set AAR_BYLINE_ORCID=on on the CronJob BEFORE --apply, not
after: while capture is off every sweep re-upserts the rows in its window with
byline_orcid NULL, which would undo this pass for those rows.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb); OPENALEX_MAILTO (optional, the
OpenAlex lane's convention). AAR_BYLINE_ORCID is not read: this is the explicit tool, and
its own environment says nothing about the CronJob's.

Usage:
  python3 backfill_byline_orcid.py --selftest          # offline checks, no DB, no network
  python3 backfill_byline_orcid.py                     # dry run: counts, sample, ledger
  python3 backfill_byline_orcid.py --limit 100         # ...over the first 100 rows
  python3 backfill_byline_orcid.py --apply
"""
import argparse, os, sys
from collections import Counter
from datetime import datetime

from sqlalchemy import text

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import aar_db                                                    # noqa: E402
import aar_universe_scopus as scop                               # noqa: E402
from backfill_venue_fit import _write_ledger                     # noqa: E402

# Everything --apply may write.
WRITE_COLS = ["byline_orcid"]
assert all(c in aar_db._REFRESH_COLS for c in WRITE_COLS), (
    "WRITE_COLS names a column the producer itself does not refresh: "
    f"{[c for c in WRITE_COLS if c not in aar_db._REFRESH_COLS]}")

_READ_COLS = ["id", "source", "status", "author_key", "author_position", "wcm_author", "doi"]
# `{unfilled}` is "AND byline_orcid IS NULL" once the column exists, and empty before the
# migration, when every row is unfilled and naming the column would fail the dry run.
_SELECT_SQL = ("SELECT {cols} FROM authorship_review WHERE source IN ('scopus', 'openalex') "
               "AND top_person_type LIKE 'Cornell%' AND doi IS NOT NULL AND doi <> '' "
               "{unfilled} ORDER BY id")


def fill_row(row, by_doi, run_ts):
    """One row -> (verdict, entry). `by_doi` is openalex_authorships()' answer for the
    run; `entry` is the ledger record, or None when the row is left alone.

      skipped: DOI not in OpenAlex            no such work, an unsendable DOI, or the
                                              lookup stopped before reaching it
      skipped: no ORCID on the byline author  the work is there; the author at the row's
                                              position has none, or cannot be told apart
      ORCID_FOUND                             byline_orcid would be written"""
    authorships = by_doi.get(str(row["doi"]).lower())
    if not authorships:
        return "skipped: DOI not in OpenAlex", None
    position = row.get("author_position")
    orcid = scop.pick_byline_orcid(authorships, int(position) - 1 if position else -1,
                                   row.get("wcm_author"))
    if not orcid:
        return "skipped: no ORCID on the byline author", None
    return "ORCID_FOUND", {
        "id": row["id"], "source": row["source"], "status": row["status"],
        "author_key": row["author_key"], "wcm_author": row["wcm_author"], "doi": row["doi"],
        "rule": "ORCID_FOUND", "before": {"byline_orcid": None},
        "after": {"byline_orcid": orcid}, "swept_at": run_ts, "applied": False}


def _update_sql():
    """The only write. byline_orcid IS NULL keeps a value the producer wrote since the
    row was read; the source list keeps it off the PubMed lane. No status test: the
    column describes the byline and is filled on resolved rows too."""
    sets = ", ".join(f"{c}=:{c}" for c in WRITE_COLS)
    return (f"UPDATE authorship_review SET {sets} WHERE id=:id AND byline_orcid IS NULL "
            "AND source IN ('scopus', 'openalex')")


def apply_entries(conn, entries):
    """Write each entry and set its `applied` from the UPDATE's own row count, so the
    ledger never claims a row the guard skipped."""
    stmt, n = text(_update_sql()), 0
    for e in entries:
        e["applied"] = conn.execute(stmt, dict(e["after"], id=e["id"])).rowcount == 1
        n += e["applied"]
    return n


def ledger_then_apply(eng, entries, path, apply_writes):
    """The ledger, then the write, then the ledger again, as backfill_venue_fit does it.
    Returns the number of rows updated."""
    _write_ledger(path, entries)
    if not (apply_writes and entries):
        return 0
    with eng.begin() as c:
        n = apply_entries(c, entries)
    try:
        _write_ledger(path, entries)
    except OSError:
        print(f"\nWARNING: {n} rows were UPDATED but the ledger could not be rewritten. "
              f"{path} still holds the full before/after of every row, with applied=false "
              f"on all of them.", flush=True)
        raise
    return n


def default_ledger_path(now, apply_writes):
    """One file per run: the time to the second, and whether it wrote."""
    return os.path.join(HERE, f"backfill_byline_orcid_ledger_{now:%Y%m%d_%H%M%S}_"
                              f"{'apply' if apply_writes else 'dryrun'}.jsonl")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--ledger", default=None,
                    help="JSONL ledger of every row touched, before and after (default: "
                         "backfill_byline_orcid_ledger_<time>_<dryrun|apply>.jsonl beside "
                         "this script; an existing file is refused, never overwritten)")
    ap.add_argument("--limit", type=int, default=None,
                    help="consider only the first N rows (one OpenAlex call per 50 DOIs)")
    ap.add_argument("--sample", type=int, default=15, help="rows to print")
    ap.add_argument("--selftest", action="store_true", help="offline checks, no DB")
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    now = datetime.now()
    run_ts = now.strftime("%Y-%m-%d %H:%M:%S")
    ledger = args.ledger or default_ledger_path(now, args.apply)
    if os.path.exists(ledger):
        print(f"{ledger} already exists and a ledger is never overwritten: it may be the "
              f"only before/after record of an earlier run. Pass another --ledger. "
              f"Nothing read, nothing written.")
        sys.exit(2)
    eng = aar_db.engine()
    with eng.connect() as c:
        column_ready = "byline_orcid" not in aar_db.missing_optional_cols(c)
        if args.apply and not column_ready:
            print("authorship_review has no byline_orcid column: apply "
                  "setup/alter_authorship_review_add_byline_orcid_v3.15.sql first. "
                  "Nothing written.")
            sys.exit(2)
        rows = [dict(r) for r in c.execute(text(_SELECT_SQL.format(
            cols=", ".join(_READ_COLS),
            unfilled="AND byline_orcid IS NULL" if column_ready else ""))).mappings().all()]
    if args.limit:
        rows = rows[:args.limit]
    dois = {r["doi"] for r in rows}
    print(f"[{'APPLY' if args.apply else 'DRY-RUN'}] {len(rows)} Cornell-top external rows "
          f"with a DOI and no byline_orcid; {len(dois)} DOIs, about "
          f"{-(-len(dois) // scop.OPENALEX_DOI_BATCH)} OpenAlex calls", flush=True)
    if not column_ready:
        print("NOTE: authorship_review has no byline_orcid column yet (v3.15 is not "
              "applied); every row counts as unfilled", flush=True)
    by_doi = scop.openalex_authorships(dois)
    print(f"  {len(by_doi)} of {len(dois)} DOIs found in OpenAlex", flush=True)

    counts, entries = Counter(), []
    for r in rows:
        verdict, entry = fill_row(r, by_doi, run_ts)
        counts[verdict] += 1
        if entry:
            entries.append(entry)
    for verdict, n in sorted(counts.items(), key=lambda kv: (kv[0].startswith("skipped"), -kv[1])):
        print(f"  {n:6d}  {verdict}")
    for e in entries[:args.sample]:
        print(f"    id={e['id']} [{e['status']}] {e['wcm_author']!r} {e['doi']} "
              f"-> {e['after']['byline_orcid']}")

    n = ledger_then_apply(eng, entries, ledger, args.apply)
    if args.apply and entries:
        print(f"\nAPPLIED: {n} of {len(entries)} rows updated"
              + (f" ({len(entries) - n} skipped: filled by the producer since they were "
                 f"read)" if n != len(entries) else ""))
    print(f"\nledger: {ledger} ({len(entries)} rows)")
    if args.apply:
        print("AAR_BYLINE_ORCID must be on for the CronJob, or its next sweep rewrites the "
              "rows in its window with no ORCID (see ORDER in this script's docstring).")
    else:
        print("DRY-RUN: nothing written (pass --apply)")


# ---- offline self-test ----------------------------------------------------------
def _selftest():
    import json, tempfile
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    ts = "2026-11-17 12:00:00"
    by_doi = {"10.1/x": [("Rhea Weiss", "0000-0001-0000-0001"), ("Robert Weiss", None),
                         ("Ann Lee", "0000-0001-0000-0003")]}

    def row(position=1, wcm_author="Rhea Weiss", doi="10.1/X", **kw):
        return dict({"id": 1, "source": "scopus", "status": "open",
                     "author_key": f"scopus:{doi}:{(position or 1) - 1}",
                     "author_position": position, "wcm_author": wcm_author, "doi": doi}, **kw)

    verdict, e = fill_row(row(), by_doi, ts)
    check("the author at the row's 1-based position, DOI matched case-insensitively: "
          "ORCID_FOUND with the before and after of exactly the written column",
          verdict == "ORCID_FOUND" and e["after"] == {"byline_orcid": "0000-0001-0000-0001"}
          and e["before"] == {"byline_orcid": None} and e["applied"] is False
          and set(e["after"]) == set(WRITE_COLS))
    check("a resolved row is filled like an open one: the ORCID describes the byline",
          fill_row(row(status="accepted"), by_doi, ts)[0] == "ORCID_FOUND"
          and fill_row(row(status="accepted"), by_doi, ts)[1]["status"] == "accepted")
    check("a position that is off falls back to the one author with the surname and initial",
          fill_row(row(position=3, wcm_author="A. Lee"), by_doi, ts)[1]["after"]
          == {"byline_orcid": "0000-0001-0000-0003"}
          and fill_row(row(position=None, wcm_author="Ann Lee"), by_doi, ts)[0] == "ORCID_FOUND")
    check("an author with no ORCID, or one who cannot be told from a namesake, is skipped",
          fill_row(row(position=2, wcm_author="Robert Weiss"), by_doi, ts)
          == fill_row(row(position=3, wcm_author="R Weiss"), by_doi, ts)
          == ("skipped: no ORCID on the byline author", None))
    check("a DOI OpenAlex did not answer for is skipped",
          fill_row(row(doi="10.9/none"), by_doi, ts) == fill_row(row(), {}, ts)
          == ("skipped: DOI not in OpenAlex", None))

    sql = _update_sql()
    check("the UPDATE writes byline_orcid only, and only where it is still NULL on an "
          "external row",
          sql == "UPDATE authorship_review SET byline_orcid=:byline_orcid WHERE id=:id "
                 "AND byline_orcid IS NULL AND source IN ('scopus', 'openalex')")
    check("the written column is producer-owned; no curator column and no pick is named",
          all(c in aar_db._REFRESH_COLS for c in WRITE_COLS)
          and not any(c in sql for c in ("status", "top_cwid", "candidate_cwids_json",
                                         "resolution_cwid", "reviewer", "last_refreshed")))
    check("the SELECT takes Cornell-top external rows with a DOI, of any status",
          all(s in _SELECT_SQL for s in ("source IN ('scopus', 'openalex')",
                                         "top_person_type LIKE 'Cornell%'", "doi IS NOT NULL"))
          and "status" not in _SELECT_SQL)

    class _Engine:
        """Records the order of events; answers each UPDATE with the queued row count."""
        def __init__(self, rowcounts=()):
            self.events, self.rowcounts, self.rowcount = [], list(rowcounts), 1

        def begin(self):
            return self

        def __enter__(self):
            self.events.append("BEGIN")
            return self

        def __exit__(self, *exc):
            self.events.append("COMMIT" if exc[0] is None else "ROLLBACK")
            return False

        def execute(self, _stmt, params):
            self.events.append(f"UPDATE {params['id']} {params['byline_orcid']}")
            self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 1
            return self

    def fresh():
        return [fill_row(row(id=i), by_doi, ts)[1] for i in (1, 2)]

    def read(path):
        with open(path) as fh:
            return [json.loads(line) for line in fh]

    with tempfile.TemporaryDirectory() as d:
        eng, path = _Engine(), os.path.join(d, "no-such-dir", "ledger.jsonl")
        try:
            ledger_then_apply(eng, fresh(), path, True)
            raised = False
        except OSError:
            raised = True
        check("a ledger that cannot be written stops the run before any UPDATE",
              raised and eng.events == [])

        eng, path = _Engine(), os.path.join(d, "dry.jsonl")
        n = ledger_then_apply(eng, fresh(), path, False)
        check("a dry run writes the ledger and nothing else",
              n == 0 and eng.events == [] and [e["applied"] for e in read(path)] == [False, False])

        eng, path, seen = _Engine([1, 0]), os.path.join(d, "apply.jsonl"), []
        real_apply = globals()["apply_entries"]

        def spy(conn, entries):
            seen.extend(read(path))             # what is on disk when the writes begin
            return real_apply(conn, entries)
        globals()["apply_entries"] = spy
        try:
            n = ledger_then_apply(eng, fresh(), path, True)
        finally:
            globals()["apply_entries"] = real_apply
        check("on --apply the before/after is on disk, applied=false, before the first UPDATE",
              [(e["id"], e["applied"]) for e in seen] == [(1, False), (2, False)]
              and eng.events == ["BEGIN", "UPDATE 1 0000-0001-0000-0001",
                                 "UPDATE 2 0000-0001-0000-0001", "COMMIT"])
        check("...and is rewritten with each UPDATE's own row count: a row the producer "
              "filled in between is logged applied=false and not counted",
              n == 1 and [e["applied"] for e in read(path)] == [True, False])

    a = default_ledger_path(datetime(2026, 11, 17, 9, 30, 5), True)
    b = default_ledger_path(datetime(2026, 11, 17, 9, 31, 0), False)
    check("each run's default ledger is its own file",
          a != b and a.endswith("backfill_byline_orcid_ledger_20261117_093005_apply.jsonl")
          and b.endswith("backfill_byline_orcid_ledger_20261117_093100_dryrun.jsonl"))

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
