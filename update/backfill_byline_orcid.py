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
not a decision.

WITH THE MATCHER ON (AAR_BYLINE_ORCID=on in this shell) IT ALSO RE-RANKS OPEN ROWS. The
switch is the one that makes `IdentityIndex.load` read the ORCID map, so this ranks with
what the CronJob ranks with or not at all. For each OPEN row with a byline ORCID, stored
or found in this run, that is on file for somebody on the Ithaca roster:
  ORCID_CONFIRMS     that person is already the top pick. orcid_match is recorded on the
                     stored candidates and top_orcid_match set; nobody moves.
  PICK_CHANGE        that person is a lower-ranked stored candidate, and now leads.
  ORCID_NEW_PERSON   that person is not among the stored candidates. They are added,
                     built by the matcher itself from the row's stored byline
                     (IdentityIndex.candidates with the ORCID), and lead.
Like backfill_venue_fit.py this works from what the row stores, with no Scopus
re-fetch, and that sets the same limits. The stored list is the top five: a row whose
only WCM candidates ranked sixth or lower reads here as all-Cornell. The name-matched
candidates keep the confidence they have; the added person's is computed from a byline
the row stores as one string (wcm_author, last token read as the surname), which can
give them a weaker given-name tier than a sweep of the document would. The next sweep
that reaches the row writes the producer's own answer.
Rows left alone by the re-rank (their ORCID is still filled):
  * a row held in Publication Manager's duplicates queue (accept_conflict set): a
    curator's Accept is parked on the person the row names;
  * a row that is not open: an ORCID match never changes a status, or a decided row;
  * a row with a WCM candidate on it, as the matcher leaves it (CampusRoster);
  * a row whose candidates already carry orcid_match, which makes a second run a no-op.

SAFETY, the same rules as backfill_venue_fit.py:
  * Dry run by default. It reads, looks up, counts, prints a sample and writes a JSONL
    ledger with the before and after of every row it would touch.
  * The ledger is on disk BEFORE the first UPDATE and rewritten after it with each row's
    `applied`. A ledger that cannot be written stops the run with nothing changed. Every
    run writes its own file and an existing file is never overwritten.
  * --apply writes producer-owned columns only: byline_orcid on a fill, and the pick
    columns as well on a re-rank (WRITE_COLS / PICK_COLS, checked at import against
    aar_db._REFRESH_COLS). It never names status, resolution_cwid, reviewer, note or
    snooze_until.
  * The fill's UPDATE re-checks byline_orcid IS NULL, so a row the producer filled
    between the SELECT and the write keeps the producer's value. The re-rank's UPDATE
    re-checks status='open', accept_conflict IS NULL and the last_refreshed it read, so
    a row a curator resolved or the producer rewrote in between is skipped. `applied`
    in the ledger is the UPDATE's own row count, per row.
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
OpenAlex lane's convention). The fill does not read AAR_BYLINE_ORCID: it is the explicit
tool. The re-rank runs only with AAR_BYLINE_ORCID=on, and scores an added person's venue
only with AAR_VENUE_FIT=on: run it with the CronJob's values of both.

Usage:
  python3 backfill_byline_orcid.py --selftest          # offline checks, no DB, no network
  python3 backfill_byline_orcid.py                     # dry run: counts, sample, ledger
  python3 backfill_byline_orcid.py --limit 100         # ...over the first 100 rows
  python3 backfill_byline_orcid.py --open-only         # ...over open rows only
  AAR_BYLINE_ORCID=on python3 backfill_byline_orcid.py # ...and the re-rank of open rows
  python3 backfill_byline_orcid.py --apply
"""
import argparse, json, os, sys
from collections import Counter
from datetime import datetime

from sqlalchemy import text

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import aar_db                                                    # noqa: E402
import aar_universe_scopus as scop                               # noqa: E402
from backfill_venue_fit import _write_ledger, _is_ithaca, _cwid_eq, _trunc   # noqa: E402
from identity_index import (IdentityIndex, CAMPUS_ITHACA, _rank_key,          # noqa: E402
                            byline_orcid_enabled)

# Everything --apply may write.
WRITE_COLS = ["byline_orcid"]
assert all(c in aar_db._REFRESH_COLS for c in WRITE_COLS), (
    "WRITE_COLS names a column the producer itself does not refresh: "
    f"{[c for c in WRITE_COLS if c not in aar_db._REFRESH_COLS]}")

# What a re-rank writes beside the ORCID: the top_* columns, which follow whoever is
# first, the candidate list, and the three cohort columns, which move when a person is
# added.
PICK_COLS = ["top_cwid", "top_name", "top_person_type", "top_dept", "top_given_match",
             "top_affil_match", "top_confidence", "top_years_after_wcm", "top_venue_fit",
             "top_orcid_match", "top_cohort_size", "single_candidate", "n_candidates",
             "candidate_cwids_json", "last_refreshed"]
assert all(c in aar_db._REFRESH_COLS for c in PICK_COLS), (
    "PICK_COLS names a column the producer itself does not refresh: "
    f"{[c for c in PICK_COLS if c not in aar_db._REFRESH_COLS]}")
assert not (set(WRITE_COLS + PICK_COLS) & {"status", "resolution_cwid", "reviewer", "note",
                                           "snooze_until", "resolved_at",
                                           "matched_pmid_verdict"}), (
    "a curator-owned column is named -- this backfill must never write one")

_READ_COLS = ["id", "source", "status", "author_key", "author_position", "wcm_author", "doi"]
# The re-rank also reads the byline as stored, the venue, and everything it may write.
_PICK_READ_COLS = ["byline_orcid", "author_affiliation", "entrez_date", "issn", "journal",
                   "accept_conflict"] + PICK_COLS
# `{unfilled}` is "AND byline_orcid IS NULL" once the column exists, and empty before the
# migration, when every row is unfilled and naming the column would fail the dry run.
_SELECT_SQL = ("SELECT {cols} FROM authorship_review WHERE source IN ('scopus', 'openalex') "
               "AND top_person_type LIKE 'Cornell%' AND doi IS NOT NULL AND doi <> '' "
               "{unfilled} ORDER BY id")
# With the matcher on: the same rows, plus the ones whose ORCID is already stored (by the
# producer or an earlier run) and whose pick has yet to be ranked with it.
_SELECT_PICKS_SQL = ("SELECT {cols} FROM authorship_review WHERE source IN ('scopus', "
                     "'openalex') AND top_person_type LIKE 'Cornell%' AND "
                     "((doi IS NOT NULL AND doi <> '' AND byline_orcid IS NULL) "
                     "OR (byline_orcid IS NOT NULL AND status = 'open')) ORDER BY id")


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


def _key(c):
    """identity_index._rank_key over a STORED candidate, tolerant of a missing field."""
    return _rank_key({"orcid_match": c.get("orcid_match"),
                      "given_match": c.get("given_match"),
                      "affil_dept_match": bool(c.get("affil_dept_match")),
                      "confidence": c.get("confidence") or 0.0})


def _pub_year(row):
    s = str(row.get("entrez_date") or "")
    return int(s[:4]) if s[:4].isdigit() else None


def rerank_row(row, orcid, idx, venue_lookup, run_ts):
    """One open row and its byline ORCID -> (verdict, after). `after` is the PICK_COLS
    payload, or None when the row's pick is left alone. Verdicts are in the module
    docstring; the skips are

      pick skipped: row is not open                 never a decided row
      pick skipped: held in the duplicates queue    accept_conflict is set
      pick skipped: ORCID is on file for nobody     not in the map, or not on the roster
      pick skipped: no stored candidates
      pick skipped: already ranked by ORCID         a candidate carries orcid_match
      pick skipped: WCM candidate on the row        a mixed list (the matcher's rule)"""
    if row.get("status") != "open":
        return "pick skipped: row is not open", None
    if row.get("accept_conflict"):
        return "pick skipped: held in the duplicates queue", None
    rec = idx.orcid_rec.get(orcid)
    if rec is None:
        return "pick skipped: ORCID is on file for nobody", None
    try:
        cands = json.loads(row.get("candidate_cwids_json") or "[]")
    except ValueError:
        cands = None
    if not isinstance(cands, list) or not cands or not all(isinstance(c, dict) for c in cands):
        return "pick skipped: no stored candidates", None
    if any("orcid_match" in c for c in cands):
        return "pick skipped: already ranked by ORCID", None
    if not all(_is_ithaca(c) for c in cands):
        return "pick skipped: WCM candidate on the row", None

    out = [dict(c, orcid_match=_cwid_eq(c.get("cwid"), rec["cwid"])) for c in cands]
    cohort = row.get("top_cohort_size")
    if _cwid_eq(row.get("top_cwid"), rec["cwid"]) and out[0]["orcid_match"]:
        verdict = "ORCID_CONFIRMS"
    elif any(c["orcid_match"] for c in out):
        verdict = "PICK_CHANGE"
    else:
        # Not on the stored list: have the matcher build them from the stored byline.
        # The same call without the ORCID says whether the name reaches them at all (they
        # ranked sixth or lower, so the stored cohort already counts them) or the ORCID
        # is what adds them (and the cohort grows by one).
        verdict = "ORCID_NEW_PERSON"
        toks = str(row.get("wcm_author") or "").split()
        if not toks:
            return "pick skipped: no stored candidates", None
        byline = (toks[-1], " ".join(toks[:-1]) or None, None,
                  [a for a in (row.get("author_affiliation") or "").split(" | ") if a],
                  None, _pub_year(row),
                  scop.stored_venue_subfield(venue_lookup, row.get("issn"), row.get("journal")))
        person = next((c for c in idx.candidates(*byline, orcid)[0] if c["orcid_match"]),
                      None)
        if person is None:                         # a byline with no usable surname
            return "pick skipped: no stored candidates", None
        named = any(_cwid_eq(c["cwid"], rec["cwid"]) for c in idx.candidates(*byline)[0])
        cohort = (cohort or len(cands)) + (0 if named else 1)
        out = [dict(c, cohort_size=cohort) for c in scop._compact([person]) + out]
    out.sort(key=_key, reverse=True)               # stable: ties keep the stored order
    out, top = out[:5], out[0]                     # five, as the producer stores
    return verdict, {
        "top_cwid": top.get("cwid"),
        "top_name": _trunc(top.get("name"), 255),
        "top_person_type": _trunc(top.get("person_type"), 64),
        "top_dept": _trunc(top.get("dept"), 255),
        "top_given_match": top.get("given_match"),
        "top_affil_match": int(bool(top.get("affil_dept_match"))),
        "top_confidence": top.get("confidence"),
        "top_years_after_wcm": top.get("years_after_wcm"),
        "top_venue_fit": top.get("venue_fit"),
        "top_orcid_match": scop.top_orcid_match(top),
        "top_cohort_size": cohort,
        "single_candidate": int(cohort == 1) if cohort else None,
        "n_candidates": len(out),
        "candidate_cwids_json": json.dumps(out),
        "last_refreshed": run_ts,
    }


def with_pick(row, entry, verdict, after, orcid, run_ts):
    """The ledger entry for a re-ranked row: the fill's entry widened to the pick columns,
    or a new one when the ORCID was already stored."""
    if entry is None:
        entry = {"id": row["id"], "source": row["source"], "status": row["status"],
                 "author_key": row["author_key"], "wcm_author": row["wcm_author"],
                 "doi": row["doi"], "before": {"byline_orcid": row.get("byline_orcid")},
                 "after": {"byline_orcid": orcid}, "swept_at": run_ts, "applied": False}
    entry["rule"] = verdict
    entry["before"].update({c: row.get(c) for c in PICK_COLS})
    entry["after"].update(after)
    return entry


def _update_sql():
    """The only write. byline_orcid IS NULL keeps a value the producer wrote since the
    row was read; the source list keeps it off the PubMed lane. No status test: the
    column describes the byline and is filled on resolved rows too."""
    sets = ", ".join(f"{c}=:{c}" for c in WRITE_COLS)
    return (f"UPDATE authorship_review SET {sets} WHERE id=:id AND byline_orcid IS NULL "
            "AND source IN ('scopus', 'openalex')")


def _pick_update_sql():
    """The re-rank's write: the ORCID and the pick together. status='open' and
    accept_conflict IS NULL keep a curator's decision, made or parked; last_refreshed
    <=> the value this run read keeps it off a row the producer rewrote in between."""
    sets = ", ".join(f"{c}=:{c}" for c in WRITE_COLS + PICK_COLS)
    return (f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open' "
            "AND accept_conflict IS NULL "
            "AND source IN ('scopus', 'openalex') AND last_refreshed <=> :seen_refreshed")


def apply_entries(conn, entries):
    """Write each entry and set its `applied` from the UPDATE's own row count, so the
    ledger never claims a row the guard skipped. A re-ranked row (its entry carries the
    pick columns) goes through the pick statement and its guards."""
    fill, pick, n = text(_update_sql()), text(_pick_update_sql()), 0
    for e in entries:
        if "top_cwid" in e["after"]:
            res = conn.execute(pick, dict(e["after"], id=e["id"],
                                          seen_refreshed=e["before"]["last_refreshed"]))
        else:
            res = conn.execute(fill, dict(e["after"], id=e["id"]))
        e["applied"] = res.rowcount == 1
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
    ap.add_argument("--open-only", action="store_true",
                    help="consider only open rows: the re-rank's report, without looking "
                         "up the resolved rows' ORCIDs")
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
        missing = aar_db.missing_optional_cols(c)
        column_ready = "byline_orcid" not in missing
        if args.apply and missing:
            print(f"authorship_review has no {', '.join(missing)} column: apply "
                  "setup/alter_venue_fit_v3.14.sql and "
                  "setup/alter_authorship_review_add_byline_orcid_v3.15.sql first. "
                  "Nothing written.")
            sys.exit(2)
        picks = byline_orcid_enabled()
        sql = _SELECT_PICKS_SQL if picks and column_ready else _SELECT_SQL
        rows = [dict(r) for r in c.execute(text(sql.format(
            cols=", ".join(aar_db.present_cols(c, _READ_COLS + (_PICK_READ_COLS if picks
                                                                else []))),
            unfilled="AND byline_orcid IS NULL" if column_ready else ""))).mappings().all()]
    if args.open_only:
        rows = [r for r in rows if r["status"] == "open"]
    if args.limit:
        rows = rows[:args.limit]
    unfilled = [r for r in rows if not r.get("byline_orcid")]
    dois = {r["doi"] for r in unfilled if r["doi"]}
    print(f"[{'APPLY' if args.apply else 'DRY-RUN'}] {len(unfilled)} Cornell-top external "
          f"rows with a DOI and no byline_orcid; {len(dois)} DOIs, about "
          f"{-(-len(dois) // scop.OPENALEX_DOI_BATCH)} OpenAlex calls", flush=True)
    if not column_ready:
        print("NOTE: authorship_review has no byline_orcid column yet (v3.15 is not "
              "applied); every row counts as unfilled", flush=True)
    idx = venue_lookup = None
    if picks:
        idx = IdentityIndex.load(campus=CAMPUS_ITHACA)
        venue_lookup = scop.load_venue_lookup()
        print(f"  matcher on: {len(rows) - len(unfilled)} more open rows already store an "
              f"ORCID; {len(idx.orcid_rec)} ORCIDs are on file for somebody on the Ithaca "
              f"roster", flush=True)
        if not idx.orcid_rec:
            print("  the ORCID map is empty or could not be read: no row is re-ranked",
                  flush=True)
            idx = None
    else:
        print("  AAR_BYLINE_ORCID is not on in this shell: ORCIDs are filled, no pick is "
              "re-ranked", flush=True)
    by_doi = scop.openalex_authorships(dois)
    print(f"  {len(by_doi)} of {len(dois)} DOIs found in OpenAlex", flush=True)

    counts, pick_counts, entries = Counter(), Counter(), []
    for r in rows:
        entry, orcid = None, r.get("byline_orcid")
        if not orcid:
            verdict, entry = fill_row(r, by_doi, run_ts)
            counts[verdict] += 1
            orcid = entry["after"]["byline_orcid"] if entry else None
        if idx is not None and orcid:
            verdict, after = rerank_row(r, orcid, idx, venue_lookup, run_ts)
            pick_counts[verdict] += 1
            if after:
                entry = with_pick(r, entry, verdict, after, orcid, run_ts)
        if entry:
            entries.append(entry)
    for verdict, n in sorted(counts.items(), key=lambda kv: (kv[0].startswith("skipped"), -kv[1])):
        print(f"  {n:6d}  {verdict}")
    for e in [e for e in entries if e["rule"] == "ORCID_FOUND"][:args.sample]:
        print(f"    id={e['id']} [{e['status']}] {e['wcm_author']!r} {e['doi']} "
              f"-> {e['after']['byline_orcid']}")
    if idx is not None:
        print("\n  re-rank of open rows with an ORCID:")
        for verdict in ("PICK_CHANGE", "ORCID_CONFIRMS", "ORCID_NEW_PERSON"):
            print(f"  {pick_counts[verdict]:6d}  {verdict}")
        for verdict, n in sorted(pick_counts.items(), key=lambda kv: -kv[1]):
            if verdict.startswith("pick skipped"):
                print(f"  {n:6d}  {verdict}")
        for rule in ("PICK_CHANGE", "ORCID_NEW_PERSON"):
            for e in [e for e in entries if e["rule"] == rule][:args.sample]:
                b, a = e["before"], e["after"]
                print(f"    {rule} id={e['id']} {e['wcm_author']!r} {a['byline_orcid']}\n"
                      f"      {b['top_cwid']} {b['top_name']!r} ({b['top_given_match']}, "
                      f"conf {b['top_confidence']})\n"
                      f"   -> {a['top_cwid']} {a['top_name']!r} ({a['top_given_match']}, "
                      f"conf {a['top_confidence']})")

    n = ledger_then_apply(eng, entries, ledger, args.apply)
    if args.apply and entries:
        print(f"\nAPPLIED: {n} of {len(entries)} rows updated"
              + (f" ({len(entries) - n} skipped: filled or rewritten by the producer, or "
                 f"resolved by a curator, since they were read)" if n != len(entries) else ""))
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

    # ---- the re-rank of open rows (matcher on) ---------------------------------
    from identity_index import _norm, orcid_map
    O_ROBERT, O_LIND, O_SIXTH, O_NOBODY = (f"0000-0002-0000-000{k}" for k in range(1, 5))

    def person(given, surname, cwid):
        return {"cwid": cwid, "given": given, "middle": "", "surname": surname,
                "given_norm": _norm(given), "surname_norm": _norm(surname), "pref_norm": "",
                "pref_full": "", "dept": "Physics", "division": "", "program": "",
                "title": "", "person_type": "Cornell Faculty", "historical": False,
                "end_year": None, "campus": CAMPUS_ITHACA}

    people = [person("Rhea", "Weiss", "rh_c"), person("Robert", "Weiss", "rw_c"),
              person("Rex", "Weiss", "rx_c"), person("Rhea", "Lindqvist", "rl_c")]
    idx = IdentityIndex(people, campus=CAMPUS_ITHACA,
                        orcid_cwid=orcid_map([("rw_c", O_ROBERT), ("rl_c", O_LIND)]))
    affil = ["Cornell University, Ithaca, NY"]

    def stored(orcid=None, index=idx, fore="Rhea", **kw):
        """The open row the producer wrote WITHOUT the matcher, for byline `fore` Weiss."""
        cands = scop._compact(index.candidates("Weiss", fore, None, affil)[0])
        top = cands[0]
        return dict(dict(row(wcm_author=f"{fore} Weiss"), byline_orcid=orcid,
                    author_affiliation=" | ".join(affil), entrez_date="2024-05-01",
                    issn=None, journal="J", accept_conflict=None, top_cwid=top["cwid"],
                    top_name=top["name"], top_person_type=top["person_type"],
                    top_dept=top["dept"], top_given_match=top["given_match"],
                    top_affil_match=int(bool(top["affil_dept_match"])),
                    top_confidence=top["confidence"], top_years_after_wcm=None,
                    top_venue_fit=None, top_orcid_match=None,
                    top_cohort_size=top["cohort_size"],
                    single_candidate=int(top["cohort_size"] == 1), n_candidates=len(cands),
                    candidate_cwids_json=json.dumps(cands),
                    last_refreshed="2026-11-01 00:00:00"), **kw)

    def produced(orcid, index=idx, fore="Rhea"):
        """What the producer writes for the same byline WITH the matcher."""
        return scop._compact(index.candidates("Weiss", fore, None, affil, 5, 2024, None,
                                              orcid)[0])

    verdict, after = rerank_row(stored(), O_ROBERT, idx, None, ts)
    check("PICK_CHANGE: the ORCID is on file for a lower-ranked stored candidate, who now "
          "leads a full given-name match; cohort and candidate count do not move",
          verdict == "PICK_CHANGE" and after["top_cwid"] == "rw_c"
          and after["top_orcid_match"] == 1 and after["top_given_match"] == "initial"
          and (after["top_cohort_size"], after["single_candidate"], after["n_candidates"])
          == (3, 0, 3) and after["last_refreshed"] == ts and set(after) == set(PICK_COLS))
    check("...and the stored list is byte for byte the one the matcher writes with the ORCID",
          after["candidate_cwids_json"] == json.dumps(produced(O_ROBERT)))

    verdict, after = rerank_row(stored(fore="Robert"), O_ROBERT, idx, None, ts)
    check("ORCID_CONFIRMS: the ORCID is on file for the top pick; nobody moves, "
          "orcid_match is recorded and top_orcid_match set",
          verdict == "ORCID_CONFIRMS" and after["top_cwid"] == "rw_c"
          and after["top_orcid_match"] == 1
          and after["candidate_cwids_json"] == json.dumps(produced(O_ROBERT, fore="Robert"))
          and [c["orcid_match"] for c in json.loads(after["candidate_cwids_json"])]
          == [True, False, False])

    verdict, after = rerank_row(stored(), O_LIND, idx, None, ts)
    check("ORCID_NEW_PERSON: the ORCID is on file for somebody the name did not reach; the "
          "matcher builds them, they lead, and the cohort and candidate count grow by one",
          verdict == "ORCID_NEW_PERSON" and after["top_cwid"] == "rl_c"
          and after["top_name"] == "Rhea Lindqvist" and after["top_given_match"] == "full"
          and after["top_orcid_match"] == 1
          and (after["top_cohort_size"], after["single_candidate"], after["n_candidates"])
          == (4, 0, 4))
    check("...again byte for byte the matcher's own list for that byline and ORCID",
          after["candidate_cwids_json"] == json.dumps(produced(O_LIND)))
    solo_idx = IdentityIndex([people[0], people[3]], campus=CAMPUS_ITHACA,
                             orcid_cwid=orcid_map([("rl_c", O_LIND)]))
    verdict, after = rerank_row(stored(index=solo_idx), O_LIND, solo_idx, None, ts)
    check("...and a single-candidate row stops being one",
          verdict == "ORCID_NEW_PERSON" and after["single_candidate"] == 0
          and after["top_cohort_size"] == 2
          and after["candidate_cwids_json"] == json.dumps(produced(O_LIND, solo_idx)))

    crowd = IdentityIndex([person(f"R{k}x", "Weiss", f"r{k}_c") for k in range(7)],
                          campus=CAMPUS_ITHACA, orcid_cwid=orcid_map([("r6_c", O_SIXTH)]))
    verdict, after = rerank_row(stored(index=crowd, fore="R"), O_SIXTH, crowd, None, ts)
    kept = json.loads(after["candidate_cwids_json"])
    check("ORCID_NEW_PERSON for a homonym the producer ranked below the top five: they "
          "lead, the list stays five, and the cohort of seven already counted them",
          verdict == "ORCID_NEW_PERSON" and [c["cwid"] for c in kept][0] == "r6_c"
          and len(kept) == after["n_candidates"] == 5 and after["top_cohort_size"] == 7
          and all(c["cohort_size"] == 7 for c in kept)
          and after["candidate_cwids_json"] == json.dumps(produced(O_SIXTH, crowd, "R")))

    check("a row that is not open is never re-ranked, whatever its ORCID says",
          all(rerank_row(stored(status=s), O_ROBERT, idx, None, ts)
              == ("pick skipped: row is not open", None)
              for s in ("accepted", "assigned", "rejected", "dismissed", "snoozed")))
    check("a row held in the duplicates queue (accept_conflict) is left alone",
          rerank_row(stored(accept_conflict="title+year collision"), O_ROBERT, idx, None, ts)
          == ("pick skipped: held in the duplicates queue", None))
    check("an ORCID on file for nobody on the roster moves nothing",
          rerank_row(stored(), O_NOBODY, idx, None, ts)
          == ("pick skipped: ORCID is on file for nobody", None))
    wcm_c = dict(json.loads(stored()["candidate_cwids_json"])[0], cwid="wcm1",
                 person_type="Full-Time Faculty")
    wcm_c.pop("campus")
    check("a row with a WCM candidate on it is left as the matcher leaves it",
          rerank_row(stored(candidate_cwids_json=json.dumps(
              json.loads(stored()["candidate_cwids_json"]) + [wcm_c])), O_ROBERT, idx, None, ts)
          == ("pick skipped: WCM candidate on the row", None))
    again = stored(candidate_cwids_json=rerank_row(stored(), O_ROBERT, idx, None, ts)[1][
        "candidate_cwids_json"])
    check("a row already ranked by its ORCID is skipped, which makes a second run a no-op",
          rerank_row(again, O_ROBERT, idx, None, ts)
          == ("pick skipped: already ranked by ORCID", None))
    check("empty, null and broken candidate JSON are skipped, not crashed on",
          all(rerank_row(stored(candidate_cwids_json=j), O_ROBERT, idx, None, ts)
              == ("pick skipped: no stored candidates", None)
              for j in ("[]", None, "", "{}", "[1, 2]", "[{")))

    # the ledger entry and the write
    r0 = stored()
    _v, e_fill = fill_row(r0, by_doi, ts)
    verdict, after = rerank_row(r0, O_ROBERT, idx, None, ts)
    e = with_pick(r0, e_fill, verdict, after, O_ROBERT, ts)
    check("a re-ranked row's ledger entry carries the before and after of the ORCID and of "
          "every pick column, under the re-rank's verdict",
          e["rule"] == "PICK_CHANGE" and set(e["before"]) == set(e["after"])
          == set(WRITE_COLS + PICK_COLS) and e["before"]["top_cwid"] == "rh_c"
          and e["after"]["top_cwid"] == "rw_c" and e["before"]["byline_orcid"] is None)
    e_stored = with_pick(stored(orcid=O_ROBERT), None, verdict, after, O_ROBERT, ts)
    check("...and a row whose ORCID was already stored gets an entry of its own",
          e_stored["before"]["byline_orcid"] == e_stored["after"]["byline_orcid"] == O_ROBERT
          and e_stored["applied"] is False)
    psql = _pick_update_sql()
    check("the re-rank's UPDATE re-checks status='open', that no Accept is parked on the "
          "row, the external sources, and that the producer has not rewritten it",
          psql.endswith("WHERE id=:id AND status='open' AND accept_conflict IS NULL "
                        "AND source IN ('scopus', 'openalex') "
                        "AND last_refreshed <=> :seen_refreshed"))
    pset = psql.split(" WHERE ")[0]
    check("...and writes exactly the ORCID and the pick columns, all producer-owned, and "
          "no curator column",
          pset.count("=:") == len(WRITE_COLS + PICK_COLS)
          and all(f"{c}=:{c}" in pset for c in WRITE_COLS + PICK_COLS)
          and all(c in aar_db._REFRESH_COLS for c in PICK_COLS)
          and not any(c in pset for c in ("status", "resolution_cwid", "reviewer", "note",
                                          "snooze_until")))

    class _Conn:
        def __init__(self):
            self.calls, self.rowcount = [], 1

        def execute(self, stmt, params):
            self.calls.append((str(stmt), params))
            return self
    conn = _Conn()
    n = apply_entries(conn, [fill_row(row(id=5, status="accepted"), by_doi, ts)[1], e])
    check("a fill goes through the fill statement, a re-rank through the guarded one, "
          "bound to the last_refreshed that was read",
          n == 2 and "byline_orcid IS NULL" in conn.calls[0][0]
          and "status" not in conn.calls[0][0]
          and "status='open'" in conn.calls[1][0]
          and conn.calls[1][1]["seen_refreshed"] == "2026-11-01 00:00:00"
          and conn.calls[1][1]["top_cwid"] == "rw_c"
          and conn.calls[1][1]["byline_orcid"] == "0000-0001-0000-0001")
    check("the re-rank reads open rows whose ORCID is already stored as well as the "
          "unfilled ones",
          "byline_orcid IS NOT NULL AND status = 'open'" in _SELECT_PICKS_SQL
          and "byline_orcid IS NULL" in _SELECT_PICKS_SQL
          and "top_person_type LIKE 'Cornell%'" in _SELECT_PICKS_SQL)

    a = default_ledger_path(datetime(2026, 11, 17, 9, 30, 5), True)
    b = default_ledger_path(datetime(2026, 11, 17, 9, 31, 0), False)
    check("each run's default ledger is its own file",
          a != b and a.endswith("backfill_byline_orcid_ledger_20261117_093005_apply.jsonl")
          and b.endswith("backfill_byline_orcid_ledger_20261117_093100_dryrun.jsonl"))

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
