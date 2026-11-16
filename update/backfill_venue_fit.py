#!/usr/bin/env python3
"""
One-off backfill of the venue term onto OPEN Scopus and OpenAlex rows whose top pick is
a Cornell person (v3.14).

WHY THIS EXISTS. Producer columns do not self-heal. The lanes write a row's proposal
when they sweep the document, and a Scopus sweep only reaches documents loaded in the
last 90 days, so setting AAR_VENUE_FIT=on changes the rows written from then on and
leaves the open queue exactly as it was. On 2026-11-16 that queue held about 4,000 open
Cornell rows in the one class (byline spells out a different given name) where venue fit
separates right from wrong most sharply. This pass brings the rows already in the queue
to what the matcher would say about them now.

WHAT IT DOES, per row, from what the row already stores and nothing else:
  1. resolves the venue's subfield from the stored issn / journal (venue_fit.subfield_for);
  2. for each stored candidate, looks up the department's fit for that subfield with the
     candidate's own accepted rows left out (venue_fit.DeptVenueFit), records it as
     `venue_fit` and adds the venue term to `confidence`;
  3. re-sorts the stored candidates with the matcher's own key (identity_index._rank_key:
     full given name, affiliation match, confidence);
  4. rewrites the top_* columns from whoever is first, and the candidate list.

NO SCOPUS RE-FETCH, and that sets its limits:
  * Only the candidates the row stored can be re-ranked: at most five. Someone the
    producer ranked sixth is not in candidate_cwids_json and cannot be promoted here,
    however well the venue fits. n_candidates is left alone for the same reason.
  * A row with a WCM candidate on it is SKIPPED, as the matcher leaves it alone
    (identity_index.CampusRoster): only the Cornell side could earn the term, so a mixed
    list carries none. The stored list is the top five, so a row whose only WCM
    candidates ranked sixth or lower reads here as all-Cornell and is scored where the
    producer would not score it; the next sweep that reaches it writes the producer's
    answer.
  * `confidence` is the stored value plus the term, clipped. The matcher clips once,
    after adding every term, and the stored value has already been clipped; the two
    agree except at the 1.0 ceiling, where the stored value is rebuilt from the
    candidate's own stored evidence before the term is added (see `rescored`).

A row any of whose candidates already carries `venue_fit` is SKIPPED. Its confidence
already contains the term, written by the producer or by an earlier run of this script,
and adding it again would double it. That is what makes a second run a no-op.

A row held in Publication Manager's duplicates queue (accept_conflict set) is left
alone: a curator's Accept is parked on the person the row names.

SAFETY, the same rules as aar_reconcile_open.py:
  * Dry run by default. It reads, counts, prints a sample and writes a JSONL ledger with
    the full before and after of every row it would touch.
  * The ledger is on disk BEFORE the first UPDATE and rewritten after it with each
    row's `applied`. A ledger that cannot be written stops the run with nothing changed.
    Every run writes its own file (the default name carries the time and the mode) and
    an existing file is never overwritten, so the dry run that follows an apply cannot
    truncate the record of it.
  * --apply writes producer-owned columns only (WRITE_COLS, checked at import against
    aar_db._REFRESH_COLS). It never names status, resolution_cwid, reviewer, note or
    snooze_until.
  * The UPDATE re-checks status='open', so a row a curator resolved between the SELECT
    and the write is skipped, not overwritten; and it re-checks last_refreshed, so a row
    the nightly producer rewrote in between is skipped too. `applied` in the ledger is
    the UPDATE's own row count, per row.
  * --apply refuses until setup/alter_venue_fit_v3.14.sql is applied.

The --apply run is a prod write; it is handed to the owner to run, out of hours and not
alongside the nightly (14:30 UTC).

ORDER. Dry-run at any time. Set AAR_VENUE_FIT=on on the CronJob BEFORE --apply, not
after: while the feature is off every sweep re-upserts the open rows in its window with
no term and top_venue_fit NULL, which would undo this pass for those rows and move their
picks twice.

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb). AAR_VENUE_FIT is not read: this
is the explicit tool, and its own environment says nothing about the CronJob's.

Usage:
  python3 backfill_venue_fit.py --selftest          # offline checks, no DB
  python3 backfill_venue_fit.py                     # dry run: counts, sample, ledger
  python3 backfill_venue_fit.py --apply
"""
import argparse, json, os, sys
from collections import Counter
from datetime import datetime

from sqlalchemy import text

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import aar_db                                                    # noqa: E402
import venue_fit                                                 # noqa: E402
from identity_index import (IdentityIndex, CAMPUS_ITHACA,        # noqa: E402
                            CORNELL_PERSON_TYPES, _rank_key, temporal_penalty)

# Everything --apply may write. top_cohort_size, single_candidate and n_candidates are
# not here: they describe the cohort, which a re-sort of the same people does not change.
WRITE_COLS = ["top_cwid", "top_name", "top_person_type", "top_dept", "top_given_match",
              "top_affil_match", "top_confidence", "top_years_after_wcm", "top_venue_fit",
              "candidate_cwids_json", "last_refreshed"]
assert all(c in aar_db._REFRESH_COLS for c in WRITE_COLS), (
    "WRITE_COLS names a column the producer itself does not refresh: "
    f"{[c for c in WRITE_COLS if c not in aar_db._REFRESH_COLS]}")
assert not (set(WRITE_COLS) & {"status", "resolution_cwid", "reviewer", "note",
                               "snooze_until", "resolved_at", "matched_pmid_verdict"}), (
    "WRITE_COLS names a curator-owned column -- this backfill must never write one")

_READ_COLS = ["id", "source", "author_key", "wcm_author", "issn", "journal"] + [
    c for c in WRITE_COLS if c != "top_venue_fit"]
_SELECT_SQL = ("SELECT {cols} FROM authorship_review WHERE status='open' "
               "AND source IN ('scopus', 'openalex') AND top_person_type LIKE 'Cornell%' "
               "AND candidate_cwids_json IS NOT NULL AND accept_conflict IS NULL ORDER BY id")
# Cornell labels whose holder is `historical` whatever else they hold (identity_index
# CORNELL_HISTORICAL_TYPES, by label because the label is what a stored candidate keeps).
_TERMINAL_LABELS = frozenset(label for _t, label, hist in CORNELL_PERSON_TYPES if hist)


def _trunc(s, n):
    return s[:n] if isinstance(s, str) and len(s) > n else s


def _cwid_eq(a, b):
    """Case-folded, as in aar_report_changed_picks: top_cwid's collation is
    case-insensitive, so two spellings of one cwid are not a changed pick."""
    if a is None or b is None:
        return a is b
    return str(a).strip().lower() == str(b).strip().lower()


def _unclipped(c):
    """A stored candidate's confidence before the clip and before any venue term, rebuilt
    from its own stored evidence with identity_index._confidence's arithmetic. Used only
    at the ceiling. `historical` is read off the stored person-type label: a terminal
    label is certainly historical; an emeritus who still holds a current role is stored
    under the current label and reads as not, which over-states them by 0.10. That one
    case (full name, cohort of one, affiliation match, hidden emeritus) is this
    function's known error. The selftest holds the arithmetic to _confidence's."""
    base = (0.50 if c.get("given_match") == "full"
            else 0.25 if c.get("given_match") == "initial" else 0.15)
    rarity = 0.40 / max(int(c.get("cohort_size") or 1), 1)
    affil = 0.25 if c.get("affil_dept_match") else 0.0
    hist = -0.10 if c.get("person_type") in _TERMINAL_LABELS else 0.0
    return base + rarity + affil + hist - temporal_penalty(c.get("years_after_wcm"))


def rescored(c, term):
    """The candidate's confidence with the venue term added: stored + term, clipped to
    [0, 1] and rounded as _confidence rounds.

    The matcher adds the term BEFORE its clip. A stored 1.0 may stand for 1.15 (full
    name, cohort of one, affiliation match), and 1.0 - 0.20 = 0.80 is not the 0.95 the
    matcher would write, so at the ceiling the pre-clip value is rebuilt first. Below
    the ceiling the stored value is the pre-clip value. At the 0.0 floor it is not, and
    nothing is rebuilt: such a candidate sits at the bottom of a deep cohort either way."""
    stored = float(c.get("confidence") or 0.0)
    pre = max(stored, _unclipped(c)) if stored >= 1.0 else stored
    return round(max(0.0, min(1.0, pre + term)), 3)


def _key(c):
    """identity_index._rank_key over a STORED candidate, tolerant of a missing field."""
    return _rank_key({"given_match": c.get("given_match"),
                      "affil_dept_match": bool(c.get("affil_dept_match")),
                      "confidence": c.get("confidence") or 0.0})



def _is_ithaca(c):
    """Is this stored candidate a Cornell Ithaca one? The `campus` key says so on rows
    written since it existed. The 2026-09-26 initial Ithaca load predates the key (7,016
    of the 7,340 open rows from that day lack it), and reading "no key" as WCM skipped
    them all. Every Cornell person-type label starts with "Cornell"
    (identity_index.CORNELL_PERSON_TYPES and the "Cornell Ithaca" fallback); no WCM
    label does."""
    return (c.get("campus") == CAMPUS_ITHACA
            or str(c.get("person_type") or "").startswith("Cornell"))


def replay_row(row, lookup, table, run_ts):
    """One open row -> (verdict, entry). `verdict` names what would happen; `entry` is
    the ledger record with the full before/after, or None when the row is left alone.

      skipped: no stored candidates      nothing to re-rank
      skipped: already has venue fit     the term is already in its confidence
      skipped: WCM candidate on the row  a mixed list carries no term (the matcher's rule)
      skipped: venue has no category     no subfield, so no term (the matcher's answer too)
      skipped: no scorable department    no Cornell candidate's department has 20 rows
      PICK_CHANGE                        a different person leads the row
      CONFIDENCE_CHANGE                  same person, top_confidence moved
      FIT_RECORDED                       same person, same confidence; the fit is
                                         between the thresholds, or the term fell on
                                         another candidate"""
    try:
        cands = json.loads(row["candidate_cwids_json"] or "[]")
    except ValueError:
        cands = None
    if not isinstance(cands, list) or not cands or not all(isinstance(c, dict) for c in cands):
        return "skipped: no stored candidates", None
    if any("venue_fit" in c for c in cands):
        return "skipped: already has venue fit", None
    if not all(_is_ithaca(c) for c in cands):
        return "skipped: WCM candidate on the row", None
    subfield = venue_fit.subfield_for(lookup, row["issn"], row["journal"])
    if not subfield:
        return "skipped: venue has no category", None

    scored, out = 0, []
    for c in cands:
        fit = table.fit(c.get("dept"), subfield, c.get("cwid"))
        if fit is None:
            out.append(c)
            continue
        scored += 1
        out.append(dict(c, confidence=rescored(c, table.term(fit)), venue_fit=fit))
    if not scored:
        return "skipped: no scorable department", None

    out.sort(key=_key, reverse=True)           # stable: ties keep the stored order
    top = out[0]
    after = {
        "top_cwid": top.get("cwid"),
        "top_name": _trunc(top.get("name"), 255),
        "top_person_type": _trunc(top.get("person_type"), 64),
        "top_dept": _trunc(top.get("dept"), 255),
        "top_given_match": top.get("given_match"),
        "top_affil_match": int(bool(top.get("affil_dept_match"))),
        "top_confidence": top.get("confidence"),
        "top_years_after_wcm": top.get("years_after_wcm"),
        "top_venue_fit": top.get("venue_fit"),
        "candidate_cwids_json": json.dumps(out),
        "last_refreshed": run_ts,
    }
    before = {c: row.get(c) for c in WRITE_COLS}
    if not _cwid_eq(row["top_cwid"], after["top_cwid"]):
        verdict = "PICK_CHANGE"
    elif (row["top_confidence"] is None
          or abs(float(row["top_confidence"]) - float(after["top_confidence"] or 0)) > 5e-4):
        verdict = "CONFIDENCE_CHANGE"        # 5e-4: half the 0.001 quantum, under FLOAT noise
    else:
        verdict = "FIT_RECORDED"
    return verdict, {"id": row["id"], "source": row["source"], "author_key": row["author_key"],
                     "wcm_author": row["wcm_author"], "journal": row["journal"],
                     "subfield": subfield, "rule": verdict, "before": before, "after": after,
                     "swept_at": run_ts, "applied": False}


def _update_sql():
    """The only write. status='open' and accept_conflict IS NULL keep a curator's
    decision, made or parked; the source list keeps it off the PubMed lane;
    last_refreshed <=> the value this run read keeps it off a row the nightly producer
    rewrote in between (NULL-safe: old rows carry none)."""
    sets = ", ".join(f"{c}=:{c}" for c in WRITE_COLS)
    return (f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open' "
            "AND accept_conflict IS NULL "
            "AND source IN ('scopus', 'openalex') AND last_refreshed <=> :seen_refreshed")


def apply_entries(conn, entries):
    """Write each entry and set its `applied` from the UPDATE's own row count, so the
    ledger never claims a row the guards skipped. One statement per row: a few thousand
    rows, once, and a per-row count is the point."""
    stmt, n = text(_update_sql()), 0
    for e in entries:
        params = dict(e["after"], id=e["id"], seen_refreshed=e["before"]["last_refreshed"])
        e["applied"] = conn.execute(stmt, params).rowcount == 1
        n += e["applied"]
    return n


def _write_ledger(path, entries):
    """Whole file or nothing: written beside `path` and moved into place, so a failed
    rewrite leaves the previous complete ledger where it was."""
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        for e in entries:
            fh.write(json.dumps(e, default=str) + "\n")
    os.replace(tmp, path)


def ledger_then_apply(eng, entries, path, apply_writes):
    """The ledger, then the write, then the ledger again -- in that order, as
    aar_reconcile_open does it. Returns the number of rows updated.

    The first write is the before/after of every row with applied=false, and it happens
    before any UPDATE: if it fails the run stops having changed nothing, and if the run
    dies later the before-state of every row it may have touched is already on disk. The
    second records what the UPDATEs did, per row. Should that one fail, the rows ARE
    written and the file still holds their full before/after with applied=false, which
    is said out loud before the error is re-raised."""
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
    return os.path.join(HERE, f"backfill_venue_fit_ledger_{now:%Y%m%d_%H%M%S}_"
                              f"{'apply' if apply_writes else 'dryrun'}.jsonl")


def _print_sample(entries, rule, n):
    hits = [e for e in entries if e["rule"] == rule]
    print(f"\n  {rule}: {len(hits)}" + (f" (first {min(n, len(hits))})" if hits else ""))
    for e in hits[:n]:
        b, a = e["before"], e["after"]
        print(f"    id={e['id']} {e['wcm_author']!r} in {(e['journal'] or '')[:50]!r} "
              f"[{e['subfield']}]\n"
              f"      {b['top_cwid']} {b['top_name']!r} ({b['top_dept']}) "
              f"conf {b['top_confidence']}\n"
              f"   -> {a['top_cwid']} {a['top_name']!r} ({a['top_dept']}) "
              f"conf {a['top_confidence']} fit {a['top_venue_fit']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--ledger", default=None,
                    help="JSONL ledger of every row touched, before and after (default: "
                         "backfill_venue_fit_ledger_<time>_<dryrun|apply>.jsonl beside this "
                         "script; an existing file is refused, never overwritten)")
    ap.add_argument("--limit", type=int, default=None, help="consider only the first N rows")
    ap.add_argument("--sample", type=int, default=15, help="rows to print per class")
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
        column_ready = not aar_db.missing_optional_cols(c)
        if args.apply and not column_ready:
            print("authorship_review has no top_venue_fit column: apply "
                  "setup/alter_venue_fit_v3.14.sql first. Nothing written.")
            sys.exit(2)
        lookup = venue_fit.load_venue_lookup(c)
        table = venue_fit.load_dept_fit(c, CAMPUS_ITHACA, lookup)
        cols = _READ_COLS + (["top_venue_fit"] if column_ready else [])
        rows = [dict(r) for r in c.execute(text(_SELECT_SQL.format(cols=", ".join(cols))))
                .mappings().all()]
    if args.limit:
        rows = rows[:args.limit]
    scorable = sum(1 for n in table.n_d.values() if n >= venue_fit.MIN_DEPT)
    print(f"[{'APPLY' if args.apply else 'DRY-RUN'}] {len(rows)} open Cornell-top external "
          f"rows; {len(lookup)} venues with a category; fit table: {table.total} training "
          f"rows, {scorable} of {len(table.n_d)} departments scorable", flush=True)

    counts, entries = Counter(), []
    for r in rows:
        verdict, entry = replay_row(r, lookup, table, run_ts)
        counts[verdict] += 1
        if entry:
            entries.append(entry)
    for verdict, n in sorted(counts.items(), key=lambda kv: (kv[0].startswith("skipped"), -kv[1])):
        print(f"  {n:6d}  {verdict}")
    for rule in ("PICK_CHANGE", "CONFIDENCE_CHANGE"):
        _print_sample(entries, rule, args.sample)

    n = ledger_then_apply(eng, entries, ledger, args.apply)
    if args.apply and entries:
        print(f"\nAPPLIED: {n} of {len(entries)} rows updated"
              + (f" ({len(entries) - n} skipped: resolved by a curator or rewritten by the "
                 f"producer since they were read)" if n != len(entries) else ""))
    print(f"\nledger: {ledger} ({len(entries)} rows)")
    if args.apply:
        print("AAR_VENUE_FIT must be on for the CronJob, or its next sweep rewrites these "
              "rows without the term (see ORDER in this script's docstring).")
    else:
        print("DRY-RUN: nothing written (pass --apply)")


# ---- offline self-test ----------------------------------------------------------
def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    # Physics: 20 rows in Optics. Law School: 100 in Law.
    #   fit(Physics, Optics) = 1.586 (+0.15)    fit(Physics, Law) = -1.609 (-0.20)
    #   fit(Law School, Law) = 0.189 (0)        fit(Law School, Optics) = -3.045 (-0.20)
    table = venue_fit.DeptVenueFit([("Physics", "Optics", 20), ("Law School", "Law", 100)])
    lookup = venue_fit.build_venue_lookup(
        [(100001, "0146-9592", None, "Optics"), (100002, "0010-8847", None, "Law")],
        [("cleo", "Optics")])

    def cand(cwid, dept, given_match="initial", confidence=0.45, cohort=2, affil=False,
             campus=CAMPUS_ITHACA, person_type="Cornell Faculty", **kw):
        c = {"cwid": cwid, "name": cwid.upper(), "name_source": "directory", "name_n": None,
             "person_type": person_type, "dept": dept, "given_match": given_match,
             "affil_dept_match": affil, "cohort_size": cohort, "confidence": confidence,
             "years_after_wcm": None}
        if campus:
            c["campus"] = campus
        return dict(c, **kw)

    def row(cands, issn="0146-9592", journal="Optics Letters", **kw):
        top = cands[0]
        return dict({"id": 1, "source": "scopus", "author_key": "scopus:10.1/x:0",
                     "wcm_author": "R Weiss", "issn": issn, "journal": journal,
                     "top_cwid": top["cwid"], "top_name": top["name"],
                     "top_person_type": top["person_type"], "top_dept": top["dept"],
                     "top_given_match": top["given_match"],
                     "top_affil_match": int(top["affil_dept_match"]),
                     "top_confidence": top["confidence"], "top_years_after_wcm": None,
                     "top_venue_fit": None, "candidate_cwids_json": json.dumps(cands),
                     "last_refreshed": "2026-11-01 00:00:00"}, **kw)

    ts = "2026-11-16 12:00:00"
    law, phys = cand("rh1", "Law School"), cand("rw2", "Physics")

    # --- a tie the venue breaks: the Law School led on stored order, the paper is in Optics
    verdict, e = replay_row(row([law, phys]), lookup, table, ts)
    after = e["after"]
    stored = json.loads(after["candidate_cwids_json"])
    check("a tied initial-tier row re-ranks to the department the venue fits: PICK_CHANGE",
          verdict == "PICK_CHANGE" and after["top_cwid"] == "rw2"
          and [c["cwid"] for c in stored] == ["rw2", "rh1"])
    check("...with the term in each candidate's confidence and the fit recorded on each",
          [(c["confidence"], c["venue_fit"]) for c in stored] == [(0.6, 1.586), (0.25, -3.045)])
    check("...and every top_* column follows the new first candidate",
          after == {"top_cwid": "rw2", "top_name": "RW2", "top_person_type": "Cornell Faculty",
                    "top_dept": "Physics", "top_given_match": "initial", "top_affil_match": 0,
                    "top_confidence": 0.6, "top_years_after_wcm": None,
                    "top_venue_fit": 1.586, "candidate_cwids_json": json.dumps(stored),
                    "last_refreshed": ts})
    check("the ledger entry carries the full before and after of exactly the written columns",
          set(e["before"]) == set(e["after"]) == set(WRITE_COLS)
          and e["before"]["top_cwid"] == "rh1" and e["before"]["top_venue_fit"] is None
          and e["applied"] is False and e["subfield"] == "Optics")
    check("a stored candidate keeps its keys and order, venue_fit appended last, as the "
          "producer's _compact writes it",
          list(stored[0]) == list(phys) + ["venue_fit"])

    # --- same pick, confidence moves
    verdict, e = replay_row(row([phys, law]), lookup, table, ts)
    check("the pick the venue agrees with stays and gains 0.15: CONFIDENCE_CHANGE",
          verdict == "CONFIDENCE_CHANGE" and e["after"]["top_cwid"] == "rw2"
          and e["after"]["top_confidence"] == 0.6)
    verdict, e = replay_row(row([law], issn="0010-8847", journal="Cornell Law Review"),
                            lookup, table, ts)
    check("a fit between the thresholds is recorded and moves nothing: FIT_RECORDED",
          verdict == "FIT_RECORDED" and e["after"]["top_confidence"] == 0.45
          and e["after"]["top_venue_fit"] == 0.189)

    # --- the tiers hold
    full_bad = cand("rw2", "Physics", given_match="full", confidence=0.70)
    init_good = cand("rh1", "Law School", confidence=0.45)
    table2 = venue_fit.DeptVenueFit([("Physics", "Optics", 100), ("Law School", "Law", 20)])
    verdict, e = replay_row(row([full_bad, init_good], issn="0010-8847"), lookup, table2, ts)
    check("a full given-name match keeps the row against a rival the venue favours "
          "(0.50 vs 0.60): the sort is the matcher's _rank_key",
          verdict == "CONFIDENCE_CHANGE" and e["after"]["top_cwid"] == "rw2"
          and [c["confidence"] for c in json.loads(e["after"]["candidate_cwids_json"])]
          == [0.5, 0.6])
    aff_bad = cand("rw2", "Physics", confidence=0.70, affil=True)
    verdict, e = replay_row(row([aff_bad, init_good], issn="0010-8847"), lookup, table2, ts)
    check("...and so does an affiliation match", e["after"]["top_cwid"] == "rw2"
          and e["after"]["top_affil_match"] == 1)

    # --- two campuses: a mixed list carries no term, so the row is left as it is
    wcm = cand("wcm1", "Physics", campus=None, person_type="Full-Time Faculty", confidence=0.45)
    check("a row with a WCM candidate on it is skipped wherever that candidate ranks: "
          "the Cornell pick is not penalised below a rival the venue cannot touch",
          replay_row(row([law, wcm]), lookup, table, ts)
          == replay_row(row([phys, law, wcm]), lookup, table, ts)
          == ("skipped: WCM candidate on the row", None))

    # --- rows stored before candidates carried `campus` (the 2026-09-26 Ithaca load)
    old = [dict((k, v) for k, v in c.items() if k != "campus") for c in (phys, law)]
    check("Cornell candidates stored without a `campus` key are still Cornell: the row is "
          "scored as the same row with the key is, not skipped as a WCM one",
          replay_row(row(old), lookup, table, ts)[0]
          == replay_row(row([phys, law]), lookup, table, ts)[0]
          != "skipped: WCM candidate on the row")

    # --- the candidate's own accepted rows are left out of their fit
    own = venue_fit.DeptVenueFit([("Physics", "Optics", 15, "rw2"), ("Physics", "Optics", 5, "zz"),
                                  ("Law School", "Law", 100)])
    verdict, e = replay_row(row([law, phys, cand("rx4", "Physics")]), lookup, own, ts)
    check("the fit is asked for with the candidate's cwid: the person whose own rows carry "
          "the department gets none, a colleague gets the department's",
          [(c["cwid"], c.get("venue_fit"), c["confidence"])
           for c in json.loads(e["after"]["candidate_cwids_json"])]
          == [("rx4", 1.586, 0.6), ("rw2", None, 0.45), ("rh1", -3.045, 0.25)])

    # --- rows left alone
    check("a row whose candidates already carry venue_fit is skipped (the term is not "
          "added twice), which makes a second run a no-op",
          replay_row(row([dict(phys, venue_fit=1.586, confidence=0.6), law]),
                     lookup, table, ts) == ("skipped: already has venue fit", None)
          and replay_row(row(json.loads(after["candidate_cwids_json"])), lookup, table, ts)[1]
          is None)
    check("a venue with no category is skipped: unmapped ISSN, or an unknown title",
          replay_row(row([law, phys], issn="9999-9999"), lookup, table, ts)
          == ("skipped: venue has no category", None)
          and replay_row(row([law, phys], issn=None, journal="Never Seen"), lookup, table, ts)
          == ("skipped: venue has no category", None))
    check("a no-ISSN row resolves through the normalised title",
          replay_row(row([law, phys], issn=None, journal="CLEO 2024"), lookup, table, ts)[0]
          == "PICK_CHANGE")
    check("no candidate in a department with 20 training rows -> skipped, nothing invented",
          replay_row(row([cand("x1", "Hotel Administration"), cand("x2", "")]),
                     lookup, table, ts) == ("skipped: no scorable department", None))
    check("empty, null, non-list and broken candidate JSON are skipped, not crashed on",
          all(replay_row(row([law], candidate_cwids_json=j), lookup, table, ts)
              == ("skipped: no stored candidates", None)
              for j in ("[]", None, "", "{}", "[1, 2]", "[{")))
    check("a differently-cased cwid is the same pick",
          replay_row(row([phys, law], top_cwid="RW2"), lookup, table, ts)[0]
          == "CONFIDENCE_CHANGE")

    # --- confidence at the ceiling
    top1 = cand("rw2", "Physics", given_match="full", cohort=1, affil=True, confidence=1.0)
    check("at the ceiling the pre-clip value is rebuilt: 1.15 - 0.20 is 0.95, not 0.80",
          rescored(top1, -0.20) == 0.95 and rescored(top1, 0.15) == 1.0)
    check("...a terminal label carries its -0.10 (1.05 - 0.20 = 0.85)",
          rescored(dict(top1, person_type="Cornell Emeritus"), -0.20) == 0.85)
    check("below the ceiling the stored value is used as stored, floor included",
          rescored(cand("a", "Physics", confidence=0.7), 0.15) == 0.85
          and rescored(cand("a", "Physics", confidence=0.7), -0.20) == 0.5
          and rescored(cand("a", "Physics", confidence=0.05), -0.20) == 0.0
          and rescored(cand("a", "Physics", confidence=0.95), 0.15) == 1.0
          and rescored(cand("a", "Physics", confidence=None), 0.15) == 0.15)
    check("the rebuilt value is identity_index._confidence's arithmetic, for every "
          "combination of evidence, with and without each term",
          all(round(max(0.0, min(1.0, _unclipped(
              cand("a", "d", given_match=g, cohort=n, affil=a, years_after_wcm=y,
                   person_type="Cornell Emeritus" if h else "Cornell Faculty")) + v)), 3)
              == IdentityIndex._confidence(g, n, a, h, temporal_penalty(y), v)
              for g in ("full", "initial", "unknown") for n in (1, 2, 7, 266)
              for a in (True, False) for h in (True, False) for y in (None, -3, 6, 30)
              for v in (0.0, 0.15, -0.20)))
    check("the terminal labels are identity_index's historical Cornell types",
          _TERMINAL_LABELS == {"Cornell Emeritus", "Cornell Retired Faculty",
                               "Cornell Former Postdoc", "Cornell Alumni", "Cornell Retiree",
                               "Cornell Inactive"})

    # --- the backfill and the producer must agree. Run the matcher twice over one roster,
    # without the venue subfield (what an open row stored) and with it (what the lane
    # writes once AAR_VENUE_FIT is on), and replay the first into the second.
    from identity_index import _norm
    from aar_universe_scopus import _compact

    def person(given, dept, cwid, types=("Cornell Faculty", False)):
        return {"cwid": cwid, "given": given, "middle": "", "surname": "Weiss",
                "given_norm": _norm(given), "surname_norm": "weiss", "pref_norm": "",
                "pref_full": "", "dept": dept, "division": "", "program": "", "title": "",
                "person_type": types[0], "historical": types[1], "end_year": None,
                "campus": CAMPUS_ITHACA}

    idx = IdentityIndex([person("Rhea", "Law School", "rh1"),
                         person("Robert", "Physics", "rw2"),
                         person("Ruth", "Hotel Administration", "ru3"),
                         person("Rex", "Physics", "rx4", ("Cornell Emeritus", True))],
                        campus=CAMPUS_ITHACA, dept_venue_fit=table)
    agree = []
    for fore, initials, affils in ((None, "R", None), ("Robert", "R", None),
                                   ("Rhea", "R", ["Department of Physics, Cornell University"]),
                                   ("Robert", "R", ["Department of Physics, Cornell University"]),
                                   ("Ruth", "R", ["Law School, Cornell University, Ithaca"])):
        for sub, issn in (("Optics", "0146-9592"), ("Law", "0010-8847")):
            was = _compact(idx.candidates("Weiss", fore, initials, affils)[0])
            now = _compact(idx.candidates("Weiss", fore, initials, affils,
                                          venue_subfield=sub)[0])
            _v, e = replay_row(row(was, issn=issn), lookup, table, ts)
            agree.append(e is not None and e["after"]["candidate_cwids_json"] == json.dumps(now)
                         and e["after"]["top_cwid"] == now[0]["cwid"]
                         and e["after"]["top_confidence"] == now[0]["confidence"]
                         and e["after"]["top_venue_fit"] == now[0].get("venue_fit"))
    check("replaying a stored row gives byte-for-byte the candidate list, top pick, "
          "confidence and fit the matcher writes with the venue term on (10 bylines)",
          len(agree) == 10 and all(agree))
    # The one ordering the stored row cannot reproduce: an EXACT tie after the term. The
    # matcher breaks it by roster order, which the row does not store; the backfill
    # keeps the stored order, so the pick already on the card stays.
    tie = [cand("b", "Hotel Administration", confidence=0.60),      # unscored: stays 0.60
           cand("a", "Physics", confidence=0.45)]                     # Optics: 0.45 -> 0.60
    _v, e = replay_row(row(tie), lookup, table, ts)
    check("an exact tie after the term (0.60 vs 0.60) keeps the stored order",
          [(c["cwid"], c["confidence"])
           for c in json.loads(e["after"]["candidate_cwids_json"])] == [("b", 0.6), ("a", 0.6)]
          and _v == "FIT_RECORDED")

    # --- the write
    sql = _update_sql()
    check("the UPDATE re-checks status='open', that no Accept is parked on the row, the "
          "external sources, and that the producer has not rewritten it since it was read",
          sql.endswith("WHERE id=:id AND status='open' AND accept_conflict IS NULL "
                       "AND source IN ('scopus', 'openalex') "
                       "AND last_refreshed <=> :seen_refreshed"))
    set_clause = sql.split(" WHERE ")[0]
    check("it writes exactly WRITE_COLS and no curator-owned column",
          all(f"{c}=:{c}" in set_clause for c in WRITE_COLS)
          and set_clause.count("=:") == len(WRITE_COLS)
          and not any(c in set_clause for c in ("status", "resolution_cwid", "reviewer",
                                                "note", "snooze_until", "n_candidates",
                                                "single_candidate", "top_cohort_size")))
    check("every written column is one the producer refreshes; top_venue_fit is among them",
          all(c in aar_db._REFRESH_COLS for c in WRITE_COLS) and "top_venue_fit" in WRITE_COLS)
    check("the SELECT takes open external rows with a Cornell top pick that are not held "
          "in the duplicates queue, and nothing else",
          all(s in _SELECT_SQL for s in ("status='open'", "source IN ('scopus', 'openalex')",
                                         "top_person_type LIKE 'Cornell%'",
                                         "accept_conflict IS NULL")))

    class _Conn:
        """Answers each UPDATE with the row count queued for it."""
        def __init__(self, rowcounts):
            self.rowcounts, self.params, self.rowcount = list(rowcounts), [], None

        def execute(self, _stmt, params):
            self.params.append(params)
            self.rowcount = self.rowcounts.pop(0)
            return self

    entries = [replay_row(row([law, phys], id=i), lookup, table, ts)[1] for i in (1, 2, 3)]
    conn = _Conn([1, 0, 1])
    n = apply_entries(conn, entries)
    check("applied is the UPDATE's own row count per row: a row the guards skipped is "
          "logged applied=false and not counted",
          n == 2 and [e["applied"] for e in entries] == [True, False, True])
    check("each UPDATE is bound to its row id, the after payload and the last_refreshed "
          "that was read",
          [p["id"] for p in conn.params] == [1, 2, 3]
          and conn.params[0]["seen_refreshed"] == "2026-11-01 00:00:00"
          and conn.params[0]["last_refreshed"] == ts and conn.params[0]["top_cwid"] == "rw2"
          and set(conn.params[0]) == set(WRITE_COLS) | {"id", "seen_refreshed"})

    # --- the ledger comes first, and is never overwritten
    import tempfile

    class _Engine:
        """Records the order of events; each UPDATE reports one row."""
        def __init__(self):
            self.events, self.rowcount = [], 1

        def begin(self):
            return self

        def __enter__(self):
            self.events.append("BEGIN")
            return self

        def __exit__(self, *exc):
            self.events.append("COMMIT" if exc[0] is None else "ROLLBACK")
            return False

        def execute(self, _stmt, params):
            self.events.append(f"UPDATE {params['id']}")
            return self

    def fresh():
        return [replay_row(row([law, phys], id=i), lookup, table, ts)[1] for i in (1, 2)]

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

        eng, path, seen = _Engine(), os.path.join(d, "apply.jsonl"), []
        real_apply = globals()["apply_entries"]

        def spy(conn, entries):
            seen.extend(read(path))             # what is on disk when the writes begin
            return real_apply(conn, entries)
        globals()["apply_entries"] = spy
        try:
            n = ledger_then_apply(eng, fresh(), path, True)
        finally:
            globals()["apply_entries"] = real_apply
        check("on --apply the full before/after is on disk, applied=false, before the "
              "first UPDATE",
              [(e["id"], e["applied"], e["before"]["top_cwid"], e["after"]["top_cwid"])
               for e in seen] == [(1, False, "rh1", "rw2"), (2, False, "rh1", "rw2")]
              and eng.events == ["BEGIN", "UPDATE 1", "UPDATE 2", "COMMIT"])
        check("...and is rewritten afterwards with what each UPDATE did",
              n == 2 and [e["applied"] for e in read(path)] == [True, True]
              and not os.path.exists(path + ".tmp"))

        real_write, calls = globals()["_write_ledger"], []

        def fail_second(p_, entries):
            calls.append(p_)
            if len(calls) == 2:
                raise OSError(28, "No space left on device")
            return real_write(p_, entries)
        globals()["_write_ledger"] = fail_second
        eng, path = _Engine(), os.path.join(d, "second.jsonl")
        import contextlib, io
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                ledger_then_apply(eng, fresh(), path, True)
            raised = False
        except OSError:
            raised = True
        finally:
            globals()["_write_ledger"] = real_write
        check("if the rewrite fails after the UPDATEs, the first ledger is intact and the "
              "run says so before failing",
              raised and "COMMIT" in eng.events and len(read(path)) == 2
              and "rows were UPDATED" in buf.getvalue())

    a = default_ledger_path(datetime(2026, 11, 16, 9, 30, 5), True)
    b = default_ledger_path(datetime(2026, 11, 16, 9, 31, 0), False)
    check("each run's default ledger is its own file: an apply and the dry run after it "
          "cannot share a path",
          a != b and a.endswith("backfill_venue_fit_ledger_20261116_093005_apply.jsonl")
          and b.endswith("backfill_venue_fit_ledger_20261116_093100_dryrun.jsonl"))

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
