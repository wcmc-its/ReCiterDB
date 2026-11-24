#!/usr/bin/env python3
"""
Re-derive the stored candidates of OPEN Cornell Ithaca Scopus/OpenAlex rows from today's
roster, and rewrite a row only when that changes something.

WHY THIS EXISTS. A lane writes a row's proposal when it sweeps the document and never
again unless a later sweep reaches it; the Scopus rolling window reaches a few dozen
open Ithaca rows a week. Neither existing tool can bring the rest up to date:
  * aar_reconcile_open --drift-only re-fetches each row from Scopus and looks for the
    author through the WCM afid family only, so every Ithaca row is UNRESOLVABLE and
    left alone;
  * backfill_venue_fit skips a row that already carries `venue_fit` and never looks at
    the department.
So a roster change that moves a Cornell candidate's evidence -- the Cornell Tech
department fold (#311), a new title, a person who has since become emeritus -- strands
the open queue. This pass re-derives, from what the row stores plus today's roster:

  per stored candidate (found on the Ithaca roster by cwid):
    dept, title, person_type   the roster record, as IdentityIndex._record builds it
                               (department through cornell_hr_feed.canonical_dept)
    affil_dept_match           IdentityIndex._affil_match against the stored byline
                               affiliation (author_affiliation, " | "-joined)
    venue_fit / venue term     venue_fit.DeptVenueFit, the scored person's own rows left
                               out, for the venue's subfield from the stored issn/journal
    confidence                 IdentityIndex._confidence from the candidate's stored
                               given_match, cohort and years_after_wcm plus the above
  then re-sorts with identity_index._rank_key, stores the list through the Scopus
  lane's own _compact, and rebuilds the top_* columns from whoever is first.

What it does NOT re-derive, because the row does not store what it would need: the
given-name tier and the cohort (both come from the byline name, which a stored row
holds only as a display string -- the reason the replay tools re-fetch), and who is on
the list. A candidate no longer on the roster is kept exactly as stored. A row with a
WCM candidate on it is skipped: the matcher gives a mixed list no venue term, and the
WCM side is the PubMed-era reconcile's business.

SAFETY, the backfill_venue_fit rules, reused rather than copied:
  * Dry run by default. Every run writes its own JSONL ledger, never overwriting one,
    with the full before and after of every row it would touch.
  * --apply writes backfill_venue_fit.WRITE_COLS only (producer-owned; asserted there
    against aar_db._REFRESH_COLS and against every curator-owned column) through
    backfill_venue_fit._update_sql: status='open' AND accept_conflict IS NULL AND an
    external source AND last_refreshed unchanged since the read. A row a curator
    resolved, or the producer rewrote, in between is skipped and recorded applied=false.
  * Commits in batches (--batch-size, default 200), the ledger rewritten after each, so
    an interrupted run leaves every committed row recorded.
  * RESUME: just run it again. A row this pass already wrote re-derives to itself and is
    UNCHANGED, so a second run writes only what the first did not reach.
  * ROLLBACK: --rollback LEDGER puts each applied row's `before` back, only where the
    row is still open and its last_refreshed is still the value this pass wrote (a row
    the producer or a curator has touched since is left alone and reported). Dry run
    unless --apply, and it writes its own ledger.

The --apply run is a prod write, handed to the owner; not alongside the nightly
(14:30 UTC). Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb). AAR_VENUE_FIT is
not read: the term is computed as the CronJob computes it with the switch on (it is on).

WHICH RUN. --only-folded-depts is the #311 (Cornell Tech) refresh: 745 rows on the
2026-11-23 dry run, 20 pick changes. The unscoped run re-derives every candidate from
today's roster, and that roster carries Cornell HR's Workday renames ("Cornell Lab for
Accelerator-based ScienceS & Ed", "Pop Medicine & Diagnstc Sci", ...) that the venue-fit
training rows, stored under the old names, do not know: those candidates lose their
venue term. The same dry run moved 216 picks, most of them for that reason. Fold the
renames (cornell_hr_feed.DEPT_ALIASES) before running it unscoped.

Usage:
  python3 backfill_rederive_candidates.py --selftest
  python3 backfill_rederive_candidates.py --only-folded-depts  # dry run, Cornell Tech only
  python3 backfill_rederive_candidates.py                      # dry run
  python3 backfill_rederive_candidates.py --apply
  python3 backfill_rederive_candidates.py --rollback LEDGER    # dry run of the undo
  python3 backfill_rederive_candidates.py --rollback LEDGER --apply
"""
import argparse, json, os, sys
from collections import Counter
from datetime import datetime

from sqlalchemy import text

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import aar_db                                                    # noqa: E402
import venue_fit                                                 # noqa: E402
import backfill_venue_fit as bvf                                 # noqa: E402
from aar_universe_scopus import _compact                         # noqa: E402
from cornell_hr_feed import canonical_dept                       # noqa: E402
from identity_index import (IdentityIndex, CAMPUS_ITHACA,        # noqa: E402
                            _affil_tokens, temporal_penalty)

WRITE_COLS = bvf.WRITE_COLS
_READ_COLS = ["id", "source", "author_key", "wcm_author", "issn", "journal",
              "author_affiliation"] + [c for c in WRITE_COLS if c != "top_venue_fit"]
_SELECT_SQL = bvf._SELECT_SQL
_CONF_TOL = 5e-4          # half the 0.001 quantum, above FLOAT noise (as everywhere else)
# The per-candidate fields this pass re-derives and compares; anything else a stored
# candidate carries is kept as it is.
_DERIVED = ("dept", "title", "person_type", "affil_dept_match", "venue_fit", "confidence")


def _ne(field, a, b):
    if field == "confidence":
        return a is None or b is None or abs(float(a) - float(b)) > _CONF_TOL
    if field == "affil_dept_match":
        return bool(a) != bool(b)
    if field in ("dept", "title", "person_type"):
        return (a or "") != (b or "")
    return _fit_ne(a, b)


def _fit_ne(a, b):
    """A venue fit differs when it appears, disappears or moves the venue TERM. The
    number itself drifts a little every night as accepted rows accumulate (1.480 ->
    1.486), and on the first dry run that alone flagged 1,914 rows whose ranking nothing
    had touched. A row rewritten for another reason stores the current number anyway."""
    if a is None or b is None:
        return (a is None) != (b is None)
    return venue_fit.venue_term(float(a)) != venue_fit.venue_term(float(b))


def ithaca_roster(records):
    """{cwid lowercased: record} for the primary Ithaca records (not the alternate-name
    copies, which carry `alt_source` and the same person under another surname)."""
    return {str(r["cwid"]).lower(): r for r in records
            if r.get("campus") == CAMPUS_ITHACA and not r.get("alt_source")}


def load_roster():
    """Today's Ithaca roster, exactly as the lane builds its records. No DynamoDB
    alternate names: they add surname copies of the same record, and this pass looks
    people up by cwid."""
    idx = IdentityIndex.load(campus=CAMPUS_ITHACA, alternate_names={})
    return ithaca_roster(r for pool in idx.by_surname.values() for r in pool)


def has_folded_dept(row):
    """Does any stored candidate carry a department `canonical_dept` folds (the Cornell
    Tech units)? The --only-folded-depts row filter: the change #311 made, and nothing else
    the roster has done since."""
    try:
        cands = json.loads(row.get("candidate_cwids_json") or "[]")
    except ValueError:
        return False
    return any(isinstance(c, dict) and isinstance(c.get("dept"), str)
               and canonical_dept(c["dept"]) != c["dept"] for c in cands or [])


def rederive_row(row, roster, lookup, table, run_ts, folded_only=False):
    """One open row -> (verdict, flags, entry). `entry` is the ledger record, or None
    when the row is left alone. Verdicts, first that applies:

      skipped: no stored candidates / WCM candidate on the row
      PICK_CHANGE        a different person leads the row
      CONFIDENCE_CHANGE  same person, top_confidence moved
      TOP_RELABEL        same person and confidence, a displayed top_* value moved
                         (top_dept, top_person_type, top_affil_match, top_venue_fit)
      CANDIDATE_CHANGE   the top columns stand; another candidate's evidence moved
      TITLE_ONLY         nothing but candidate titles appear or change
      UNCHANGED          nothing to write

    `folded_only` (--only-folded-depts) re-derives only the candidates whose STORED
    department folds; every other candidate keeps its stored evidence and gains only
    its title. That is #311's change and nothing else the roster has done since --
    on 2026-11-23 Cornell's HR feed had renamed dozens of departments to Workday short
    forms ("Cornell Lab for Accelerator-based ScienceS & Ed"), which the venue-fit
    training rows (stored under the old names) do not know, so re-deriving those
    candidates moves fits for a reason that has nothing to do with Cornell Tech.

    `flags` counts the individual effects (non-exclusive) for the report."""
    try:
        cands = json.loads(row["candidate_cwids_json"] or "[]")
    except ValueError:
        cands = None
    if not isinstance(cands, list) or not cands or not all(isinstance(c, dict) for c in cands):
        return "skipped: no stored candidates", set(), None
    if not all(bvf._is_ithaca(c) for c in cands):
        return "skipped: WCM candidate on the row", set(), None

    subfield = venue_fit.subfield_for(lookup, row.get("issn"), row.get("journal"))
    toks = _affil_tokens([a for a in (row.get("author_affiliation") or "").split(" | ") if a])
    # candidates() scores a name-matched candidate against the name cohort and the one
    # only the byline ORCID added against the cohort including them; the stored
    # cohort_size is the latter.
    added = sum(1 for c in cands if c.get("orcid_added"))
    flags, out = set(), []
    for c in cands:
        rec = roster.get(str(c.get("cwid") or "").lower())
        if rec is None:
            flags.add("candidate not on roster (kept as stored)")
            out.append(dict(c))
            continue
        if folded_only and canonical_dept(c.get("dept")) == c.get("dept"):
            n = dict(c, title=rec["title"])
            if _ne("title", c.get("title"), n.get("title")):
                flags.add("title changed")
            out.append(n)
            continue
        affil, _where = IdentityIndex._affil_match(rec, toks)
        fit = table.fit(rec["dept"], subfield, rec["cwid"]) if subfield else None
        cohort = int(c.get("cohort_size") or 1) - (0 if c.get("orcid_added") else added)
        conf = IdentityIndex._confidence(
            c.get("given_match"), max(cohort, 1), affil, rec["historical"],
            temporal_penalty(c.get("years_after_wcm")), venue_fit.venue_term(fit))
        n = dict(c, dept=rec["dept"], title=rec["title"], person_type=rec["person_type"],
                 affil_dept_match=affil, confidence=conf)
        n.pop("venue_fit", None)
        if fit is not None:
            n["venue_fit"] = fit
        for f in _DERIVED:
            if _ne(f, c.get(f), n.get(f)):
                if f == "affil_dept_match":
                    flags.add("affiliation match gained" if affil else "affiliation match lost")
                else:
                    flags.add(f"{f} changed")
        out.append(n)
    out.sort(key=bvf._key, reverse=True)       # stable: ties keep the stored order
    stored = _compact(out)
    top = stored[0]
    after = {
        "top_cwid": top.get("cwid"),
        "top_name": bvf._trunc(top.get("name"), 255),
        "top_person_type": bvf._trunc(top.get("person_type"), 64),
        "top_dept": bvf._trunc(top.get("dept"), 255),
        "top_given_match": top.get("given_match"),
        "top_affil_match": int(bool(top.get("affil_dept_match"))),
        "top_confidence": top.get("confidence"),
        "top_years_after_wcm": top.get("years_after_wcm"),
        "top_venue_fit": top.get("venue_fit"),
        "candidate_cwids_json": json.dumps(stored),
        "last_refreshed": run_ts,
    }
    before = {c: row.get(c) for c in WRITE_COLS}

    order_moved = [str(c.get("cwid")).lower() for c in cands] != \
                  [str(c.get("cwid")).lower() for c in out]
    if order_moved:
        flags.add("candidate order changed")
    if not bvf._cwid_eq(row.get("top_cwid"), after["top_cwid"]):
        verdict = "PICK_CHANGE"
    elif _ne("confidence", row.get("top_confidence"), after["top_confidence"]):
        verdict = "CONFIDENCE_CHANGE"
    elif ((row.get("top_dept") or "") != (after["top_dept"] or "")
          or (row.get("top_person_type") or "") != (after["top_person_type"] or "")
          or bool(row.get("top_affil_match")) != bool(after["top_affil_match"])
          or _fit_ne(row.get("top_venue_fit"), after["top_venue_fit"])):
        verdict = "TOP_RELABEL"
    elif order_moved or flags - {"title changed", "candidate not on roster (kept as stored)"}:
        verdict = "CANDIDATE_CHANGE"
    elif "title changed" in flags:
        verdict = "TITLE_ONLY"
    else:
        return "UNCHANGED", flags, None
    if (row.get("top_dept") or "") != (after["top_dept"] or ""):
        flags.add("top_dept relabelled")
    return verdict, flags, {
        "id": row["id"], "source": row["source"], "author_key": row["author_key"],
        "wcm_author": row["wcm_author"], "journal": row["journal"],
        "author_affiliation": row.get("author_affiliation"), "subfield": subfield,
        "rule": verdict, "flags": sorted(flags), "before": before, "after": after,
        "swept_at": run_ts, "applied": False}


# ---- writes ------------------------------------------------------------------------
def _batches(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def apply_in_batches(eng, entries, path, batch_size):
    """bvf.apply_entries one batch per transaction, the ledger rewritten after each, so
    an interrupted run has every committed row on disk with applied=true. The ledger is
    written once before the first UPDATE (all applied=false), as bvf does."""
    bvf._write_ledger(path, entries)
    n = 0
    for chunk in _batches(entries, batch_size):
        with eng.begin() as c:
            n += bvf.apply_entries(c, chunk)
        bvf._write_ledger(path, entries)
        print(f"  committed {n} so far", flush=True)
    return n


def _rollback_sql():
    """Put `before` back, only on a row still open, not parked, and still carrying the
    last_refreshed this pass wrote -- anything touched since is left alone."""
    sets = ", ".join(f"{c}=:{c}" for c in WRITE_COLS)
    return (f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open' "
            "AND accept_conflict IS NULL AND source IN ('scopus', 'openalex') "
            "AND last_refreshed <=> :written_refreshed")


def rollback_entries(path):
    """The entries a rollback would write, from an --apply ledger: one per applied row,
    `after` = that row's `before`, guarded by the `after.last_refreshed` it wrote."""
    out = []
    with open(path) as fh:
        for line in fh:
            e = json.loads(line)
            if e.get("applied"):
                out.append({"id": e["id"], "values": dict(e["before"]),
                            "written_refreshed": e["after"]["last_refreshed"],
                            "applied": False})
    return out


def run_rollback(eng, src, path, apply_writes, batch_size):
    entries = rollback_entries(src)
    print(f"[{'APPLY' if apply_writes else 'DRY-RUN'}] rollback of {src}: "
          f"{len(entries)} applied rows to restore", flush=True)
    bvf._write_ledger(path, entries)
    if not apply_writes:
        print(f"ledger: {path}\nDRY-RUN: nothing written (pass --apply)")
        return 0
    stmt, n = text(_rollback_sql()), 0
    for chunk in _batches(entries, batch_size):
        with eng.begin() as c:
            for e in chunk:
                e["applied"] = c.execute(stmt, dict(e["values"], id=e["id"],
                                                    written_refreshed=e["written_refreshed"])
                                         ).rowcount == 1
                n += e["applied"]
        bvf._write_ledger(path, entries)
    print(f"RESTORED {n} of {len(entries)}"
          + (f" ({len(entries) - n} left alone: touched since this pass wrote them)"
             if n != len(entries) else "") + f"\nledger: {path}")
    return n


def _print_sample(entries, rule, n):
    hits = [e for e in entries if e["rule"] == rule]
    print(f"\n  {rule}: {len(hits)}" + (f" (first {min(n, len(hits))})" if hits else ""))
    for e in hits[:n]:
        b, a = e["before"], e["after"]
        print(f"    id={e['id']} {e['wcm_author']!r} in {(e['journal'] or '')[:50]!r} "
              f"[{e['subfield']}]\n      byline: {(e['author_affiliation'] or '')[:110]!r}\n"
              f"      {b['top_cwid']} {b['top_name']!r} ({b['top_dept']}) "
              f"conf {b['top_confidence']}\n"
              f"   -> {a['top_cwid']} {a['top_name']!r} ({a['top_dept']}) "
              f"conf {a['top_confidence']} fit {a['top_venue_fit']}\n"
              f"      flags: {', '.join(e['flags'])}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--ledger", default=None,
                    help="JSONL ledger (default: backfill_rederive_ledger_<time>_<mode>.jsonl "
                         "beside this script; an existing file is refused)")
    ap.add_argument("--rollback", metavar="LEDGER", default=None,
                    help="restore the `before` of every applied row in an --apply ledger")
    ap.add_argument("--batch-size", type=int, default=200, help="rows per commit")
    ap.add_argument("--skip-title-only", action="store_true",
                    help="leave TITLE_ONLY rows alone (they change nothing but candidate "
                         "titles); they pick the title up on their next rewrite")
    ap.add_argument("--only-folded-depts", action="store_true",
                    help="only rows with a stored candidate whose department "
                         "cornell_hr_feed.canonical_dept folds (the Cornell Tech units), "
                         "and on them re-derive only those candidates (the rest keep "
                         "their stored evidence and gain their title)")
    ap.add_argument("--limit", type=int, default=None, help="consider only the first N rows")
    ap.add_argument("--sample", type=int, default=5, help="rows to print per class")
    ap.add_argument("--selftest", action="store_true", help="offline checks, no DB")
    args = ap.parse_args()
    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    now = datetime.now()
    run_ts = now.strftime("%Y-%m-%d %H:%M:%S")
    mode = ("rollback_" if args.rollback else "") + ("apply" if args.apply else "dryrun")
    ledger = args.ledger or os.path.join(
        HERE, f"backfill_rederive_ledger_{now:%Y%m%d_%H%M%S}_{mode}.jsonl")
    if os.path.exists(ledger):
        print(f"{ledger} already exists and a ledger is never overwritten. Pass another "
              f"--ledger. Nothing read, nothing written.")
        sys.exit(2)
    eng = aar_db.engine()
    if args.rollback:
        run_rollback(eng, args.rollback, ledger, args.apply, args.batch_size)
        return

    with eng.connect() as c:
        if "top_venue_fit" in aar_db.missing_optional_cols(c):
            print("authorship_review has no top_venue_fit column (v3.14). Nothing done.")
            sys.exit(2)
        lookup = venue_fit.load_venue_lookup(c)
        table = venue_fit.load_dept_fit(c, CAMPUS_ITHACA, lookup)
        rows = [dict(r) for r in c.execute(text(_SELECT_SQL.format(
            cols=", ".join(_READ_COLS + ["top_venue_fit"])))).mappings().all()]
    roster = load_roster()
    if args.only_folded_depts:
        rows = [r for r in rows if has_folded_dept(r)]
    if args.limit:
        rows = rows[:args.limit]
    print(f"[{'APPLY' if args.apply else 'DRY-RUN'}] {len(rows)} open Cornell-top external "
          f"rows; Ithaca roster {len(roster)} people; fit table {table.total} training rows",
          flush=True)

    counts, flag_counts, entries = Counter(), Counter(), []
    for r in rows:
        verdict, flags, entry = rederive_row(r, roster, lookup, table, run_ts,
                                             folded_only=args.only_folded_depts)
        counts[verdict] += 1
        flag_counts.update(flags)
        if entry and not (args.skip_title_only and verdict == "TITLE_ONLY"):
            entries.append(entry)
    print("  verdicts:")
    for v, n in sorted(counts.items(), key=lambda kv: (kv[0].startswith("skipped"), -kv[1])):
        print(f"    {n:6d}  {v}")
    print("  effects (a row can have several):")
    for f, n in sorted(flag_counts.items(), key=lambda kv: -kv[1]):
        print(f"    {n:6d}  {f}")
    for rule in ("PICK_CHANGE", "CONFIDENCE_CHANGE", "TOP_RELABEL"):
        _print_sample(entries, rule, args.sample)

    if args.apply and entries:
        n = apply_in_batches(eng, entries, ledger, args.batch_size)
        print(f"\nAPPLIED: {n} of {len(entries)} rows updated"
              + (f" ({len(entries) - n} skipped: resolved, parked or rewritten since "
                 f"they were read)" if n != len(entries) else ""))
    else:
        bvf._write_ledger(ledger, entries)
        if not args.apply:
            print("\nDRY-RUN: nothing written (pass --apply)")
    print(f"ledger: {ledger} ({len(entries)} rows)")


# ---- offline self-test ----------------------------------------------------------
def _selftest():
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    from identity_index import _PTYPE_COLS

    def rec(cwid, dept, title="", types=("cornell-ithaca", "cornell-faculty"), division=None):
        row = dict({c: None for c in _PTYPE_COLS}, cwid=cwid, givenName="X", middleName="",
                   surname="Quill", primaryAcademicDepartment=dept,
                   primaryAcademicDivision=division, primaryTitle=title, primaryProgram=None,
                   endDateWCMFaculty=None, endDateWCMStudent=None, prefFirstName=None,
                   prefMiddleName=None, prefLastName=None)
        return IdentityIndex._record(row, campus_types=types)

    roster = ithaca_roster([
        rec("ct1", "Faculty and Research", "Graduate Research Assistant"),
        rec("cs2", "Computer Science", "Professor"),
        rec("em3", "Law School", "", ("cornell-ithaca", "cornell-emeritus")),
        dict(rec("cs2", "Computer Science"), alt_source="alternate"),   # alt copy: ignored
        rec("wcm9", "Medicine", types=()),                               # WCM: not Ithaca
    ])
    check("roster: primary Ithaca records by lowercased cwid, alt copies and WCM out, "
          "department folded",
          set(roster) == {"ct1", "cs2", "em3"} and roster["ct1"]["dept"] == "Cornell Tech")

    # Human Factors: Cornell Tech 30 rows (other people), Computer Science 30, half each.
    table = venue_fit.DeptVenueFit(
        [("Faculty and Research", "Human Factors", 30, "z1"), ("Computer Science", "Law", 30),
         ("Law School", "Law", 100)])
    lookup = venue_fit.build_venue_lookup([(1, "1111-1111", None, "Human Factors")], [])

    def cand(cwid, dept, conf, affil=False, gm="initial", cohort=3, **kw):
        return dict({"cwid": cwid, "name": cwid.upper(), "name_source": "directory",
                     "name_n": None, "person_type": "Cornell Faculty", "dept": dept,
                     "given_match": gm, "affil_dept_match": affil, "cohort_size": cohort,
                     "confidence": conf, "years_after_wcm": None, "campus": CAMPUS_ITHACA},
                    **kw)

    def row(cands, affil="Cornell Tech, New York, NY", issn="1111-1111", **kw):
        top = cands[0]
        return dict({"id": 7, "source": "scopus", "author_key": "k", "wcm_author": "A Quill",
                     "issn": issn, "journal": "CHI", "author_affiliation": affil,
                     "top_cwid": top["cwid"], "top_name": top["name"],
                     "top_person_type": top["person_type"], "top_dept": top["dept"],
                     "top_given_match": top["given_match"],
                     "top_affil_match": int(bool(top["affil_dept_match"])),
                     "top_confidence": top["confidence"], "top_years_after_wcm": None,
                     "top_venue_fit": top.get("venue_fit"),
                     "candidate_cwids_json": json.dumps(cands),
                     "last_refreshed": "2026-11-16 10:00:00"}, **kw)

    # cs2 led on stored evidence; ct1 was stored as "Faculty and Research" with no
    # affiliation match. Re-derived: ct1 is "Cornell Tech", matches the byline (+0.25)
    # and fits Human Factors (+0.15), so it leads.
    stored = [cand("cs2", "Computer Science", 0.383), cand("ct1", "Faculty and Research", 0.383)]
    v, flags, e = rederive_row(row(stored), roster, lookup, table, "T")
    a = e["after"] if e else {}
    new = json.loads(a.get("candidate_cwids_json", "[]"))
    exp_ct1 = IdentityIndex._confidence("initial", 3, True, False, 0.0, 0.15)
    check("the Cornell Tech candidate is relabelled, gains the byline match and the venue "
          "term, and takes the row", v == "PICK_CHANGE" and a["top_cwid"] == "ct1"
          and a["top_dept"] == "Cornell Tech" and a["top_affil_match"] == 1
          and a["top_confidence"] == exp_ct1 and a["top_venue_fit"] is not None
          and {"affiliation match gained", "dept changed", "top_dept relabelled",
               "candidate order changed"} <= flags)
    check("...the stored list goes through _compact: titles published, both fits scored "
          "(Computer Science never publishes in Human Factors: -0.20)",
          new[0]["title"] == "Graduate Research Assistant" and new[1]["title"] == "Professor"
          and new[0]["venue_fit"] >= 1.0 and new[1]["venue_fit"] < -1.0
          and new[1]["confidence"] == IdentityIndex._confidence("initial", 3, False, False,
                                                                0.0, -0.20)
          and new == _compact(new))
    _v, _f, e_nofit = rederive_row(row(stored, issn=None), roster, lookup, table, "T")
    check("...and a venue with no category scores nobody: no venue_fit key at all",
          not any("venue_fit" in c
                  for c in json.loads(e_nofit["after"]["candidate_cwids_json"])))
    check("...the ledger keeps the full before, written columns only",
          set(e["before"]) == set(WRITE_COLS) and e["before"]["top_cwid"] == "cs2"
          and set(a) == set(WRITE_COLS))

    # Idempotent: the row as written re-derives to itself (this is what makes a second
    # run a resume rather than a repeat).
    written = dict(row(stored), **{k: v for k, v in a.items()})
    v2, _f, e2 = rederive_row(written, roster, lookup, table, "T2")
    check("a row this pass already wrote is UNCHANGED on the next run (resume-safe)",
          v2 == "UNCHANGED" and e2 is None)
    f32 = dict(written, top_confidence=a["top_confidence"] - 3e-8,
               top_venue_fit=a["top_venue_fit"] + 4e-8)       # FLOAT read-back noise
    check("...including when its FLOAT columns read back a few ulps off",
          rederive_row(f32, roster, lookup, table, "T2")[0] == "UNCHANGED")
    moved = json.loads(a["candidate_cwids_json"])
    moved[0]["venue_fit"] = round(moved[0]["venue_fit"] - 0.01, 3)   # still >= 1: same term
    check("...and when only the fit NUMBER drifted, not its term",
          rederive_row(dict(written, candidate_cwids_json=json.dumps(moved),
                            top_venue_fit=moved[0]["venue_fit"]),
                       roster, lookup, table, "T2")[0] == "UNCHANGED")
    # folded_only: cs2's stored evidence is deliberately stale (affil True, conf 0.9);
    # only ct1 is re-derived, cs2 keeps its stored numbers and gains its title.
    stale = [cand("cs2", "Computer Science", 0.9, affil=True),
             cand("ct1", "Faculty and Research", 0.383)]
    _v, _f, e8 = rederive_row(row(stale), roster, lookup, table, "T8", folded_only=True)
    c8 = {c["cwid"]: c for c in json.loads(e8["after"]["candidate_cwids_json"])}
    check("--only-folded-depts re-derives only the folded candidate; the other keeps its "
          "stored evidence and gains only its title",
          c8["cs2"]["confidence"] == 0.9 and c8["cs2"]["affil_dept_match"] is True
          and "venue_fit" not in c8["cs2"] and c8["cs2"]["title"] == "Professor"
          and c8["ct1"]["dept"] == "Cornell Tech" and c8["ct1"]["affil_dept_match"] is True)
    check("--only-folded-depts: a row is in scope when a stored candidate's department "
          "folds", has_folded_dept(row(stored)) and not has_folded_dept(written)
          and not has_folded_dept({"candidate_cwids_json": "{bad"}))

    # Title only: everything else already current.
    cur = json.loads(a["candidate_cwids_json"])
    no_titles = [{k: x for k, x in c.items() if k != "title"} for c in cur]
    v3, f3, _e = rederive_row(dict(written, candidate_cwids_json=json.dumps(no_titles)),
                              roster, lookup, table, "T3")
    check("a row missing nothing but titles is TITLE_ONLY", v3 == "TITLE_ONLY"
          and f3 == {"title changed"})

    # Same person, displayed department moves but confidence does not (no byline match,
    # venue unknown): TOP_RELABEL.
    plain = [cand("ct1", "Faculty and Research",
                  IdentityIndex._confidence("initial", 1, False, False), cohort=1)]
    v4, _f, e4 = rederive_row(row(plain, affil="Some Lab, Boston", issn=None),
                              roster, lookup, table, "T4")
    check("same pick, same confidence, department relabelled -> TOP_RELABEL",
          v4 == "TOP_RELABEL" and e4["after"]["top_dept"] == "Cornell Tech"
          and e4["after"]["top_confidence"] == e4["before"]["top_confidence"])

    # Roster-derived historical: an emeritus loses 0.10 even if stored as current.
    em = [cand("em3", "Law School", IdentityIndex._confidence("initial", 1, False, False),
               cohort=1)]
    v5, f5, e5 = rederive_row(row(em, affil="Elsewhere", issn=None), roster, lookup, table, "T5")
    check("person type and the historical penalty come from today's roster",
          v5 == "CONFIDENCE_CHANGE" and e5["after"]["top_person_type"] == "Cornell Emeritus"
          and e5["after"]["top_confidence"] == round(em[0]["confidence"] - 0.10, 3))

    # Guards
    gone = [cand("zz9", "Physics", 0.5, gm="full", cohort=1)]
    v6, f6, e6 = rederive_row(row(gone, issn=None, affil="X"), roster, lookup, table, "T6")
    check("a candidate no longer on the roster is kept exactly as stored",
          v6 == "UNCHANGED" and "candidate not on roster (kept as stored)" in f6)
    mixed = [cand("cs2", "Computer Science", 0.4), dict(cand("w1", "Medicine", 0.4),
                                                        campus=None, person_type="Full-Time Faculty")]
    check("a row with a WCM candidate is skipped",
          rederive_row(row(mixed), roster, lookup, table, "T")[0]
          == "skipped: WCM candidate on the row")
    check("no stored candidates / bad JSON is skipped",
          rederive_row(row(stored, candidate_cwids_json="[]"), roster, lookup, table, "T")[0]
          == "skipped: no stored candidates"
          and rederive_row(row(stored, candidate_cwids_json="{x"), roster, lookup, table,
                           "T")[0] == "skipped: no stored candidates")

    # ORCID-added candidate keeps the cohort it was scored against
    oc = [cand("cs2", "Computer Science", 0.0, cohort=3, orcid_match=True, orcid_added=True),
          cand("ct1", "Faculty and Research", 0.0, cohort=3, orcid_match=False)]
    _v, _f, e7 = rederive_row(row(oc, affil="X", issn=None), roster, lookup, table, "T7")
    c7 = {c["cwid"]: c for c in json.loads(e7["after"]["candidate_cwids_json"])}
    check("an ORCID-added candidate is scored on the full cohort, the name-matched ones on "
          "the cohort without them, and the ORCID person still leads",
          c7["cs2"]["confidence"] == IdentityIndex._confidence("initial", 3, False, False)
          and c7["ct1"]["confidence"] == IdentityIndex._confidence("initial", 2, False, False)
          and e7["after"]["top_cwid"] == "cs2")

    # Writes: the guarded UPDATE is backfill_venue_fit's; rollback is guarded the same way
    sql = bvf._update_sql()
    check("--apply uses backfill_venue_fit's guarded UPDATE (open, not parked, external, "
          "last_refreshed unchanged) and writes no curator column",
          all(s in sql for s in ("status='open'", "accept_conflict IS NULL",
                                 "last_refreshed <=> :seen_refreshed"))
          and "status=" not in sql.split("WHERE")[0])
    rsql = _rollback_sql()
    check("rollback restores only rows still open, unparked and carrying the value this "
          "pass wrote", all(s in rsql for s in ("status='open'", "accept_conflict IS NULL",
                                                "last_refreshed <=> :written_refreshed"))
          and "status=" not in rsql.split("WHERE")[0])

    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "l.jsonl")
        e_applied, e_skipped = dict(e, applied=True), dict(e4, applied=False)
        bvf._write_ledger(p, [e_applied, e_skipped])
        rb = rollback_entries(p)
        check("rollback reads only applied rows, restores their `before`, and guards on the "
              "last_refreshed they were written with",
              len(rb) == 1 and rb[0]["values"] == e["before"]
              and rb[0]["written_refreshed"] == "T")

        class _Res:
            rowcount = 1

        class _Conn:
            def __init__(self):
                self.n = 0

            def execute(self, *_a, **_k):
                self.n += 1
                return _Res()

        class _Begin:
            def __init__(self, eng):
                self.eng = eng

            def __enter__(self):
                self.eng.txn += 1
                return self.eng.conn

            def __exit__(self, *a):
                return False

        class _Eng:
            def __init__(self):
                self.txn, self.conn = 0, _Conn()

            def begin(self):
                return _Begin(self)

        eng = _Eng()
        ents = [dict(e, id=i, applied=False) for i in range(5)]
        n = apply_in_batches(eng, ents, os.path.join(d, "a.jsonl"), 2)
        lines = [json.loads(x) for x in open(os.path.join(d, "a.jsonl"))]
        check("apply commits in batches (5 rows, batch 2 -> 3 transactions) and the ledger "
              "records every row applied", n == 5 and eng.txn == 3
              and all(x["applied"] for x in lines) and len(lines) == 5)

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
