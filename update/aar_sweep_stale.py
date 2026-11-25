#!/usr/bin/env python3
"""
One-off sweep for issue #177 (Option 1) — retract candidate proposals the CURRENT
matcher would refuse to make.

PRs #175 (#173, middleName exact-match-only), #176 (#174, gate byline owner) and
#172 (#171, preferred publishing name) changed `identity_index.IdentityIndex.candidates`
so that some bylines no longer resolve to any WCM identity. Merging a matcher fix does
not retract proposals already sitting in `authorship_review`: `aar_orchestrator._db_rows`
skips an unmatched authorship outright (`if top is None: continue`) rather than writing
a null-candidate row, and `aar_db.upsert` is INSERT ... ON DUPLICATE KEY UPDATE, which
only ever touches rows it is handed. A row whose byline the matcher would now refuse
keeps its OLD top_cwid/top_name/top_confidence forever, never advances, and sits open
in front of a curator indefinitely.

WHAT THIS DOES (Option 1 from the issue, and only Option 1)
  For every OPEN row, replay the ORIGINAL authorship (re-fetched from PubMed/Scopus,
  not reconstructed from the stored byline text -- see FIDELITY below) through the
  CURRENT `IdentityIndex`. Where that replay yields ZERO candidates -- not "a different
  candidate than before", ZERO -- NULL the producer-owned candidate columns and leave
  the row `status='open'`. The authorship is still a real, unattributed WCM byline
  worth a curator's attention; it just no longer carries a suggestion. Curator columns
  (status, resolution_cwid, reviewer, note, snooze_until) are never touched, and a row
  is only ever selected/updated by primary key with `AND status='open'` re-checked at
  UPDATE time, so a curator decision made between the SELECT and the UPDATE wins.

  NULL_COLUMNS is exactly the 9 columns the issue names. It is checked at import time
  against `aar_db._REFRESH_COLS` (the columns the producer itself refreshes on every
  upsert) so a typo or a renamed column fails loudly instead of silently no-op'ing.
  Columns the issue does NOT name -- `classification`, `top_fg_score`, `top_io_score`,
  `top_years_after_wcm`, `top_cohort_size`, `single_candidate`, `dup_flag`, `dup_reason`
  -- are producer-refreshed too but are left as-is on a swept row (see the handoff
  notes at the bottom of this docstring).

WHAT THIS DOES NOT DO
  - Does not touch `status`, `resolution_cwid`, `reviewer`, `note`, `snooze_until`.
  - Does not dismiss anything (issue's Option 3, explicitly not recommended).
  - Does not "fix" a row whose OLD top_cwid is no longer a candidate but some OTHER
    person now is (e.g. the issue's own row 70797, "Tony Rosen" -> aer2006 via #172's
    preferred-name match, once ltr4001's middleName-initial path was closed by #173).
    That is a real, larger population (542 of the 12,293 open pubmed rows measured
    below) but it is a DIFFERENT problem -- the row already has a correct answer
    available, and Option 1 as specified only fires on the zero-candidate case. Fixing
    it means re-emitting the row with the new top pick, which only the producer's own
    upsert path should do (a future targeted re-explosion of those pmids, or waiting
    for the recurring/backfill window to reach them). Nulling that population would
    make it WORSE, not better -- it would throw away a proposal that is now correct.

FIDELITY: re-fetches, does not reconstruct from stored columns
  `authorship_review.wcm_author` stores only the combined byline text (fore+" "+last,
  or bare last when there is no forename -- see aar_universe._byline /
  aar_universe_scopus._build_row). Splitting that string back into (fore, last) loses
  `initials` whenever the original had no forename, which can turn a row that still
  matches via the initials tier into a false "stale". Measured on the live scopus
  queue below: doing exactly that reconstruction and treating its empty-candidate
  result as final would have wrongly flagged 24 of 54 rows as stale. So: the pubmed
  lane re-fetches the source article from PubMed (`aar_universe.efetch_by_ids`, the
  producer's own EFetch/XML parser) and indexes into the article's real AuthorList by
  the row's stored `author_position`; the scopus lane uses a cheap reconstruction ONLY
  as a pre-filter to bound live Scopus calls to the rows it flags, then re-fetches each
  one from Scopus (`aar_universe_scopus._scopus_get` + `wcm_authorships`, the producer's
  own query/parse) and re-classifies from the real given-name/initials fields before
  ever queuing it to be nulled. A row whose source document can no longer be re-fetched,
  or whose author_position no longer lines up (retraction, author-list correction), is
  reported separately as "unresolvable" and is never touched.

  This bounds the NULLED set honestly: every row this script actually writes to has
  been checked against a live, freshly re-fetched document (pubmed always; scopus only
  after the live re-verify above), so there is no path from "still matches" to "nulled"
  that skips a real document -- the NULLED set cannot contain a false null, only an
  undercount of true staleness.

  The RETAINED set does NOT carry that same guarantee for scopus. A row the
  reconstruction pre-filter calls `matched` (see `_classify_scopus`) is trusted on the
  split alone and is NEVER live re-verified -- only rows the pre-filter calls
  `candidate_stale` make a live Scopus call. `_split_byline` takes the LAST whitespace
  token as the surname; a genuinely multi-token surname (e.g. "Robert St John" ->
  surname "John", forename "Robert St") is misread, and if that wrong surname's
  candidate pool happens to still yield an initials-tier hit, the row is filed
  `matched` and a truly stale row silently survives the sweep, forever, with no
  downstream check catching it. This is one-directional in the same safe sense as
  above (it can only leave a row in the RETAINED set that current matching would
  actually clear -- it can never produce a bad null), but it means "stale=0 this run"
  is not the same claim as "the current matcher has a candidate for every retained
  scopus row." Measured 2026-08-30 (read-only, no live calls): of the 4,886 scopus
  rows the pre-filter retained as `matched` in that run, 3,207 have a wcm_author with
  more than two whitespace tokens (the shape where a multi-token surname could hide --
  most of these are actually "Fore MI. Last" and split correctly, so this is a loose
  upper bound on exposure, not a defect count); narrowing to rows with a common
  surname-particle token (bin/Al/El/von/... ) in the middle position finds 6. See the
  `ponytail:` note at the `matched.append(r)` call in `_classify_scopus` for the
  upgrade path.

SYS.PATH FORK TRAP
  A stale, pre-#171/#173/#174 copy of these producer modules lives at
  "~/Dropbox/Projects/ReCiter Research/scripts/" (superseded; the live producer is
  this repo's update/). Importing IT instead of this file's own directory would
  replay against the OLD matcher and silently invert this script's purpose -- it is
  missing `pref_norm`/preferred-name matching, the middleName exact-only fix, and the
  byline-owner gate entirely. The last `sys.path.insert(0, ...)` wins, so this module
  inserts its OWN directory last, immediately before the sibling imports, and asserts
  each resolved module's `__file__` lives under that same directory before trusting
  anything it returns. The dry-run output prints every resolved path.

COLLATION TRAP (identity.cwid/person.personIdentifier = utf8mb4_unicode_ci vs
  authorship_review.top_cwid/.resolution_cwid = utf8mb4_general_ci -- MySQL 1267, or a
  filtered variant that silently returns zero rows): this script never joins those two
  column families in SQL. The roster comes from one `IdentityIndex.load()` (which
  already carries its own documented COLLATE-free join, see that module) and
  `authorship_review` is read as a single, un-joined SELECT; any cwid comparison
  against the roster happens in Python, not in a MySQL WHERE/JOIN clause.

COUNT RECONCILIATION vs the issue's reported 559 (503 pubmed / 56 scopus)
  The issue's replay ran "through merged master (d0859a1)" -- BEFORE PR #172 (preferred
  publishing name) merged as 3712b35, which this branch is based on. #172 itself
  recovers some of the 559 (the Rosen anchor above is exactly this shape), so a replay
  against current `origin/master` is EXPECTED to report fewer stale rows than 559, and
  that is correct, not a bug: "current IdentityIndex" means current, not
  d0859a1-current. See the dry-run output below for the actual, up-to-date counts.

Usage:
  python aar_sweep_stale.py                 # dry run: report + JSONL ledger, no writes
  python aar_sweep_stale.py --apply         # NULL the confirmed-stale rows for real
  python aar_sweep_stale.py --limit 25      # cap rows per lane (fast sanity check)
  python aar_sweep_stale.py --selftest      # offline checks, no DB/network
"""
import argparse, json, os, sys, time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # must be the LAST insert(0, ...) before these imports -- see
                          # the SYS.PATH FORK TRAP note above.
import aar_db
import identity_index as idxmod
import aar_universe as uni
import aar_universe_scopus as scop
from identity_index import IdentityIndex

# Every module this script trusts for matching must resolve to THIS directory, not the
# stale ~/Dropbox/Projects/ReCiter Research/scripts/ copy.
for _mod in (aar_db, idxmod, uni, scop):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning. Refusing to trust any "
        "replay result computed against it.")

# Exactly the 9 columns issue #177 Option 1 names. Cross-checked against what the
# producer itself refreshes (aar_db._REFRESH_COLS) so a renamed/typo'd column here
# fails at import time instead of silently no-op'ing in the UPDATE below.
NULL_COLUMNS = [
    "top_cwid", "top_name", "top_person_type", "top_dept",
    "top_given_match", "top_affil_match", "top_confidence",
    "candidate_cwids_json", "n_candidates",
]
_missing = [c for c in NULL_COLUMNS if c not in aar_db._REFRESH_COLS]
assert not _missing, f"NULL_COLUMNS names columns the producer doesn't refresh: {_missing}"

# top_venue_fit (v3.14) is the top candidate's department/venue fit, and a row this sweep
# leaves naming nobody must not go on carrying one: Publication Manager's bulk accept
# filters on that column. It is nulled WITH the nine whenever the live table has it (see
# _null_sql), but it is not IN the nine: NULL_COLUMNS is tied by assertion to
# aar_report_changed_picks._DRIFT_COLS and is the shape of the #177 ledger. Nothing is
# lost by leaving it out of the ledger's before-snapshot: the value is the `venue_fit`
# of the first entry in the snapshot's own candidate_cwids_json. top_orcid_match (v3.15)
# by the same rule and for the same reason: it says the top pick is the byline ORCID's
# person, and its value is the first stored entry's `orcid_match`.
ALSO_NULL = ["top_venue_fit", "top_orcid_match"]
assert set(ALSO_NULL) <= set(aar_db._OPTIONAL_COLS), (
    "ALSO_NULL names a column aar_db does not treat as optional -- _null_sql only knows "
    "how to check for those")


# ---- scopus reconstruction pre-filter (no network) --------------------------
def _split_byline(wcm_author):
    """Best-effort (fore, last) from the stored combined byline -- the same shape
    `aar_universe._byline` / `aar_universe_scopus._build_row` produce: '{fore} {last}',
    or bare '{last}' when there was no forename. Used ONLY to cheaply pre-filter which
    of ~4,940 open scopus rows are worth a live Scopus re-fetch; every row this flags
    is re-confirmed against the real document (see `_classify_scopus`) before it is
    ever queued to be nulled -- this function alone is not trusted to decide anything,
    because a bare-last-name byline here could equally mean the original author record
    carried initials this reconstruction has no way to recover.

    ponytail: string-split reconstruction, not a stored-initials column (there isn't
    one). Ceiling: undercounts "still matches" on Scopus rows whose byline had no
    forename -- caught downstream by the live re-verify, never by silent inclusion in
    the stale set. If Scopus quota ever stops being the constraint, drop this
    pre-filter and always live-verify."""
    if not wcm_author or not isinstance(wcm_author, str):
        return None, None
    parts = wcm_author.split()
    if len(parts) <= 1:
        return None, wcm_author
    return " ".join(parts[:-1]), parts[-1]


# ---- classification ----------------------------------------------------------
def _snapshot(row):
    return {c: row[c] for c in NULL_COLUMNS}


def _orcid_guard(rows, idx, lane):
    """Raise rather than sweep `rows` on a roster missing the ORCID lists they were ranked
    with (see the comment below). Both lanes: a PubMed row stores a byline ORCID too once
    AAR_BYLINE_ORCID_PUBMED is on or backfill_byline_orcid --pubmed has run."""
    # v3.15, the rule aar_report_changed_picks._replay_orcid_guard applies: a row whose
    # stored candidates carry `orcid_match` may name somebody only the byline ORCID
    # reaches. On a roster with no ORCID map (AAR_BYLINE_ORCID not on in this shell, or
    # the map failed to load) it would replay to nobody in both passes and be nulled.
    # idxmod.orcid_rows_without_map asks, per row, the roster its matched candidate is
    # on: a WCM index holding a map does not answer for an Ithaca index that holds none.
    stored = [str(r.get("candidate_cwids_json") or "") for r in rows]
    ranked, sourced = idxmod.orcid_rows_without_map(idx, stored)
    if ranked:
        raise RuntimeError(
            f"{ranked} open {lane} rows were ranked by a byline ORCID and this roster "
            f"holds no ORCID map for them (AAR_BYLINE_ORCID is not on here, or the map "
            f"could not be loaded -- see any warning above). Refusing to sweep the {lane} "
            f"lane without it: rows only the ORCID reaches would be classed stale and nulled. "
            f"Set AAR_BYLINE_ORCID=on to sweep as the CronJob matches.")
    # The second switch, by the same rule: a row whose candidates carry `orcid_source`
    # was ranked with AAR_BYLINE_ORCID_WCM on, and may name a WCM person (or a Cornell
    # one on a two-campus list) whom only a labelled map reaches.
    if sourced:
        raise RuntimeError(
            f"{sourced} open {lane} rows were ranked with AAR_BYLINE_ORCID_WCM on (their "
            f"candidates carry orcid_source) and this roster holds no Scholars or "
            f"fallback ORCID list (the switch is not on here, or the lists could not be "
            f"loaded -- see any warning above). Refusing to sweep the {lane} lane without "
            f"them: WCM rows only the ORCID reaches would be classed stale and nulled. Set "
            f"AAR_BYLINE_ORCID=on and AAR_BYLINE_ORCID_WCM=on to sweep as the CronJob matches.")


def _classify_pubmed(rows, idx, limit=None):
    """-> (stale, matched, unresolvable). `stale` entries are {row, before}."""
    import aar_matcher as matcher      # here: it loads the scoring engine, which nothing else needs
    io = None
    rows = rows[:limit] if limit else rows
    _orcid_guard(rows, idx, "pubmed")
    pmids = sorted({r["pmid"] for r in rows if r["pmid"]})
    print(f"      distinct pmids to re-fetch: {len(pmids)}", flush=True)
    groups = uni.load_home_institution_groups()
    arts = uni.efetch_by_ids(pmids, groups)
    by_pmid = {a["pmid"]: a for a in arts}
    print(f"      re-fetched {len(by_pmid)}/{len(pmids)} articles from PubMed", flush=True)

    stale, matched, unresolvable = [], [], []
    for r in rows:
        art = by_pmid.get(r["pmid"])
        pos = r["author_position"]
        if art is None or not pos or pos < 1 or pos > len(art["authors"]):
            unresolvable.append(r)
            continue
        au = art["authors"][pos - 1]
        # The row's stored byline ORCID, as on the scopus lane: on some rows it is the only
        # reason there is a candidate.
        cands, _cohort = idx.candidates(au.get("last"), au.get("fore"), au.get("initials"),
                                        au.get("affiliations"), top_k=5,
                                        pub_year=art.get("pub_year"),
                                        **({"byline_orcid": r["byline_orcid"]}
                                           if r.get("byline_orcid") else {}))
        if cands and matcher.msk_only(au.get("affiliations")):
            # The one rule that empties a list AFTER scoring (withhold_msk_initial).
            io = io or matcher.IdentityOnlyScorer()
            cands = matcher.match_authorship(
                dict(au, byline_orcid=r.get("byline_orcid")), r["pmid"], idx, io, top_k=5,
                pub_year=art.get("pub_year"))
        if cands:
            matched.append(r)
        else:
            stale.append({"row": r, "before": _snapshot(r)})
    return stale, matched, unresolvable


def _classify_scopus(rows, idx, limit=None):
    """-> (stale, matched, unresolvable). Two passes: a free reconstruction pre-filter
    over every open row, then a live Scopus re-fetch of ONLY the rows it flags (see
    `_split_byline`) -- bounds live API calls to roughly the true affected set rather
    than all ~4,940 open scopus rows."""
    rows = rows[:limit] if limit else rows
    family = scop.load_family_afids()

    _orcid_guard(rows, idx, "scopus")
    candidate_stale, matched = [], []
    for r in rows:
        fore, last = _split_byline(r["wcm_author"])
        affils = [a for a in (r["author_affiliation"] or "").split(" | ") if a]
        # The row's stored byline ORCID (v3.15) goes with the name: on some rows the
        # ORCID is the only reason there is a candidate, and without it this sweep would
        # read them as naming nobody and null the pick.
        cands, _cohort = idx.candidates(last, fore, None, affils, top_k=5,
                                        **({"byline_orcid": r["byline_orcid"]}
                                           if r.get("byline_orcid") else {}))
        if cands:
            # ponytail: trusted on the split alone -- this row is NEVER live re-
            # verified below. `_split_byline` reads the LAST token as the surname, so
            # a genuinely multi-token surname ("Robert St John") is misread; if that
            # wrong surname's pool still happens to yield an initials-tier candidate,
            # a truly stale row lands here and silently survives the sweep for good.
            # Undercount-only (never a false null -- this path never nulls anything).
            # Measured 2026-08-30: 3,207 of 4,886 rows retained here have a >2-token
            # wcm_author (loose upper bound; see the FIDELITY note in the module
            # docstring). Upgrade: drop this pre-filter and live-verify every open
            # scopus row (removes the Scopus-quota rationale it exists for).
            matched.append(r)
        else:
            candidate_stale.append(r)
    print(f"      reconstruction pre-filter: {len(candidate_stale)} candidate-stale "
          f"(need live re-verify), {len(matched)} retained as still-matching "
          f"(not live re-verified)", flush=True)

    stale, unresolvable = [], []
    for i, r in enumerate(candidate_stale, 1):
        doi, ext = r["doi"], r["external_id"]
        query = f"DOI({doi})" if doi else (f"EID(2-s2.0-{ext})" if ext else None)
        if not query:
            unresolvable.append(r)
            continue
        try:
            res = scop._scopus_get(query, 0).get("search-results", {})
            entries = res.get("entry", []) or []
            entry = entries[0] if entries and "error" not in entries[0] else None
            hit = None
            if entry is not None:
                for pos, _n, au, _hits in scop.wcm_authorships(entry, family):
                    if pos == r["author_position"] - 1:
                        hit = au
                        break
            if hit is None:
                unresolvable.append(r)
            else:
                # No venue subfield (v3.14): the venue term re-orders a candidate list
                # and never empties one, and empty-or-not is all this sweep asks. The
                # byline ORCID (v3.15) can fill an empty one, so it is passed.
                cands, _cohort = scop.wcm_lane_candidates(
                    idx, hit, scop._pub_year(entry), byline_orcid=r.get("byline_orcid"))
                if cands:
                    matched.append(r)
                else:
                    stale.append({"row": r, "before": _snapshot(r)})
        except Exception as e:                          # noqa: BLE001
            print(f"      [{i}/{len(candidate_stale)}] ERROR {query} (id={r['id']}): {e}",
                  flush=True)
            unresolvable.append(r)
        if i % 20 == 0:
            print(f"      live-verified {i}/{len(candidate_stale)}", flush=True)
        time.sleep(scop.SLEEP)
    return stale, matched, unresolvable


# ---- DB I/O ------------------------------------------------------------------
_SELECT_COLS = ["id", "source", "pmid", "external_id", "doi", "author_key",
                "author_position", "wcm_author", "author_affiliation", "status",
                "first_seen"] + NULL_COLUMNS + ["byline_orcid"]   # v3.15, optional


def _open_rows(engine, source):
    from sqlalchemy import text
    with engine.connect() as c:
        cols = ", ".join(aar_db.present_cols(c, _SELECT_COLS))
        rows = c.execute(text(
            f"SELECT {cols} FROM authorship_review "
            "WHERE source=:s AND status='open'"), {"s": source}).mappings().all()
    return [dict(r) for r in rows]


def _apply_updates(engine, stale_entries):
    """NULL the 9 columns (and top_venue_fit and top_orcid_match, where the table has
    them: see ALSO_NULL) for
    the given confirmed-stale rows, by primary key, re-checking status='open' at UPDATE
    time so a curator action between the SELECT and here wins. Chunked 500/statement,
    same convention as aar_db.upsert."""
    from sqlalchemy import text
    if not stale_entries:
        return 0
    n = 0
    ids = [{"id": e["row"]["id"]} for e in stale_entries]
    with engine.begin() as c:
        stmt = text(_null_sql(aar_db.missing_optional_cols(c)))
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            c.execute(stmt, chunk)
            n += len(chunk)
    return n


def _null_sql(absent=()):
    """The sweep's UPDATE: the nine, plus each ALSO_NULL column the live table has
    (`absent` is aar_db.missing_optional_cols). Before migration v3.14 that is the
    statement this sweep has always run."""
    sets = ", ".join(f"{c}=NULL" for c in NULL_COLUMNS
                     + [c for c in ALSO_NULL if c not in absent])
    return f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open'"


def _write_ledger(path, entries, run_ts, applied):
    with open(path, "w") as fh:
        for e in entries:
            r = e["row"]
            fh.write(json.dumps({
                "id": r["id"], "source": r["source"], "author_key": r["author_key"],
                "pmid": r["pmid"], "external_id": r["external_id"], "doi": r["doi"],
                "author_position": r["author_position"], "wcm_author": r["wcm_author"],
                "before": e["before"], "after": {c: None for c in NULL_COLUMNS},
                "swept_at": run_ts, "applied": applied,
            }) + "\n")


# ---- driver --------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true",
                    help="perform the UPDATE; default is a read-only dry run")
    ap.add_argument("--ledger", default=os.path.join(HERE, "aar_sweep_stale_ledger.jsonl"),
                    help="JSONL before/after output path (written on every run, "
                         "dry-run included)")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap rows considered per lane, before any network call "
                         "(sanity-check runs)")
    ap.add_argument("--selftest", action="store_true", help="offline checks, no DB/network")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print("Resolved producer modules (must all live under this file's directory):")
    for _mod in (aar_db, idxmod, uni, scop):
        print(f"  {_mod.__name__:20} {_mod.__file__}")
    print()

    engine = aar_db.engine()
    # Both campuses, per byline, exactly as the producer now matches (CampusRoster) --
    # a WCM-only replay would read every Cornell proposal as drift.
    idx = idxmod.CampusRoster.load()
    n_roster = sum(len(v) for v in idx.by_surname.values())
    print(f"Identity roster: {n_roster} people\n")

    print("[pubmed] loading open rows ...", flush=True)
    pm_rows = _open_rows(engine, "pubmed")
    print(f"      {len(pm_rows)} open pubmed rows", flush=True)
    pm_stale, pm_matched, pm_unresolvable = _classify_pubmed(pm_rows, idx, args.limit)

    print("\n[scopus] loading open rows ...", flush=True)
    sc_rows = _open_rows(engine, "scopus")
    print(f"      {len(sc_rows)} open scopus rows", flush=True)
    sc_stale, sc_matched, sc_unresolvable = _classify_scopus(sc_rows, idx, args.limit)

    stale = pm_stale + sc_stale
    _write_ledger(args.ledger, stale, run_ts, applied=args.apply)

    pm_processed = len(pm_stale) + len(pm_matched) + len(pm_unresolvable)
    sc_processed = len(sc_stale) + len(sc_matched) + len(sc_unresolvable)
    print("\n==== SWEEP SUMMARY (issue #177, Option 1) ====")
    print(f"  pubmed : open={len(pm_rows)} considered={pm_processed}"
          f"  still-matched={len(pm_matched)}  STALE={len(pm_stale)}"
          f"  unresolvable={len(pm_unresolvable)}")
    print(f"  scopus : open={len(sc_rows)} considered={sc_processed}"
          f"  still-matched={len(sc_matched)}  STALE={len(sc_stale)}"
          f"  unresolvable={len(sc_unresolvable)}")
    print(f"  TOTAL STALE (would be cleared): {len(stale)}"
          f"  ({len(pm_stale)} pubmed + {len(sc_stale)} scopus)")
    print(f"\n  issue #177 reported 559 (503 pubmed / 56 scopus), measured against merged "
          f"commit d0859a1 -- BEFORE PR #172 (preferred publishing name) merged as "
          f"3712b35, which this run is on. #172 independently recovers part of that "
          f"559 (see the docstring's Rosen-anchor example), so a smaller count here is "
          f"expected, not a discrepancy to chase.")
    if pm_unresolvable:
        print(f"\n  pubmed unresolvable ids (untouched): "
              f"{[r['id'] for r in pm_unresolvable][:20]}"
              f"{' ...' if len(pm_unresolvable) > 20 else ''}")
    if sc_unresolvable:
        print(f"  scopus unresolvable ids (untouched): "
              f"{[r['id'] for r in sc_unresolvable][:20]}"
              f"{' ...' if len(sc_unresolvable) > 20 else ''}")
    print(f"\n  ledger -> {args.ledger} ({len(stale)} rows)")

    if args.apply:
        n = _apply_updates(engine, stale)
        print(f"\n  APPLIED: {n} rows updated (NULL: {', '.join(NULL_COLUMNS)})")
    else:
        print(f"\n  DRY RUN -- 0 rows written. Re-run with --apply to NULL these "
              f"{len(stale)} rows for real.")


# ---- self-test -------------------------------------------------------------
def _selftest():
    """Offline: no DB, no network. Exercises the byline reconstruction and proves the
    classify logic's core distinction (empty vs non-empty candidates) against a hand-
    built roster, the same pattern identity_index._selftest / aar_matcher._rank_selftest
    use."""
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    check("NULL_COLUMNS is exactly the issue's 9 columns",
          NULL_COLUMNS == ["top_cwid", "top_name", "top_person_type", "top_dept",
                           "top_given_match", "top_affil_match", "top_confidence",
                           "candidate_cwids_json", "n_candidates"])
    check("every NULL_COLUMNS entry is producer-refreshed (aar_db._REFRESH_COLS)",
          all(c in aar_db._REFRESH_COLS for c in NULL_COLUMNS))
    check("a swept row loses its top_venue_fit (v3.14) and top_orcid_match (v3.15) with "
          "its top pick: neither is left on a row that names nobody",
          _null_sql() == "UPDATE authorship_review SET "
          + ", ".join(f"{c}=NULL" for c in NULL_COLUMNS)
          + ", top_venue_fit=NULL, top_orcid_match=NULL WHERE id=:id AND status='open'")
    check("...before v3.15 only the fit goes, and before both migrations the UPDATE is "
          "the one it always was",
          _null_sql(["top_orcid_match", "byline_orcid"]) == "UPDATE authorship_review SET "
          + ", ".join(f"{c}=NULL" for c in NULL_COLUMNS)
          + ", top_venue_fit=NULL WHERE id=:id AND status='open'"
          and _null_sql(aar_db._OPTIONAL_COLS) == "UPDATE authorship_review SET "
          + ", ".join(f"{c}=NULL" for c in NULL_COLUMNS) + " WHERE id=:id AND status='open'")

    check("_split_byline: 'Riley D Mayne' -> fore='Riley D', last='Mayne'",
          _split_byline("Riley D Mayne") == ("Riley D", "Mayne"))
    check("_split_byline: bare last name -> (None, last)",
          _split_byline("Rosen") == (None, "Rosen"))
    check("_split_byline: empty/None -> (None, None)",
          _split_byline(None) == (None, None) and _split_byline("") == (None, None))

    def rec(given, middle, surname, cwid, pref_first=None):
        return {"cwid": cwid, "given": given, "middle": middle, "surname": surname,
                "given_norm": idxmod._norm(given), "surname_norm": idxmod._norm(surname),
                "pref_norm": idxmod._norm(pref_first),
                "dept": "", "division": "", "program": "", "title": "",
                "person_type": "Full-Time Faculty", "historical": False, "end_year": None}

    # Mirrors the live row-70797 shape: post-#173 the middleName-initial path is gone,
    # so a byline naming the person's PREFERRED first name is the only way in (#172).
    idx = IdentityIndex([rec("Anthony", "Ehren", "Rosen", "aer2006", pref_first="Tony"),
                        rec("Leah", "Teresa", "Rosen", "ltr4001")])
    matched, _ = idx.candidates("Rosen", "Tony", "T", [])
    check("still-matches case: 'Tony Rosen' resolves via the preferred-name path",
          [c["cwid"] for c in matched] == ["aer2006"])

    # Mirrors the live row-74858 shape: a middleName-initial-only tie has no path in
    # the current matcher at all -- genuinely zero candidates.
    idx2 = IdentityIndex([rec("Nicholas", "R", "Mayne", "ohs9007")])
    stale_cands, _ = idx2.candidates("Mayne", "Riley D", "RD", [])
    check("stale case: 'Riley D Mayne' resolves to nobody (middleName-initial is gone)",
          stale_cands == [])

    # v3.15: a row whose only candidate came through its byline ORCID is not stale. The
    # byline is "Chidi Okafor"; the ORCID on it is on file for Rhea Lindqvist.
    o_idx = IdentityIndex([dict(rec("Rhea", "", "Lindqvist", "rl_c"),
                                campus=idxmod.CAMPUS_ITHACA)], campus=idxmod.CAMPUS_ITHACA,
                          orcid_cwid={"0000-0001-0000-0002": "rl_c"})
    o_row = {"id": 9, "wcm_author": "Chidi Okafor", "author_affiliation": "Cornell University",
             "doi": None, "external_id": None, "author_position": 1,
             "byline_orcid": "0000-0001-0000-0002", **{c: None for c in NULL_COLUMNS}}
    real_family = scop.load_family_afids
    scop.load_family_afids = lambda *a: frozenset()
    import contextlib, io
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            kept = _classify_scopus([o_row], o_idx)
            dropped = _classify_scopus([dict(o_row, byline_orcid=None)], o_idx)
            no_col = _classify_scopus([{k: v for k, v in o_row.items() if k != "byline_orcid"}],
                                      o_idx)
        ranked_row = dict(o_row, candidate_cwids_json='[{"cwid": "rl_c", "orcid_match": true}]')
        try:
            _classify_scopus([ranked_row], IdentityIndex(
                [dict(rec("Rhea", "", "Lindqvist", "rl_c"), campus=idxmod.CAMPUS_ITHACA)],
                campus=idxmod.CAMPUS_ITHACA))
            refused = ""
        except RuntimeError as e:
            refused = str(e)
        with contextlib.redirect_stdout(io.StringIO()):
            ranked_kept = _classify_scopus([ranked_row], o_idx)
        # AAR_BYLINE_ORCID_WCM: a WCM row only a Scholars-confirmed iD reaches. Byline
        # "Chidi Okafor" again, WCM affiliation; the ORCID is on file for Bea Other.
        maps, _ = idxmod.orcid_sources([("bo_w", "0000-0001-0000-0002", "wcm")])
        w_people = [rec("Bea", "", "Other", "bo_w")]
        l_roster = idxmod.CampusRoster(IdentityIndex(w_people,
                                                     orcid_cwid=maps[idxmod.CAMPUS_WCM]))
        w_row = dict(o_row, id=10, author_affiliation="Weill Cornell Medicine, New York, NY",
                     candidate_cwids_json='[{"cwid": "bo_w", "orcid_match": true, '
                                          '"orcid_added": true, "orcid_source": "scholars"}]')
        try:
            _classify_scopus([w_row], idxmod.CampusRoster(IdentityIndex(w_people), ithaca=o_idx))
            w_refused = ""
        except RuntimeError as e:
            w_refused = str(e)
        with contextlib.redirect_stdout(io.StringIO()):
            w_kept = _classify_scopus([w_row], l_roster)
            v315_kept = _classify_scopus(
                [dict(ranked_row, author_affiliation="Cornell University, Ithaca, NY")],
                idxmod.CampusRoster(IdentityIndex(w_people), ithaca=o_idx))
        # Each row is asked of ITS roster: the WCM index holds its labelled map, the
        # Ithaca index (loaded separately) holds none.
        half = idxmod.CampusRoster(l_roster.wcm, ithaca=IdentityIndex(
            [dict(rec("Rhea", "", "Lindqvist", "rl_c"), campus=idxmod.CAMPUS_ITHACA)],
            campus=idxmod.CAMPUS_ITHACA))
        i_row = dict(ranked_row, author_affiliation="Cornell University, Ithaca, NY",
                     candidate_cwids_json='[{"cwid": "rl_c", "campus": "ithaca", '
                                          '"orcid_match": true, "orcid_added": true}]')
        half_refused = []
        for r in (i_row, dict(i_row, candidate_cwids_json=i_row["candidate_cwids_json"].replace(
                "}]", ', "orcid_source": "affiliation_manager"}]'))):
            try:
                _classify_scopus([r], half)
                half_refused.append("")
            except RuntimeError as e:
                half_refused.append(str(e))
        with contextlib.redirect_stdout(io.StringIO()):
            half_wcm = _classify_scopus([w_row], half)
    finally:
        scop.load_family_afids = real_family
    check("a WCM index that holds a map does not pass Ithaca rows when the Ithaca index "
          "holds none: the v3.15 row and the labelled Ithaca row are refused, not nulled; "
          "the WCM row is still swept",
          "1 open scopus rows were ranked by a byline ORCID" in half_refused[0]
          and "1 open scopus rows were ranked with AAR_BYLINE_ORCID_WCM on" in half_refused[1]
          and [r["id"] for r in half_wcm[1]] == [10])
    check("rows ranked through AAR_BYLINE_ORCID_WCM are not swept on a roster holding only "
          "the v3.15 Ithaca map: the sweep refuses and names the switch",
          "1 open scopus rows were ranked with AAR_BYLINE_ORCID_WCM on" in w_refused
          and "AAR_BYLINE_ORCID_WCM=on" in w_refused)
    check("...on a roster with the labelled maps the WCM row only its ORCID reaches still "
          "matches and is not nulled; a v3.15 row needs no labelled map",
          [r["id"] for r in w_kept[1]] == [10] and w_kept[0] == [] and w_kept[2] == []
          and [r["id"] for r in v315_kept[1]] == [9] and v315_kept[0] == [])
    check("rows ranked by a byline ORCID are not swept on a roster with no ORCID map: the "
          "sweep refuses rather than null them; with the map it runs",
          "1 open scopus rows were ranked by a byline ORCID" in refused
          and [r["id"] for r in ranked_kept[1]] == [9] and ranked_kept[0] == [])
    check("a row reached only through its stored byline ORCID still matches: the sweep "
          "passes the ORCID, and does not null the pick",
          [r["id"] for r in kept[1]] == [9] and kept[0] == [] and kept[2] == [])
    check("...and the same byline with no ORCID (or on a table without the column) goes "
          "to live re-verification as before, never straight to stale",
          dropped[0] == no_col[0] == [] and dropped[1] == no_col[1] == []
          and [r["id"] for r in dropped[2]] == [r["id"] for r in no_col[2]] == [9])
    check("byline_orcid is read with the open rows",
          _SELECT_COLS[-1] == "byline_orcid" and "byline_orcid" not in NULL_COLUMNS)

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
