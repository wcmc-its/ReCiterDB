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


def _classify_pubmed(rows, idx, limit=None):
    """-> (stale, matched, unresolvable). `stale` entries are {row, before}."""
    rows = rows[:limit] if limit else rows
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
        cands, _cohort = idx.candidates(au.get("last"), au.get("fore"), au.get("initials"),
                                        au.get("affiliations"), top_k=5,
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

    candidate_stale, matched = [], []
    for r in rows:
        fore, last = _split_byline(r["wcm_author"])
        affils = [a for a in (r["author_affiliation"] or "").split(" | ") if a]
        cands, _cohort = idx.candidates(last, fore, None, affils, top_k=5)
        if cands:
            matched.append(r)
        else:
            candidate_stale.append(r)
    print(f"      reconstruction pre-filter: {len(candidate_stale)} candidate-stale "
          f"(need live re-verify), {len(matched)} already confirmed still-matching",
          flush=True)

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
                cands, _cohort = idx.candidates(hit["last"], hit["fore"], hit["initials"],
                                                hit["affiliations"], top_k=5,
                                                pub_year=scop._pub_year(entry))
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
                "first_seen"] + NULL_COLUMNS


def _open_rows(engine, source):
    from sqlalchemy import text
    cols = ", ".join(_SELECT_COLS)
    with engine.connect() as c:
        rows = c.execute(text(
            f"SELECT {cols} FROM authorship_review "
            "WHERE source=:s AND status='open'"), {"s": source}).mappings().all()
    return [dict(r) for r in rows]


def _apply_updates(engine, stale_entries):
    """NULL the 9 columns for the given confirmed-stale rows, by primary key, re-
    checking status='open' at UPDATE time so a curator action between the SELECT and
    here wins. Chunked 500/statement, same convention as aar_db.upsert."""
    from sqlalchemy import text
    if not stale_entries:
        return 0
    sets = ", ".join(f"{c}=NULL" for c in NULL_COLUMNS)
    stmt = text(f"UPDATE authorship_review SET {sets} WHERE id=:id AND status='open'")
    n = 0
    ids = [{"id": e["row"]["id"]} for e in stale_entries]
    with engine.begin() as c:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            c.execute(stmt, chunk)
            n += len(chunk)
    return n


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
    idx = IdentityIndex.load()
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

    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


if __name__ == "__main__":
    main()
