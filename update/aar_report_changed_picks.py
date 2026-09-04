#!/usr/bin/env python3
"""
Read-only measurement: open authorship_review rows whose stored top_cwid the CURRENT
matcher (identity_index.py as of ReCiterDB master 3712b35 -- PRs #172/#175/#176) would
now name a DIFFERENT person for. This is the third staleness class described in the
2026-08-30 measurement task, distinct from:

  #177 / PR #180 (aar_sweep_stale.py) -- the row now matches NOBODY  -> candidate columns
      nulled. That tool already measures/fixes this population; this script reports it
      again only as the NO_MATCH bucket, for the same run, so the three buckets partition
      every open row exactly once and nothing is double-counted.
  #181 -- the row proposes someone who ALREADY HOLDS the pmid -- filed, no tool, not this.
  THIS SCRIPT -- the row now matches SOMEONE BETTER (a different top_cwid, still a real
      candidate) -- nothing upstream refreshes this. See the finding on why
      aar_orchestrator._recheck does NOT already do this: it never rewrites top_cwid/
      top_name/top_given_match/top_confidence (only status/resolution_cwid/resolved_date/
      last_checked -- see _resolve()), and its own resolution path requires the pmid to
      already be attributed via reciterdb/GoldStandard, which is orthogonal to whether the
      identity matcher's ranking changed. It also runs on a separate CSV/S3 ledger, not
      this table -- and the scopus lane's own recheck (aar_universe_scopus.recheck_open_
      scopus) is unrelated (PubMed-reachability, not identity matching).

WHAT THIS DOES
  For every OPEN row (both lanes), replay the ORIGINAL authorship through the CURRENT
  IdentityIndex + (pubmed only) the identity-only rescore, exactly as each lane's own
  producer code would today, and classify:
    UNCHANGED  new top-1 cwid == stored top_cwid
    CHANGED    new top-1 cwid != stored top_cwid (a real candidate, just a different one)
    NO_MATCH   new candidate list is empty (already PR #180's territory -- reported here
               only so the three buckets add up to the open total; never double-counted
               against #177's own count, which came from a different run)

  Since #186 each record ALSO carries `drift`: the producer-owned columns the current
  matcher would write differently even when the top pick itself has not moved (an
  UNCHANGED row with a non-empty `drift` is aar_reconcile_open's DRIFT_ONLY class). The
  three classification values above are untouched -- they still partition every row
  exactly once, and this module's CSV and its >50%-UNCHANGED collation sanity check
  mean exactly what they meant before. See the block comment above `_drift_cols` for
  which columns are compared and, crucially, why io_score/final_score/io_source are not.
  One behavioural consequence on the scopus lane: a row the reconstruction pre-filter
  flags as drifted now gets the same live Scopus re-verify a CHANGED row gets, because
  a reconstruction is never a trustworthy basis for a write.

  PUBMED fidelity matches _classify_pubmed in aar_sweep_stale.py exactly: re-fetch the
  source article from PubMed by pmid (aar_universe.efetch_by_ids), index into the real
  AuthorList by the row's stored author_position. The new top-1 is computed with
  aar_matcher.match_authorship() -- the ACTUAL function aar_orchestrator._db_rows calls
  at row-creation time, io-rescore included -- not idx.candidates()[0], because the
  pubmed lane re-sorts idx.candidates() by identity-only score before taking top-1 and a
  replay that skipped that step could report a "change" the producer would never
  actually write, or miss one masked by score-based reordering.

  SCOPUS fidelity: aar_universe_scopus._build_row takes cands[0] directly off
  idx.candidates() with NO io-rescore (there is no per-pmid identity-only score on this
  lane at all), so that is what this script reuses -- literally idx.candidates()[0], no
  aar_matcher involved. A cheap reconstruction pre-filter (_split_byline, the same helper
  aar_sweep_stale.py uses) runs over every open scopus row first, matching exactly the
  bounded-cost two-pass shape that tool already established (dry run 2026-08-30 needed a
  live Scopus call for only the tiny fraction the pre-filter flagged, not all ~4,940).
  Only rows where the reconstruction disagrees with the stored top_cwid -- CHANGED or
  NO_MATCH by that first pass -- get a live Scopus re-fetch (real given-name/initials,
  not a byline split) to confirm before being counted; rows the reconstruction calls
  UNCHANGED are NOT live re-verified, exactly the same disclosed limitation
  aar_sweep_stale.py's own "matched" bucket carries (an undercount-only risk, never a
  false CHANGED/NO_MATCH -- see that module's FIDELITY section). A live re-fetch that
  fails to resolve (retraction, EID/author-list drift) is reported as unresolvable and
  left out of all three buckets, same convention as aar_sweep_stale.py.

SYS.PATH FORK TRAP: same guard as aar_sweep_stale.py -- this file's own directory must
  win over the stale ~/Dropbox/Projects/ReCiter Research/scripts/ copy. Asserted below,
  resolved paths printed.

COLLATION TRAP: authorship_review is read alone, no SQL join against identity/person
  (both unicode_ci vs authorship_review's general_ci -- MySQL 1267, or a filtered
  variant that silently returns zero rows). All cwid comparisons happen in Python via
  `_cwid_eq`, which folds case (general_ci and unicode_ci both fold case; a naive `==`
  would not, and a silent all-mismatch from that bug looks exactly like "everything
  changed" -- see the loud sanity assertion in main() that refuses to report a suspicious
  zero-UNCHANGED result).

THIS SCRIPT NEVER WRITES. It issues SELECT and PubMed/Scopus GET requests only. There is
no --apply flag; that decision belongs to a human after reading this report, per the
task's own instruction.

Usage:
  python aar_report_changed_picks.py                    # full run, both lanes
  python aar_report_changed_picks.py --limit 50          # cap rows per lane (sanity)
  python aar_report_changed_picks.py --check 70797 74858 # named-row check only, no
                                                          # full sweep
"""
import argparse, csv, json, os, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # must be the LAST insert(0, ...) before these imports.
import aar_db
import identity_index as idxmod
import aar_universe as uni
import aar_universe_scopus as scop
import aar_matcher as matcher
from identity_index import IdentityIndex

# Every module this script trusts for matching must resolve to THIS directory, not the
# stale ~/Dropbox/Projects/ReCiter Research/scripts/ copy (sys.path fork trap).
for _mod in (aar_db, idxmod, uni, scop, matcher):
    assert os.path.dirname(os.path.abspath(_mod.__file__)) == HERE, (
        f"{_mod.__name__} resolved to {_mod.__file__}, not {HERE} -- "
        "sys.path fork trap: a stale producer copy is winning. Refusing to trust any "
        "replay result computed against it.")

TIER_RANK = {"unknown": 0, "initial": 1, "full": 2}

# Carries all 9 producer-owned columns (aar_sweep_stale.NULL_COLUMNS) since #186, not
# just the 4 the CHANGED report prints: _drift_cols() below compares every one of them
# against what the current matcher would write. top_person_type/top_dept/
# top_affil_match/n_candidates/candidate_cwids_json ride along unused by this module's
# own CSV -- they exist so the drift check has something to compare.
_SELECT_COLS = ["id", "source", "pmid", "external_id", "doi", "author_key",
                "author_position", "wcm_author", "author_affiliation", "status",
                "first_seen", "top_cwid", "top_name", "top_given_match",
                "top_confidence", "classification", "entrez_date",
                "top_person_type", "top_dept", "top_affil_match",
                "n_candidates", "candidate_cwids_json"]


def _cwid_eq(a, b):
    """Case-folded comparison -- both authorship_review's general_ci and identity's/
    person's unicode_ci collations are case-INsensitive, so a naive Python `==` on
    differently-cased-but-equal cwids would misreport a real UNCHANGED row as CHANGED.
    Never joined in SQL (that's the 1267 trap); this is the Python-side equivalent
    guard for the same underlying fact."""
    if a is None or b is None:
        return a is b
    return str(a).strip().lower() == str(b).strip().lower()


def _open_rows(engine, source):
    from sqlalchemy import text
    cols = ", ".join(_SELECT_COLS)
    with engine.connect() as c:
        rows = c.execute(text(
            f"SELECT {cols} FROM authorship_review "
            "WHERE source=:s AND status='open'"), {"s": source}).mappings().all()
    return [dict(r) for r in rows]


def _tier_move(old_tier, new_tier):
    if old_tier not in TIER_RANK or new_tier not in TIER_RANK:
        return "unknown"
    o, n = TIER_RANK[old_tier], TIER_RANK[new_tier]
    return "stronger" if n > o else "weaker" if n < o else "sideways"


# ---- producer-column drift (ReCiterDB #186) ----------------------------------
# WHY THIS EXISTS, and why `top_cwid != top_cwid` was never enough.
#
# The producer writes a row's proposal once and never revises it, so every matcher
# tightening strands its predecessors' suggestions. #177/#181/#182 each moved the top
# PICK, so keying staleness on top_cwid inequality caught them. #203 (affiliation
# matches must name a department, not the institution) does not: its own replay over
# all 30,711 authorship_review rows measured 3,970 rows seeing a candidate change but
# only 203 top picks moving. The other ~3,767 keep an inflated `top_confidence` (a
# phantom affil match is worth +0.25, the whole gap between a `full` and an `initial`
# given-name match) and render a "Dept match" chip in the curator UI that the current
# matcher knows is wrong -- classified UNCHANGED, never rewritten, wrong forever.
# `tier_move` cannot see it either: #203 never touches `given_match`, so all 203 of the
# picks that DO move are `sideways`, reachable only by --include-sideways, which drags
# in ~1,584 unrelated homonym reshuffles to get at them.
#
# So: drift is "any producer-owned column the current matcher would write differently",
# not "the top pick moved".
#
# THE IO TRAP -- the single thing that makes this comparison hard.
# `io_score`/`final_score`/`io_source` are re-read from live S3 scoring inputs the
# nightly inst-client keeps refreshing. Issue #182 measured the resulting run-to-run
# wobble directly: CHANGED = 2,394 vs 2,398 on two runs 50 minutes apart, same code,
# same queue. Those three fields are stored inside candidate_cwids_json (pubmed lane
# only -- aar_orchestrator._compact keeps them, aar_universe_scopus._compact has no io
# layer at all). If they were part of the drift TRIGGER, nearly every pubmed row would
# drift on every run and the pass would degenerate into rewriting the whole open queue
# nightly. They are therefore excluded from the comparison below -- but they are still
# WRITTEN, by aar_reconcile_open._write_payload, whenever a refresh fires for some
# other reason. Trigger and payload are deliberately different sets.
#
# The exclusion is safe because the candidate SET is io-independent by construction:
# `identity_index.candidates()` sorts by (full, affil_dept_match, confidence) -- all
# identity-derived -- and applies the top_k cut BEFORE `aar_matcher.match_authorship`
# re-sorts the survivors by io score. io can therefore reorder the stored candidate
# list but can never change which 5 people are in it. Two consequences the comparison
# relies on: candidate_cwids_json is compared as an unordered map keyed by cwid (never
# positionally -- position IS io-dependent), and the top_* columns are safe to compare
# field-by-field only because the caller has already established that top_cwid did not
# move, so they all describe the same person.
#
# `years_after_wcm` is identity-derived and deterministic, but is deliberately not a
# trigger either: it reaches the curator only through `confidence` (identity_index's
# TEMPORAL_PENALTY block: "the single route by which staleness reaches either sort
# key"), which IS compared, and through the separate top_years_after_wcm column, which
# is not one of the 9 columns this reconciliation owns.
_DRIFT_CAND_FIELDS = ("name", "person_type", "dept", "given_match",
                      "affil_dept_match", "cohort_size", "confidence")

# VARCHAR widths aar_reconcile_open._write_payload truncates to, so a stored value that
# was cut to fit is not mistaken for drift. (Asserted consistent with _write_payload's
# own output by aar_reconcile_open._selftest.)
_DRIFT_TRUNC = {"top_name": 255, "top_person_type": 64, "top_dept": 255}

# `confidence` is round(..., 3) at the source (identity_index._confidence), and
# `top_confidence` is a 4-byte FLOAT column, so a stored 0.775 reads back as
# 0.774999976... Half the 0.001 quantum is therefore both far above float32's ~6e-8
# error on [0,1] and far below the smallest real change -- it cannot manufacture drift,
# and cannot hide one.
_CONF_TOL = 5e-4


def _norm_txt(v, n=None):
    """None and '' are the same absence for drift purposes: refreshing a NULL dept to
    '' would rewrite a row to no visible effect. Truncated to the column width when one
    is given, so `_trunc`'s own cut never reads as a difference."""
    if v is None:
        return None
    s = str(v)
    if n is not None:
        s = s[:n]
    return s or None


def _norm_flag(v):
    return None if v is None else int(bool(v))


def _conf_ne(a, b):
    if a is None or b is None:
        return (a is None) != (b is None)
    return abs(float(a) - float(b)) > _CONF_TOL


def _cand_map(cands):
    """cwid (case-folded, per the collation trap) -> the identity-derived fields only.
    Unordered on purpose: candidate ORDER is io-dependent on the pubmed lane."""
    out = {}
    for c in cands or []:
        cw = c.get("cwid")
        if cw is None:
            continue
        out[str(cw).strip().lower()] = {f: c.get(f) for f in _DRIFT_CAND_FIELDS}
    return out


def _cands_drifted(stored_json, cands):
    stored = stored_json
    if isinstance(stored, str):
        try:
            stored = json.loads(stored) if stored.strip() else []
        except ValueError:
            return True            # unparseable stored JSON -- rewriting it is the fix
    old, new = _cand_map(stored), _cand_map(cands)
    if set(old) != set(new):
        return True
    for cw, o in old.items():
        n = new[cw]
        if _conf_ne(o.get("confidence"), n.get("confidence")):
            return True
        if _norm_flag(o.get("affil_dept_match")) != _norm_flag(n.get("affil_dept_match")):
            return True
        if (o.get("cohort_size") is None) != (n.get("cohort_size") is None) or (
                o.get("cohort_size") is not None
                and int(o["cohort_size"]) != int(n["cohort_size"])):
            return True
        for f in ("name", "person_type", "dept", "given_match"):
            if _norm_txt(o.get(f)) != _norm_txt(n.get(f)):
                return True
    return False


def _drift_cols(r, cands):
    """Which of the 9 producer-owned columns the CURRENT matcher would write
    differently from what this row already stores. Returns a sorted list of column
    names; [] means the stored proposal is still exactly what the producer would emit.

    Only meaningful for a row whose top_cwid did NOT move -- top_cwid is the 9th column
    and its comparison IS the CHANGED/UNCHANGED split in _row_result, so it is not
    re-tested here. See the block comment above for what is and is not a trigger."""
    top = (cands or [None])[0]
    if top is None:
        return []
    out = []
    for col, new in (("top_name", top.get("name")),
                     ("top_person_type", top.get("person_type")),
                     ("top_dept", top.get("dept"))):
        if _norm_txt(r.get(col), _DRIFT_TRUNC[col]) != _norm_txt(new, _DRIFT_TRUNC[col]):
            out.append(col)
    if _norm_txt(r.get("top_given_match")) != _norm_txt(top.get("given_match")):
        out.append("top_given_match")
    if _norm_flag(r.get("top_affil_match")) != _norm_flag(top.get("affil_dept_match")):
        out.append("top_affil_match")
    if _conf_ne(r.get("top_confidence"), top.get("confidence")):
        out.append("top_confidence")
    n_old = r.get("n_candidates")
    if n_old is None or int(n_old) != len(cands):
        out.append("n_candidates")
    if _cands_drifted(r.get("candidate_cwids_json"), cands):
        out.append("candidate_cwids_json")
    return sorted(out)


def _row_result(r, new_top, source, cands=None):
    """Common CHANGED/UNCHANGED/NO_MATCH record for either lane.

    `cands` is the FULL ranked candidate list the classification was computed from
    (#186 -- this is the additive hand-off aar_reconcile_open's REUSE note named as the
    upgrade path out of its monkeypatch). Passing it adds `rec["drift"]`: the
    producer-owned columns that are stale even though the top PICK is not. The
    classification VALUE is deliberately unchanged -- UNCHANGED/CHANGED/NO_MATCH still
    partition every row exactly once, so this module's own CSV, its buckets and its
    >50%-UNCHANGED collation sanity check all keep their existing meaning."""
    old_cwid = r["top_cwid"]
    if new_top is None:
        cls = "NO_MATCH"
    elif _cwid_eq(old_cwid, new_top["cwid"]):
        cls = "UNCHANGED"
    else:
        cls = "CHANGED"
    rec = {
        "id": r["id"], "source": source, "pmid": r["pmid"], "external_id": r["external_id"],
        "wcm_author": r["wcm_author"], "classification": cls,
        "old_cwid": old_cwid, "old_name": r["top_name"],
        "old_given_match": r["top_given_match"], "old_confidence": r["top_confidence"],
        "new_cwid": new_top["cwid"] if new_top else None,
        "new_name": new_top["name"] if new_top else None,
        "new_given_match": new_top["given_match"] if new_top else None,
        "new_confidence": round(new_top["confidence"], 3) if new_top else None,
        "drift": _drift_cols(r, cands) if cls == "UNCHANGED" else [],
    }
    if cls == "CHANGED":
        rec["tier_move"] = _tier_move(r["top_given_match"], new_top["given_match"])
    return rec


# ---- pubmed lane -------------------------------------------------------------
def _replay_pubmed(rows, idx, io_scorer, limit=None, io_rescore=True):
    """io_rescore=True (default) is what aar_orchestrator._db_rows would ACTUALLY write
    if this pmid were reprocessed today -- aar_matcher.match_authorship()'s real ranking.
    io_rescore=False reports the identity_index.py ranking alone (no IO layer at all,
    exactly what the scopus lane does), which isolates a set of identity PRs from
    elapsed-time churn in ReCiter's ongoing retrieval.

    HISTORICAL, and the reason the "weaker" bucket below exists at all: match_authorship
    used to LEAD its key with the identity-only score, so "any io_score beats no
    io_score" however small -- a candidate ReCiter's ordinary, unrelated ongoing
    retrieval had since scored, even at 1-3/100 ("not this person"), outranked a full
    given-name match nobody had retrieved yet. Measured 2026-08-31 (see the run note this
    function's caller prints): with the rescore, pubmed CHANGED=2394 including 67 weaker;
    with it turned off, pubmed CHANGED=3320, stronger=620, sideways=2700, weaker=0 --
    exactly zero, on every one of the 12,292 open rows. That tie-break was the ENTIRE
    explanation for every weaker tier-move.

    That key now leads with the given-name tier instead (see aar_matcher.match_authorship
    -- 76 curator-resolved rows fixed, 1 broken, over 19,050 replayed pubmed rows), so
    NEITHER ranking can produce a weaker-tier top pick from the io layer any more, and
    the two modes should differ only in which candidate wins WITHIN a tier. A weaker move
    surviving this replay now means the underlying identity data itself changed, not a
    tie-break artifact -- worth looking at rather than assuming, though the callers'
    hard-exclusion of weaker moves stays correct either way."""
    rows = rows[:limit] if limit else rows
    pmids = sorted({r["pmid"] for r in rows if r["pmid"]})
    print(f"      distinct pmids to re-fetch: {len(pmids)}", flush=True)
    groups = uni.load_home_institution_groups()
    arts = uni.efetch_by_ids(pmids, groups)
    by_pmid = {a["pmid"]: a for a in arts}
    print(f"      re-fetched {len(by_pmid)}/{len(pmids)} articles from PubMed", flush=True)

    # first pass (no network): resolve each row's real author block + raw candidates,
    # so we can pre-warm the identity-only scorer over the exact distinct cwid pool the
    # io-rescore will need, mirroring aar_orchestrator.run() step 4.
    prepped, unresolvable = [], []
    pool = set()
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
        pool.update(c["cwid"] for c in cands)
        prepped.append((r, au, art.get("pub_year")))

    if not io_rescore:
        print(f"      --no-io-rescore: skipping identity-only warm-up ({len(pool)} cwids "
              f"never fetched); ranking by identity_index.py alone", flush=True)
        results = []
        for r, au, pub_year in prepped:
            cands, _c = idx.candidates(au.get("last"), au.get("fore"), au.get("initials"),
                                       au.get("affiliations"), top_k=5, pub_year=pub_year)
            top = cands[0] if cands else None
            results.append(_row_result(r, top, "pubmed", cands))
        return results, unresolvable

    print(f"      identity-only pre-warm pool: {len(pool)} distinct candidate cwids "
          f"(only rows with >=1 candidate contribute)", flush=True)
    warmed = [0]

    def _warm(c):
        io_scorer.scores(c)
        warmed[0] += 1
        if warmed[0] % 500 == 0:
            print(f"      IO-warmed {warmed[0]}/{len(pool)}", flush=True)

    if pool:
        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(_warm, sorted(pool)))
    print(f"      identity-only cache warm complete: {len(pool)} cwids", flush=True)

    results = []
    for r, au, pub_year in prepped:
        cands = matcher.match_authorship(au, r["pmid"], idx, io_scorer, top_k=5,
                                         pub_year=pub_year)
        top = cands[0] if cands else None
        results.append(_row_result(r, top, "pubmed", cands))
    return results, unresolvable


# ---- scopus lane --------------------------------------------------------------
def _split_byline(wcm_author):
    """Identical to aar_sweep_stale._split_byline -- kept local so this script has no
    import-order dependency on that module's own guarded sys.path setup."""
    if not wcm_author or not isinstance(wcm_author, str):
        return None, None
    parts = wcm_author.split()
    if len(parts) <= 1:
        return None, wcm_author
    return " ".join(parts[:-1]), parts[-1]


def _scopus_pub_year(r):
    """The paper's own publication year, recovered from the row itself with no network
    call: aar_universe_scopus._build_row stores Scopus `prism:coverDate` in `entrez_date`
    ("Scopus has no entrez date; coverDate ~ recency"), and that lane's own _pub_year()
    is exactly that date's first four characters.

    Passing it makes the reconstruction agree with the producer on the temporal penalty
    (issue #159). Without it every candidate who has left WCM comes back unpenalised, so
    a scopus row whose ONLY staleness is that penalty reconstructs to exactly its stored
    value, reads as un-drifted, is never live-verified and is missed -- the same
    undercount-only failure the pre-filter's own disclosed limitation describes, now
    reaching drift as well as CHANGED.

    It is a completeness fix, not a cost saving, and the measurement says so plainly:
    over the same 200 open scopus rows on 2026-09-04 the pre-filter flagged 147 rows for
    live re-verify without the year and 144 with it. The flag rate is high because the
    lane really is stale (135 of those 144 were confirmed drifted or CHANGED by the live
    re-fetch), not because the reconstruction is guessing badly."""
    d = r.get("entrez_date")
    if d is None:
        return None
    s = str(d)
    return int(s[:4]) if s[:4].isdigit() else None


def _replay_scopus(rows, idx, limit=None):
    rows = rows[:limit] if limit else rows
    family = scop.load_family_afids()

    # pass 1: free reconstruction, every row, no network.
    prelim = []
    for r in rows:
        fore, last = _split_byline(r["wcm_author"])
        affils = [a for a in (r["author_affiliation"] or "").split(" | ") if a]
        cands, _cohort = idx.candidates(last, fore, None, affils, top_k=5,
                                        pub_year=_scopus_pub_year(r))
        top = cands[0] if cands else None
        rec = _row_result(r, top, "scopus", cands)
        prelim.append((r, rec))

    # `or rec["drift"]` (#186): the reconstruction pass feeds idx.candidates() a byline
    # split (initials=None) and NO pub_year, so its `confidence` carries no temporal
    # penalty and its given_match tier can be weaker than the producer's -- a
    # reconstruction is good enough to FLAG a row but never good enough to write one.
    # Routing drift suspects through the same live re-fetch CHANGED/NO_MATCH already
    # get keeps the standing guarantee that no scopus row is ever written from a
    # reconstruction; the second _row_result call on the live candidates is what
    # actually decides. Over-inclusive here is free (a wasted verify), under-inclusive
    # is a permanently stale row, so this filter is the cheap side of the trade.
    need_verify = [(r, rec) for r, rec in prelim
                   if rec["classification"] != "UNCHANGED" or rec["drift"]]
    kept_unverified = [rec for r, rec in prelim
                       if rec["classification"] == "UNCHANGED" and not rec["drift"]]
    print(f"      reconstruction pre-filter: {len(kept_unverified)} preliminary UNCHANGED "
          f"(not live-verified, same disclosed limitation as aar_sweep_stale.py's "
          f"'matched' bucket) / {len(need_verify)} flagged for live Scopus re-verify",
          flush=True)

    results, unresolvable = list(kept_unverified), []
    for i, (r, prelim_rec) in enumerate(need_verify, 1):
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
                top = cands[0] if cands else None
                results.append(_row_result(r, top, "scopus", cands))
        except Exception as e:                          # noqa: BLE001
            print(f"      [{i}/{len(need_verify)}] ERROR {query} (id={r['id']}): {e}",
                  flush=True)
            unresolvable.append(r)
        if i % 20 == 0:
            print(f"      live-verified {i}/{len(need_verify)}", flush=True)
        time.sleep(scop.SLEEP)
    return results, unresolvable


# ---- named-row check ----------------------------------------------------------
def _check_ids(engine, idx, io_scorer, ids):
    from sqlalchemy import text
    cols = ", ".join(_SELECT_COLS)
    with engine.connect() as c:
        rows = [dict(r) for r in c.execute(text(
            f"SELECT {cols} FROM authorship_review WHERE id IN :ids"),
            {"ids": tuple(ids)}).mappings().all()]
    pm_rows = [r for r in rows if r["source"] == "pubmed"]
    sc_rows = [r for r in rows if r["source"] == "scopus"]
    out = []
    if pm_rows:
        res, unresolv = _replay_pubmed(pm_rows, idx, io_scorer)
        out += res
        out += [{"id": r["id"], "source": "pubmed", "classification": "UNRESOLVABLE"}
                for r in unresolv]
    if sc_rows:
        res, unresolv = _replay_scopus(sc_rows, idx)
        out += res
        out += [{"id": r["id"], "source": "scopus", "classification": "UNRESOLVABLE"}
                for r in unresolv]
    return out


# ---- driver --------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None,
                    help="cap rows considered per lane, before any network call "
                         "(sanity-check runs)")
    ap.add_argument("--check", type=int, nargs="*", default=None,
                    help="only replay these specific authorship_review ids and print "
                         "old vs new, then exit -- no full sweep")
    ap.add_argument("--no-io-rescore", action="store_true",
                    help="pubmed lane: rank by identity_index.py alone (skip "
                         "aar_matcher.match_authorship's identity-only rescore). Isolates "
                         "PRs #172/#173/#174's own effect from ordinary io-score-"
                         "availability churn -- see _replay_pubmed's docstring. The "
                         "default (rescore on) is what the producer would actually write "
                         "if these rows were reprocessed today; this flag answers a "
                         "narrower question, not a more correct one.")
    ap.add_argument("--out-dir", default=HERE,
                    help="where the CSV/JSONL of CHANGED rows lands (default: this "
                         "file's own directory, same convention as aar_sweep_stale.py's "
                         "--ledger default -- gitignored, real prod row content, never "
                         "committed)")
    args = ap.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print("Resolved producer modules (must all live under this file's directory):")
    for _mod in (aar_db, idxmod, uni, scop, matcher):
        print(f"  {_mod.__name__:20} {_mod.__file__}")
    print()

    engine = aar_db.engine()
    idx = IdentityIndex.load()
    n_roster = sum(len(v) for v in idx.by_surname.values())
    print(f"Identity roster: {n_roster} people\n")
    io_scorer = matcher.IdentityOnlyScorer()

    if args.check:
        print(f"==== NAMED-ROW CHECK: {args.check} ====\n")
        out = _check_ids(engine, idx, io_scorer, args.check)
        for rec in out:
            print(json.dumps(rec, indent=2, default=str))
        return

    print("[pubmed] loading open rows ...", flush=True)
    pm_rows = _open_rows(engine, "pubmed")
    print(f"      {len(pm_rows)} open pubmed rows", flush=True)
    pm_results, pm_unresolvable = _replay_pubmed(pm_rows, idx, io_scorer, args.limit,
                                                  io_rescore=not args.no_io_rescore)

    print("\n[scopus] loading open rows ...", flush=True)
    sc_rows = _open_rows(engine, "scopus")
    print(f"      {len(sc_rows)} open scopus rows", flush=True)
    sc_results, sc_unresolvable = _replay_scopus(sc_rows, idx, args.limit)

    all_results = pm_results + sc_results

    # ---- loud collation/comparison sanity check ----
    # A silent case-fold / type-mismatch bug in _cwid_eq would make EVERY row look
    # CHANGED (nothing would ever compare equal). That is indistinguishable from a real
    # "everything changed" result unless we assert UNCHANGED is a large, plausible
    # majority -- which it must be, since #172's own PR body measured only 37 of 12,171
    # open pubmed rows changing. Refuse to report a suspiciously-empty UNCHANGED bucket.
    n_unchanged = sum(1 for x in all_results if x["classification"] == "UNCHANGED")
    n_considered = len(all_results)
    assert n_considered == 0 or n_unchanged > 0.5 * n_considered, (
        f"SANITY CHECK FAILED: only {n_unchanged}/{n_considered} rows came back UNCHANGED. "
        "This is the exact signature of a silent cwid-comparison bug (collation/case/type "
        "mismatch), not a real result -- refusing to report. Check _cwid_eq and the "
        "resolved module paths above.")
    print(f"\n  [sanity check OK] {n_unchanged}/{n_considered} rows UNCHANGED "
          f"(>50% required, else refuse to report) -- cwid comparison is not silently "
          f"failing.")

    os.makedirs(args.out_dir, exist_ok=True)
    changed = [x for x in all_results if x["classification"] == "CHANGED"]
    fields = ["id", "source", "pmid", "external_id", "wcm_author", "classification",
              "old_cwid", "old_name", "old_given_match", "old_confidence",
              "new_cwid", "new_name", "new_given_match", "new_confidence", "tier_move"]
    csv_path = os.path.join(args.out_dir, f"{run_ts.replace(':', '').replace(' ', '_')}_changed_picks.csv")
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for rec in changed:
            w.writerow({k: rec.get(k) for k in fields})
    jsonl_path = csv_path.replace(".csv", ".jsonl")
    with open(jsonl_path, "w") as fh:
        for rec in changed:
            fh.write(json.dumps(rec) + "\n")

    def _bucket(results, label):
        by_cls = {}
        for x in results:
            by_cls.setdefault(x["classification"], []).append(x)
        print(f"\n  {label}: considered={len(results)}"
              + "".join(f"  {k}={len(v)}" for k, v in sorted(by_cls.items())))
        return by_cls

    print("\n==== CHANGED-PICKS SWEEP SUMMARY ====")
    pm_by = _bucket(pm_results, "pubmed")
    sc_by = _bucket(sc_results, "scopus")

    pm_changed = pm_by.get("CHANGED", [])
    sc_changed = sc_by.get("CHANGED", [])
    for label, lst in (("pubmed", pm_changed), ("scopus", sc_changed)):
        moves = {}
        for x in lst:
            moves[x.get("tier_move", "unknown")] = moves.get(x.get("tier_move", "unknown"), 0) + 1
        print(f"  {label} CHANGED tier moves: " +
              ", ".join(f"{k}={v}" for k, v in sorted(moves.items())))

    weaker = [x for x in changed if x.get("tier_move") == "weaker"]
    print(f"\n  CHANGED total: {len(changed)} ({len(pm_changed)} pubmed + "
          f"{len(sc_changed)} scopus)")
    print(f"  -> moved to a WEAKER given_match tier: {len(weaker)}")
    if weaker and not args.no_io_rescore:
        print("     CAVEAT: every pubmed weaker-move measured 2026-08-31 traced to "
              "match_authorship's io-rescore ('any io_score beats none', however tiny) "
              "picking up a candidate ReCiter's ordinary unrelated scoring has since "
              "touched -- not a #172/#173/#174 regression. Re-run with --no-io-rescore: "
              "that population drops to exactly 0 weaker (identity_index.py's own given-"
              "name-tier-first ranking can never demote a full match). See "
              "_replay_pubmed's docstring.")
    if weaker:
        print("     examples:")
        for x in weaker[:10]:
            print(f"       id={x['id']} ({x['source']}) '{x['wcm_author']}': "
                  f"{x['old_cwid']}/{x['old_given_match']} -> "
                  f"{x['new_cwid']}/{x['new_given_match']}")

    if pm_unresolvable:
        print(f"\n  pubmed unresolvable ids (untouched, not in any bucket): "
              f"{[r['id'] for r in pm_unresolvable][:20]}"
              f"{' ...' if len(pm_unresolvable) > 20 else ''} ({len(pm_unresolvable)} total)")
    if sc_unresolvable:
        print(f"  scopus unresolvable ids (untouched, not in any bucket): "
              f"{[r['id'] for r in sc_unresolvable][:20]}"
              f"{' ...' if len(sc_unresolvable) > 20 else ''} ({len(sc_unresolvable)} total)")

    print(f"\n  CSV   -> {csv_path}")
    print(f"  JSONL -> {jsonl_path}")
    print(f"  ({len(changed)} CHANGED rows written)")


if __name__ == "__main__":
    main()
