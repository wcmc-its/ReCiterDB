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

_SELECT_COLS = ["id", "source", "pmid", "external_id", "doi", "author_key",
                "author_position", "wcm_author", "author_affiliation", "status",
                "first_seen", "top_cwid", "top_name", "top_given_match",
                "top_confidence", "classification"]


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


def _row_result(r, new_top, source):
    """Common CHANGED/UNCHANGED/NO_MATCH record for either lane."""
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
    }
    if cls == "CHANGED":
        rec["tier_move"] = _tier_move(r["top_given_match"], new_top["given_match"])
    return rec


# ---- pubmed lane -------------------------------------------------------------
def _replay_pubmed(rows, idx, io_scorer, limit=None, io_rescore=True):
    """io_rescore=True (default) is what aar_orchestrator._db_rows would ACTUALLY write
    if this pmid were reprocessed today -- aar_matcher.match_authorship()'s real ranking,
    identity-only score first. io_rescore=False reports the identity_index.py ranking
    alone (given-name tier first, exactly what the scopus lane already does with no IO
    layer at all), which isolates PRs #172/#173/#174's own effect from a confound this
    script's own measurement surfaced: match_authorship's tie-break ("any io_score beats
    no io_score", however small) means a candidate ReCiter's ORDINARY, unrelated ongoing
    retrieval has since scored -- even at 1-3/100, essentially "not this person" -- now
    outranks a full given-name match nobody has retrieved yet. That is real production
    behavior, not a bug, but it is pure elapsed-time churn, unconnected to this task's
    three PRs, and it is the ENTIRE explanation for every "weaker" tier-move this script
    finds with io_rescore=True. Measured 2026-08-31 (see the run note this function's
    caller prints): with the rescore, pubmed CHANGED=2394 including 67 weaker; with it
    turned off, pubmed CHANGED=3320, stronger=620, sideways=2700, weaker=0 -- exactly
    zero, on every one of the 12,292 open rows. The identity-index ranking itself (given
    name tier before confidence) can literally never produce a weaker-tier top pick;
    only the io-rescore's null-vs-any-score tie-break can, and only via candidates
    neither #172 nor #173 nor #174 touches."""
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
            results.append(_row_result(r, top, "pubmed"))
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
        results.append(_row_result(r, top, "pubmed"))
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


def _replay_scopus(rows, idx, limit=None):
    rows = rows[:limit] if limit else rows
    family = scop.load_family_afids()

    # pass 1: free reconstruction, every row, no network.
    prelim = []
    for r in rows:
        fore, last = _split_byline(r["wcm_author"])
        affils = [a for a in (r["author_affiliation"] or "").split(" | ") if a]
        cands, _cohort = idx.candidates(last, fore, None, affils, top_k=5)
        top = cands[0] if cands else None
        rec = _row_result(r, top, "scopus")
        prelim.append((r, rec))

    need_verify = [(r, rec) for r, rec in prelim if rec["classification"] != "UNCHANGED"]
    kept_unverified = [rec for r, rec in prelim if rec["classification"] == "UNCHANGED"]
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
                results.append(_row_result(r, top, "scopus"))
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
                         "committed; analysis/adversarial_attribution_review/ does not "
                         "exist in this repo, the S3-backed ledger superseded it)")
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
