#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 3: identity matcher.

Given a WCM-affiliated authorship on an orphan article (one PubMed author block
carrying a home-institution affiliation, from `aar_universe.py`), propose the
likely CWID(s) and rank them. This is the lightweight matcher the plan calls for
(reciterdb `identity` name-match, NOT a ReCiter feature-generator call).

Pipeline per authorship:
  1. Name-match against reciterdb `identity` (surname + given/initial). The whole
     35k-row table is loaded once and indexed by normalised surname in memory
     (surname is unindexed in the DB, so per-author WHERE would full-scan).
  2. Homonym guard: cohort size (how many WCM people share this surname+initial)
     and an affiliation/department signal (does the PubMed affiliation text name
     the candidate's department/division?). Both feed a transparent confidence.
  3. Identity-only ranking for *buried* candidates: ReCiter's pinned identity-only
     calibrated score (0-100) for (candidate, pmid), recomputed from the S3 scoring
     input via the Step-0 engine. Candidates ReCiter retrieved-but-buried get a real
     score; never-retrieved / cold-storage ones fall back to matcher confidence.

Ranking key per candidate: (identity-only score desc, full>initial given match,
confidence desc). The orchestrator (next step) calls `match_authorship()` for each
WCM authorship on each orphan article and writes the ranked candidates to the ledger.

Confidence is an explainable ordering aid, NOT a probability:
    base   = 0.50 full given-name match | 0.25 initial-only
    rarity = 0.40 / cohort_size          (1 person -> 0.40, 2 -> 0.20, ...)
    affil  = +0.25 if the affiliation text names the candidate's dept/division
    hist   = -0.10 if the person is alumni / inactive / emeritus
    -> clipped to [0, 1]

Env: DB_USERNAME/DB_PASSWORD/DB_HOST/DB_NAME (reciterdb, read-only). S3 + pinned
models inherited from the Step-0 engine for the identity-only ranking layer.

Usage:
  python aar_matcher.py --selftest
  python aar_matcher.py --surname Worgall --given Stefan --pmid 42220538 \
      --affil "Department of Pediatrics, Weill Cornell Medicine, New York, NY"
"""
import argparse, json, os, sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adversarial_attribution_review as det   # Step-0 scoring engine (S3 + pinned models)
# IdentityIndex + name helpers + person-type table extracted to identity_index.py so the
# Scopus lane can reuse them without this module's S3/model deps. Re-exported here for
# any importer that still reaches for aar_matcher.IdentityIndex / _norm.
from identity_index import IdentityIndex, _norm, _first_initial, PERSON_TYPES  # noqa: F401

IDENTITY_ONLY_SUFFIX = "-identityOnlyScoringInput.json"


# ---- identity-only ranking layer (S3 + pinned models) ----------------------
class IdentityOnlyScorer:
    """Per-CWID cache of {pmid: (io, final)} from ReCiter's pinned models.

    io    = identity-only calibrated score (0-100) — ranks buried candidates.
    final = the PRODUCTION final the storage threshold sees: feedback users
            min(fb, io*33)*100; feedback-less users = identity-only (that IS their
            production pipeline). A pub with final >= 30 is already SUGGESTED in the
            curator's pending queue, so it is NOT buried — the orchestrator drops
            articles where any WCM candidate reaches final >= 30.

    Reuses the Step-0 engine: feedback users carry identity base features in their
    feedback input (score both pipelines on it); feedback-less users use their
    identity-only input. Cold-storage / missing -> {} (absent: never scored)."""

    def __init__(self):
        self._cache = {}

    def scores(self, cwid):
        if cwid not in self._cache:
            self._cache[cwid] = self._compute(cwid)
        return self._cache[cwid]

    def score(self, cwid, pmid):
        v = self.scores(cwid).get(int(pmid))       # io (ranking)
        return v[0] if v else None

    def final(self, cwid, pmid):
        v = self.scores(cwid).get(int(pmid))       # production final (gate)
        return v[1] if v else None

    def _compute(self, cwid):
        status, _, rows = det.score_user(cwid)     # feedback-input path
        if status == "ok":
            return {r["pmid"]: (r["io_score"], r["final_score"]) for r in rows}
        if status == "missing":
            return self._identity_only_input(cwid)
        return {}                                  # error / empty / cold-storage

    def _identity_only_input(self, cwid):
        try:
            obj = det._s3.get_object(Bucket=det.BUCKET, Key=cwid + IDENTITY_ONLY_SUFFIX)
            arts = json.loads(obj["Body"].read())
        except det._s3.exceptions.NoSuchKey:
            return {}
        except Exception:                          # noqa: BLE001  (cold-storage etc.)
            return {}
        if not isinstance(arts, list) or not arts:
            return {}
        df = pd.DataFrame(arts)
        pmid_key = next((k for k in det.PMID_KEYS if k in df.columns), None)
        if pmid_key is None:
            return {}
        try:
            io = det._score(df, det.IO_MODEL, det.IO_SCALER, det.IO_CALIB,
                            det.IDENTITY_ONLY_BASE_FEATURES,
                            det.compute_derived_features_identity_only,
                            det.IDENTITY_ONLY_FEATURES) * 100.0
        except Exception:                          # noqa: BLE001
            return {}
        pmids = pd.to_numeric(df[pmid_key], errors="coerce")
        # feedback-less: production final == identity-only score (their pipeline)
        return {int(p): (float(s), float(s)) for p, s in zip(pmids, io) if pd.notna(p)}


# ---- public entry point ----------------------------------------------------
def match_authorship(author, pmid, idx, io_scorer, top_k=5):
    """Rank candidate CWIDs for one WCM authorship on an orphan article.

    author = {"last","fore","initials","affiliations"} (a universe author block).
    Returns the ranked candidate list; each candidate gains:
      io_score  (float 0-100 | None)   ReCiter identity-only score for this pmid
      io_source ("retrieved" | "not_retrieved")
    Ranking: identity-only score desc (nulls last) -> full given match -> confidence."""
    cands, cohort = idx.candidates(
        author.get("last"), author.get("fore"), author.get("initials"),
        author.get("affiliations"), top_k=top_k)
    for c in cands:
        v = io_scorer.scores(c["cwid"]).get(int(pmid)) if io_scorer else None
        c["io_score"] = round(v[0], 2) if v else None
        c["final_score"] = round(v[1], 2) if v else None   # production final (>=30 == suggested)
        c["io_source"] = "retrieved" if v else "not_retrieved"
    cands.sort(key=lambda d: (
        d["io_score"] if d["io_score"] is not None else -1.0,
        d["given_match"] == "full", d["confidence"]), reverse=True)
    return cands


# ---- CLI / self-test -------------------------------------------------------
def _print_candidates(cands, cohort_size):
    print(f"  cohort_size (surname+initial homonyms) = {cohort_size}")
    if not cands:
        print("  (no candidates)")
        return
    for i, c in enumerate(cands, 1):
        io = f"{c['io_score']:.2f}" if c["io_score"] is not None else "  -  "
        print(f"  {i}. cwid={c['cwid']:10} io={io:>6} ({c['io_source']:13}) "
              f"conf={c['confidence']:.3f} {c['given_match']:7} "
              f"affil={'Y' if c['affil_dept_match'] else '.'} "
              f"| {c['name']} — {c['person_type']}, {c['dept']}")


def _selftest():
    print("Loading identity index from reciterdb ...", flush=True)
    idx = IdentityIndex.load()
    n = sum(len(v) for v in idx.by_surname.values())
    print(f"  indexed {n} identities across {len(idx.by_surname)} surnames")
    io = IdentityOnlyScorer()

    # Trigger case: Stefan Worgall on the buried PMID 42220538.
    worgall = {"last": "Worgall", "fore": "Stefan", "initials": "S",
               "affiliations": ["Department of Pediatrics, Weill Cornell Medicine, New York, NY"]}
    print("\n=== Worgall / PMID 42220538 ===")
    cands = match_authorship(worgall, 42220538, idx, io)
    cohort = idx.candidates("Worgall", "Stefan", "S")[1]
    _print_candidates(cands, cohort)

    top = cands[0] if cands else None
    checks = [
        ("surname 'Worgall' resolves", bool(cands)),
        ("top candidate is stw2006", top and top["cwid"] == "stw2006"),
        ("Worgall is unambiguous (cohort==1)", cohort == 1),
        ("full given-name match", top and top["given_match"] == "full"),
        ("affiliation names dept (Pediatrics)", top and top["affil_dept_match"]),
        ("identity-only score recovered (>=90)",
         top and top["io_score"] is not None and top["io_score"] >= 90),
    ]
    # Homonym sanity: a common surname returns a cohort > 1.
    common, common_cohort = idx.candidates("Wang", "Jun", "J", top_k=5)
    checks.append(("common surname (Wang/J) is ambiguous (cohort>1)", common_cohort > 1))
    print("\n=== Homonym check: Wang, J (top 5 of cohort=%d) ===" % common_cohort)
    _print_candidates([dict(c, io_score=None, io_source="(skipped)") for c in common],
                      common_cohort)

    print("\n==== SELFTEST ====")
    ok = True
    for label, passed in checks:
        ok &= bool(passed)
        print(f"  [{'OK' if passed else '** FAIL'}] {label}")
    print("\nSELFTEST", "PASS" if ok else "FAIL")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--surname")
    ap.add_argument("--given")
    ap.add_argument("--initials")
    ap.add_argument("--affil", action="append", default=None)
    ap.add_argument("--pmid", type=int)
    ap.add_argument("--top-k", type=int, default=5)
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)
    if not args.surname:
        ap.error("use --selftest or --surname [...]")

    idx = IdentityIndex.load()
    io = IdentityOnlyScorer() if args.pmid else None
    author = {"last": args.surname, "fore": args.given,
              "initials": args.initials, "affiliations": args.affil}
    if args.pmid:
        cands = match_authorship(author, args.pmid, idx, io, top_k=args.top_k)
        cohort = idx.candidates(args.surname, args.given, args.initials, args.affil)[1]
    else:
        cands, cohort = idx.candidates(args.surname, args.given, args.initials,
                                       args.affil, top_k=args.top_k)
        for c in cands:
            c["io_score"], c["io_source"] = None, "(no --pmid)"
    print(f"=== {args.surname}, {args.given or args.initials or '?'} ===")
    _print_candidates(cands, cohort)


if __name__ == "__main__":
    main()
