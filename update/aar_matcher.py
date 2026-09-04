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

Ranking key per candidate: (full>initial given match, identity-only score desc,
confidence desc) — the same order `identity_index.candidates()` already uses, which this
module used to contradict. The orchestrator (next step) calls `match_authorship()` for
each WCM authorship on each orphan article and writes the ranked candidates to the ledger.

Temporal plausibility (issue #159) reaches that key ONLY through `confidence`. A
WCM-affiliated paper was written by someone who was at WCM when it was published, so a
candidate proposed for a paper that appeared years after their appointment ended loses
confidence points — a graduated, capped penalty (`identity_index.temporal_penalty`)
that breaks ties but deliberately does NOT outrank the identity-only score or the
given-name match, and never drops anyone. It needs the paper's year:
`match_authorship(..., pub_year=None)` is exactly the pre-#159 ranking, so a caller
that has no year loses nothing. The gap itself is published per row as
`authorship_review.top_years_after_wcm`, which is the lever for the stale rows whose
homonym cohort is one person and where re-ranking can do nothing.

Confidence is an explainable ordering aid, NOT a probability:
    base   = 0.50 full given-name match | 0.25 initial-only
    rarity = 0.40 / cohort_size          (1 person -> 0.40, 2 -> 0.20, ...)
    affil  = +0.25 if the affiliation text names the candidate's dept/division
    hist   = -0.10 if the person is alumni / inactive / emeritus
    stale  = -0.01 per year the paper postdates their WCM end year, after 5 grace
             years, capped at -0.15 (reached at a 20-year gap)
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
from identity_index import (IdentityIndex, _norm, _first_initial,  # noqa: F401
                            name_tokens, PERSON_TYPES)

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
def match_authorship(author, pmid, idx, io_scorer, top_k=5, pub_year=None):
    """Rank candidate CWIDs for one WCM authorship on an orphan article.

    author = {"last","fore","initials","affiliations"} (a universe author block).
    pub_year = the paper's publication year (None -> no temporal penalty, pre-#159
    ranking). Returns the ranked candidate list; each candidate gains:
      io_score  (float 0-100 | None)   ReCiter identity-only score for this pmid
      io_source ("retrieved" | "not_retrieved")
    Ranking: full given match -> identity-only score desc (nulls last) -> confidence.

    The given-name tier LEADS, as it already does in `identity_index.candidates()` three
    lines away. Leading with the io score instead meant any non-null score outranked a
    null one however small, so a byline naming one person outright lost to a rival scored
    0.62 out of 100 — the model saying "not this person" — purely because ReCiter had
    never retrieved the pmid for the right one. Live anchor: authorship_review 3043, pmid
    39629475, byline "Eileen Ruth Samson Torres", where eft4002 ("Emily Fujika Torres",
    io 0.62) took top_cwid from est4003 (io None). Measured by replaying all 19,050
    recoverable pubmed rows: 528 top picks move, 76 curator-resolved rows are fixed and
    1 breaks (row 70284, pmid 42270866, byline "April Chiu" — awc9002 and aechiu both
    reach `full`, so dropping io as the lead term leaves confidence to break a genuine
    homonym tie and it breaks it the other way). A real cost, taken for the 76.

    The temporal penalty is inside `confidence` (issue #159) and is deliberately NOT a
    term of its own: leading the key with it would let a one-year-past-grace gap
    outrank a 50-point identity-only score, and would sink the penalised candidate out
    of `candidates()`'s top_k altogether. `confidence` therefore keeps its LAST position
    here, unchanged by the reorder above. So on this lane the penalty moves the top pick
    only among candidates otherwise tied — in practice the ones production never scored."""
    cands, cohort = idx.candidates(
        author.get("last"), author.get("fore"), author.get("initials"),
        author.get("affiliations"), top_k=top_k, pub_year=pub_year)
    for c in cands:
        v = io_scorer.scores(c["cwid"]).get(int(pmid)) if io_scorer else None
        c["io_score"] = round(v[0], 2) if v else None
        c["final_score"] = round(v[1], 2) if v else None   # production final (>=30 == suggested)
        c["io_source"] = "retrieved" if v else "not_retrieved"
    cands.sort(key=lambda d: (
        d["given_match"] == "full",
        d["io_score"] if d["io_score"] is not None else -1.0,
        d["confidence"]), reverse=True)
    return cands


# ---- CLI / self-test -------------------------------------------------------
def _print_candidates(cands, cohort_size):
    print(f"  cohort_size (surname+initial homonyms) = {cohort_size}")
    if not cands:
        print("  (no candidates)")
        return
    for i, c in enumerate(cands, 1):
        io = f"{c['io_score']:.2f}" if c["io_score"] is not None else "  -  "
        gap = c.get("years_after_wcm")
        stale = f"stale={gap:+d}y" if gap is not None else ""
        print(f"  {i}. cwid={c['cwid']:10} io={io:>6} ({c['io_source']:13}) "
              f"conf={c['confidence']:.3f} {c['given_match']:7} "
              f"affil={'Y' if c['affil_dept_match'] else '.'} {stale} "
              f"| {c['name']} — {c['person_type']}, {c['dept']}")


def _rank_selftest():
    """Offline (no DB, no S3): the temporal penalty rides inside `confidence` and must
    stay BEHIND the identity-only score and the given-name match in this module's
    re-sort (issue #159). Two Lees who both match "K Lee", plus the live Weiss row that
    a penalty-leading sort key got wrong — and the Torres row that an io-leading one did.

    All three Lee/Weiss cases below tie on `given_match`, so promoting the tier to the
    front of the key leaves every one of them decided exactly as before; that is the
    point of the Torres and same-tier cases that follow them."""
    def rec(given, surname, end_year, cwid):
        return {"cwid": cwid, "given": given, "middle": "", "surname": surname,
                "given_norm": _norm(given), "surname_norm": _norm(surname),
                "dept": "", "division": "", "program": "", "title": "",
                "person_type": "Inactive Faculty", "historical": True,
                "end_year": end_year}

    class _FakeIO:
        """io_scorer stub: `scored` names the CWIDs production ever retrieved."""
        def __init__(self, scored=("departed",)):
            self.scored = scored

        def scores(self, cwid):
            return {99: (95.0, 0.0)} if cwid in self.scored else {}

    idx = IdentityIndex([rec("Kevin", "Lee", 1999, "departed"),
                         rec("Karen", "Lee", 2027, "here")])
    author = {"last": "Lee", "fore": None, "initials": "K", "affiliations": []}
    before = match_authorship(author, 99, idx, _FakeIO())                   # no pub_year
    after = match_authorship(author, 99, idx, _FakeIO(), pub_year=2024)     # 25y stale
    # nobody scored -> io ties at -1.0 and given_match ties, so confidence (carrying the
    # penalty) is what decides. This is the `absent` classification, the common case.
    tie = match_authorship(author, 99, idx, _FakeIO(scored=()), pub_year=2024)

    # Blocker 2 regression. The shape is taken from live row 60896 / PMID 42430466
    # ("Robert S Weiss"), where a penalty-leading key demoted weissro (io 50.61) below
    # rww2001 (io 1.47) -- but the gap on that live row is 35 years (weissro's
    # endDateWCMFaculty is 1991, the paper is 2026), NOT six. The six-year gap below is
    # SYNTHETIC, chosen because a lexicographic lead term ignores magnitude: the
    # smallest gap one year past the grace has to beat 49 points of identity evidence
    # for the point to hold. Replaying the rejected key over 12,007 open rows, 23 flips
    # did turn on a gap of exactly 6, but none of those demoted a candidate carrying a
    # real io_score -- the smallest gap that demoted a SCORED candidate is 8 (row 34105,
    # jjt2004, io 50.61). So do not read this as evidence that six-year gaps are
    # dangerous in the live data, and do not tighten TEMPORAL_GRACE_YEARS on its basis.
    weiss = IdentityIndex([rec("Robert", "Weiss", 2018, "weissro"),
                           rec("Ronald", "Weiss", 2030, "rww2001")])

    class _WeissIO:
        def scores(self, cwid):
            return {99: (50.61, 0.0) if cwid == "weissro" else (1.47, 0.0)}

    wr = match_authorship({"last": "Weiss", "fore": None, "initials": "R",
                           "affiliations": []}, 99, weiss, _WeissIO(), pub_year=2024)

    # --- given-name tier leads the key --------------------------------------
    # Live anchor: authorship_review 3043, pmid 39629475, byline "Eileen Ruth Samson
    # Torres". est4003 IS that byline spelled out (givenName "Eileen Ruth", middleName
    # "Samson") but ReCiter never retrieved the pmid for her, so io is None. eft4002
    # ("Emily Fujika Torres") carries io 0.62 -- 0.62 out of 100, the model saying "not
    # this person" -- and under the old io-leading key a 0.62 beat a null and took
    # top_cwid outright. Needs BOTH halves of this change: without the givenName+
    # middleName clause in identity_index, est4003 is not `full` and the tier cannot
    # rescue her; without the reorder, `full` never gets to speak.
    torres = IdentityIndex([
        dict(rec("Eileen Ruth", "Torres", 2024, "est4003"), middle="Samson"),
        rec("Emily", "Torres", 2030, "eft4002"),
    ])

    class _TorresIO:
        def scores(self, cwid):
            return {99: (0.62, 0.0)} if cwid == "eft4002" else {}

    tor = match_authorship({"last": "Torres", "fore": "Eileen Ruth Samson",
                            "initials": "ERS", "affiliations": []},
                           99, torres, _TorresIO())

    # ...but this is a TIER-FIRST key, not an io-blind one. Two candidates at the SAME
    # tier are still separated by the identity-only score, exactly as before -- which is
    # what keeps the Weiss regression above meaningful and is the whole reason io stays
    # in the key rather than being dropped for confidence.
    same_tier = IdentityIndex([rec("Robert", "Weiss", 2030, "scored"),
                               rec("Robert", "Weiss", 2030, "unscored")])

    class _OneScoredIO:
        def scores(self, cwid):
            return {99: (50.61, 0.0)} if cwid == "scored" else {}

    st = match_authorship({"last": "Weiss", "fore": "Robert", "initials": "R",
                           "affiliations": []}, 99, same_tier, _OneScoredIO())

    checks = [
        ("pre-#159 (no pub_year): io_score alone puts the departed Lee on top",
         before[0]["cwid"] == "departed" and before[0]["io_score"] == 95.0),
        ("INTENDED: a 25-year gap does NOT outrank a 95-point identity-only score",
         after[0]["cwid"] == "departed"),
        ("...the gap is still recorded for the curator-facing column",
         after[0]["years_after_wcm"] == 25),
        ("...and it costs confidence, capped so it never reaches 0.000",
         0 < after[0]["confidence"] < before[0]["confidence"]),
        ("with no identity-only score on either side the penalty breaks the tie",
         tie[0]["cwid"] == "here" and tie[-1]["cwid"] == "departed"),
        ("nothing is dropped: both Lees come back either way",
         len(after) == 2 and len(tie) == 2),
        ("a synthetic 6-year gap does not promote a 1.47 io_score over a 50.61 one "
         "(shape of live row 60896, whose real gap is 35y)", wr[0]["cwid"] == "weissro"),
        ("a FULL given match carrying NO identity-only score now outranks an "
         "initial-tier rival scored 0.62/100 (pmid 39629475, row 3043)",
         tor[0]["cwid"] == "est4003" and tor[0]["given_match"] == "full"
         and tor[0]["io_score"] is None),
        ("...and the rival is not dropped, just ranked below with its score intact",
         len(tor) == 2 and tor[1]["cwid"] == "eft4002"
         and tor[1]["given_match"] == "initial" and tor[1]["io_score"] == 0.62),
        ("tier-FIRST, not io-blind: within one tier the identity-only score still "
         "decides", [c["cwid"] for c in st] == ["scored", "unscored"]
         and st[0]["given_match"] == st[1]["given_match"] == "full"),
    ]
    ok = True
    for label, passed in checks:
        ok &= bool(passed)
        print(f"  [{'OK' if passed else '** FAIL'}] {label}")
    return ok


def _selftest():
    print("=== Temporal-penalty ranking (offline) ===")
    rank_ok = _rank_selftest()
    print("\nLoading identity index from reciterdb ...", flush=True)
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
    ok = rank_ok
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
    ap.add_argument("--pub-year", type=int, help="paper's publication year (enables the "
                                                 "temporal-plausibility penalty)")
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
        cands = match_authorship(author, args.pmid, idx, io, top_k=args.top_k,
                                 pub_year=args.pub_year)
        cohort = idx.candidates(args.surname, args.given, args.initials, args.affil)[1]
    else:
        cands, cohort = idx.candidates(args.surname, args.given, args.initials,
                                       args.affil, top_k=args.top_k,
                                       pub_year=args.pub_year)
        for c in cands:
            c["io_score"], c["io_source"] = None, "(no --pmid)"
    print(f"=== {args.surname}, {args.given or args.initials or '?'} ===")
    _print_candidates(cands, cohort)


if __name__ == "__main__":
    main()
