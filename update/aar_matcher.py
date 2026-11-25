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
  python aar_matcher.py --selftest          # offline: ranking checks only
  python aar_matcher.py --selftest --live   # also reads reciterdb, DynamoDB and S3
  python aar_matcher.py --surname Worgall --given Stefan --pmid 42220538 \
      --affil "Department of Pediatrics, Weill Cornell Medicine, New York, NY"
"""
import argparse, json, os, re, sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adversarial_attribution_review as det   # Step-0 scoring engine (S3 + pinned models)
# IdentityIndex + name helpers + person-type table extracted to identity_index.py so the
# Scopus lane can reuse them without this module's S3/model deps. Re-exported here for
# any importer that still reaches for aar_matcher.IdentityIndex / _norm.
from identity_index import (IdentityIndex, CampusRoster, _norm, _first_initial,  # noqa: F401
                            name_tokens, PERSON_TYPES, CAMPUS_WCM, CAMPUS_ITHACA)

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
# ---- MSK-only bylines, initial-only picks -------------------------------------------
# Sloan Kettering is a home institution (aar_universe.DEFAULT_HOME_INSTITUTION_KEYWORDS) and
# should stay one: of the decided PubMed rows whose byline names it and nothing of ours,
# 5,913 were accepted or assigned (2026-11-25). But such a byline gives a first initial
# nothing to lean on, and for these person types an initial-only pick there was almost
# never right. This function replayed on the stored list of every decided row (2026-11-25):
# 661 rejected rows and 118 given to somebody else would have had no such pick, against 7
# rows that lose the right person from the list.
# ponytail: PubMed lane only (it alone has MSK-only bylines: 0 open on the other lanes).
_MSK_RE = re.compile(r"sloan.*kettering", re.I | re.S)
_OURS_RE = re.compile(r"weill|cornell|presbyterian", re.I)
MSK_WEAK_TYPES = frozenset((
    "Full-Time Faculty", "Part-Time Faculty", "Adjunct Faculty", "Voluntary Faculty",
    "Postdoc", "Non-Faculty"))
# The PhD and MD-PhD programs are shared with MSK, so their people are expected there.
_MSK_PROGRAM = ("Student PhD", "Student MD-PhD", "Alumni PhD", "Alumni MD-PhD")
MSK_IO_FLOOR = 20.0          # at or above it the weak picks were right about half the time
_SUGGESTED = 30.0            # aar_gate.STORAGE_THRESHOLD (checked in the self-test)


def msk_only(affiliations):
    """The byline names Sloan Kettering and nothing of WCM, Cornell or NewYork-Presbyterian."""
    text = " | ".join(a or "" for a in affiliations or [])
    return bool(_MSK_RE.search(text)) and not _OURS_RE.search(text)


def withhold_msk_initial(cands, affiliations, grad_faculty):
    """`cands` (ranked), less the initial-only guesses an MSK-only byline cannot support.

    Fires only when the PICK is initial-only, of a weak person type, not on the graduate
    school faculty list (`grad_faculty`, a set of lowercase cwids; None = list unavailable
    = the rule is off) and not backed by an ORCID or a score. Then the whole list goes, not
    just the pick: the next person in line was right 19 times in 661. Anyone matched by
    full name, by ORCID or on a score, the graduate school faculty, and PhD and MD-PhD
    students and alumni stay. So does the "Faculty" type: 83% right when it is the pick,
    and which of it and a weak type leads is often a tie between scores near zero (a replay
    of the open rows reversed it on dozens), so it must not be lost on that. The authorship is still queued, with no suggestion if nobody
    is left, as any unmatched byline is.

    Never on a pick production already suggests (>= 30): with no pick `_already_curated`
    could not see that, and the authorship would come back as an orphan it is not."""
    if not cands or grad_faculty is None or not msk_only(affiliations):
        return cands

    def backed(c):
        return (c["given_match"] != "initial" or bool(c.get("orcid_match"))
                or str(c["cwid"]).lower() in grad_faculty
                or (c.get("io_score") or 0) >= MSK_IO_FLOOR
                or (c.get("final_score") or 0) >= _SUGGESTED)
    top = cands[0]
    if backed(top) or top["person_type"] not in MSK_WEAK_TYPES:
        return cands
    return [c for c in cands if backed(c) or c["person_type"] == "Faculty"
            or c["person_type"].startswith(_MSK_PROGRAM)]


def match_authorship(author, pmid, idx, io_scorer, top_k=5, pub_year=None, withhold=True):
    """Rank candidate CWIDs for one WCM authorship on an orphan article.

    author = {"last","fore","initials","affiliations"} (a universe author block).
    withhold = False leaves out `withhold_msk_initial`, for aar_replay_row.py only.
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
    only among candidates otherwise tied — in practice the ones production never scored.

    CORNELL CANDIDATES (`idx` a `CampusRoster`, byline naming Ithaca) have no identity-only
    score by construction: ReCiter never retrieves or scores Cornell people, so there is
    no S3 input to read and the lookup is skipped outright (not attempted and missed).
    They rank exactly as a WCM candidate ReCiter never retrieved does -- io_score None,
    io_source "not_retrieved", -1.0 in the key. That is the rule, and why it cannot bury
    them: on a Cornell-only byline every candidate is Cornell, so io ties across the
    whole list and `candidates()`' own (tier, affiliation, confidence) order stands; on a
    two-campus byline the given-name tier still LEADS, so a Cornell `full` match beats any
    WCM `initial` one whatever its score. The one place a WCM rival wins is the same tier
    WITH a retrieved score -- the ordinary "retrieved beats never-retrieved" tie-break
    this key already applies between two WCM people, applied unchanged across campuses.
    ponytail: that includes a near-zero WCM score (the Torres shape, one tier down).
    Revisit if two-campus rows show curators overriding it."""
    # The byline ORCID rides on the author block: set by the producer only with
    # AAR_BYLINE_ORCID_PUBMED on, and by the replay tools from the row's stored
    # byline_orcid. Absent, the call is the one it always was.
    orcid = author.get("byline_orcid")
    cands, cohort = idx.candidates(
        author.get("last"), author.get("fore"), author.get("initials"),
        author.get("affiliations"), top_k=top_k, pub_year=pub_year,
        **({"byline_orcid": orcid} if orcid else {}))
    for c in cands:
        scored = io_scorer and c.get("campus", CAMPUS_WCM) == CAMPUS_WCM
        v = io_scorer.scores(c["cwid"]).get(int(pmid)) if scored else None
        c["io_score"] = round(v[0], 2) if v else None
        c["final_score"] = round(v[1], 2) if v else None   # production final (>=30 == suggested)
        c["io_source"] = "retrieved" if v else "not_retrieved"
    # `orcid_match` leads, as in identity_index._rank_key (owner decision 2026-11-17: the
    # ORCID outranks the name tier). Only a row whose ORCID found somebody carries it, so
    # every other list sorts exactly as before.
    cands.sort(key=lambda d: (
        bool(d.get("orcid_match")),
        d["given_match"] == "full",
        d["io_score"] if d["io_score"] is not None else -1.0,
        d["confidence"]), reverse=True)
    if withhold:
        cands = withhold_msk_initial(cands, author.get("affiliations"),
                                     getattr(getattr(idx, "wcm", idx), "grad_faculty", None))
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

    # --- Cornell candidates (CampusRoster): no io exists, none is fetched, none buries ---
    def crec(given, surname, cwid):
        return dict(rec(given, surname, None, cwid), campus=CAMPUS_ITHACA,
                    person_type="Cornell Faculty", historical=False)

    class _RecordingIO:
        """Scores every WCM cwid 0.62 and records which cwids were asked for."""
        def __init__(self):
            self.asked = []

        def scores(self, cwid):
            self.asked.append(cwid)
            return {99: (0.62, 0.0)}

    ppl = [dict(rec("Karl", "Moss", None, "km_w"), historical=False),
           crec("Kate", "Moss", "km_c"), crec("Kim", "Moss", "km_c2")]
    roster = CampusRoster(IdentityIndex(ppl), ithaca=IdentityIndex(ppl, campus=CAMPUS_ITHACA))
    ith = ["Department of Animal Science, Cornell University, Ithaca, NY"]
    wcm = ["Department of Medicine, Weill Cornell Medicine, New York, NY"]
    rio = _RecordingIO()
    cu_only = match_authorship({"last": "Moss", "fore": "Kate", "initials": "K",
                                "affiliations": ith}, 99, roster, rio)
    cu_asked = list(rio.asked)
    both_full = match_authorship({"last": "Moss", "fore": "Kate", "initials": "K",
                                  "affiliations": wcm + ith}, 99, roster, _RecordingIO())
    both_tie = match_authorship({"last": "Moss", "fore": None, "initials": "K",
                                 "affiliations": wcm + ith}, 99, roster, _RecordingIO())

    # The byline ORCID never reaches this lane. Rosters holding the labelled ORCID maps of
    # AAR_BYLINE_ORCID_WCM (the ORCID is on file for the Karl and Kim that Kate outranks)
    # give the list the plain rosters give, because match_authorship passes no ORCID.
    from identity_index import orcid_sources
    omaps, _ = orcid_sources([("km_w", "0000-0001-0000-0001", "wcm")],
                             [("km_c2", "0000-0001-0000-0002")])
    labelled = CampusRoster(
        IdentityIndex(ppl, orcid_cwid=omaps[CAMPUS_WCM]),
        ithaca=IdentityIndex(ppl, campus=CAMPUS_ITHACA, orcid_cwid=omaps[CAMPUS_ITHACA]))
    pubmed_same = all(
        json.dumps(match_authorship(a, 99, labelled, _RecordingIO()))
        == json.dumps(match_authorship(a, 99, roster, _RecordingIO()))
        for a in ({"last": "Moss", "fore": "Kate", "initials": "K", "affiliations": wcm},
                  {"last": "Moss", "fore": "Kate", "initials": "K", "affiliations": ith},
                  {"last": "Moss", "fore": None, "initials": "K", "affiliations": wcm + ith}))

    # With a byline ORCID on the author block (AAR_BYLINE_ORCID_PUBMED, or a replay of a
    # row that stores one) the ORCID's person leads: over a full name match on a
    # Cornell-only byline, and over a WCM rival carrying a retrieved identity-only score on
    # a two-campus one. Nobody is dropped.
    o_cu = match_authorship({"last": "Moss", "fore": "Kate", "initials": "K", "affiliations": ith,
                             "byline_orcid": "0000-0001-0000-0002"}, 99, labelled, _RecordingIO())
    o_both = match_authorship({"last": "Moss", "fore": None, "initials": "K",
                               "affiliations": wcm + ith, "byline_orcid": "0000-0001-0000-0002"},
                              99, labelled, _RecordingIO())

    checks = [
        ("a byline ORCID on the author block puts its person first, over a full given-name "
         "match, labelled with its source; the others stay, orcid_match false",
         [c["cwid"] for c in o_cu] == ["km_c2", "km_c"] and o_cu[0]["orcid_match"] is True
         and o_cu[0].get("orcid_source") == "affiliation_manager"
         and o_cu[1]["orcid_match"] is False),
        ("...and over a WCM rival with a retrieved identity-only score (the io tie-break that "
         "wins a same-tier two-campus row without the ORCID)",
         o_both[0]["cwid"] == "km_c2"
         and any(c["cwid"] == "km_w" and c["io_score"] == 0.62 for c in o_both)
         and both_tie[0]["cwid"] == "km_w"),
        ("an author block with no byline_orcid: rosters holding the WCM and Ithaca ORCID "
         "maps (AAR_BYLINE_ORCID_WCM) rank a WCM, a Cornell and a two-campus byline exactly "
         "as rosters without them, and no candidate carries an orcid key",
         pubmed_same and labelled.wcm.orcid_rec and labelled.ithaca.orcid_rec
         and "orcid" not in json.dumps(match_authorship(
             {"last": "Moss", "fore": None, "initials": "K", "affiliations": wcm + ith},
             99, labelled, _RecordingIO()))),
        ("Cornell-only byline: the Cornell people come back, top is the full match, "
         "io_score None / not_retrieved, and the scorer was NEVER asked for a Cornell cwid",
         [c["cwid"] for c in cu_only] == ["km_c", "km_c2"]
         and all(c["io_score"] is None and c["io_source"] == "not_retrieved" for c in cu_only)
         and cu_asked == []),
        ("two-campus byline: a Cornell FULL match outranks a WCM initial-tier rival that "
         "carries a retrieved score (the tier leads)",
         both_full[0]["cwid"] == "km_c" and both_full[0]["io_score"] is None
         and any(c["cwid"] == "km_w" and c["io_score"] == 0.62 for c in both_full)),
        ("two-campus byline, same tier: the retrieved WCM candidate wins the tie-break "
         "(documented rule), and every Cornell candidate is still offered",
         both_tie[0]["cwid"] == "km_w"
         and {c["cwid"] for c in both_tie} == {"km_w", "km_c", "km_c2"}),
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


def _msk_selftest():
    """Offline. The MSK-only rule: what it withholds, and everything it must leave alone."""
    from aar_gate import STORAGE_THRESHOLD
    MSK = ["Department of Medical Physics, Memorial Sloan-Kettering Cancer Center, New York"]
    BOTH = MSK + ["Weill Cornell Medicine, New York, NY"]
    G = {"grd1"}

    def c(cwid, ptype, given="initial", **kw):
        return dict({"cwid": cwid, "person_type": ptype, "given_match": given,
                     "io_score": None, "final_score": None}, **kw)
    ft = c("ft1", "Full-Time Faculty")

    def left(cands, aff=MSK, g=G):
        return [x["cwid"] for x in withhold_msk_initial(cands, aff, g)]
    inact, phd, grad = (c("in1", "Inactive Faculty"), c("ph1", "Student PhD (Tri-I)"),
                        c("GRD1", "Full-Time Faculty"))

    class _IO:
        def scores(self, cwid):
            return {}
    rec = {"cwid": "ft1", "given": "Ann", "middle": "", "surname": "Roe", "given_norm": "ann",
           "surname_norm": "roe", "dept": "", "division": "", "program": "", "title": "",
           "person_type": "Full-Time Faculty", "historical": False, "end_year": None}
    idx = IdentityIndex([rec])
    au = {"last": "Roe", "fore": "Abe", "initials": "A", "affiliations": MSK}

    def wired(roster, **kw):
        return [x["cwid"] for x in match_authorship(au, 99, roster, _IO(), **kw)]
    off = wired(idx)
    idx.grad_faculty = G
    checks = [
        ("each weak type is withheld", all(left([c("x", t)]) == [] for t in MSK_WEAK_TYPES)),
        ("the next person in line goes with the pick", left([ft, inact]) == []),
        ("grad faculty and PhD students further down stay",
         left([ft, inact, phd, grad]) == ["ph1", "GRD1"]),
        ("so does the Faculty type, and only that exact type",
         left([ft, c("fc", "Faculty"), c("em", "Emeritus Faculty")]) == ["fc"]),
        ("a full-name match further down stays", left([ft, c("f", "Postdoc", "full")]) == ["f"]),
        ("a full-name pick is left alone", left([c("f", "Postdoc", "full"), ft]) == ["f", "ft1"]),
        ("a grad faculty pick is left alone (cwid case ignored)", left([grad, ft]) == ["GRD1", "ft1"]),
        ("other person types are left alone", all(
            left([c("x", t), ft]) == ["x", "ft1"] for t in
            ("Faculty", "Inactive Faculty", "Resident (NYP)", "Student MD-PhD", "Emeritus Faculty"))),
        ("an ORCID pick is left alone", left([dict(ft, orcid_match=True)]) == ["ft1"]),
        ("a score at the floor is left alone, below it is not",
         left([dict(ft, io_score=MSK_IO_FLOOR)]) == ["ft1"]
         and left([dict(ft, io_score=MSK_IO_FLOOR - 0.01)]) == []),
        ("a pick production already suggests is left alone", left([dict(ft, final_score=30.0)]) == ["ft1"]),
        ("a byline that also names us is left alone", left([ft], BOTH) == ["ft1"]),
        ("New York-Presbyterian counts as us", left([ft], [MSK[0] + "; NewYork-Presbyterian"]) == ["ft1"]),
        ("a byline without MSK is left alone", left([ft], ["Rockefeller University"]) == ["ft1"]),
        ("both spellings and a split affiliation", msk_only(["Memorial Sloan Kettering"])
         and msk_only(["Memorial Sloan-Kettering"]) and not msk_only([]) and not msk_only(None)),
        ("no grad faculty list: the rule is off", left([ft], MSK, None) == ["ft1"]),
        ("no candidates", left([]) == []),
        ("match_authorship: off without the list, on with it, off for the replay",
         off == ["ft1"] and wired(idx) == [] and wired(idx, withhold=False) == ["ft1"]),
        ("match_authorship: a CampusRoster takes the list from its WCM roster",
         wired(CampusRoster(idx)) == []),
        ("_SUGGESTED is the gate's threshold", _SUGGESTED == STORAGE_THRESHOLD),
    ]
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    return all(ok for _n, ok in checks)


def _selftest(live=False):
    """Offline unless `live`. Every sibling's --selftest is offline, so this one going
    to reciterdb, DynamoDB and S3 by default surprised a run that expected no I/O."""
    print("=== Temporal-penalty ranking (offline) ===")
    rank_ok = _rank_selftest()
    print("\n=== MSK-only bylines (offline) ===")
    rank_ok = _msk_selftest() and rank_ok
    if not live:
        print("\nSELFTEST", "PASS" if rank_ok else "FAIL",
              "(offline only; add --live for the Worgall and homonym checks)")
        return rank_ok
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
    ap.add_argument("--live", action="store_true",
                    help="with --selftest: also run the checks that read reciterdb, "
                         "DynamoDB and S3")
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
        sys.exit(0 if _selftest(args.live) else 1)
    if not args.surname:
        ap.error("use --selftest or --surname [...]")

    idx = CampusRoster.load()          # --affil naming Ithaca reaches the Cornell roster
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
