#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 4: per-run orchestrator + stateful ledger.

Ties Steps 1-3 into one idempotent, en-masse run and maintains the two persistent
stores (orphan ledger + processed-PMID log) the plan calls for. Designed to clear
the initial backlog (`--mode initial`, ~2y window) in one batch and then run
nightly (from run_all.py, AAR_PUBMED_LANE_CADENCE=daily) (`--mode recurring`) over a
rolling slice, with
overlapping windows harmless because the processed log prevents re-gating.

Per run (docs/ADVERSARIAL_ATTRIBUTION_REVIEW_PLAN.md §Per-run algorithm):
  1. Pull universe          — aar_universe.pull_universe (PubMed WCM-affiliation, EDAT window)
  2. Gate NEW pmids         — aar_gate.attributed_pmids (one batched reciterdb query, global)
                              orphan = WCM-author article with no WCM person attributed >=30
  3. Explode + match        — each orphan article -> one ledger row per WCM-affiliated
                              authorship; aar_matcher proposes ranked candidate CWIDs
  4. Identity-only ranking  — PARALLEL pre-warm of the IO scorer over the DISTINCT candidate
                              CWID set (each person scored once), then attach scores
  5. Re-check open rows     — ALL open/snoozed ledger rows (any window): article now
                              attributed in reciterdb? candidate accepted/rejected in gold
                              standard? -> close; expire snoozes; bump last_checked
  6. Persist + export       — canonical ledger.csv / processed_log.csv (git audit trail) +
                              a dated curator export (open orphans, NEW/CARRYOVER stamped)

NOT in scope here (next step = the SharePoint reconciliation bridge): reading a curator
decision column and writing it back to the gold standard + `curatedBy`. Once decisions
land in the gold standard (by any path), step 5 resolves the rows automatically.

State (v1 = versioned files; git is the audit trail):
  analysis/adversarial_attribution_review/state/ledger.csv         (canonical, in place)
  analysis/adversarial_attribution_review/state/processed_log.csv  (canonical, in place)
  analysis/adversarial_attribution_review/exports/<run_date>/      (curator-facing views)

Env: PUBMED_API_KEY (universe), DB_* (reciterdb gate+matcher), S3 + pinned models (ranking).

Usage:
  python aar_orchestrator.py --from 2026/05/26 --to 2026/06/02 --state-dir /tmp/aar_test   # test
  python aar_orchestrator.py --mode initial        # one-time backlog clear (long)
  python aar_orchestrator.py --mode recurring      # nightly rolling slice (from run_all.py)
  python aar_orchestrator.py --mode backfill --s3-state --before 2026-08-29
                                                   # re-explode the pre-#160 backlog
"""
import argparse, json, os, sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3
import botocore
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import aar_universe as uni
import aar_gate as gate
import aar_matcher as matcher
import aar_db

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_STATE = os.path.join(REPO, "analysis", "adversarial_attribution_review", "state")
DEFAULT_EXPORT = os.path.join(REPO, "analysis", "adversarial_attribution_review", "exports")
_dyn = boto3.client("dynamodb", region_name="us-east-1")

# ---- S3-backed state (in-cluster CronJob has no persistent FS / git) -------
# ponytail: S3 pull-modify-push, safe because the reciterdb CronJob is
# concurrencyPolicy=Forbid (single writer). Upgrade to DB-only state if the
# ledger ever needs concurrent producers.
_s3 = boto3.client("s3", region_name="us-east-1")
S3_STATE_BUCKET = os.environ.get("AAR_S3_BUCKET") or os.environ.get("S3_BUCKET")
S3_STATE_PREFIX = os.environ.get("AAR_S3_PREFIX", "aar-state")
_STATE_FILES = ("ledger.csv", "processed_log.csv")

def _s3_pull_state(local_dir):
    """Download ledger/processed_log from S3 into local_dir (missing key = first run)."""
    os.makedirs(local_dir, exist_ok=True)
    for f in _STATE_FILES:
        try:
            _s3.download_file(S3_STATE_BUCKET, f"{S3_STATE_PREFIX}/{f}",
                              os.path.join(local_dir, f))
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                continue
            raise

def _s3_push_state(local_dir, run_date):
    """Upload state back to S3 (canonical) + a dated archive copy (replaces the git audit trail)."""
    for f in _STATE_FILES:
        p = os.path.join(local_dir, f)
        if os.path.exists(p):
            _s3.upload_file(p, S3_STATE_BUCKET, f"{S3_STATE_PREFIX}/{f}")
            _s3.upload_file(p, S3_STATE_BUCKET, f"{S3_STATE_PREFIX}/archive/{run_date}/{f}")

LEDGER_COLS = [
    "pmid", "author_key", "author_position", "author_position_label", "wcm_author",
    "author_affiliation", "entrez_date", "title", "journal", "doi",
    "match_status", "n_candidates",
    "top_cwid", "top_name", "top_person_type", "top_dept", "top_given_match",
    "top_affil_match", "top_cohort_size", "top_confidence",
    "top_io_score", "top_final_score", "top_io_source",
    "candidate_cwids_json",
    "status", "first_seen", "last_checked", "snooze_until", "reviewer", "note",
    "resolved_date", "resolution_cwid",
]
PROCESSED_COLS = ["pmid", "entrez_date", "first_seen", "last_status", "last_checked",
                  "n_wcm_authorships"]
OPEN = {"open", "snoozed"}


# ===========================================================================
# State store
# ===========================================================================
class LedgerStore:
    def __init__(self, state_dir):
        self.dir = state_dir
        self.ledger_path = os.path.join(state_dir, "ledger.csv")
        self.processed_path = os.path.join(state_dir, "processed_log.csv")
        self.ledger = self._load(self.ledger_path, LEDGER_COLS, {"pmid": "Int64"})
        self.processed = self._load(self.processed_path, PROCESSED_COLS, {"pmid": "Int64"})

    @staticmethod
    def _load(path, cols, dtypes):
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=dtypes)
            for c in cols:
                if c not in df.columns:
                    df[c] = pd.NA
            return df[cols]
        return pd.DataFrame({c: pd.Series(dtype=dtypes.get(c, "object")) for c in cols})

    def processed_pmids(self):
        return set(int(p) for p in self.processed["pmid"].dropna())

    def save(self):
        os.makedirs(self.dir, exist_ok=True)
        self.ledger[LEDGER_COLS].to_csv(self.ledger_path, index=False)
        self.processed[PROCESSED_COLS].to_csv(self.processed_path, index=False)


# ===========================================================================
# Helpers
# ===========================================================================
def _position_label(i, n):
    return "first" if i == 0 else "last" if i == n - 1 else "middle"


def _compact(cands):
    """Trim candidate dicts for JSON storage in the ledger."""
    keep = ("cwid", "name", "person_type", "dept", "given_match", "affil_dept_match",
            "cohort_size", "confidence", "years_after_wcm", "io_score", "final_score",
            "io_source")
    return [{k: c.get(k) for k in keep} for c in cands]


def _trunc(s, n):
    return s[:n] if isinstance(s, str) and len(s) > n else s


def _byline(au):
    """The author's name as PubMed printed it -- what lands in `wcm_author`."""
    return " ".join(x for x in (au.get("fore"), au.get("last")) if x) \
        or au.get("initials") or au.get("last")


def _byline_owner(byline, attributed):
    """The already-attributed cwid that this BYLINE names, or None. `attributed` is one
    pmid's entry from gate.attributions().

    Match is deliberately strict: the person's surname must be the tail of the byline AND
    the byline's first token must equal the first token of their given name, no initials on
    either side. First TOKEN, not the whole given name -- an owner recorded as "Kristin Lees"
    matches a byline "Kristin Haggerty". That is consistent with the census key, which is
    built on the same `f[0]`, so a spelling either can reach is a spelling both can.
    'Tony Rosen' vs Tony Rosen = suppress; 'J Kim' vs Junbum Kim = do not. The loose
    first-initial tier was measured on the live queue and rejected -- 19 extra rows for a
    predicate that matches half a surname cohort.

    Homonyms are handled upstream, not here: gate.attributions() has already dropped any
    spelling two or more WCM people answer to, so a name this function can match belongs
    to exactly one person on the roster. Measured, that withholds 90 of the 479 byline
    matches; signal 1 covers 10 of them anyway, so 80 rows stay in the queue that the
    unguarded predicate would have closed -- 16.5% of the 485. It is the deliberate trade,
    and the asymmetry decides it: an authorship suppressed here is never WRITTEN, so no
    curator can notice the mistake, while one left open is only noise. The measured case
    is id 7896, byline 'David J Chung': lrz9005 (David H Chung, Psychiatry) holds that
    pmid at 100/ACCEPTED, and WCM also employs djc2004 (David J Chung, Medicine) and
    dic2022 (David I Chung, Pediatrics). Suppressing on 'David Chung' would have buried
    the likelier reading of that byline.

    One class this reaches that signal 1 never can: `person` carries 6,991 external-validation
    cohort ids (ucsf_/ucsd_/uci_/ucdavis_/fh_) that `identity` does not, so no open row has one
    as its top_cwid and the candidate test can never fire on them -- but a byline can. Five live
    rows suppress on such an owner (42874, 46056, 46487, 72789, 72813); all five were inspected
    and are AAR false positives with UC Davis / MSKCC / UCSF / Rockefeller affiliations and no
    WCM roster row of that name. The census is fed from `person`, so the dangerous variant --
    a WCM person sharing a name with an external cohort id -- is withheld like any other homonym.

    ponytail: position would break some of those ties -- the anchor row and its holder are
    both 'last'. It is not available as a general guard and this does not try:
    analysis_summary_author.authorPosition is NULL on 321,533 of 554,581 rows (58%) and
    only ever takes 'first'/'last', while 9,770 of the 12,293 open PubMed rows are
    'middle'. Upgrade path if that column is ever filled in: spare an ambiguous byline
    only when both sides carry a position and they disagree."""
    b = matcher.name_tokens(byline)
    if not b:
        return None
    for cwid, _pos, names in attributed or ():
        for first, last in names:
            f, l = matcher.name_tokens(first), matcher.name_tokens(last)
            if not f or not l or len(b) <= len(l) or b[-len(l):] != l:
                continue                               # surname is not the byline's tail
            if b[0] == f[0] and len(b[0]) > 1:         # equal on both sides, not initials
                return cwid
    return None


def _already_curated(top, attributed=(), byline=None):
    """This authorship is already resolved, so it is not a curation record. Three sites
    decide this -- the DB sink, the CSV sink, and the run log -- and they must never
    disagree, so they all call here.

    Three signals, and the order matters:

    1. `attributed` -- reciterdb's OWN per-authorship attribution for this pmid, from
       gate.attributions() over analysis_summary_author. Authoritative: if reciterdb says
       this cwid holds this article, it is curated, full stop.
    2. THE BYLINE'S OWNER (issue #174). Signal 1 asks whether the candidate this run
       PROPOSED is attributed, which is the wrong question whenever the matcher proposed
       the wrong person -- and it cannot be right, because a row only exists when nobody
       obvious was found. The anchor: byline 'Tony Rosen', proposed ltr4001 'Leah Teresa
       Rosen' off a middle-initial fallback, while aer2006 (Tony Rosen, Emergency
       Medicine) had held that pmid at 100/ACCEPTED for 44 days before the row was even
       created. Asking "is this byline attributed, to ANYONE?" fires on 389 of the 12,293
       open PubMed rows, 212 of which no other signal reaches; every one of the 389 names
       a holder whose person_article row is userAssertion='ACCEPTED'. The other 177 are
       cwids signal 1 should already have caught -- they are open only because _recheck
       resolves the CSV ledger and never reaches reciterdb.authorship_review.
    3. `final_score >= STORAGE_THRESHOLD` -- aar_matcher's score for the pair. This is a
       LOCAL rescore (det._score runs pinned XGBoost + isotonic over production's S3
       feature vectors), not a value read back from production, so it can be None on any
       S3 or scoring failure: NoSuchKey, cold-storage InvalidObjectState, an empty or
       malformed artifact, a scoring exception. Signals 1 and 2 exist precisely because of
       that: without them a transient S3 failure would silently reclassify an attributed
       authorship as `absent` and push a spurious row into the curator queue -- a hole
       that could not exist before #160, when attributed articles were never exploded.
       The anchor row's top_fg_score is NULL, so it took this fail-open path."""
    if top is None:
        return False
    if attributed and any(cwid == top.get("cwid") for cwid, _pos, _names in attributed):
        return True
    if _byline_owner(byline, attributed):
        return True
    fg = top.get("final_score")
    return fg is not None and fg >= gate.STORAGE_THRESHOLD


def _db_rows(resolved_auth, run_date, attr_by_pmid=None):
    """authorship_review rows for matched authorships, classified PER-AUTHORSHIP:
    absent (top candidate never scored) / buried (FG<30). An authorship _already_curated
    is skipped entirely rather than hidden behind a PM filter -- see that function for
    the three signals and why reciterdb's own attribution leads. (Was
    previously written and left for PM to display; per product decision 2026-08-19
    that is wrong -- don't create a queue record for something already resolved.)
    Unmatched authorships (no candidate to assign) are also skipped. single_candidate
    uses the true cohort size (unique surname+initial), the strongest precision signal.

    dup_flag/dup_reason: one batched aar_db.dup_flags_by_doi() call over every DOI in
    this run's resolved_auth (not a query per row), narrowed per row to THIS
    authorship's own candidates by aar_db.dup_uid_for_authorship — a co-author's
    already-added authorship must not flag this one (issue #158). See those
    functions' docstrings."""
    attr_by_pmid = attr_by_pmid or {}
    dois = {a.get("doi") for a, i, n, au, cands, top in resolved_auth if a.get("doi")}
    dup_map = aar_db.dup_flags_by_doi(dois) if dois else {}
    out = []
    for a, i, n, au, cands, top in resolved_auth:
        if top is None:
            continue                                   # unmatched: nothing to assign
        fg = top.get("final_score")
        byline = _byline(au)
        if _already_curated(top, attr_by_pmid.get(a["pmid"], ()), byline):
            continue                                   # already curated -- not a queue record
        cls = "absent" if fg is None else "buried"
        cohort = top.get("cohort_size")
        doi = a.get("doi")
        dup_uid = aar_db.dup_uid_for_authorship(dup_map, doi, top, cands)
        out.append({
            "source": "pubmed",
            "pmid": a["pmid"],
            "author_key": f"{a['pmid']}:{i}",
            "author_position": i + 1,
            "author_position_label": _position_label(i, n),
            "wcm_author": _trunc(byline, 255),
            "author_affiliation": " | ".join(au.get("affiliations") or []),
            "entrez_date": a["entrez_date"], "title": a["title"],
            "journal": _trunc(a["journal"], 512), "doi": _trunc(a["doi"], 255),
            "classification": cls,
            "top_cwid": top["cwid"], "top_name": _trunc(top["name"], 255),
            "top_person_type": _trunc(top["person_type"], 64),
            "top_dept": _trunc(top["dept"], 255),
            "top_fg_score": fg, "top_io_score": top.get("io_score"),
            "top_confidence": top["confidence"],
            "top_years_after_wcm": top.get("years_after_wcm"),
            "top_cohort_size": cohort,
            "top_given_match": top["given_match"],
            "top_affil_match": int(bool(top["affil_dept_match"])),
            "n_candidates": len(cands), "single_candidate": int(cohort == 1),
            "candidate_cwids_json": json.dumps(_compact(cands)),
            "dup_flag": int(bool(dup_uid)),
            "dup_reason": (f"Already added as ExternalArticle for {dup_uid} (DOI match)"
                           if dup_uid else None),
            "status": "open", "first_seen": run_date,
            "last_checked": run_date, "last_refreshed": run_date,
        })
    return out


def _batch_gold_standard(cwids):
    """cwid -> (knownpmids set, rejectedpmids set) via DynamoDB BatchGetItem (100/call)."""
    cwids = sorted({c for c in cwids if c})
    out = {}

    def absorb(items):
        for it in items:
            uid = it["uid"]["S"]
            known = {int(x["N"]) for x in it.get("knownpmids", {}).get("L", []) if "N" in x}
            rej = {int(x["N"]) for x in it.get("rejectedpmids", {}).get("L", []) if "N" in x}
            out[uid] = (known, rej)

    for i in range(0, len(cwids), 100):
        chunk = cwids[i:i + 100]
        req = {"GoldStandard": {"Keys": [{"uid": {"S": c}} for c in chunk],
                                "ProjectionExpression": "uid, knownpmids, rejectedpmids"}}
        resp = _dyn.batch_get_item(RequestItems=req)
        absorb(resp["Responses"].get("GoldStandard", []))
        unproc = resp.get("UnprocessedKeys") or {}
        while unproc:
            resp = _dyn.batch_get_item(RequestItems=unproc)
            absorb(resp["Responses"].get("GoldStandard", []))
            unproc = resp.get("UnprocessedKeys") or {}
    for c in cwids:
        out.setdefault(c, (set(), set()))
    return out


# ===========================================================================
# Orchestrator
# ===========================================================================
def _new_ctx(state_dir):
    """Shared context across tiled slices: one ledger store, one identity index, one
    identity-only score cache (so a CWID is downloaded+scored once for the whole run)."""
    return {"store": LedgerStore(state_dir),
            "idx": matcher.IdentityIndex.load(),
            "io": matcher.IdentityOnlyScorer()}


def run(date_from, date_to, state_dir, export_dir, run_date, workers=16, max_records=None,
        recheck=True, ctx=None, write_db=True):
    if ctx is None:
        ctx = _new_ctx(state_dir)
    store, idx, io = ctx["store"], ctx["idx"], ctx["io"]
    already = store.processed_pmids()
    log = lambda m: print(m, flush=True)  # noqa: E731

    # --- 1. universe ---------------------------------------------------------
    log(f"[1/6] Universe pull: entrez {date_from} .. {date_to}")
    u = uni.pull_universe(date_from, date_to, max_records=max_records)
    arts = [a for a in u["articles"] if a["wcm_author_count"] > 0]
    by_pmid = {a["pmid"]: a for a in arts}
    new_pmids = [p for p in by_pmid if p not in already]
    log(f"      {u['in_window']} in window, {len(arts)} with a WCM author, "
        f"{len(new_pmids)} new (not yet processed)")

    # --- 2. gate (batched, global) -- LABEL ONLY, no longer an explosion filter
    # Article-level "attributed" (>=1 WCM co-author already >=30) used to skip step 3
    # entirely for the whole article. That's wrong: an article can have SOME WCM
    # co-authors correctly attributed and OTHERS never scored at all (the
    # prs4005/PMID 41000987 case -- 5 co-authors attributed, prs4005 himself never
    # even retrieved). Every new WCM-authored pmid now gets exploded; the
    # per-authorship classification in step 4 (not this query) decides what a
    # curator sees. Kept here for the processed_log label and for _recheck's
    # attribution lookups (attr_who).
    log("[2/6] Gating new pmids against reciterdb (analysis_summary_author) ...")
    attributed = gate.attributed_pmids(new_pmids) if new_pmids else set()
    # Per-authorship attribution for the same set. gate.attributed_pmids answers "is this
    # ARTICLE attributed"; this answers "to WHOM, and under what name", which is what the
    # per-authorship gate below actually needs and what makes the skip authoritative
    # rather than dependent on a local rescore that can fail open. One batched query.
    # The name half is not decoration: narrowing this to a bare cwid set (what it did
    # before #174) is what let a byline whose real owner was attributed pass the gate,
    # because the only cwid tested was the one this run happened to propose.
    attr_by_pmid = gate.attributions(sorted(attributed)) if attributed else {}
    orphan_pmids = [p for p in new_pmids if p not in attributed]  # metric only now
    log(f"      {len(attributed)} of {len(new_pmids)} new pmids have >=1 WCM co-author "
        f"already attributed (article-level label; ALL still get exploded below)")

    # --- 3. explode ALL new WCM-authored articles into authorships + match ---
    log("[3/6] Matching WCM authorships on new articles ...")
    log(f"      identity index: {sum(len(v) for v in idx.by_surname.values())} people")
    authorships = []  # (article, position, author, candidates)
    cwid_pool = set()
    for p in new_pmids:
        a = by_pmid[p]
        n = len(a["authors"])
        for i, au in enumerate(a["authors"]):
            if not au.get("home_inst"):
                continue
            cands, _ = idx.candidates(au.get("last"), au.get("fore"),
                                      au.get("initials"), au.get("affiliations"), top_k=5,
                                      pub_year=a.get("pub_year"))
            cwid_pool.update(c["cwid"] for c in cands)
            authorships.append((a, i, n, au, cands))
    log(f"      {len(authorships)} WCM authorships; {len(cwid_pool)} distinct candidate CWIDs")

    # --- 4. identity-only ranking: PARALLEL pre-warm over distinct CWIDs ------
    log(f"[4/6] Pre-warming identity-only scores ({workers} workers) ...")
    warmed = [0]
    pool = sorted(c for c in cwid_pool if c not in io._cache)  # skip already-warm (tiling)

    def _warm(c):
        io.scores(c)
        warmed[0] += 1
        if warmed[0] % 500 == 0:
            log(f"      scored {warmed[0]}/{len(pool)} candidate CWIDs")

    if pool:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(_warm, pool))
    log(f"      identity-only cache warm: {len(pool)} CWIDs")

    # production-final gate (cache hits now; no further S3). Resolve the top candidate per
    # authorship. suggested_pmids is the ARTICLE-level roll-up -- >=1 authorship on the
    # article scored >=30 -- and since #160 it is informational only: exclusion is decided
    # per authorship below, not per article (the prs4005 / PMID 41000987 case).
    resolved_auth, suggested_pmids = [], set()
    for a, i, n, au, _ in authorships:
        cands = matcher.match_authorship(au, a["pmid"], idx, io, top_k=5,
                                         pub_year=a.get("pub_year"))
        top = cands[0] if cands else None
        if top and top.get("final_score") is not None \
                and top["final_score"] >= gate.STORAGE_THRESHOLD:
            suggested_pmids.add(a["pmid"])
        resolved_auth.append((a, i, n, au, cands, top))
    n_row_suggested = sum(
        1 for a, i, n, au, cands, top in resolved_auth
        if _already_curated(top, attr_by_pmid.get(a["pmid"], ()), _byline(au)))
    log(f"      {len(suggested_pmids)} articles have >=1 authorship already SUGGESTED "
        f"(article-level, informational); {n_row_suggested}/{len(resolved_auth)} individual "
        f"authorships are SUGGESTED and excluded from the curator queue below, "
        f"{len(resolved_auth) - n_row_suggested} kept (buried/absent, per-authorship)")

    # DB sink (Publication Manager source): upsert the matched authorships that are still
    # curation records -- absent/buried. Since #160 an authorship already at FG>=30 is
    # already curated, so _db_rows skips it rather than writing it for PM to filter.
    # Curator status on existing rows is preserved.
    if write_db:
        db_rows = _db_rows(resolved_auth, run_date, attr_by_pmid)
        aar_db.upsert(db_rows)
        log(f"      upserted {len(db_rows)} matched authorships -> reciterdb.authorship_review")

    new_rows = []
    for a, i, n, au, cands, top in resolved_auth:
        if _already_curated(top, attr_by_pmid.get(a["pmid"], ()), _byline(au)):
            continue                                  # THIS authorship is already suggested/covered
        new_rows.append({
            "pmid": a["pmid"],
            "author_key": f"{a['pmid']}:{i}",
            "author_position": i + 1,
            "author_position_label": _position_label(i, n),
            "wcm_author": _byline(au),
            "author_affiliation": " | ".join(au.get("affiliations") or []),
            "entrez_date": a["entrez_date"], "title": a["title"],
            "journal": a["journal"], "doi": a["doi"],
            "match_status": "matched" if top else "no_identity_match",
            "n_candidates": len(cands),
            "top_cwid": top["cwid"] if top else None,
            "top_name": top["name"] if top else None,
            "top_person_type": top["person_type"] if top else None,
            "top_dept": top["dept"] if top else None,
            "top_given_match": top["given_match"] if top else None,
            "top_affil_match": top["affil_dept_match"] if top else None,
            "top_cohort_size": top["cohort_size"] if top else None,
            "top_confidence": top["confidence"] if top else None,
            "top_io_score": top["io_score"] if top else None,
            "top_final_score": top["final_score"] if top else None,
            "top_io_source": top["io_source"] if top else None,
            "candidate_cwids_json": json.dumps(_compact(cands)),
            "status": "open", "first_seen": run_date, "last_checked": run_date,
            "snooze_until": None, "reviewer": None, "note": None,
            "resolved_date": None, "resolution_cwid": None,
        })

    # processed log for ALL new pmids (attributed / suggested / orphan[buried|absent])
    wcm_ct = {p: sum(1 for au in by_pmid[p]["authors"] if au.get("home_inst"))
              for p in new_pmids}
    proc_new = [{
        "pmid": p, "entrez_date": by_pmid[p]["entrez_date"], "first_seen": run_date,
        "last_status": ("attributed" if p in attributed
                        else "suggested" if p in suggested_pmids else "orphan"),
        "last_checked": run_date, "n_wcm_authorships": wcm_ct[p],
    } for p in new_pmids]

    if new_rows:
        store.ledger = pd.concat([store.ledger, pd.DataFrame(new_rows)], ignore_index=True)
    if proc_new:
        store.processed = pd.concat([store.processed, pd.DataFrame(proc_new)],
                                    ignore_index=True)

    # --- 5. re-check ALL open/snoozed rows (any window) ----------------------
    if recheck:
        log("[5/6] Re-checking open ledger rows for resolution ...")
        resolved = _recheck(store, run_date)
        log(f"      resolved this run: {resolved}")
    else:
        resolved = {"deferred": True}
        log("[5/6] Re-check deferred to final pass (tiled run)")

    # --- 6. persist + export -------------------------------------------------
    store.save()
    # orphan_pmids (no attribution at all) and suggested_pmids (>=1 authorship
    # rescored >=30) are no longer nested subsets now that step 3 explodes every
    # new pmid, not just orphan ones -- an already-`attributed` pmid can also land
    # in suggested_pmids (its co-author reproduces the same production score on
    # rescore). Compute the overlap-safe count explicitly rather than subtracting.
    n_orphan_buried = len(set(orphan_pmids) - suggested_pmids)
    summary = _export(store, export_dir, run_date, date_from, date_to, u,
                      len(new_pmids), len(attributed), len(orphan_pmids),
                      len(suggested_pmids), n_orphan_buried, len(authorships),
                      len(pool), len(new_rows), resolved)
    log(f"[6/6] State -> {store.ledger_path}")
    log(f"      Export -> {os.path.join(export_dir, run_date)}")
    log("\n==== RUN SUMMARY ====")
    log(json.dumps(summary, indent=2))
    return summary


def _recheck(store, run_date):
    led = store.ledger
    mask = led["status"].isin(OPEN)
    if not mask.any():
        return {"attributed": 0, "accepted": 0, "rejected": 0, "snooze_expired": 0}

    # expire snoozes
    snz = mask & (led["status"] == "snoozed") & led["snooze_until"].notna() \
        & (led["snooze_until"].astype(str) <= run_date)
    n_expired = int(snz.sum())
    led.loc[snz, "status"] = "open"

    open_idx = led.index[led["status"].isin(OPEN)]
    pmids = sorted({int(p) for p in led.loc[open_idx, "pmid"].dropna()})
    attr = gate.attributed_pmids(pmids) if pmids else set()
    attr_who = gate.attributions(sorted(attr)) if attr else {}

    # candidate cwids across open rows -> gold standard
    cwids = set()
    for j in open_idx:
        cwids.update(_row_cwids(led.loc[j]))
    gs = _batch_gold_standard(cwids)

    counts = {"attributed": 0, "accepted": 0, "rejected": 0, "snooze_expired": n_expired}
    # The old code resolved EVERY open ledger row for a pmid the instant ANY co-author
    # on it became attributed -- the same article-vs-authorship conflation as the gate
    # and dup_flag (issue #158) bugs, just in the recheck path. Now a row is resolved
    # only when that row's own candidate cwid is among the pmid's attributed cwids.
    #
    # Exception, and it is the majority of the ledger: a row with NO candidate cwids at
    # all (match_status='no_identity_match' -- 1,336 of 1,923 open rows in prod as of
    # 2026-08-28) has nothing to narrow TO. Requiring "this row's own cwid is attributed"
    # of a row that names no cwid would make resolved_attributed unreachable for it
    # forever. The conflation bug being fixed here is specifically "the row named
    # candidates and none of them is the attributed one", which presupposes candidates,
    # so candidate-less rows keep the old article-level signal.
    #
    # That narrowing carried the SAME blind spot as the emission gate (issue #174): it
    # only ever tested cwids this row proposed, so a row whose byline belongs to somebody
    # the matcher never considered stayed open forever. _byline_owner closes it, and is
    # tried before the candidate-less fallback because it names the person the byline
    # actually is rather than an arbitrary attributed co-author. Of a dated 80-row sample
    # of anchor-shaped rows, 23 became attributed AFTER the row was created, so they are
    # unreachable from the emission gate and only this path can resolve them.
    for j in open_idx:
        row = led.loc[j]
        pmid = int(row["pmid"])
        cc = _row_cwids(row)
        accept_cwid = next((c for c in cc if pmid in gs.get(c, (set(), set()))[0]), None)
        reject_cwid = next((c for c in cc if pmid in gs.get(c, (set(), set()))[1]), None)
        attr_hits = attr_who.get(pmid, []) if pmid in attr else []
        attr_cwid = next((c for c in cc if c in {w[0] for w in attr_hits}), None) \
            or _byline_owner(row.get("wcm_author"), attr_hits)
        if not cc and attr_hits:                       # candidate-less row: article-level only
            attr_cwid = attr_cwid or attr_hits[0][0]
        if accept_cwid:
            _resolve(led, j, "resolved_accepted", accept_cwid, run_date)
            counts["accepted"] += 1
        elif attr_cwid:
            _resolve(led, j, "resolved_attributed", attr_cwid, run_date)
            counts["attributed"] += 1
        elif reject_cwid:
            _resolve(led, j, "resolved_rejected", reject_cwid, run_date)
            counts["rejected"] += 1
        else:
            led.loc[j, "last_checked"] = run_date

    # reflect article-level attribution back into the processed log
    if attr:
        pm = store.processed
        hit = pm["pmid"].isin(attr)
        pm.loc[hit, "last_status"] = "attributed"
        pm.loc[hit, "last_checked"] = run_date
    return counts


def _row_cwids(row):
    out = []
    if pd.notna(row.get("top_cwid")):
        out.append(row["top_cwid"])
    try:
        for c in json.loads(row.get("candidate_cwids_json") or "[]"):
            if c.get("cwid"):
                out.append(c["cwid"])
    except (ValueError, TypeError):
        pass
    return list(dict.fromkeys(out))


def _resolve(led, j, status, cwid, run_date):
    led.loc[j, "status"] = status
    led.loc[j, "resolution_cwid"] = cwid
    led.loc[j, "resolved_date"] = run_date
    led.loc[j, "last_checked"] = run_date


def _export(store, export_dir, run_date, date_from, date_to, u, n_new, n_attr,
            n_orphan, n_suggested, n_orphan_buried, n_authorships, n_scored, n_rows,
            resolved):
    out = os.path.join(export_dir, run_date)
    os.makedirs(out, exist_ok=True)
    led = store.ledger
    open_matched = led[(led["status"].isin(OPEN)) & (led["match_status"] == "matched")].copy()
    open_matched["row_state"] = open_matched["first_seen"].apply(
        lambda fs: "NEW" if str(fs) == run_date else "CARRYOVER")
    open_matched["top_io_score_sort"] = pd.to_numeric(
        open_matched["top_io_score"], errors="coerce").fillna(-1)
    open_matched = open_matched.sort_values(
        ["top_io_score_sort", "top_confidence"], ascending=False)

    view_cols = ["row_state", "pmid", "entrez_date", "wcm_author", "author_position_label",
                 "top_cwid", "top_name", "top_person_type", "top_dept",
                 "top_io_score", "top_final_score", "top_confidence", "top_given_match",
                 "top_affil_match", "top_cohort_size", "n_candidates", "title", "journal",
                 "doi", "author_affiliation", "candidate_cwids_json", "author_key"]
    open_matched[view_cols].to_csv(os.path.join(out, "open_orphans.csv"), index=False)
    led[(led["status"].isin(OPEN)) & (led["match_status"] == "no_identity_match")][
        ["pmid", "entrez_date", "wcm_author", "author_affiliation", "title", "author_key"]
    ].to_csv(os.path.join(out, "unmatched_authorships.csv"), index=False)
    store.processed.to_csv(os.path.join(out, "processed_log_snapshot.csv"), index=False)

    summary = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "run_date": run_date, "window": {"from": date_from, "to": date_to},
        "universe": {"esearch_count": u["esearch_count"], "in_window": u["in_window"],
                     "with_wcm_author": u["with_wcm_author"]},
        "new_pmids": n_new, "attributed": n_attr,
        "orphan_articles_not_accepted": n_orphan,
        "suggested_excluded": n_suggested, "buried_articles_kept": n_orphan_buried,
        "wcm_authorships_added": n_authorships, "candidate_cwids_scored": n_scored,
        "ledger_rows_added": n_rows, "resolved_this_run": resolved,
        "ledger_totals": _status_counts(led),
        "open_matched": int(len(open_matched)),
        "model_hashes": matcher.det.MODEL_HASHES,
    }
    with open(os.path.join(out, "run_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    return summary


def _status_counts(led):
    return {k: int(v) for k, v in led["status"].value_counts().items()}


def window_slices(date_from, date_to, days=31):
    """Tile [date_from, date_to] into contiguous, non-overlapping ~`days` slices.

    EDAT windows are inclusive on both ends, so each slice ends the day before the
    next begins. 'YYYY/MM/DD' in, list of ('YYYY/MM/DD','YYYY/MM/DD') out."""
    lo = datetime.strptime(date_from, "%Y/%m/%d").date()
    hi = datetime.strptime(date_to, "%Y/%m/%d").date()
    out = []
    a = lo
    while a <= hi:
        b = min(a + timedelta(days=days - 1), hi)
        out.append((a.strftime("%Y/%m/%d"), b.strftime("%Y/%m/%d")))
        a = b + timedelta(days=1)
    return out


def run_tiled(date_from, date_to, state_dir, export_dir, run_date, workers=16,
              max_records=None, days=31, write_db=True):
    """Initial-backlog driver: process the window in monthly slices, sharing one
    identity index + identity-only cache across all slices, persisting state after
    each (checkpoint), and running the resolution re-check once at the end."""
    slices = window_slices(date_from, date_to, days=days)
    ctx = _new_ctx(state_dir)
    print(f"Tiled run: {len(slices)} slices of ~{days}d over {date_from}..{date_to}",
          flush=True)
    for i, (a, b) in enumerate(slices):
        last = i == len(slices) - 1
        print(f"\n########## SLICE {i + 1}/{len(slices)}  {a} .. {b} "
              f"{'(final: +re-check)' if last else ''} ##########", flush=True)
        run(a, b, state_dir, export_dir, run_date, workers=workers,
            max_records=max_records, recheck=last, ctx=ctx, write_db=write_db)
    led = ctx["store"].ledger
    print("\n==== BACKLOG COMPLETE ====", flush=True)
    print(json.dumps({
        "slices": len(slices), "window": {"from": date_from, "to": date_to},
        "ledger_totals": _status_counts(led),
        "processed_pmids": int(len(ctx["store"].processed)),
        "open_matched_io_ge90": int(((led["status"].isin(OPEN))
            & (pd.to_numeric(led["top_io_score"], errors="coerce") >= 90)).sum()),
    }, indent=2), flush=True)


def _selftest():
    """Offline checks for the per-authorship gate (issues #160 and #174). No network, no
    DB: every article here has doi=None so _db_rows never calls aar_db.dup_flags_by_doi,
    and _recheck's gate/GoldStandard lookups are stubbed."""
    ok = True

    def check(label, cond):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'OK' if cond else '** FAIL'}] {label}")

    T = gate.STORAGE_THRESHOLD

    def auth(fg, cwid="aaa1001", fore="F", last="N"):
        art = {"pmid": 1, "entrez_date": "2026-01-01", "title": "t", "journal": "j",
               "doi": None, "authors": [], "pub_year": 2026}
        top = {"cwid": cwid, "name": "N", "person_type": "p", "dept": "d",
               "final_score": fg, "io_score": 5.0, "confidence": "high",
               "years_after_wcm": None, "cohort_size": 1, "given_match": 1,
               "affil_dept_match": True}
        return (art, 0, 1, {"last": last, "fore": fore, "affiliations": []}, [top], top)

    rows = _db_rows([auth(T + 1), auth(None), auth(T - 1)], "2026-01-01")
    check("_db_rows skips an authorship already curated at FG>=threshold",
          len(rows) == 2 and all(r["classification"] != "suggested" for r in rows))
    check("_db_rows keeps absent (no FG) and buried (FG<threshold)",
          sorted(r["classification"] for r in rows) == ["absent", "buried"])
    check("_db_rows still skips an unmatched authorship (top is None)",
          _db_rows([(auth(None)[0], 0, 1, {}, [], None)], "2026-01-01") == [])

    # The S3/scoring failure hole. final_score is None on NoSuchKey, cold storage, a
    # malformed artifact, or a scoring exception. Before #160 that never mattered --
    # attributed articles were not exploded at all. Now they are, so without reciterdb's
    # own attribution the failure would write a spurious "absent" row for an authorship
    # production already holds.
    check("_db_rows writes an 'absent' row when the score is unavailable and reciterdb "
          "has no attribution for the pmid",
          [r["classification"] for r in _db_rows([auth(None)], "2026-01-01")] == ["absent"])
    check("_db_rows skips it once reciterdb says that cwid holds the article, even with "
          "no score at all",
          _db_rows([auth(None, cwid="aaa1001")], "2026-01-01",
                   {1: [("aaa1001", None, ())]}) == [])
    check("reciterdb attribution for a DIFFERENT cwid, whose name is not this byline, "
          "does not suppress this authorship",
          [r["classification"] for r in
           _db_rows([auth(None, cwid="aaa1001")], "2026-01-01",
                    {1: [("bbb2002", None, (("Jane", "Doe"),))]})]
          == ["absent"])

    # ---- issue #174: the byline's OWNER, not the proposed candidate ---------
    # The anchor. Byline "Tony Rosen"; the matcher proposed ltr4001 ("Leah Teresa Rosen",
    # middle-initial fallback); aer2006 has held the pmid at 100/ACCEPTED since 44 days
    # before the row existed. Under the two-signal gate every check missed it: aer2006 is
    # not the proposed cwid, and top_fg_score is NULL so the score check failed open.
    rosen = ("aer2006", "last", (("Tony", "Rosen"), ("Anthony", "Rosen")))
    check("a byline whose real owner is attributed is suppressed even though the "
          "PROPOSED candidate is someone else",
          _db_rows([auth(None, cwid="ltr4001", fore="Tony", last="Rosen")],
                   "2026-01-01", {1: [rosen]}) == [])
    check("the HR legal name alone would NOT have caught the anchor -- publishing name "
          "and legal name disagree, which is why all three sources are returned",
          [r["classification"] for r in
           _db_rows([auth(None, cwid="ltr4001", fore="Tony", last="Rosen")],
                    "2026-01-01", {1: [("aer2006", "last", (("Anthony", "Rosen"),))]})]
          == ["absent"])
    check("a genuinely unattributed byline is still written -- the attributed co-author "
          "shares the pmid, not the name",
          [r["classification"] for r in
           _db_rows([auth(None, cwid="ltr4001", fore="Tony", last="Rosen")],
                    "2026-01-01",
                    {1: [("klh4011", None, (("Kristin", "Lees Haggerty"),))]})]
          == ["absent"])
    check("the first-initial tier is NOT shipped: byline 'J Kim' does not match "
          "Junbum Kim (19 rows, and it matches half a surname cohort)",
          [r["classification"] for r in
           _db_rows([auth(None, cwid="jyk9001", fore="J", last="Kim")], "2026-01-01",
                    {1: [("jbk9001", None, (("Junbum", "Kim"),))]})]
          == ["absent"])

    # The homonym decision, both halves. gate._identifying_name is the census key, so a
    # spelling 2+ WCM people answer to never reaches here -- the holder arrives with an
    # empty `names` and the row stays open. 80 rows of the 485 pay for it; see
    # _byline_owner for why that trade is the safe direction.
    check("a homonym-ambiguous byline is SPARED: the gate withholds the shared spelling, "
          "so nothing matches and the authorship stays in the queue",
          [r["classification"] for r in
           _db_rows([auth(None, cwid="acl2007", fore="Andrew", last="Lee")],
                    "2026-01-01", {1: [("agl2003", None, ())]})]
          == ["absent"])
    check("the census key is (given token, surname tokens), so 'Han-Jo' and 'Han Jo' "
          "collide and a bare initial is not a key at all",
          gate._identifying_name("Han-Jo", "Kim") == ("han", ("kim",))
          and gate._identifying_name("Han Jo", "Kim") == ("han", ("kim",))
          and gate._identifying_name("J", "Kim") is None)

    # _recheck: an open row must resolve only when ITS OWN cwid is attributed.
    led = pd.DataFrame([
        {"pmid": 99, "top_cwid": "mine001", "candidate_cwids_json": "[]",
         "wcm_author": "Mine One", "status": "open", "snooze_until": None,
         "last_checked": None},
        {"pmid": 99, "top_cwid": "other02", "candidate_cwids_json": "[]",
         "wcm_author": "Other Two", "status": "open", "snooze_until": None,
         "last_checked": None},
    ])
    for c in LEDGER_COLS:
        if c not in led.columns:
            led[c] = pd.NA
    store = type("S", (), {"ledger": led[LEDGER_COLS],
                           "processed": pd.DataFrame({c: pd.Series(dtype="object")
                                                      for c in PROCESSED_COLS})})()
    g_attr, g_who, g_gs = gate.attributed_pmids, gate.attributions, _batch_gold_standard
    gate.attributed_pmids = lambda ps: {98, 99}
    gate.attributions = lambda ps: {99: [("mine001", None, ())]}
    globals()["_batch_gold_standard"] = lambda cwids: {}
    try:
        counts = _recheck(store, "2026-01-02")
    finally:
        gate.attributed_pmids, gate.attributions = g_attr, g_who
        globals()["_batch_gold_standard"] = g_gs
    st = list(store.ledger["status"])
    check("_recheck resolves the row whose own cwid is attributed",
          counts["attributed"] == 1 and st[0] == "resolved_attributed")
    check("_recheck leaves a co-author's row on the same pmid OPEN",
          st[1] == "open")

    # A row that names no candidate at all has nothing to narrow to; it must still close
    # on article-level attribution, or 1,336 of prod's 1,923 open rows never resolve.
    led2 = pd.DataFrame([{"pmid": 99, "top_cwid": None, "candidate_cwids_json": "[]",
                          "wcm_author": None, "status": "open", "snooze_until": None,
                          "last_checked": None}])
    for c in LEDGER_COLS:
        if c not in led2.columns:
            led2[c] = pd.NA
    store2 = type("S", (), {"ledger": led2[LEDGER_COLS],
                            "processed": pd.DataFrame({c: pd.Series(dtype="object")
                                                       for c in PROCESSED_COLS})})()
    gate.attributed_pmids = lambda ps: {98, 99}
    gate.attributions = lambda ps: {99: [("someone1", None, ())]}
    globals()["_batch_gold_standard"] = lambda cwids: {}
    try:
        c2 = _recheck(store2, "2026-01-02")
    finally:
        gate.attributed_pmids, gate.attributions = g_attr, g_who
        globals()["_batch_gold_standard"] = g_gs
    check("_recheck still closes a candidate-less (no_identity_match) row on "
          "article-level attribution",
          c2["attributed"] == 1
          and store2.ledger["status"].iloc[0] == "resolved_attributed")

    # #174 in the recheck path. 23 of a dated 80-row anchor-shape sample became attributed
    # AFTER the row was created, so the emission gate can never see them; the row names
    # ltr4001, nobody attributes ltr4001, and only the byline closes it. The second row is
    # the control: same pmid, same run, a byline nobody holds stays open.
    led3 = pd.DataFrame([
        {"pmid": 99, "top_cwid": "ltr4001", "candidate_cwids_json": "[]",
         "wcm_author": "Tony Rosen", "status": "open", "snooze_until": None,
         "last_checked": None},
        {"pmid": 99, "top_cwid": "xyz1234", "candidate_cwids_json": "[]",
         "wcm_author": "Dana Vance", "status": "open", "snooze_until": None,
         "last_checked": None},
        # Candidate-less row (match_status='no_identity_match' -- the majority of the
        # ledger). It keeps the pre-#174 article-level fallback "credit the first
        # attributed cwid", but the byline owner must WIN over it, and `bxx1111` is
        # deliberately listed first so the fallback would credit the wrong person if the
        # ordering were reversed. Without this row every fixture has a top_cwid, `cc` is
        # never empty, and the fallback branch is unreachable -- the ordering could be
        # inverted with the suite still green.
        {"pmid": 98, "top_cwid": None, "candidate_cwids_json": "[]",
         "wcm_author": "Tony Rosen", "status": "open", "snooze_until": None,
         "last_checked": None},
    ])
    for c in LEDGER_COLS:
        if c not in led3.columns:
            led3[c] = pd.NA
    store3 = type("S", (), {"ledger": led3[LEDGER_COLS],
                            "processed": pd.DataFrame({c: pd.Series(dtype="object")
                                                       for c in PROCESSED_COLS})})()
    gate.attributed_pmids = lambda ps: {98, 99}
    other = ("bxx1111", "first", (("Dana", "Vance"),))
    gate.attributions = lambda ps: {99: [rosen], 98: [other, rosen]}
    globals()["_batch_gold_standard"] = lambda cwids: {}
    try:
        c3 = _recheck(store3, "2026-01-02")
    finally:
        gate.attributed_pmids, gate.attributions = g_attr, g_who
        globals()["_batch_gold_standard"] = g_gs
    check("_recheck closes a row on the BYLINE's owner and credits that owner, not the "
          "candidate the matcher proposed",
          c3["attributed"] == 2                    # this row and the candidate-less one below
          and store3.ledger["status"].iloc[0] == "resolved_attributed"
          and store3.ledger["resolution_cwid"].iloc[0] == "aer2006")
    check("_recheck leaves a different byline on the same attributed pmid OPEN",
          store3.ledger["status"].iloc[1] == "open")
    check("_recheck on a CANDIDATE-LESS row credits the byline's owner, not merely the "
          "first attributed cwid on the pmid",
          store3.ledger["status"].iloc[2] == "resolved_attributed"
          and store3.ledger["resolution_cwid"].iloc[2] == "aer2006")

    print("SELFTEST", "PASS" if ok else "FAIL")
    return ok


def run_backfill(state_dir, run_date, workers=16, batch_size=500, limit=None, write_db=True,
                 before=None, pmid_file=None):
    """Re-processes pmids previously logged as article-level 'attributed' -- under the
    OLD gate these were skipped at step 3 entirely, so any co-author who wasn't the
    one already attributed (e.g. prs4005 on PMID 41000987) was never scored or
    written to reciterdb.authorship_review. Re-fetches those specific pmids by ID
    (EFetch id= list, not an ESearch/date window -- processed_log only stores
    pmid/entrez_date, not full author metadata), explodes into WCM authorships,
    classifies per-authorship, and upserts into reciterdb.authorship_review only
    (idempotent by author_key, never touches an existing curator decision; does NOT
    also append to ledger.csv/processed_log -- those stay accurate as article-level
    records, this only backfills the PM-facing per-authorship sink).

    `before` (YYYY-MM-DD, matched against processed_log.first_seen) is what keeps this
    targeted. 'attributed' meant "skipped at step 3" only under the OLD article-level
    gate; from #160 on, every new pmid is exploded, so rows first seen after the deploy
    carry that label while already being fully processed. Pass the deploy date and the
    target set stays the actual backlog. Without it the run is still correct -- the
    upsert is idempotent -- just increasingly wasteful of EFetch and IO quota.

    `pmid_file` (one pmid per line, blanks and #comments ignored) drives the run from an
    explicit list instead of processed_log, and is how the pre-in-cluster backlog gets
    recovered. The S3 processed_log only goes back to the first in-cluster run: 2,747
    rows, entrez 2026-04-25 onward. The 2024-06-08..2026-04-29 backlog -- 21,080 rows,
    10,325 of them 'attributed' -- exists only in the Mac-era state file now living at
    `analysis/adversarial_attribution_review/state/processed_log.csv` in the ReCiter
    Research repo. PMID 41000987, the prs4005 case this function's first paragraph names,
    is in that file and NOT in S3, so --s3-state alone cannot reach the case that
    motivates the whole recovery. Extract the list from there and pass it here."""
    store = LedgerStore(state_dir)
    idx = matcher.IdentityIndex.load()
    io = matcher.IdentityOnlyScorer()
    if pmid_file:
        with open(pmid_file) as fh:
            pmids = sorted({int(ln.split("#")[0].strip())
                            for ln in fh if ln.split("#")[0].strip()})
        scope = f"explicit list from {pmid_file}"
    else:
        target = store.processed["last_status"] == "attributed"
        if before:
            target &= store.processed["first_seen"].astype(str) < before
        pmids = sorted(int(p) for p in store.processed.loc[target, "pmid"].dropna())
        scope = (f"processed_log 'attributed', first_seen < {before}" if before else
                 "processed_log 'attributed', UNBOUNDED -- pass --before <deploy date> "
                 "to target only the pre-#160 backlog")
    if limit:
        pmids = pmids[:limit]
    if not pmid_file and not len(store.processed):
        # In-cluster the ledger lives in S3, so `--mode backfill` without `--s3-state`
        # reads an empty state dir, finds nothing, and exits 0 looking like a success.
        raise SystemExit(f"Backfill: processed_log at {store.processed_path} is empty or "
                         f"missing -- in-cluster that means --s3-state was omitted. "
                         f"Refusing to report success on a zero-row run.")
    print(f"Backfill: {len(pmids)} pmids to re-explode "
          f"({len(store.processed)} in processed_log; scope = {scope})", flush=True)
    groups = uni.load_home_institution_groups()
    total_rows = 0
    n_batches = -(-len(pmids) // batch_size) if pmids else 0
    for bi, i in enumerate(range(0, len(pmids), batch_size), 1):
        chunk = pmids[i:i + batch_size]
        arts = uni.efetch_by_ids(chunk, groups)
        by_pmid = {a["pmid"]: a for a in arts}
        authorships = []
        cwid_pool = set()
        for p in chunk:
            a = by_pmid.get(p)
            if not a:
                continue
            n = len(a["authors"])
            for j, au in enumerate(a["authors"]):
                if not au.get("home_inst"):
                    continue
                cands, _ = idx.candidates(au.get("last"), au.get("fore"),
                                          au.get("initials"), au.get("affiliations"),
                                          top_k=5, pub_year=a.get("pub_year"))
                cwid_pool.update(c["cwid"] for c in cands)
                authorships.append((a, j, n, au, cands))
        pool = sorted(c for c in cwid_pool if c not in io._cache)
        if pool:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(io.scores, pool))
        resolved_auth = []
        for a, j, n, au, _ in authorships:
            cands = matcher.match_authorship(au, a["pmid"], idx, io, top_k=5,
                                             pub_year=a.get("pub_year"))
            top = cands[0] if cands else None
            resolved_auth.append((a, j, n, au, cands, top))
        if write_db:
            chunk_attr = gate.attributions(chunk) if chunk else {}
            db_rows = _db_rows(resolved_auth, run_date, chunk_attr)
            aar_db.upsert(db_rows)
            total_rows += len(db_rows)
        missing = [p for p in chunk if p not in by_pmid]
        print(f"  batch {bi}/{n_batches}: {len(chunk)} requested, {len(by_pmid)} fetched"
              f"{f' ({len(missing)} NOT RETURNED by EFetch)' if missing else ''} -> "
              f"{len(authorships)} authorships ({len(pool)} newly IO-scored)", flush=True)
    print(f"Backfill done: {total_rows} authorship_review rows upserted "
          f"({'DRY RUN, no DB write' if not write_db else 'written'})", flush=True)
    return total_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true",
                    help="offline per-authorship gate checks (no network, no DB)")
    ap.add_argument("--mode", choices=["initial", "recurring", "backfill"])
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument("--max", type=int, default=None, help="cap universe fetch (testing)")
    ap.add_argument("--state-dir", default=DEFAULT_STATE)
    ap.add_argument("--export-dir", default=DEFAULT_EXPORT)
    ap.add_argument("--run-date", default=date.today().isoformat())
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--pmid-file", default=None,
                    help="backfill only: re-explode exactly these pmids (one per line) "
                         "instead of processed_log -- the only route to the "
                         "pre-in-cluster backlog, which is not in S3 state")
    ap.add_argument("--before", default=None,
                    help="backfill only: restrict to processed_log rows with "
                         "first_seen < this YYYY-MM-DD (pass the #160 deploy date)")
    ap.add_argument("--no-db", action="store_true",
                    help="skip the reciterdb.authorship_review sink (CSV/state only)")
    ap.add_argument("--s3-state", action="store_true",
                    help="pull/push ledger+processed_log from S3 (in-cluster CronJob); "
                         "state-dir/export-dir become ephemeral temp dirs")
    args = ap.parse_args()

    if args.selftest:
        sys.exit(0 if _selftest() else 1)

    if args.s3_state:
        if not S3_STATE_BUCKET:
            ap.error("--s3-state requires AAR_S3_BUCKET or S3_BUCKET in the environment")
        import tempfile
        args.state_dir = tempfile.mkdtemp(prefix="aar-state-")
        args.export_dir = tempfile.mkdtemp(prefix="aar-export-")
        _s3_pull_state(args.state_dir)

    if args.mode == "backfill":
        # DB sink only -- no ledger/processed_log mutation, so no _s3_push_state.
        run_backfill(args.state_dir, args.run_date, workers=args.workers,
                     limit=args.max, write_db=not args.no_db, before=args.before,
                     pmid_file=args.pmid_file)
        return

    if args.mode:
        d_from, d_to = (uni._fmt(x) for x in uni.window_for_mode(args.mode))
    elif args.date_from and args.date_to:
        d_from, d_to = args.date_from, args.date_to
    else:
        ap.error("provide --mode or both --from and --to")

    # initial backlog spans ~2y -> tile into checkpointed monthly slices;
    # recurring / custom windows are a single slice with an immediate re-check.
    if args.mode == "initial":
        run_tiled(d_from, d_to, args.state_dir, args.export_dir, args.run_date,
                  workers=args.workers, max_records=args.max, write_db=not args.no_db)
    else:
        run(d_from, d_to, args.state_dir, args.export_dir, args.run_date,
            workers=args.workers, max_records=args.max, write_db=not args.no_db)

    if args.s3_state:
        _s3_push_state(args.state_dir, args.run_date)


if __name__ == "__main__":
    main()
