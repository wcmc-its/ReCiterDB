#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 4: per-run orchestrator + stateful ledger.

Ties Steps 1-3 into one idempotent, en-masse run and maintains the two persistent
stores (orphan ledger + processed-PMID log) the plan calls for. Designed to clear
the initial backlog (`--mode initial`, ~2y window) in one batch and then run
monthly (`--mode recurring`) over a rolling slice, with overlapping windows
harmless because the processed log prevents re-gating.

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
  python aar_orchestrator.py --mode recurring      # monthly rolling slice
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
            "cohort_size", "confidence", "io_score", "final_score", "io_source")
    return [{k: c.get(k) for k in keep} for c in cands]


def _trunc(s, n):
    return s[:n] if isinstance(s, str) and len(s) > n else s


def _db_rows(resolved_auth, run_date):
    """authorship_review rows for matched authorships, classified PER-AUTHORSHIP:
    absent (top candidate never scored) / suggested (FG>=30, in a pending queue) /
    buried (FG<30). Includes suggested rows (PM shows them with FG+IO). Unmatched
    authorships (no candidate to assign) are skipped in v1. single_candidate uses the
    true cohort size (unique surname+initial), the strongest precision signal."""
    out = []
    for a, i, n, au, cands, top in resolved_auth:
        if top is None:
            continue                                   # unmatched: nothing to assign
        fg = top.get("final_score")
        cls = ("absent" if fg is None
               else "suggested" if fg >= gate.STORAGE_THRESHOLD else "buried")
        cohort = top.get("cohort_size")
        out.append({
            "source": "pubmed",
            "pmid": a["pmid"],
            "author_key": f"{a['pmid']}:{i}",
            "author_position": i + 1,
            "author_position_label": _position_label(i, n),
            "wcm_author": _trunc(" ".join(x for x in (au.get("fore"), au.get("last")) if x)
                                 or au.get("initials") or au.get("last"), 255),
            "author_affiliation": " | ".join(au.get("affiliations") or []),
            "entrez_date": a["entrez_date"], "title": a["title"],
            "journal": _trunc(a["journal"], 512), "doi": _trunc(a["doi"], 255),
            "classification": cls,
            "top_cwid": top["cwid"], "top_name": _trunc(top["name"], 255),
            "top_person_type": _trunc(top["person_type"], 64),
            "top_dept": _trunc(top["dept"], 255),
            "top_fg_score": fg, "top_io_score": top.get("io_score"),
            "top_confidence": top["confidence"], "top_cohort_size": cohort,
            "top_given_match": top["given_match"],
            "top_affil_match": int(bool(top["affil_dept_match"])),
            "n_candidates": len(cands), "single_candidate": int(cohort == 1),
            "candidate_cwids_json": json.dumps(_compact(cands)),
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

    # --- 2. gate (batched, global) ------------------------------------------
    log("[2/6] Gating new pmids against reciterdb (analysis_summary_author) ...")
    attributed = gate.attributed_pmids(new_pmids) if new_pmids else set()
    orphan_pmids = [p for p in new_pmids if p not in attributed]
    log(f"      {len(attributed)} attributed, {len(orphan_pmids)} ORPHAN articles")

    # --- 3. explode orphan articles into WCM authorships + match -------------
    log("[3/6] Matching WCM authorships on orphan articles ...")
    log(f"      identity index: {sum(len(v) for v in idx.by_surname.values())} people")
    authorships = []  # (article, position, author, candidates)
    cwid_pool = set()
    for p in orphan_pmids:
        a = by_pmid[p]
        n = len(a["authors"])
        for i, au in enumerate(a["authors"]):
            if not au.get("home_inst"):
                continue
            cands, _ = idx.candidates(au.get("last"), au.get("fore"),
                                      au.get("initials"), au.get("affiliations"), top_k=5)
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

    # production-final gate (cache hits now; no further S3): an article already SUGGESTED
    # to a WCM person at the storage threshold sits in that curator's pending queue, so it
    # is NOT buried -> exclude the whole article (the lja2002/42026694 case). Resolve the
    # top candidate per authorship, flag articles with ANY candidate final >= 30, drop them.
    resolved_auth, suggested_pmids = [], set()
    for a, i, n, au, _ in authorships:
        cands = matcher.match_authorship(au, a["pmid"], idx, io, top_k=5)
        top = cands[0] if cands else None
        if top and top.get("final_score") is not None \
                and top["final_score"] >= gate.STORAGE_THRESHOLD:
            suggested_pmids.add(a["pmid"])
        resolved_auth.append((a, i, n, au, cands, top))
    log(f"      {len(suggested_pmids)} orphan articles already SUGGESTED (final>=30, in a "
        f"pending queue) -> EXCLUDED; {len(orphan_pmids) - len(suggested_pmids)} buried/absent kept")

    # DB sink (Publication Manager source): upsert ALL matched authorships, classified
    # per-authorship (suggested/buried/absent). Unlike the CSV path below, suggested rows
    # are kept — PM shows them with FG+IO. Curator status on existing rows is preserved.
    if write_db:
        db_rows = _db_rows(resolved_auth, run_date)
        aar_db.upsert(db_rows)
        log(f"      upserted {len(db_rows)} matched authorships -> reciterdb.authorship_review")

    new_rows = []
    for a, i, n, au, cands, top in resolved_auth:
        if a["pmid"] in suggested_pmids:
            continue                                  # whole article is in a pending queue
        new_rows.append({
            "pmid": a["pmid"],
            "author_key": f"{a['pmid']}:{i}",
            "author_position": i + 1,
            "author_position_label": _position_label(i, n),
            "wcm_author": " ".join(x for x in (au.get("fore"), au.get("last")) if x)
                          or au.get("initials") or au.get("last"),
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
    summary = _export(store, export_dir, run_date, date_from, date_to, u,
                      len(new_pmids), len(attributed), len(orphan_pmids),
                      len(suggested_pmids), len(authorships), len(pool),
                      len(new_rows), resolved)
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
    for j in open_idx:
        row = led.loc[j]
        pmid = int(row["pmid"])
        cc = _row_cwids(row)
        accept_cwid = next((c for c in cc if pmid in gs.get(c, (set(), set()))[0]), None)
        reject_cwid = next((c for c in cc if pmid in gs.get(c, (set(), set()))[1]), None)
        if accept_cwid:
            _resolve(led, j, "resolved_accepted", accept_cwid, run_date)
            counts["accepted"] += 1
        elif pmid in attr:
            who = attr_who.get(pmid) or [(None, None)]
            _resolve(led, j, "resolved_attributed", who[0][0], run_date)
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
            n_orphan, n_suggested, n_authorships, n_scored, n_rows, resolved):
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
        "suggested_excluded": n_suggested, "buried_articles_kept": n_orphan - n_suggested,
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["initial", "recurring"])
    ap.add_argument("--from", dest="date_from")
    ap.add_argument("--to", dest="date_to")
    ap.add_argument("--max", type=int, default=None, help="cap universe fetch (testing)")
    ap.add_argument("--state-dir", default=DEFAULT_STATE)
    ap.add_argument("--export-dir", default=DEFAULT_EXPORT)
    ap.add_argument("--run-date", default=date.today().isoformat())
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--no-db", action="store_true",
                    help="skip the reciterdb.authorship_review sink (CSV/state only)")
    ap.add_argument("--s3-state", action="store_true",
                    help="pull/push ledger+processed_log from S3 (in-cluster CronJob); "
                         "state-dir/export-dir become ephemeral temp dirs")
    args = ap.parse_args()

    if args.s3_state:
        if not S3_STATE_BUCKET:
            ap.error("--s3-state requires AAR_S3_BUCKET or S3_BUCKET in the environment")
        import tempfile
        args.state_dir = tempfile.mkdtemp(prefix="aar-state-")
        args.export_dir = tempfile.mkdtemp(prefix="aar-export-")
        _s3_pull_state(args.state_dir)

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
