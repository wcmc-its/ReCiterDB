#!/usr/bin/env python3
"""
Adversarial Attribution Review — Step 2: attribution gate.

PRIMARY GATE (article-first, orphan detection) — reciterdb:
  An article is ATTRIBUTED iff its pmid appears in `analysis_summary_author`
  (built from the Analysis, which holds only >=30 / accepted articles). So
  `orphans(universe_pmids)` = pmids NOT in that table = no WCM person at >=30.
  One indexed SQL query, evaluated globally across all WCM people. This is the
  gate the orphan ledger uses.

SECONDARY (per-uid scoring) — kept for RANKING orphan candidates, not gating:
  AttributionResolver recomputes the production final per (uid, pmid) from the
  S3 scoring inputs with the pinned models, used to rank how likely an orphan
  belongs to a candidate (identity-only score). reciterdb can't do this because
  orphans aren't in it.

Per-(uid,pmid) status (secondary path):

Per-pair status (authoritative, cheap — no 25GB Analysis scan):
  accepted        pmid in the uid's GoldStandard.knownpmids
  rejected        pmid in the uid's GoldStandard.rejectedpmids
  suggested_ge30  production final score >= 30 (already surfaced in PM's pending queue)
  buried          retrieved & scored but final < 30  (the Worgall case)
  absent          never retrieved for this uid
  input_unavailable   scoring input missing/cold-storage — can't confirm

"final" is recomputed from the per-user scoring input with the pinned local models
(reusing the detector): feedback users -> min(fb, io*33)*100; feedback-less users ->
identity-only*100 (their production pipeline). This matches what the storage filter
persisted, so "final >= 30" is equivalent to "present in the Analysis".

A PMID is ATTRIBUTED (gate drops it) iff some candidate is accepted or suggested_ge30.
rejected / buried / absent are NOT attribution (a reject by A doesn't make it B's;
buried/absent are exactly what we want to surface).

Usage (self-test):
  python aar_gate.py --selftest
  python aar_gate.py --pmid 42220538 --uids stw2006
"""
import argparse, json, os, sys

import boto3
import pandas as pd
from sqlalchemy import create_engine, text, bindparam

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import adversarial_attribution_review as det  # model scoring engine (step 0)

_dyn = boto3.client("dynamodb", region_name="us-east-1")
IDENTITY_ONLY_SUFFIX = "-identityOnlyScoringInput.json"
STORAGE_THRESHOLD = det.STORAGE_THRESHOLD  # 30

ATTRIBUTED = {"accepted", "suggested_ge30"}


# ===========================================================================
# PRIMARY GATE (article-first, reciterdb) — orphan-article detection
# ===========================================================================
_ENGINE = None


def _reciterdb():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(
            f"mysql+pymysql://{os.environ['DB_USERNAME']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 15}, pool_pre_ping=True)
    return _ENGINE


def attributed_pmids(pmids):
    """Subset of pmids already attributed to some WCM person.

    Presence in analysis_summary_author == in some Analysis at >=30 / accepted.
    This is the gate: anything NOT returned is an orphan article."""
    pmids = sorted({int(p) for p in pmids})
    if not pmids:
        return set()
    stmt = text("SELECT DISTINCT pmid FROM analysis_summary_author WHERE pmid IN :ps") \
        .bindparams(bindparam("ps", expanding=True))
    found = set()
    with _reciterdb().connect() as c:
        for i in range(0, len(pmids), 1000):
            chunk = pmids[i:i + 1000]
            found.update(int(r[0]) for r in c.execute(stmt, {"ps": chunk}))
    return found


def attributions(pmids):
    """pmid -> [(cwid, authorPosition), ...] for the article-level matcher (who's
    already assigned, so the ledger can mark which WCM authorships are still open)."""
    pmids = sorted({int(p) for p in pmids})
    if not pmids:
        return {}
    stmt = text("SELECT pmid, personIdentifier, authorPosition "
                "FROM analysis_summary_author WHERE pmid IN :ps") \
        .bindparams(bindparam("ps", expanding=True))
    out = {}
    with _reciterdb().connect() as c:
        for i in range(0, len(pmids), 1000):
            for pmid, cwid, pos in c.execute(stmt, {"ps": pmids[i:i + 1000]}):
                out.setdefault(int(pmid), []).append((cwid, pos))
    return out


def orphan_pmids(pmids):
    """Orphan articles = pmids with no WCM attribution at all."""
    attr = attributed_pmids(pmids)
    return [int(p) for p in dict.fromkeys(int(x) for x in pmids) if int(p) not in attr]


def _final_scores_for_uid(uid):
    """Return (source, {pmid: final_score}). Tries feedback input, then identity-only."""
    status, _, rows = det.score_user(uid)          # feedback-identity input path
    if status == "ok":
        return "feedback", {r["pmid"]: r["final_score"] for r in rows}
    if status == "error":
        return "input_unavailable", {}             # e.g. cold-storage InvalidObjectState
    if status == "missing":
        try:
            obj = det._s3.get_object(Bucket=det.BUCKET, Key=uid + IDENTITY_ONLY_SUFFIX)
            arts = json.loads(obj["Body"].read())
        except det._s3.exceptions.NoSuchKey:
            return "none", {}
        except Exception:                          # noqa: BLE001
            return "input_unavailable", {}
        if not isinstance(arts, list) or not arts:
            return "none", {}
        df = pd.DataFrame(arts)
        pmid_key = next((k for k in det.PMID_KEYS if k in df.columns), None)
        if pmid_key is None:
            return "none", {}
        try:
            io_cal = det._score(df, det.IO_MODEL, det.IO_SCALER, det.IO_CALIB,
                                det.IDENTITY_ONLY_BASE_FEATURES,
                                det.compute_derived_features_identity_only,
                                det.IDENTITY_ONLY_FEATURES)
        except Exception:                          # noqa: BLE001
            return "input_unavailable", {}
        pmids = pd.to_numeric(df[pmid_key], errors="coerce")
        scores = (io_cal * 100.0)
        return "identity_only", {int(p): float(s) for p, s in zip(pmids, scores)
                                 if pd.notna(p)}
    return "none", {}                              # empty


class AttributionResolver:
    """Caches per-uid gold standard + recomputed final scores across many lookups."""

    def __init__(self):
        self._gs = {}
        self._scores = {}

    def _gold(self, uid):
        if uid not in self._gs:
            item = _dyn.get_item(
                TableName="GoldStandard", Key={"uid": {"S": uid}},
                ProjectionExpression="knownpmids, rejectedpmids").get("Item", {})

            def to_set(field):
                return {int(x["N"]) for x in item.get(field, {}).get("L", []) if "N" in x}

            self._gs[uid] = (to_set("knownpmids"), to_set("rejectedpmids"))
        return self._gs[uid]

    def _final(self, uid):
        if uid not in self._scores:
            self._scores[uid] = _final_scores_for_uid(uid)
        return self._scores[uid]

    def status(self, uid, pmid):
        pmid = int(pmid)
        known, rejected = self._gold(uid)
        if pmid in known:
            return ("accepted", None)
        if pmid in rejected:
            return ("rejected", None)
        source, scores = self._final(uid)
        if source == "input_unavailable":
            return ("input_unavailable", None)
        if pmid in scores:
            f = scores[pmid]
            return ("suggested_ge30" if f >= STORAGE_THRESHOLD else "buried", round(f, 2))
        return ("absent", None)

    def gate(self, pmid, candidate_uids):
        per = {u: self.status(u, pmid) for u in candidate_uids}
        attributed = any(s[0] in ATTRIBUTED for s in per.values())
        return {"pmid": int(pmid), "attributed": attributed, "per_candidate": per}


def _selftest():
    r = AttributionResolver()
    # an accepted pmid for stw2006 (first in his knownpmids)
    known, _ = r._gold("stw2006")
    accepted_pmid = next(iter(known))
    cases = [
        ("stw2006", 42220538, "buried"),       # the Worgall paper
        ("stw2006", 41891022, "buried"),       # second buried Worgall paper
        ("stw2006", accepted_pmid, "accepted"),
        ("stw2006", 99999999, "absent"),
    ]
    print(f"{'uid':9} {'pmid':10} {'expect':16} {'got':16} score")
    ok = True
    for uid, pmid, exp in cases:
        st, sc = r.status(uid, pmid)
        flag = "OK" if st == exp else "** MISMATCH"
        ok &= st == exp
        print(f"{uid:9} {pmid:<10} {exp:16} {st:16} {sc}  {flag}")
    print("\ngate(42220538,[stw2006]) ->", r.gate(42220538, ["stw2006"]))
    print("gate(%d,[stw2006]) ->" % accepted_pmid, r.gate(accepted_pmid, ["stw2006"]))
    print("\nSELFTEST", "PASS" if ok else "FAIL")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--pmid", type=int)
    ap.add_argument("--uids", nargs="*", default=[])
    args = ap.parse_args()
    if args.selftest:
        _selftest()
    elif args.pmid and args.uids:
        print(json.dumps(AttributionResolver().gate(args.pmid, args.uids), indent=2))
    else:
        ap.error("use --selftest or --pmid with --uids")


if __name__ == "__main__":
    main()
